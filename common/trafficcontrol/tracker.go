package trafficcontrol

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-tun"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/gofrs/uuid/v5"
)

type TrackerMetadata struct {
	ID           uuid.UUID
	Metadata     adapter.InboundContext
	CreatedAt    time.Time
	ClosedAt     time.Time
	Upload       *atomic.Int64
	Download     *atomic.Int64
	Chain        []string
	Rule         adapter.Rule
	Outbound     string
	OutboundType string
	Trace        *RouteTrace

	// DestinationDomain and DestinationIP freeze the preferred logical target
	// when the routed tracker is created. A valid domain has priority and leaves
	// DestinationIP empty. Routing metadata may be mutated later by an outbound,
	// so history must not derive these dimensions at flush time.
	DestinationDomain string
	DestinationIP     string
}

type Tracker interface {
	Metadata() *TrackerMetadata
	Close() error
}

func (m *Manager) RoutedConnection(ctx context.Context, conn net.Conn, metadata adapter.InboundContext, matchedRule adapter.Rule, matchOutbound adapter.Outbound) net.Conn {
	upload := new(atomic.Int64)
	download := new(atomic.Int64)
	tracker := &connTracker{
		metadata: m.newTrackerMetadata(ctx, metadata, matchedRule, matchOutbound, upload, download),
		manager:  m,
	}
	tracker.ExtendedConn = bufio.NewCounterConn(conn, []N.CountFunc{func(n int64) {
		upload.Add(n)
		m.uploadTotal.Add(n)
	}}, []N.CountFunc{func(n int64) {
		download.Add(n)
		m.downloadTotal.Add(n)
	}})
	if !m.join(tracker) {
		_ = tracker.Close()
	}
	return tracker
}

func (m *Manager) RoutedPacketConnection(ctx context.Context, conn N.PacketConn, metadata adapter.InboundContext, matchedRule adapter.Rule, matchOutbound adapter.Outbound) N.PacketConn {
	upload := new(atomic.Int64)
	download := new(atomic.Int64)
	tracker := &packetConnTracker{
		metadata: m.newTrackerMetadata(ctx, metadata, matchedRule, matchOutbound, upload, download),
		manager:  m,
	}
	tracker.PacketConn = bufio.NewCounterPacketConn(conn, []N.CountFunc{func(n int64) {
		upload.Add(n)
		m.uploadTotal.Add(n)
	}}, []N.CountFunc{func(n int64) {
		download.Add(n)
		m.downloadTotal.Add(n)
	}})
	if !m.join(tracker) {
		_ = tracker.Close()
	}
	return tracker
}

func (m *Manager) RoutedFlow(ctx context.Context, metadata adapter.InboundContext, matchedRule adapter.Rule, matchOutbound adapter.Outbound) tun.FlowTracker {
	return &flowTracker{
		metadata: m.newTrackerMetadata(ctx, metadata, matchedRule, matchOutbound, new(atomic.Int64), new(atomic.Int64)),
		manager:  m,
	}
}

func (m *Manager) newTrackerMetadata(ctx context.Context, metadata adapter.InboundContext, matchedRule adapter.Rule, matchOutbound adapter.Outbound, upload *atomic.Int64, download *atomic.Int64) TrackerMetadata {
	id, _ := uuid.NewV4()
	var (
		chain        []string
		next         string
		outbound     string
		outboundType string
	)
	if matchOutbound != nil {
		next = matchOutbound.Tag()
	} else {
		next = m.outbound.Default().Tag()
	}
	for {
		detour, loaded := m.outbound.Outbound(next)
		if !loaded {
			break
		}
		chain = append(chain, next)
		outbound = detour.Tag()
		outboundType = detour.Type()
		outboundGroup, isGroup := detour.(adapter.OutboundGroup)
		if !isGroup {
			break
		}
		next = outboundGroup.Now()
		if networkGroup, isNetworkGroup := outboundGroup.(adapter.NetworkAwareOutboundGroup); isNetworkGroup {
			next = networkGroup.NowForNetwork(metadata.Network)
		}
	}
	destinationDomain, destinationIP := destinationFromMetadata(metadata)
	return TrackerMetadata{
		ID:                id,
		Metadata:          metadata,
		CreatedAt:         time.Now(),
		Upload:            upload,
		Download:          download,
		Chain:             common.Reverse(chain),
		Rule:              matchedRule,
		Outbound:          outbound,
		OutboundType:      outboundType,
		Trace:             RouteTraceFromContext(ctx),
		DestinationDomain: destinationDomain,
		DestinationIP:     destinationIP,
	}
}

func destinationFromMetadata(metadata adapter.InboundContext) (string, string) {
	domain := destinationDomainFromMetadata(metadata)
	if domain != "" {
		return domain, ""
	}
	return "", destinationIPFromMetadata(metadata)
}

func destinationDomainFromMetadata(metadata adapter.InboundContext) string {
	domain := normalizeDestinationDomain(metadata.Destination.Fqdn)
	if domain != "" {
		return domain
	}
	return normalizeDestinationDomain(metadata.Domain)
}

func normalizeDestinationDomain(domain string) string {
	domain = strings.TrimSuffix(strings.ToLower(domain), ".")
	if domain == "" || strings.HasSuffix(domain, ".") ||
		net.ParseIP(domain) != nil || !M.IsDomainName(domain) {
		return ""
	}
	return domain
}

func destinationIPFromMetadata(metadata adapter.InboundContext) string {
	address := metadata.Destination.Addr
	if !address.IsValid() {
		return ""
	}
	return normalizeDestinationIP(address.String())
}

func normalizeDestinationIP(address string) string {
	parsed, err := netip.ParseAddr(address)
	if err != nil {
		return ""
	}
	return parsed.WithZone("").Unmap().String()
}

type connTracker struct {
	N.ExtendedConn
	metadata      TrackerMetadata
	manager       *Manager
	activity      trackerActivity
	closeOnce     sync.Once
	closeErr      error
	historyAccess sync.Mutex
	lastUpload    atomic.Int64
	lastDownload  atomic.Int64
	recorded      atomic.Bool
}

func (t *connTracker) Metadata() *TrackerMetadata {
	return &t.metadata
}

func (t *connTracker) Close() error {
	t.closeOnce.Do(func() {
		t.closeErr = t.activity.stopAndWait(t.ExtendedConn.Close)
		t.manager.leave(t)
	})
	return t.closeErr
}

func (t *connTracker) Read(buffer []byte) (int, error) {
	if !t.activity.begin() {
		return 0, net.ErrClosed
	}
	defer t.activity.end()
	return t.ExtendedConn.Read(buffer)
}

func (t *connTracker) ReadBuffer(buffer *buf.Buffer) error {
	if !t.activity.begin() {
		return net.ErrClosed
	}
	defer t.activity.end()
	return t.ExtendedConn.ReadBuffer(buffer)
}

func (t *connTracker) ReadCached() *buf.Buffer {
	if !t.activity.begin() {
		return nil
	}
	defer t.activity.end()
	reader, counters := N.UnwrapCountReader(t.ExtendedConn, nil)
	cachedReader, isCached := reader.(N.CachedReader)
	if !isCached {
		return nil
	}
	buffer := cachedReader.ReadCached()
	if buffer != nil {
		for _, counter := range counters {
			counter(int64(buffer.Len()))
		}
	}
	return buffer
}

func (t *connTracker) Write(buffer []byte) (int, error) {
	if !t.activity.begin() {
		return 0, net.ErrClosed
	}
	defer t.activity.end()
	return t.ExtendedConn.Write(buffer)
}

func (t *connTracker) WriteBuffer(buffer *buf.Buffer) error {
	if !t.activity.begin() {
		buffer.Release()
		return net.ErrClosed
	}
	defer t.activity.end()
	return t.ExtendedConn.WriteBuffer(buffer)
}

func (t *connTracker) RecordHistory(bool) {
	if t.manager.deltaRecorder == nil || !historyTraceResolved(&t.metadata) {
		return
	}
	t.historyAccess.Lock()
	defer t.historyAccess.Unlock()
	uplink := t.metadata.Upload.Load()
	downlink := t.metadata.Download.Load()
	t.manager.deltaRecorder.RecordDelta(
		&t.metadata,
		uplink-t.lastUpload.Swap(uplink),
		downlink-t.lastDownload.Swap(downlink),
		!t.recorded.Swap(true),
	)
}

func (t *connTracker) Upstream() any {
	return t.ExtendedConn
}

func (t *connTracker) ReaderReplaceable() bool {
	return t.manager.deltaRecorder == nil
}

func (t *connTracker) WriterReplaceable() bool {
	return t.manager.deltaRecorder == nil
}

var (
	_ Tracker         = (*flowTracker)(nil)
	_ tun.FlowTracker = (*flowTracker)(nil)
)

type flowTracker struct {
	metadata      TrackerMetadata
	manager       *Manager
	handle        tun.FlowHandle
	activity      trackerActivity
	closeOnce     sync.Once
	historyAccess sync.Mutex
	lastUpload    atomic.Int64
	lastDownload  atomic.Int64
	recorded      atomic.Bool
}

func (t *flowTracker) Metadata() *TrackerMetadata {
	return &t.metadata
}

func (t *flowTracker) AttachFlow(handle tun.FlowHandle) {
	t.handle = handle
	if !t.manager.join(t) {
		handle.CloseFlow()
		t.finish()
	}
}

func (t *flowTracker) CountForward(n int) {
	if !t.activity.begin() {
		return
	}
	defer t.activity.end()
	t.metadata.Upload.Add(int64(n))
	t.manager.uploadTotal.Add(int64(n))
}

func (t *flowTracker) CountReverse(n int) {
	if !t.activity.begin() {
		return
	}
	defer t.activity.end()
	t.metadata.Download.Add(int64(n))
	t.manager.downloadTotal.Add(int64(n))
}

func (t *flowTracker) RecordHistory(bool) {
	if t.manager.deltaRecorder == nil || !historyTraceResolved(&t.metadata) {
		return
	}
	t.historyAccess.Lock()
	defer t.historyAccess.Unlock()
	uplink := t.metadata.Upload.Load()
	downlink := t.metadata.Download.Load()
	t.manager.deltaRecorder.RecordDelta(
		&t.metadata,
		uplink-t.lastUpload.Swap(uplink),
		downlink-t.lastDownload.Swap(downlink),
		!t.recorded.Swap(true),
	)
}

func (t *flowTracker) FlowEstablished() {
}

func (t *flowTracker) CloseFlow(reason tun.FlowCloseReason) {
	t.finish()
}

func (t *flowTracker) Close() error {
	handle := t.handle
	if handle != nil {
		handle.CloseFlow()
	}
	t.finish()
	return nil
}

func (t *flowTracker) finish() {
	t.closeOnce.Do(func() {
		_ = t.activity.stopAndWait(nil)
		t.manager.leave(t)
	})
}

type packetConnTracker struct {
	N.PacketConn
	metadata      TrackerMetadata
	manager       *Manager
	activity      trackerActivity
	closeOnce     sync.Once
	closeErr      error
	historyAccess sync.Mutex
	lastUpload    atomic.Int64
	lastDownload  atomic.Int64
	recorded      atomic.Bool
}

func (t *packetConnTracker) Metadata() *TrackerMetadata {
	return &t.metadata
}

func (t *packetConnTracker) Close() error {
	t.closeOnce.Do(func() {
		t.closeErr = t.activity.stopAndWait(t.PacketConn.Close)
		t.manager.leave(t)
	})
	return t.closeErr
}

func (t *packetConnTracker) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	if !t.activity.begin() {
		return M.Socksaddr{}, net.ErrClosed
	}
	defer t.activity.end()
	return t.PacketConn.ReadPacket(buffer)
}

func (t *packetConnTracker) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	if !t.activity.begin() {
		buffer.Release()
		return net.ErrClosed
	}
	defer t.activity.end()
	return t.PacketConn.WritePacket(buffer, destination)
}

func (t *packetConnTracker) RecordHistory(bool) {
	if t.manager.deltaRecorder == nil || !historyTraceResolved(&t.metadata) {
		return
	}
	t.historyAccess.Lock()
	defer t.historyAccess.Unlock()
	uplink := t.metadata.Upload.Load()
	downlink := t.metadata.Download.Load()
	t.manager.deltaRecorder.RecordDelta(
		&t.metadata,
		uplink-t.lastUpload.Swap(uplink),
		downlink-t.lastDownload.Swap(downlink),
		!t.recorded.Swap(true),
	)
}

func (t *packetConnTracker) Upstream() any {
	return t.PacketConn
}

func (t *packetConnTracker) ReaderReplaceable() bool {
	return t.manager.deltaRecorder == nil
}

func (t *packetConnTracker) WriterReplaceable() bool {
	return t.manager.deltaRecorder == nil
}

type trackerActivity struct {
	access  sync.Mutex
	stopped bool
	active  sync.WaitGroup
}

func (a *trackerActivity) begin() bool {
	a.access.Lock()
	defer a.access.Unlock()
	if a.stopped {
		return false
	}
	a.active.Add(1)
	return true
}

func (a *trackerActivity) end() {
	a.active.Done()
}

func (a *trackerActivity) stopAndWait(closeFunc func() error) error {
	a.access.Lock()
	a.stopped = true
	a.access.Unlock()
	var err error
	if closeFunc != nil {
		err = closeFunc()
	}
	a.active.Wait()
	return err
}

func historyTraceResolved(metadata *TrackerMetadata) bool {
	if metadata.Trace == nil {
		return true
	}
	return metadata.Trace.Snapshot().Resolved
}

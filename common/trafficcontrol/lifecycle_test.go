package trafficcontrol

import (
	"bytes"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/gofrs/uuid/v5"
)

func TestManagerCloseDrainsConnectionCounter(t *testing.T) {
	recorder := new(recordingDeltaRecorder)
	manager := NewManager(nil, recorder)
	client, server := net.Pipe()
	defer server.Close()

	tracker := newLifecycleConnTracker(t, manager)
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	var callbackOnce sync.Once
	tracker.ExtendedConn = bufio.NewCounterConn(
		bufio.NewExtendedConn(client),
		[]N.CountFunc{func(n int64) {
			callbackOnce.Do(func() {
				close(callbackStarted)
			})
			<-releaseCallback
			tracker.metadata.Upload.Add(n)
		}},
		nil,
	)
	if !manager.join(tracker) {
		t.Fatal("manager rejected tracker before close")
	}

	payload := []byte("connection tail")
	readResult := make(chan lifecycleIOResult, 1)
	go func() {
		buffer := make([]byte, len(payload))
		n, err := tracker.Read(buffer)
		readResult <- lifecycleIOResult{n: n, err: err}
	}()
	writeResult := make(chan error, 1)
	go func() {
		_, err := server.Write(payload)
		writeResult <- err
	}()
	waitForSignal(t, callbackStarted, "connection counter callback")

	closeResult := make(chan error, 1)
	closeStarted := make(chan struct{})
	go func() {
		close(closeStarted)
		closeResult <- manager.Close()
	}()
	waitForSignal(t, closeStarted, "manager close start")
	assertStillBlocked(t, closeResult, "manager close before connection counter completed")

	close(releaseCallback)
	if err := <-closeResult; err != nil {
		t.Fatal("close manager:", err)
	}
	if result := <-readResult; result.err != nil || result.n != len(payload) {
		t.Fatalf("unexpected read result: %#v", result)
	}
	if err := <-writeResult; err != nil {
		t.Fatal("write test payload:", err)
	}
	assertSingleDelta(t, recorder.records, int64(len(payload)), 0)
	if manager.ConnectionsLen() != 0 {
		t.Fatalf("manager returned with %d live connections", manager.ConnectionsLen())
	}
}

func TestManagerCloseDrainsPacketCounter(t *testing.T) {
	recorder := new(recordingDeltaRecorder)
	manager := NewManager(nil, recorder)
	tracker := newLifecyclePacketTracker(t, manager)
	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	var callbackOnce sync.Once
	tracker.PacketConn = bufio.NewCounterPacketConn(
		new(lifecyclePacketConn),
		nil,
		[]N.CountFunc{func(n int64) {
			callbackOnce.Do(func() {
				close(callbackStarted)
			})
			<-releaseCallback
			tracker.metadata.Download.Add(n)
		}},
	)
	if !manager.join(tracker) {
		t.Fatal("manager rejected tracker before close")
	}

	payload := []byte("packet tail")
	writeResult := make(chan error, 1)
	go func() {
		writeResult <- tracker.WritePacket(buf.As(payload), M.Socksaddr{})
	}()
	waitForSignal(t, callbackStarted, "packet counter callback")

	closeResult := make(chan error, 1)
	closeStarted := make(chan struct{})
	go func() {
		close(closeStarted)
		closeResult <- manager.Close()
	}()
	waitForSignal(t, closeStarted, "manager close start")
	assertStillBlocked(t, closeResult, "manager close before packet counter completed")

	close(releaseCallback)
	if err := <-closeResult; err != nil {
		t.Fatal("close manager:", err)
	}
	if err := <-writeResult; err != nil {
		t.Fatal("write test packet:", err)
	}
	assertSingleDelta(t, recorder.records, 0, int64(len(payload)))
	if manager.ConnectionsLen() != 0 {
		t.Fatalf("manager returned with %d live packet connections", manager.ConnectionsLen())
	}
}

func TestManagerCloseWaitsForLeaveRecorderAndSealsJoin(t *testing.T) {
	recorder := &blockingDeltaRecorder{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewManager(nil, recorder)
	tracker := newLifecycleConnTracker(t, manager)
	tracker.metadata.Upload.Store(1)
	if !manager.join(tracker) {
		t.Fatal("manager rejected tracker before close")
	}

	leaveDone := make(chan struct{})
	go func() {
		manager.leave(tracker)
		close(leaveDone)
	}()
	waitForSignal(t, recorder.started, "final recorder")

	closeResult := make(chan error, 1)
	closeStarted := make(chan struct{})
	go func() {
		close(closeStarted)
		closeResult <- manager.Close()
	}()
	waitForSignal(t, closeStarted, "manager close start")
	assertStillBlocked(t, closeResult, "manager close before an in-flight leave completed")

	close(recorder.release)
	waitForSignal(t, leaveDone, "tracker leave")
	if err := <-closeResult; err != nil {
		t.Fatal("close manager:", err)
	}

	rejected := newLifecycleConnTracker(t, manager)
	if manager.join(rejected) {
		t.Fatal("manager accepted a tracker after close")
	}
	rejected.metadata.Upload.Store(100)
	manager.leave(rejected)
	if recorder.calls.Load() != 1 {
		t.Fatalf("late tracker reached recorder: %d calls", recorder.calls.Load())
	}
}

func TestTrackerFinalSkipsUnresolvedTrace(t *testing.T) {
	recorder := new(recordingDeltaRecorder)
	manager := NewManager(nil, recorder)
	tracker := newLifecycleConnTracker(t, manager)
	tracker.metadata.Trace = NewRouteTrace("Proxy", "selector", true)
	tracker.metadata.Upload.Store(100)
	tracker.metadata.Download.Store(50)

	tracker.RecordHistory(true)
	if len(recorder.records) != 0 {
		t.Fatalf("final sample wrote an unresolved leaf: %#v", recorder.records)
	}
}

func TestRoutedTrackersCloseWhenManagerSealed(t *testing.T) {
	outbound := &lifecycleOutbound{tag: "direct-out"}
	outboundManager := &lifecycleOutboundManager{outbound: outbound}
	manager := NewManager(outboundManager, new(recordingDeltaRecorder))
	if err := manager.Close(); err != nil {
		t.Fatal("seal manager:", err)
	}

	client, server := net.Pipe()
	defer server.Close()
	conn := manager.RoutedConnection(
		context.Background(),
		client,
		adapter.InboundContext{Network: N.NetworkTCP},
		nil,
		outbound,
	)
	if _, err := conn.Read(make([]byte, 1)); !isClosedError(err) {
		t.Fatalf("rejected connection remained open: %v", err)
	}

	packetSource := new(lifecyclePacketConn)
	packetConn := manager.RoutedPacketConnection(
		context.Background(),
		packetSource,
		adapter.InboundContext{Network: N.NetworkUDP},
		nil,
		outbound,
	)
	if err := packetConn.WritePacket(buf.As([]byte{1}), M.Socksaddr{}); !isClosedError(err) {
		t.Fatalf("rejected packet connection remained open: %v", err)
	}
	if packetSource.closeCalls.Load() != 1 {
		t.Fatalf("rejected packet connection was closed %d times", packetSource.closeCalls.Load())
	}

	flow := manager.RoutedFlow(
		context.Background(),
		adapter.InboundContext{Network: N.NetworkTCP},
		nil,
		outbound,
	).(*flowTracker)
	handle := &lifecycleFlowHandle{tracker: flow}
	flow.AttachFlow(handle)
	if handle.closeCalls.Load() != 1 {
		t.Fatalf("rejected flow was closed %d times", handle.closeCalls.Load())
	}
	if manager.ConnectionsLen() != 0 {
		t.Fatalf("sealed manager retained %d trackers", manager.ConnectionsLen())
	}
}

func TestTrackerMetadataUsesNetworkAwareGroupSelection(t *testing.T) {
	tcpNode := &lifecycleOutbound{tag: "tcp-node"}
	udpNode := &lifecycleOutbound{tag: "udp-node"}
	group := &lifecycleNetworkGroup{
		lifecycleOutbound: &lifecycleOutbound{tag: "Auto"},
		tcpTag:            tcpNode.Tag(),
		udpTag:            udpNode.Tag(),
	}
	outboundManager := &lifecycleOutboundManager{
		outbound: group,
		outbounds: map[string]adapter.Outbound{
			group.Tag():   group,
			tcpNode.Tag(): tcpNode,
			udpNode.Tag(): udpNode,
		},
	}
	manager := NewManager(outboundManager)
	metadata := manager.newTrackerMetadata(
		context.Background(),
		adapter.InboundContext{Network: N.NetworkUDP},
		nil,
		group,
		new(atomic.Int64),
		new(atomic.Int64),
	)
	if metadata.Outbound != udpNode.Tag() {
		t.Fatalf("legacy metadata selected %q for UDP, want %q", metadata.Outbound, udpNode.Tag())
	}
}

func TestConnectionTrackerPreservesCachedPayloadForLazyHandshake(t *testing.T) {
	outbound := &lifecycleOutbound{tag: "direct-out"}
	outboundManager := &lifecycleOutboundManager{outbound: outbound}
	recorder := new(recordingDeltaRecorder)
	manager := NewManager(outboundManager, recorder)
	client, server := net.Pipe()
	defer server.Close()

	payload := []byte("sniffed first payload")
	conn := manager.RoutedConnection(
		context.Background(),
		bufio.NewCachedConn(client, buf.As(payload)),
		adapter.InboundContext{Network: N.NetworkTCP},
		nil,
		outbound,
	)
	source, counters := N.UnwrapCountReader(conn, nil)
	if len(counters) != 0 {
		t.Fatalf("history tracker was unwrapped into %d counters", len(counters))
	}
	cachedReader, loaded := source.(N.CachedReader)
	if !loaded {
		t.Fatalf("history tracker no longer exposes cached payload: %T", source)
	}
	cachedPayload := cachedReader.ReadCached()
	if cachedPayload == nil {
		t.Fatal("missing cached payload")
	}
	var destination bytes.Buffer
	if _, err := destination.Write(cachedPayload.Bytes()); err != nil {
		t.Fatal("write cached payload:", err)
	}
	cachedPayload.Release()
	for _, counter := range counters {
		counter(int64(len(payload)))
	}
	if destination.String() != string(payload) {
		t.Fatalf("unexpected forwarded cached payload: %q", destination.String())
	}

	tracker := conn.(*connTracker)
	if tracker.metadata.Upload.Load() != int64(len(payload)) {
		t.Fatalf("cached payload counted as %d bytes, want %d", tracker.metadata.Upload.Load(), len(payload))
	}
	if err := conn.Close(); err != nil {
		t.Fatal("close cached connection:", err)
	}
	assertSingleDelta(t, recorder.records, int64(len(payload)), 0)
}

func newLifecycleConnTracker(t *testing.T, manager *Manager) *connTracker {
	t.Helper()
	id, err := uuid.NewV4()
	if err != nil {
		t.Fatal("create tracker ID:", err)
	}
	return &connTracker{
		metadata: TrackerMetadata{
			ID:       id,
			Metadata: adapter.InboundContext{Network: N.NetworkTCP},
			Upload:   new(atomic.Int64),
			Download: new(atomic.Int64),
			Trace:    NewRouteTrace("direct-out", "direct", false),
		},
		manager: manager,
	}
}

func newLifecyclePacketTracker(t *testing.T, manager *Manager) *packetConnTracker {
	t.Helper()
	id, err := uuid.NewV4()
	if err != nil {
		t.Fatal("create tracker ID:", err)
	}
	return &packetConnTracker{
		metadata: TrackerMetadata{
			ID:       id,
			Metadata: adapter.InboundContext{Network: N.NetworkUDP},
			Upload:   new(atomic.Int64),
			Download: new(atomic.Int64),
			Trace:    NewRouteTrace("direct-out", "direct", false),
		},
		manager: manager,
	}
}

func assertSingleDelta(t *testing.T, records []recordedHistoryDelta, uplink int64, downlink int64) {
	t.Helper()
	if len(records) != 1 {
		t.Fatalf("expected one final delta, got %#v", records)
	}
	if records[0].uplink != uplink || records[0].downlink != downlink || !records[0].newConnection {
		t.Fatalf("unexpected final delta: %#v", records[0])
	}
}

func waitForSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ", description)
	}
}

func assertStillBlocked(t *testing.T, result <-chan error, description string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("%s: %v", description, err)
	case <-time.After(25 * time.Millisecond):
	}
}

type lifecycleIOResult struct {
	n   int
	err error
}

type blockingDeltaRecorder struct {
	startOnce sync.Once
	started   chan struct{}
	release   chan struct{}
	calls     atomic.Int64
}

func (r *blockingDeltaRecorder) RecordDelta(*TrackerMetadata, int64, int64, bool) {
	r.calls.Add(1)
	r.startOnce.Do(func() {
		close(r.started)
	})
	<-r.release
}

type lifecyclePacketConn struct {
	closeCalls atomic.Int64
}

func (c *lifecyclePacketConn) ReadPacket(*buf.Buffer) (M.Socksaddr, error) {
	return M.Socksaddr{}, net.ErrClosed
}

func (c *lifecyclePacketConn) WritePacket(buffer *buf.Buffer, _ M.Socksaddr) error {
	buffer.Release()
	return nil
}

func (c *lifecyclePacketConn) Close() error {
	c.closeCalls.Add(1)
	return nil
}

func (c *lifecyclePacketConn) LocalAddr() net.Addr {
	return nil
}

func (c *lifecyclePacketConn) SetDeadline(time.Time) error {
	return nil
}

func (c *lifecyclePacketConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *lifecyclePacketConn) SetWriteDeadline(time.Time) error {
	return nil
}

type lifecycleFlowHandle struct {
	tracker    *flowTracker
	closeCalls atomic.Int64
}

func (h *lifecycleFlowHandle) CloseFlow() {
	h.closeCalls.Add(1)
	h.tracker.CloseFlow(0)
}

type lifecycleOutbound struct {
	N.Dialer
	tag string
}

func (o *lifecycleOutbound) Type() string {
	return "direct"
}

func (o *lifecycleOutbound) Tag() string {
	return o.tag
}

func (o *lifecycleOutbound) Network() []string {
	return []string{N.NetworkTCP, N.NetworkUDP}
}

func (o *lifecycleOutbound) Dependencies() []string {
	return nil
}

type lifecycleOutboundManager struct {
	adapter.OutboundManager
	outbound  adapter.Outbound
	outbounds map[string]adapter.Outbound
}

func (m *lifecycleOutboundManager) Outbounds() []adapter.Outbound {
	return []adapter.Outbound{m.outbound}
}

func (m *lifecycleOutboundManager) Outbound(tag string) (adapter.Outbound, bool) {
	if m.outbounds != nil {
		outbound, loaded := m.outbounds[tag]
		return outbound, loaded
	}
	return m.outbound, tag == m.outbound.Tag()
}

func (m *lifecycleOutboundManager) Default() adapter.Outbound {
	return m.outbound
}

type lifecycleNetworkGroup struct {
	*lifecycleOutbound
	tcpTag string
	udpTag string
}

func (g *lifecycleNetworkGroup) Type() string {
	return "urltest"
}

func (g *lifecycleNetworkGroup) Now() string {
	return g.tcpTag
}

func (g *lifecycleNetworkGroup) NowForNetwork(network string) string {
	if network == N.NetworkUDP {
		return g.udpTag
	}
	return g.tcpTag
}

func (g *lifecycleNetworkGroup) All() []string {
	return []string{g.tcpTag, g.udpTag}
}

func isClosedError(err error) bool {
	return err == net.ErrClosed
}

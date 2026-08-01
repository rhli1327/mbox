package trafficcontrol

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/netip"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

func runHistoryPendingAndFlushedResultsAreEquivalent(t *testing.T, harness historyBackendHarness) {
	history := harness.Open(t, []byte("pending-equivalence"))
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close history:", err)
		}
	}()

	metadata := resolvedMetadata("tcp", "Proxy", []groupSelection{
		{parent: "Proxy", selected: "Auto", selectedType: "urltest", selectedIsGroup: true},
		{parent: "Auto", selected: "node-a", selectedType: "vmess"},
	})
	metadata.DestinationDomain = "Pending.Example."
	expectedCounters := HistoryCounters{
		UplinkBytes:   18,
		DownlinkBytes: 24,
		Connections:   1,
	}
	history.RecordDelta(metadata, 18, 24, true)

	seed, err := history.Query(context.Background(), HistoryQuery{})
	if err != nil {
		t.Fatal("query pending bucket:", err)
	}
	if seed.TotalRows != 1 ||
		len(seed.Rows) != 1 ||
		seed.Totals != expectedCounters ||
		seed.ActualFrom.IsZero() ||
		!seed.ActualTo.Equal(seed.ActualFrom.Add(HistoryBucketInterval)) {
		t.Fatalf("unexpected pending bucket seed: %#v", seed)
	}

	bucketStart := seed.ActualFrom

	query := HistoryQuery{
		From:               bucketStart,
		To:                 bucketStart.Add(HistoryBucketInterval),
		RouteTags:          []string{"Proxy"},
		GroupTags:          []string{"Auto"},
		ActualOutboundTags: []string{"node-a"},
		Networks:           []string{"tcp"},
	}
	firstPending, err := history.Query(context.Background(), query)
	if err != nil {
		t.Fatal("query first pending snapshot:", err)
	}
	secondPending, err := history.Query(context.Background(), query)
	if err != nil {
		t.Fatal("query second pending snapshot:", err)
	}
	if !reflect.DeepEqual(secondPending, firstPending) {
		t.Fatalf("repeated pending query changed the result:\nfirst:  %#v\nsecond: %#v", firstPending, secondPending)
	}
	if !firstPending.ActualFrom.Equal(bucketStart) ||
		!firstPending.ActualTo.Equal(bucketStart.Add(HistoryBucketInterval)) {
		t.Fatalf("unexpected included bucket range: %#v", firstPending)
	}
	assertCharacterizedHistoryResult(
		t,
		firstPending,
		history.configRevision,
		"Proxy",
		[]string{"Proxy", "Auto"},
		"node-a",
		"vmess",
		"tcp",
		expectedCounters,
	)

	targetQuery := query
	targetQuery.GroupBy = HistoryGroupByDestination
	targetQuery.Destinations = []string{"pending.example"}
	pendingTarget, err := history.Query(context.Background(), targetQuery)
	if err != nil {
		t.Fatal("query pending target:", err)
	}
	if pendingTarget.TotalRows != 1 ||
		pendingTarget.Rows[0].Destination != "pending.example" ||
		pendingTarget.Rows[0].DestinationType != HistoryDestinationTypeDomain ||
		pendingTarget.Rows[0].DestinationDomain != "pending.example" ||
		pendingTarget.Totals != firstPending.Totals {
		t.Fatalf("unexpected pending target result: %#v", pendingTarget)
	}

	if err = history.flush(); err != nil {
		t.Fatal("flush pending history:", err)
	}
	flushed, err := history.Query(context.Background(), query)
	if err != nil {
		t.Fatal("query flushed snapshot:", err)
	}
	if !reflect.DeepEqual(flushed, firstPending) {
		t.Fatalf("flush changed the query result:\npending: %#v\nflushed: %#v", firstPending, flushed)
	}
	flushedTarget, err := history.Query(context.Background(), targetQuery)
	if err != nil {
		t.Fatal("query flushed target:", err)
	}
	if !reflect.DeepEqual(flushedTarget, pendingTarget) {
		t.Fatalf("flush changed the target result:\npending: %#v\nflushed: %#v", pendingTarget, flushedTarget)
	}

	excludedAfter, err := history.Query(context.Background(), HistoryQuery{
		From: bucketStart.Add(HistoryBucketInterval),
		To:   bucketStart.Add(2 * HistoryBucketInterval),
	})
	if err != nil {
		t.Fatal("query range after bucket:", err)
	}
	if excludedAfter.TotalRows != 0 || excludedAfter.Totals != (HistoryCounters{}) {
		t.Fatalf("bucket leaked past its half-open range: %#v", excludedAfter)
	}
	excludedBefore, err := history.Query(context.Background(), HistoryQuery{
		From: bucketStart.Add(-HistoryBucketInterval),
		To:   bucketStart,
	})
	if err != nil {
		t.Fatal("query range before bucket:", err)
	}
	if excludedBefore.TotalRows != 0 || excludedBefore.Totals != (HistoryCounters{}) {
		t.Fatalf("bucket leaked before its half-open range: %#v", excludedBefore)
	}
}

func TestManagerTrackerRecordsLogicalPayloadIntoHistory(t *testing.T) {
	t.Run("direct TCP", func(t *testing.T) {
		history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "manager-direct-tcp")
		enableCurrentCharacterizationTargets(history)
		defer closeCharacterizationHistory(t, history)
		outbound := &lifecycleOutbound{tag: "direct-out"}
		manager := NewManager(&lifecycleOutboundManager{outbound: outbound}, history)
		defer closeCharacterizationManager(t, manager)

		trace := NewRouteTrace(outbound.Tag(), outbound.Type(), false)
		ctx := ContextWithExistingRouteTrace(context.Background(), trace)
		client, peer := net.Pipe()
		defer peer.Close()
		tracked := manager.RoutedConnection(
			ctx,
			client,
			adapter.InboundContext{
				Network:     N.NetworkTCP,
				Destination: M.Socksaddr{Fqdn: "TCP.Example."},
			},
			nil,
			outbound,
		)
		exchangeCharacterizationTCP(t, tracked, peer, bytes.Repeat([]byte{0x13}, 13), bytes.Repeat([]byte{0x17}, 17))
		if err := tracked.Close(); err != nil {
			t.Fatal("close tracked TCP connection:", err)
		}

		result, err := history.Query(context.Background(), HistoryQuery{})
		if err != nil {
			t.Fatal("query direct TCP history:", err)
		}
		assertCharacterizedHistoryResult(
			t,
			result,
			history.configRevision,
			"direct-out",
			[]string{},
			"direct-out",
			"direct",
			N.NetworkTCP,
			HistoryCounters{UplinkBytes: 13, DownlinkBytes: 17, Connections: 1},
		)
		target, err := history.Query(context.Background(), HistoryQuery{
			GroupBy:      HistoryGroupByDestination,
			Destinations: []string{"tcp.example"},
		})
		if err != nil {
			t.Fatal("query direct TCP target:", err)
		}
		assertCharacterizedDestinationResult(
			t,
			target,
			"tcp.example",
			HistoryDestinationTypeDomain,
			"tcp.example",
			HistoryCounters{
				UplinkBytes:   13,
				DownlinkBytes: 17,
				Connections:   1,
			},
		)
	})

	t.Run("direct UDP", func(t *testing.T) {
		history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "manager-direct-udp")
		enableCurrentCharacterizationTargets(history)
		defer closeCharacterizationHistory(t, history)
		outbound := &lifecycleOutbound{tag: "direct-out"}
		manager := NewManager(&lifecycleOutboundManager{outbound: outbound}, history)
		defer closeCharacterizationManager(t, manager)
		source := &characterizationPacketConn{
			readPayloads: [][]byte{
				bytes.Repeat([]byte{0x07}, 7),
				bytes.Repeat([]byte{0x0b}, 11),
			},
		}
		trace := NewRouteTrace(outbound.Tag(), outbound.Type(), false)
		ctx := ContextWithExistingRouteTrace(context.Background(), trace)
		tracked := manager.RoutedPacketConnection(
			ctx,
			source,
			adapter.InboundContext{
				Network:     N.NetworkUDP,
				Destination: M.Socksaddr{Addr: netip.MustParseAddr("192.0.2.60")},
			},
			nil,
			outbound,
		)
		for _, expectedSize := range []int{7, 11} {
			packet := buf.NewPacket()
			_, err := tracked.ReadPacket(packet)
			if err != nil {
				packet.Release()
				t.Fatal("read tracked UDP packet:", err)
			}
			if packet.Len() != expectedSize {
				packet.Release()
				t.Fatalf("unexpected tracked UDP read size: got %d, want %d", packet.Len(), expectedSize)
			}
			packet.Release()
		}
		for _, payload := range [][]byte{
			bytes.Repeat([]byte{0x05}, 5),
			bytes.Repeat([]byte{0x0d}, 13),
		} {
			if err := tracked.WritePacket(buf.As(payload), M.Socksaddr{}); err != nil {
				t.Fatal("write tracked UDP packet:", err)
			}
		}
		if err := tracked.Close(); err != nil {
			t.Fatal("close tracked UDP connection:", err)
		}
		if !reflect.DeepEqual(source.writeSizes, []int{5, 13}) {
			t.Fatalf("unexpected UDP payloads reaching the source: %#v", source.writeSizes)
		}

		result, err := history.Query(context.Background(), HistoryQuery{})
		if err != nil {
			t.Fatal("query direct UDP history:", err)
		}
		assertCharacterizedHistoryResult(
			t,
			result,
			history.configRevision,
			"direct-out",
			[]string{},
			"direct-out",
			"direct",
			N.NetworkUDP,
			HistoryCounters{UplinkBytes: 18, DownlinkBytes: 18, Connections: 1},
		)
		target, err := history.Query(context.Background(), HistoryQuery{
			GroupBy:      HistoryGroupByDestination,
			Destinations: []string{"192.0.2.60"},
		})
		if err != nil {
			t.Fatal("query direct UDP target:", err)
		}
		assertCharacterizedDestinationResult(
			t,
			target,
			"192.0.2.60",
			HistoryDestinationTypeIP,
			"",
			HistoryCounters{
				UplinkBytes:   18,
				DownlinkBytes: 18,
				Connections:   1,
			},
		)
	})

	t.Run("nested selector path", func(t *testing.T) {
		history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "manager-nested-selector")
		enableCurrentCharacterizationTargets(history)
		defer closeCharacterizationHistory(t, history)
		leaf := &lifecycleOutbound{tag: "node-a"}
		auto := &lifecycleNetworkGroup{
			lifecycleOutbound: &lifecycleOutbound{tag: "Auto"},
			tcpTag:            leaf.Tag(),
			udpTag:            leaf.Tag(),
		}
		proxy := &lifecycleNetworkGroup{
			lifecycleOutbound: &lifecycleOutbound{tag: "Proxy"},
			tcpTag:            auto.Tag(),
			udpTag:            auto.Tag(),
		}
		manager := NewManager(&lifecycleOutboundManager{
			outbound: proxy,
			outbounds: map[string]adapter.Outbound{
				proxy.Tag(): proxy,
				auto.Tag():  auto,
				leaf.Tag():  leaf,
			},
		}, history)
		defer closeCharacterizationManager(t, manager)

		ctx := ContextWithRouteTrace(context.Background(), proxy)
		RecordOutboundSelection(ctx, proxy, auto)
		RecordOutboundSelection(ctx, auto, leaf)
		client, peer := net.Pipe()
		defer peer.Close()
		tracked := manager.RoutedConnection(
			ctx,
			client,
			adapter.InboundContext{
				Network:     N.NetworkTCP,
				Destination: M.Socksaddr{Fqdn: "Nested.Example."},
			},
			nil,
			proxy,
		)
		exchangeCharacterizationTCP(t, tracked, peer, bytes.Repeat([]byte{0x1d}, 29), bytes.Repeat([]byte{0x1f}, 31))
		if err := tracked.Close(); err != nil {
			t.Fatal("close nested tracked connection:", err)
		}

		result, err := history.Query(context.Background(), HistoryQuery{})
		if err != nil {
			t.Fatal("query nested selector history:", err)
		}
		assertCharacterizedHistoryResult(
			t,
			result,
			history.configRevision,
			"Proxy",
			[]string{"Proxy", "Auto"},
			"node-a",
			"direct",
			N.NetworkTCP,
			HistoryCounters{UplinkBytes: 29, DownlinkBytes: 31, Connections: 1},
		)
	})
}

func exchangeCharacterizationTCP(t *testing.T, tracked net.Conn, peer net.Conn, uplink []byte, downlink []byte) {
	t.Helper()
	uplinkWrite := make(chan error, 1)
	go func() {
		_, err := peer.Write(uplink)
		uplinkWrite <- err
	}()
	receivedUplink := make([]byte, len(uplink))
	if _, err := io.ReadFull(tracked, receivedUplink); err != nil {
		t.Fatal("read tracked uplink:", err)
	}
	if err := <-uplinkWrite; err != nil {
		t.Fatal("write peer uplink:", err)
	}
	if !bytes.Equal(receivedUplink, uplink) {
		t.Fatalf("unexpected uplink payload: %x != %x", receivedUplink, uplink)
	}

	type readResult struct {
		payload []byte
		err     error
	}
	downlinkRead := make(chan readResult, 1)
	go func() {
		payload := make([]byte, len(downlink))
		_, err := io.ReadFull(peer, payload)
		downlinkRead <- readResult{payload: payload, err: err}
	}()
	if _, err := tracked.Write(downlink); err != nil {
		t.Fatal("write tracked downlink:", err)
	}
	read := <-downlinkRead
	if read.err != nil {
		t.Fatal("read peer downlink:", read.err)
	}
	if !bytes.Equal(read.payload, downlink) {
		t.Fatalf("unexpected downlink payload: %x != %x", read.payload, downlink)
	}
}

func assertCharacterizedHistoryResult(
	t *testing.T,
	result HistoryQueryResult,
	revision string,
	routeTag string,
	groupPath []string,
	actualOutboundTag string,
	actualOutboundType string,
	network string,
	counters HistoryCounters,
) {
	t.Helper()
	if result.TotalRows != 1 || len(result.Rows) != 1 {
		t.Fatalf("expected one characterized row, got %#v", result)
	}
	if result.Totals != counters {
		t.Fatalf("unexpected characterized totals: got %#v, want %#v", result.Totals, counters)
	}
	row := result.Rows[0]
	if row.ConfigRevision != revision ||
		row.RouteTag != routeTag ||
		!reflect.DeepEqual(row.GroupPath, groupPath) ||
		row.ActualOutboundTag != actualOutboundTag ||
		row.ActualOutboundType != actualOutboundType ||
		row.Network != network ||
		row.UplinkBytes != counters.UplinkBytes ||
		row.DownlinkBytes != counters.DownlinkBytes ||
		row.Connections != counters.Connections {
		t.Fatalf("unexpected characterized row: %#v", row)
	}
}

func assertCharacterizedDestinationResult(
	t *testing.T,
	result HistoryQueryResult,
	destination string,
	destinationType string,
	destinationDomain string,
	counters HistoryCounters,
) {
	t.Helper()
	if result.TotalRows != 1 || len(result.Rows) != 1 {
		t.Fatalf("expected one characterized destination row, got %#v", result)
	}
	if result.Totals != counters {
		t.Fatalf("unexpected characterized destination totals: got %#v, want %#v", result.Totals, counters)
	}
	row := result.Rows[0]
	if row.Destination != destination ||
		row.DestinationType != destinationType ||
		row.DestinationDomain != destinationDomain ||
		row.UplinkBytes != counters.UplinkBytes ||
		row.DownlinkBytes != counters.DownlinkBytes ||
		row.Connections != counters.Connections {
		t.Fatalf("unexpected characterized destination row: %#v", row)
	}
}

func closeCharacterizationManager(t *testing.T, manager *Manager) {
	t.Helper()
	if err := manager.Close(); err != nil {
		t.Error("close manager:", err)
	}
}

func closeCharacterizationHistory(t *testing.T, history *History) {
	t.Helper()
	if err := history.Close(); err != nil {
		t.Error("close history:", err)
	}
}

func enableCurrentCharacterizationTargets(history *History) {
	bucketStart := time.Now().UTC().Truncate(HistoryBucketInterval)
	setTestHistoryAvailability(history, bucketStart, bucketStart)
}

type characterizationPacketConn struct {
	access       sync.Mutex
	readPayloads [][]byte
	writeSizes   []int
	closed       bool
}

func (c *characterizationPacketConn) ReadPacket(buffer *buf.Buffer) (M.Socksaddr, error) {
	c.access.Lock()
	defer c.access.Unlock()
	if c.closed || len(c.readPayloads) == 0 {
		return M.Socksaddr{}, net.ErrClosed
	}
	payload := c.readPayloads[0]
	c.readPayloads = c.readPayloads[1:]
	if _, err := buffer.Write(payload); err != nil {
		return M.Socksaddr{}, err
	}
	return M.Socksaddr{Addr: netip.MustParseAddr("192.0.2.60")}, nil
}

func (c *characterizationPacketConn) WritePacket(buffer *buf.Buffer, _ M.Socksaddr) error {
	c.access.Lock()
	defer c.access.Unlock()
	if c.closed {
		buffer.Release()
		return net.ErrClosed
	}
	c.writeSizes = append(c.writeSizes, buffer.Len())
	buffer.Release()
	return nil
}

func (c *characterizationPacketConn) Close() error {
	c.access.Lock()
	c.closed = true
	c.access.Unlock()
	return nil
}

func (c *characterizationPacketConn) LocalAddr() net.Addr {
	return nil
}

func (c *characterizationPacketConn) SetDeadline(time.Time) error {
	return nil
}

func (c *characterizationPacketConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *characterizationPacketConn) SetWriteDeadline(time.Time) error {
	return nil
}

package trafficcontrol

import (
	"reflect"
	"sync/atomic"
	"testing"
)

func TestRouteTraceNestedSelection(t *testing.T) {
	trace := NewRouteTrace("Proxy", "selector", true)

	trace.RecordSelection("Proxy", "AI", "selector", true)
	trace.RecordSelection("AI", "AI-Auto", "urltest", true)
	trace.RecordSelection("AI-Auto", "ai-node", "vmess", false)

	snapshot := trace.Snapshot()
	if snapshot.RouteTag != "Proxy" {
		t.Fatalf("unexpected route tag: %q", snapshot.RouteTag)
	}
	if !reflect.DeepEqual(snapshot.GroupPath, []string{"Proxy", "AI", "AI-Auto"}) {
		t.Fatalf("unexpected group path: %#v", snapshot.GroupPath)
	}
	if snapshot.ActualOutboundTag != "ai-node" {
		t.Fatalf("unexpected actual outbound tag: %q", snapshot.ActualOutboundTag)
	}
	if snapshot.ActualOutboundType != "vmess" {
		t.Fatalf("unexpected actual outbound type: %q", snapshot.ActualOutboundType)
	}

	snapshot.GroupPath[0] = "mutated"
	if nextSnapshot := trace.Snapshot(); nextSnapshot.GroupPath[0] != "Proxy" {
		t.Fatal("Snapshot returned an alias of the internal group path")
	}
}

func TestRouteTraceDirectSelection(t *testing.T) {
	snapshot := NewRouteTrace("direct-out", "direct", false).Snapshot()
	if snapshot.RouteTag != "direct-out" {
		t.Fatalf("unexpected route tag: %q", snapshot.RouteTag)
	}
	if len(snapshot.GroupPath) != 0 {
		t.Fatalf("unexpected group path: %#v", snapshot.GroupPath)
	}
	if snapshot.ActualOutboundTag != "direct-out" {
		t.Fatalf("unexpected actual outbound tag: %q", snapshot.ActualOutboundTag)
	}
	if snapshot.ActualOutboundType != "direct" {
		t.Fatalf("unexpected actual outbound type: %q", snapshot.ActualOutboundType)
	}
	if !snapshot.Resolved {
		t.Fatal("direct outbound trace was not resolved")
	}
}

func TestRouteTraceUntaggedDirectSelectionIsResolved(t *testing.T) {
	snapshot := NewRouteTrace("", "direct", false).Snapshot()
	if !snapshot.Resolved {
		t.Fatal("untagged direct outbound trace was not resolved")
	}
	if snapshot.ActualOutboundTag != "" || snapshot.ActualOutboundType != "direct" {
		t.Fatalf("unexpected untagged direct snapshot: %#v", snapshot)
	}
}

func TestTrackerHistoryWaitsForResolvedTrace(t *testing.T) {
	recorder := new(recordingDeltaRecorder)
	manager := NewManager(nil, recorder)
	trace := NewRouteTrace("Proxy", "selector", true)
	upload := new(atomic.Int64)
	download := new(atomic.Int64)
	tracker := &connTracker{
		metadata: TrackerMetadata{
			Upload:   upload,
			Download: download,
			Trace:    trace,
		},
		manager: manager,
	}

	upload.Store(100)
	download.Store(50)
	tracker.RecordHistory(false)
	if len(recorder.records) != 0 {
		t.Fatalf("recorded unresolved trace: %#v", recorder.records)
	}

	trace.RecordSelection("Proxy", "AI", "selector", true)
	tracker.RecordHistory(false)
	if len(recorder.records) != 0 {
		t.Fatalf("recorded trace before a leaf was selected: %#v", recorder.records)
	}

	trace.RecordSelection("AI", "ai-node", "vmess", false)
	tracker.RecordHistory(false)
	if len(recorder.records) != 1 {
		t.Fatalf("expected one record after resolution, got %d", len(recorder.records))
	}
	first := recorder.records[0]
	if first.uplink != 100 || first.downlink != 50 || !first.newConnection {
		t.Fatalf("unexpected first delta: %#v", first)
	}
	if !reflect.DeepEqual(first.trace.GroupPath, []string{"Proxy", "AI"}) {
		t.Fatalf("unexpected resolved group path: %#v", first.trace.GroupPath)
	}
	if first.trace.ActualOutboundTag != "ai-node" {
		t.Fatalf("unexpected resolved outbound: %q", first.trace.ActualOutboundTag)
	}

	upload.Add(25)
	download.Add(35)
	tracker.RecordHistory(false)
	if len(recorder.records) != 2 {
		t.Fatalf("expected a second incremental record, got %d", len(recorder.records))
	}
	second := recorder.records[1]
	if second.uplink != 25 || second.downlink != 35 || second.newConnection {
		t.Fatalf("unexpected second delta: %#v", second)
	}
}

type recordedHistoryDelta struct {
	trace         RouteTraceSnapshot
	uplink        int64
	downlink      int64
	newConnection bool
}

type recordingDeltaRecorder struct {
	records []recordedHistoryDelta
}

func (r *recordingDeltaRecorder) RecordDelta(metadata *TrackerMetadata, uplink int64, downlink int64, newConnection bool) {
	r.records = append(r.records, recordedHistoryDelta{
		trace:         metadata.Trace.Snapshot(),
		uplink:        uplink,
		downlink:      downlink,
		newConnection: newConnection,
	})
}

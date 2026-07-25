package trafficcontrol

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
)

func TestHistoryPersistenceReopenAndFilters(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "traffic.db")
	history := openTestHistory(t, databasePath, "revision-a")
	revisionA := history.configRevision

	aiMetadata := resolvedMetadata(
		"tcp",
		"AI",
		[]groupSelection{
			{parent: "AI", selected: "AI-Auto", selectedType: "urltest", selectedIsGroup: true},
			{parent: "AI-Auto", selected: "ai-node", selectedType: "vmess"},
		},
	)
	history.RecordDelta(aiMetadata, 100, 40, true)
	history.RecordDelta(aiMetadata, 50, 10, false)

	proxyMetadata := resolvedMetadata(
		"udp",
		"Proxy",
		[]groupSelection{
			{parent: "Proxy", selected: "jp-node", selectedType: "trojan"},
		},
	)
	history.RecordDelta(proxyMetadata, 20, 70, true)

	if err := history.Close(); err != nil {
		t.Fatal("close initial history:", err)
	}

	reopened := openTestHistory(t, databasePath, "revision-b")
	revisionB := reopened.configRevision
	if revisionA == revisionB {
		t.Fatal("different configuration inputs produced the same revision")
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error("close reopened history:", err)
		}
	}()

	result, err := reopened.Query(context.Background(), HistoryQuery{})
	if err != nil {
		t.Fatal("query reopened history:", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected two persisted rows, got %d: %#v", len(result.Rows), result.Rows)
	}
	if result.ActualFrom.IsZero() || result.ActualTo.IsZero() || !result.ActualFrom.Before(result.ActualTo) {
		t.Fatalf("unexpected actual range: %v to %v", result.ActualFrom, result.ActualTo)
	}

	aiRow := findHistoryRow(t, result.Rows, revisionA, "AI", "ai-node")
	if !reflect.DeepEqual(aiRow.GroupPath, []string{"AI", "AI-Auto"}) {
		t.Fatalf("unexpected AI group path: %#v", aiRow.GroupPath)
	}
	if aiRow.ActualOutboundType != "vmess" || aiRow.Network != "tcp" {
		t.Fatalf("unexpected AI dimensions: %#v", aiRow)
	}
	if aiRow.UplinkBytes != 150 || aiRow.DownlinkBytes != 50 || aiRow.Connections != 1 {
		t.Fatalf("unexpected AI counters: %#v", aiRow)
	}

	proxyRow := findHistoryRow(t, result.Rows, revisionA, "Proxy", "jp-node")
	if proxyRow.UplinkBytes != 20 || proxyRow.DownlinkBytes != 70 || proxyRow.Connections != 1 {
		t.Fatalf("unexpected Proxy counters: %#v", proxyRow)
	}

	filtered, err := reopened.Query(context.Background(), HistoryQuery{
		RouteTags:          []string{"AI"},
		ActualOutboundTags: []string{"ai-node"},
		Networks:           []string{"tcp"},
	})
	if err != nil {
		t.Fatal("query filtered history:", err)
	}
	if len(filtered.Rows) != 1 || filtered.Rows[0].RouteTag != "AI" {
		t.Fatalf("unexpected filtered rows: %#v", filtered.Rows)
	}

	empty, err := reopened.Query(context.Background(), HistoryQuery{
		RouteTags: []string{"AI"},
		Networks:  []string{"udp"},
	})
	if err != nil {
		t.Fatal("query non-matching history:", err)
	}
	if len(empty.Rows) != 0 {
		t.Fatalf("expected no rows for intersected filters, got %#v", empty.Rows)
	}

	reopened.RecordDelta(aiMetadata, 7, 9, true)
	withNewRevision, err := reopened.Query(context.Background(), HistoryQuery{
		RouteTags: []string{"AI"},
	})
	if err != nil {
		t.Fatal("query pending revision:", err)
	}
	if len(withNewRevision.Rows) != 2 {
		t.Fatalf("expected revisions to remain separate, got %#v", withNewRevision.Rows)
	}
	findHistoryRow(t, withNewRevision.Rows, revisionA, "AI", "ai-node")
	revisionBRow := findHistoryRow(t, withNewRevision.Rows, revisionB, "AI", "ai-node")
	if revisionBRow.UplinkBytes != 7 || revisionBRow.DownlinkBytes != 9 || revisionBRow.Connections != 1 {
		t.Fatalf("unexpected revision-b counters: %#v", revisionBRow)
	}

	limited, err := reopened.Query(context.Background(), HistoryQuery{Limit: 1})
	if err != nil {
		t.Fatal("query limited history:", err)
	}
	if len(limited.Rows) != 1 || limited.Rows[0].ConfigRevision != revisionA || limited.Rows[0].RouteTag != "AI" {
		t.Fatalf("unexpected top row: %#v", limited.Rows)
	}
	if !limited.Truncated {
		t.Fatal("limited query did not report truncation")
	}
	if limited.Totals != (HistoryCounters{UplinkBytes: 177, DownlinkBytes: 129, Connections: 3}) {
		t.Fatalf("limited query totals were truncated: %#v", limited.Totals)
	}
}

func TestHistoryRecordsUntaggedDirectLeafWithEmptyGroupPath(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-direct")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close direct history:", err)
		}
	}()
	history.RecordDelta(&TrackerMetadata{
		Metadata: adapter.InboundContext{Network: "tcp"},
		Trace:    NewRouteTrace("", "direct", false),
	}, 1, 2, true)
	result, err := history.Query(context.Background(), HistoryQuery{})
	if err != nil {
		t.Fatal("query direct history:", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected one direct row, got %#v", result.Rows)
	}
	row := result.Rows[0]
	if row.ActualOutboundTag != "" || row.ActualOutboundType != "direct" {
		t.Fatalf("unexpected direct leaf: %#v", row)
	}
	if row.GroupPath == nil || len(row.GroupPath) != 0 {
		t.Fatalf("direct group path must be an empty array: %#v", row.GroupPath)
	}
}

func TestHistoryHTTPUsesDecimalStrings(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-http")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close HTTP history:", err)
		}
	}()

	const (
		uplink   int64 = 9_007_199_254_740_993
		downlink int64 = 9_007_199_254_740_995
	)
	metadata := resolvedMetadata(
		"tcp",
		"AI",
		[]groupSelection{
			{parent: "AI", selected: "ai-node", selectedType: "vmess"},
		},
	)
	history.RecordDelta(metadata, uplink, downlink, true)

	request := httptest.NewRequest(
		http.MethodPost,
		"/query",
		strings.NewReader(`{"route_tags":["AI"],"actual_outbound_tags":["ai-node"],"networks":["tcp"]}`),
	)
	response := httptest.NewRecorder()
	NewHistoryHTTPHandler(history).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("unexpected response status %d: %s", response.Code, response.Body.String())
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("unexpected content type: %q", contentType)
	}

	var body struct {
		Totals map[string]json.RawMessage   `json:"totals"`
		Rows   []map[string]json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal("decode query response:", err)
	}
	if len(body.Rows) != 1 {
		t.Fatalf("expected one response row, got %#v", body.Rows)
	}
	assertJSONString(t, body.Rows[0], "uplink_bytes", strconv.FormatInt(uplink, 10))
	assertJSONString(t, body.Rows[0], "downlink_bytes", strconv.FormatInt(downlink, 10))
	assertJSONString(t, body.Rows[0], "connections", "1")
	assertJSONString(t, body.Totals, "uplink_bytes", strconv.FormatInt(uplink, 10))
	assertJSONString(t, body.Totals, "downlink_bytes", strconv.FormatInt(downlink, 10))
	assertJSONString(t, body.Totals, "connections", "1")
}

func TestHistoryHTTPAlwaysReturnsFullTotals(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-http-totals")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close HTTP totals history:", err)
		}
	}()
	history.RecordDelta(
		resolvedMetadata("tcp", "AI", []groupSelection{
			{parent: "AI", selected: "ai-node", selectedType: "vmess"},
		}),
		100,
		200,
		true,
	)
	history.RecordDelta(
		resolvedMetadata("udp", "Proxy", []groupSelection{
			{parent: "Proxy", selected: "proxy-node", selectedType: "trojan"},
		}),
		10,
		20,
		true,
	)

	request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{"limit":1}`))
	response := httptest.NewRecorder()
	NewHistoryHTTPHandler(history).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("unexpected response status %d: %s", response.Code, response.Body.String())
	}
	var body struct {
		Totals    map[string]json.RawMessage   `json:"totals"`
		Rows      []map[string]json.RawMessage `json:"rows"`
		Truncated bool                         `json:"truncated"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal("decode limited query response:", err)
	}
	if len(body.Rows) != 1 || !body.Truncated {
		t.Fatalf("unexpected limited response: rows=%d truncated=%v", len(body.Rows), body.Truncated)
	}
	assertJSONString(t, body.Totals, "uplink_bytes", "110")
	assertJSONString(t, body.Totals, "downlink_bytes", "220")
	assertJSONString(t, body.Totals, "connections", "2")
}

func TestHistoryHTTPEmptyResultTotalsAreZero(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-http-empty")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close empty HTTP history:", err)
		}
	}()
	request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	NewHistoryHTTPHandler(history).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("unexpected response status %d: %s", response.Code, response.Body.String())
	}
	var body struct {
		ActualFrom string                     `json:"actual_from"`
		ActualTo   string                     `json:"actual_to"`
		Totals     map[string]json.RawMessage `json:"totals"`
		Rows       []json.RawMessage          `json:"rows"`
		Truncated  bool                       `json:"truncated"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal("decode empty query response:", err)
	}
	if body.ActualFrom != "" || body.ActualTo != "" || len(body.Rows) != 0 || body.Truncated {
		t.Fatalf("unexpected empty response: %#v", body)
	}
	assertJSONString(t, body.Totals, "uplink_bytes", "0")
	assertJSONString(t, body.Totals, "downlink_bytes", "0")
	assertJSONString(t, body.Totals, "connections", "0")
}

func TestHistoryHTTPRejectsInvalidQuery(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-http-invalid")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close invalid-query history:", err)
		}
	}()
	for name, body := range map[string]string{
		"negative limit": `{"limit":-1}`,
		"large limit":    `{"limit":5001}`,
		"network":        `{"networks":["icmp"]}`,
		"unknown field":  `{"outbound_tags":["node"]}`,
		"trailing value": `{} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(body))
			response := httptest.NewRecorder()
			NewHistoryHTTPHandler(history).ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("unexpected response status %d: %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestHistoryConfigRevisionIsStableOpaqueAndDatabaseLocal(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "traffic.db")
	first := openTestHistory(t, databasePath, "secret-config-a")
	revisionA := first.configRevision
	if len(first.configContent) != 0 {
		t.Fatal("history retained canonical configuration content after deriving its revision")
	}
	if _, err := hex.DecodeString(revisionA); err != nil || len(revisionA) != historyRevisionSize*2 {
		t.Fatalf("configuration revision is not an opaque 128-bit hex identifier: %q", revisionA)
	}
	if err := first.Close(); err != nil {
		t.Fatal("close first history:", err)
	}

	same := openTestHistory(t, databasePath, "secret-config-a")
	if same.configRevision != revisionA {
		t.Fatalf("same database and configuration changed revision: %q != %q", same.configRevision, revisionA)
	}
	if err := same.Close(); err != nil {
		t.Fatal("close same-config history:", err)
	}

	changed := openTestHistory(t, databasePath, "secret-config-b")
	if changed.configRevision == revisionA {
		t.Fatal("changed configuration retained the same revision")
	}
	if err := changed.Close(); err != nil {
		t.Fatal("close changed-config history:", err)
	}

	otherDatabase := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "secret-config-a")
	if otherDatabase.configRevision == revisionA {
		t.Fatal("different databases reused the same keyed configuration revision")
	}
	if err := otherDatabase.Close(); err != nil {
		t.Fatal("close other database:", err)
	}
}

func TestHistoryQueryHonorsCancellation(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-cancel")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close canceled-query history:", err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := history.Query(ctx, HistoryQuery{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected canceled query error: %v", err)
	}
}

func TestHistoryConcurrentFlushQueryHasExactSnapshot(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-snapshot")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close snapshot history:", err)
		}
	}()
	metadata := resolvedMetadata(
		"tcp",
		"AI",
		[]groupSelection{
			{parent: "AI", selected: "ai-node", selectedType: "vmess"},
		},
	)
	for iteration := 1; iteration <= 25; iteration++ {
		history.RecordDelta(metadata, 1, 2, iteration == 1)
		start := make(chan struct{})
		queryResult := make(chan HistoryQueryResult, 1)
		queryError := make(chan error, 1)
		flushError := make(chan error, 1)
		go func() {
			<-start
			result, err := history.Query(context.Background(), HistoryQuery{})
			queryResult <- result
			queryError <- err
		}()
		go func() {
			<-start
			flushError <- history.flush()
		}()
		close(start)
		result := <-queryResult
		if err := <-queryError; err != nil {
			t.Fatal("query concurrent snapshot:", err)
		}
		if err := <-flushError; err != nil {
			t.Fatal("flush concurrent snapshot:", err)
		}
		expected := HistoryCounters{
			UplinkBytes:   uint64(iteration),
			DownlinkBytes: uint64(iteration * 2),
			Connections:   1,
		}
		if result.Totals != expected {
			t.Fatalf("iteration %d produced duplicate or missing counters: got %#v, want %#v", iteration, result.Totals, expected)
		}
	}
}

func TestSaturatingAdd(t *testing.T) {
	if result := saturatingAdd(^uint64(0)-1, 2); result != ^uint64(0) {
		t.Fatalf("overflow wrapped to %d", result)
	}
	if result := saturatingAdd(40, 2); result != 42 {
		t.Fatalf("ordinary addition returned %d", result)
	}
}

func TestHistoryCloseRejectsLateDeltaAndClearsPending(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "traffic.db")
	history := openTestHistory(t, databasePath, "revision-close")
	revision := history.configRevision
	metadata := resolvedMetadata(
		"tcp",
		"Proxy",
		[]groupSelection{
			{parent: "Proxy", selected: "proxy-node", selectedType: "vmess"},
		},
	)
	history.RecordDelta(metadata, 10, 20, true)
	if err := history.Close(); err != nil {
		t.Fatal("close history:", err)
	}

	history.RecordDelta(metadata, 100, 200, true)
	history.access.Lock()
	accepting := history.accepting
	pendingCount := len(history.pending)
	history.access.Unlock()
	if accepting {
		t.Fatal("closed history still accepts deltas")
	}
	if pendingCount != 0 {
		t.Fatalf("closed history retained %d pending deltas", pendingCount)
	}

	reopened := openTestHistory(t, databasePath, "revision-reopen")
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error("close reopened history:", err)
		}
	}()
	result, err := reopened.Query(context.Background(), HistoryQuery{})
	if err != nil {
		t.Fatal("query reopened history:", err)
	}
	row := findHistoryRow(t, result.Rows, revision, "Proxy", "proxy-node")
	if row.UplinkBytes != 10 || row.DownlinkBytes != 20 || row.Connections != 1 {
		t.Fatalf("late delta reached the closed history: %#v", row)
	}
}

func TestHistoryConcurrentCloseQueryAndRecord(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-race")
	if err := history.Start(adapter.StartStateStart); err != nil {
		t.Fatal("start history loop:", err)
	}
	metadata := resolvedMetadata(
		"udp",
		"Proxy",
		[]groupSelection{
			{parent: "Proxy", selected: "proxy-node", selectedType: "vmess"},
		},
	)
	history.RecordDelta(metadata, 1, 1, true)

	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 500 {
				history.RecordDelta(metadata, 1, 1, false)
			}
		}()
	}
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 100 {
				_, _ = history.Query(context.Background(), HistoryQuery{})
			}
		}()
	}
	closeResults := make(chan error, 4)
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			closeResults <- history.Close()
		}()
	}
	close(start)
	workers.Wait()
	close(closeResults)
	for err := range closeResults {
		if err != nil {
			t.Fatal("concurrent close history:", err)
		}
	}

	history.RecordDelta(metadata, 1, 1, false)
	history.access.Lock()
	accepting := history.accepting
	pendingCount := len(history.pending)
	history.access.Unlock()
	if accepting || pendingCount != 0 {
		t.Fatalf("unexpected closed state: accepting=%v pending=%d", accepting, pendingCount)
	}
	history.flushAccess.Lock()
	databaseOpen := history.db != nil
	history.flushAccess.Unlock()
	if databaseOpen {
		t.Fatal("history database remained open after concurrent Close")
	}
}

func openTestHistory(t *testing.T, path string, revision string) *History {
	t.Helper()
	history := NewHistory(
		context.Background(),
		log.NewNOPFactory().NewLogger("traffic-test"),
		HistoryOptions{
			Path:          path,
			ConfigContent: []byte(revision),
		},
	)
	if err := history.Start(adapter.StartStateInitialize); err != nil {
		t.Fatal("open history:", err)
	}
	return history
}

type groupSelection struct {
	parent          string
	selected        string
	selectedType    string
	selectedIsGroup bool
}

func resolvedMetadata(network string, routeTag string, selections []groupSelection) *TrackerMetadata {
	trace := NewRouteTrace(routeTag, "selector", true)
	for _, selection := range selections {
		trace.RecordSelection(selection.parent, selection.selected, selection.selectedType, selection.selectedIsGroup)
	}
	return &TrackerMetadata{
		Metadata: adapter.InboundContext{
			Network: network,
		},
		Trace: trace,
	}
}

func findHistoryRow(t *testing.T, rows []HistoryRow, revision string, routeTag string, outboundTag string) HistoryRow {
	t.Helper()
	for _, row := range rows {
		if row.ConfigRevision == revision && row.RouteTag == routeTag && row.ActualOutboundTag == outboundTag {
			return row
		}
	}
	t.Fatalf("missing history row revision=%q route=%q outbound=%q in %#v", revision, routeTag, outboundTag, rows)
	return HistoryRow{}
}

func assertJSONString(t *testing.T, row map[string]json.RawMessage, key string, expected string) {
	t.Helper()
	content, loaded := row[key]
	if !loaded {
		t.Fatalf("missing JSON field %q", key)
	}
	var actual string
	if err := json.Unmarshal(content, &actual); err != nil {
		t.Fatalf("field %q is not a JSON string: %s", key, content)
	}
	if actual != expected {
		t.Fatalf("unexpected %s: got %q, want %q", key, actual, expected)
	}
}

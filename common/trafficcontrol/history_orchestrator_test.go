package trafficcontrol

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
)

func TestHistoryRecordDeltaRemainsMemoryOnly(t *testing.T) {
	store := &fakeHistoryStore{
		state: historyStoreState{
			configRevision:   "memory-only",
			targetsFrom:      time.Unix(0, 0).UTC(),
			destinationsFrom: time.Unix(0, 0).UTC(),
		},
	}
	history := newHistory(context.Background(), log.NewNOPFactory().NewLogger("traffic-test"), store)
	if err := history.Start(adapter.StartStateInitialize); err != nil {
		t.Fatal("initialize history:", err)
	}
	metadata := resolvedMetadata("tcp", "Proxy", []groupSelection{
		{parent: "Proxy", selected: "node-a", selectedType: "vmess"},
	})

	history.RecordDelta(metadata, 10, 20, true)
	history.RecordDelta(metadata, 5, 7, false)

	store.access.Lock()
	if store.writeCalls != 0 || store.beginReadCalls != 0 || store.cleanupCalls != 0 {
		t.Fatalf(
			"RecordDelta touched the store: writes=%d reads=%d cleanups=%d",
			store.writeCalls,
			store.beginReadCalls,
			store.cleanupCalls,
		)
	}
	store.access.Unlock()

	history.access.Lock()
	if len(history.pending) != 1 {
		history.access.Unlock()
		t.Fatalf("unexpected pending batch: %#v", history.pending)
	}
	var pending historyCounters
	for _, counters := range history.pending {
		pending = counters
	}
	history.access.Unlock()
	if pending != (historyCounters{UplinkBytes: 15, DownlinkBytes: 27, Connections: 1}) {
		t.Fatalf("unexpected memory-only counters: %#v", pending)
	}

	if err := history.flush(); err != nil {
		t.Fatal("flush memory-only history:", err)
	}
	store.access.Lock()
	if store.writeCalls != 1 || len(store.writes) != 1 || len(store.writes[0]) != 1 {
		store.access.Unlock()
		t.Fatalf("unexpected writes after flush: %#v", store.writes)
	}
	sealed := store.writes[0]
	var sealedCounters historyCounters
	for _, counters := range sealed {
		sealedCounters = counters
	}
	store.access.Unlock()
	if sealedCounters != pending {
		t.Fatalf("unexpected sealed counters: got %#v, want %#v", sealedCounters, pending)
	}

	history.RecordDelta(metadata, 100, 200, true)
	store.access.Lock()
	var retained historyCounters
	for _, counters := range store.writes[0] {
		retained = counters
	}
	store.access.Unlock()
	if retained != pending {
		t.Fatalf("new delta mutated the detached sealed batch: got %#v, want %#v", retained, pending)
	}
}

func TestHistoryRestoresPendingAfterStoreWriteFailure(t *testing.T) {
	sentinel := errors.New("sentinel write failure")
	maxCounter := ^uint64(0)
	store := &fakeHistoryStore{
		state: historyStoreState{
			configRevision:   "restore-after-failure",
			targetsFrom:      time.Unix(0, 0).UTC(),
			destinationsFrom: time.Unix(0, 0).UTC(),
		},
		writeErrors: []error{sentinel, nil},
	}
	history := newHistory(context.Background(), log.NewNOPFactory().NewLogger("traffic-test"), store)
	if err := history.Start(adapter.StartStateInitialize); err != nil {
		t.Fatal("initialize history:", err)
	}
	key := historyKey{
		Bucket:             time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC).Unix(),
		ConfigRevision:     history.configRevision,
		RouteTag:           "Proxy",
		GroupPath:          `["Proxy"]`,
		ActualOutboundTag:  "node-a",
		ActualOutboundType: "vmess",
		Network:            "tcp",
	}
	initial := historyCounters{
		UplinkBytes:   maxCounter - 3,
		DownlinkBytes: maxCounter - 5,
		Connections:   maxCounter - 1,
	}
	concurrent := historyCounters{
		UplinkBytes:   10,
		DownlinkBytes: 20,
		Connections:   5,
	}
	expected := historyCounters{
		UplinkBytes:   maxCounter,
		DownlinkBytes: maxCounter,
		Connections:   maxCounter,
	}
	history.access.Lock()
	history.pending[key] = initial
	history.access.Unlock()
	store.onWrite = func() {
		store.access.Lock()
		store.onWrite = nil
		store.access.Unlock()
		history.access.Lock()
		history.pending[key] = concurrent
		history.access.Unlock()
	}

	if err := history.flush(); !errors.Is(err, sentinel) {
		t.Fatalf("unexpected first flush error: %v", err)
	}
	history.access.Lock()
	if len(history.pending) != 1 {
		history.access.Unlock()
		t.Fatalf("failed write did not restore pending: %#v", history.pending)
	}
	restored := history.pending[key]
	history.access.Unlock()
	if restored != expected {
		t.Fatalf("unexpected restored counters: got %#v, want %#v", restored, expected)
	}

	if err := history.flush(); err != nil {
		t.Fatal("second flush:", err)
	}
	store.access.Lock()
	defer store.access.Unlock()
	if store.writeCalls != 2 || len(store.writes) != 2 {
		t.Fatalf("unexpected write attempts: calls=%d batches=%d", store.writeCalls, len(store.writes))
	}
	first := store.writes[0][key]
	second := store.writes[1][key]
	if first != initial {
		t.Fatalf("first immutable batch changed: %#v", first)
	}
	if second != expected {
		t.Fatalf("restored batch was lost or double-counted: got %#v, want %#v", second, expected)
	}
}

func TestHistoryHTTPHandlerUsesReaderInterface(t *testing.T) {
	targetAvailableFrom := time.Date(2026, time.July, 1, 0, 0, 0, 0, time.UTC)
	destinationAvailableFrom := targetAvailableFrom.Add(time.Minute)
	contextKey := struct{}{}
	reader := &fakeHistoryReader{
		targetAvailableFrom:      targetAvailableFrom,
		destinationAvailableFrom: destinationAvailableFrom,
		result: HistoryQueryResult{
			TargetAvailableFrom:      targetAvailableFrom,
			DestinationAvailableFrom: destinationAvailableFrom,
			GroupBy:                  HistoryGroupByDestination,
			Page:                     1,
			PageSize:                 HistoryPageSizeDefault,
			TotalRows:                1,
			Totals: HistoryCounters{
				UplinkBytes:   ^uint64(0),
				DownlinkBytes: 42,
				Connections:   7,
			},
			Rows: []HistoryRow{{
				GroupPath:       []string{},
				Destination:     "example.com",
				DestinationType: HistoryDestinationTypeDomain,
				UplinkBytes:     ^uint64(0),
				DownlinkBytes:   42,
				Connections:     7,
			}},
		},
		queryCheck: func(ctx context.Context, query HistoryQuery) {
			if ctx.Value(contextKey) != "request" {
				t.Fatal("reader did not receive request context")
			}
			if query.GroupBy != HistoryGroupByDestination ||
				query.Page != 1 ||
				query.PageSize != HistoryPageSizeDefault ||
				query.SortBy != HistorySortByTotalBytes ||
				query.SortOrder != HistorySortOrderDescending ||
				len(query.Destinations) != 1 ||
				query.Destinations[0] != "example.com" {
				t.Fatalf("reader received an unnormalized query: %#v", query)
			}
		},
	}
	handler := NewHistoryHTTPHandler(reader)

	capabilities := httptest.NewRecorder()
	handler.ServeHTTP(capabilities, httptest.NewRequest(http.MethodGet, "/capabilities", nil))
	if capabilities.Code != http.StatusOK {
		t.Fatalf("unexpected capabilities status %d: %s", capabilities.Code, capabilities.Body.String())
	}
	if reader.targetCalls != 1 || reader.destinationCalls != 1 {
		t.Fatalf(
			"capabilities did not use reader availability methods: target=%d destination=%d",
			reader.targetCalls,
			reader.destinationCalls,
		)
	}

	request := httptest.NewRequest(
		http.MethodPost,
		"/query",
		strings.NewReader(`{"group_by":"destination","destinations":["Example.COM."]}`),
	).WithContext(context.WithValue(context.Background(), contextKey, "request"))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("unexpected query status %d: %s", response.Code, response.Body.String())
	}
	if reader.queryCalls != 1 {
		t.Fatalf("unexpected reader query count: %d", reader.queryCalls)
	}
	body := response.Body.String()
	for _, expected := range []string{
		`"uplink_bytes":"18446744073709551615"`,
		`"downlink_bytes":"42"`,
		`"connections":"7"`,
	} {
		if !strings.Contains(body, expected) {
			t.Fatalf("response lost decimal-string counter %s: %s", expected, body)
		}
	}
}

type fakeHistoryStore struct {
	access sync.Mutex

	state          historyStoreState
	openErr        error
	closeErr       error
	writeErrors    []error
	beginReadErr   error
	snapshot       historyStoreSnapshot
	onWrite        func()
	openCalls      int
	closeCalls     int
	writeCalls     int
	beginReadCalls int
	cleanupCalls   int
	writes         []historyBatch
}

func (s *fakeHistoryStore) Open() (historyStoreState, error) {
	s.access.Lock()
	defer s.access.Unlock()
	s.openCalls++
	return s.state, s.openErr
}

func (s *fakeHistoryStore) Close() error {
	s.access.Lock()
	defer s.access.Unlock()
	s.closeCalls++
	return s.closeErr
}

func (s *fakeHistoryStore) Write(batch historyBatch) error {
	s.access.Lock()
	s.writeCalls++
	s.writes = append(s.writes, batch)
	var err error
	if len(s.writeErrors) > 0 {
		err = s.writeErrors[0]
		s.writeErrors = s.writeErrors[1:]
	}
	onWrite := s.onWrite
	s.access.Unlock()
	if onWrite != nil {
		onWrite()
	}
	return err
}

func (s *fakeHistoryStore) Cleanup(time.Time) error {
	s.access.Lock()
	s.cleanupCalls++
	s.access.Unlock()
	return nil
}

func (s *fakeHistoryStore) BeginRead(context.Context) (historyStoreSnapshot, error) {
	s.access.Lock()
	defer s.access.Unlock()
	s.beginReadCalls++
	if s.beginReadErr != nil {
		return nil, s.beginReadErr
	}
	if s.snapshot == nil {
		return fakeHistorySnapshot{}, nil
	}
	return s.snapshot, nil
}

type fakeHistorySnapshot struct{}

func (fakeHistorySnapshot) Query(
	context.Context,
	HistoryQuery,
	historyQueryOverlay,
) (HistoryQueryResult, error) {
	return HistoryQueryResult{}, nil
}

func (fakeHistorySnapshot) Close() {}

type fakeHistoryReader struct {
	targetAvailableFrom      time.Time
	destinationAvailableFrom time.Time
	result                   HistoryQueryResult
	queryErr                 error
	queryCheck               func(context.Context, HistoryQuery)
	targetCalls              int
	destinationCalls         int
	queryCalls               int
}

func (r *fakeHistoryReader) TargetAvailableFrom() time.Time {
	r.targetCalls++
	return r.targetAvailableFrom
}

func (r *fakeHistoryReader) DestinationAvailableFrom() time.Time {
	r.destinationCalls++
	return r.destinationAvailableFrom
}

func (r *fakeHistoryReader) Query(ctx context.Context, query HistoryQuery) (HistoryQueryResult, error) {
	r.queryCalls++
	if r.queryCheck != nil {
		r.queryCheck(ctx, query)
	}
	return r.result, r.queryErr
}

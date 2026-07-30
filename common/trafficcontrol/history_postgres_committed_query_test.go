package trafficcontrol

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"

	"github.com/gofrs/uuid/v5"
)

func TestPostgresCommittedQueryWaitsForDelivery(t *testing.T) {
	applyStarted := make(chan struct{}, 1)
	applyRelease := make(chan struct{})
	remote := newCommittedQueryTestRemote()
	remote.apply = func(ctx context.Context, _ historyBatch) error {
		select {
		case applyStarted <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-applyRelease:
			return nil
		}
	}
	history := newCommittedQueryTestHistory(t, remote, 1<<20)
	addCommittedQueryPending(history, 1)

	resultCh := make(chan HistoryQueryResult, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := history.Query(context.Background(), HistoryQuery{})
		resultCh <- result
		errCh <- err
	}()

	select {
	case err := <-errCh:
		<-resultCh
		t.Fatalf("query returned before delivery completed: %v", err)
	case <-applyStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("query did not start boundary delivery")
	}
	select {
	case err := <-errCh:
		<-resultCh
		t.Fatalf("query returned while boundary delivery was blocked: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(applyRelease)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	result := <-resultCh
	if result.Totals.Connections != 1 {
		t.Fatalf("committed query result = %#v", result)
	}
}

func TestPostgresCommittedQueryRejectsLossDuringWrite(t *testing.T) {
	remote := newCommittedQueryTestRemote()
	history := newCommittedQueryTestHistory(t, remote, 1)
	addCommittedQueryPending(history, 1)

	_, err := history.Query(context.Background(), HistoryQuery{})
	if !errors.Is(err, ErrHistoryUnavailable) {
		t.Fatalf("lossy boundary query error = %v, want %v", err, ErrHistoryUnavailable)
	}
	if remote.applyCallCount() != 0 {
		t.Fatal("lossy boundary was delivered")
	}
}

func TestPostgresCommittedQueryReturnsUnavailableOnTimeout(t *testing.T) {
	applyStarted := make(chan struct{}, 1)
	remote := newCommittedQueryTestRemote()
	remote.apply = func(ctx context.Context, _ historyBatch) error {
		select {
		case applyStarted <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return ctx.Err()
	}
	history := newCommittedQueryTestHistory(t, remote, 1<<20)
	addCommittedQueryPending(history, 1)

	ctx := newCommittedQueryDeadlineContext()
	errCh := make(chan error, 1)
	go func() {
		_, err := history.Query(ctx, HistoryQuery{})
		errCh <- err
	}()
	select {
	case <-applyStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed-out query never attempted boundary delivery")
	}
	ctx.expire()
	err := <-errCh
	if !errors.Is(err, ErrHistoryUnavailable) {
		t.Fatalf("timed-out boundary query error = %v, want %v", err, ErrHistoryUnavailable)
	}
}

func TestPostgresCommittedQueryNoPendingOverlay(t *testing.T) {
	remote := newCommittedQueryTestRemote()
	history := newCommittedQueryTestHistory(t, remote, 1<<20)
	addCommittedQueryPending(history, 1)

	result, err := history.Query(context.Background(), HistoryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Totals.Connections != 1 {
		t.Fatalf("committed result = %#v", result)
	}
	if overlays := remote.overlaySizesCopy(); len(overlays) != 1 || overlays[0] != 0 {
		t.Fatalf("committed query overlays = %v, want [0]", overlays)
	}
	if remote.applyCallCount() != 1 {
		t.Fatalf("boundary apply calls = %d, want 1", remote.applyCallCount())
	}
}

func TestPostgresCommittedQuerySnapshotConsistency(t *testing.T) {
	snapshotStarted := make(chan struct{}, 1)
	snapshotRelease := make(chan struct{})
	remote := newCommittedQueryTestRemote()
	remote.firstSnapshotStarted = snapshotStarted
	remote.firstSnapshotRelease = snapshotRelease
	history := newCommittedQueryTestHistory(t, remote, 1<<20)
	addCommittedQueryPending(history, 1)

	resultCh := make(chan HistoryQueryResult, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := history.Query(context.Background(), HistoryQuery{})
		resultCh <- result
		errCh <- err
	}()
	select {
	case <-snapshotStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first committed snapshot did not start")
	}
	firstWasCommitted := remote.applyCallCount() == 1
	addCommittedQueryPending(history, 2)
	if err := history.flush(); err != nil {
		close(snapshotRelease)
		t.Fatal(err)
	}
	close(snapshotRelease)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	result := <-resultCh
	if !firstWasCommitted {
		t.Fatal("snapshot was acquired before its boundary committed")
	}
	if result.Totals.Connections != 1 {
		t.Fatalf("snapshot observed a later commit: %#v", result)
	}
	waitForPostgresSpoolCondition(t, func() bool {
		return remote.applyCallCount() == 2
	})
	if remote.applyCallCount() != 2 {
		t.Fatalf("snapshot test apply calls = %d, want 2", remote.applyCallCount())
	}
}

func TestPostgresCommittedQueryConcurrentIsolation(t *testing.T) {
	firstSnapshotStarted := make(chan struct{}, 1)
	firstSnapshotRelease := make(chan struct{})
	remote := newCommittedQueryTestRemote()
	remote.firstSnapshotStarted = firstSnapshotStarted
	remote.firstSnapshotRelease = firstSnapshotRelease
	history := newCommittedQueryTestHistory(t, remote, 1<<20)
	addCommittedQueryPending(history, 1)

	firstResult := make(chan HistoryQueryResult, 1)
	firstError := make(chan error, 1)
	go func() {
		result, err := history.Query(context.Background(), HistoryQuery{})
		firstResult <- result
		firstError <- err
	}()
	select {
	case <-firstSnapshotStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first concurrent snapshot did not start")
	}

	addCommittedQueryPending(history, 2)
	secondResult := make(chan HistoryQueryResult, 1)
	secondError := make(chan error, 1)
	go func() {
		result, err := history.Query(context.Background(), HistoryQuery{})
		secondResult <- result
		secondError <- err
	}()
	var second HistoryQueryResult
	select {
	case err := <-secondError:
		if err != nil {
			close(firstSnapshotRelease)
			t.Fatal(err)
		}
		second = <-secondResult
	case <-time.After(2 * time.Second):
		close(firstSnapshotRelease)
		t.Fatal("second committed query was blocked by first snapshot")
	}
	close(firstSnapshotRelease)
	if err := <-firstError; err != nil {
		t.Fatal(err)
	}
	first := <-firstResult

	if first.Totals.Connections != 1 || second.Totals.Connections != 3 {
		t.Fatalf("isolated results = %d then %d, want 1 then 3",
			first.Totals.Connections,
			second.Totals.Connections,
		)
	}
	if remote.applyCallCount() != 2 {
		t.Fatalf("concurrent boundary apply calls = %d, want 2", remote.applyCallCount())
	}
	if overlays := remote.overlaySizesCopy(); len(overlays) != 2 ||
		overlays[0] != 0 || overlays[1] != 0 {
		t.Fatalf("concurrent committed overlays = %v, want [0 0]", overlays)
	}
}

func TestPostgresCommittedQueryCloseUnblocksWaiter(t *testing.T) {
	applyStarted := make(chan struct{}, 1)
	applyRelease := make(chan struct{})
	remote := newCommittedQueryTestRemote()
	remote.apply = func(ctx context.Context, _ historyBatch) error {
		select {
		case applyStarted <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-applyRelease:
			return nil
		}
	}
	history := newCommittedQueryTestHistory(t, remote, 1<<20)
	addCommittedQueryPending(history, 1)

	queryErr := make(chan error, 1)
	go func() {
		_, err := history.Query(context.Background(), HistoryQuery{})
		queryErr <- err
	}()
	select {
	case <-applyStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("close test boundary delivery did not start")
	}
	closeErr := make(chan error, 1)
	go func() {
		closeErr <- history.Close()
	}()

	select {
	case err := <-queryErr:
		if !errors.Is(err, ErrHistoryUnavailable) {
			close(applyRelease)
			<-closeErr
			t.Fatalf("query error during Close = %v, want %v", err, ErrHistoryUnavailable)
		}
	case <-time.After(250 * time.Millisecond):
		close(applyRelease)
		<-queryErr
		<-closeErr
		t.Fatal("Close did not cancel a committed-boundary waiter")
	}
	select {
	case err := <-closeErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close remained blocked after committed query exited")
	}
}

func TestPostgresCommittedQueryDoesNotLoseAcknowledgeWakeup(t *testing.T) {
	remote := newCommittedQueryTestRemote()
	spool, initial, err := openHistorySpool(historySpoolTestOptions(t, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	store := newPostgresSpoolStore(
		spool,
		initial,
		remote,
		postgresSpoolStoreOptions{},
	)
	store.access.Lock()
	store.opened = true
	store.workerCtx, store.cancel = context.WithCancel(spool.ctx)
	store.access.Unlock()
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})

	boundary, err := store.Commit(historySpoolTestBatch(1))
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if envelope == nil || envelope.Sequence != boundary.Sequence {
		t.Fatalf("leased envelope = %#v, want sequence %d", envelope, boundary.Sequence)
	}

	store.statusAccess.Lock()
	statusLocked := true
	defer func() {
		if statusLocked {
			store.statusAccess.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	snapshotCh := make(chan historyStoreSnapshot, 1)
	errCh := make(chan error, 1)
	go func() {
		snapshot, readErr := store.BeginReadCommitted(ctx, boundary)
		snapshotCh <- snapshot
		errCh <- readErr
	}()
	waitForCommittedQueryStack(
		t,
		"(*postgresSpoolStore).BeginReadCommitted",
		"sync.(*RWMutex).RLock",
	)

	if err = spool.Acknowledge(boundary.Sequence, historySpoolTestNow()); err != nil {
		t.Fatal(err)
	}
	store.state = postgresSpoolStateReady
	store.signalCommitProgressLocked()
	store.statusAccess.Unlock()
	statusLocked = false

	if err = <-errCh; err != nil {
		t.Fatalf("query missed completed acknowledgment: %v", err)
	}
	snapshot := <-snapshotCh
	if snapshot == nil {
		t.Fatal("committed query returned a nil snapshot")
	}
	snapshot.Close()
}

func TestPostgresCommittedQueryCloseBeforeStoreLockReturnsUnavailable(t *testing.T) {
	t.Run("query", func(t *testing.T) {
		history := newCommittedQueryTestHistory(
			t,
			newCommittedQueryTestRemote(),
			1<<20,
		)
		history.queryAccess = make(chan struct{})
		history.storeAccess.Lock()
		queryErr := make(chan error, 1)
		go func() {
			_, err := history.Query(context.Background(), HistoryQuery{})
			queryErr <- err
		}()
		select {
		case <-history.queryAccess:
		case <-time.After(2 * time.Second):
			history.storeAccess.Unlock()
			t.Fatal("query was not admitted before lifecycle cancellation")
		}
		history.cancelQuery()
		if err := history.store.Close(); err != nil {
			history.storeAccess.Unlock()
			t.Fatal(err)
		}
		history.storeAccess.Unlock()
		queryAccessRestored := make(chan struct{})
		go func() {
			history.queryAccess <- struct{}{}
			close(queryAccessRestored)
		}()

		if err := <-queryErr; !errors.Is(err, ErrHistoryUnavailable) {
			t.Fatalf("query error after Close won store lock = %v, want %v",
				err,
				ErrHistoryUnavailable,
			)
		}
		<-queryAccessRestored
	})

	t.Run("http", func(t *testing.T) {
		history := newCommittedQueryTestHistory(
			t,
			newCommittedQueryTestRemote(),
			1<<20,
		)
		history.queryAccess = make(chan struct{})
		history.storeAccess.Lock()
		responseCh := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			response := httptest.NewRecorder()
			request := httptest.NewRequest(
				http.MethodPost,
				"/query",
				bytes.NewBufferString(`{}`),
			)
			NewHistoryHTTPHandler(history).ServeHTTP(response, request)
			responseCh <- response
		}()
		select {
		case <-history.queryAccess:
		case <-time.After(2 * time.Second):
			history.storeAccess.Unlock()
			t.Fatal("HTTP query was not admitted before lifecycle cancellation")
		}
		history.cancelQuery()
		if err := history.store.Close(); err != nil {
			history.storeAccess.Unlock()
			t.Fatal(err)
		}
		history.storeAccess.Unlock()
		queryAccessRestored := make(chan struct{})
		go func() {
			history.queryAccess <- struct{}{}
			close(queryAccessRestored)
		}()

		response := <-responseCh
		<-queryAccessRestored
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("closed query status = %d, want 503; body=%s",
				response.Code,
				response.Body.String(),
			)
		}
		if response.Body.String() !=
			`{"error":"traffic statistics are temporarily unavailable"}`+"\n" {
			t.Fatalf("closed query body = %q", response.Body.String())
		}
	})
}

func TestHistoryHTTPCommittedQueryReturns503(t *testing.T) {
	reader := committedQueryErrorReader{err: ErrHistoryUnavailable}
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/query", bytes.NewBufferString(`{}`))
	NewHistoryHTTPHandler(reader).ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unavailable query status = %d, want 503; body=%s",
			response.Code,
			response.Body.String(),
		)
	}
	if response.Body.String() != `{"error":"traffic statistics are temporarily unavailable"}`+"\n" {
		t.Fatalf("unavailable query body = %q", response.Body.String())
	}
}

func TestHistoryHTTPCommittedQuerySuccess200(t *testing.T) {
	applyStarted := make(chan struct{}, 1)
	applyRelease := make(chan struct{})
	remote := newCommittedQueryTestRemote()
	remote.apply = func(ctx context.Context, _ historyBatch) error {
		select {
		case applyStarted <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-applyRelease:
			return nil
		}
	}
	history := newCommittedQueryTestHistory(t, remote, 1<<20)
	addCommittedQueryPending(history, 1)

	responseCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(
			http.MethodPost,
			"/query",
			bytes.NewBufferString(`{}`),
		)
		NewHistoryHTTPHandler(history).ServeHTTP(response, request)
		responseCh <- response
	}()
	select {
	case response := <-responseCh:
		t.Fatalf("HTTP query returned before commit: status=%d body=%s",
			response.Code,
			response.Body.String(),
		)
	case <-applyStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP query did not start boundary delivery")
	}
	close(applyRelease)
	select {
	case response := <-responseCh:
		if response.Code != http.StatusOK {
			t.Fatalf("committed HTTP query status = %d; body=%s",
				response.Code,
				response.Body.String(),
			)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("committed HTTP query did not finish")
	}
}

type committedQueryErrorReader struct {
	err error
}

type committedQueryDeadlineContext struct {
	done chan struct{}
	once sync.Once
}

func newCommittedQueryDeadlineContext() *committedQueryDeadlineContext {
	return &committedQueryDeadlineContext{
		done: make(chan struct{}),
	}
}

func (c *committedQueryDeadlineContext) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (c *committedQueryDeadlineContext) Done() <-chan struct{} {
	return c.done
}

func (c *committedQueryDeadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func (c *committedQueryDeadlineContext) Value(any) any {
	return nil
}

func (c *committedQueryDeadlineContext) expire() {
	c.once.Do(func() {
		close(c.done)
	})
}

func (r committedQueryErrorReader) TargetAvailableFrom() time.Time {
	return time.Time{}
}

func (r committedQueryErrorReader) DestinationAvailableFrom() time.Time {
	return time.Time{}
}

func (r committedQueryErrorReader) Query(
	context.Context,
	HistoryQuery,
) (HistoryQueryResult, error) {
	return HistoryQueryResult{}, r.err
}

type committedQueryTestRemote struct {
	access sync.Mutex

	state                historyStoreState
	apply                func(context.Context, historyBatch) error
	committed            historyBatch
	applyCalls           int
	beginReadCalls       int
	overlaySizes         []int
	firstSnapshotStarted chan<- struct{}
	firstSnapshotRelease <-chan struct{}
}

func newCommittedQueryTestRemote() *committedQueryTestRemote {
	return &committedQueryTestRemote{
		state:     historySpoolRemoteTestState(historySpoolTestRevision),
		committed: make(historyBatch),
	}
}

func (r *committedQueryTestRemote) Open() (historyStoreState, error) {
	return r.state, nil
}

func (r *committedQueryTestRemote) Apply(
	ctx context.Context,
	_ uuid.UUID,
	batch historyBatch,
) error {
	r.access.Lock()
	apply := r.apply
	r.access.Unlock()
	if apply != nil {
		if err := apply(ctx, batch); err != nil {
			return err
		}
	}
	r.access.Lock()
	defer r.access.Unlock()
	r.applyCalls++
	mergeCommittedQueryBatch(r.committed, batch)
	return nil
}

func (r *committedQueryTestRemote) Cleanup(time.Time) error {
	return nil
}

func (r *committedQueryTestRemote) BeginRead(
	context.Context,
) (historyStoreSnapshot, error) {
	r.access.Lock()
	r.beginReadCalls++
	index := r.beginReadCalls
	committed := cloneCommittedQueryBatch(r.committed)
	started := r.firstSnapshotStarted
	release := r.firstSnapshotRelease
	r.access.Unlock()
	return &committedQueryTestSnapshot{
		remote:    r,
		committed: committed,
		started:   started,
		release:   release,
		block:     index == 1 && release != nil,
	}, nil
}

func (r *committedQueryTestRemote) Cancel() {}

func (r *committedQueryTestRemote) Close() error {
	return nil
}

func (r *committedQueryTestRemote) applyCallCount() int {
	r.access.Lock()
	defer r.access.Unlock()
	return r.applyCalls
}

func (r *committedQueryTestRemote) overlaySizesCopy() []int {
	r.access.Lock()
	defer r.access.Unlock()
	return append([]int(nil), r.overlaySizes...)
}

type committedQueryTestSnapshot struct {
	remote    *committedQueryTestRemote
	committed historyBatch
	started   chan<- struct{}
	release   <-chan struct{}
	block     bool
}

func (s *committedQueryTestSnapshot) Query(
	ctx context.Context,
	query HistoryQuery,
	overlay historyQueryOverlay,
) (HistoryQueryResult, error) {
	s.remote.access.Lock()
	s.remote.overlaySizes = append(s.remote.overlaySizes, len(overlay.pending))
	s.remote.access.Unlock()
	if s.block {
		select {
		case s.started <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return HistoryQueryResult{}, ctx.Err()
		case <-s.release:
		}
	}
	combined := cloneCommittedQueryBatch(s.committed)
	mergeCommittedQueryBatch(combined, overlay.pending)
	var totals HistoryCounters
	for _, counters := range combined {
		totals.UplinkBytes = saturatingAdd(totals.UplinkBytes, counters.UplinkBytes)
		totals.DownlinkBytes = saturatingAdd(totals.DownlinkBytes, counters.DownlinkBytes)
		totals.Connections = saturatingAdd(totals.Connections, counters.Connections)
	}
	return HistoryQueryResult{
		GroupBy:  query.GroupBy,
		Page:     query.Page,
		PageSize: query.PageSize,
		Totals:   totals,
	}, nil
}

func (s *committedQueryTestSnapshot) Close() {}

func newCommittedQueryTestHistory(
	t *testing.T,
	remote postgresSpoolRemote,
	maxSize uint64,
) *History {
	t.Helper()
	spool, initial, err := openHistorySpool(historySpoolTestOptions(t, maxSize))
	if err != nil {
		t.Fatal(err)
	}
	store := newPostgresSpoolStore(
		spool,
		initial,
		remote,
		postgresSpoolStoreOptions{},
	)
	history := newHistory(
		context.Background(),
		log.NewNOPFactory().NewLogger("committed-query-test"),
		store,
	)
	if err = history.Start(adapter.StartStateInitialize); err != nil {
		_ = history.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := history.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	return history
}

func addCommittedQueryPending(history *History, connections uint64) {
	history.access.Lock()
	defer history.access.Unlock()
	key := historySpoolTestKey("committed-query.example")
	current := history.pending[key]
	current.Connections = saturatingAdd(current.Connections, connections)
	history.pending[key] = current
}

func cloneCommittedQueryBatch(batch historyBatch) historyBatch {
	cloned := make(historyBatch, len(batch))
	for key, counters := range batch {
		cloned[key] = counters
	}
	return cloned
}

func mergeCommittedQueryBatch(destination historyBatch, batch historyBatch) {
	for key, counters := range batch {
		current := destination[key]
		current.UplinkBytes = saturatingAdd(current.UplinkBytes, counters.UplinkBytes)
		current.DownlinkBytes = saturatingAdd(current.DownlinkBytes, counters.DownlinkBytes)
		current.Connections = saturatingAdd(current.Connections, counters.Connections)
		destination[key] = current
	}
}

func waitForCommittedQueryStack(t *testing.T, markers ...string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	buffer := make([]byte, 1<<20)
	for {
		size := runtime.Stack(buffer, true)
		for _, stack := range bytes.Split(buffer[:size], []byte("\n\n")) {
			matches := true
			for _, marker := range markers {
				if !bytes.Contains(stack, []byte(marker)) {
					matches = false
					break
				}
			}
			if matches {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for goroutine stack markers %q", markers)
		}
		runtime.Gosched()
	}
}

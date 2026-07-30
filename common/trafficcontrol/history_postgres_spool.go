package trafficcontrol

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/gofrs/uuid/v5"
)

const (
	postgresSpoolRetryBase = time.Second
	postgresSpoolRetryMax  = time.Minute
)

var _ historyStore = (*postgresSpoolStore)(nil)

type postgresSpoolRemote interface {
	Open() (historyStoreState, error)
	Apply(context.Context, uuid.UUID, historyBatch) error
	Cleanup(time.Time) error
	BeginRead(context.Context) (historyStoreSnapshot, error)
	// Cancel must cause in-flight methods to return promptly.
	Cancel()
	Close() error
}

type postgresStoreSpoolRemote struct {
	store      *postgresStore
	cancel     context.CancelFunc
	cancelOnce sync.Once
}

func newPostgresSpoolRemote(store *postgresStore) postgresSpoolRemote {
	store.access.Lock()
	storeContext, cancel := context.WithCancel(store.ctx)
	store.ctx = storeContext
	store.access.Unlock()
	return &postgresStoreSpoolRemote{
		store:  store,
		cancel: cancel,
	}
}

func (r *postgresStoreSpoolRemote) Open() (historyStoreState, error) {
	return r.store.Open()
}

func (r *postgresStoreSpoolRemote) Apply(
	ctx context.Context,
	batchID uuid.UUID,
	batch historyBatch,
) error {
	return r.store.applyBatch(ctx, batchID, batch)
}

func (r *postgresStoreSpoolRemote) Cleanup(now time.Time) error {
	return r.store.Cleanup(now)
}

func (r *postgresStoreSpoolRemote) BeginRead(
	ctx context.Context,
) (historyStoreSnapshot, error) {
	return r.store.BeginRead(ctx)
}

func (r *postgresStoreSpoolRemote) Cancel() {
	r.cancelOnce.Do(r.cancel)
}

func (r *postgresStoreSpoolRemote) Close() error {
	r.Cancel()
	return r.store.Close()
}

type postgresSpoolStoreOptions struct {
	Now     func() time.Time
	Uniform func(time.Duration) time.Duration
	Wait    func(context.Context, time.Duration) error
}

type postgresSpoolState string

const (
	postgresSpoolStateReady     postgresSpoolState = "ready"
	postgresSpoolStateDegraded  postgresSpoolState = "degraded"
	postgresSpoolStatePermanent postgresSpoolState = "permanent"
	postgresSpoolStateClosed    postgresSpoolState = "closed"
)

type postgresSpoolErrorCategory string

const (
	postgresSpoolErrorNone               postgresSpoolErrorCategory = ""
	postgresSpoolErrorTransientNetwork   postgresSpoolErrorCategory = "transient_network"
	postgresSpoolErrorTimeout            postgresSpoolErrorCategory = "timeout"
	postgresSpoolErrorDetourUnavailable  postgresSpoolErrorCategory = "detour_unavailable"
	postgresSpoolErrorDetourNoStream     postgresSpoolErrorCategory = "detour_no_stream"
	postgresSpoolErrorBatchCollision     postgresSpoolErrorCategory = "batch_collision"
	postgresSpoolErrorIdentityConflict   postgresSpoolErrorCategory = "identity_conflict"
	postgresSpoolErrorRevisionConflict   postgresSpoolErrorCategory = "revision_conflict"
	postgresSpoolErrorConfiguration      postgresSpoolErrorCategory = "configuration"
	postgresSpoolErrorAuthentication     postgresSpoolErrorCategory = "authentication"
	postgresSpoolErrorDatabaseMissing    postgresSpoolErrorCategory = "database_missing"
	postgresSpoolErrorPermission         postgresSpoolErrorCategory = "permission"
	postgresSpoolErrorSchemaMissing      postgresSpoolErrorCategory = "schema_missing"
	postgresSpoolErrorSchemaIncompatible postgresSpoolErrorCategory = "schema_incompatible"
	postgresSpoolErrorSchemaFuture       postgresSpoolErrorCategory = "schema_future"
	postgresSpoolErrorUnknown            postgresSpoolErrorCategory = "unknown"
)

type postgresSpoolStatus struct {
	State             postgresSpoolState
	LastErrorCategory postgresSpoolErrorCategory
	RetryCount        uint64
	NextRetry         time.Time

	QueueDepth   uint64
	QueueRecords uint64
	QueueBytes   uint64
	InFlight     bool

	DroppedBatches uint64
	DroppedRecords uint64
	DroppedBytes   uint64
	LossGeneration uint64

	LastSuccessfulFlush time.Time
}

type postgresSpoolStoreSnapshot struct {
	store    *postgresSpoolStore
	snapshot historyStoreSnapshot
	ctx      context.Context
	cancel   context.CancelFunc

	access sync.RWMutex
	closed bool
}

type postgresSpoolStore struct {
	spool   *historySpool
	initial historyStoreState
	remote  postgresSpoolRemote
	options postgresSpoolStoreOptions

	revisionAccess sync.RWMutex
	access         sync.Mutex
	opening        bool
	opened         bool
	closed         bool
	openDone       chan struct{}
	configRevision string
	cleanupAt      time.Time
	wakeCh         chan struct{}
	cleanupCh      chan struct{}
	workerCtx      context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	closeDone      chan struct{}
	closeErr       error
	readCalls      sync.WaitGroup
	snapshots      map[*postgresSpoolStoreSnapshot]struct{}
	commitProgress chan struct{}

	statusAccess      sync.RWMutex
	state             postgresSpoolState
	lastErrorCategory postgresSpoolErrorCategory
	retryAttempt      uint64
	nextRetry         time.Time
	lastSpoolStatus   historySpoolStatus
}

func newPostgresSpoolStore(
	spool *historySpool,
	initial historyStoreState,
	remote postgresSpoolRemote,
	options postgresSpoolStoreOptions,
) *postgresSpoolStore {
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Uniform == nil {
		options.Uniform = func(upper time.Duration) time.Duration {
			if upper <= 0 {
				return 0
			}
			return time.Duration(rand.Int63n(int64(upper)))
		}
	}
	if options.Wait == nil {
		options.Wait = waitPostgresSpoolRetry
	}
	return &postgresSpoolStore{
		spool:             spool,
		initial:           initial,
		remote:            remote,
		options:           options,
		configRevision:    initial.configRevision,
		wakeCh:            make(chan struct{}, 1),
		cleanupCh:         make(chan struct{}, 1),
		closeDone:         make(chan struct{}),
		snapshots:         make(map[*postgresSpoolStoreSnapshot]struct{}),
		commitProgress:    make(chan struct{}),
		state:             postgresSpoolStateReady,
		lastSpoolStatus:   spool.Status(),
		lastErrorCategory: postgresSpoolErrorNone,
	}
}

func (s *postgresSpoolStore) Open() (historyStoreState, error) {
	for {
		s.access.Lock()
		if s.closed {
			s.access.Unlock()
			return historyStoreState{}, ErrSpoolClosed
		}
		if s.opened {
			state := s.currentStoreStateLocked()
			s.access.Unlock()
			return state, nil
		}
		if s.opening {
			openDone := s.openDone
			s.access.Unlock()
			<-openDone
			continue
		}
		s.opening = true
		s.openDone = make(chan struct{})
		openDone := s.openDone
		s.workerCtx, s.cancel = context.WithCancel(s.spool.ctx)
		s.access.Unlock()

		_, remoteErr := s.connectRemote()

		s.access.Lock()
		s.opening = false
		if s.closed {
			close(openDone)
			s.access.Unlock()
			return historyStoreState{}, ErrSpoolClosed
		}
		s.opened = true
		state := s.currentStoreStateLocked()
		if remoteErr == nil {
			s.markReady()
			s.refreshSpoolStatus()
		}
		s.wg.Add(1)
		go s.run(remoteErr == nil, remoteErr)
		close(openDone)
		s.access.Unlock()
		s.signal(s.wakeCh)
		return state, nil
	}
}

func (s *postgresSpoolStore) Close() error {
	s.access.Lock()
	if s.closed {
		done := s.closeDone
		s.access.Unlock()
		<-done
		return s.closeErr
	}
	s.closed = true
	if s.cancel != nil {
		s.cancel()
	}
	openDone := s.openDone
	opening := s.opening
	s.access.Unlock()

	s.signalCommitProgress()
	s.remote.Cancel()
	if opening {
		<-openDone
	}
	s.wg.Wait()
	s.readCalls.Wait()

	s.access.Lock()
	snapshots := make([]*postgresSpoolStoreSnapshot, 0, len(s.snapshots))
	for snapshot := range s.snapshots {
		snapshots = append(snapshots, snapshot)
	}
	s.access.Unlock()
	for _, snapshot := range snapshots {
		snapshot.Close()
	}

	remoteErr := s.remote.Close()
	lastStatus := s.spool.Status()
	spoolErr := s.spool.Close()

	s.statusAccess.Lock()
	if lastStatus.FormatVersion != 0 {
		s.lastSpoolStatus = lastStatus
	}
	s.state = postgresSpoolStateClosed
	s.nextRetry = time.Time{}
	s.signalCommitProgressLocked()
	s.statusAccess.Unlock()

	s.access.Lock()
	s.closeErr = errors.Join(remoteErr, spoolErr)
	close(s.closeDone)
	closeErr := s.closeErr
	s.access.Unlock()
	return closeErr
}

func (s *postgresSpoolStore) Write(batch historyBatch) error {
	_, err := s.Commit(batch)
	return err
}

func (s *postgresSpoolStore) Commit(
	batch historyBatch,
) (historyCommitBoundary, error) {
	for {
		s.revisionAccess.RLock()
		s.access.Lock()
		if s.closed {
			s.access.Unlock()
			s.revisionAccess.RUnlock()
			return historyCommitBoundary{}, ErrSpoolClosed
		}
		if s.opening {
			openDone := s.openDone
			s.access.Unlock()
			s.revisionAccess.RUnlock()
			<-openDone
			continue
		}
		normalized := normalizePostgresSpoolBatch(batch, s.configRevision)
		boundary, err := s.spool.Append(normalized)
		if err != nil {
			s.access.Unlock()
			s.revisionAccess.RUnlock()
			return historyCommitBoundary{}, err
		}
		s.refreshSpoolStatus()
		s.signal(s.wakeCh)
		s.access.Unlock()
		s.revisionAccess.RUnlock()
		s.signalCommitProgress()
		return boundary, nil
	}
}

func (s *postgresSpoolStore) Cleanup(now time.Time) error {
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed {
		return ErrSpoolClosed
	}
	if now.After(s.cleanupAt) {
		s.cleanupAt = now
	}
	s.signal(s.cleanupCh)
	return nil
}

func (s *postgresSpoolStore) BeginRead(
	ctx context.Context,
) (historyStoreSnapshot, error) {
	for {
		s.access.Lock()
		if s.closed {
			s.access.Unlock()
			return nil, ErrSpoolClosed
		}
		if s.opening {
			openDone := s.openDone
			s.access.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-openDone:
			}
			continue
		}
		if !s.opened {
			s.access.Unlock()
			return nil, ErrHistoryUnavailable
		}
		workerCtx := s.workerCtx
		s.readCalls.Add(1)
		s.access.Unlock()

		readCtx, cancel := context.WithCancel(ctx)
		stopWorkerCancel := context.AfterFunc(workerCtx, cancel)
		snapshot, err := s.remote.BeginRead(readCtx)
		stopWorkerCancel()
		cancel()
		if err != nil {
			s.readCalls.Done()
			return nil, err
		}

		s.access.Lock()
		if s.closed {
			s.access.Unlock()
			snapshot.Close()
			s.readCalls.Done()
			return nil, ErrSpoolClosed
		}
		snapshotCtx, snapshotCancel := context.WithCancel(workerCtx)
		tracked := &postgresSpoolStoreSnapshot{
			store:    s,
			snapshot: snapshot,
			ctx:      snapshotCtx,
			cancel:   snapshotCancel,
		}
		s.snapshots[tracked] = struct{}{}
		s.access.Unlock()
		s.readCalls.Done()
		return tracked, nil
	}
}

func (s *postgresSpoolStore) BeginReadCommitted(
	ctx context.Context,
	boundary historyCommitBoundary,
) (historyStoreSnapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !boundary.Durable || boundary.LossDuringWrite {
		return nil, ErrHistoryUnavailable
	}
	for {
		s.statusAccess.RLock()
		progress := s.commitProgress
		s.statusAccess.RUnlock()

		status := s.spool.Status()
		s.statusAccess.RLock()
		state := s.state
		progressUnchanged := progress == s.commitProgress
		s.statusAccess.RUnlock()
		if !progressUnchanged {
			continue
		}
		if status.FormatVersion == 0 ||
			status.LossGeneration != boundary.LossGeneration ||
			state == postgresSpoolStatePermanent ||
			state == postgresSpoolStateClosed {
			return nil, ErrHistoryUnavailable
		}
		if state == postgresSpoolStateReady &&
			status.ResolvedSequence >= boundary.Sequence {
			snapshot, err := s.BeginRead(ctx)
			if err != nil {
				return nil, postgresCommittedQueryError(ctx)
			}
			latest := s.spool.Status()
			s.statusAccess.RLock()
			latestState := s.state
			s.statusAccess.RUnlock()
			if latest.FormatVersion == 0 ||
				latest.LossGeneration != boundary.LossGeneration ||
				latest.ResolvedSequence < boundary.Sequence ||
				latestState != postgresSpoolStateReady {
				snapshot.Close()
				return nil, ErrHistoryUnavailable
			}
			return snapshot, nil
		}

		s.access.Lock()
		closed := s.closed
		workerCtx := s.workerCtx
		s.access.Unlock()
		if closed || workerCtx == nil {
			return nil, ErrHistoryUnavailable
		}
		select {
		case <-ctx.Done():
			return nil, postgresCommittedQueryError(ctx)
		case <-workerCtx.Done():
			return nil, ErrHistoryUnavailable
		case <-progress:
		}
	}
}

func (s *postgresSpoolStore) Status() postgresSpoolStatus {
	live := s.spool.Status()
	s.statusAccess.RLock()
	state := s.state
	category := s.lastErrorCategory
	retryCount := s.retryAttempt
	nextRetry := s.nextRetry
	spoolStatus := s.lastSpoolStatus
	s.statusAccess.RUnlock()
	if live.FormatVersion != 0 {
		spoolStatus = live
	}
	return postgresSpoolStatus{
		State:               state,
		LastErrorCategory:   category,
		RetryCount:          retryCount,
		NextRetry:           nextRetry,
		QueueDepth:          spoolStatus.QueueDepth,
		QueueRecords:        spoolStatus.QueueRecords,
		QueueBytes:          spoolStatus.QueueBytes,
		InFlight:            spoolStatus.InFlight,
		DroppedBatches:      spoolStatus.DroppedBatches,
		DroppedRecords:      spoolStatus.DroppedRecords,
		DroppedBytes:        spoolStatus.DroppedBytes,
		LossGeneration:      spoolStatus.LossGeneration,
		LastSuccessfulFlush: spoolStatus.LastSuccessfulFlush,
	}
}

func (s *postgresSpoolStoreSnapshot) Query(
	ctx context.Context,
	query HistoryQuery,
	overlay historyQueryOverlay,
) (HistoryQueryResult, error) {
	s.access.RLock()
	defer s.access.RUnlock()
	if s.closed {
		return HistoryQueryResult{}, ErrHistoryUnavailable
	}
	queryCtx, cancel := context.WithCancel(ctx)
	stopSnapshotCancel := context.AfterFunc(s.ctx, cancel)
	defer func() {
		stopSnapshotCancel()
		cancel()
	}()
	result, err := s.snapshot.Query(queryCtx, query, overlay)
	if err != nil {
		return HistoryQueryResult{}, postgresCommittedQueryError(ctx)
	}
	return result, nil
}

func (s *postgresSpoolStoreSnapshot) Close() {
	s.cancel()
	s.access.Lock()
	if s.closed {
		s.access.Unlock()
		return
	}
	s.closed = true
	s.snapshot.Close()
	s.access.Unlock()
	s.store.releaseSnapshot(s)
}

func (s *postgresSpoolStore) releaseSnapshot(
	snapshot *postgresSpoolStoreSnapshot,
) {
	s.access.Lock()
	delete(s.snapshots, snapshot)
	s.access.Unlock()
}

func (s *postgresSpoolStore) run(
	remoteOpened bool,
	initialErr error,
) {
	defer s.wg.Done()
	if !remoteOpened {
		if initialErr != nil && !s.retry(initialErr) {
			return
		}
		if !s.openRemote() {
			return
		}
	}
	for {
		if s.workerCtx.Err() != nil {
			return
		}
		envelope, err := s.spool.Lease()
		if err != nil {
			if s.workerCtx.Err() == nil {
				s.markPermanent(err)
			}
			return
		}
		if envelope != nil {
			if !s.deliver(envelope) {
				return
			}
			if cleanupAt, ok := s.takeCleanup(); ok &&
				!s.runCleanup(cleanupAt) {
				return
			}
			continue
		}
		if cleanupAt, ok := s.takeCleanup(); ok {
			if !s.runCleanup(cleanupAt) {
				return
			}
			continue
		}
		select {
		case <-s.workerCtx.Done():
			return
		case <-s.wakeCh:
		case <-s.cleanupCh:
		}
	}
}

func (s *postgresSpoolStore) openRemote() bool {
	for {
		_, err := s.connectRemote()
		if err != nil {
			if !s.retry(err) {
				return false
			}
			continue
		}
		s.markReady()
		s.refreshSpoolStatus()
		return true
	}
}

func (s *postgresSpoolStore) connectRemote() (historyStoreState, error) {
	s.revisionAccess.Lock()
	defer s.revisionAccess.Unlock()
	state, err := s.remote.Open()
	if err != nil {
		return historyStoreState{}, err
	}
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed {
		return historyStoreState{}, ErrSpoolClosed
	}
	if err = s.spool.reconcileRemote(state); err != nil {
		return historyStoreState{}, err
	}
	s.configRevision = state.configRevision
	s.initial = state
	return state, nil
}

func (s *postgresSpoolStore) deliver(envelope *historyBatchEnvelope) bool {
	for {
		err := s.remote.Apply(
			s.workerCtx,
			envelope.BatchID,
			envelope.Batch,
		)
		if err == nil {
			err = s.spool.Acknowledge(envelope.Sequence, s.options.Now())
			if err == nil {
				s.markReady()
				s.refreshSpoolStatus()
				return true
			}
		}
		if s.workerCtx.Err() != nil {
			return false
		}
		if !s.retry(err) {
			return false
		}
	}
}

func (s *postgresSpoolStore) runCleanup(cleanupAt time.Time) bool {
	for {
		err := s.remote.Cleanup(cleanupAt)
		if err == nil {
			s.markReady()
			return true
		}
		if s.workerCtx.Err() != nil {
			return false
		}
		if !s.retry(err) {
			return false
		}
		if newer, ok := s.takeCleanup(); ok && newer.After(cleanupAt) {
			cleanupAt = newer
		}
	}
}

func (s *postgresSpoolStore) retry(err error) bool {
	if s.workerCtx.Err() != nil {
		return false
	}
	category, retryable := classifyPostgresSpoolError(err)
	if !retryable {
		s.markPermanentCategory(category)
		return false
	}

	s.statusAccess.Lock()
	attempt := s.retryAttempt
	delay := postgresSpoolRetryDelay(attempt, s.options.Uniform)
	if attempt != math.MaxUint64 {
		attempt++
	}
	s.retryAttempt = attempt
	s.nextRetry = s.options.Now().Add(delay)
	s.state = postgresSpoolStateDegraded
	s.lastErrorCategory = category
	s.signalCommitProgressLocked()
	s.statusAccess.Unlock()

	if waitErr := s.options.Wait(s.workerCtx, delay); waitErr != nil {
		return false
	}
	return s.workerCtx.Err() == nil
}

func (s *postgresSpoolStore) markReady() {
	s.statusAccess.Lock()
	if s.state == postgresSpoolStateClosed {
		s.statusAccess.Unlock()
		return
	}
	s.state = postgresSpoolStateReady
	s.lastErrorCategory = postgresSpoolErrorNone
	s.retryAttempt = 0
	s.nextRetry = time.Time{}
	s.signalCommitProgressLocked()
	s.statusAccess.Unlock()
}

func (s *postgresSpoolStore) markPermanent(err error) {
	category, _ := classifyPostgresSpoolError(err)
	s.markPermanentCategory(category)
}

func (s *postgresSpoolStore) markPermanentCategory(
	category postgresSpoolErrorCategory,
) {
	s.statusAccess.Lock()
	if s.state == postgresSpoolStateClosed {
		s.statusAccess.Unlock()
		return
	}
	s.state = postgresSpoolStatePermanent
	s.lastErrorCategory = category
	s.nextRetry = time.Time{}
	s.signalCommitProgressLocked()
	s.statusAccess.Unlock()
}

func (s *postgresSpoolStore) refreshSpoolStatus() {
	status := s.spool.Status()
	if status.FormatVersion == 0 {
		return
	}
	s.statusAccess.Lock()
	s.lastSpoolStatus = status
	s.statusAccess.Unlock()
}

func (s *postgresSpoolStore) takeCleanup() (time.Time, bool) {
	s.access.Lock()
	defer s.access.Unlock()
	if s.cleanupAt.IsZero() {
		return time.Time{}, false
	}
	cleanupAt := s.cleanupAt
	s.cleanupAt = time.Time{}
	return cleanupAt, true
}

func (s *postgresSpoolStore) currentStoreStateLocked() historyStoreState {
	status := s.spool.Status()
	if status.FormatVersion == 0 {
		return s.initial
	}
	return historyStoreState{
		configRevision:   status.ConfigRevision,
		targetsFrom:      status.TargetAvailableFrom,
		destinationsFrom: status.DestinationAvailableFrom,
	}
}

func (s *postgresSpoolStore) signal(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}

func (s *postgresSpoolStore) signalCommitProgress() {
	s.statusAccess.Lock()
	s.signalCommitProgressLocked()
	s.statusAccess.Unlock()
}

func (s *postgresSpoolStore) signalCommitProgressLocked() {
	if s.commitProgress != nil {
		close(s.commitProgress)
	}
	s.commitProgress = make(chan struct{})
}

func postgresCommittedQueryError(ctx context.Context) error {
	if ctx != nil && errors.Is(ctx.Err(), context.Canceled) {
		return context.Canceled
	}
	return ErrHistoryUnavailable
}

func normalizePostgresSpoolBatch(
	batch historyBatch,
	configRevision string,
) historyBatch {
	normalized := make(historyBatch, len(batch))
	for key, counters := range batch {
		key.ConfigRevision = configRevision
		normalized[key] = counters
	}
	return normalized
}

func classifyPostgresSpoolError(
	err error,
) (postgresSpoolErrorCategory, bool) {
	switch {
	case errors.Is(err, ErrPostgresTransientNetwork):
		return postgresSpoolErrorTransientNetwork, true
	case errors.Is(err, ErrPostgresTimeout):
		return postgresSpoolErrorTimeout, true
	case errors.Is(err, ErrPostgresDetourUnavailable):
		return postgresSpoolErrorDetourUnavailable, true
	case errors.Is(err, ErrPostgresDetourNoStream):
		return postgresSpoolErrorDetourNoStream, false
	case errors.Is(err, ErrPostgresBatchCollision):
		return postgresSpoolErrorBatchCollision, false
	case errors.Is(err, ErrPostgresIdentityConflict):
		return postgresSpoolErrorIdentityConflict, false
	case errors.Is(err, ErrPostgresRevisionConflict):
		return postgresSpoolErrorRevisionConflict, false
	case errors.Is(err, ErrPostgresConfiguration):
		return postgresSpoolErrorConfiguration, false
	case errors.Is(err, ErrPostgresAuthentication):
		return postgresSpoolErrorAuthentication, false
	case errors.Is(err, ErrPostgresDatabaseMissing):
		return postgresSpoolErrorDatabaseMissing, false
	case errors.Is(err, ErrPostgresPermission):
		return postgresSpoolErrorPermission, false
	case errors.Is(err, ErrPostgresSchemaMissing):
		return postgresSpoolErrorSchemaMissing, false
	case errors.Is(err, ErrPostgresSchemaIncompatible):
		return postgresSpoolErrorSchemaIncompatible, false
	case errors.Is(err, ErrPostgresSchemaFuture):
		return postgresSpoolErrorSchemaFuture, false
	default:
		return postgresSpoolErrorUnknown, false
	}
}

func postgresSpoolRetryDelay(
	attempt uint64,
	uniform func(time.Duration) time.Duration,
) time.Duration {
	const saturation = 6
	if attempt > saturation {
		attempt = saturation
	}
	capacity := postgresSpoolRetryBase * time.Duration(uint64(1)<<attempt)
	if capacity > postgresSpoolRetryMax {
		capacity = postgresSpoolRetryMax
	}
	half := capacity / 2
	offset := uniform(half)
	if offset < 0 {
		offset = 0
	}
	if offset >= half {
		offset = half - 1
	}
	return half + offset
}

func waitPostgresSpoolRetry(
	ctx context.Context,
	delay time.Duration,
) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

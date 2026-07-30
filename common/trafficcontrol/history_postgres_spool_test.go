package trafficcontrol

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
)

func TestPostgresSpoolStoreWriteLocalDurability(t *testing.T) {
	applyStarted := make(chan struct{}, 1)
	remote := &fakePostgresSpoolRemote{
		apply: func(ctx context.Context, _ uuid.UUID, _ historyBatch) error {
			select {
			case applyStarted <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}
	store := openPostgresSpoolStoreForTest(t, remote, postgresSpoolStoreOptions{})

	started := time.Now()
	if err := store.Write(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("Write waited for remote delivery")
	}
	select {
	case <-applyStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("remote delivery did not start")
	}
	status := store.Status()
	if status.QueueDepth != 1 || !status.InFlight {
		t.Fatalf("batch was not durably retained while inflight: %#v", status)
	}
}

func TestPostgresSpoolStoreStrictOrder(t *testing.T) {
	remote := &fakePostgresSpoolRemote{}
	store := openPostgresSpoolStoreForTest(t, remote, postgresSpoolStoreOptions{})
	spoolID := store.spool.Status().SpoolID

	for sequence := uint64(1); sequence <= 4; sequence++ {
		if err := store.Write(historySpoolTestBatch(sequence)); err != nil {
			t.Fatal(err)
		}
	}
	waitForPostgresSpoolCondition(t, func() bool {
		return len(remote.applyCallsCopy()) == 4 &&
			store.Status().QueueDepth == 0
	})
	calls := remote.applyCallsCopy()
	for index, call := range calls {
		want := historyBatchID(spoolID, uint64(index+1))
		if call.batchID != want {
			t.Fatalf("call %d batch ID = %s, want %s", index, call.batchID, want)
		}
	}
}

func TestPostgresSpoolStoreStableBatchID(t *testing.T) {
	testPostgresSpoolStoreUncertainCommit(t)
}

func TestPostgresSpoolStoreUncertainCommitRetry(t *testing.T) {
	testPostgresSpoolStoreUncertainCommit(t)
}

func TestPostgresSpoolStoreRetryClassification(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		category  postgresSpoolErrorCategory
		retryable bool
	}{
		{"network", ErrPostgresTransientNetwork, postgresSpoolErrorTransientNetwork, true},
		{"timeout", ErrPostgresTimeout, postgresSpoolErrorTimeout, true},
		{"detour unavailable", ErrPostgresDetourUnavailable, postgresSpoolErrorDetourUnavailable, true},
		{"detour no stream", ErrPostgresDetourNoStream, postgresSpoolErrorDetourNoStream, false},
		{"collision", ErrPostgresBatchCollision, postgresSpoolErrorBatchCollision, false},
		{"identity", ErrPostgresIdentityConflict, postgresSpoolErrorIdentityConflict, false},
		{"revision", ErrPostgresRevisionConflict, postgresSpoolErrorRevisionConflict, false},
		{"configuration", ErrPostgresConfiguration, postgresSpoolErrorConfiguration, false},
		{"authentication", ErrPostgresAuthentication, postgresSpoolErrorAuthentication, false},
		{"database", ErrPostgresDatabaseMissing, postgresSpoolErrorDatabaseMissing, false},
		{"permission", ErrPostgresPermission, postgresSpoolErrorPermission, false},
		{"schema missing", ErrPostgresSchemaMissing, postgresSpoolErrorSchemaMissing, false},
		{"schema incompatible", ErrPostgresSchemaIncompatible, postgresSpoolErrorSchemaIncompatible, false},
		{"schema future", ErrPostgresSchemaFuture, postgresSpoolErrorSchemaFuture, false},
		{"unknown", errors.New("contains postgres://secret@db/traffic"), postgresSpoolErrorUnknown, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			category, retryable := classifyPostgresSpoolError(
				fmt.Errorf("wrapped: %w", test.err),
			)
			if category != test.category || retryable != test.retryable {
				t.Fatalf(
					"classification = %q/%v, want %q/%v",
					category,
					retryable,
					test.category,
					test.retryable,
				)
			}
		})
	}
}

func TestPostgresSpoolStoreDegradedOnFirstRetryableOpen(t *testing.T) {
	retryStarted := make(chan time.Duration, 1)
	releaseRetry := make(chan struct{})
	var opens atomic.Uint64
	remote := &fakePostgresSpoolRemote{
		open: func() (historyStoreState, error) {
			if opens.Add(1) == 1 {
				return historyStoreState{}, ErrPostgresDetourUnavailable
			}
			return historySpoolRemoteTestState(historySpoolTestRevision), nil
		},
	}
	store := openPostgresSpoolStoreForTest(t, remote, postgresSpoolStoreOptions{
		Uniform: func(time.Duration) time.Duration { return 0 },
		Wait: func(ctx context.Context, delay time.Duration) error {
			retryStarted <- delay
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-releaseRetry:
				return nil
			}
		},
	})
	select {
	case delay := <-retryStarted:
		if delay != 500*time.Millisecond {
			t.Fatalf("first retry delay = %v, want 500ms", delay)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first retry did not start")
	}
	status := store.Status()
	if status.State != postgresSpoolStateDegraded ||
		status.LastErrorCategory != postgresSpoolErrorDetourUnavailable ||
		status.RetryCount != 1 ||
		status.NextRetry.IsZero() {
		t.Fatalf("first retry was not degraded: %#v", status)
	}
	close(releaseRetry)
	waitForPostgresSpoolCondition(t, func() bool {
		status = store.Status()
		return status.State == postgresSpoolStateReady &&
			status.RetryCount == 0 &&
			status.NextRetry.IsZero()
	})
	if opens.Load() != 2 {
		t.Fatalf("remote Open calls = %d, want 2", opens.Load())
	}
}

func TestPostgresSpoolStorePermanentStopsRemote(t *testing.T) {
	remote := &fakePostgresSpoolRemote{
		apply: func(context.Context, uuid.UUID, historyBatch) error {
			return ErrPostgresBatchCollision
		},
	}
	store := openPostgresSpoolStoreForTest(t, remote, postgresSpoolStoreOptions{})
	if err := store.Write(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	waitForPostgresSpoolCondition(t, func() bool {
		return store.Status().State == postgresSpoolStatePermanent
	})
	if err := store.Write(historySpoolTestBatch(2)); err != nil {
		t.Fatal(err)
	}
	if err := store.Cleanup(historySpoolTestNow()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if calls := len(remote.applyCallsCopy()); calls != 1 {
		t.Fatalf("permanent failure made %d apply calls, want 1", calls)
	}
	if calls := remote.cleanupCallCount(); calls != 0 {
		t.Fatalf("permanent failure made %d cleanup calls, want 0", calls)
	}
	status := store.Status()
	if status.QueueDepth != 2 ||
		status.LastErrorCategory != postgresSpoolErrorBatchCollision {
		t.Fatalf("unexpected permanent status: %#v", status)
	}
}

func TestPostgresSpoolStoreEqualJitterBounds(t *testing.T) {
	tests := []struct {
		attempt uint64
		wantCap time.Duration
	}{
		{0, time.Second},
		{1, 2 * time.Second},
		{5, 32 * time.Second},
		{6, time.Minute},
		{math.MaxUint64, time.Minute},
	}
	for _, test := range tests {
		low := postgresSpoolRetryDelay(
			test.attempt,
			func(time.Duration) time.Duration { return 0 },
		)
		high := postgresSpoolRetryDelay(
			test.attempt,
			func(upper time.Duration) time.Duration { return upper - 1 },
		)
		if low != test.wantCap/2 || high != test.wantCap-1 {
			t.Fatalf(
				"attempt %d delay bounds = %s..%s, want %s..%s",
				test.attempt,
				low,
				high,
				test.wantCap/2,
				test.wantCap-1,
			)
		}
	}
}

func TestPostgresSpoolStoreAliasWriteNormalization(t *testing.T) {
	remote := &fakePostgresSpoolRemote{
		state: historySpoolRemoteTestState(historySpoolRemoteAliasRevision),
	}
	store := openPostgresSpoolStoreForTest(t, remote, postgresSpoolStoreOptions{})
	waitForPostgresSpoolCondition(t, func() bool {
		status := store.spool.Status()
		return status.RemoteRevisionConfirmed &&
			status.ConfigRevision == historySpoolRemoteAliasRevision
	})

	if err := store.Write(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	waitForPostgresSpoolCondition(t, func() bool {
		return len(remote.applyCallsCopy()) == 1 &&
			store.Status().QueueDepth == 0
	})
	for key := range remote.applyCallsCopy()[0].batch {
		if key.ConfigRevision != historySpoolRemoteAliasRevision {
			t.Fatalf(
				"Write kept stale revision %q, want %q",
				key.ConfigRevision,
				historySpoolRemoteAliasRevision,
			)
		}
	}
}

func TestPostgresSpoolStoreAsyncCleanup(t *testing.T) {
	openStarted := make(chan struct{})
	releaseOpen := make(chan struct{})
	remote := &fakePostgresSpoolRemote{
		open: func() (historyStoreState, error) {
			close(openStarted)
			<-releaseOpen
			return historySpoolRemoteTestState(historySpoolTestRevision), nil
		},
	}
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
	openResult := make(chan error, 1)
	go func() {
		_, openErr := store.Open()
		openResult <- openErr
	}()
	select {
	case <-openStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("remote open did not start")
	}
	first := historySpoolTestNow()
	second := first.Add(time.Minute)

	started := time.Now()
	if err := store.Cleanup(first); err != nil {
		t.Fatal(err)
	}
	if err := store.Cleanup(second); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("Cleanup waited for PostgreSQL")
	}
	close(releaseOpen)
	if err = <-openResult; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	waitForPostgresSpoolCondition(t, func() bool {
		return remote.cleanupCallCount() == 1
	})
	calls := remote.cleanupCallsCopy()
	if !calls[0].Equal(second) {
		t.Fatalf("cleanup time = %v, want newest %v", calls[0], second)
	}
}

func TestPostgresSpoolStoreWriteWaitsForInitialAliasReconcile(t *testing.T) {
	openStarted := make(chan struct{})
	releaseOpen := make(chan struct{})
	var releaseOnce sync.Once
	remote := &fakePostgresSpoolRemote{
		state: historySpoolRemoteTestState(historySpoolRemoteAliasRevision),
		open: func() (historyStoreState, error) {
			close(openStarted)
			<-releaseOpen
			return historySpoolRemoteTestState(
				historySpoolRemoteAliasRevision,
			), nil
		},
		cancel: func() {
			releaseOnce.Do(func() {
				close(releaseOpen)
			})
		},
	}
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
	openResult := make(chan error, 1)
	go func() {
		_, openErr := store.Open()
		openResult <- openErr
	}()
	select {
	case <-openStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("remote Open did not start")
	}
	writeResult := make(chan error, 1)
	go func() {
		writeResult <- store.Write(historySpoolTestBatch(1))
	}()
	select {
	case err = <-writeResult:
		releaseOnce.Do(func() {
			close(releaseOpen)
		})
		_ = <-openResult
		_ = store.Close()
		t.Fatalf("Write raced initial alias reconciliation: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	releaseOnce.Do(func() {
		close(releaseOpen)
	})
	if err = <-openResult; err != nil {
		t.Fatal(err)
	}
	if err = <-writeResult; err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	waitForPostgresSpoolCondition(t, func() bool {
		return len(remote.applyCallsCopy()) == 1 &&
			store.Status().QueueDepth == 0
	})
	for key := range remote.applyCallsCopy()[0].batch {
		if key.ConfigRevision != historySpoolRemoteAliasRevision {
			t.Fatalf(
				"Write used revision %q, want reconciled alias %q",
				key.ConfigRevision,
				historySpoolRemoteAliasRevision,
			)
		}
	}
}

func TestPostgresSpoolStoreCloseCancelsRemoteOpen(t *testing.T) {
	openStarted := make(chan struct{})
	releaseOpen := make(chan struct{})
	var releaseOnce sync.Once
	remote := &fakePostgresSpoolRemote{
		open: func() (historyStoreState, error) {
			close(openStarted)
			<-releaseOpen
			return historyStoreState{}, context.Canceled
		},
		cancel: func() {
			releaseOnce.Do(func() {
				close(releaseOpen)
			})
		},
	}
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
	openResult := make(chan error, 1)
	go func() {
		_, openErr := store.Open()
		openResult <- openErr
	}()
	select {
	case <-openStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("remote Open did not start")
	}
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- store.Close()
	}()
	select {
	case err = <-closeResult:
	case <-time.After(250 * time.Millisecond):
		releaseOnce.Do(func() {
			close(releaseOpen)
		})
		err = <-closeResult
		t.Fatalf("Close did not cancel remote Open promptly: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = <-openResult; !errors.Is(err, ErrSpoolClosed) {
		t.Fatalf("concurrent Open error = %v, want ErrSpoolClosed", err)
	}
	if remote.cancelCallCount() != 1 || remote.closeCallCount() != 1 {
		t.Fatalf(
			"remote lifecycle calls = cancel:%d close:%d, want 1/1",
			remote.cancelCallCount(),
			remote.closeCallCount(),
		)
	}
}

func TestPostgresSpoolStoreCloseCancelsRemoteCleanup(t *testing.T) {
	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	var releaseOnce sync.Once
	remote := &fakePostgresSpoolRemote{
		cleanup: func(time.Time) error {
			close(cleanupStarted)
			<-releaseCleanup
			return context.Canceled
		},
		cancel: func() {
			releaseOnce.Do(func() {
				close(releaseCleanup)
			})
		},
	}
	store := openPostgresSpoolStoreForTest(t, remote, postgresSpoolStoreOptions{})
	if err := store.Cleanup(historySpoolTestNow()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cleanupStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("remote Cleanup did not start")
	}
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- store.Close()
	}()
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(250 * time.Millisecond):
		releaseOnce.Do(func() {
			close(releaseCleanup)
		})
		err := <-closeResult
		t.Fatalf("Close did not cancel remote Cleanup promptly: %v", err)
	}
}

func TestPostgresSpoolStoreCloseOwnsActiveSnapshots(t *testing.T) {
	snapshotClosed := make(chan struct{})
	snapshot := &fakePostgresSpoolSnapshot{
		close: func() {
			close(snapshotClosed)
		},
	}
	var store *postgresSpoolStore
	remote := &fakePostgresSpoolRemote{
		beginRead: func(context.Context) (historyStoreSnapshot, error) {
			return snapshot, nil
		},
		close: func() error {
			select {
			case <-snapshotClosed:
			default:
				return errors.New("remote closed before active snapshot")
			}
			if store.spool.Status().FormatVersion == 0 {
				return errors.New("spool closed before remote")
			}
			return nil
		},
	}
	store = openPostgresSpoolStoreForTest(t, remote, postgresSpoolStoreOptions{})
	openedSnapshot, err := store.BeginRead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	openedSnapshot.Close()
	if snapshot.closeCallCount() != 1 {
		t.Fatalf("snapshot Close calls = %d, want 1", snapshot.closeCallCount())
	}
}

func TestPostgresSpoolStoreCloseCancelsBeginRead(t *testing.T) {
	readStarted := make(chan struct{})
	remote := &fakePostgresSpoolRemote{
		beginRead: func(
			ctx context.Context,
		) (historyStoreSnapshot, error) {
			close(readStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	store := openPostgresSpoolStoreForTest(t, remote, postgresSpoolStoreOptions{})
	readResult := make(chan error, 1)
	go func() {
		_, err := store.BeginRead(context.Background())
		readResult <- err
	}()
	select {
	case <-readStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("BeginRead did not start")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-readResult; !errors.Is(err, context.Canceled) &&
		!errors.Is(err, ErrSpoolClosed) {
		t.Fatalf("BeginRead close error = %v", err)
	}
}

func TestPostgresSpoolStoreCloseWaitsForRemoteBeforeSpool(t *testing.T) {
	remoteCloseStarted := make(chan struct{})
	releaseRemoteClose := make(chan struct{})
	remote := &fakePostgresSpoolRemote{
		close: func() error {
			close(remoteCloseStarted)
			<-releaseRemoteClose
			return nil
		},
	}
	store := openPostgresSpoolStoreForTest(t, remote, postgresSpoolStoreOptions{})
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- store.Close()
	}()
	select {
	case <-remoteCloseStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("remote Close did not start")
	}
	if store.spool.Status().FormatVersion == 0 {
		t.Fatal("spool closed before remote Close completed")
	}
	select {
	case err := <-closeResult:
		t.Fatalf("store Close returned before remote Close: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseRemoteClose)
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}
	if store.spool.Status().FormatVersion != 0 {
		t.Fatal("spool remained open after Close")
	}
}

func TestPostgresSpoolStoreCloseCancellation(t *testing.T) {
	applyStarted := make(chan struct{})
	applyStopped := make(chan struct{})
	remote := &fakePostgresSpoolRemote{
		apply: func(ctx context.Context, _ uuid.UUID, _ historyBatch) error {
			close(applyStarted)
			<-ctx.Done()
			close(applyStopped)
			return ctx.Err()
		},
	}
	store := openPostgresSpoolStoreForTest(t, remote, postgresSpoolStoreOptions{})
	if err := store.Write(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-applyStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("remote apply did not start")
	}

	started := time.Now()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("Close did not cancel remote apply promptly")
	}
	select {
	case <-applyStopped:
	default:
		t.Fatal("Close returned before the delivery worker stopped")
	}
	status := store.Status()
	if status.State != postgresSpoolStateClosed ||
		status.QueueDepth != 1 ||
		!status.InFlight {
		t.Fatalf("Close lost the unacknowledged inflight batch: %#v", status)
	}
}

func TestPostgresSpoolStoreRetryCounterSaturates(t *testing.T) {
	store := &postgresSpoolStore{
		options: postgresSpoolStoreOptions{
			Now:     historySpoolTestNow,
			Uniform: func(time.Duration) time.Duration { return 0 },
			Wait: func(context.Context, time.Duration) error {
				return nil
			},
		},
		workerCtx: context.Background(),
		state:     postgresSpoolStateReady,
	}
	store.retryAttempt = math.MaxUint64
	if !store.retry(ErrPostgresTimeout) {
		t.Fatal("saturating retry unexpectedly stopped")
	}
	store.statusAccess.RLock()
	retryAttempt := store.retryAttempt
	store.statusAccess.RUnlock()
	if retryAttempt != math.MaxUint64 {
		t.Fatalf("retry count wrapped to %d", retryAttempt)
	}
}

func TestPostgresSpoolStoreStatusRedaction(t *testing.T) {
	const secret = "postgres://traffic:unique-secret@db.example/traffic"
	remote := &fakePostgresSpoolRemote{
		apply: func(context.Context, uuid.UUID, historyBatch) error {
			return fmt.Errorf("%s: %w", secret, ErrPostgresAuthentication)
		},
	}
	store := openPostgresSpoolStoreForTest(t, remote, postgresSpoolStoreOptions{})
	if err := store.Write(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	waitForPostgresSpoolCondition(t, func() bool {
		return store.Status().State == postgresSpoolStatePermanent
	})
	status := store.Status()
	if status.LastErrorCategory != postgresSpoolErrorAuthentication ||
		strings.Contains(fmt.Sprintf("%#v", status), secret) {
		t.Fatalf("status was not redacted: %#v", status)
	}
}

func TestPostgresSpoolStoreConcurrentAccessRace(t *testing.T) {
	remote := &fakePostgresSpoolRemote{}
	store := openPostgresSpoolStoreForTest(t, remote, postgresSpoolStoreOptions{})

	var group sync.WaitGroup
	var unexpected atomic.Value
	for worker := 0; worker < 8; worker++ {
		group.Add(1)
		go func(worker int) {
			defer group.Done()
			for index := 0; index < 32; index++ {
				switch index % 3 {
				case 0:
					err := store.Write(historySpoolTestBatch(uint64(worker*32 + index + 1)))
					if err != nil && !errors.Is(err, ErrSpoolClosed) {
						unexpected.Store(err)
						return
					}
				case 1:
					err := store.Cleanup(historySpoolTestNow().Add(time.Duration(index)))
					if err != nil && !errors.Is(err, ErrSpoolClosed) {
						unexpected.Store(err)
						return
					}
				default:
					_ = store.Status()
				}
			}
		}(worker)
	}
	group.Wait()
	if err, _ := unexpected.Load().(error); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(historySpoolTestBatch(999)); !errors.Is(err, ErrSpoolClosed) {
		t.Fatalf("post-close Write error = %v", err)
	}
}

func TestPostgresSpoolStoreRestartInflight(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool, initial, err := openHistorySpool(options)
	if err != nil {
		t.Fatal(err)
	}
	firstStarted := make(chan uuid.UUID, 1)
	firstRemote := &fakePostgresSpoolRemote{
		state: historySpoolRemoteTestState(historySpoolTestRevision),
		apply: func(ctx context.Context, batchID uuid.UUID, _ historyBatch) error {
			firstStarted <- batchID
			<-ctx.Done()
			return ctx.Err()
		},
	}
	first := newPostgresSpoolStore(
		spool,
		initial,
		firstRemote,
		postgresSpoolStoreOptions{},
	)
	if _, err = first.Open(); err != nil {
		t.Fatal(err)
	}
	if err = first.Write(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	var firstBatchID uuid.UUID
	select {
	case firstBatchID = <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first delivery did not start")
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}

	spool, initial, err = openHistorySpool(options)
	if err != nil {
		t.Fatal(err)
	}
	secondRemote := &fakePostgresSpoolRemote{
		state: historySpoolRemoteTestState(historySpoolTestRevision),
	}
	second := newPostgresSpoolStore(
		spool,
		initial,
		secondRemote,
		postgresSpoolStoreOptions{},
	)
	if _, err = second.Open(); err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	waitForPostgresSpoolCondition(t, func() bool {
		return second.Status().QueueDepth == 0 &&
			len(secondRemote.applyCallsCopy()) == 1
	})
	if got := secondRemote.applyCallsCopy()[0].batchID; got != firstBatchID {
		t.Fatalf("batch ID changed across restart: %s != %s", got, firstBatchID)
	}
}

func testPostgresSpoolStoreUncertainCommit(t *testing.T) {
	t.Helper()
	var attempts atomic.Uint64
	remote := &fakePostgresSpoolRemote{
		apply: func(context.Context, uuid.UUID, historyBatch) error {
			if attempts.Add(1) == 1 {
				return fmt.Errorf("commit outcome unknown: %w", ErrPostgresTimeout)
			}
			return nil
		},
	}
	var delaysMu sync.Mutex
	var delays []time.Duration
	store := openPostgresSpoolStoreForTest(t, remote, postgresSpoolStoreOptions{
		Uniform: func(time.Duration) time.Duration { return 0 },
		Wait: func(_ context.Context, delay time.Duration) error {
			delaysMu.Lock()
			delays = append(delays, delay)
			delaysMu.Unlock()
			return nil
		},
	})
	if err := store.Write(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	waitForPostgresSpoolCondition(t, func() bool {
		status := store.Status()
		return status.QueueDepth == 0 &&
			len(remote.applyCallsCopy()) == 2 &&
			status.State == postgresSpoolStateReady &&
			status.RetryCount == 0 &&
			status.NextRetry.IsZero()
	})
	calls := remote.applyCallsCopy()
	if calls[0].batchID != calls[1].batchID {
		t.Fatalf("retry changed batch ID: %s != %s", calls[0].batchID, calls[1].batchID)
	}
	delaysMu.Lock()
	defer delaysMu.Unlock()
	if len(delays) != 1 || delays[0] != 500*time.Millisecond {
		t.Fatalf("retry delays = %v, want [500ms]", delays)
	}
}

func openPostgresSpoolStoreForTest(
	t *testing.T,
	remote *fakePostgresSpoolRemote,
	options postgresSpoolStoreOptions,
) *postgresSpoolStore {
	t.Helper()
	spool, initial, err := openHistorySpool(historySpoolTestOptions(t, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	if remote.state.configRevision == "" {
		remote.state = historySpoolRemoteTestState(initial.configRevision)
	}
	store := newPostgresSpoolStore(spool, initial, remote, options)
	if _, err = store.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func waitForPostgresSpoolCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for PostgreSQL spool condition")
		}
		time.Sleep(time.Millisecond)
	}
}

type fakePostgresSpoolApplyCall struct {
	batchID uuid.UUID
	batch   historyBatch
}

type fakePostgresSpoolRemote struct {
	access sync.Mutex

	state        historyStoreState
	open         func() (historyStoreState, error)
	apply        func(context.Context, uuid.UUID, historyBatch) error
	cleanup      func(time.Time) error
	beginRead    func(context.Context) (historyStoreSnapshot, error)
	close        func() error
	cancel       func()
	openCalls    int
	applyCalls   []fakePostgresSpoolApplyCall
	cleanupCalls []time.Time
	cancelCalls  int
	closeCalls   int
}

func (r *fakePostgresSpoolRemote) Open() (historyStoreState, error) {
	r.access.Lock()
	r.openCalls++
	open := r.open
	state := r.state
	r.access.Unlock()
	if open != nil {
		return open()
	}
	return state, nil
}

func (r *fakePostgresSpoolRemote) Apply(
	ctx context.Context,
	batchID uuid.UUID,
	batch historyBatch,
) error {
	copied := make(historyBatch, len(batch))
	for key, counters := range batch {
		copied[key] = counters
	}
	r.access.Lock()
	r.applyCalls = append(r.applyCalls, fakePostgresSpoolApplyCall{
		batchID: batchID,
		batch:   copied,
	})
	apply := r.apply
	r.access.Unlock()
	if apply != nil {
		return apply(ctx, batchID, batch)
	}
	return nil
}

func (r *fakePostgresSpoolRemote) Cleanup(now time.Time) error {
	r.access.Lock()
	r.cleanupCalls = append(r.cleanupCalls, now)
	cleanup := r.cleanup
	r.access.Unlock()
	if cleanup != nil {
		return cleanup(now)
	}
	return nil
}

func (r *fakePostgresSpoolRemote) BeginRead(
	ctx context.Context,
) (historyStoreSnapshot, error) {
	r.access.Lock()
	beginRead := r.beginRead
	r.access.Unlock()
	if beginRead != nil {
		return beginRead(ctx)
	}
	return nil, ErrHistoryUnavailable
}

func (r *fakePostgresSpoolRemote) Close() error {
	r.access.Lock()
	r.closeCalls++
	closeRemote := r.close
	r.access.Unlock()
	if closeRemote != nil {
		return closeRemote()
	}
	return nil
}

func (r *fakePostgresSpoolRemote) Cancel() {
	r.access.Lock()
	r.cancelCalls++
	cancel := r.cancel
	r.access.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *fakePostgresSpoolRemote) applyCallsCopy() []fakePostgresSpoolApplyCall {
	r.access.Lock()
	defer r.access.Unlock()
	return append([]fakePostgresSpoolApplyCall(nil), r.applyCalls...)
}

func (r *fakePostgresSpoolRemote) cleanupCallCount() int {
	r.access.Lock()
	defer r.access.Unlock()
	return len(r.cleanupCalls)
}

func (r *fakePostgresSpoolRemote) cleanupCallsCopy() []time.Time {
	r.access.Lock()
	defer r.access.Unlock()
	return append([]time.Time(nil), r.cleanupCalls...)
}

func (r *fakePostgresSpoolRemote) cancelCallCount() int {
	r.access.Lock()
	defer r.access.Unlock()
	return r.cancelCalls
}

func (r *fakePostgresSpoolRemote) closeCallCount() int {
	r.access.Lock()
	defer r.access.Unlock()
	return r.closeCalls
}

type fakePostgresSpoolSnapshot struct {
	access sync.Mutex
	close  func()
	closed bool
	calls  int
}

func (s *fakePostgresSpoolSnapshot) Query(
	context.Context,
	HistoryQuery,
	historyQueryOverlay,
) (HistoryQueryResult, error) {
	return HistoryQueryResult{}, nil
}

func (s *fakePostgresSpoolSnapshot) Close() {
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.calls++
	if s.close != nil {
		s.close()
	}
}

func (s *fakePostgresSpoolSnapshot) closeCallCount() int {
	s.access.Lock()
	defer s.access.Unlock()
	return s.calls
}

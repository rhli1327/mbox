package trafficcontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/sagernet/bbolt"
)

const historySpoolTestRevision = "00112233445566778899aabbccddeeff"

func TestHistorySpoolFirstRunInitializesV1(t *testing.T) {
	spool := openHistorySpoolForTest(t, historySpoolTestOptions(t, 1<<20))
	defer spool.Close()

	status := spool.Status()
	if status.FormatVersion != historySpoolFormatVersion ||
		status.NextSequence != 1 ||
		status.ResolvedSequence != 0 ||
		status.QueueDepth != 0 ||
		status.QueueRecords != 0 ||
		status.QueueBytes != 0 ||
		status.SpoolID == uuid.Nil {
		t.Fatalf("unexpected initial status: %#v", status)
	}
	wantAvailable := historySpoolTestNow().UTC().
		Truncate(HistoryBucketInterval).
		Add(HistoryBucketInterval)
	if !status.TargetAvailableFrom.Equal(wantAvailable) ||
		!status.DestinationAvailableFrom.Equal(wantAvailable) {
		t.Fatalf("unexpected initial availability: %#v", status)
	}
	info, err := os.Stat(spool.path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("spool mode is %04o, want 0600", info.Mode().Perm())
	}
	assertHistorySpoolFormat(t, spool)
}

func TestHistorySpoolParentSyncRunsOnExistingOpen(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool := openHistorySpoolForTest(t, options)
	before := spool.Status()
	_, beforeRevisionHash := readHistorySpoolStateFromOpenForTest(t, spool)
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}

	var parentSyncCalls atomic.Uint64
	options.Faults = &historySpoolFaults{
		ParentSync: func() error {
			parentSyncCalls.Add(1)
			return nil
		},
	}
	spool = openHistorySpoolForTest(t, options)
	defer spool.Close()
	after := spool.Status()
	_, afterRevisionHash := readHistorySpoolStateFromOpenForTest(t, spool)
	t.Logf(
		"existing open: parent_sync_calls=%d before=%#v after=%#v",
		parentSyncCalls.Load(),
		before,
		after,
	)
	if parentSyncCalls.Load() != 1 {
		t.Fatalf("parent sync count = %d, want 1", parentSyncCalls.Load())
	}
	if before != after || beforeRevisionHash != afterRevisionHash {
		t.Fatalf("parent sync changed spool metadata: %#v %#v", before, after)
	}
}

func TestHistorySpoolStableBatchIdentityAcrossRestart(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool := openHistorySpoolForTest(t, options)
	boundary, err := spool.Append(historySpoolTestBatch(1))
	if err != nil {
		t.Fatal(err)
	}
	first, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if first == nil {
		t.Fatal("missing first lease")
	}
	if err = spool.Close(); err != nil {
		t.Fatal(err)
	}

	spool = openHistorySpoolForTest(t, options)
	defer spool.Close()
	second, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if second == nil ||
		second.Sequence != boundary.Sequence ||
		second.BatchID != first.BatchID ||
		!bytes.Equal(second.Encoded, first.Encoded) {
		t.Fatalf("lease identity changed after restart: %#v %#v", first, second)
	}
}

func TestHistorySpoolNewGenerationChangesBatchIdentity(t *testing.T) {
	firstOptions := historySpoolTestOptions(t, 1<<20)
	first := openHistorySpoolForTest(t, firstOptions)
	if _, err := first.Append(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	firstLease, err := first.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}

	secondOptions := firstOptions
	secondOptions.Path = filepath.Join(t.TempDir(), "traffic-spool.db")
	second := openHistorySpoolForTest(t, secondOptions)
	defer second.Close()
	if _, err = second.Append(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	secondLease, err := second.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if firstLease.BatchID == secondLease.BatchID ||
		firstLease.PayloadSHA256 != secondLease.PayloadSHA256 {
		t.Fatalf("unexpected generation identity: %#v %#v", firstLease, secondLease)
	}
}

func TestHistorySpoolOrdersSequences(t *testing.T) {
	spool := openHistorySpoolForTest(t, historySpoolTestOptions(t, 1<<20))
	defer spool.Close()
	for index := uint64(1); index <= 8; index++ {
		boundary, err := spool.Append(historySpoolTestBatch(index))
		if err != nil {
			t.Fatal(err)
		}
		if boundary.Sequence != index {
			t.Fatalf("sequence = %d, want %d", boundary.Sequence, index)
		}
	}
	for index := uint64(1); index <= 8; index++ {
		envelope, err := spool.Lease()
		if err != nil {
			t.Fatal(err)
		}
		if envelope == nil || envelope.Sequence != index {
			t.Fatalf("lease = %#v, want sequence %d", envelope, index)
		}
		if err = spool.Acknowledge(index, historySpoolTestNow()); err != nil {
			t.Fatal(err)
		}
	}
	if envelope, err := spool.Lease(); err != nil || envelope != nil {
		t.Fatalf("unexpected final lease: %#v %v", envelope, err)
	}
}

func TestHistorySpoolRejectsUnsafeFile(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	if err := os.WriteFile(options.Path, []byte("not bolt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if err := os.Chmod(options.Path, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, _, err := openHistorySpool(options); !errors.Is(err, ErrSpoolIO) {
			t.Fatalf("loose permissions: %v", err)
		}
	}

	symlinkOptions := historySpoolTestOptions(t, 1<<20)
	target := filepath.Join(t.TempDir(), "target.db")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, symlinkOptions.Path); err != nil {
		if runtime.GOOS == "windows" {
			t.Skip("symlink privilege unavailable")
		}
		t.Fatal(err)
	}
	if _, _, err := openHistorySpool(symlinkOptions); !errors.Is(err, ErrSpoolIO) {
		t.Fatalf("symlink: %v", err)
	}
}

func TestHistorySpoolRejectsFutureVersion(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool := openHistorySpoolForTest(t, options)
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	mutateHistorySpool(t, options.Path, func(tx *bbolt.Tx) error {
		return tx.Bucket(historySpoolMetadataBucket).Put(
			historySpoolFormatVersionKey,
			historySpoolUint32(historySpoolFormatVersion+1),
		)
	})
	if _, _, err := openHistorySpool(options); !errors.Is(err, ErrSpoolFuture) {
		t.Fatalf("future format: %v", err)
	}
}

func TestHistorySpoolRejectsUnknownBucket(t *testing.T) {
	t.Run("top level", func(t *testing.T) {
		options := historySpoolTestOptions(t, 1<<20)
		spool := openHistorySpoolForTest(t, options)
		if err := spool.Close(); err != nil {
			t.Fatal(err)
		}
		mutateHistorySpool(t, options.Path, func(tx *bbolt.Tx) error {
			_, err := tx.CreateBucket([]byte("unexpected"))
			return err
		})
		if _, _, err := openHistorySpool(options); !errors.Is(
			err,
			ErrSpoolCorrupt,
		) {
			t.Fatalf("unknown bucket: %v", err)
		}
	})
	t.Run("metadata", func(t *testing.T) {
		options := historySpoolTestOptions(t, 1<<20)
		spool := openHistorySpoolForTest(t, options)
		if err := spool.Close(); err != nil {
			t.Fatal(err)
		}
		mutateHistorySpool(t, options.Path, func(tx *bbolt.Tx) error {
			return tx.Bucket(historySpoolMetadataBucket).Put(
				[]byte("unexpected"),
				[]byte{1},
			)
		})
		if _, _, err := openHistorySpool(options); !errors.Is(
			err,
			ErrSpoolCorrupt,
		) {
			t.Fatalf("unknown metadata: %v", err)
		}
	})
}

func TestHistorySpoolRejectsCorruptIndexes(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool := openHistorySpoolForTest(t, options)
	if _, err := spool.Append(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	mutateHistorySpool(t, options.Path, func(tx *bbolt.Tx) error {
		return tx.Bucket(historySpoolMetadataBucket).Put(
			historySpoolQueuedBatchesKey,
			encodeHistorySpoolUint64(99),
		)
	})
	if _, _, err := openHistorySpool(options); !errors.Is(err, ErrSpoolCorrupt) {
		t.Fatalf("corrupt indexes: %v", err)
	}
}

func TestHistorySpoolRejectsConcurrentOpen(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	first := openHistorySpoolForTest(t, options)
	defer first.Close()
	start := time.Now()
	_, _, err := openHistorySpool(options)
	if !errors.Is(err, ErrSpoolLocked) {
		t.Fatalf("concurrent open: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond ||
		elapsed > 3*time.Second {
		t.Fatalf("lock timeout = %v, want approximately one second", elapsed)
	}
}

func TestHistorySpoolRejectsIdentityConflict(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool := openHistorySpoolForTest(t, options)
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	options.Identity.InstanceID = uuid.Must(uuid.NewV4()).String()
	options.Identity.RevisionKey = bytes.Repeat([]byte{0xa5}, sha256.Size)
	if _, _, err := openHistorySpool(options); !errors.Is(
		err,
		ErrSpoolIdentityConflict,
	) {
		t.Fatalf("identity conflict: %v", err)
	}
}

func TestHistorySpoolDropOldest(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool := openHistorySpoolForTest(t, options)
	defer spool.Close()
	first, err := spool.Append(historySpoolTestBatch(1))
	if err != nil {
		t.Fatal(err)
	}
	firstStatus := spool.Status()
	spool.maxSize = firstStatus.QueueBytes*2 + firstStatus.QueueBytes/2
	if _, err = spool.Append(historySpoolTestBatch(2)); err != nil {
		t.Fatal(err)
	}
	third, err := spool.Append(historySpoolTestBatch(3))
	if err != nil {
		t.Fatal(err)
	}
	status := spool.Status()
	if !third.LossDuringWrite ||
		third.LossGeneration != 1 ||
		status.DroppedBatches != 1 ||
		status.DroppedRecords != 1 ||
		status.LossGeneration != 1 ||
		status.QueueDepth != 2 {
		t.Fatalf("unexpected overflow status: %#v %#v", third, status)
	}
	lease, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if lease == nil || lease.Sequence != first.Sequence+1 {
		t.Fatalf("oldest pending sequence was not retired: %#v", lease)
	}
}

func TestHistorySpoolDropsOversizeIncoming(t *testing.T) {
	options := historySpoolTestOptions(t, 1)
	spool := openHistorySpoolForTest(t, options)
	defer spool.Close()
	boundary, err := spool.Append(historySpoolTestBatch(1))
	if err != nil {
		t.Fatal(err)
	}
	status := spool.Status()
	if boundary.Durable ||
		!boundary.LossDuringWrite ||
		boundary.Sequence != 0 ||
		status.QueueDepth != 0 ||
		status.DroppedBatches != 1 ||
		status.LossGeneration != 1 {
		t.Fatalf("oversize append was not reported as loss: %#v %#v", boundary, status)
	}
}

func TestHistorySpoolProtectsInflightFromOverflow(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool := openHistorySpoolForTest(t, options)
	defer spool.Close()
	if _, err := spool.Append(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	lease, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	status := spool.Status()
	spool.maxSize = status.QueueBytes
	boundary, err := spool.Append(historySpoolTestBatch(2))
	if err != nil {
		t.Fatal(err)
	}
	next, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if boundary.Durable ||
		!boundary.LossDuringWrite ||
		next == nil ||
		next.BatchID != lease.BatchID ||
		spool.Status().QueueDepth != 1 {
		t.Fatalf("inflight batch was not protected: %#v %#v", boundary, next)
	}
}

func TestHistorySpoolLoweredCapacityDropsAtOpen(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool := openHistorySpoolForTest(t, options)
	for index := uint64(1); index <= 3; index++ {
		if _, err := spool.Append(historySpoolTestBatch(index)); err != nil {
			t.Fatal(err)
		}
	}
	oneBatchSize := spool.Status().QueueBytes / 3
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	options.MaxSize = oneBatchSize
	spool = openHistorySpoolForTest(t, options)
	defer spool.Close()
	status := spool.Status()
	if status.QueueDepth != 1 ||
		status.DroppedBatches != 2 ||
		status.LossGeneration != 1 {
		t.Fatalf("lowered capacity not applied: %#v", status)
	}
	lease, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if lease == nil || lease.Sequence != 3 {
		t.Fatalf("wrong batch survived lowered capacity: %#v", lease)
	}
}

func TestHistorySpoolLoweredCapacityPreservesRecoveredInflight(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool := openHistorySpoolForTest(t, options)
	if _, err := spool.Append(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	recovered, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if recovered == nil {
		t.Fatal("missing inflight batch")
	}
	for sequence := uint64(2); sequence <= 3; sequence++ {
		if _, err = spool.Append(historySpoolTestBatch(sequence)); err != nil {
			t.Fatal(err)
		}
	}
	before := spool.Status()
	oneBatchSize := historySpoolLogicalSize(recovered.Encoded)
	if err = spool.Close(); err != nil {
		t.Fatal(err)
	}

	options.MaxSize = oneBatchSize - 1
	spool = openHistorySpoolForTest(t, options)
	defer spool.Close()
	after := spool.Status()
	t.Logf("before reopen: %#v", before)
	t.Logf("after reopen: %#v", after)
	t.Logf(
		"before restart sequence=%d batch_id=%s payload_sha256=%x envelope_sha256=%x logical_size=%d max_size=%d",
		recovered.Sequence,
		recovered.BatchID,
		recovered.PayloadSHA256,
		recovered.EnvelopeSHA256,
		oneBatchSize,
		options.MaxSize,
	)
	if after.QueueDepth != 1 ||
		after.QueueRecords != uint64(recovered.RecordCount) ||
		after.QueueBytes != oneBatchSize ||
		after.DroppedBatches != 2 ||
		after.DroppedRecords != 2 ||
		after.DroppedBytes != before.QueueBytes-oneBatchSize ||
		after.LossGeneration != 1 ||
		after.QueueBytes <= options.MaxSize {
		t.Fatalf("unexpected recovered queue: %#v", after)
	}
	leased, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if leased == nil ||
		leased.Sequence != recovered.Sequence ||
		leased.BatchID != recovered.BatchID ||
		leased.PayloadSHA256 != recovered.PayloadSHA256 ||
		leased.EnvelopeSHA256 != recovered.EnvelopeSHA256 ||
		!bytes.Equal(leased.Encoded, recovered.Encoded) {
		t.Fatalf("recovered inflight batch changed: %#v %#v", recovered, leased)
	}
	t.Logf(
		"after restart sequence=%d batch_id=%s payload_sha256=%x envelope_sha256=%x logical_size=%d",
		leased.Sequence,
		leased.BatchID,
		leased.PayloadSHA256,
		leased.EnvelopeSHA256,
		historySpoolLogicalSize(leased.Encoded),
	)
	var retired []uint64
	if err = spool.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(historySpoolRetiredBucket).ForEach(
			func(key []byte, value []byte) error {
				retired = append(retired, binary.BigEndian.Uint64(key))
				return nil
			},
		)
	}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(retired, []uint64{2, 3}) {
		t.Fatalf("retired sequences = %v, want [2 3]", retired)
	}
	t.Logf("retained sequence=%d retired sequences=%v", leased.Sequence, retired)
}

func TestHistorySpoolDropsOversizeBeforeEnvelopeAllocation(t *testing.T) {
	var allocations atomic.Uint64
	options := historySpoolTestOptions(t, 1)
	options.Faults = &historySpoolFaults{
		BeforeEnvelopeAllocation: func() error {
			allocations.Add(1)
			return nil
		},
	}
	spool := openHistorySpoolForTest(t, options)
	defer spool.Close()
	boundary, err := spool.Append(historySpoolTestBatch(1))
	if err != nil {
		t.Fatal(err)
	}
	status := spool.Status()
	if allocations.Load() != 0 {
		t.Fatalf("oversize append entered envelope allocation %d times", allocations.Load())
	}
	t.Logf(
		"oversize result: allocation_hook=%d next_sequence=%d dropped_batches=%d dropped_bytes=%d",
		allocations.Load(),
		status.NextSequence,
		status.DroppedBatches,
		status.DroppedBytes,
	)
	if boundary.Durable ||
		boundary.Sequence != 0 ||
		status.NextSequence != 1 ||
		status.QueueDepth != 0 ||
		status.DroppedBatches != 1 {
		t.Fatalf("unexpected oversize result: %#v %#v", boundary, status)
	}
	spool.maxSize = 1 << 20
	if _, err = spool.Append(historySpoolTestBatch(2)); err != nil {
		t.Fatal(err)
	}
	if allocations.Load() != 1 {
		t.Fatalf("allocation observer is inactive: %d", allocations.Load())
	}
}

func TestHistorySpoolLogicalCapacityIncludesSequenceKey(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool := openHistorySpoolForTest(t, options)
	if _, err := spool.Append(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	status := spool.Status()
	envelope, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if envelope == nil {
		t.Fatal("missing lease")
	}
	if status.QueueBytes != uint64(len(envelope.Encoded))+8 {
		t.Fatalf(
			"queue bytes = %d, want sequence key plus envelope = %d",
			status.QueueBytes,
			len(envelope.Encoded)+8,
		)
	}
	if err = spool.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHistorySpoolLogicalCapacityExactBoundary(t *testing.T) {
	probeOptions := historySpoolTestOptions(t, 1<<20)
	probe := openHistorySpoolForTest(t, probeOptions)
	if _, err := probe.Append(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	logicalSize := probe.Status().QueueBytes
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}

	for _, testCase := range []struct {
		name    string
		maxSize uint64
		durable bool
	}{
		{"max_minus_one", logicalSize - 1, false},
		{"max", logicalSize, true},
		{"max_plus_one", logicalSize + 1, true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			options := historySpoolTestOptions(t, testCase.maxSize)
			spool := openHistorySpoolForTest(t, options)
			defer spool.Close()
			boundary, err := spool.Append(historySpoolTestBatch(1))
			if err != nil {
				t.Fatal(err)
			}
			t.Logf(
				"logical_size=%d max_size=%d durable=%v",
				logicalSize,
				testCase.maxSize,
				boundary.Durable,
			)
			if boundary.Durable != testCase.durable {
				t.Fatalf("boundary = %#v", boundary)
			}
		})
	}
}

func TestHistorySpoolLoweredCapacityInspectsOversizePendingSafely(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool := openHistorySpoolForTest(t, options)
	if _, err := spool.Append(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	logicalSize := spool.Status().QueueBytes
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}

	var materializations atomic.Uint64
	options.MaxSize = logicalSize - 1
	options.Faults = &historySpoolFaults{
		BeforeEnvelopeMaterialization: func() error {
			materializations.Add(1)
			return nil
		},
	}
	spool = openHistorySpoolForTest(t, options)
	defer spool.Close()
	if materializations.Load() != 0 {
		t.Fatalf(
			"dropped pending envelope was materialized %d times",
			materializations.Load(),
		)
	}
	status := spool.Status()
	if status.QueueDepth != 0 ||
		status.DroppedBatches != 1 ||
		status.LossGeneration != 1 {
		t.Fatalf("unexpected lowered-capacity status: %#v", status)
	}
}

func TestHistorySpoolClassifiesMalformedDatabase(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	if err := os.WriteFile(options.Path, []byte("not bolt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(options.Path, 0o600); err != nil {
		t.Fatal(err)
	}
	_, _, err := openHistorySpool(options)
	t.Logf("malformed database error: %v", err)
	if !errors.Is(err, ErrSpoolCorrupt) {
		t.Fatalf("malformed database: %v", err)
	}
}

func TestHistorySpoolClassifiesOpenPermissionFailure(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	options.Faults = &historySpoolFaults{
		OpenDatabase: func() error {
			return &os.PathError{
				Op:   "open",
				Path: options.Path,
				Err:  os.ErrPermission,
			}
		},
	}
	_, _, err := openHistorySpool(options)
	t.Logf("open permission error: %v", err)
	if !errors.Is(err, ErrSpoolIO) ||
		errors.Is(err, ErrSpoolCorrupt) {
		t.Fatalf("permission failure: %v", err)
	}
}

func TestHistorySpoolClassifiesTransactionCommitFailure(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool := openHistorySpoolForTest(t, options)
	spool.faults = &historySpoolFaults{
		TransactionWrite: func(operation string) error {
			if operation == "append" {
				return errors.New("injected transaction write failure")
			}
			return nil
		},
	}
	_, err := spool.Append(historySpoolTestBatch(1))
	t.Logf("transaction commit error: %v", err)
	if !errors.Is(
		err,
		ErrSpoolIO,
	) {
		t.Fatalf("transaction failure: %v", err)
	}
	spool.faults = nil
	status := spool.Status()
	if status.NextSequence != 1 ||
		status.QueueDepth != 0 ||
		status.DroppedBatches != 0 {
		t.Fatalf("failed transaction changed queue: %#v", status)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHistorySpoolClassifiesViewFailure(t *testing.T) {
	spool := openHistorySpoolForTest(t, historySpoolTestOptions(t, 1<<20))
	spool.faults = &historySpoolFaults{
		View: func(operation string) error {
			if operation == "status" {
				return errors.New("injected view failure")
			}
			return nil
		},
	}
	_, err := spool.readStatus()
	t.Logf("view error: %v", err)
	if !errors.Is(err, ErrSpoolIO) ||
		errors.Is(err, ErrSpoolCorrupt) {
		t.Fatalf("view failure: %v", err)
	}
	spool.faults = nil
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHistorySpoolLockClassification(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	first := openHistorySpoolForTest(t, options)
	defer first.Close()
	if _, _, err := openHistorySpool(options); !errors.Is(err, ErrSpoolLocked) ||
		errors.Is(err, ErrSpoolIO) ||
		errors.Is(err, ErrSpoolCorrupt) {
		t.Fatalf("lock failure: %v", err)
	}
}

func TestHistorySpoolRecoversInflightAfterRestart(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool := openHistorySpoolForTest(t, options)
	if _, err := spool.Append(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	first, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if err = spool.Close(); err != nil {
		t.Fatal(err)
	}
	spool = openHistorySpoolForTest(t, options)
	defer spool.Close()
	if spool.Status().InFlight {
		t.Fatal("startup did not return inflight batch to pending")
	}
	second, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if second == nil ||
		first.Sequence != second.Sequence ||
		first.BatchID != second.BatchID ||
		!bytes.Equal(first.Encoded, second.Encoded) {
		t.Fatalf("recovered lease changed: %#v %#v", first, second)
	}
}

func TestHistorySpoolSequenceExhaustion(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool := openHistorySpoolForTest(t, options)
	defer spool.Close()
	if err := spool.db.Update(func(tx *bbolt.Tx) error {
		metadata := tx.Bucket(historySpoolMetadataBucket)
		if err := metadata.Put(
			historySpoolResolvedSequenceKey,
			encodeHistorySpoolUint64(^uint64(0)-1),
		); err != nil {
			return err
		}
		return metadata.Put(
			historySpoolNextSequenceKey,
			encodeHistorySpoolUint64(^uint64(0)),
		)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := spool.Append(historySpoolTestBatch(1)); !errors.Is(
		err,
		ErrSpoolSequenceExhausted,
	) {
		t.Fatalf("sequence exhaustion: %v", err)
	}
}

func TestHistorySpoolConcurrentAppend(t *testing.T) {
	spool := openHistorySpoolForTest(t, historySpoolTestOptions(t, 1<<24))
	defer spool.Close()
	const workers = 12
	const perWorker = 25
	var wait sync.WaitGroup
	sequences := make(chan uint64, workers*perWorker)
	errorsSeen := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for index := 0; index < perWorker; index++ {
				boundary, err := spool.Append(
					historySpoolTestBatch(uint64(worker*perWorker + index + 1)),
				)
				if err != nil {
					errorsSeen <- err
					return
				}
				sequences <- boundary.Sequence
			}
		}(worker)
	}
	wait.Wait()
	close(sequences)
	close(errorsSeen)
	for err := range errorsSeen {
		t.Fatal(err)
	}
	got := make([]uint64, 0, workers*perWorker)
	for sequence := range sequences {
		got = append(got, sequence)
	}
	slices.Sort(got)
	for index, sequence := range got {
		if sequence != uint64(index+1) {
			t.Fatalf("sequence[%d] = %d", index, sequence)
		}
	}
	status := spool.Status()
	if status.QueueDepth != workers*perWorker ||
		status.QueueRecords != workers*perWorker {
		t.Fatalf("unexpected concurrent status: %#v", status)
	}
}

func TestHistorySpoolDoesNotPersistSecrets(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	dsnSecret := []byte("postgres://user:unique-password@db.example/traffic")
	revisionSecret := append([]byte(nil), options.Identity.RevisionKey...)
	spool := openHistorySpoolForTest(t, options)
	if _, err := spool.Append(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(options.Path)
	if err != nil {
		t.Fatal(err)
	}
	for name, secret := range map[string][]byte{
		"DSN":          dsnSecret,
		"revision key": revisionSecret,
	} {
		if bytes.Contains(raw, secret) {
			t.Fatalf("spool persisted %s", name)
		}
	}
	if !bytes.Contains(raw, []byte(options.Identity.InstanceID)) {
		t.Fatal("spool does not contain its non-secret instance identity")
	}
}

func historySpoolTestOptions(t *testing.T, maxSize uint64) historySpoolOptions {
	t.Helper()
	return historySpoolOptions{
		Context:            context.Background(),
		Path:               filepath.Join(t.TempDir(), "traffic-spool.db"),
		MaxSize:            maxSize,
		Identity:           historySpoolTestOptionsIdentity(),
		RoutingFingerprint: historySpoolTestOptionsFingerprint(),
		ConfigRevision:     historySpoolTestRevision,
		Now:                historySpoolTestNow,
	}
}

func historySpoolTestOptionsIdentity() trafficIdentity {
	return trafficIdentity{
		Version:     trafficIdentityVersion,
		InstanceID:  "9e43a9d3-63c4-4d8b-a8a0-16b7d12c792b",
		RevisionKey: bytes.Repeat([]byte{0x6d}, sha256.Size),
	}
}

func historySpoolTestOptionsFingerprint() [sha256.Size]byte {
	return sha256.Sum256([]byte("canonical routing"))
}

func historySpoolTestNow() time.Time {
	return time.Date(2026, 7, 29, 12, 34, 56, 789, time.UTC)
}

func historySpoolTestKey(domain string) historyKey {
	return historyKey{
		Bucket:             120,
		ConfigRevision:     historySpoolTestRevision,
		RouteTag:           "route",
		GroupPath:          `["group"]`,
		DestinationDomain:  domain,
		ActualOutboundTag:  "proxy",
		ActualOutboundType: "mieru",
		Network:            "tcp",
	}
}

func historySpoolTestBatch(value uint64) historyBatch {
	key := historySpoolTestKey("batch.example")
	key.RouteTag = string(rune('a' + value%26))
	return historyBatch{
		key: {
			UplinkBytes:   value,
			DownlinkBytes: value + 1,
			Connections:   1,
		},
	}
}

func openHistorySpoolForTest(
	t *testing.T,
	options historySpoolOptions,
) *historySpool {
	t.Helper()
	spool, _, err := openHistorySpool(options)
	if err != nil {
		t.Fatal(err)
	}
	return spool
}

func mutateHistorySpool(
	t *testing.T,
	path string,
	mutate func(*bbolt.Tx) error,
) {
	t.Helper()
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Update(mutate); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
}

func readHistorySpoolDiskStateForTest(
	t *testing.T,
	path string,
) (historySpoolStatus, [sha256.Size]byte) {
	t.Helper()
	database, err := bbolt.Open(path, 0o600, &bbolt.Options{
		ReadOnly: true,
		Timeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	var status historySpoolStatus
	var revisionHash [sha256.Size]byte
	if err = database.View(func(tx *bbolt.Tx) error {
		var viewErr error
		status, viewErr = validateHistorySpoolTransaction(
			tx,
			historySpoolSafeLogicalSize,
			true,
			nil,
		)
		if viewErr != nil {
			return viewErr
		}
		copy(
			revisionHash[:],
			tx.Bucket(historySpoolMetadataBucket).Get(historySpoolRevisionHashKey),
		)
		return nil
	}); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	return status, revisionHash
}

func readHistorySpoolStateFromOpenForTest(
	t *testing.T,
	spool *historySpool,
) (historySpoolStatus, [sha256.Size]byte) {
	t.Helper()
	var status historySpoolStatus
	var revisionHash [sha256.Size]byte
	if err := spool.db.View(func(tx *bbolt.Tx) error {
		var viewErr error
		status, viewErr = validateHistorySpoolTransaction(
			tx,
			historySpoolSafeLogicalSize,
			true,
			nil,
		)
		if viewErr != nil {
			return viewErr
		}
		copy(
			revisionHash[:],
			tx.Bucket(historySpoolMetadataBucket).Get(historySpoolRevisionHashKey),
		)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return status, revisionHash
}

func assertEmptyHistorySpoolStatus(
	t *testing.T,
	status historySpoolStatus,
) {
	t.Helper()
	if status.SpoolID == uuid.Nil ||
		status.NextSequence != 1 ||
		status.ResolvedSequence != 0 ||
		status.QueueDepth != 0 ||
		status.QueueRecords != 0 ||
		status.QueueBytes != 0 ||
		status.InFlight ||
		status.DroppedBatches != 0 ||
		status.DroppedRecords != 0 ||
		status.DroppedBytes != 0 ||
		status.LossGeneration != 0 {
		t.Fatalf("unexpected nonempty spool status: %#v", status)
	}
}

func assertHistorySpoolFormat(t *testing.T, spool *historySpool) {
	t.Helper()
	wantBuckets := [][]byte{
		historySpoolInflightBucket,
		historySpoolMetadataBucket,
		historySpoolPendingBucket,
		historySpoolRetiredBucket,
	}
	var gotBuckets [][]byte
	var gotMetadata [][]byte
	err := spool.db.View(func(tx *bbolt.Tx) error {
		err := tx.ForEach(func(name []byte, bucket *bbolt.Bucket) error {
			gotBuckets = append(gotBuckets, append([]byte(nil), name...))
			return nil
		})
		if err != nil {
			return err
		}
		return tx.Bucket(historySpoolMetadataBucket).ForEach(
			func(key []byte, value []byte) error {
				if value != nil {
					gotMetadata = append(gotMetadata, append([]byte(nil), key...))
				}
				return nil
			},
		)
	})
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(gotBuckets, bytes.Compare)
	slices.SortFunc(wantBuckets, bytes.Compare)
	if !slices.EqualFunc(gotBuckets, wantBuckets, bytes.Equal) {
		t.Fatalf("buckets = %q, want %q", gotBuckets, wantBuckets)
	}
	wantMetadata := append([][]byte(nil), historySpoolMetadataKeys...)
	slices.SortFunc(gotMetadata, bytes.Compare)
	slices.SortFunc(wantMetadata, bytes.Compare)
	if !slices.EqualFunc(gotMetadata, wantMetadata, bytes.Equal) {
		t.Fatalf("metadata = %q, want %q", gotMetadata, wantMetadata)
	}
}

func historySpoolUint32(value uint32) []byte {
	encoded := make([]byte, 4)
	binary.BigEndian.PutUint32(encoded, value)
	return encoded
}

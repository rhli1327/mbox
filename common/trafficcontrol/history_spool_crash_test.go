package trafficcontrol

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"sync/atomic"
	"testing"
)

func TestHistorySpoolCrashRecovery(t *testing.T) {
	if os.Getenv("MBOX_HISTORY_SPOOL_CRASH_CHILD") == "1" {
		options := historySpoolTestOptionsFromEnvironment()
		spool, _, err := openHistorySpool(options)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = spool.Append(historySpoolTestBatch(7)); err != nil {
			t.Fatal(err)
		}
		if _, err = spool.Lease(); err != nil {
			t.Fatal(err)
		}
		os.Exit(86)
	}

	options := historySpoolTestOptions(t, 1<<20)
	command := exec.Command(os.Args[0], "-test.run=^TestHistorySpoolCrashRecovery$")
	command.Env = append(
		os.Environ(),
		"MBOX_HISTORY_SPOOL_CRASH_CHILD=1",
		"MBOX_HISTORY_SPOOL_CRASH_PATH="+options.Path,
	)
	err := command.Run()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 86 {
		t.Fatalf("child exit = %v, want 86", err)
	}

	spool := openHistorySpoolForTest(t, options)
	defer spool.Close()
	envelope, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if envelope == nil ||
		envelope.Sequence != 1 ||
		envelope.BatchID != historyBatchID(spool.Status().SpoolID, 1) {
		t.Fatalf("crash recovery lost stable batch: %#v", envelope)
	}
}

func TestHistorySpoolCrashBeforeAppendCommit(t *testing.T) {
	if historySpoolRunCrashChild(t, "before_append_commit") {
		return
	}
	options := historySpoolTestOptions(t, 1<<20)
	historySpoolRunCrashProcess(t, options, "before_append_commit")
	spool := openHistorySpoolForTest(t, options)
	defer spool.Close()
	status := spool.Status()
	t.Logf("restart metadata: %#v", status)
	if status.NextSequence != 1 ||
		status.ResolvedSequence != 0 ||
		status.QueueDepth != 0 ||
		status.DroppedBatches != 0 ||
		status.LossGeneration != 0 {
		t.Fatalf("pre-commit crash changed queue: %#v", status)
	}
}

func TestHistorySpoolCrashAfterAppendCommit(t *testing.T) {
	if historySpoolRunCrashChild(t, "after_append_commit") {
		return
	}
	options := historySpoolTestOptions(t, 1<<20)
	historySpoolRunCrashProcess(t, options, "after_append_commit")
	spool := openHistorySpoolForTest(t, options)
	defer spool.Close()
	status := spool.Status()
	envelope, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("restart metadata: %#v", status)
	if status.NextSequence != 2 ||
		status.QueueDepth != 1 ||
		envelope == nil ||
		envelope.Sequence != 1 ||
		envelope.BatchID != historyBatchID(status.SpoolID, 1) {
		t.Fatalf("post-commit crash lost append: %#v %#v", status, envelope)
	}
}

func TestHistorySpoolCrashDuringOverflow(t *testing.T) {
	for _, point := range []string{
		"before_overflow_commit",
		"after_overflow_commit",
	} {
		t.Run(point, func(t *testing.T) {
			if historySpoolRunCrashChild(t, point) {
				return
			}
			options := historySpoolTestOptions(t, 1<<20)
			spool := openHistorySpoolForTest(t, options)
			if _, err := spool.Append(historySpoolTestBatch(1)); err != nil {
				t.Fatal(err)
			}
			original, err := spool.Lease()
			if err != nil {
				t.Fatal(err)
			}
			if err = spool.Close(); err != nil {
				t.Fatal(err)
			}
			options.MaxSize = historySpoolLogicalSize(original.Encoded)
			historySpoolRunCrashProcess(t, options, point)

			spool = openHistorySpoolForTest(t, options)
			defer spool.Close()
			status := spool.Status()
			envelope, err := spool.Lease()
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%s restart metadata: %#v", point, status)
			if point == "before_overflow_commit" {
				if status.NextSequence != 2 ||
					status.QueueDepth != 1 ||
					status.DroppedBatches != 0 ||
					status.LossGeneration != 0 ||
					envelope == nil ||
					envelope.Sequence != 1 ||
					!bytes.Equal(envelope.Encoded, original.Encoded) {
					t.Fatalf("overflow rollback was partial: %#v %#v", status, envelope)
				}
			} else if status.NextSequence != 3 ||
				status.QueueDepth != 1 ||
				status.DroppedBatches != 1 ||
				status.LossGeneration != 1 ||
				envelope == nil ||
				envelope.Sequence != 2 {
				t.Fatalf("overflow commit was partial: %#v %#v", status, envelope)
			}
		})
	}
}

func TestHistorySpoolCrashAfterLeaseCommit(t *testing.T) {
	if historySpoolRunCrashChild(t, "after_lease_commit") {
		return
	}
	options := historySpoolTestOptions(t, 1<<20)
	historySpoolRunCrashProcess(t, options, "after_lease_commit")
	spool := openHistorySpoolForTest(t, options)
	defer spool.Close()
	envelope, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	if envelope == nil ||
		envelope.Sequence != 1 ||
		envelope.BatchID != historyBatchID(spool.Status().SpoolID, 1) {
		t.Fatalf("leased batch did not recover: %#v", envelope)
	}
	t.Logf(
		"recovered lease sequence=%d batch_id=%s payload_sha256=%x envelope_sha256=%x",
		envelope.Sequence,
		envelope.BatchID,
		envelope.PayloadSHA256,
		envelope.EnvelopeSHA256,
	)
}

func TestHistorySpoolCrashBeforeAcknowledgeCommit(t *testing.T) {
	if historySpoolRunCrashChild(t, "before_ack_commit") {
		return
	}
	options := historySpoolTestOptions(t, 1<<20)
	historySpoolRunCrashProcess(t, options, "before_ack_commit")
	spool := openHistorySpoolForTest(t, options)
	defer spool.Close()
	status := spool.Status()
	envelope, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("restart metadata: %#v", status)
	if status.ResolvedSequence != 0 ||
		status.QueueDepth != 1 ||
		!status.LastSuccessfulFlush.IsZero() ||
		envelope == nil ||
		envelope.Sequence != 1 {
		t.Fatalf("pre-ack crash changed state: %#v %#v", status, envelope)
	}
}

func TestHistorySpoolCrashAfterAcknowledgeCommit(t *testing.T) {
	if historySpoolRunCrashChild(t, "after_ack_commit") {
		return
	}
	options := historySpoolTestOptions(t, 1<<20)
	historySpoolRunCrashProcess(t, options, "after_ack_commit")
	spool := openHistorySpoolForTest(t, options)
	defer spool.Close()
	status := spool.Status()
	envelope, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("restart metadata: %#v", status)
	if status.ResolvedSequence != 1 ||
		status.QueueDepth != 0 ||
		!status.LastSuccessfulFlush.Equal(historySpoolTestNow()) ||
		envelope != nil {
		t.Fatalf("post-ack crash lost commit: %#v %#v", status, envelope)
	}
}

func TestHistorySpoolInitialSyncFailures(t *testing.T) {
	t.Run("file_sync", func(t *testing.T) {
		options := historySpoolTestOptions(t, 1<<20)
		var fileSyncCalls atomic.Uint64
		var parentSyncCalls atomic.Uint64
		options.Faults = &historySpoolFaults{
			FileSync: func() error {
				fileSyncCalls.Add(1)
				return errors.New("injected file sync failure")
			},
			ParentSync: func() error {
				parentSyncCalls.Add(1)
				return nil
			},
		}
		_, _, err := openHistorySpool(options)
		t.Logf(
			"first file-sync open: error=%v file_sync_calls=%d parent_sync_calls=%d",
			err,
			fileSyncCalls.Load(),
			parentSyncCalls.Load(),
		)
		if !errors.Is(err, ErrSpoolIO) ||
			fileSyncCalls.Load() != 1 ||
			parentSyncCalls.Load() != 0 {
			t.Fatalf("initial file sync failure: %v", err)
		}

		options.Faults = &historySpoolFaults{
			ParentSync: func() error {
				parentSyncCalls.Add(1)
				return nil
			},
		}
		spool, _, err := openHistorySpool(options)
		if err != nil {
			t.Fatalf("reopen after file sync failure: %v", err)
		}
		status := spool.Status()
		t.Logf(
			"file-sync retry: parent_sync_calls=%d status=%#v",
			parentSyncCalls.Load(),
			status,
		)
		if parentSyncCalls.Load() != 1 {
			t.Fatalf("parent sync retry count = %d, want 1", parentSyncCalls.Load())
		}
		assertEmptyHistorySpoolStatus(t, status)
		assertHistorySpoolFormat(t, spool)
		if err = spool.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("parent_sync", func(t *testing.T) {
		options := historySpoolTestOptions(t, 1<<20)
		var parentSyncCalls atomic.Uint64
		options.Faults = &historySpoolFaults{
			ParentSync: func() error {
				if parentSyncCalls.Add(1) == 1 {
					return errors.New("injected parent sync failure")
				}
				return nil
			},
		}
		_, _, err := openHistorySpool(options)
		t.Logf(
			"first parent-sync open: error=%v parent_sync_calls=%d",
			err,
			parentSyncCalls.Load(),
		)
		if !errors.Is(err, ErrSpoolIO) || parentSyncCalls.Load() != 1 {
			t.Fatalf("initial parent sync failure: %v", err)
		}
		before, beforeRevisionHash := readHistorySpoolDiskStateForTest(
			t,
			options.Path,
		)

		spool, _, err := openHistorySpool(options)
		if err != nil {
			t.Fatalf("reopen after parent sync failure: %v", err)
		}
		after := spool.Status()
		_, afterRevisionHash := readHistorySpoolStateFromOpenForTest(t, spool)
		t.Logf(
			"parent-sync retry: parent_sync_calls=%d before=%#v after=%#v",
			parentSyncCalls.Load(),
			before,
			after,
		)
		if parentSyncCalls.Load() != 2 {
			t.Fatalf("parent sync retry count = %d, want 2", parentSyncCalls.Load())
		}
		if before != after || beforeRevisionHash != afterRevisionHash {
			t.Fatalf("parent sync retry regenerated spool: %#v %#v", before, after)
		}
		assertEmptyHistorySpoolStatus(t, after)
		if err = spool.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestHistorySpoolCrashBeforeInitialParentSync(t *testing.T) {
	if historySpoolRunCrashChild(t, "before_initial_parent_sync") {
		return
	}
	options := historySpoolTestOptions(t, 1<<20)
	historySpoolRunCrashProcess(t, options, "before_initial_parent_sync")
	before, beforeRevisionHash := readHistorySpoolDiskStateForTest(t, options.Path)

	var parentSyncCalls atomic.Uint64
	options.Faults = &historySpoolFaults{
		ParentSync: func() error {
			parentSyncCalls.Add(1)
			return nil
		},
	}
	spool := openHistorySpoolForTest(t, options)
	after := spool.Status()
	_, afterRevisionHash := readHistorySpoolStateFromOpenForTest(t, spool)
	t.Logf(
		"crash-before-parent-sync: exit=86 parent_sync_calls=%d before=%#v after=%#v",
		parentSyncCalls.Load(),
		before,
		after,
	)
	if parentSyncCalls.Load() != 1 {
		t.Fatalf("parent sync recovery count = %d, want 1", parentSyncCalls.Load())
	}
	if before != after || beforeRevisionHash != afterRevisionHash {
		t.Fatalf("crash recovery changed committed V1 spool: %#v %#v", before, after)
	}
	assertEmptyHistorySpoolStatus(t, after)
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}

	options.Faults = nil
	spool = openHistorySpoolForTest(t, options)
	assertHistorySpoolFormat(t, spool)
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHistorySpoolTransactionWriteFailure(t *testing.T) {
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
	if _, err := spool.Append(historySpoolTestBatch(1)); !errors.Is(
		err,
		ErrSpoolIO,
	) {
		t.Fatalf("transaction write failure: %v", err)
	}
	spool.faults = nil
	status := spool.Status()
	t.Logf("metadata after failed write: %#v", status)
	if status.NextSequence != 1 ||
		status.QueueDepth != 0 ||
		status.DroppedBatches != 0 ||
		status.LossGeneration != 0 {
		t.Fatalf("failed write changed queue: %#v", status)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}
}

func historySpoolRunCrashChild(t *testing.T, expectedPoint string) bool {
	t.Helper()
	if os.Getenv("MBOX_HISTORY_SPOOL_FAULT_POINT") != expectedPoint {
		return false
	}
	options := historySpoolTestOptionsFromEnvironment()
	exit := func() error {
		os.Exit(86)
		return nil
	}
	options.Faults = &historySpoolFaults{}
	switch expectedPoint {
	case "before_append_commit":
		options.Faults.BeforeAppendCommit = exit
	case "after_append_commit":
		options.Faults.AfterAppendCommit = exit
	case "before_overflow_commit":
		options.Faults.BeforeOverflowCommit = exit
	case "after_overflow_commit":
		options.Faults.AfterAppendCommit = exit
	case "after_lease_commit":
		options.Faults.AfterLeaseCommit = exit
	case "before_ack_commit":
		options.Faults.BeforeAcknowledgeCommit = exit
	case "after_ack_commit":
		options.Faults.AfterAcknowledgeCommit = exit
	case "before_initial_parent_sync":
		options.Faults.ParentSync = exit
	default:
		t.Fatalf("unknown fault point %q", expectedPoint)
	}
	spool, _, err := openHistorySpool(options)
	if err != nil {
		t.Fatal(err)
	}
	switch expectedPoint {
	case "before_append_commit", "after_append_commit":
		_, err = spool.Append(historySpoolTestBatch(1))
	case "before_overflow_commit", "after_overflow_commit":
		_, err = spool.Append(historySpoolTestBatch(2))
	case "after_lease_commit":
		if _, err = spool.Append(historySpoolTestBatch(1)); err == nil {
			_, err = spool.Lease()
		}
	case "before_ack_commit", "after_ack_commit":
		if _, err = spool.Append(historySpoolTestBatch(1)); err == nil {
			var envelope *historyBatchEnvelope
			envelope, err = spool.Lease()
			if err == nil {
				err = spool.Acknowledge(envelope.Sequence, historySpoolTestNow())
			}
		}
	case "before_initial_parent_sync":
		t.Fatal("parent sync crash hook did not exit")
	}
	t.Fatalf("fault hook did not exit: %v", err)
	return true
}

func historySpoolRunCrashProcess(
	t *testing.T,
	options historySpoolOptions,
	point string,
) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	command.Env = append(
		os.Environ(),
		"MBOX_HISTORY_SPOOL_FAULT_POINT="+point,
		"MBOX_HISTORY_SPOOL_CRASH_PATH="+options.Path,
		"MBOX_HISTORY_SPOOL_CRASH_MAX_SIZE="+strconv.FormatUint(options.MaxSize, 10),
	)
	err := command.Run()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 86 {
		t.Fatalf("%s child exit = %v, want 86", point, err)
	}
}

func historySpoolTestOptionsFromEnvironment() historySpoolOptions {
	maxSize := uint64(1 << 20)
	if value := os.Getenv("MBOX_HISTORY_SPOOL_CRASH_MAX_SIZE"); value != "" {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			panic(err)
		}
		maxSize = parsed
	}
	options := historySpoolOptions{
		Context:            context.Background(),
		Path:               os.Getenv("MBOX_HISTORY_SPOOL_CRASH_PATH"),
		MaxSize:            maxSize,
		Identity:           historySpoolTestOptionsIdentity(),
		RoutingFingerprint: historySpoolTestOptionsFingerprint(),
		ConfigRevision:     historySpoolTestRevision,
		Now:                historySpoolTestNow,
	}
	return options
}

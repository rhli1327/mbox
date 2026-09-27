package trafficcontrol

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/sagernet/bbolt"
)

func TestHistorySpoolStatusRejectsMultipleInflightBatches(t *testing.T) {
	spool := openHistorySpoolForTest(t, historySpoolTestOptions(t, 1<<20))
	defer spool.Close()
	if _, err := spool.Append(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := spool.Lease(); err != nil {
		t.Fatal(err)
	}
	if err := spool.db.Update(func(tx *bbolt.Tx) error {
		inflight := tx.Bucket(historySpoolInflightBucket)
		_, value := inflight.Cursor().First()
		return inflight.Put(encodeHistorySpoolUint64(2), append([]byte(nil), value...))
	}); err != nil {
		t.Fatal(err)
	}
	status, err := spool.readStatus()
	if !errors.Is(err, ErrSpoolCorrupt) || status.FormatVersion != 0 {
		t.Fatalf("invalid progress returned a usable status: %#v, error=%v", status, err)
	}
	if status = spool.Status(); status.FormatVersion != 0 {
		t.Fatalf("Status ignored invalid progress: %#v", status)
	}
}

func TestHistorySpoolStatusDoesNotDecodeBacklog(t *testing.T) {
	spool := openHistorySpoolForTest(t, historySpoolTestOptions(t, 1<<20))
	defer spool.Close()
	for sequence := uint64(1); sequence <= 64; sequence++ {
		if _, err := spool.Append(historySpoolTestBatch(sequence)); err != nil {
			t.Fatal(err)
		}
	}
	materializations := 0
	spool.faults = &historySpoolFaults{
		BeforeEnvelopeMaterialization: func() error {
			materializations++
			return nil
		},
	}
	before, err := spool.readStatus()
	if err != nil {
		t.Fatal(err)
	}
	if materializations != 0 {
		t.Fatalf("status decoded %d queued batches", materializations)
	}
	if before.QueueDepth != 64 || before.QueueRecords != 64 || before.InFlight {
		t.Fatalf("unexpected queued status: %#v", before)
	}
	envelope, err := spool.Lease()
	if err != nil {
		t.Fatal(err)
	}
	inflight := spool.Status()
	if !inflight.InFlight || inflight.QueueDepth != before.QueueDepth ||
		inflight.QueueBytes != before.QueueBytes || materializations != 1 {
		t.Fatalf("lease/status did not validate only the delivered batch: %#v, decodes=%d", inflight, materializations)
	}
	if err = spool.Acknowledge(envelope.Sequence, historySpoolTestNow()); err != nil {
		t.Fatal(err)
	}
	after := spool.Status()
	if after.InFlight || after.QueueDepth != 63 || after.QueueRecords != 63 ||
		after.ResolvedSequence != envelope.Sequence || after.QueueBytes >= before.QueueBytes ||
		materializations != 2 {
		t.Fatalf("unexpected acknowledged status: %#v, decodes=%d", after, materializations)
	}
}

func BenchmarkHistorySpoolStatusBacklog(b *testing.B) {
	for _, depth := range []int{1, 6000} {
		b.Run(fmt.Sprintf("batches_%d", depth), func(b *testing.B) {
			spool, _, err := openHistorySpool(historySpoolOptions{
				Path:               filepath.Join(b.TempDir(), "spool.db"),
				MaxSize:            64 << 20,
				Identity:           historySpoolTestOptionsIdentity(),
				RoutingFingerprint: historySpoolTestOptionsFingerprint(),
				ConfigRevision:     historySpoolTestRevision,
				Now:                historySpoolTestNow,
			})
			if err != nil {
				b.Fatal(err)
			}
			defer spool.Close()
			// Fixture setup only: status benchmarks perform no writes.
			spool.db.NoSync = true
			for sequence := 1; sequence <= depth; sequence++ {
				if _, err = spool.Append(historySpoolTestBatch(uint64(sequence))); err != nil {
					b.Fatal(err)
				}
			}
			spool.db.NoSync = false
			b.Run("metadata", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					status, err := spool.readStatus()
					if err != nil || status.QueueDepth != uint64(depth) {
						b.Fatalf("status depth=%d error=%v", status.QueueDepth, err)
					}
				}
			})
			b.Run("full_validation", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if err := spool.db.View(func(tx *bbolt.Tx) error {
						_, err := validateHistorySpoolTransaction(tx, historySpoolSafeLogicalSize, true, nil)
						return err
					}); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

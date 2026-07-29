package trafficcontrol

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sagernet/bbolt"
)

type boltHistoryBackendHarness struct {
	root          string
	availableFrom time.Time
}

func newBoltHistoryBackendHarness(t *testing.T) historyBackendHarness {
	t.Helper()
	return &boltHistoryBackendHarness{
		root:          t.TempDir(),
		availableFrom: time.Now().UTC().Truncate(HistoryBucketInterval).Add(-time.Hour),
	}
}

func (h *boltHistoryBackendHarness) Open(t *testing.T, configContent []byte) *History {
	t.Helper()
	path := filepath.Join(h.root, strings.ReplaceAll(t.Name(), "/", "_"), "traffic.db")
	seedBoltHistoryContractAvailability(t, path, h.availableFrom)
	return openTestHistory(t, path, string(configContent))
}

func seedBoltHistoryContractAvailability(t *testing.T, path string, availableFrom time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal("create Bolt contract directory:", err)
	}
	database, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal("open Bolt contract database:", err)
	}
	updateErr := database.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(historyMetadataBucket)
		if err != nil {
			return err
		}
		value := historyBucketKeyPrefix(availableFrom.Unix())
		for _, key := range [][]byte{
			historyTargetsFromKey,
			historyDestinationsFromKey,
		} {
			if bucket.Get(key) != nil {
				continue
			}
			if err := bucket.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	closeErr := database.Close()
	if err := errors.Join(updateErr, closeErr); err != nil {
		t.Fatal("seed Bolt contract availability:", err)
	}
}

func TestBoltHistoryBackendContract(t *testing.T) {
	runHistoryBackendContract(t, newBoltHistoryBackendHarness(t))
}

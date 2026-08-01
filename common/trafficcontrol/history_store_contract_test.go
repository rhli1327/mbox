package trafficcontrol

import (
	"testing"
)

type historyBackendHarness interface {
	Open(t *testing.T, configContent []byte) *History
}

func runHistoryBackendContract(t *testing.T, harness historyBackendHarness) {
	t.Helper()
	t.Run("pending persisted equivalence and half-open ranges", func(t *testing.T) {
		runHistoryPendingAndFlushedResultsAreEquivalent(t, harness)
	})
	t.Run("persistence reopen revision and filters", func(t *testing.T) {
		runHistoryPersistenceReopenAndFilters(t, harness)
	})
	t.Run("grouping sorting and pagination", func(t *testing.T) {
		runHistoryGroupingsFilteringSearchSortingAndPagination(t, harness)
	})
	t.Run("query cancellation", func(t *testing.T) {
		runHistoryQueryHonorsCancellation(t, harness)
	})
}

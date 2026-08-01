package trafficcontrol

import (
	"context"
	"time"
)

type HistoryReader interface {
	TargetAvailableFrom() time.Time
	DestinationAvailableFrom() time.Time
	Query(context.Context, HistoryQuery) (HistoryQueryResult, error)
}

type historyBatch map[historyKey]historyCounters

type historyStoreState struct {
	configRevision   string
	targetsFrom      time.Time
	destinationsFrom time.Time
}

type historyQueryOverlay struct {
	pending                  historyBatch
	targetAvailableFrom      time.Time
	destinationAvailableFrom time.Time
}

type historyStoreLifecycle interface {
	Open() (historyStoreState, error)
	Close() error
}

type historyStoreWriter interface {
	Write(historyBatch) error
	Cleanup(time.Time) error
}

type historyStoreReader interface {
	BeginRead(context.Context) (historyStoreSnapshot, error)
}

type historyStoreCommittedReader interface {
	Commit(historyBatch) (historyCommitBoundary, error)
	BeginReadCommitted(
		context.Context,
		historyCommitBoundary,
	) (historyStoreSnapshot, error)
}

type historyStore interface {
	historyStoreLifecycle
	historyStoreWriter
	historyStoreReader
}

type historyStoreSnapshot interface {
	Query(
		context.Context,
		HistoryQuery,
		historyQueryOverlay,
	) (HistoryQueryResult, error)

	Close()
}

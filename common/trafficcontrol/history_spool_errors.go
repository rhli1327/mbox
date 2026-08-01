package trafficcontrol

import "errors"

var (
	ErrHistoryUnavailable = errors.New(
		"traffic statistics are temporarily unavailable",
	)
	ErrHistoryShutdownTimeout = errors.New(
		"traffic statistics remote shutdown timed out",
	)
	ErrSpoolLocked = errors.New(
		"traffic statistics spool is already in use",
	)
	ErrSpoolCorrupt = errors.New(
		"traffic statistics spool is corrupt",
	)
	ErrSpoolFuture = errors.New(
		"traffic statistics spool format is newer than this version",
	)
	ErrSpoolIdentityMissing = errors.New(
		"traffic statistics identity is missing for an existing spool",
	)
	ErrSpoolIdentityConflict = errors.New(
		"traffic statistics spool identity conflicts with configured identity",
	)
	ErrSpoolIO = errors.New(
		"traffic statistics spool I/O failed",
	)
	ErrSpoolClosed = errors.New(
		"traffic statistics spool is closed",
	)
	ErrSpoolSequenceExhausted = errors.New(
		"traffic statistics spool sequence is exhausted",
	)
	ErrPostgresRevisionConflict = errors.New(
		"traffic statistics PostgreSQL revision conflicts with queued data",
	)
)

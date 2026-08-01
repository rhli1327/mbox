package trafficcontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type trafficMigrationTarget struct {
	ctx                  context.Context
	schema               string
	schemaIdentifier     string
	instanceID           string
	identity             trafficIdentity
	source               *trafficMigrationSource
	revision             postgresRevisionInput
	activeRevision       string
	migrationID          string
	fromBucket           int64
	toBucket             int64
	effectiveTargetsFrom time.Time
	effectiveDestFrom    time.Time
	resumeRequired       bool
	connectTimeout       time.Duration
	poolFactory          *postgresPoolFactory
	pool                 *pgxpool.Pool
	connection           *pgxpool.Conn
	lockKey              int64
	jobExists            bool
	jobState             trafficMigrationJobState
	initialized          bool
	closeOnce            sync.Once
	closeErr             error
}

var errTrafficMigrationRetryBatch = errors.New("retry traffic migration batch")

type trafficMigrationJobState struct {
	summaryCursor   []byte
	targetCursor    []byte
	summaryCounts   TrafficStatisticsMigrationCounts
	targetCounts    TrafficStatisticsMigrationCounts
	summaryComplete bool
	targetComplete  bool
}

type trafficMigrationTargetRecordState struct {
	exists      bool
	matches     bool
	uplink      string
	downlink    string
	connections string
}

func openTrafficMigrationTarget(
	ctx context.Context,
	logger log.ContextLogger,
	source *trafficMigrationSource,
	identity trafficIdentity,
	revision postgresRevisionInput,
	activeRevision string,
	migrationID string,
	fromBucket int64,
	toBucket int64,
	options TrafficStatisticsMigrationOptions,
) (*trafficMigrationTarget, error) {
	_ = logger
	factory, err := newPostgresPoolFactory(ctx, postgresConnectionOptions{
		DSN:                options.Traffic.DSN,
		Dialer:             options.Traffic.Dialer,
		MaxOpenConnections: options.Traffic.MaxOpenConnections,
		MinIdleConnections: options.Traffic.MinIdleConnections,
		ConnectTimeout:     options.Traffic.ConnectTimeout,
		StatementTimeout:   options.Traffic.StatementTimeout,
	})
	if err != nil {
		return nil, err
	}
	pool, err := factory.Open(ctx)
	if err != nil {
		return nil, err
	}
	opened := false
	defer func() {
		if !opened {
			pool.Close()
		}
	}()
	if err = pool.Ping(ctx); err != nil {
		return nil, classifyPostgresError(
			ctx,
			"connect traffic statistics migration database",
			err,
		)
	}
	schemaMode := options.Traffic.SchemaManagement
	if options.DryRun {
		schemaMode = option.TrafficStatisticsSchemaManagementValidate
	}
	if err = managePostgresSchema(ctx, pool, options.Traffic.Schema, schemaMode); err != nil {
		return nil, err
	}
	connection, err := pool.Acquire(ctx)
	if err != nil {
		return nil, classifyPostgresError(
			ctx,
			"acquire traffic statistics migration connection",
			err,
		)
	}
	target := &trafficMigrationTarget{
		ctx:              ctx,
		schema:           options.Traffic.Schema,
		schemaIdentifier: pgx.Identifier{options.Traffic.Schema}.Sanitize(),
		instanceID:       identity.InstanceID,
		identity:         identity,
		source:           source,
		revision:         revision,
		activeRevision:   activeRevision,
		migrationID:      migrationID,
		fromBucket:       fromBucket,
		toBucket:         toBucket,
		resumeRequired:   options.ResumeMigrationID != "",
		connectTimeout:   options.Traffic.ConnectTimeout,
		poolFactory:      factory,
		pool:             pool,
		connection:       connection,
	}
	target.effectiveTargetsFrom = source.targetsFrom
	target.effectiveDestFrom = source.destinationsFrom
	if fromBucket != math.MinInt64 {
		from := time.Unix(fromBucket, 0).UTC()
		if from.After(target.effectiveTargetsFrom) {
			target.effectiveTargetsFrom = from
		}
		if from.After(target.effectiveDestFrom) {
			target.effectiveDestFrom = from
		}
	}
	target.lockKey = trafficMigrationAdvisoryLockKey(
		target.schema,
		target.instanceID,
	)
	var locked bool
	err = connection.QueryRow(
		ctx,
		"SELECT pg_try_advisory_lock($1)",
		target.lockKey,
	).Scan(&locked)
	if err != nil {
		connection.Release()
		return nil, classifyPostgresError(
			ctx,
			"lock traffic statistics migration target",
			err,
		)
	}
	if !locked {
		connection.Release()
		return nil, ErrTrafficMigrationTargetInUse
	}
	opened = true
	if err = target.validateExisting(ctx); err != nil {
		_ = target.Close()
		return nil, err
	}
	return target, nil
}

func (t *trafficMigrationTarget) validateExisting(ctx context.Context) error {
	var revisionKey []byte
	err := t.connection.QueryRow(ctx, fmt.Sprintf(`
SELECT revision_key
FROM %s.mbox_traffic_instances
WHERE instance_id = $1
`, t.schemaIdentifier), t.instanceID).Scan(&revisionKey)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return classifyPostgresError(ctx, "inspect traffic migration target identity", err)
	}
	if err == nil && !bytes.Equal(revisionKey, t.identity.RevisionKey) {
		return ErrTrafficMigrationIdentityConflict
	}
	var mappedRevision string
	err = t.connection.QueryRow(ctx, fmt.Sprintf(`
SELECT config_revision
FROM %s.mbox_traffic_config_revisions
WHERE instance_id = $1 AND routing_fingerprint = $2
`, t.schemaIdentifier), t.instanceID, t.revision.RoutingFingerprint[:]).Scan(
		&mappedRevision,
	)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return classifyPostgresError(ctx, "inspect traffic migration revision mapping", err)
	}
	if err == nil && mappedRevision != t.activeRevision {
		return ErrTrafficMigrationConflict
	}
	exists, err := t.migrationJobExists(ctx, t.connection)
	if err != nil {
		return err
	}
	if t.resumeRequired && !exists {
		return ErrTrafficMigrationResumeMismatch
	}
	if exists {
		if err = t.verifyMigrationJob(ctx, t.connection, false); err != nil {
			return err
		}
		if err = t.verifyMigrationManifest(ctx, t.connection); err != nil {
			return err
		}
		t.jobState, err = t.readJobState(ctx, t.connection, false)
		if err != nil {
			return err
		}
		if err = t.validateMigrationLedger(ctx, t.jobState); err != nil {
			return err
		}
		t.jobExists = true
	}
	return nil
}

func (t *trafficMigrationTarget) validateMigrationLedger(
	ctx context.Context,
	state trafficMigrationJobState,
) error {
	if !state.summaryComplete &&
		(state.targetComplete ||
			len(state.targetCursor) != 0 ||
			state.targetCounts != (TrafficStatisticsMigrationCounts{})) {
		return ErrTrafficMigrationResumeMismatch
	}
	if err := t.validateMigrationKindLedger(
		ctx,
		"summary",
		state.summaryCursor,
		state.summaryCounts,
		state.summaryComplete,
	); err != nil {
		return err
	}
	return t.validateMigrationKindLedger(
		ctx,
		"target",
		state.targetCursor,
		state.targetCounts,
		state.targetComplete,
	)
}

func (t *trafficMigrationTarget) validateMigrationKindLedger(
	ctx context.Context,
	kind string,
	jobCursor []byte,
	jobCounts TrafficStatisticsMigrationCounts,
	complete bool,
) error {
	type ledgerBatch struct {
		cursorStart   []byte
		cursorEnd     []byte
		payload       []byte
		recordCount   int64
		insertedCount int64
		skippedCount  int64
		firstBucket   int64
		lastBucket    int64
	}
	var cursor []byte
	pageCursor := []byte{}
	var counts TrafficStatisticsMigrationCounts
	for {
		rows, err := t.connection.Query(ctx, fmt.Sprintf(`
SELECT
    cursor_start,
    cursor_end,
    payload_sha256,
    record_count,
    inserted_count,
    skipped_count,
    first_bucket,
    last_bucket
FROM %s.mbox_traffic_migration_batches
WHERE instance_id = $1 AND migration_id = $2 AND kind = $3
    AND cursor_end > $4
ORDER BY cursor_end
LIMIT 256
`, t.schemaIdentifier),
			t.instanceID,
			t.migrationID,
			kind,
			pageCursor,
		)
		if err != nil {
			return classifyPostgresError(
				ctx,
				"read traffic migration batch ledger",
				err,
			)
		}
		var batches []ledgerBatch
		for rows.Next() {
			var batch ledgerBatch
			if err = rows.Scan(
				&batch.cursorStart,
				&batch.cursorEnd,
				&batch.payload,
				&batch.recordCount,
				&batch.insertedCount,
				&batch.skippedCount,
				&batch.firstBucket,
				&batch.lastBucket,
			); err != nil {
				rows.Close()
				return classifyPostgresError(
					ctx,
					"decode traffic migration batch ledger",
					err,
				)
			}
			batches = append(batches, batch)
		}
		if err = rows.Err(); err != nil {
			rows.Close()
			return classifyPostgresError(
				ctx,
				"read traffic migration batch ledger",
				err,
			)
		}
		rows.Close()
		if len(batches) == 0 {
			break
		}
		for _, batch := range batches {
			if batch.recordCount < 1 ||
				batch.insertedCount < 0 ||
				batch.skippedCount < 0 ||
				batch.insertedCount+batch.skippedCount != batch.recordCount ||
				!bytes.Equal(batch.cursorStart, cursor) {
				return ErrTrafficMigrationResumeMismatch
			}
			records, _, readErr := t.source.readBatch(
				ctx,
				kind,
				batch.cursorStart,
				t.fromBucket,
				t.toBucket,
				int(batch.recordCount),
			)
			if readErr != nil {
				return readErr
			}
			expectedDigest := trafficMigrationBatchDigest(kind, records)
			if len(records) != int(batch.recordCount) ||
				!bytes.Equal(batch.cursorEnd, records[len(records)-1].key) ||
				!bytes.Equal(batch.payload, expectedDigest[:]) ||
				batch.firstBucket != records[0].record.Bucket ||
				batch.lastBucket != records[len(records)-1].record.Bucket {
				return ErrTrafficMigrationResumeMismatch
			}
			for _, record := range records {
				targetState, stateErr := t.targetRecordState(
					ctx,
					t.connection,
					kind,
					record,
				)
				if stateErr != nil {
					return stateErr
				}
				if !targetState.matches {
					return trafficMigrationRecordConflict(
						kind,
						record,
						targetState,
					)
				}
			}
			counts.Scanned += uint64(batch.recordCount)
			counts.Inserted += uint64(batch.insertedCount)
			counts.Skipped += uint64(batch.skippedCount)
			cursor = append(cursor[:0], batch.cursorEnd...)
		}
		pageCursor = append(pageCursor[:0], batches[len(batches)-1].cursorEnd...)
		if len(batches) < 256 {
			break
		}
	}
	if !bytes.Equal(jobCursor, cursor) || jobCounts != counts {
		return ErrTrafficMigrationResumeMismatch
	}
	remaining, final, err := t.source.readBatch(
		ctx,
		kind,
		cursor,
		t.fromBucket,
		t.toBucket,
		1,
	)
	if err != nil {
		return err
	}
	sourceComplete := len(remaining) == 0 && final
	if complete != sourceComplete && (complete || len(cursor) != 0) {
		return ErrTrafficMigrationResumeMismatch
	}
	return nil
}

func (t *trafficMigrationTarget) initialize(ctx context.Context) error {
	if t.initialized {
		return nil
	}
	transaction, err := t.connection.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.Serializable,
	})
	if err != nil {
		return classifyPostgresError(ctx, "begin traffic migration initialization", err)
	}
	defer t.rollback(transaction)
	_, err = transaction.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s.mbox_traffic_instances (
    instance_id,
    revision_key,
    target_available_from,
    destination_available_from
) VALUES ($1, $2, $3, $4)
ON CONFLICT (instance_id) DO NOTHING
`, t.schemaIdentifier),
		t.instanceID,
		t.identity.RevisionKey,
		t.effectiveTargetsFrom,
		t.effectiveDestFrom,
	)
	if err != nil {
		return classifyPostgresError(ctx, "register traffic migration instance", err)
	}
	var revisionKey []byte
	err = transaction.QueryRow(ctx, fmt.Sprintf(`
SELECT revision_key
FROM %s.mbox_traffic_instances
WHERE instance_id = $1
FOR UPDATE
`, t.schemaIdentifier), t.instanceID).Scan(&revisionKey)
	if err != nil {
		return classifyPostgresError(ctx, "lock traffic migration instance", err)
	}
	if !bytes.Equal(revisionKey, t.identity.RevisionKey) {
		return ErrTrafficMigrationIdentityConflict
	}
	_, err = transaction.Exec(ctx, fmt.Sprintf(`
UPDATE %s.mbox_traffic_instances
SET target_available_from = LEAST(target_available_from, $2),
    destination_available_from = LEAST(destination_available_from, $3),
    updated_at = clock_timestamp()
WHERE instance_id = $1
`, t.schemaIdentifier),
		t.instanceID,
		t.effectiveTargetsFrom,
		t.effectiveDestFrom,
	)
	if err != nil {
		return classifyPostgresError(ctx, "widen traffic migration availability", err)
	}
	_, err = transaction.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s.mbox_traffic_config_revisions (
    instance_id,
    routing_fingerprint,
    config_revision
) VALUES ($1, $2, $3)
ON CONFLICT (instance_id, routing_fingerprint) DO NOTHING
`, t.schemaIdentifier),
		t.instanceID,
		t.revision.RoutingFingerprint[:],
		t.activeRevision,
	)
	if err != nil {
		return classifyPostgresError(ctx, "register traffic migration revision", err)
	}
	var mappedRevision string
	err = transaction.QueryRow(ctx, fmt.Sprintf(`
SELECT config_revision
FROM %s.mbox_traffic_config_revisions
WHERE instance_id = $1 AND routing_fingerprint = $2
`, t.schemaIdentifier),
		t.instanceID,
		t.revision.RoutingFingerprint[:],
	).Scan(&mappedRevision)
	if err != nil {
		return classifyPostgresError(ctx, "verify traffic migration revision", err)
	}
	if mappedRevision != t.activeRevision {
		return ErrTrafficMigrationConflict
	}
	exists, err := t.migrationJobExists(ctx, transaction)
	if err != nil {
		return err
	}
	if t.resumeRequired && !exists {
		return ErrTrafficMigrationResumeMismatch
	}
	if !exists {
		_, err = transaction.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s.mbox_traffic_migration_jobs (
    instance_id,
    migration_id,
    source_fingerprint,
    source_size,
    mode,
    from_bucket,
    to_bucket,
    active_config_revision,
    routing_fingerprint,
    source_target_available_from,
    source_destination_available_from,
    effective_target_available_from,
    effective_destination_available_from
) VALUES (
    $1, $2, $3, $4, 'restore', $5, $6, $7, $8, $9, $10, $11, $12
)
`, t.schemaIdentifier),
			t.instanceID,
			t.migrationID,
			t.source.fingerprint[:],
			t.source.size,
			t.fromBucket,
			t.toBucket,
			t.activeRevision,
			t.revision.RoutingFingerprint[:],
			t.source.targetsFrom,
			t.source.destinationsFrom,
			t.effectiveTargetsFrom,
			t.effectiveDestFrom,
		)
		if err != nil {
			return classifyPostgresError(ctx, "create traffic migration job", err)
		}
	}
	if err = t.verifyMigrationJob(ctx, transaction, true); err != nil {
		return err
	}
	for _, manifest := range t.source.revisions {
		_, err = transaction.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s.mbox_traffic_migration_revisions (
    instance_id,
    migration_id,
    config_revision,
    first_bucket,
    last_bucket,
    summary_records,
    target_records
) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (instance_id, migration_id, config_revision) DO NOTHING
`, t.schemaIdentifier),
			t.instanceID,
			t.migrationID,
			manifest.configRevision,
			manifest.firstBucket,
			manifest.lastBucket,
			manifest.summaryRecords,
			manifest.targetRecords,
		)
		if err != nil {
			return classifyPostgresError(ctx, "record traffic migration revision manifest", err)
		}
		var firstBucket int64
		var lastBucket int64
		var summaryRecords int64
		var targetRecords int64
		err = transaction.QueryRow(ctx, fmt.Sprintf(`
SELECT first_bucket, last_bucket, summary_records, target_records
FROM %s.mbox_traffic_migration_revisions
WHERE instance_id = $1 AND migration_id = $2 AND config_revision = $3
`, t.schemaIdentifier),
			t.instanceID,
			t.migrationID,
			manifest.configRevision,
		).Scan(&firstBucket, &lastBucket, &summaryRecords, &targetRecords)
		if err != nil {
			return classifyPostgresError(ctx, "verify traffic migration revision manifest", err)
		}
		if firstBucket != manifest.firstBucket ||
			lastBucket != manifest.lastBucket ||
			summaryRecords != int64(manifest.summaryRecords) ||
			targetRecords != int64(manifest.targetRecords) {
			return ErrTrafficMigrationResumeMismatch
		}
	}
	if err = t.verifyMigrationManifest(ctx, transaction); err != nil {
		return err
	}
	if err = transaction.Commit(ctx); err != nil {
		return classifyPostgresError(ctx, "commit traffic migration initialization", err)
	}
	t.initialized = true
	return nil
}

func (t *trafficMigrationTarget) verifyMigrationManifest(
	ctx context.Context,
	executor interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	},
) error {
	rows, err := executor.Query(ctx, fmt.Sprintf(`
SELECT
    config_revision,
    first_bucket,
    last_bucket,
    summary_records,
    target_records
FROM %s.mbox_traffic_migration_revisions
WHERE instance_id = $1 AND migration_id = $2
ORDER BY config_revision
`, t.schemaIdentifier), t.instanceID, t.migrationID)
	if err != nil {
		return classifyPostgresError(ctx, "read traffic migration revision manifest", err)
	}
	defer rows.Close()
	loaded := make(map[string]trafficMigrationRevision, len(t.source.revisions))
	for rows.Next() {
		var manifest trafficMigrationRevision
		var summaryRecords int64
		var targetRecords int64
		if err = rows.Scan(
			&manifest.configRevision,
			&manifest.firstBucket,
			&manifest.lastBucket,
			&summaryRecords,
			&targetRecords,
		); err != nil {
			return classifyPostgresError(ctx, "decode traffic migration revision manifest", err)
		}
		if summaryRecords < 0 || targetRecords < 0 {
			return ErrTrafficMigrationResumeMismatch
		}
		manifest.summaryRecords = uint64(summaryRecords)
		manifest.targetRecords = uint64(targetRecords)
		loaded[manifest.configRevision] = manifest
	}
	if err = rows.Err(); err != nil {
		return classifyPostgresError(ctx, "read traffic migration revision manifest", err)
	}
	if len(loaded) != len(t.source.revisions) {
		return ErrTrafficMigrationResumeMismatch
	}
	for revision, expected := range t.source.revisions {
		actual, exists := loaded[revision]
		if !exists ||
			actual.firstBucket != expected.firstBucket ||
			actual.lastBucket != expected.lastBucket ||
			actual.summaryRecords != expected.summaryRecords ||
			actual.targetRecords != expected.targetRecords {
			return ErrTrafficMigrationResumeMismatch
		}
	}
	return nil
}

func (t *trafficMigrationTarget) migrationJobExists(
	ctx context.Context,
	executor interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	},
) (bool, error) {
	var exists bool
	err := executor.QueryRow(ctx, fmt.Sprintf(`
SELECT EXISTS (
    SELECT 1
    FROM %s.mbox_traffic_migration_jobs
    WHERE instance_id = $1 AND migration_id = $2
)
`, t.schemaIdentifier), t.instanceID, t.migrationID).Scan(&exists)
	if err != nil {
		return false, classifyPostgresError(ctx, "inspect traffic migration job", err)
	}
	return exists, nil
}

func (t *trafficMigrationTarget) verifyMigrationJob(
	ctx context.Context,
	executor interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	},
	lock bool,
) error {
	suffix := ""
	if lock {
		suffix = " FOR UPDATE"
	}
	var sourceFingerprint []byte
	var sourceSize int64
	var mode string
	var fromBucket int64
	var toBucket int64
	var activeRevision string
	var routingFingerprint []byte
	var sourceTargetsFrom time.Time
	var sourceDestFrom time.Time
	var effectiveTargetsFrom time.Time
	var effectiveDestFrom time.Time
	err := executor.QueryRow(ctx, fmt.Sprintf(`
SELECT
    source_fingerprint,
    source_size,
    mode,
    from_bucket,
    to_bucket,
    active_config_revision,
    routing_fingerprint,
    source_target_available_from,
    source_destination_available_from,
    effective_target_available_from,
    effective_destination_available_from
FROM %s.mbox_traffic_migration_jobs
WHERE instance_id = $1 AND migration_id = $2%s
`, t.schemaIdentifier, suffix), t.instanceID, t.migrationID).Scan(
		&sourceFingerprint,
		&sourceSize,
		&mode,
		&fromBucket,
		&toBucket,
		&activeRevision,
		&routingFingerprint,
		&sourceTargetsFrom,
		&sourceDestFrom,
		&effectiveTargetsFrom,
		&effectiveDestFrom,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrTrafficMigrationResumeMismatch
		}
		return classifyPostgresError(ctx, "verify traffic migration job", err)
	}
	if !bytes.Equal(sourceFingerprint, t.source.fingerprint[:]) ||
		sourceSize != t.source.size ||
		mode != "restore" ||
		fromBucket != t.fromBucket ||
		toBucket != t.toBucket ||
		activeRevision != t.activeRevision ||
		!bytes.Equal(routingFingerprint, t.revision.RoutingFingerprint[:]) ||
		!sourceTargetsFrom.Equal(t.source.targetsFrom) ||
		!sourceDestFrom.Equal(t.source.destinationsFrom) ||
		!effectiveTargetsFrom.Equal(t.effectiveTargetsFrom) ||
		!effectiveDestFrom.Equal(t.effectiveDestFrom) {
		return ErrTrafficMigrationResumeMismatch
	}
	return nil
}

func (t *trafficMigrationTarget) restoreKind(
	ctx context.Context,
	kind string,
	batchSize int,
	dryRun bool,
	progress func(TrafficStatisticsMigrationProgress) error,
) (TrafficStatisticsMigrationCounts, error) {
	if dryRun {
		requireExact := false
		if t.jobExists {
			requireExact = t.jobState.summaryComplete
			if kind == "target" {
				requireExact = t.jobState.targetComplete
			}
		}
		return t.verifyRecords(
			ctx,
			kind,
			nil,
			batchSize,
			progress,
			requireExact,
		)
	}
	if err := t.initialize(ctx); err != nil {
		return TrafficStatisticsMigrationCounts{}, err
	}
	state, err := t.readJobState(ctx, t.connection, false)
	if err != nil {
		return TrafficStatisticsMigrationCounts{}, err
	}
	cursor := state.summaryCursor
	counts := state.summaryCounts
	complete := state.summaryComplete
	if kind == "target" {
		cursor = state.targetCursor
		counts = state.targetCounts
		complete = state.targetComplete
	}
	if complete {
		return t.verifyRecords(ctx, kind, nil, batchSize, progress, true)
	}
	for {
		if err = ctx.Err(); err != nil {
			return counts, err
		}
		records, final, readErr := t.source.readBatch(
			ctx,
			kind,
			cursor,
			t.fromBucket,
			t.toBucket,
			batchSize,
		)
		if readErr != nil {
			return counts, readErr
		}
		if len(records) == 0 {
			if err = t.markKindComplete(ctx, kind, cursor); err != nil {
				return counts, err
			}
			if err = emitTrafficMigrationProgress(progress, TrafficStatisticsMigrationProgress{
				Phase:       kind + "_batch",
				MigrationID: t.migrationID,
				Kind:        kind,
				Cursor:      hex.EncodeToString(cursor),
				Scanned:     counts.Scanned,
				Inserted:    counts.Inserted,
				Skipped:     counts.Skipped,
			}); err != nil {
				return counts, err
			}
			return counts, nil
		}
		batchCounts, commitErr := t.restoreBatch(
			ctx,
			kind,
			cursor,
			records,
			final,
		)
		if commitErr != nil {
			return counts, commitErr
		}
		counts.Scanned += batchCounts.Scanned
		counts.Inserted += batchCounts.Inserted
		counts.Skipped += batchCounts.Skipped
		cursor = records[len(records)-1].key
		if err = emitTrafficMigrationProgress(progress, TrafficStatisticsMigrationProgress{
			Phase:       kind + "_batch",
			MigrationID: t.migrationID,
			Kind:        kind,
			Cursor:      hex.EncodeToString(cursor),
			Scanned:     counts.Scanned,
			Inserted:    counts.Inserted,
			Skipped:     counts.Skipped,
		}); err != nil {
			return counts, err
		}
		if final {
			return counts, nil
		}
	}
}

func (t *trafficMigrationTarget) verifyRecords(
	ctx context.Context,
	kind string,
	startCursor []byte,
	batchSize int,
	progress func(TrafficStatisticsMigrationProgress) error,
	requireExact bool,
) (TrafficStatisticsMigrationCounts, error) {
	var counts TrafficStatisticsMigrationCounts
	cursor := append([]byte(nil), startCursor...)
	for {
		records, final, err := t.source.readBatch(
			ctx,
			kind,
			cursor,
			t.fromBucket,
			t.toBucket,
			batchSize,
		)
		if err != nil {
			return counts, err
		}
		for _, record := range records {
			if err = ctx.Err(); err != nil {
				return counts, err
			}
			targetState, stateErr := t.targetRecordState(
				ctx,
				t.connection,
				kind,
				record,
			)
			if stateErr != nil {
				return counts, stateErr
			}
			counts.Scanned++
			if targetState.matches {
				counts.Skipped++
			} else {
				if targetState.exists || requireExact {
					return counts, trafficMigrationRecordConflict(
						kind,
						record,
						targetState,
					)
				}
				counts.Inserted++
			}
		}
		if len(records) != 0 {
			cursor = records[len(records)-1].key
			if err = emitTrafficMigrationProgress(progress, TrafficStatisticsMigrationProgress{
				Phase:       kind + "_batch",
				MigrationID: t.migrationID,
				Kind:        kind,
				Cursor:      hex.EncodeToString(cursor),
				Scanned:     counts.Scanned,
				Inserted:    counts.Inserted,
				Skipped:     counts.Skipped,
			}); err != nil {
				return counts, err
			}
		}
		if final {
			return counts, nil
		}
	}
}

func (t *trafficMigrationTarget) restoreBatch(
	ctx context.Context,
	kind string,
	cursorStart []byte,
	records []trafficMigrationRecord,
	final bool,
) (TrafficStatisticsMigrationCounts, error) {
	counts, err := t.restoreBatchAttempt(
		ctx,
		kind,
		cursorStart,
		records,
		final,
		true,
	)
	return counts, err
}

func (t *trafficMigrationTarget) restoreBatchAttempt(
	ctx context.Context,
	kind string,
	cursorStart []byte,
	records []trafficMigrationRecord,
	final bool,
	allowUncertainRetry bool,
) (TrafficStatisticsMigrationCounts, error) {
	transaction, err := t.connection.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.Serializable,
	})
	if err != nil {
		return TrafficStatisticsMigrationCounts{}, classifyPostgresError(
			ctx,
			"begin traffic migration batch",
			err,
		)
	}
	defer t.rollback(transaction)
	state, err := t.readJobState(ctx, transaction, true)
	if err != nil {
		return TrafficStatisticsMigrationCounts{}, err
	}
	currentCursor := state.summaryCursor
	if kind == "target" {
		currentCursor = state.targetCursor
	}
	if !bytes.Equal(currentCursor, cursorStart) {
		return TrafficStatisticsMigrationCounts{}, ErrTrafficMigrationResumeMismatch
	}
	cursorEnd := records[len(records)-1].key
	digest := trafficMigrationBatchDigest(kind, records)
	var counts TrafficStatisticsMigrationCounts
	for _, record := range records {
		inserted, applyErr := t.restoreRecord(ctx, transaction, kind, record)
		if applyErr != nil {
			return TrafficStatisticsMigrationCounts{}, applyErr
		}
		counts.Scanned++
		if inserted {
			counts.Inserted++
		} else {
			counts.Skipped++
		}
	}
	_, err = transaction.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s.mbox_traffic_migration_batches (
    instance_id,
    migration_id,
    kind,
    cursor_start,
    cursor_end,
    payload_sha256,
    record_count,
    inserted_count,
    skipped_count,
    first_bucket,
    last_bucket
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
`, t.schemaIdentifier),
		t.instanceID,
		t.migrationID,
		kind,
		cursorStart,
		cursorEnd,
		digest[:],
		len(records),
		counts.Inserted,
		counts.Skipped,
		records[0].record.Bucket,
		records[len(records)-1].record.Bucket,
	)
	if err != nil {
		return TrafficStatisticsMigrationCounts{}, classifyPostgresError(
			ctx,
			"record traffic migration batch",
			err,
		)
	}
	if err = t.updateJobAfterBatch(
		ctx,
		transaction,
		kind,
		cursorEnd,
		counts,
		final,
	); err != nil {
		return TrafficStatisticsMigrationCounts{}, err
	}
	if err = transaction.Commit(ctx); err != nil {
		recoveryErr := t.resolveUncertainBatchCommit(
			ctx,
			kind,
			cursorStart,
			cursorEnd,
			digest,
			counts,
			state,
			err,
		)
		if errors.Is(recoveryErr, errTrafficMigrationRetryBatch) &&
			allowUncertainRetry {
			return t.restoreBatchAttempt(
				ctx,
				kind,
				cursorStart,
				records,
				final,
				false,
			)
		}
		if errors.Is(recoveryErr, errTrafficMigrationRetryBatch) {
			return TrafficStatisticsMigrationCounts{}, classifyPostgresError(
				ctx,
				"commit traffic migration batch after retry",
				err,
			)
		}
		if recoveryErr != nil {
			return TrafficStatisticsMigrationCounts{}, recoveryErr
		}
	}
	return counts, nil
}

func (t *trafficMigrationTarget) restoreRecord(
	ctx context.Context,
	transaction pgx.Tx,
	kind string,
	record trafficMigrationRecord,
) (bool, error) {
	table := "mbox_traffic_minute_summary"
	columns := `
    instance_id, bucket_start, config_revision, route_tag, group_path,
    actual_outbound_tag, actual_outbound_type, network,
    uplink_bytes, downlink_bytes, connections`
	placeholders := "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11"
	arguments := []any{
		t.instanceID,
		time.Unix(record.record.Bucket, 0).UTC(),
		record.record.ConfigRevision,
		record.record.RouteTag,
		record.groupPath,
		record.record.ActualOutboundTag,
		record.record.ActualOutboundType,
		record.record.Network,
		strconv.FormatUint(record.record.UplinkBytes, 10),
		strconv.FormatUint(record.record.DownlinkBytes, 10),
		strconv.FormatUint(record.record.Connections, 10),
	}
	if kind == "target" {
		table = "mbox_traffic_minute_targets"
		columns = `
    instance_id, bucket_start, config_revision, route_tag, group_path,
    actual_outbound_tag, actual_outbound_type, network,
    destination_domain, destination_ip,
    uplink_bytes, downlink_bytes, connections`
		placeholders = "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13"
		arguments = append(arguments[:8],
			record.record.DestinationDomain,
			record.record.DestinationIP,
			arguments[8],
			arguments[9],
			arguments[10],
		)
	}
	tag, err := transaction.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s.%s (%s)
VALUES (%s)
ON CONFLICT DO NOTHING
`, t.schemaIdentifier, table, columns, placeholders), arguments...)
	if err != nil {
		return false, classifyPostgresError(ctx, "restore traffic migration record", err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}
	targetState, err := t.targetRecordState(
		ctx,
		transaction,
		kind,
		record,
	)
	if err != nil {
		return false, err
	}
	if !targetState.exists || !targetState.matches {
		return false, trafficMigrationRecordConflict(kind, record, targetState)
	}
	return false, nil
}

func (t *trafficMigrationTarget) targetRecordState(
	ctx context.Context,
	executor interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	},
	kind string,
	record trafficMigrationRecord,
) (trafficMigrationTargetRecordState, error) {
	table := "mbox_traffic_minute_summary"
	destinationWhere := ""
	arguments := []any{
		t.instanceID,
		time.Unix(record.record.Bucket, 0).UTC(),
		record.record.ConfigRevision,
		record.record.RouteTag,
		record.groupPath,
		record.record.ActualOutboundTag,
		record.record.ActualOutboundType,
		record.record.Network,
	}
	if kind == "target" {
		table = "mbox_traffic_minute_targets"
		destinationWhere = " AND destination_domain = $9 AND destination_ip = $10"
		arguments = append(
			arguments,
			record.record.DestinationDomain,
			record.record.DestinationIP,
		)
	}
	var uplink string
	var downlink string
	var connections string
	err := executor.QueryRow(ctx, fmt.Sprintf(`
SELECT uplink_bytes::text, downlink_bytes::text, connections::text
FROM %s.%s
WHERE instance_id = $1
    AND bucket_start = $2
    AND config_revision = $3
    AND route_tag = $4
    AND group_path = $5
    AND actual_outbound_tag = $6
    AND actual_outbound_type = $7
    AND network = $8%s
`, t.schemaIdentifier, table, destinationWhere), arguments...).Scan(
		&uplink,
		&downlink,
		&connections,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return trafficMigrationTargetRecordState{}, nil
	}
	if err != nil {
		return trafficMigrationTargetRecordState{}, classifyPostgresError(
			ctx,
			"compare traffic migration record",
			err,
		)
	}
	return trafficMigrationTargetRecordState{
		exists: true,
		matches: uplink == strconv.FormatUint(record.record.UplinkBytes, 10) &&
			downlink == strconv.FormatUint(record.record.DownlinkBytes, 10) &&
			connections == strconv.FormatUint(record.record.Connections, 10),
		uplink:      uplink,
		downlink:    downlink,
		connections: connections,
	}, nil
}

func trafficMigrationRecordConflict(
	kind string,
	record trafficMigrationRecord,
	target trafficMigrationTargetRecordState,
) error {
	keyDigest := sha256.Sum256(record.key)
	targetUplink := target.uplink
	targetDownlink := target.downlink
	targetConnections := target.connections
	if !target.exists {
		targetUplink = "missing"
		targetDownlink = "missing"
		targetConnections = "missing"
	}
	return fmt.Errorf(
		"%w: kind=%s cursor=%s key=%s source=%d/%d/%d target=%s/%s/%s",
		ErrTrafficMigrationConflict,
		kind,
		hex.EncodeToString(record.key),
		hex.EncodeToString(keyDigest[:6]),
		record.record.UplinkBytes,
		record.record.DownlinkBytes,
		record.record.Connections,
		targetUplink,
		targetDownlink,
		targetConnections,
	)
}

func (t *trafficMigrationTarget) readJobState(
	ctx context.Context,
	executor interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	},
	lock bool,
) (trafficMigrationJobState, error) {
	suffix := ""
	if lock {
		suffix = " FOR UPDATE"
	}
	var state trafficMigrationJobState
	err := executor.QueryRow(ctx, fmt.Sprintf(`
SELECT
    summary_cursor,
    target_cursor,
    summary_processed,
    summary_inserted,
    summary_skipped,
    target_processed,
    target_inserted,
    target_skipped,
    summary_complete,
    target_complete
FROM %s.mbox_traffic_migration_jobs
WHERE instance_id = $1 AND migration_id = $2%s
`, t.schemaIdentifier, suffix), t.instanceID, t.migrationID).Scan(
		&state.summaryCursor,
		&state.targetCursor,
		&state.summaryCounts.Scanned,
		&state.summaryCounts.Inserted,
		&state.summaryCounts.Skipped,
		&state.targetCounts.Scanned,
		&state.targetCounts.Inserted,
		&state.targetCounts.Skipped,
		&state.summaryComplete,
		&state.targetComplete,
	)
	if err != nil {
		return state, classifyPostgresError(ctx, "read traffic migration job state", err)
	}
	return state, nil
}

func (t *trafficMigrationTarget) updateJobAfterBatch(
	ctx context.Context,
	transaction pgx.Tx,
	kind string,
	cursor []byte,
	counts TrafficStatisticsMigrationCounts,
	complete bool,
) error {
	cursorColumn := "summary_cursor"
	processedColumn := "summary_processed"
	insertedColumn := "summary_inserted"
	skippedColumn := "summary_skipped"
	completeColumn := "summary_complete"
	if kind == "target" {
		cursorColumn = "target_cursor"
		processedColumn = "target_processed"
		insertedColumn = "target_inserted"
		skippedColumn = "target_skipped"
		completeColumn = "target_complete"
	}
	_, err := transaction.Exec(ctx, fmt.Sprintf(`
UPDATE %s.mbox_traffic_migration_jobs
SET %s = $3,
    %s = %s + $4,
    %s = %s + $5,
    %s = %s + $6,
    %s = $7,
    updated_at = clock_timestamp(),
    completed_at = CASE
        WHEN (CASE WHEN $8 = 'summary' THEN $7 ELSE summary_complete END)
            AND (CASE WHEN $8 = 'target' THEN $7 ELSE target_complete END)
        THEN clock_timestamp()
        ELSE NULL
    END
WHERE instance_id = $1 AND migration_id = $2
`, t.schemaIdentifier,
		cursorColumn,
		processedColumn,
		processedColumn,
		insertedColumn,
		insertedColumn,
		skippedColumn,
		skippedColumn,
		completeColumn,
	),
		t.instanceID,
		t.migrationID,
		cursor,
		counts.Scanned,
		counts.Inserted,
		counts.Skipped,
		complete,
		kind,
	)
	if err != nil {
		return classifyPostgresError(ctx, "advance traffic migration job", err)
	}
	return nil
}

func (t *trafficMigrationTarget) markKindComplete(
	ctx context.Context,
	kind string,
	cursor []byte,
) error {
	transaction, err := t.connection.BeginTx(ctx, pgx.TxOptions{
		IsoLevel: pgx.Serializable,
	})
	if err != nil {
		return classifyPostgresError(ctx, "begin empty traffic migration batch", err)
	}
	defer t.rollback(transaction)
	if _, err = t.readJobState(ctx, transaction, true); err != nil {
		return err
	}
	if err = t.updateJobAfterBatch(
		ctx,
		transaction,
		kind,
		cursor,
		TrafficStatisticsMigrationCounts{},
		true,
	); err != nil {
		return err
	}
	if err = transaction.Commit(ctx); err != nil {
		return classifyPostgresError(ctx, "commit empty traffic migration batch", err)
	}
	return nil
}

func trafficMigrationBatchDigest(
	kind string,
	records []trafficMigrationRecord,
) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("mbox/traffic-statistics/restore-batch/v1"))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(kind))
	_, _ = hash.Write([]byte{0})
	var length [4]byte
	for _, record := range records {
		binary.BigEndian.PutUint32(length[:], uint32(len(record.key)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(record.key)
		binary.BigEndian.PutUint32(length[:], uint32(len(record.value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(record.value)
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func (t *trafficMigrationTarget) resolveUncertainBatchCommit(
	ctx context.Context,
	kind string,
	cursorStart []byte,
	cursorEnd []byte,
	digest [sha256.Size]byte,
	counts TrafficStatisticsMigrationCounts,
	prior trafficMigrationJobState,
	commitErr error,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := t.replaceUncertainConnection(ctx); err != nil {
		return err
	}
	var storedDigest []byte
	var inserted uint64
	var skipped uint64
	err := t.connection.QueryRow(ctx, fmt.Sprintf(`
SELECT payload_sha256, inserted_count, skipped_count
FROM %s.mbox_traffic_migration_batches
WHERE instance_id = $1
    AND migration_id = $2
    AND kind = $3
    AND cursor_start = $4
    AND cursor_end = $5
`, t.schemaIdentifier),
		t.instanceID,
		t.migrationID,
		kind,
		cursorStart,
		cursorEnd,
	).Scan(&storedDigest, &inserted, &skipped)
	state, stateErr := t.readJobState(ctx, t.connection, false)
	if stateErr != nil {
		return stateErr
	}
	priorCursor := prior.summaryCursor
	priorCounts := prior.summaryCounts
	currentCursor := state.summaryCursor
	currentCounts := state.summaryCounts
	if kind == "target" {
		priorCursor = prior.targetCursor
		priorCounts = prior.targetCounts
		currentCursor = state.targetCursor
		currentCounts = state.targetCounts
	}
	expectedCounts := TrafficStatisticsMigrationCounts{
		Scanned:  priorCounts.Scanned + counts.Scanned,
		Inserted: priorCounts.Inserted + counts.Inserted,
		Skipped:  priorCounts.Skipped + counts.Skipped,
	}
	if err == nil {
		if bytes.Equal(storedDigest, digest[:]) &&
			inserted == counts.Inserted &&
			skipped == counts.Skipped &&
			bytes.Equal(currentCursor, cursorEnd) &&
			currentCounts == expectedCounts {
			return nil
		}
		return ErrTrafficMigrationConflict
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return classifyPostgresError(ctx, "resolve uncertain traffic migration commit", err)
	}
	if bytes.Equal(currentCursor, cursorStart) &&
		bytes.Equal(priorCursor, cursorStart) &&
		currentCounts == priorCounts {
		return errTrafficMigrationRetryBatch
	}
	_ = commitErr
	return ErrTrafficMigrationConflict
}

func (t *trafficMigrationTarget) replaceUncertainConnection(
	ctx context.Context,
) error {
	if t.connection != nil {
		closeCtx, cancel := context.WithTimeout(
			context.Background(),
			t.connectTimeout,
		)
		_ = t.connection.Conn().Close(closeCtx)
		cancel()
		t.connection.Release()
		t.connection = nil
	}
	if t.pool != nil {
		t.pool.Close()
		t.pool = nil
	}
	acquireCtx, cancel := context.WithTimeout(ctx, t.connectTimeout)
	defer cancel()
	pool, err := t.poolFactory.Open(acquireCtx)
	if err != nil {
		return err
	}
	if err = pool.Ping(acquireCtx); err != nil {
		pool.Close()
		return classifyPostgresError(
			acquireCtx,
			"reconnect uncertain traffic migration database",
			err,
		)
	}
	connection, err := pool.Acquire(acquireCtx)
	if err != nil {
		pool.Close()
		return classifyPostgresError(
			acquireCtx,
			"reacquire uncertain traffic migration connection",
			err,
		)
	}
	var locked bool
	err = connection.QueryRow(
		acquireCtx,
		"SELECT pg_try_advisory_lock($1)",
		t.lockKey,
	).Scan(&locked)
	if err != nil {
		connection.Release()
		pool.Close()
		return classifyPostgresError(
			acquireCtx,
			"relock uncertain traffic migration target",
			err,
		)
	}
	if !locked {
		connection.Release()
		pool.Close()
		return ErrTrafficMigrationTargetInUse
	}
	t.pool = pool
	t.connection = connection
	return nil
}

func trafficMigrationAdvisoryLockKey(schema string, instanceID string) int64 {
	hash := sha256.New()
	_, _ = hash.Write([]byte("mbox/traffic-statistics/migration-lock/v1"))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(schema))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(instanceID))
	digest := hash.Sum(nil)
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

func (t *trafficMigrationTarget) rollback(transaction pgx.Tx) {
	timeout := t.connectTimeout
	if timeout <= 0 || timeout > 5*time.Second {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = transaction.Rollback(ctx)
}

func (t *trafficMigrationTarget) Close() error {
	t.closeOnce.Do(func() {
		if t.connection != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			var unlocked bool
			err := t.connection.QueryRow(
				ctx,
				"SELECT pg_advisory_unlock($1)",
				t.lockKey,
			).Scan(&unlocked)
			if err != nil {
				t.closeErr = classifyPostgresError(
					ctx,
					"unlock traffic statistics migration target",
					err,
				)
			} else if !unlocked {
				t.closeErr = errors.New("traffic statistics migration advisory lock was not held")
			}
			cancel()
			t.connection.Release()
			t.connection = nil
		}
		if t.pool != nil {
			t.pool.Close()
			t.pool = nil
		}
	})
	return t.closeErr
}

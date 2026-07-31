//go:build with_postgres

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/common/trafficcontrol"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/pflag"
)

type trafficMigrationPGHarness struct {
	t          *testing.T
	adminDSN   string
	targetDSN  string
	schema     string
	directory  string
	sourcePath string
	configPath string
	identity   string
	instanceID string
}

func TestTrafficStatisticsMigrateCommandPG14DryRunHasNoWrites(t *testing.T) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	harness.migrateSchema()
	result := harness.run(t, "--dry-run")
	if !result.DryRun || !result.Completed ||
		result.Summary != (trafficcontrol.TrafficStatisticsMigrationCounts{
			Scanned:  3,
			Inserted: 3,
		}) ||
		result.Targets != (trafficcontrol.TrafficStatisticsMigrationCounts{
			Scanned:  4,
			Inserted: 4,
		}) {
		t.Fatalf("unexpected dry-run result: %+v", result)
	}
	if harness.tableCount("mbox_traffic_instances") != 0 ||
		harness.tableCount("mbox_traffic_migration_jobs") != 0 ||
		harness.tableCount("mbox_traffic_minute_summary") != 0 ||
		harness.tableCount("mbox_traffic_minute_targets") != 0 {
		t.Fatal("dry-run changed PostgreSQL state")
	}
	if _, err := os.Lstat(harness.identity); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dry-run created identity state: %v", err)
	}
	conflict := newTrafficMigrationPGHarness(t, "auto")
	conflict.run(t)
	conflict.exec(fmt.Sprintf(`
UPDATE %s.mbox_traffic_minute_targets
SET downlink_bytes = 999
WHERE route_tag = 'hysteria-route'
`, conflict.schemaIdentifier()))
	beforeBatches := conflict.tableCount("mbox_traffic_migration_batches")
	_, _, err := conflict.execute("--dry-run")
	if !errors.Is(err, trafficcontrol.ErrTrafficMigrationConflict) {
		t.Fatalf("dry-run did not detect exact target conflict: %v", err)
	}
	if conflict.tableCount("mbox_traffic_migration_batches") != beforeBatches ||
		conflict.tableCount("mbox_traffic_minute_summary") != 3 ||
		conflict.tableCount("mbox_traffic_minute_targets") != 4 {
		t.Fatal("conflicting dry-run changed target state")
	}
}

func TestTrafficStatisticsMigrateCommandPG14DryRunAutoDoesNotApplySchema(
	t *testing.T,
) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	_, _, err := harness.execute("--dry-run")
	if !errors.Is(err, trafficcontrol.ErrPostgresSchemaMissing) {
		t.Fatalf("auto dry-run did not remain validate-only: %v", err)
	}
	var exists bool
	if err = harness.queryRow(
		"SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)",
		harness.schema,
	).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("auto dry-run created the PostgreSQL schema")
	}
	if _, statErr := os.Lstat(harness.identity); !errors.Is(
		statErr,
		os.ErrNotExist,
	) {
		t.Fatalf("auto dry-run created identity state: %v", statErr)
	}
}

func TestTrafficStatisticsMigrateCommandPG14DryRunRejectsMissingCompletedRow(
	t *testing.T,
) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	harness.run(t)
	harness.exec(fmt.Sprintf(`
DELETE FROM %s.mbox_traffic_minute_targets
WHERE route_tag = 'hysteria-route'
`, harness.schemaIdentifier()))
	beforeSummary := harness.tableCount("mbox_traffic_minute_summary")
	beforeTargets := harness.tableCount("mbox_traffic_minute_targets")
	beforeBatches := harness.tableCount("mbox_traffic_migration_batches")
	_, _, err := harness.execute("--dry-run")
	if !errors.Is(err, trafficcontrol.ErrTrafficMigrationConflict) {
		t.Fatalf("dry-run accepted a missing row from a completed job: %v", err)
	}
	if harness.tableCount("mbox_traffic_minute_summary") != beforeSummary ||
		harness.tableCount("mbox_traffic_minute_targets") != beforeTargets ||
		harness.tableCount("mbox_traffic_migration_batches") != beforeBatches {
		t.Fatal("failed dry-run changed target state")
	}
}

func TestTrafficStatisticsMigrateCommandPG14DryRunIncompleteJobReportsCommittedPrefixAsSkipped(
	t *testing.T,
) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	previous := migrateBoltTrafficStatisticsToPostgres
	defer func() { migrateBoltTrafficStatisticsToPostgres = previous }()
	ctx, cancel := context.WithCancel(context.Background())
	migrateBoltTrafficStatisticsToPostgres = func(
		callCtx context.Context,
		logger log.ContextLogger,
		options trafficcontrol.TrafficStatisticsMigrationOptions,
	) (trafficcontrol.TrafficStatisticsMigrationResult, error) {
		originalProgress := options.Progress
		options.Progress = func(event trafficcontrol.TrafficStatisticsMigrationProgress) error {
			if err := originalProgress(event); err != nil {
				return err
			}
			if event.Phase == "summary_batch" && event.Scanned == 1 {
				cancel()
			}
			return nil
		}
		return previous(callCtx, logger, options)
	}
	_, _, err := harness.executeContext(ctx, "--batch-size", "1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected interrupted result: %v", err)
	}
	migrateBoltTrafficStatisticsToPostgres = previous
	if harness.tableCount("mbox_traffic_minute_summary") != 1 ||
		harness.tableCount("mbox_traffic_minute_targets") != 0 {
		t.Fatal("interruption did not leave exactly one committed summary row")
	}
	before := harness.snapshotTables()
	result := harness.run(t, "--dry-run", "--batch-size", "2")
	if !result.DryRun || !result.Completed ||
		result.Summary != (trafficcontrol.TrafficStatisticsMigrationCounts{
			Scanned:  3,
			Inserted: 2,
			Skipped:  1,
		}) ||
		result.Targets != (trafficcontrol.TrafficStatisticsMigrationCounts{
			Scanned:  4,
			Inserted: 4,
		}) {
		t.Fatalf("dry-run did not recount the complete selected range: %+v", result)
	}
	after := harness.snapshotTables()
	if !equalTrafficMigrationTableSnapshots(before, after) {
		t.Fatalf("incomplete-job dry-run changed PostgreSQL state:\nbefore=%v\nafter=%v", before, after)
	}
}

func TestTrafficStatisticsMigrateCommandPG14RestoreLossless(t *testing.T) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	before := snapshotTrafficMigrationFixture(t, harness.sourcePath)
	result := harness.run(t)
	assertTrafficMigrationRestoreResult(t, result, 3, 3, 0, 4, 4, 0)
	assertTrafficMigrationFixtureState(t, harness.sourcePath, before)
	if harness.tableCount("mbox_traffic_minute_summary") != 3 ||
		harness.tableCount("mbox_traffic_minute_targets") != 4 ||
		harness.tableCount("mbox_traffic_migration_revisions") != 2 {
		t.Fatal("migration did not restore the complete raw fixture")
	}
	var groupPath []string
	var network string
	harness.queryRow(fmt.Sprintf(`
SELECT group_path, network
FROM %s.mbox_traffic_minute_summary
WHERE route_tag = 'selector-route'
`, harness.schemaIdentifier())).Scan(&groupPath, &network)
	if !equalTrafficMigrationStrings(groupPath, []string{"selector", "proxy-a"}) ||
		network != "udp" {
		t.Fatalf("ordered dimensions changed: %v %s", groupPath, network)
	}
}

func TestTrafficStatisticsMigrateCommandPG14ExactRerunSkips(t *testing.T) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	first := harness.run(t)
	assertTrafficMigrationRestoreResult(t, first, 3, 3, 0, 4, 4, 0)
	second := harness.run(t, "--batch-size", "1")
	assertTrafficMigrationRestoreResult(t, second, 3, 0, 3, 4, 0, 4)
	if first.MigrationID != second.MigrationID {
		t.Fatal("batch size changed deterministic migration ID")
	}
}

func TestTrafficStatisticsMigrateCommandPG14ConflictRollsBackBatch(t *testing.T) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	previous := migrateBoltTrafficStatisticsToPostgres
	defer func() { migrateBoltTrafficStatisticsToPostgres = previous }()
	ctx, cancel := context.WithCancel(context.Background())
	migrateBoltTrafficStatisticsToPostgres = func(
		callCtx context.Context,
		logger log.ContextLogger,
		options trafficcontrol.TrafficStatisticsMigrationOptions,
	) (trafficcontrol.TrafficStatisticsMigrationResult, error) {
		originalProgress := options.Progress
		options.Progress = func(event trafficcontrol.TrafficStatisticsMigrationProgress) error {
			if err := originalProgress(event); err != nil {
				return err
			}
			if event.Phase == "summary_batch" {
				cancel()
			}
			return nil
		}
		return previous(callCtx, logger, options)
	}
	_, _, err := harness.executeContext(ctx, "--batch-size", "1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("failed to stop after the first committed batch: %v", err)
	}
	migrateBoltTrafficStatisticsToPostgres = previous

	var conflictRecord testTrafficMigrationRecord
	database, err := bbolt.Open(
		harness.sourcePath,
		0o600,
		&bbolt.Options{ReadOnly: true, Timeout: time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = database.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(testTrafficSummaryBucket).Cursor()
		_, value := cursor.First()
		_, value = cursor.Next()
		_, value = cursor.Next()
		return json.Unmarshal(value, &conflictRecord)
	})
	closeErr := database.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	var groupPath []string
	if err = json.Unmarshal([]byte(conflictRecord.GroupPath), &groupPath); err != nil {
		t.Fatal(err)
	}
	harness.exec(fmt.Sprintf(`
INSERT INTO %s.mbox_traffic_minute_summary (
    instance_id,
    bucket_start,
    config_revision,
    route_tag,
    group_path,
    actual_outbound_tag,
    actual_outbound_type,
    network,
    uplink_bytes,
    downlink_bytes,
    connections
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 99, 99, 99)
`, harness.schemaIdentifier()),
		harness.instanceID,
		time.Unix(conflictRecord.Bucket, 0).UTC(),
		conflictRecord.ConfigRevision,
		conflictRecord.RouteTag,
		groupPath,
		conflictRecord.ActualOutboundTag,
		conflictRecord.ActualOutboundType,
		conflictRecord.Network,
	)
	var processedBefore int64
	harness.queryRow(fmt.Sprintf(`
SELECT summary_processed
FROM %s.mbox_traffic_migration_jobs
WHERE instance_id = $1
`, harness.schemaIdentifier()), harness.instanceID).Scan(&processedBefore)
	batchesBefore := harness.tableCount("mbox_traffic_migration_batches")
	rowsBefore := harness.tableCount("mbox_traffic_minute_summary")
	_, _, err = harness.execute("--batch-size", "3")
	if !errors.Is(err, trafficcontrol.ErrTrafficMigrationConflict) {
		t.Fatalf("unexpected conflict result: %v", err)
	}
	var processedAfter int64
	harness.queryRow(fmt.Sprintf(`
SELECT summary_processed
FROM %s.mbox_traffic_migration_jobs
WHERE instance_id = $1
`, harness.schemaIdentifier()), harness.instanceID).Scan(&processedAfter)
	if harness.tableCount("mbox_traffic_minute_summary") != rowsBefore ||
		harness.tableCount("mbox_traffic_migration_batches") != batchesBefore ||
		processedBefore != 1 ||
		processedAfter != processedBefore {
		t.Fatal("conflicting second record did not roll back the entire batch")
	}
}

func TestTrafficStatisticsMigrateCommandPG14InterruptedResume(t *testing.T) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	previous := migrateBoltTrafficStatisticsToPostgres
	defer func() { migrateBoltTrafficStatisticsToPostgres = previous }()
	ctx, cancel := context.WithCancel(context.Background())
	migrateBoltTrafficStatisticsToPostgres = func(
		callCtx context.Context,
		logger log.ContextLogger,
		options trafficcontrol.TrafficStatisticsMigrationOptions,
	) (trafficcontrol.TrafficStatisticsMigrationResult, error) {
		originalProgress := options.Progress
		options.Progress = func(event trafficcontrol.TrafficStatisticsMigrationProgress) error {
			if err := originalProgress(event); err != nil {
				return err
			}
			if event.Phase == "summary_batch" {
				cancel()
			}
			return nil
		}
		return previous(callCtx, logger, options)
	}
	_, _, err := harness.executeContext(ctx, "--batch-size", "1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected interrupted result: %v", err)
	}
	migrateBoltTrafficStatisticsToPostgres = previous
	resumed := harness.run(t, "--batch-size", "2")
	if !resumed.Completed ||
		harness.tableCount("mbox_traffic_minute_summary") != 3 ||
		harness.tableCount("mbox_traffic_minute_targets") != 4 {
		t.Fatalf("resume was not lossless: %+v", resumed)
	}
}

func TestTrafficStatisticsMigrateCommandPG14ResumeSelectionRejectsMismatch(t *testing.T) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	harness.run(t)
	before := harness.tableCount("mbox_traffic_migration_batches")
	_, _, err := harness.execute(
		"--resume",
		strings.Repeat("0", 64),
	)
	if !errors.Is(err, trafficcontrol.ErrTrafficMigrationResumeMismatch) {
		t.Fatalf("unexpected resume mismatch: %v", err)
	}
	if harness.tableCount("mbox_traffic_migration_batches") != before {
		t.Fatal("resume mismatch changed target state")
	}
}

func TestTrafficStatisticsMigrateCommandPG14RejectsCorruptResumeLedger(
	t *testing.T,
) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	harness.run(t, "--batch-size", "1")
	harness.exec(fmt.Sprintf(`
DELETE FROM %s.mbox_traffic_migration_batches
WHERE instance_id = $1
    AND kind = 'summary'
    AND cursor_start = ''::bytea
`, harness.schemaIdentifier()), harness.instanceID)
	_, _, err := harness.execute()
	if !errors.Is(err, trafficcontrol.ErrTrafficMigrationResumeMismatch) {
		t.Fatalf("corrupt resume ledger was accepted: %v", err)
	}
}

func TestTrafficStatisticsMigrateCommandPG14UncertainCommitExactlyOnce(t *testing.T) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	proxy := newUncertainTrafficMigrationProxy(t, harness.targetDSN, 3)
	harness.targetDSN = proxy.dsn(t, harness.targetDSN)
	harness.writeConfig("auto")
	first := harness.run(t, "--batch-size", "1")
	assertTrafficMigrationRestoreResult(t, first, 3, 3, 0, 4, 4, 0)
	second := harness.run(t, "--batch-size", "2")
	assertTrafficMigrationRestoreResult(t, second, 3, 0, 3, 4, 0, 4)
	if !proxy.dropped.Load() ||
		first.MigrationID != second.MigrationID ||
		harness.tableCount("mbox_traffic_migration_batches") != 7 {
		t.Fatal("recovery markers do not prove exactly-once restoration")
	}
}

func TestTrafficStatisticsMigrateCommandPG14TimeBoundsAndAvailability(t *testing.T) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	result := harness.run(
		t,
		"--from",
		"2025-01-02T03:05:00Z",
		"--to",
		"2025-01-02T03:06:00Z",
	)
	assertTrafficMigrationRestoreResult(t, result, 1, 1, 0, 2, 2, 0)
	var targetsFrom time.Time
	var destinationsFrom time.Time
	harness.queryRow(fmt.Sprintf(`
SELECT target_available_from, destination_available_from
FROM %s.mbox_traffic_instances
WHERE instance_id = $1
`, harness.schemaIdentifier()), harness.instanceID).Scan(
		&targetsFrom,
		&destinationsFrom,
	)
	expected := time.Date(2025, 1, 2, 3, 5, 0, 0, time.UTC)
	if !targetsFrom.Equal(expected) || !destinationsFrom.Equal(expected) {
		t.Fatalf("unexpected bounded availability: %v %v", targetsFrom, destinationsFrom)
	}
}

func TestTrafficStatisticsMigrateCommandPG14ZeroRecordKindsCompleteTransactionally(
	t *testing.T,
) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	arguments := []string{
		"--from",
		"2030-01-01T00:00:00Z",
		"--to",
		"2030-01-01T00:01:00Z",
	}
	result := harness.run(t, arguments...)
	if !result.Completed ||
		result.Summary != (trafficcontrol.TrafficStatisticsMigrationCounts{}) ||
		result.Targets != (trafficcontrol.TrafficStatisticsMigrationCounts{}) {
		t.Fatalf("zero-record migration did not complete: %+v", result)
	}
	var summaryComplete bool
	var targetComplete bool
	var completedAtPresent bool
	if err := harness.queryRow(fmt.Sprintf(`
SELECT summary_complete, target_complete, completed_at IS NOT NULL
FROM %s.mbox_traffic_migration_jobs
WHERE instance_id = $1
`, harness.schemaIdentifier()), harness.instanceID).Scan(
		&summaryComplete,
		&targetComplete,
		&completedAtPresent,
	); err != nil {
		t.Fatal(err)
	}
	if !summaryComplete ||
		!targetComplete ||
		!completedAtPresent ||
		harness.tableCount("mbox_traffic_migration_batches") != 0 {
		t.Fatal("zero-record completion created fake markers or incomplete state")
	}
	rerun := harness.run(t, arguments...)
	if !rerun.Completed ||
		rerun.MigrationID != result.MigrationID ||
		rerun.Summary != (trafficcontrol.TrafficStatisticsMigrationCounts{}) ||
		rerun.Targets != (trafficcontrol.TrafficStatisticsMigrationCounts{}) {
		t.Fatalf("zero-record rerun was not exact: %+v", rerun)
	}
}

func TestTrafficStatisticsMigrateCommandPG14TargetIdentityAndRevisionContinuity(t *testing.T) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	harness.run(t)
	var revisionKey []byte
	var activeRevision string
	harness.queryRow(fmt.Sprintf(`
SELECT instance.revision_key, revision.config_revision
FROM %s.mbox_traffic_instances instance
JOIN %s.mbox_traffic_config_revisions revision
    ON revision.instance_id = instance.instance_id
WHERE instance.instance_id = $1
`, harness.schemaIdentifier(), harness.schemaIdentifier()), harness.instanceID).Scan(
		&revisionKey,
		&activeRevision,
	)
	expectedKey := make([]byte, 32)
	for index := range expectedKey {
		expectedKey[index] = byte(index)
	}
	if !bytes.Equal(revisionKey, expectedKey) ||
		activeRevision != "ffeeddccbbaa99887766554433221100" {
		t.Fatalf("identity continuity failed: %x %s", revisionKey, activeRevision)
	}
	identityContent, err := os.ReadFile(harness.identity)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(identityContent), hex.EncodeToString(expectedKey)) {
		t.Fatal("identity file does not contain source revision key")
	}
	conflict := newTrafficMigrationPGHarness(t, "validate")
	conflict.migrateSchema()
	conflict.exec(fmt.Sprintf(`
INSERT INTO %s.mbox_traffic_instances (
    instance_id,
    revision_key,
    target_available_from,
    destination_available_from
) VALUES ($1, decode(repeat('ff', 32), 'hex'), $2, $2)
`, conflict.schemaIdentifier()),
		conflict.instanceID,
		time.Date(2025, 1, 2, 3, 0, 0, 0, time.UTC),
	)
	_, _, err = conflict.execute()
	if !errors.Is(err, trafficcontrol.ErrTrafficMigrationIdentityConflict) {
		t.Fatalf("target identity mismatch was accepted: %v", err)
	}
	if _, statErr := os.Lstat(conflict.identity); !errors.Is(
		statErr,
		os.ErrNotExist,
	) {
		t.Fatalf("identity conflict persisted local identity: %v", statErr)
	}
}

func TestTrafficStatisticsMigrateCommandPG14StableFingerprintAndMigrationIdentity(t *testing.T) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	harness.migrateSchema()
	first := harness.run(t, "--dry-run", "--batch-size", "1")
	copyPath := filepath.Join(harness.directory, "copy.db")
	if err := os.WriteFile(
		copyPath,
		trafficMigrationFixtureBytes(t, harness.sourcePath),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	second := harness.run(
		t,
		"--dry-run",
		"--batch-size",
		"10000",
		"--source",
		copyPath,
	)
	if first.SourceFingerprint != second.SourceFingerprint ||
		first.MigrationID != second.MigrationID {
		t.Fatalf("path or batch size changed identity: %+v %+v", first, second)
	}
	changedPath := filepath.Join(harness.directory, "changed.db")
	if err := os.WriteFile(
		changedPath,
		trafficMigrationFixtureBytes(t, harness.sourcePath),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	database, err := bbolt.Open(changedPath, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = database.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(testTrafficSummaryBucket)
		cursor := bucket.Cursor()
		key, value := cursor.First()
		var record testTrafficMigrationRecord
		if decodeErr := json.Unmarshal(value, &record); decodeErr != nil {
			return decodeErr
		}
		record.DownlinkBytes++
		content, encodeErr := json.Marshal(record)
		if encodeErr != nil {
			return encodeErr
		}
		return bucket.Put(key, content)
	})
	closeErr := database.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	third := harness.run(t, "--dry-run", "--source", changedPath)
	if third.SourceFingerprint == first.SourceFingerprint ||
		third.MigrationID == first.MigrationID {
		t.Fatal("valid source-byte change did not change fingerprint and migration ID")
	}
}

func TestTrafficStatisticsMigrateCommandPG14DetectsSourceReplacementSameHash(
	t *testing.T,
) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	before := snapshotTrafficMigrationFixture(t, harness.sourcePath)
	previous := migrateBoltTrafficStatisticsToPostgres
	defer func() { migrateBoltTrafficStatisticsToPostgres = previous }()
	var replaced bool
	migrateBoltTrafficStatisticsToPostgres = func(
		ctx context.Context,
		logger log.ContextLogger,
		options trafficcontrol.TrafficStatisticsMigrationOptions,
	) (trafficcontrol.TrafficStatisticsMigrationResult, error) {
		originalProgress := options.Progress
		options.Progress = func(event trafficcontrol.TrafficStatisticsMigrationProgress) error {
			if err := originalProgress(event); err != nil {
				return err
			}
			if event.Phase != "source_validated" || replaced {
				return nil
			}
			replaced = true
			content, err := os.ReadFile(harness.sourcePath)
			if err != nil {
				return err
			}
			replacement := filepath.Join(harness.directory, "replacement.db")
			if err = os.WriteFile(replacement, content, before.mode.Perm()); err != nil {
				return err
			}
			if err = os.Chtimes(replacement, before.modTime, before.modTime); err != nil {
				return err
			}
			original := filepath.Join(harness.directory, "original.db")
			if err = os.Rename(harness.sourcePath, original); err != nil {
				return err
			}
			return os.Rename(replacement, harness.sourcePath)
		}
		return previous(ctx, logger, options)
	}
	_, _, err := harness.execute()
	if !errors.Is(err, trafficcontrol.ErrTrafficMigrationSourceChanged) {
		t.Fatalf("same-hash source replacement was not detected: %v", err)
	}
	if !replaced {
		t.Fatal("source replacement hook did not run")
	}
	assertTrafficMigrationFixtureState(t, harness.sourcePath, before)
}

func TestTrafficStatisticsMigrateCommandPG14UsesConfiguredDatabaseWithoutCreateDatabase(t *testing.T) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	before := harness.databaseNames()
	harness.run(t)
	after := harness.databaseNames()
	if !equalTrafficMigrationStrings(before, after) {
		t.Fatalf("migration changed database list: %v %v", before, after)
	}
	var version string
	var database string
	var createdb bool
	connection, err := pgx.Connect(context.Background(), harness.targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())
	if err = connection.QueryRow(
		context.Background(),
		"SELECT current_setting('server_version'), current_database(), rolcreatedb FROM pg_roles WHERE rolname = current_user",
	).Scan(&version, &database, &createdb); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(version, "14.") ||
		database == "traffic" ||
		createdb ||
		harness.tableCount("mbox_traffic_migration_jobs") != 1 {
		t.Fatalf("database proof failed: version=%s database=%s createdb=%v", version, database, createdb)
	}
}

func TestTrafficStatisticsMigrateCommandPG14DirectAndDetoured(t *testing.T) {
	direct := newTrafficMigrationPGHarness(t, "auto")
	if result := direct.run(t); !result.Completed {
		t.Fatal("direct migration did not complete")
	}
	proxy := newRecordingTrafficMigrationSOCKS(t)
	detoured := newTrafficMigrationPGHarness(t, "auto")
	detoured.writeDetourConfig("auto", proxy.address())
	if result := detoured.run(t); !result.Completed {
		t.Fatal("detoured migration did not complete")
	}
	if proxy.connections.Load() == 0 {
		t.Fatal("configured SOCKS detour did not carry PostgreSQL TCP")
	}
}

func TestTrafficStatisticsMigrateCommandPG14ConcurrentMigrationExclusion(t *testing.T) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	harness.migrateSchema()
	resolved := harness.resolvedOptions(t)
	configuration := harness.configuration(t)
	blocked := make(chan struct{})
	release := make(chan struct{})
	var firstErr error
	go func() {
		_, firstErr = trafficcontrol.MigrateBoltTrafficStatisticsToPostgres(
			context.Background(),
			log.StdLogger(),
			trafficcontrol.TrafficStatisticsMigrationOptions{
				SourcePath:    harness.sourcePath,
				Configuration: configuration,
				Traffic:       resolved,
				InstanceID:    harness.instanceID,
				BatchSize:     1,
				DryRun:        true,
				Progress: func(event trafficcontrol.TrafficStatisticsMigrationProgress) error {
					if event.Phase == "target_validated" {
						close(blocked)
						<-release
					}
					return nil
				},
			},
		)
	}()
	<-blocked
	copyPath := filepath.Join(harness.directory, "concurrent-copy.db")
	if err := os.WriteFile(
		copyPath,
		trafficMigrationFixtureBytes(t, harness.sourcePath),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	_, secondErr := trafficcontrol.MigrateBoltTrafficStatisticsToPostgres(
		context.Background(),
		log.StdLogger(),
		trafficcontrol.TrafficStatisticsMigrationOptions{
			SourcePath:    copyPath,
			Configuration: configuration,
			Traffic:       resolved,
			InstanceID:    harness.instanceID,
			BatchSize:     1,
			DryRun:        true,
		},
	)
	close(release)
	if !errors.Is(secondErr, trafficcontrol.ErrTrafficMigrationTargetInUse) {
		t.Fatalf("concurrent migration was not excluded: %v", secondErr)
	}
	if firstErr != nil {
		t.Fatal(firstErr)
	}
}

func TestTrafficStatisticsMigrateCommandPG14CancellationCleanupAndSecretRedaction(t *testing.T) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	proxy := newRecordingTrafficMigrationSOCKS(t)
	harness.writeDetourConfig("auto", proxy.address())
	previous := migrateBoltTrafficStatisticsToPostgres
	defer func() { migrateBoltTrafficStatisticsToPostgres = previous }()
	ctx, cancel := context.WithCancel(context.Background())
	migrateBoltTrafficStatisticsToPostgres = func(
		callCtx context.Context,
		logger log.ContextLogger,
		options trafficcontrol.TrafficStatisticsMigrationOptions,
	) (trafficcontrol.TrafficStatisticsMigrationResult, error) {
		originalProgress := options.Progress
		options.Progress = func(event trafficcontrol.TrafficStatisticsMigrationProgress) error {
			if err := originalProgress(event); err != nil {
				return err
			}
			if event.Phase == "target_validated" {
				cancel()
			}
			return nil
		}
		return previous(callCtx, logger, options)
	}
	stdout, stderr, err := harness.executeContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected cancellation result: %v", err)
	}
	migrateBoltTrafficStatisticsToPostgres = previous
	const secret = "mbox_schema_phase5"
	if strings.Contains(stdout, secret) ||
		strings.Contains(stderr, secret) ||
		strings.Contains(fmt.Sprint(err), secret) {
		t.Fatal("migration leaked DSN credentials")
	}
	database, openErr := bbolt.Open(
		harness.sourcePath,
		0o600,
		&bbolt.Options{Timeout: time.Second},
	)
	if openErr != nil {
		t.Fatalf("cancellation leaked source lock: %v", openErr)
	}
	_ = database.Close()
	if result := harness.run(t); !result.Completed {
		t.Fatalf("cancellation leaked advisory lock or detour runtime: %+v", result)
	}
	if proxy.connections.Load() < 2 {
		t.Fatal("detoured cancellation did not exercise both cleanup and retry")
	}
}

func TestTrafficStatisticsMigrateCommandPG14NoSecretLeakInOutputsAndMetadata(
	t *testing.T,
) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	stdout, stderr, err := harness.execute()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(harness.targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	secret, present := parsed.User.Password()
	if !present || secret == "" {
		t.Fatal("test DSN does not contain a password sentinel")
	}
	identityContent, err := os.ReadFile(harness.identity)
	if err != nil {
		t.Fatal(err)
	}
	sourceContent, err := os.ReadFile(harness.sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	var metadata string
	if err = harness.queryRow(fmt.Sprintf(`
SELECT concat_ws(
    E'\n',
    COALESCE((SELECT json_agg(j)::text FROM %s.mbox_traffic_migration_jobs j), ''),
    COALESCE((SELECT json_agg(r)::text FROM %s.mbox_traffic_migration_revisions r), ''),
    COALESCE((SELECT json_agg(b)::text FROM %s.mbox_traffic_migration_batches b), '')
)
`, harness.schemaIdentifier(),
		harness.schemaIdentifier(),
		harness.schemaIdentifier(),
	)).Scan(&metadata); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"stdout":   stdout,
		"stderr":   stderr,
		"identity": string(identityContent),
		"source":   string(sourceContent),
		"metadata": metadata,
	} {
		if strings.Contains(content, secret) {
			t.Fatalf("migration leaked the password sentinel in %s", name)
		}
	}
}

func TestTrafficStatisticsMigrateCommandPG14SchemaAutoAndValidate(t *testing.T) {
	auto := newTrafficMigrationPGHarness(t, "auto")
	auto.run(t)
	validate := newTrafficMigrationPGHarness(t, "validate")
	_, _, err := validate.execute()
	if !errors.Is(err, trafficcontrol.ErrPostgresSchemaMissing) {
		t.Fatalf("validate mode created absent schema: %v", err)
	}
	validate.migrateSchema()
	if result := validate.run(t); !result.Completed {
		t.Fatal("validate mode failed after explicit schema migration")
	}
}

func TestTrafficStatisticsMigrateCommandPG14MaxUint64(t *testing.T) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	harness.run(t)
	var value string
	harness.queryRow(fmt.Sprintf(`
SELECT uplink_bytes::text
FROM %s.mbox_traffic_minute_summary
WHERE route_tag = 'direct-route'
`, harness.schemaIdentifier())).Scan(&value)
	if value != fmt.Sprint(uint64(math.MaxUint64)) {
		t.Fatalf("math.MaxUint64 did not round-trip: %s", value)
	}
}

func newTrafficMigrationPGHarness(
	t *testing.T,
	schemaManagement string,
) *trafficMigrationPGHarness {
	t.Helper()
	adminDSN := requirePostgresCommandEnvironment(t, "MBOX_TEST_POSTGRES_ADMIN_DSN")
	targetDSN := requirePostgresCommandEnvironment(t, "MBOX_TEST_POSTGRES_SCHEMA_DSN")
	schema := "mbox_migration_" + strings.ReplaceAll(
		uuid.Must(uuid.NewV4()).String(),
		"-",
		"",
	)
	directory := t.TempDir()
	harness := &trafficMigrationPGHarness{
		t:          t,
		adminDSN:   adminDSN,
		targetDSN:  targetDSN,
		schema:     schema,
		directory:  directory,
		sourcePath: filepath.Join(directory, "traffic.db"),
		configPath: filepath.Join(directory, "config.json"),
		identity:   filepath.Join(directory, "identity.json"),
		instanceID: "migration-fixture-instance",
	}
	writeTrafficMigrationFixture(t, harness.sourcePath)
	harness.writeConfig(schemaManagement)
	t.Cleanup(func() {
		connection, err := pgx.Connect(context.Background(), adminDSN)
		if err != nil {
			t.Errorf("connect migration cleanup: %v", err)
			return
		}
		defer connection.Close(context.Background())
		if _, err = connection.Exec(
			context.Background(),
			"DROP SCHEMA IF EXISTS "+harness.schemaIdentifier()+" CASCADE",
		); err != nil {
			t.Errorf("drop migration schema: %v", err)
		}
	})
	return harness
}

func (h *trafficMigrationPGHarness) writeConfig(schemaManagement string) {
	h.t.Helper()
	h.writeConfigOptions(schemaManagement, "", "")
}

func (h *trafficMigrationPGHarness) writeDetourConfig(
	schemaManagement string,
	address string,
) {
	h.t.Helper()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		h.t.Fatal(err)
	}
	h.writeConfigOptions(schemaManagement, host, port)
}

func (h *trafficMigrationPGHarness) writeConfigOptions(
	schemaManagement string,
	detourHost string,
	detourPort string,
) {
	h.t.Helper()
	storage := map[string]any{
		"type":                 "postgres",
		"dsn":                  h.targetDSN,
		"schema":               h.schema,
		"schema_management":    schemaManagement,
		"max_open_connections": 1,
		"min_idle_connections": 0,
		"connect_timeout":      "5s",
		"statement_timeout":    "10s",
	}
	config := map[string]any{
		"experimental": map[string]any{
			"traffic_statistics": map[string]any{
				"enabled":       true,
				"instance_id":   h.instanceID,
				"identity_path": h.identity,
				"storage":       storage,
			},
		},
	}
	if detourHost != "" {
		port, err := strconv.Atoi(detourPort)
		if err != nil {
			h.t.Fatal(err)
		}
		storage["dialer"] = map[string]any{"detour": "migration-socks"}
		config["outbounds"] = []any{map[string]any{
			"type":        "socks",
			"tag":         "migration-socks",
			"server":      detourHost,
			"server_port": port,
		}}
	}
	content, err := json.Marshal(map[string]any{
		"experimental": config["experimental"],
		"outbounds":    config["outbounds"],
	})
	if err != nil {
		h.t.Fatal(err)
	}
	if err = os.WriteFile(h.configPath, content, 0o600); err != nil {
		h.t.Fatal(err)
	}
}

func (h *trafficMigrationPGHarness) migrateSchema() {
	h.t.Helper()
	output, err := executeTrafficStatisticsSchemaCommand(
		h.t,
		"tools",
		"traffic-statistics",
		"schema",
		"migrate",
		"-c",
		h.configPath,
	)
	if err != nil {
		h.t.Fatalf("migrate schema: %v\n%s", err, output)
	}
}

func (h *trafficMigrationPGHarness) run(
	t *testing.T,
	arguments ...string,
) trafficcontrol.TrafficStatisticsMigrationResult {
	t.Helper()
	stdout, stderr, err := h.execute(arguments...)
	if err != nil {
		t.Fatalf("execute migration: %v\nstdout=%s\nstderr=%s", err, stdout, stderr)
	}
	var result trafficcontrol.TrafficStatisticsMigrationResult
	if err = json.Unmarshal([]byte(strings.TrimSpace(stdout)), &result); err != nil {
		t.Fatalf("decode migration result: %v\n%s", err, stdout)
	}
	if !strings.Contains(stderr, `"phase":"source_validated"`) ||
		!strings.Contains(stderr, `"phase":"completed"`) {
		t.Fatalf("missing compact progress events: %s", stderr)
	}
	return result
}

func (h *trafficMigrationPGHarness) execute(
	arguments ...string,
) (string, string, error) {
	return h.executeContext(context.Background(), arguments...)
}

func (h *trafficMigrationPGHarness) executeContext(
	ctx context.Context,
	arguments ...string,
) (string, string, error) {
	h.t.Helper()
	resetTrafficMigrationCommandFlags(h.t)
	arguments = append([]string{
		"tools",
		"traffic-statistics",
		"migrate",
		"-c",
		h.configPath,
		"--source",
		h.sourcePath,
		"--instance-id",
		h.instanceID,
	}, arguments...)
	previousGlobalContext := globalCtx
	previousConfigPaths := configPaths
	previousConfigDirectories := configDirectories
	previousWorkingDirectory := workingDir
	previousDisableColor := disableColor
	previousOutbound := commandToolsFlagOutbound
	configFlag := mainCommand.PersistentFlags().Lookup("config")
	configDirectoryFlag := mainCommand.PersistentFlags().Lookup("config-directory")
	previousConfigChanged := configFlag.Changed
	previousConfigDirectoryChanged := configDirectoryFlag.Changed
	defer func() {
		globalCtx = previousGlobalContext
		configPaths = previousConfigPaths
		configDirectories = previousConfigDirectories
		workingDir = previousWorkingDirectory
		disableColor = previousDisableColor
		commandToolsFlagOutbound = previousOutbound
		configFlag.Changed = previousConfigChanged
		configDirectoryFlag.Changed = previousConfigDirectoryChanged
		mainCommand.SetArgs(nil)
		mainCommand.SetOut(nil)
		mainCommand.SetErr(nil)
	}()
	configPaths = nil
	configDirectories = nil
	workingDir = ""
	disableColor = true
	commandToolsFlagOutbound = ""
	configFlag.Changed = false
	configDirectoryFlag.Changed = false
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	mainCommand.SetArgs(arguments)
	mainCommand.SetOut(&stdout)
	mainCommand.SetErr(&stderr)
	commandToolsTrafficStatisticsMigrate.SetContext(ctx)
	err := mainCommand.ExecuteContext(ctx)
	commandToolsTrafficStatisticsMigrate.SetContext(context.Background())
	return stdout.String(), stderr.String(), err
}

func resetTrafficMigrationCommandFlags(t *testing.T) {
	t.Helper()
	commandToolsTrafficStatisticsMigrate.Flags().VisitAll(func(flag *pflag.Flag) {
		if err := flag.Value.Set(flag.DefValue); err != nil {
			t.Fatal(err)
		}
		flag.Changed = false
	})
}

func (h *trafficMigrationPGHarness) schemaIdentifier() string {
	return pgx.Identifier{h.schema}.Sanitize()
}

func (h *trafficMigrationPGHarness) connection() *pgx.Conn {
	h.t.Helper()
	connection, err := pgx.Connect(context.Background(), h.adminDSN)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { _ = connection.Close(context.Background()) })
	return connection
}

func (h *trafficMigrationPGHarness) exec(statement string, arguments ...any) {
	h.t.Helper()
	if _, err := h.connection().Exec(
		context.Background(),
		statement,
		arguments...,
	); err != nil {
		h.t.Fatal(err)
	}
}

func (h *trafficMigrationPGHarness) queryRow(
	statement string,
	arguments ...any,
) pgx.Row {
	h.t.Helper()
	return h.connection().QueryRow(context.Background(), statement, arguments...)
}

func (h *trafficMigrationPGHarness) tableCount(table string) int {
	h.t.Helper()
	var count int
	h.queryRow(fmt.Sprintf(
		"SELECT count(*) FROM %s.%s",
		h.schemaIdentifier(),
		pgx.Identifier{table}.Sanitize(),
	)).Scan(&count)
	return count
}

func (h *trafficMigrationPGHarness) snapshotTables() map[string]string {
	h.t.Helper()
	tables := []string{
		"mbox_traffic_schema_migrations",
		"mbox_traffic_instances",
		"mbox_traffic_config_revisions",
		"mbox_traffic_ingest_batches",
		"mbox_traffic_minute_summary",
		"mbox_traffic_minute_targets",
		"mbox_traffic_migration_jobs",
		"mbox_traffic_migration_revisions",
		"mbox_traffic_migration_batches",
	}
	snapshots := make(map[string]string, len(tables))
	for _, table := range tables {
		var snapshot string
		if err := h.queryRow(fmt.Sprintf(`
SELECT COALESCE(jsonb_agg(to_jsonb(target_row) ORDER BY to_jsonb(target_row)::text), '[]'::jsonb)::text
FROM %s.%s AS target_row
`, h.schemaIdentifier(), pgx.Identifier{table}.Sanitize())).Scan(&snapshot); err != nil {
			h.t.Fatalf("snapshot %s: %v", table, err)
		}
		snapshots[table] = snapshot
	}
	return snapshots
}

func equalTrafficMigrationTableSnapshots(
	left map[string]string,
	right map[string]string,
) bool {
	if len(left) != len(right) {
		return false
	}
	for table, snapshot := range left {
		if right[table] != snapshot {
			return false
		}
	}
	return true
}

func (h *trafficMigrationPGHarness) databaseNames() []string {
	h.t.Helper()
	rows, err := h.connection().Query(
		context.Background(),
		"SELECT datname FROM pg_database ORDER BY datname",
	)
	if err != nil {
		h.t.Fatal(err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		h.t.Fatal(err)
	}
	return names
}

func (h *trafficMigrationPGHarness) configuration(t *testing.T) option.Options {
	t.Helper()
	previous := globalCtx
	previousPaths := configPaths
	defer func() {
		globalCtx = previous
		configPaths = previousPaths
	}()
	globalCtx = include.Context(context.Background())
	configPaths = []string{h.configPath}
	configuration, err := readConfigAndMerge()
	if err != nil {
		t.Fatal(err)
	}
	return configuration
}

func (h *trafficMigrationPGHarness) resolvedOptions(
	t *testing.T,
) option.ResolvedTrafficStatisticsOptions {
	t.Helper()
	configuration := h.configuration(t)
	resolved, err := option.ResolveTrafficStatisticsOptions(
		configuration.Experimental.TrafficStatistics,
	)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func assertTrafficMigrationRestoreResult(
	t *testing.T,
	result trafficcontrol.TrafficStatisticsMigrationResult,
	summaryScanned uint64,
	summaryInserted uint64,
	summarySkipped uint64,
	targetScanned uint64,
	targetInserted uint64,
	targetSkipped uint64,
) {
	t.Helper()
	if !result.Completed ||
		result.DryRun ||
		result.Summary != (trafficcontrol.TrafficStatisticsMigrationCounts{
			Scanned:  summaryScanned,
			Inserted: summaryInserted,
			Skipped:  summarySkipped,
		}) ||
		result.Targets != (trafficcontrol.TrafficStatisticsMigrationCounts{
			Scanned:  targetScanned,
			Inserted: targetInserted,
			Skipped:  targetSkipped,
		}) {
		t.Fatalf("unexpected migration result: %+v", result)
	}
}

func equalTrafficMigrationStrings(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

type recordingTrafficMigrationSOCKS struct {
	listener    net.Listener
	connections atomic.Uint64
}

type uncertainTrafficMigrationProxy struct {
	listener     net.Listener
	target       string
	dropCommitAt int64
	commitCount  atomic.Int64
	dropped      atomic.Bool
}

func newUncertainTrafficMigrationProxy(
	t *testing.T,
	targetDSN string,
	dropCommitAt int64,
) *uncertainTrafficMigrationProxy {
	t.Helper()
	parsed, err := url.Parse(targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxy := &uncertainTrafficMigrationProxy{
		listener:     listener,
		target:       parsed.Host,
		dropCommitAt: dropCommitAt,
	}
	go proxy.serve()
	t.Cleanup(func() { _ = listener.Close() })
	return proxy
}

func (p *uncertainTrafficMigrationProxy) dsn(
	t *testing.T,
	targetDSN string,
) string {
	t.Helper()
	parsed, err := url.Parse(targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Host = p.listener.Addr().String()
	return parsed.String()
}

func (p *uncertainTrafficMigrationProxy) serve() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		go p.handle(client)
	}
}

func (p *uncertainTrafficMigrationProxy) handle(client net.Conn) {
	server, err := net.DialTimeout("tcp", p.target, 5*time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	var closeOnce sync.Once
	closeBoth := func() {
		closeOnce.Do(func() {
			_ = client.Close()
			_ = server.Close()
		})
	}
	defer closeBoth()
	var dropResponse atomic.Bool
	clientDone := make(chan struct{}, 1)
	go func() {
		defer func() { clientDone <- struct{}{} }()
		buffer := make([]byte, 32*1024)
		for {
			count, readErr := client.Read(buffer)
			if count > 0 {
				if bytes.Contains(
					bytes.ToLower(buffer[:count]),
					[]byte("commit"),
				) && p.commitCount.Add(1) == p.dropCommitAt {
					dropResponse.Store(true)
				}
				if _, writeErr := server.Write(buffer[:count]); writeErr != nil {
					return
				}
			}
			if readErr != nil {
				return
			}
		}
	}()
	buffer := make([]byte, 32*1024)
	for {
		count, readErr := server.Read(buffer)
		if count > 0 {
			if dropResponse.Load() && p.dropped.CompareAndSwap(false, true) {
				closeBoth()
				<-clientDone
				return
			}
			if _, writeErr := client.Write(buffer[:count]); writeErr != nil {
				<-clientDone
				return
			}
		}
		if readErr != nil {
			<-clientDone
			return
		}
	}
}

func newRecordingTrafficMigrationSOCKS(t *testing.T) *recordingTrafficMigrationSOCKS {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &recordingTrafficMigrationSOCKS{listener: listener}
	go server.serve()
	t.Cleanup(func() { _ = listener.Close() })
	return server
}

func (s *recordingTrafficMigrationSOCKS) address() string {
	return s.listener.Addr().String()
}

func (s *recordingTrafficMigrationSOCKS) serve() {
	for {
		connection, err := s.listener.Accept()
		if err != nil {
			return
		}
		go s.handle(connection)
	}
}

func (s *recordingTrafficMigrationSOCKS) handle(connection net.Conn) {
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(30 * time.Second))
	header := make([]byte, 2)
	if _, err := io.ReadFull(connection, header); err != nil ||
		header[0] != 5 {
		return
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(connection, methods); err != nil {
		return
	}
	if _, err := connection.Write([]byte{5, 0}); err != nil {
		return
	}
	request := make([]byte, 4)
	if _, err := io.ReadFull(connection, request); err != nil ||
		request[0] != 5 ||
		request[1] != 1 {
		return
	}
	var host string
	switch request[3] {
	case 1:
		address := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(connection, address); err != nil {
			return
		}
		host = net.IP(address).String()
	case 3:
		length := []byte{0}
		if _, err := io.ReadFull(connection, length); err != nil {
			return
		}
		address := make([]byte, int(length[0]))
		if _, err := io.ReadFull(connection, address); err != nil {
			return
		}
		host = string(address)
	case 4:
		address := make([]byte, net.IPv6len)
		if _, err := io.ReadFull(connection, address); err != nil {
			return
		}
		host = net.IP(address).String()
	default:
		return
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(connection, portBytes); err != nil {
		return
	}
	target, err := net.DialTimeout(
		"tcp",
		net.JoinHostPort(host, strconv.Itoa(int(binary.BigEndian.Uint16(portBytes)))),
		5*time.Second,
	)
	if err != nil {
		_, _ = connection.Write([]byte{5, 5, 0, 1, 0, 0, 0, 0, 0, 0})
		return
	}
	defer target.Close()
	if _, err = connection.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
		return
	}
	s.connections.Add(1)
	_ = connection.SetDeadline(time.Time{})
	_ = target.SetDeadline(time.Time{})
	done := make(chan struct{}, 1)
	go func() {
		_, _ = io.Copy(target, connection)
		done <- struct{}{}
	}()
	_, _ = io.Copy(connection, target)
	<-done
}

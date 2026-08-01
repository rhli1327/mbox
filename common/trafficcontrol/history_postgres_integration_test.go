//go:build with_postgres

package trafficcontrol

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresHistoryBackendContract(t *testing.T) {
	runHistoryBackendContract(t, newPostgresHistoryBackendHarness())
}

func TestPostgresPersistedDestinationNormalization(t *testing.T) {
	harness := newPostgresHistoryBackendHarness()
	history := harness.Open(t, []byte("destination-normalization"))
	bucket := time.Now().UTC().Truncate(HistoryBucketInterval).Unix()
	baseKey := historyKey{
		Bucket:             bucket,
		ConfigRevision:     history.configRevision,
		RouteTag:           "route",
		GroupPath:          `["group"]`,
		ActualOutboundTag:  "proxy",
		ActualOutboundType: "mieru",
		Network:            "tcp",
	}
	domainUpper := baseKey
	domainUpper.DestinationDomain = "Pending.Example."
	domainLower := baseKey
	domainLower.DestinationDomain = "pending.example"
	ipMapped := baseKey
	ipMapped.DestinationIP = "::ffff:192.0.2.44"
	ipCanonical := baseKey
	ipCanonical.DestinationIP = "192.0.2.44"
	history.access.Lock()
	history.pending[domainUpper] = historyCounters{
		UplinkBytes:   math.MaxUint64,
		DownlinkBytes: 2,
		Connections:   3,
	}
	history.pending[domainLower] = historyCounters{
		UplinkBytes:   1,
		DownlinkBytes: 4,
		Connections:   5,
	}
	history.pending[ipMapped] = historyCounters{
		UplinkBytes:   6,
		DownlinkBytes: 7,
		Connections:   8,
	}
	history.pending[ipCanonical] = historyCounters{
		UplinkBytes:   9,
		DownlinkBytes: 10,
		Connections:   11,
	}
	history.access.Unlock()

	assertPostgresNormalizedDestinations(t, history)
	if err := history.flush(); err != nil {
		t.Fatal("flush non-canonical destinations:", err)
	}
	assertPostgresNormalizedDestinations(t, history)
	if err := history.Close(); err != nil {
		t.Fatal("close destination history:", err)
	}
	reopened := harness.Open(t, []byte("destination-normalization"))
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error("close reopened destination history:", err)
		}
	}()
	assertPostgresNormalizedDestinations(t, reopened)
}

func assertPostgresNormalizedDestinations(t *testing.T, history *History) {
	t.Helper()
	domain, err := history.Query(context.Background(), HistoryQuery{
		GroupBy:      HistoryGroupByDestination,
		Destinations: []string{"pending.example"},
	})
	if err != nil {
		t.Fatal("query normalized domain:", err)
	}
	if domain.TotalRows != 1 || len(domain.Rows) != 1 ||
		domain.Rows[0].Destination != "pending.example" ||
		domain.Rows[0].DestinationDomain != "pending.example" ||
		domain.Rows[0].DestinationType != HistoryDestinationTypeDomain ||
		domain.Totals != (HistoryCounters{
			UplinkBytes:   math.MaxUint64,
			DownlinkBytes: 6,
			Connections:   8,
		}) {
		t.Fatalf("unexpected normalized domain result: %#v", domain)
	}
	address, err := history.Query(context.Background(), HistoryQuery{
		GroupBy:      HistoryGroupByDestination,
		Destinations: []string{"192.0.2.44"},
	})
	if err != nil {
		t.Fatal("query normalized IP:", err)
	}
	if address.TotalRows != 1 || len(address.Rows) != 1 ||
		address.Rows[0].Destination != "192.0.2.44" ||
		address.Rows[0].DestinationType != HistoryDestinationTypeIP ||
		address.Totals != (HistoryCounters{
			UplinkBytes:   15,
			DownlinkBytes: 17,
			Connections:   19,
		}) {
		t.Fatalf("unexpected normalized IP result: %#v", address)
	}
}

func TestPostgresSchemaEmptyAuto(t *testing.T) {
	schema := newPostgresTestSchema(t)
	if err := migrateTestPostgresSchema(t, testPostgresSchemaDSN, schema); err != nil {
		t.Fatal(err)
	}
	var count int
	testPostgresAdminQueryRow(t, fmt.Sprintf(`
SELECT count(*)
FROM %s.mbox_traffic_schema_migrations
`, pgx.Identifier{schema}.Sanitize())).Scan(&count)
	if count != currentPostgresSchemaVersion {
		t.Fatalf("unexpected migration count: %d", count)
	}
}

func TestPostgresSchemaRestartIdempotent(t *testing.T) {
	schema := newPostgresTestSchema(t)
	for range 3 {
		if err := migrateTestPostgresSchema(t, testPostgresSchemaDSN, schema); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	testPostgresAdminQueryRow(t, fmt.Sprintf(`
SELECT count(*)
FROM %s.mbox_traffic_schema_migrations
`, pgx.Identifier{schema}.Sanitize())).Scan(&count)
	if count != currentPostgresSchemaVersion {
		t.Fatalf("unexpected migration count: %d", count)
	}
}

func TestPostgresSchemaConcurrentInitializers(t *testing.T) {
	schema := newPostgresTestSchema(t)
	var waitGroup sync.WaitGroup
	errorsChannel := make(chan error, 12)
	for range 12 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			errorsChannel <- migrateTestPostgresSchema(t, testPostgresSchemaDSN, schema)
		}()
	}
	waitGroup.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPostgresSchemaUpgradeV1ToV2(t *testing.T) {
	schema := newPostgresTestSchema(t)
	migrations, err := loadPostgresMigrations()
	if err != nil {
		t.Fatal(err)
	}
	pool, err := newPostgresPool(context.Background(), testPostgresConnectionOptions(
		testPostgresSchemaDSN,
	))
	if err != nil {
		t.Fatal(err)
	}
	if err = migratePostgresSchema(
		context.Background(),
		pool,
		schema,
		migrations[:1],
	); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()
	var before bool
	testPostgresAdminQueryRow(
		t,
		"SELECT to_regclass($1) IS NOT NULL",
		schema+".mbox_traffic_ingest_batches",
	).Scan(&before)
	if before {
		t.Fatal("v2 table exists before the v2 migration")
	}
	if err = migrateTestPostgresSchema(t, testPostgresSchemaDSN, schema); err != nil {
		t.Fatal(err)
	}
	var after bool
	testPostgresAdminQueryRow(
		t,
		"SELECT to_regclass($1) IS NOT NULL",
		schema+".mbox_traffic_ingest_batches",
	).Scan(&after)
	if !after {
		t.Fatal("v2 table was not created")
	}
}

func TestPostgresSchemaUpgradeV2ToV3(t *testing.T) {
	schema := newPostgresTestSchema(t)
	migrations, err := loadPostgresMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if currentPostgresSchemaVersion != 3 || len(migrations) != 3 {
		t.Fatalf(
			"PostgreSQL traffic schema v3 is absent: version=%d migrations=%d",
			currentPostgresSchemaVersion,
			len(migrations),
		)
	}
	pool, err := newPostgresPool(context.Background(), testPostgresConnectionOptions(
		testPostgresSchemaDSN,
	))
	if err != nil {
		t.Fatal(err)
	}
	if err = migratePostgresSchema(
		context.Background(),
		pool,
		schema,
		migrations[:2],
	); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	var before bool
	testPostgresAdminQueryRow(
		t,
		"SELECT to_regclass($1) IS NOT NULL",
		schema+".mbox_traffic_migration_jobs",
	).Scan(&before)
	if before {
		pool.Close()
		t.Fatal("v3 migration table exists before v3 upgrade")
	}
	if err = migratePostgresSchema(
		context.Background(),
		pool,
		schema,
		migrations,
	); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if err = validatePostgresSchema(
		context.Background(),
		pool,
		schema,
		migrations,
	); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()
	for _, table := range []string{
		"mbox_traffic_migration_jobs",
		"mbox_traffic_migration_revisions",
		"mbox_traffic_migration_batches",
	} {
		var exists bool
		testPostgresAdminQueryRow(
			t,
			"SELECT to_regclass($1) IS NOT NULL",
			schema+"."+table,
		).Scan(&exists)
		if !exists {
			t.Fatalf("v3 table %s was not created", table)
		}
	}
	rollbackSchema := newPostgresTestSchema(t)
	rollbackPool, err := newPostgresPool(
		context.Background(),
		testPostgresConnectionOptions(testPostgresSchemaDSN),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rollbackPool.Close()
	if err = migratePostgresSchema(
		context.Background(),
		rollbackPool,
		rollbackSchema,
		migrations[:2],
	); err != nil {
		t.Fatal(err)
	}
	broken := append([]postgresMigration(nil), migrations...)
	broken[2].SQL += "\n-- mbox:statement\nSELECT phase5_broken_migration;"
	broken[2].Checksum = sha256.Sum256([]byte(broken[2].SQL))
	if err = migratePostgresSchema(
		context.Background(),
		rollbackPool,
		rollbackSchema,
		broken,
	); err == nil {
		t.Fatal("broken v3 migration unexpectedly succeeded")
	}
	var rolledBack bool
	testPostgresAdminQueryRow(
		t,
		"SELECT to_regclass($1) IS NULL",
		rollbackSchema+".mbox_traffic_migration_jobs",
	).Scan(&rolledBack)
	if !rolledBack {
		t.Fatal("failed v3 migration left migration metadata tables")
	}
}

func TestPostgresSchemaV3MigrationMetadataValidated(t *testing.T) {
	migrations, err := loadPostgresMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != 3 ||
		migrations[2].Version != 3 ||
		migrations[2].Name != "003_migration.sql" {
		t.Fatalf("PostgreSQL traffic migration metadata schema is absent: %#v", migrations)
	}
	for name, mutation := range map[string]string{
		"extra_column": `
ALTER TABLE %s.mbox_traffic_migration_jobs
ADD COLUMN future_column text`,
		"column_default": `
ALTER TABLE %s.mbox_traffic_migration_jobs
ALTER COLUMN summary_cursor SET DEFAULT decode('00', 'hex')`,
		"unique_constraint": `
ALTER TABLE %s.mbox_traffic_migration_batches
DROP CONSTRAINT mbox_traffic_migration_batches_start_key`,
		"source_index": `
DROP INDEX %s.mbox_traffic_migration_jobs_source_idx`,
	} {
		t.Run(name, func(t *testing.T) {
			schema := newPostgresTestSchema(t)
			if err := migrateTestPostgresSchema(
				t,
				testPostgresSchemaDSN,
				schema,
			); err != nil {
				t.Fatal(err)
			}
			testPostgresAdminExec(
				t,
				fmt.Sprintf(mutation, pgx.Identifier{schema}.Sanitize()),
			)
			err := validateTestPostgresSchema(t, testPostgresSchemaDSN, schema)
			if !errors.Is(err, ErrPostgresSchemaIncompatible) {
				t.Fatalf("unexpected validation result: %v", err)
			}
		})
	}
}

func TestTrafficStatisticsMigrationIDVector(t *testing.T) {
	var sourceFingerprint [sha256.Size]byte
	var routingFingerprint [sha256.Size]byte
	for index := range sourceFingerprint {
		sourceFingerprint[index] = byte(index)
		routingFingerprint[index] = byte(255 - index)
	}
	const expected = "fd30390f14268e30476d48e45f2785d37ff68407dbc7da55a1799a0931e0f6b3"
	actual := buildTrafficMigrationID(
		sourceFingerprint,
		123456,
		"migration-fixture-instance",
		"mbox_fixed",
		routingFingerprint,
		math.MinInt64,
		math.MaxInt64,
		"00112233445566778899aabbccddeeff",
	)
	if actual != expected {
		t.Fatalf("migration ID framing changed: %s", actual)
	}
}

func TestPostgresSchemaFailedMigrationRollsBack(t *testing.T) {
	schema := newPostgresTestSchema(t)
	migrations, err := loadPostgresMigrations()
	if err != nil {
		t.Fatal(err)
	}
	broken := append([]postgresMigration(nil), migrations...)
	broken[1].SQL = "CREATE TABLE {{schema}}.should_rollback (id integer);\n" +
		"-- mbox:statement\n" +
		"SELECT broken;"
	broken[1].Checksum = [32]byte{1}
	pool, err := newPostgresPool(context.Background(), testPostgresConnectionOptions(
		testPostgresSchemaDSN,
	))
	if err != nil {
		t.Fatal(err)
	}
	err = migratePostgresSchema(context.Background(), pool, schema, broken)
	pool.Close()
	if err == nil {
		t.Fatal("broken migration unexpectedly succeeded")
	}
	var exists bool
	testPostgresAdminQueryRow(t, `
SELECT EXISTS (
    SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1
)`, schema).Scan(&exists)
	if exists {
		t.Fatal("failed migration left its schema behind")
	}
}

func TestPostgresSchemaRejectsFutureVersion(t *testing.T) {
	schema := newPostgresTestSchema(t)
	if err := migrateTestPostgresSchema(t, testPostgresSchemaDSN, schema); err != nil {
		t.Fatal(err)
	}
	testPostgresAdminExec(t, fmt.Sprintf(`
INSERT INTO %s.mbox_traffic_schema_migrations (version, name, checksum)
VALUES (%d, 'future.sql', decode(repeat('00', 32), 'hex'))
`, pgx.Identifier{schema}.Sanitize(), currentPostgresSchemaVersion+1))
	err := validateTestPostgresSchema(t, testPostgresSchemaDSN, schema)
	if !errors.Is(err, ErrPostgresSchemaFuture) {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestPostgresSchemaRejectsChecksumMismatch(t *testing.T) {
	schema := newPostgresTestSchema(t)
	if err := migrateTestPostgresSchema(t, testPostgresSchemaDSN, schema); err != nil {
		t.Fatal(err)
	}
	testPostgresAdminExec(t, fmt.Sprintf(`
UPDATE %s.mbox_traffic_schema_migrations
SET checksum = decode(repeat('ff', 32), 'hex')
WHERE version = 1
`, pgx.Identifier{schema}.Sanitize()))
	err := validateTestPostgresSchema(t, testPostgresSchemaDSN, schema)
	if !errors.Is(err, ErrPostgresSchemaIncompatible) {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestPostgresSchemaRejectsNonContiguousLedger(t *testing.T) {
	schema := newPostgresTestSchema(t)
	if err := migrateTestPostgresSchema(t, testPostgresSchemaDSN, schema); err != nil {
		t.Fatal(err)
	}
	testPostgresAdminExec(t, fmt.Sprintf(`
DELETE FROM %s.mbox_traffic_schema_migrations
WHERE version = 1
`, pgx.Identifier{schema}.Sanitize()))
	err := validateTestPostgresSchema(t, testPostgresSchemaDSN, schema)
	if !errors.Is(err, ErrPostgresSchemaIncompatible) {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestPostgresSchemaValidateDoesNotDDL(t *testing.T) {
	schema := newPostgresTestSchema(t)
	err := validateTestPostgresSchema(t, testPostgresValidateDSN, schema)
	if !errors.Is(err, ErrPostgresSchemaMissing) {
		t.Fatalf("unexpected validation error: %v", err)
	}
	var exists bool
	testPostgresAdminQueryRow(t, `
SELECT EXISTS (
    SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1
)`, schema).Scan(&exists)
	if exists {
		t.Fatal("validate mode created a schema")
	}
}

func TestPostgresSchemaMixedCaseAutoValidate(t *testing.T) {
	schema := newPostgresTestSchemaWithPrefix(t, "TenantA_")
	if err := option.ValidateTrafficStatisticsSchema(schema); err != nil {
		t.Fatal("production schema validator rejected mixed-case schema:", err)
	}
	for range 2 {
		if err := migrateTestPostgresSchema(t, testPostgresSchemaDSN, schema); err != nil {
			t.Fatal("auto migrate mixed-case schema:", err)
		}
	}
	if err := validateTestPostgresSchema(t, testPostgresSchemaDSN, schema); err != nil {
		t.Fatal("validate mixed-case schema:", err)
	}
	var exactExists bool
	var lowercaseExists bool
	testPostgresAdminQueryRow(t, `
SELECT
    EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1),
    EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $2)
`, schema, strings.ToLower(schema)).Scan(&exactExists, &lowercaseExists)
	if !exactExists || lowercaseExists {
		t.Fatalf(
			"mixed-case schema resolution changed: exact=%v lowercase=%v",
			exactExists,
			lowercaseExists,
		)
	}
}

func TestPostgresSchemaValidateRejectsMutatedDefinitions(t *testing.T) {
	mutations := map[string]func(t *testing.T, schemaIdentifier string){
		"group_path_type": func(t *testing.T, schemaIdentifier string) {
			testPostgresAdminExec(t, `
ALTER TABLE `+schemaIdentifier+`.mbox_traffic_minute_summary
ALTER COLUMN group_path TYPE integer[] USING ARRAY[]::integer[]`)
		},
		"summary_primary_key_order": func(t *testing.T, schemaIdentifier string) {
			testPostgresAdminExec(t, `
ALTER TABLE `+schemaIdentifier+`.mbox_traffic_minute_summary
DROP CONSTRAINT mbox_traffic_minute_summary_pkey,
ADD CONSTRAINT mbox_traffic_minute_summary_pkey PRIMARY KEY (
    instance_id,
    config_revision,
    bucket_start,
    route_tag,
    group_path,
    actual_outbound_tag,
    actual_outbound_type,
    network
)`)
		},
		"instance_foreign_key_delete_action": func(t *testing.T, schemaIdentifier string) {
			testPostgresAdminExec(t, `
ALTER TABLE `+schemaIdentifier+`.mbox_traffic_config_revisions
DROP CONSTRAINT mbox_traffic_config_revisions_instance_fk,
ADD CONSTRAINT mbox_traffic_config_revisions_instance_fk
FOREIGN KEY (instance_id)
REFERENCES `+schemaIdentifier+`.mbox_traffic_instances(instance_id)
ON DELETE NO ACTION`)
		},
		"uint64_counter_check": func(t *testing.T, schemaIdentifier string) {
			testPostgresAdminExec(t, `
ALTER TABLE `+schemaIdentifier+`.mbox_traffic_minute_summary
DROP CONSTRAINT mbox_traffic_minute_summary_uplink,
ADD CONSTRAINT mbox_traffic_minute_summary_uplink
CHECK (uplink_bytes >= 0)`)
		},
		"domain_partial_index_predicate": func(t *testing.T, schemaIdentifier string) {
			testPostgresAdminExec(t, `
DROP INDEX `+schemaIdentifier+`.mbox_traffic_targets_domain_idx;
CREATE INDEX mbox_traffic_targets_domain_idx
ON `+schemaIdentifier+`.mbox_traffic_minute_targets (
    instance_id,
    destination_domain,
    bucket_start
)`)
		},
		"created_at_default": func(t *testing.T, schemaIdentifier string) {
			testPostgresAdminExec(t, `
ALTER TABLE `+schemaIdentifier+`.mbox_traffic_instances
ALTER COLUMN created_at DROP DEFAULT`)
		},
		"accepted_at_default": func(t *testing.T, schemaIdentifier string) {
			testPostgresAdminExec(t, `
ALTER TABLE `+schemaIdentifier+`.mbox_traffic_ingest_batches
ALTER COLUMN accepted_at DROP DEFAULT`)
		},
		"extra_not_null_column": func(t *testing.T, schemaIdentifier string) {
			testPostgresAdminExec(t, `
ALTER TABLE `+schemaIdentifier+`.mbox_traffic_minute_summary
ADD COLUMN poison text NOT NULL`)
		},
		"generated_counter_column": func(t *testing.T, schemaIdentifier string) {
			testPostgresAdminExec(t, `
ALTER TABLE `+schemaIdentifier+`.mbox_traffic_minute_summary
DROP CONSTRAINT mbox_traffic_minute_summary_uplink;
ALTER TABLE `+schemaIdentifier+`.mbox_traffic_minute_summary
DROP COLUMN uplink_bytes;
ALTER TABLE `+schemaIdentifier+`.mbox_traffic_minute_summary
ADD COLUMN uplink_bytes numeric(20,0)
GENERATED ALWAYS AS (0::numeric) STORED NOT NULL;
ALTER TABLE `+schemaIdentifier+`.mbox_traffic_minute_summary
ADD CONSTRAINT mbox_traffic_minute_summary_uplink
CHECK (
    uplink_bytes >= 0::numeric
    AND uplink_bytes <= '18446744073709551615'::numeric
)`)
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			schema := newPostgresTestSchema(t)
			if err := migrateTestPostgresSchema(t, testPostgresSchemaDSN, schema); err != nil {
				t.Fatal(err)
			}
			mutate(t, pgx.Identifier{schema}.Sanitize())
			err := validateTestPostgresSchema(t, testPostgresSchemaDSN, schema)
			if !errors.Is(err, ErrPostgresSchemaIncompatible) {
				t.Fatalf("mutated schema was accepted: %v", err)
			}
		})
	}
}

func TestPostgresSchemaValidateWithNoDDLRole(t *testing.T) {
	schema := newPostgresTestSchema(t)
	provisionPostgresTestSchema(t, schema)
	assertPostgresValidateRoleHasNoDDL(t, schema)
	before := postgresTestSchemaFingerprint(t, schema)
	if err := validateTestPostgresSchema(t, testPostgresValidateDSN, schema); err != nil {
		t.Fatal("validate-only role could not validate compatible schema:", err)
	}
	after := postgresTestSchemaFingerprint(t, schema)
	if after != before {
		t.Fatal("validate-only operation changed schema objects")
	}
}

func TestPostgresSchemaAutoRejectsNoDDLRole(t *testing.T) {
	schema := newPostgresTestSchema(t)
	provisionPostgresTestSchema(t, schema)
	assertPostgresValidateRoleHasNoDDL(t, schema)
	before := postgresTestSchemaFingerprint(t, schema)
	err := migrateTestPostgresSchema(t, testPostgresValidateDSN, schema)
	if !errors.Is(err, ErrPostgresPermission) {
		t.Fatalf("unexpected validate-only auto migration result: %v", err)
	}
	after := postgresTestSchemaFingerprint(t, schema)
	if after != before {
		t.Fatal("rejected auto migration changed schema objects")
	}
}

func TestPostgresSchemaInsufficientPrivileges(t *testing.T) {
	schema := newPostgresTestSchema(t)
	var applicationRole string
	testPostgresQueryRow(t, testPostgresDSN, "SELECT current_user").Scan(&applicationRole)
	quotedRole := pgx.Identifier{applicationRole}.Sanitize()
	testPostgresAdminExec(t, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize())
	testPostgresAdminExec(t, "REVOKE ALL ON SCHEMA "+
		pgx.Identifier{schema}.Sanitize()+" FROM PUBLIC")
	testPostgresAdminExec(t, "REVOKE ALL ON SCHEMA "+
		pgx.Identifier{schema}.Sanitize()+" FROM "+quotedRole)
	err := migrateTestPostgresSchema(t, testPostgresDSN, schema)
	if !errors.Is(err, ErrPostgresPermission) {
		t.Fatalf("unexpected privilege error: %v", err)
	}
}

func TestPostgresArbitraryDatabaseAndSchema(t *testing.T) {
	schema := newPostgresTestSchema(t)
	if schema == "public" || schema == "traffic" {
		t.Fatalf("test schema is not arbitrary: %s", schema)
	}
	if err := migrateTestPostgresSchema(t, testPostgresSchemaDSN, schema); err != nil {
		t.Fatal(err)
	}
	var database string
	testPostgresQueryRow(t, testPostgresDSN, "SELECT current_database()").Scan(&database)
	if database == "" || database == "traffic" {
		t.Fatalf("test database is not arbitrary: %q", database)
	}
}

func TestPostgresStoreIdentityKeyConflict(t *testing.T) {
	schema := newPostgresTestSchema(t)
	instanceID := newPostgresTestID(t)
	first, _ := newPostgresTestStore(t, schema, instanceID, bytesOf(1))
	secondOptions := testPostgresStoreOptions(schema, instanceID, bytesOf(2))
	second, err := newPostgresStore(context.Background(), secondOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	_, err = second.Open()
	if !errors.Is(err, ErrPostgresIdentityConflict) {
		t.Fatalf("unexpected identity conflict: %v", err)
	}
	_ = first
}

func TestPostgresStoreScopesTwoInstances(t *testing.T) {
	schema := newPostgresTestSchema(t)
	first, firstState := newPostgresTestStore(
		t,
		schema,
		newPostgresTestID(t),
		bytesOf(1),
	)
	second, secondState := newPostgresTestStore(
		t,
		schema,
		newPostgresTestID(t),
		bytesOf(2),
	)
	key := testPostgresHistoryKey(firstState.targetsFrom)
	if err := first.Write(historyBatch{key: {UplinkBytes: 11}}); err != nil {
		t.Fatal(err)
	}
	if err := second.Write(historyBatch{key: {UplinkBytes: 29}}); err != nil {
		t.Fatal(err)
	}
	firstResult := queryPostgresTestStore(t, first, firstState)
	secondResult := queryPostgresTestStore(t, second, secondState)
	if firstResult.Totals.UplinkBytes != 11 ||
		secondResult.Totals.UplinkBytes != 29 {
		t.Fatalf("instances were not isolated: %+v %+v", firstResult, secondResult)
	}
	destinationResult := queryPostgresTestStoreWithQuery(
		t,
		first,
		firstState,
		HistoryQuery{
			GroupBy:      HistoryGroupByDestination,
			Destinations: []string{"example.com"},
		},
	)
	if destinationResult.TotalRows != 1 ||
		destinationResult.Rows[0].Destination != "example.com" ||
		destinationResult.Totals.UplinkBytes != 11 {
		t.Fatalf("target query did not preserve compatibility: %+v", destinationResult)
	}
}

func TestPostgresStoreNumericSaturation(t *testing.T) {
	schema := newPostgresTestSchema(t)
	store, state := newPostgresTestStore(
		t,
		schema,
		newPostgresTestID(t),
		bytesOf(3),
	)
	key := testPostgresHistoryKey(time.Now())
	firstID := uuid.Must(uuid.NewV4())
	secondID := uuid.Must(uuid.NewV4())
	if err := store.applyBatch(
		context.Background(),
		firstID,
		historyBatch{key: {UplinkBytes: math.MaxUint64}},
	); err != nil {
		t.Fatal(err)
	}
	if err := store.applyBatch(
		context.Background(),
		secondID,
		historyBatch{key: {UplinkBytes: 1}},
	); err != nil {
		t.Fatal(err)
	}
	result := queryPostgresTestStore(t, store, state)
	if result.Totals.UplinkBytes != math.MaxUint64 {
		t.Fatalf("counter did not saturate: %d", result.Totals.UplinkBytes)
	}
}

func TestPostgresStoreApplyBatchIsIdempotent(t *testing.T) {
	schema := newPostgresTestSchema(t)
	store, state := newPostgresTestStore(
		t,
		schema,
		newPostgresTestID(t),
		bytesOf(4),
	)
	batchID := uuid.Must(uuid.NewV4())
	batch := historyBatch{
		testPostgresHistoryKey(time.Now()): {Connections: 7},
	}
	if err := store.applyBatch(context.Background(), batchID, batch); err != nil {
		t.Fatal(err)
	}
	if err := store.applyBatch(context.Background(), batchID, batch); err != nil {
		t.Fatal(err)
	}
	result := queryPostgresTestStore(t, store, state)
	if result.Totals.Connections != 7 {
		t.Fatalf("idempotent retry duplicated counters: %d", result.Totals.Connections)
	}
}

func TestPostgresStoreRejectsBatchIDCollision(t *testing.T) {
	schema := newPostgresTestSchema(t)
	store, _ := newPostgresTestStore(
		t,
		schema,
		newPostgresTestID(t),
		bytesOf(5),
	)
	batchID := uuid.Must(uuid.NewV4())
	key := testPostgresHistoryKey(time.Now())
	if err := store.applyBatch(
		context.Background(),
		batchID,
		historyBatch{key: {Connections: 1}},
	); err != nil {
		t.Fatal(err)
	}
	err := store.applyBatch(
		context.Background(),
		batchID,
		historyBatch{key: {Connections: 2}},
	)
	if !errors.Is(err, ErrPostgresBatchCollision) {
		t.Fatalf("unexpected collision result: %v", err)
	}
}

func TestPostgresStoreRetention(t *testing.T) {
	schema := newPostgresTestSchema(t)
	store, state := newPostgresTestStore(
		t,
		schema,
		newPostgresTestID(t),
		bytesOf(6),
	)
	now := time.Now().UTC()
	oldKey := testPostgresHistoryKey(now.Add(-HistoryRetention - time.Hour))
	newKey := testPostgresHistoryKey(now)
	if err := store.Write(historyBatch{
		oldKey: {Connections: 1},
		newKey: {Connections: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Cleanup(now); err != nil {
		t.Fatal(err)
	}
	result := queryPostgresTestStore(t, store, state)
	if result.Totals.Connections != 2 {
		t.Fatalf("retention kept expired records: %+v", result)
	}
	var markers int
	testPostgresAdminQueryRow(t, fmt.Sprintf(`
SELECT count(*)
FROM %s.mbox_traffic_ingest_batches
WHERE instance_id = $1
`, pgx.Identifier{schema}.Sanitize()), store.identity.InstanceID).Scan(&markers)
	if markers != 1 {
		t.Fatalf("retention deleted ingest markers: %d", markers)
	}
}

func TestPostgresStoreQuerySnapshot(t *testing.T) {
	schema := newPostgresTestSchema(t)
	store, state := newPostgresTestStore(
		t,
		schema,
		newPostgresTestID(t),
		bytesOf(7),
	)
	key := testPostgresHistoryKey(time.Now())
	if err := store.Write(historyBatch{key: {Connections: 1}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.BeginRead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if err = store.Write(historyBatch{key: {Connections: 2}}); err != nil {
		t.Fatal(err)
	}
	query, err := normalizeHistoryQuery(HistoryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := snapshot.Query(
		context.Background(),
		query,
		historyQueryOverlay{
			pending: historyBatch{
				key: {Connections: 4},
			},
			targetAvailableFrom:      state.targetsFrom,
			destinationAvailableFrom: state.destinationsFrom,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Totals.Connections != 5 {
		t.Fatalf("snapshot observed a later commit: %+v", result)
	}
	latest := queryPostgresTestStore(t, store, state)
	if latest.Totals.Connections != 3 {
		t.Fatalf("new snapshot missed committed data: %+v", latest)
	}
}

func TestPostgresStoreQueryCancellation(t *testing.T) {
	schema := newPostgresTestSchema(t)
	store, state := newPostgresTestStore(
		t,
		schema,
		newPostgresTestID(t),
		bytesOf(8),
	)
	snapshot, err := store.BeginRead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	query, err := normalizeHistoryQuery(HistoryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = snapshot.Query(ctx, query, testPostgresOverlay(state))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected cancellation error: %v", err)
	}
}

func TestPostgresSSLModeDisable(t *testing.T) {
	schema := newPostgresTestSchema(t)
	if !strings.Contains(testPostgresDSN, "sslmode=disable") {
		t.Fatal("MBOX_TEST_POSTGRES_DSN must explicitly use sslmode=disable")
	}
	provisionPostgresTestSchema(t, schema)
	store, err := newPostgresStore(
		context.Background(),
		testPostgresStoreOptionsWithDSN(
			schema,
			newPostgresTestID(t),
			bytesOf(7),
			testPostgresDSN,
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.Open(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresSSLModeVerifyFull(t *testing.T) {
	schema := newPostgresTestSchema(t)
	if !strings.Contains(testPostgresTLSDSN, "sslmode=verify-full") {
		t.Fatal("MBOX_TEST_POSTGRES_TLS_DSN must explicitly use sslmode=verify-full")
	}
	if _, err := os.Stat(testPostgresRootCert); err != nil {
		t.Fatal(err)
	}
	provisionPostgresTestSchema(t, schema)
	store, err := newPostgresStore(
		context.Background(),
		testPostgresStoreOptionsWithDSN(
			schema,
			newPostgresTestID(t),
			bytesOf(8),
			testPostgresTLSDSN,
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.Open(); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresSSLRejectsWrongCA(t *testing.T) {
	config, err := pgxpool.ParseConfig(testPostgresTLSDSN)
	if err != nil {
		t.Fatal(err)
	}
	if config.ConnConfig.TLSConfig == nil {
		t.Fatal("verify-full DSN did not configure TLS")
	}
	config.ConnConfig.TLSConfig.RootCAs = x509.NewCertPool()
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = pool.Ping(context.Background()); err == nil {
		t.Fatal("verify-full accepted an untrusted certificate authority")
	}
}

func TestPostgresSSLRejectsWrongHostname(t *testing.T) {
	config, err := pgxpool.ParseConfig(testPostgresTLSDSN)
	if err != nil {
		t.Fatal(err)
	}
	if config.ConnConfig.TLSConfig == nil {
		t.Fatal("verify-full DSN did not configure TLS")
	}
	config.ConnConfig.TLSConfig.ServerName = "wrong-hostname.invalid"
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err = pool.Ping(context.Background()); err == nil {
		t.Fatal("verify-full accepted a certificate for the wrong hostname")
	}
}

func newPostgresTestSchema(t *testing.T) string {
	return newPostgresTestSchemaWithPrefix(t, "mbox_test_")
}

func newPostgresTestSchemaWithPrefix(t *testing.T, prefix string) string {
	t.Helper()
	schema := prefix + strings.ReplaceAll(newPostgresTestID(t), "-", "")
	if err := option.ValidateTrafficStatisticsSchema(schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		connection, err := pgx.Connect(context.Background(), testPostgresAdminDSN)
		if err != nil {
			t.Errorf("connect admin cleanup: %v", err)
			return
		}
		defer connection.Close(context.Background())
		_, err = connection.Exec(
			context.Background(),
			"DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE",
		)
		if err != nil {
			t.Errorf("drop test schema: %v", err)
		}
	})
	return schema
}

func provisionPostgresTestSchema(t *testing.T, schema string) {
	t.Helper()
	if err := migrateTestPostgresSchema(t, testPostgresSchemaDSN, schema); err != nil {
		t.Fatal("migrate test schema:", err)
	}
	dataRole := postgresTestCurrentUser(t, testPostgresDSN)
	validateRole := postgresTestCurrentUser(t, testPostgresValidateDSN)
	schemaIdentifier := pgx.Identifier{schema}.Sanitize()
	testPostgresAdminExec(
		t,
		"GRANT USAGE ON SCHEMA "+schemaIdentifier+" TO "+
			pgx.Identifier{dataRole}.Sanitize()+", "+
			pgx.Identifier{validateRole}.Sanitize(),
	)
	testPostgresAdminExec(
		t,
		"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "+
			schemaIdentifier+" TO "+pgx.Identifier{dataRole}.Sanitize(),
	)
	testPostgresAdminExec(
		t,
		"GRANT SELECT ON "+schemaIdentifier+
			".mbox_traffic_schema_migrations TO "+
			pgx.Identifier{validateRole}.Sanitize(),
	)
}

func postgresTestCurrentUser(t *testing.T, dsn string) string {
	t.Helper()
	var role string
	if err := testPostgresQueryRow(t, dsn, "SELECT current_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	return role
}

func assertPostgresValidateRoleHasNoDDL(t *testing.T, schema string) {
	t.Helper()
	var databaseCreate bool
	var schemaCreate bool
	var schemaOwner bool
	if err := testPostgresQueryRow(t, testPostgresValidateDSN, `
SELECT
    has_database_privilege(current_user, current_database(), 'CREATE'),
    has_schema_privilege(current_user, $1, 'CREATE'),
    namespace.nspowner = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname = current_user)
FROM pg_catalog.pg_namespace namespace
WHERE namespace.nspname = $1
`, schema).Scan(&databaseCreate, &schemaCreate, &schemaOwner); err != nil {
		t.Fatal(err)
	}
	if databaseCreate || schemaCreate || schemaOwner {
		t.Fatalf(
			"validate-only role has DDL privileges: database=%v schema=%v owner=%v",
			databaseCreate,
			schemaCreate,
			schemaOwner,
		)
	}
}

func postgresTestSchemaFingerprint(t *testing.T, schema string) string {
	t.Helper()
	var fingerprint string
	statement := fmt.Sprintf(`
WITH objects AS (
    SELECT
        'column'::text AS kind,
        class.relname || '.' || attribute.attname AS name,
        pg_catalog.format_type(attribute.atttypid, attribute.atttypmod)
            || '|' || attribute.attnotnull::text
            || '|' || COALESCE(pg_catalog.pg_get_expr(definition.adbin, definition.adrelid, true), '')
            AS definition
    FROM pg_catalog.pg_namespace namespace
    JOIN pg_catalog.pg_class class ON class.relnamespace = namespace.oid
    JOIN pg_catalog.pg_attribute attribute ON attribute.attrelid = class.oid
    LEFT JOIN pg_catalog.pg_attrdef definition
        ON definition.adrelid = class.oid AND definition.adnum = attribute.attnum
    WHERE namespace.nspname = $1
        AND class.relkind = 'r'
        AND attribute.attnum > 0
        AND NOT attribute.attisdropped
    UNION ALL
    SELECT
        'constraint',
        class.relname || '.' || constraint_entry.conname,
        constraint_entry.contype::text || '|'
            || pg_catalog.pg_get_constraintdef(constraint_entry.oid, true)
    FROM pg_catalog.pg_namespace namespace
    JOIN pg_catalog.pg_class class ON class.relnamespace = namespace.oid
    JOIN pg_catalog.pg_constraint constraint_entry
        ON constraint_entry.conrelid = class.oid
    WHERE namespace.nspname = $1
    UNION ALL
    SELECT
        'index',
        table_class.relname || '.' || index_class.relname,
        pg_catalog.pg_get_indexdef(index_class.oid, 0, true)
    FROM pg_catalog.pg_namespace namespace
    JOIN pg_catalog.pg_class table_class ON table_class.relnamespace = namespace.oid
    JOIN pg_catalog.pg_index index_entry ON index_entry.indrelid = table_class.oid
    JOIN pg_catalog.pg_class index_class ON index_class.oid = index_entry.indexrelid
    WHERE namespace.nspname = $1
    UNION ALL
    SELECT
        'migration',
        version::text,
        name || '|' || encode(checksum, 'hex')
    FROM %s.mbox_traffic_schema_migrations
)
SELECT md5(string_agg(kind || '|' || name || '|' || definition, E'\n'
    ORDER BY kind, name))
FROM objects
`, pgx.Identifier{schema}.Sanitize())
	err := testPostgresAdminQueryRow(t, statement, schema).Scan(&fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	return fingerprint
}

func newPostgresTestID(t *testing.T) string {
	t.Helper()
	return uuid.Must(uuid.NewV4()).String()
}

func bytesOf(value byte) []byte {
	content := make([]byte, 32)
	for index := range content {
		content[index] = value
	}
	return content
}

func testPostgresConnectionOptions(dsn string) postgresConnectionOptions {
	return postgresConnectionOptions{
		DSN:                dsn,
		MaxOpenConnections: 4,
		MinIdleConnections: 0,
		ConnectTimeout:     5 * time.Second,
		StatementTimeout:   10 * time.Second,
	}
}

func testPostgresSchemaOptions(dsn string, schema string) PostgresSchemaOptions {
	return PostgresSchemaOptions{
		DSN:                dsn,
		Schema:             schema,
		MaxOpenConnections: 4,
		MinIdleConnections: 0,
		ConnectTimeout:     5 * time.Second,
		StatementTimeout:   10 * time.Second,
	}
}

func migrateTestPostgresSchema(t *testing.T, dsn string, schema string) error {
	t.Helper()
	return MigratePostgresSchema(
		context.Background(),
		log.StdLogger(),
		testPostgresSchemaOptions(dsn, schema),
	)
}

func validateTestPostgresSchema(t *testing.T, dsn string, schema string) error {
	t.Helper()
	pool, err := newPostgresPool(
		context.Background(),
		testPostgresConnectionOptions(dsn),
	)
	if err != nil {
		return err
	}
	defer pool.Close()
	return managePostgresSchema(
		context.Background(),
		pool,
		schema,
		option.TrafficStatisticsSchemaManagementValidate,
	)
}

func testPostgresStoreOptions(
	schema string,
	instanceID string,
	revisionKey []byte,
) postgresHistoryOptions {
	return testPostgresStoreOptionsWithDSN(
		schema,
		instanceID,
		revisionKey,
		testPostgresDSN,
	)
}

func testPostgresStoreOptionsWithDSN(
	schema string,
	instanceID string,
	revisionKey []byte,
	dsn string,
) postgresHistoryOptions {
	var routingFingerprint [32]byte
	copy(routingFingerprint[:], bytesOf(9))
	return postgresHistoryOptions{
		DSN:                dsn,
		Schema:             schema,
		SchemaManagement:   option.TrafficStatisticsSchemaManagementValidate,
		MaxOpenConnections: 4,
		MinIdleConnections: 0,
		ConnectTimeout:     5 * time.Second,
		StatementTimeout:   10 * time.Second,
		Identity: trafficIdentity{
			Version:     trafficIdentityVersion,
			InstanceID:  instanceID,
			RevisionKey: revisionKey,
		},
		Revision: postgresRevisionInput{
			CanonicalContent:   []byte(`{"route":{}}`),
			RoutingFingerprint: routingFingerprint,
			ConfigRevision:     "00112233445566778899aabbccddeeff",
		},
	}
}

func newPostgresTestStore(
	t *testing.T,
	schema string,
	instanceID string,
	revisionKey []byte,
) (*postgresStore, historyStoreState) {
	t.Helper()
	provisionPostgresTestSchema(t, schema)
	store, err := newPostgresStore(
		context.Background(),
		testPostgresStoreOptions(schema, instanceID, revisionKey),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	state, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	return store, state
}

type postgresHistoryBackendHarness struct {
	access   sync.Mutex
	fixtures map[string]*postgresHistoryBackendFixture
}

type postgresHistoryBackendFixture struct {
	schema        string
	instanceID    string
	revisionKey   []byte
	availableFrom time.Time
}

func newPostgresHistoryBackendHarness() historyBackendHarness {
	return &postgresHistoryBackendHarness{
		fixtures: make(map[string]*postgresHistoryBackendFixture),
	}
}

func (h *postgresHistoryBackendHarness) Open(
	t *testing.T,
	configContent []byte,
) *History {
	t.Helper()
	fixture := h.fixture(t)
	revisionMAC := hmac.New(sha256.New, fixture.revisionKey)
	_, _ = revisionMAC.Write(configContent)
	revisionDigest := revisionMAC.Sum(nil)
	storeOptions := testPostgresStoreOptions(
		fixture.schema,
		fixture.instanceID,
		fixture.revisionKey,
	)
	storeOptions.Revision = postgresRevisionInput{
		CanonicalContent:   append([]byte(nil), configContent...),
		RoutingFingerprint: sha256.Sum256(configContent),
		ConfigRevision:     hex.EncodeToString(revisionDigest[:historyRevisionSize]),
	}
	store, err := newPostgresStore(context.Background(), storeOptions)
	if err != nil {
		t.Fatal("construct PostgreSQL contract store:", err)
	}
	history := newHistory(
		context.Background(),
		log.NewNOPFactory().NewLogger("traffic-postgres-contract"),
		store,
	)
	if err = history.Start(adapter.StartStateInitialize); err != nil {
		_ = history.Close()
		t.Fatal("open PostgreSQL contract history:", err)
	}
	if !history.targetsFrom.Equal(fixture.availableFrom) ||
		!history.destinationsFrom.Equal(fixture.availableFrom) {
		_ = history.Close()
		t.Fatalf(
			"PostgreSQL contract availability changed: targets=%v destinations=%v want=%v",
			history.targetsFrom,
			history.destinationsFrom,
			fixture.availableFrom,
		)
	}
	return history
}

func (h *postgresHistoryBackendHarness) fixture(
	t *testing.T,
) *postgresHistoryBackendFixture {
	t.Helper()
	name := t.Name()
	h.access.Lock()
	defer h.access.Unlock()
	if fixture := h.fixtures[name]; fixture != nil {
		return fixture
	}
	fixture := &postgresHistoryBackendFixture{
		schema:        newPostgresTestSchema(t),
		instanceID:    newPostgresTestID(t),
		revisionKey:   bytesOf(11),
		availableFrom: time.Now().UTC().Truncate(HistoryBucketInterval).Add(-time.Hour),
	}
	provisionPostgresTestSchema(t, fixture.schema)
	testPostgresAdminExec(t, fmt.Sprintf(`
INSERT INTO %s.mbox_traffic_instances (
    instance_id,
    revision_key,
    target_available_from,
    destination_available_from
) VALUES ($1, $2, $3, $3)
`, pgx.Identifier{fixture.schema}.Sanitize()),
		fixture.instanceID,
		fixture.revisionKey,
		fixture.availableFrom,
	)
	h.fixtures[name] = fixture
	return fixture
}

func testPostgresHistoryKey(now time.Time) historyKey {
	return historyKey{
		Bucket:             now.UTC().Truncate(HistoryBucketInterval).Unix(),
		ConfigRevision:     "00112233445566778899aabbccddeeff",
		RouteTag:           "route",
		GroupPath:          `["group"]`,
		DestinationDomain:  "example.com",
		ActualOutboundTag:  "proxy",
		ActualOutboundType: "mieru",
		Network:            "tcp",
	}
}

func testPostgresOverlay(state historyStoreState) historyQueryOverlay {
	return historyQueryOverlay{
		pending:                  historyBatch{},
		targetAvailableFrom:      state.targetsFrom,
		destinationAvailableFrom: state.destinationsFrom,
	}
}

func queryPostgresTestStore(
	t *testing.T,
	store *postgresStore,
	state historyStoreState,
) HistoryQueryResult {
	t.Helper()
	return queryPostgresTestStoreWithQuery(t, store, state, HistoryQuery{})
}

func queryPostgresTestStoreWithQuery(
	t *testing.T,
	store *postgresStore,
	state historyStoreState,
	input HistoryQuery,
) HistoryQueryResult {
	t.Helper()
	snapshot, err := store.BeginRead(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	query, err := normalizeHistoryQuery(input)
	if err != nil {
		t.Fatal(err)
	}
	result, err := snapshot.Query(
		context.Background(),
		query,
		testPostgresOverlay(state),
	)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func testPostgresAdminExec(t *testing.T, statement string, arguments ...any) {
	t.Helper()
	connection, err := pgx.Connect(context.Background(), testPostgresAdminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())
	if _, err = connection.Exec(context.Background(), statement, arguments...); err != nil {
		t.Fatal(err)
	}
}

func testPostgresAdminQueryRow(
	t *testing.T,
	statement string,
	arguments ...any,
) pgx.Row {
	t.Helper()
	connection, err := pgx.Connect(context.Background(), testPostgresAdminDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close(context.Background()) })
	return connection.QueryRow(context.Background(), statement, arguments...)
}

func testPostgresQueryRow(
	t *testing.T,
	dsn string,
	statement string,
	arguments ...any,
) pgx.Row {
	t.Helper()
	connection, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close(context.Background()) })
	return connection.QueryRow(context.Background(), statement, arguments...)
}

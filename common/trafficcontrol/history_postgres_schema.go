package trafficcontrol

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/binary"
	"fmt"
	"io/fs"
	"slices"
	"strconv"
	"strings"

	"github.com/sagernet/sing-box/option"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const currentPostgresSchemaVersion = 2

//go:embed postgres_migrations/*.sql
var postgresMigrationFiles embed.FS

type postgresMigration struct {
	Version  int
	Name     string
	SQL      string
	Checksum [sha256.Size]byte
}

func managePostgresSchema(
	ctx context.Context,
	pool *pgxpool.Pool,
	schema string,
	mode string,
) error {
	migrations, err := loadPostgresMigrations()
	if err != nil {
		return err
	}
	switch mode {
	case option.TrafficStatisticsSchemaManagementAuto:
		return migratePostgresSchema(ctx, pool, schema, migrations)
	case option.TrafficStatisticsSchemaManagementValidate:
		return validatePostgresSchema(ctx, pool, schema, migrations)
	default:
		return fmt.Errorf("%w", ErrPostgresConfiguration)
	}
}

func loadPostgresMigrations() ([]postgresMigration, error) {
	entries, err := fs.ReadDir(postgresMigrationFiles, "postgres_migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded PostgreSQL migrations: %w", err)
	}
	migrations := make([]postgresMigration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		if len(entry.Name()) < 5 || entry.Name()[3] != '_' {
			return nil, fmt.Errorf(
				"invalid embedded PostgreSQL migration name %q",
				entry.Name(),
			)
		}
		version, parseErr := strconv.Atoi(entry.Name()[:3])
		if parseErr != nil || version < 1 {
			return nil, fmt.Errorf(
				"invalid embedded PostgreSQL migration name %q",
				entry.Name(),
			)
		}
		content, readErr := postgresMigrationFiles.ReadFile(
			"postgres_migrations/" + entry.Name(),
		)
		if readErr != nil {
			return nil, fmt.Errorf(
				"read embedded PostgreSQL migration %q: %w",
				entry.Name(),
				readErr,
			)
		}
		migrations = append(migrations, postgresMigration{
			Version:  version,
			Name:     entry.Name(),
			SQL:      string(content),
			Checksum: sha256.Sum256(content),
		})
	}
	slices.SortFunc(migrations, func(left postgresMigration, right postgresMigration) int {
		return left.Version - right.Version
	})
	if len(migrations) != currentPostgresSchemaVersion {
		return nil, fmt.Errorf("%w", ErrPostgresSchemaIncompatible)
	}
	for index, migration := range migrations {
		if migration.Version != index+1 {
			return nil, fmt.Errorf("%w", ErrPostgresSchemaIncompatible)
		}
	}
	return migrations, nil
}

func migratePostgresSchema(
	ctx context.Context,
	pool *pgxpool.Pool,
	schema string,
	migrations []postgresMigration,
) error {
	transaction, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return classifyPostgresError(ctx, "begin traffic schema migration", err)
	}
	defer transaction.Rollback(context.Background())
	_, err = transaction.Exec(
		ctx,
		"SELECT pg_advisory_xact_lock($1)",
		postgresSchemaLockKey(schema),
	)
	if err != nil {
		return classifyPostgresError(ctx, "lock traffic schema migration", err)
	}
	schemaIdentifier := pgx.Identifier{schema}.Sanitize()
	_, err = transaction.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+schemaIdentifier)
	if err != nil {
		return classifyPostgresError(ctx, "create traffic statistics schema", err)
	}
	_, err = transaction.Exec(ctx, fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s.mbox_traffic_schema_migrations (
    version integer PRIMARY KEY,
    name text NOT NULL,
    checksum bytea NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT mbox_traffic_schema_migrations_version CHECK (version > 0),
    CONSTRAINT mbox_traffic_schema_migrations_checksum CHECK (octet_length(checksum) = 32)
)`, schemaIdentifier))
	if err != nil {
		return classifyPostgresError(ctx, "create traffic schema migration ledger", err)
	}
	applied, err := readPostgresMigrationLedger(ctx, transaction, schemaIdentifier)
	if err != nil {
		return err
	}
	if err = checkPostgresMigrationLedger(applied, migrations); err != nil {
		return err
	}
	for _, migration := range migrations {
		if _, loaded := applied[migration.Version]; loaded {
			continue
		}
		statements, statementErr := postgresMigrationStatements(
			migration.SQL,
			schemaIdentifier,
		)
		if statementErr != nil {
			return statementErr
		}
		for statementIndex, statement := range statements {
			if _, err = transaction.Exec(ctx, statement); err != nil {
				return classifyPostgresError(
					ctx,
					fmt.Sprintf(
						"apply traffic schema migration %d statement %d",
						migration.Version,
						statementIndex+1,
					),
					err,
				)
			}
		}
		_, err = transaction.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s.mbox_traffic_schema_migrations (version, name, checksum)
VALUES ($1, $2, $3)`, schemaIdentifier),
			migration.Version,
			migration.Name,
			migration.Checksum[:],
		)
		if err != nil {
			return classifyPostgresError(ctx, "record traffic schema migration", err)
		}
	}
	if len(migrations) == currentPostgresSchemaVersion {
		if err = validatePostgresSchemaStructure(ctx, transaction, schema); err != nil {
			return err
		}
	}
	if err = transaction.Commit(ctx); err != nil {
		return classifyPostgresError(ctx, "commit traffic schema migration", err)
	}
	return nil
}

func postgresMigrationStatements(
	content string,
	schemaIdentifier string,
) ([]string, error) {
	const delimiter = "\n-- mbox:statement\n"
	parts := strings.Split(content, delimiter)
	statements := make([]string, 0, len(parts))
	for _, part := range parts {
		statement := strings.TrimSpace(strings.ReplaceAll(
			part,
			"{{schema}}",
			schemaIdentifier,
		))
		if statement == "" || strings.Contains(statement, "{{schema}}") {
			return nil, fmt.Errorf("%w", ErrPostgresSchemaIncompatible)
		}
		statements = append(statements, statement)
	}
	return statements, nil
}

func validatePostgresSchema(
	ctx context.Context,
	pool *pgxpool.Pool,
	schema string,
	migrations []postgresMigration,
) error {
	var schemaExists bool
	err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_namespace
    WHERE nspname = $1
)`, schema).Scan(&schemaExists)
	if err != nil {
		return classifyPostgresError(ctx, "inspect traffic statistics schema", err)
	}
	if !schemaExists {
		return ErrPostgresSchemaMissing
	}
	schemaIdentifier := pgx.Identifier{schema}.Sanitize()
	var migrationTableExists bool
	err = pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_namespace namespace
    JOIN pg_catalog.pg_class class ON class.relnamespace = namespace.oid
    WHERE namespace.nspname = $1
        AND class.relname = $2
        AND class.relkind = 'r'
)
`, schema, "mbox_traffic_schema_migrations").Scan(&migrationTableExists)
	if err != nil {
		return classifyPostgresError(ctx, "inspect traffic schema migration ledger", err)
	}
	if !migrationTableExists {
		return ErrPostgresSchemaIncompatible
	}
	applied, err := readPostgresMigrationLedger(ctx, pool, schemaIdentifier)
	if err != nil {
		return err
	}
	if err = checkPostgresMigrationLedger(applied, migrations); err != nil {
		return err
	}
	if len(applied) != len(migrations) {
		return ErrPostgresSchemaIncompatible
	}
	return validatePostgresSchemaStructure(ctx, pool, schema)
}

type appliedPostgresMigration struct {
	Name     string
	Checksum []byte
}

func readPostgresMigrationLedger(
	ctx context.Context,
	executor interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	},
	schemaIdentifier string,
) (map[int]appliedPostgresMigration, error) {
	rows, err := executor.Query(ctx, fmt.Sprintf(`
SELECT version, name, checksum
FROM %s.mbox_traffic_schema_migrations
ORDER BY version`, schemaIdentifier))
	if err != nil {
		return nil, classifyPostgresError(ctx, "read traffic schema migration ledger", err)
	}
	defer rows.Close()
	applied := make(map[int]appliedPostgresMigration)
	for rows.Next() {
		var version int
		var migration appliedPostgresMigration
		if err = rows.Scan(&version, &migration.Name, &migration.Checksum); err != nil {
			return nil, classifyPostgresError(ctx, "decode traffic schema migration ledger", err)
		}
		applied[version] = migration
	}
	if err = rows.Err(); err != nil {
		return nil, classifyPostgresError(ctx, "read traffic schema migration ledger", err)
	}
	return applied, nil
}

func checkPostgresMigrationLedger(
	applied map[int]appliedPostgresMigration,
	migrations []postgresMigration,
) error {
	expected := make(map[int]postgresMigration, len(migrations))
	for _, migration := range migrations {
		expected[migration.Version] = migration
	}
	for version, actual := range applied {
		migration, loaded := expected[version]
		if !loaded || version > currentPostgresSchemaVersion {
			return ErrPostgresSchemaFuture
		}
		if actual.Name != migration.Name ||
			!slices.Equal(actual.Checksum, migration.Checksum[:]) {
			return ErrPostgresSchemaIncompatible
		}
	}
	for version := 1; version <= len(applied); version++ {
		if _, loaded := applied[version]; !loaded {
			return ErrPostgresSchemaIncompatible
		}
	}
	return nil
}

type postgresRequiredColumn struct {
	Table            string
	Name             string
	DataType         string
	Nullable         bool
	NumericPrecision int
	NumericScale     int
}

var postgresRequiredColumns = []postgresRequiredColumn{
	{"mbox_traffic_schema_migrations", "version", "integer", false, 0, 0},
	{"mbox_traffic_schema_migrations", "name", "text", false, 0, 0},
	{"mbox_traffic_schema_migrations", "checksum", "bytea", false, 0, 0},
	{"mbox_traffic_schema_migrations", "applied_at", "timestamp with time zone", false, 0, 0},
	{"mbox_traffic_instances", "instance_id", "text", false, 0, 0},
	{"mbox_traffic_instances", "revision_key", "bytea", false, 0, 0},
	{"mbox_traffic_instances", "target_available_from", "timestamp with time zone", false, 0, 0},
	{"mbox_traffic_instances", "destination_available_from", "timestamp with time zone", false, 0, 0},
	{"mbox_traffic_instances", "created_at", "timestamp with time zone", false, 0, 0},
	{"mbox_traffic_instances", "updated_at", "timestamp with time zone", false, 0, 0},
	{"mbox_traffic_config_revisions", "instance_id", "text", false, 0, 0},
	{"mbox_traffic_config_revisions", "routing_fingerprint", "bytea", false, 0, 0},
	{"mbox_traffic_config_revisions", "config_revision", "text", false, 0, 0},
	{"mbox_traffic_config_revisions", "created_at", "timestamp with time zone", false, 0, 0},
	{"mbox_traffic_minute_summary", "instance_id", "text", false, 0, 0},
	{"mbox_traffic_minute_summary", "bucket_start", "timestamp with time zone", false, 0, 0},
	{"mbox_traffic_minute_summary", "config_revision", "text", false, 0, 0},
	{"mbox_traffic_minute_summary", "route_tag", "text", false, 0, 0},
	{"mbox_traffic_minute_summary", "group_path", "ARRAY", false, 0, 0},
	{"mbox_traffic_minute_summary", "actual_outbound_tag", "text", false, 0, 0},
	{"mbox_traffic_minute_summary", "actual_outbound_type", "text", false, 0, 0},
	{"mbox_traffic_minute_summary", "network", "text", false, 0, 0},
	{"mbox_traffic_minute_summary", "uplink_bytes", "numeric", false, 20, 0},
	{"mbox_traffic_minute_summary", "downlink_bytes", "numeric", false, 20, 0},
	{"mbox_traffic_minute_summary", "connections", "numeric", false, 20, 0},
	{"mbox_traffic_minute_targets", "instance_id", "text", false, 0, 0},
	{"mbox_traffic_minute_targets", "bucket_start", "timestamp with time zone", false, 0, 0},
	{"mbox_traffic_minute_targets", "config_revision", "text", false, 0, 0},
	{"mbox_traffic_minute_targets", "route_tag", "text", false, 0, 0},
	{"mbox_traffic_minute_targets", "group_path", "ARRAY", false, 0, 0},
	{"mbox_traffic_minute_targets", "actual_outbound_tag", "text", false, 0, 0},
	{"mbox_traffic_minute_targets", "actual_outbound_type", "text", false, 0, 0},
	{"mbox_traffic_minute_targets", "network", "text", false, 0, 0},
	{"mbox_traffic_minute_targets", "destination_domain", "text", false, 0, 0},
	{"mbox_traffic_minute_targets", "destination_ip", "text", false, 0, 0},
	{"mbox_traffic_minute_targets", "uplink_bytes", "numeric", false, 20, 0},
	{"mbox_traffic_minute_targets", "downlink_bytes", "numeric", false, 20, 0},
	{"mbox_traffic_minute_targets", "connections", "numeric", false, 20, 0},
	{"mbox_traffic_ingest_batches", "instance_id", "text", false, 0, 0},
	{"mbox_traffic_ingest_batches", "batch_id", "uuid", false, 0, 0},
	{"mbox_traffic_ingest_batches", "payload_sha256", "bytea", false, 0, 0},
	{"mbox_traffic_ingest_batches", "record_count", "integer", false, 0, 0},
	{"mbox_traffic_ingest_batches", "first_bucket", "timestamp with time zone", true, 0, 0},
	{"mbox_traffic_ingest_batches", "last_bucket", "timestamp with time zone", true, 0, 0},
	{"mbox_traffic_ingest_batches", "accepted_at", "timestamp with time zone", false, 0, 0},
}

type postgresRequiredConstraint struct {
	Table             string
	Name              string
	Type              string
	Columns           []string
	ReferencedTable   string
	ReferencedColumns []string
	DeleteAction      string
	CheckExpression   string
}

var postgresRequiredConstraints = []postgresRequiredConstraint{
	postgresPrimaryKey(
		"mbox_traffic_schema_migrations",
		"mbox_traffic_schema_migrations_pkey",
		"version",
	),
	postgresCheck(
		"mbox_traffic_schema_migrations",
		"mbox_traffic_schema_migrations_version",
		"version > 0",
	),
	postgresCheck(
		"mbox_traffic_schema_migrations",
		"mbox_traffic_schema_migrations_checksum",
		"octet_length(checksum) = 32",
	),
	postgresPrimaryKey(
		"mbox_traffic_instances",
		"mbox_traffic_instances_pkey",
		"instance_id",
	),
	postgresCheck(
		"mbox_traffic_instances",
		"mbox_traffic_instances_revision_key_length",
		"octet_length(revision_key) = 32",
	),
	postgresCheck(
		"mbox_traffic_instances",
		"mbox_traffic_instances_id_length",
		"octet_length(instance_id) >= 1 AND octet_length(instance_id) <= 128",
	),
	postgresCheck(
		"mbox_traffic_instances",
		"mbox_traffic_instances_availability_order",
		"destination_available_from >= target_available_from",
	),
	postgresPrimaryKey(
		"mbox_traffic_config_revisions",
		"mbox_traffic_config_revisions_pkey",
		"instance_id",
		"routing_fingerprint",
	),
	postgresInstanceForeignKey(
		"mbox_traffic_config_revisions",
		"mbox_traffic_config_revisions_instance_fk",
	),
	postgresCheck(
		"mbox_traffic_config_revisions",
		"mbox_traffic_config_revisions_fingerprint_length",
		"octet_length(routing_fingerprint) = 32",
	),
	postgresCheck(
		"mbox_traffic_config_revisions",
		"mbox_traffic_config_revisions_format",
		"config_revision ~ '^[0-9a-f]{32}$'::text",
	),
	postgresPrimaryKey(
		"mbox_traffic_minute_summary",
		"mbox_traffic_minute_summary_pkey",
		"instance_id",
		"bucket_start",
		"config_revision",
		"route_tag",
		"group_path",
		"actual_outbound_tag",
		"actual_outbound_type",
		"network",
	),
	postgresInstanceForeignKey(
		"mbox_traffic_minute_summary",
		"mbox_traffic_minute_summary_instance_fk",
	),
	postgresCheck(
		"mbox_traffic_minute_summary",
		"mbox_traffic_minute_summary_bucket",
		"bucket_start = date_trunc('minute'::text, bucket_start)",
	),
	postgresCheck(
		"mbox_traffic_minute_summary",
		"mbox_traffic_minute_summary_uplink",
		"uplink_bytes >= 0::numeric AND uplink_bytes <= '18446744073709551615'::numeric",
	),
	postgresCheck(
		"mbox_traffic_minute_summary",
		"mbox_traffic_minute_summary_downlink",
		"downlink_bytes >= 0::numeric AND downlink_bytes <= '18446744073709551615'::numeric",
	),
	postgresCheck(
		"mbox_traffic_minute_summary",
		"mbox_traffic_minute_summary_connections",
		"connections >= 0::numeric AND connections <= '18446744073709551615'::numeric",
	),
	postgresPrimaryKey(
		"mbox_traffic_minute_targets",
		"mbox_traffic_minute_targets_pkey",
		"instance_id",
		"bucket_start",
		"config_revision",
		"route_tag",
		"group_path",
		"actual_outbound_tag",
		"actual_outbound_type",
		"network",
		"destination_domain",
		"destination_ip",
	),
	postgresInstanceForeignKey(
		"mbox_traffic_minute_targets",
		"mbox_traffic_minute_targets_instance_fk",
	),
	postgresCheck(
		"mbox_traffic_minute_targets",
		"mbox_traffic_minute_targets_bucket",
		"bucket_start = date_trunc('minute'::text, bucket_start)",
	),
	postgresCheck(
		"mbox_traffic_minute_targets",
		"mbox_traffic_minute_targets_uplink",
		"uplink_bytes >= 0::numeric AND uplink_bytes <= '18446744073709551615'::numeric",
	),
	postgresCheck(
		"mbox_traffic_minute_targets",
		"mbox_traffic_minute_targets_downlink",
		"downlink_bytes >= 0::numeric AND downlink_bytes <= '18446744073709551615'::numeric",
	),
	postgresCheck(
		"mbox_traffic_minute_targets",
		"mbox_traffic_minute_targets_connections",
		"connections >= 0::numeric AND connections <= '18446744073709551615'::numeric",
	),
	postgresPrimaryKey(
		"mbox_traffic_ingest_batches",
		"mbox_traffic_ingest_batches_pkey",
		"instance_id",
		"batch_id",
	),
	postgresInstanceForeignKey(
		"mbox_traffic_ingest_batches",
		"mbox_traffic_ingest_batches_instance_fk",
	),
	postgresCheck(
		"mbox_traffic_ingest_batches",
		"mbox_traffic_ingest_batches_payload_length",
		"octet_length(payload_sha256) = 32",
	),
	postgresCheck(
		"mbox_traffic_ingest_batches",
		"mbox_traffic_ingest_batches_record_count",
		"record_count >= 0",
	),
	postgresCheck(
		"mbox_traffic_ingest_batches",
		"mbox_traffic_ingest_batches_bucket_pair",
		"first_bucket IS NULL AND last_bucket IS NULL OR first_bucket IS NOT NULL AND last_bucket IS NOT NULL AND last_bucket >= first_bucket",
	),
}

func postgresPrimaryKey(
	table string,
	name string,
	columns ...string,
) postgresRequiredConstraint {
	return postgresRequiredConstraint{
		Table:   table,
		Name:    name,
		Type:    "p",
		Columns: columns,
	}
}

func postgresInstanceForeignKey(
	table string,
	name string,
) postgresRequiredConstraint {
	return postgresRequiredConstraint{
		Table:             table,
		Name:              name,
		Type:              "f",
		Columns:           []string{"instance_id"},
		ReferencedTable:   "mbox_traffic_instances",
		ReferencedColumns: []string{"instance_id"},
		DeleteAction:      "c",
	}
}

func postgresCheck(
	table string,
	name string,
	expression string,
) postgresRequiredConstraint {
	return postgresRequiredConstraint{
		Table:           table,
		Name:            name,
		Type:            "c",
		CheckExpression: expression,
	}
}

type postgresRequiredIndex struct {
	Table     string
	Name      string
	Method    string
	Unique    bool
	Columns   []string
	Predicate string
}

var postgresRequiredIndexes = []postgresRequiredIndex{
	{
		Table:   "mbox_traffic_minute_summary",
		Name:    "mbox_traffic_summary_actual_outbound_idx",
		Method:  "btree",
		Columns: []string{"instance_id", "actual_outbound_tag", "bucket_start"},
	},
	{
		Table:     "mbox_traffic_minute_targets",
		Name:      "mbox_traffic_targets_domain_idx",
		Method:    "btree",
		Columns:   []string{"instance_id", "destination_domain", "bucket_start"},
		Predicate: "destination_domain <> ''::text",
	},
	{
		Table:     "mbox_traffic_minute_targets",
		Name:      "mbox_traffic_targets_ip_idx",
		Method:    "btree",
		Columns:   []string{"instance_id", "destination_ip", "bucket_start"},
		Predicate: "destination_ip <> ''::text",
	},
}

func validatePostgresSchemaStructure(
	ctx context.Context,
	executor interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	},
	schema string,
) error {
	rows, err := executor.Query(ctx, `
SELECT
    table_class.relname,
    attribute.attname,
    pg_catalog.format_type(attribute.atttypid, attribute.atttypmod),
    attribute.attnotnull,
    COALESCE(
        pg_catalog.pg_get_expr(
            default_entry.adbin,
            default_entry.adrelid,
            true
        ),
        ''
    ),
    attribute.attgenerated::text,
    attribute.attidentity::text
FROM pg_catalog.pg_namespace namespace
JOIN pg_catalog.pg_class table_class
    ON table_class.relnamespace = namespace.oid
JOIN pg_catalog.pg_attribute attribute
    ON attribute.attrelid = table_class.oid
LEFT JOIN pg_catalog.pg_attrdef default_entry
    ON default_entry.adrelid = table_class.oid
    AND default_entry.adnum = attribute.attnum
WHERE namespace.nspname = $1
    AND table_class.relkind = 'r'
    AND attribute.attnum > 0
    AND NOT attribute.attisdropped
`, schema)
	if err != nil {
		return classifyPostgresError(ctx, "inspect traffic schema structure", err)
	}
	defer rows.Close()
	type columnKey struct {
		Table string
		Name  string
	}
	type columnValue struct {
		FormattedType string
		Nullable      bool
		Default       string
		Generated     string
		Identity      string
	}
	columns := make(map[columnKey]columnValue)
	for rows.Next() {
		var tableName string
		var columnName string
		var formattedType string
		var notNull bool
		var defaultExpression string
		var generated string
		var identity string
		if err = rows.Scan(
			&tableName,
			&columnName,
			&formattedType,
			&notNull,
			&defaultExpression,
			&generated,
			&identity,
		); err != nil {
			return classifyPostgresError(ctx, "decode traffic schema structure", err)
		}
		columns[columnKey{tableName, columnName}] = columnValue{
			FormattedType: formattedType,
			Nullable:      !notNull,
			Default:       normalizePostgresDefinition(defaultExpression),
			Generated:     generated,
			Identity:      identity,
		}
	}
	if err = rows.Err(); err != nil {
		return classifyPostgresError(ctx, "inspect traffic schema structure", err)
	}
	expectedColumns := make(map[columnKey]postgresRequiredColumn, len(postgresRequiredColumns))
	ownedTables := make(map[string]bool)
	for _, required := range postgresRequiredColumns {
		key := columnKey{required.Table, required.Name}
		expectedColumns[key] = required
		ownedTables[required.Table] = true
	}
	for _, required := range postgresRequiredColumns {
		actual, loaded := columns[columnKey{required.Table, required.Name}]
		if !loaded ||
			actual.FormattedType != postgresRequiredColumnType(required) ||
			actual.Nullable != required.Nullable ||
			actual.Default != postgresRequiredColumnDefault(required.Table, required.Name) ||
			actual.Generated != "" ||
			actual.Identity != "" {
			return ErrPostgresSchemaIncompatible
		}
	}
	for key := range columns {
		if ownedTables[key.Table] {
			if _, loaded := expectedColumns[key]; !loaded {
				return ErrPostgresSchemaIncompatible
			}
		}
	}
	if err = validatePostgresConstraints(ctx, executor, schema); err != nil {
		return err
	}
	return validatePostgresIndexes(ctx, executor, schema)
}

func postgresRequiredColumnType(required postgresRequiredColumn) string {
	switch required.DataType {
	case "ARRAY":
		return "text[]"
	case "numeric":
		return fmt.Sprintf(
			"numeric(%d,%d)",
			required.NumericPrecision,
			required.NumericScale,
		)
	default:
		return required.DataType
	}
}

func postgresRequiredColumnDefault(table string, column string) string {
	switch table + "." + column {
	case "mbox_traffic_schema_migrations.applied_at",
		"mbox_traffic_instances.created_at",
		"mbox_traffic_instances.updated_at",
		"mbox_traffic_config_revisions.created_at",
		"mbox_traffic_ingest_batches.accepted_at":
		return "clock_timestamp()"
	default:
		return ""
	}
}

func validatePostgresConstraints(
	ctx context.Context,
	executor interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	},
	schema string,
) error {
	names := make([]string, 0, len(postgresRequiredConstraints))
	expected := make(map[string]postgresRequiredConstraint, len(postgresRequiredConstraints))
	for _, constraint := range postgresRequiredConstraints {
		names = append(names, constraint.Name)
		expected[constraint.Name] = constraint
	}
	rows, err := executor.Query(ctx, `
SELECT
    table_class.relname,
    constraint_entry.conname,
    constraint_entry.contype::text,
    COALESCE(ARRAY(
        SELECT attribute.attname::text
        FROM unnest(constraint_entry.conkey) WITH ORDINALITY key(attnum, position)
        JOIN pg_catalog.pg_attribute attribute
            ON attribute.attrelid = constraint_entry.conrelid
            AND attribute.attnum = key.attnum
        ORDER BY key.position
    ), ARRAY[]::text[]),
    CASE
        WHEN constraint_entry.contype = 'f' THEN referenced_namespace.nspname
        ELSE ''
    END,
    CASE
        WHEN constraint_entry.contype = 'f' THEN referenced_class.relname
        ELSE ''
    END,
    COALESCE(ARRAY(
        SELECT attribute.attname::text
        FROM unnest(constraint_entry.confkey) WITH ORDINALITY key(attnum, position)
        JOIN pg_catalog.pg_attribute attribute
            ON attribute.attrelid = constraint_entry.confrelid
            AND attribute.attnum = key.attnum
        ORDER BY key.position
    ), ARRAY[]::text[]),
    CASE
        WHEN constraint_entry.contype = 'f' THEN constraint_entry.confdeltype::text
        ELSE ''
    END,
    CASE
        WHEN constraint_entry.contype = 'c'
            THEN pg_catalog.pg_get_expr(
                constraint_entry.conbin,
                constraint_entry.conrelid,
                true
            )
        ELSE ''
    END,
    constraint_entry.convalidated,
    constraint_entry.condeferrable,
    constraint_entry.condeferred,
    CASE
        WHEN constraint_entry.contype = 'f' THEN constraint_entry.confupdtype::text
        ELSE ''
    END,
    CASE
        WHEN constraint_entry.contype = 'f' THEN constraint_entry.confmatchtype::text
        ELSE ''
    END
FROM pg_catalog.pg_namespace namespace
JOIN pg_catalog.pg_class table_class
    ON table_class.relnamespace = namespace.oid
JOIN pg_catalog.pg_constraint constraint_entry
    ON constraint_entry.conrelid = table_class.oid
LEFT JOIN pg_catalog.pg_class referenced_class
    ON referenced_class.oid = constraint_entry.confrelid
LEFT JOIN pg_catalog.pg_namespace referenced_namespace
    ON referenced_namespace.oid = referenced_class.relnamespace
WHERE namespace.nspname = $1
    AND constraint_entry.conname = ANY($2::text[])
`, schema, names)
	if err != nil {
		return classifyPostgresError(ctx, "inspect traffic schema constraints", err)
	}
	defer rows.Close()
	found := make(map[string]struct{}, len(expected))
	for rows.Next() {
		var table string
		var name string
		var constraintType string
		var columns []string
		var referencedSchema string
		var referencedTable string
		var referencedColumns []string
		var deleteAction string
		var checkExpression string
		var validated bool
		var deferrable bool
		var deferred bool
		var updateAction string
		var matchType string
		if err = rows.Scan(
			&table,
			&name,
			&constraintType,
			&columns,
			&referencedSchema,
			&referencedTable,
			&referencedColumns,
			&deleteAction,
			&checkExpression,
			&validated,
			&deferrable,
			&deferred,
			&updateAction,
			&matchType,
		); err != nil {
			return classifyPostgresError(ctx, "decode traffic schema constraints", err)
		}
		required, loaded := expected[name]
		if !loaded {
			continue
		}
		if _, duplicate := found[name]; duplicate ||
			table != required.Table ||
			constraintType != required.Type ||
			referencedTable != required.ReferencedTable ||
			!slices.Equal(referencedColumns, required.ReferencedColumns) ||
			deleteAction != required.DeleteAction ||
			normalizePostgresDefinition(checkExpression) !=
				normalizePostgresDefinition(required.CheckExpression) ||
			!validated ||
			deferrable ||
			deferred {
			return ErrPostgresSchemaIncompatible
		}
		if constraintType != "c" && !slices.Equal(columns, required.Columns) {
			return ErrPostgresSchemaIncompatible
		}
		if constraintType == "f" &&
			(referencedSchema != schema || updateAction != "a" || matchType != "s") {
			return ErrPostgresSchemaIncompatible
		}
		found[name] = struct{}{}
	}
	if err = rows.Err(); err != nil {
		return classifyPostgresError(ctx, "inspect traffic schema constraints", err)
	}
	for name := range expected {
		if _, loaded := found[name]; !loaded {
			return ErrPostgresSchemaIncompatible
		}
	}
	return nil
}

func validatePostgresIndexes(
	ctx context.Context,
	executor interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	},
	schema string,
) error {
	names := make([]string, 0, len(postgresRequiredIndexes))
	expected := make(map[string]postgresRequiredIndex, len(postgresRequiredIndexes))
	for _, index := range postgresRequiredIndexes {
		names = append(names, index.Name)
		expected[index.Name] = index
	}
	rows, err := executor.Query(ctx, `
SELECT
    table_class.relname,
    index_class.relname,
    access_method.amname,
    index_entry.indisunique,
    index_entry.indisvalid,
    index_entry.indisready,
    index_entry.indislive,
    ARRAY(
        SELECT pg_catalog.pg_get_indexdef(
            index_class.oid,
            position,
            true
        )
        FROM generate_series(1, index_entry.indnkeyatts) position
        ORDER BY position
    ),
    COALESCE(
        pg_catalog.pg_get_expr(
            index_entry.indpred,
            index_entry.indrelid,
            true
        ),
        ''
    )
FROM pg_catalog.pg_namespace namespace
JOIN pg_catalog.pg_class table_class
    ON table_class.relnamespace = namespace.oid
JOIN pg_catalog.pg_index index_entry
    ON index_entry.indrelid = table_class.oid
JOIN pg_catalog.pg_class index_class
    ON index_class.oid = index_entry.indexrelid
JOIN pg_catalog.pg_am access_method
    ON access_method.oid = index_class.relam
WHERE namespace.nspname = $1
    AND index_class.relname = ANY($2::text[])
`, schema, names)
	if err != nil {
		return classifyPostgresError(ctx, "inspect traffic schema indexes", err)
	}
	defer rows.Close()
	found := make(map[string]struct{}, len(expected))
	for rows.Next() {
		var table string
		var name string
		var method string
		var unique bool
		var valid bool
		var ready bool
		var live bool
		var columns []string
		var predicate string
		if err = rows.Scan(
			&table,
			&name,
			&method,
			&unique,
			&valid,
			&ready,
			&live,
			&columns,
			&predicate,
		); err != nil {
			return classifyPostgresError(ctx, "decode traffic schema indexes", err)
		}
		required, loaded := expected[name]
		if !loaded {
			continue
		}
		if _, duplicate := found[name]; duplicate ||
			table != required.Table ||
			method != required.Method ||
			unique != required.Unique ||
			!valid ||
			!ready ||
			!live ||
			!slices.Equal(columns, required.Columns) ||
			normalizePostgresDefinition(predicate) !=
				normalizePostgresDefinition(required.Predicate) {
			return ErrPostgresSchemaIncompatible
		}
		found[name] = struct{}{}
	}
	if err = rows.Err(); err != nil {
		return classifyPostgresError(ctx, "inspect traffic schema indexes", err)
	}
	for name := range expected {
		if _, loaded := found[name]; !loaded {
			return ErrPostgresSchemaIncompatible
		}
	}
	return nil
}

func normalizePostgresDefinition(definition string) string {
	return strings.Join(strings.Fields(definition), " ")
}

func postgresSchemaLockKey(schema string) int64 {
	digest := sha256.Sum256([]byte(schema))
	return int64(binary.BigEndian.Uint64(digest[:8]))
}

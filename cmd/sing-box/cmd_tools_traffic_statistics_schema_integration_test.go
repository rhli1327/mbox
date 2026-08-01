//go:build with_postgres

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/common/trafficcontrol"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
)

func TestTrafficStatisticsSchemaMigrateCommandIntegration(t *testing.T) {
	adminDSN := requirePostgresCommandEnvironment(t, "MBOX_TEST_POSTGRES_ADMIN_DSN")
	schemaDSN := requirePostgresCommandEnvironment(t, "MBOX_TEST_POSTGRES_SCHEMA_DSN")
	schema := "mbox_cli_" + strings.ReplaceAll(uuid.Must(uuid.NewV4()).String(), "-", "")
	t.Cleanup(func() {
		connection, err := pgx.Connect(context.Background(), adminDSN)
		if err != nil {
			t.Errorf("connect CLI cleanup: %v", err)
			return
		}
		defer connection.Close(context.Background())
		if _, err = connection.Exec(
			context.Background(),
			"DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE",
		); err != nil {
			t.Errorf("drop CLI schema: %v", err)
		}
	})

	temporaryDirectory := t.TempDir()
	configPath := filepath.Join(temporaryDirectory, "config.json")
	writePostgresCommandConfig(t, configPath, schemaDSN, schema, "")
	t.Chdir(temporaryDirectory)

	for range 2 {
		output, err := executeTrafficStatisticsSchemaCommand(
			t,
			"tools",
			"traffic-statistics",
			"schema",
			"migrate",
			"-c",
			configPath,
		)
		if err != nil {
			t.Fatalf("execute real schema command: %v\n%s", err, output)
		}
	}

	connection, err := pgx.Connect(context.Background(), adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())
	var versions []int32
	rows, err := connection.Query(context.Background(), fmt.Sprintf(`
SELECT version
FROM %s.mbox_traffic_schema_migrations
ORDER BY version
`, pgx.Identifier{schema}.Sanitize()))
	if err != nil {
		t.Fatal(err)
	}
	versions, err = pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 3 ||
		versions[0] != 1 ||
		versions[1] != 2 ||
		versions[2] != 3 {
		t.Fatalf("unexpected migration ledger: %v", versions)
	}
	for _, forbidden := range []string{
		"traffic-instance.json",
		"traffic-spool.db",
		"traffic.db",
	} {
		if _, err = os.Stat(filepath.Join(temporaryDirectory, forbidden)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("schema command created %s: %v", forbidden, err)
		}
	}

	const sentinel = "cli-password-must-not-leak"
	detourSchema := schema + "_detour"
	detourConfigPath := filepath.Join(temporaryDirectory, "detour.json")
	writePostgresCommandConfig(
		t,
		detourConfigPath,
		"postgres://user:"+sentinel+"@invalid.invalid/arbitrary_database?sslmode=disable",
		detourSchema,
		"unreachable-outbound",
	)
	output, err := executeTrafficStatisticsSchemaCommand(
		t,
		"tools",
		"traffic-statistics",
		"schema",
		"migrate",
		"-c",
		detourConfigPath,
	)
	if !errors.Is(err, trafficcontrol.ErrPostgresDetourNotReady) {
		t.Fatalf("unexpected detour error: %v", err)
	}
	if strings.Contains(output, sentinel) ||
		(err != nil && strings.Contains(err.Error(), sentinel)) {
		t.Fatal("schema command leaked PostgreSQL credentials")
	}
	var detourSchemaExists bool
	if err = connection.QueryRow(context.Background(), `
SELECT EXISTS (
    SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $1
)`, detourSchema).Scan(&detourSchemaExists); err != nil {
		t.Fatal(err)
	}
	if detourSchemaExists {
		t.Fatal("detour rejection created a schema")
	}
}

func requirePostgresCommandEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("with_postgres requires %s", name)
	}
	return value
}

func writePostgresCommandConfig(
	t *testing.T,
	path string,
	dsn string,
	schema string,
	detour string,
) {
	t.Helper()
	config := map[string]any{
		"experimental": map[string]any{
			"traffic_statistics": map[string]any{
				"enabled": true,
				"storage": map[string]any{
					"type":                 "postgres",
					"dsn":                  dsn,
					"schema":               schema,
					"schema_management":    "validate",
					"max_open_connections": 4,
					"min_idle_connections": 0,
					"connect_timeout":      "5s",
					"statement_timeout":    "10s",
					"dialer": map[string]any{
						"detour": detour,
					},
				},
			},
		},
	}
	content, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func executeTrafficStatisticsSchemaCommand(
	t *testing.T,
	arguments ...string,
) (string, error) {
	t.Helper()
	previousGlobalContext := globalCtx
	previousConfigPaths := configPaths
	previousConfigDirectories := configDirectories
	previousWorkingDirectory := workingDir
	previousDisableColor := disableColor
	configFlag := mainCommand.PersistentFlags().Lookup("config")
	configDirectoryFlag := mainCommand.PersistentFlags().Lookup("config-directory")
	previousConfigChanged := configFlag.Changed
	previousConfigDirectoryChanged := configDirectoryFlag.Changed
	var output bytes.Buffer
	t.Cleanup(func() {
		globalCtx = previousGlobalContext
		configPaths = previousConfigPaths
		configDirectories = previousConfigDirectories
		workingDir = previousWorkingDirectory
		disableColor = previousDisableColor
		configFlag.Changed = previousConfigChanged
		configDirectoryFlag.Changed = previousConfigDirectoryChanged
		mainCommand.SetArgs(nil)
		mainCommand.SetOut(nil)
		mainCommand.SetErr(nil)
	})
	configPaths = nil
	configDirectories = nil
	workingDir = ""
	disableColor = true
	configFlag.Changed = false
	configDirectoryFlag.Changed = false
	mainCommand.SetArgs(arguments)
	mainCommand.SetOut(&output)
	mainCommand.SetErr(&output)
	err := mainCommand.ExecuteContext(context.Background())
	return output.String(), err
}

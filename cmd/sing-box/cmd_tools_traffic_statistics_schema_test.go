package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sagernet/sing-box/common/trafficcontrol"
	"github.com/sagernet/sing-box/log"
)

func TestTrafficStatisticsSchemaMigrateCommandWiring(t *testing.T) {
	temporaryDirectory := t.TempDir()
	configPath := filepath.Join(temporaryDirectory, "config.json")
	err := os.WriteFile(configPath, []byte(`{
  "experimental": {
    "traffic_statistics": {
      "enabled": true,
      "storage": {
        "type": "postgres",
        "dsn": "postgres://user:secret@db.example/arbitrary_database",
        "schema": "arbitrary_schema",
        "schema_management": "validate",
        "max_open_connections": 7,
        "min_idle_connections": 2,
        "connect_timeout": "3s",
        "statement_timeout": "4s"
      }
    }
  }
}`), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	previousGlobalContext := globalCtx
	previousConfigPaths := configPaths
	previousConfigDirectories := configDirectories
	previousMigrate := migrateTrafficStatisticsSchema
	t.Cleanup(func() {
		globalCtx = previousGlobalContext
		configPaths = previousConfigPaths
		configDirectories = previousConfigDirectories
		migrateTrafficStatisticsSchema = previousMigrate
	})
	globalCtx = context.Background()
	configPaths = []string{configPath}
	configDirectories = nil
	called := false
	migrateTrafficStatisticsSchema = func(
		ctx context.Context,
		logger log.ContextLogger,
		options trafficcontrol.PostgresSchemaOptions,
	) error {
		called = true
		if ctx != globalCtx || logger == nil {
			t.Fatal("command did not pass its context and logger")
		}
		if options.DSN != "postgres://user:secret@db.example/arbitrary_database" ||
			options.Schema != "arbitrary_schema" ||
			options.MaxOpenConnections != 7 ||
			options.MinIdleConnections != 2 ||
			options.ConnectTimeout.String() != "3s" ||
			options.StatementTimeout.String() != "4s" {
			t.Fatalf("unexpected migration options: %+v", options)
		}
		return nil
	}
	if err = runTrafficStatisticsSchemaMigrate(globalCtx); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("schema migration was not invoked")
	}
}

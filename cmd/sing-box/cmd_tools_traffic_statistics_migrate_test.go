package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/trafficcontrol"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/service"

	"github.com/spf13/cobra"
)

func TestTrafficStatisticsMigrateCommandHierarchyAndFlags(t *testing.T) {
	command := requireTrafficStatisticsMigrateCommand(t)
	expected := map[string]string{
		"source":                 "traffic.db",
		"instance-id":            "",
		"batch-size":             "500",
		"from":                   "",
		"to":                     "",
		"resume":                 "",
		"active-config-revision": "",
		"dry-run":                "false",
	}
	for name, defaultValue := range expected {
		flag := command.Flags().Lookup(name)
		if flag == nil || flag.DefValue != defaultValue {
			t.Fatalf("unexpected --%s flag: %#v", name, flag)
		}
	}
	for _, forbidden := range []string{
		"dsn",
		"schema",
		"detour",
		"mode",
		"merge",
		"delete-source",
	} {
		if command.Flags().Lookup(forbidden) != nil {
			t.Fatalf("unexpected --%s flag", forbidden)
		}
	}
}

func TestTrafficStatisticsMigrateCommandLoadsMergedPostgresConfig(t *testing.T) {
	directory := t.TempDir()
	first := filepath.Join(directory, "first.json")
	second := filepath.Join(directory, "second.json")
	writeTrafficMigrationTestConfig(t, first, `{
  "experimental": {
    "traffic_statistics": {
      "enabled": true,
      "instance_id": "merged-instance",
      "storage": {
        "type": "postgres",
        "dsn": "postgres://user:secret@invalid.invalid/not_traffic",
        "schema": "first_schema",
        "schema_management": "validate",
        "max_open_connections": 2,
        "min_idle_connections": 0,
        "connect_timeout": "1s",
        "statement_timeout": "2s"
      }
    }
  }
}`)
	writeTrafficMigrationTestConfig(t, second, `{
  "experimental": {
    "traffic_statistics": {
      "storage": {
        "schema": "merged_schema"
      }
    }
  }
}`)
	previousMigrate := migrateBoltTrafficStatisticsToPostgres
	defer func() { migrateBoltTrafficStatisticsToPostgres = previousMigrate }()
	called := false
	migrateBoltTrafficStatisticsToPostgres = func(
		_ context.Context,
		_ log.ContextLogger,
		options trafficcontrol.TrafficStatisticsMigrationOptions,
	) (trafficcontrol.TrafficStatisticsMigrationResult, error) {
		called = true
		if options.Traffic.Schema != "first_schema" ||
			options.Traffic.InstanceID != "merged-instance" ||
			options.Traffic.MaxOpenConnections != 2 ||
			options.Traffic.ConnectTimeout != time.Second ||
			options.Traffic.StatementTimeout != 2*time.Second {
			t.Fatalf("unexpected merged migration options: %+v", options.Traffic)
		}
		return trafficcontrol.TrafficStatisticsMigrationResult{
			MigrationID: "test",
			Completed:   true,
		}, nil
	}
	_, _, err := runTrafficMigrationCommandForTest(
		t,
		[]string{first, second},
		context.Background(),
		trafficStatisticsMigrateCLIOptions{
			Source:    "unused.db",
			BatchSize: 500,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("migration operation was not invoked")
	}
}

func TestTrafficStatisticsMigrateCommandRejectsNonPostgresConfig(t *testing.T) {
	for name, content := range map[string]string{
		"disabled": `{"experimental":{"traffic_statistics":{"enabled":false}}}`,
		"bolt":     `{"experimental":{"traffic_statistics":{"enabled":true}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.json")
			writeTrafficMigrationTestConfig(t, path, content)
			_, _, err := runTrafficMigrationCommandForTest(
				t,
				[]string{path},
				context.Background(),
				trafficStatisticsMigrateCLIOptions{BatchSize: 500},
			)
			if err == nil {
				t.Fatal("non-PostgreSQL configuration was accepted")
			}
		})
	}
}

func TestTrafficStatisticsMigrateCommandRejectsOutboundOverride(t *testing.T) {
	previousOutbound := commandToolsFlagOutbound
	defer func() { commandToolsFlagOutbound = previousOutbound }()
	commandToolsFlagOutbound = "forbidden"
	_, _, err := runTrafficMigrationCommandForTest(
		t,
		[]string{filepath.Join(t.TempDir(), "missing.json")},
		context.Background(),
		trafficStatisticsMigrateCLIOptions{BatchSize: 500},
	)
	if err == nil || !strings.Contains(err.Error(), "--outbound") {
		t.Fatalf("unexpected outbound override result: %v", err)
	}
}

func TestTrafficStatisticsMigrateCommandDefersDetourRuntimeUntilSourcePrepared(
	t *testing.T,
) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "config.json")
	writeTrafficMigrationTestConfig(t, configPath, `{
  "outbounds": [
    {
      "type": "socks",
      "tag": "migration-socks",
      "server": "127.0.0.1",
      "server_port": 1
    }
  ],
  "route": {
    "rule_set": [
      {
        "type": "remote",
        "tag": "unrelated-rule-set",
        "format": "binary",
        "url": "https://invalid.invalid/unrelated.srs"
      }
    ]
  },
  "experimental": {
    "traffic_statistics": {
      "enabled": true,
      "instance_id": "migration-runtime-order",
      "identity_path": "`+filepath.ToSlash(filepath.Join(directory, "identity.json"))+`",
      "storage": {
        "type": "postgres",
        "dsn": "postgres://user:password@127.0.0.1:1/statistics?sslmode=disable",
        "schema": "migration_runtime_order",
        "schema_management": "validate",
        "max_open_connections": 1,
        "min_idle_connections": 0,
        "connect_timeout": "100ms",
        "statement_timeout": "1s",
        "dialer": {
          "detour": "migration-socks"
        }
      }
    }
  }
}`)

	previousMigrate := migrateBoltTrafficStatisticsToPostgres
	defer func() {
		migrateBoltTrafficStatisticsToPostgres = previousMigrate
	}()
	var called bool
	runtimeStartedEarly := errors.New("detour runtime started before source preparation")
	migrateBoltTrafficStatisticsToPostgres = func(
		ctx context.Context,
		_ log.ContextLogger,
		_ trafficcontrol.TrafficStatisticsMigrationOptions,
	) (trafficcontrol.TrafficStatisticsMigrationResult, error) {
		called = true
		if service.FromContext[adapter.OutboundManager](ctx) != nil {
			return trafficcontrol.TrafficStatisticsMigrationResult{}, runtimeStartedEarly
		}
		startRuntime := service.FromContext[func() (
			context.Context,
			log.ContextLogger,
			func() error,
			error,
		)](ctx)
		if startRuntime == nil {
			return trafficcontrol.TrafficStatisticsMigrationResult{},
				errors.New("detour runtime starter is missing")
		}
		runtimeCtx, _, closeRuntime, startErr := startRuntime()
		if startErr != nil {
			return trafficcontrol.TrafficStatisticsMigrationResult{}, startErr
		}
		if service.FromContext[adapter.HTTPClientManager](runtimeCtx) != nil {
			_ = closeRuntime()
			return trafficcontrol.TrafficStatisticsMigrationResult{},
				errors.New("detour runtime constructed an HTTP client manager")
		}
		inboundManager := service.FromContext[adapter.InboundManager](runtimeCtx)
		if inboundManager == nil || len(inboundManager.Inbounds()) != 0 {
			_ = closeRuntime()
			return trafficcontrol.TrafficStatisticsMigrationResult{},
				errors.New("detour runtime constructed inbound objects")
		}
		if closeErr := closeRuntime(); closeErr != nil {
			return trafficcontrol.TrafficStatisticsMigrationResult{}, closeErr
		}
		return trafficcontrol.TrafficStatisticsMigrationResult{
			Completed: true,
		}, nil
	}

	baseContext := include.Context(context.Background())
	_, _, err := runTrafficMigrationCommandForTest(
		t,
		[]string{configPath},
		baseContext,
		trafficStatisticsMigrateCLIOptions{
			Source:    filepath.Join(directory, "source.db"),
			BatchSize: 500,
		},
	)
	if !called {
		t.Fatalf("migration entry point was not called: %v", err)
	}
	if err != nil {
		t.Fatalf("detour runtime was started before source preparation: %v", err)
	}
	if service.FromContext[adapter.OutboundManager](baseContext) != nil {
		t.Fatal("detour runtime polluted the caller service registry")
	}
}

func TestTrafficStatisticsMigrateCommandRejectsInvalidFlagsBeforeSideEffects(t *testing.T) {
	for _, options := range []trafficStatisticsMigrateCLIOptions{
		{BatchSize: 0},
		{BatchSize: 10001},
		{BatchSize: 500, From: "2024-01-01T00:00:01Z"},
		{BatchSize: 500, To: "invalid"},
		{BatchSize: 500, Resume: "ABC"},
		{BatchSize: 500, ActiveConfigRevision: "ABC"},
		{
			BatchSize: 500,
			From:      "2024-01-02T00:00:00Z",
			To:        "2024-01-01T00:00:00Z",
		},
	} {
		_, _, err := runTrafficMigrationCommandForTest(
			t,
			[]string{filepath.Join(t.TempDir(), "missing.json")},
			context.Background(),
			options,
		)
		if err == nil || errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalid flags reached configuration side effects: %+v: %v", options, err)
		}
	}
}

func TestTrafficStatisticsMigrateCommandDetectsLockedSourceAndPreservesBytes(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "traffic.db")
	writeTrafficMigrationFixture(t, path)
	before := snapshotTrafficMigrationFixture(t, path)
	configPath := filepath.Join(directory, "config.json")
	writeTrafficMigrationPostgresConfig(t, configPath, directory)
	database, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, migrationErr := runTrafficMigrationCommandForTest(
		t,
		[]string{configPath},
		context.Background(),
		trafficStatisticsMigrateCLIOptions{
			Source:     path,
			InstanceID: "locked-source",
			BatchSize:  500,
			DryRun:     true,
		},
	)
	if closeErr := database.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if !errors.Is(migrationErr, trafficcontrol.ErrTrafficMigrationSourceInUse) {
		t.Fatalf("unexpected locked-source error: %v", migrationErr)
	}
	assertTrafficMigrationFixtureState(t, path, before)
}

func TestTrafficStatisticsMigrateCommandRejectsSymlinkSourceWithoutMutation(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "traffic.db")
	target := filepath.Join(directory, "target.db")
	writeTrafficMigrationFixture(t, target)
	before := snapshotTrafficMigrationFixture(t, target)
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(directory, "config.json")
	writeTrafficMigrationPostgresConfig(t, configPath, directory)
	_, _, err := runTrafficMigrationCommandForTest(
		t,
		[]string{configPath},
		context.Background(),
		trafficStatisticsMigrateCLIOptions{
			Source:     path,
			InstanceID: "symlink-source",
			BatchSize:  500,
			DryRun:     true,
		},
	)
	if !errors.Is(err, trafficcontrol.ErrTrafficMigrationSourceInvalid) {
		t.Fatalf("unexpected symlink-source error: %v", err)
	}
	assertTrafficMigrationFixtureState(t, target, before)
}

func TestTrafficStatisticsMigrateCommandRejectsCorruptRawBoltWithoutMutation(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "traffic.db")
	writeTrafficMigrationFixture(t, path)
	database, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = database.Update(func(tx *bbolt.Tx) error {
		metadata := tx.Bucket(testTrafficMetadata)
		return metadata.Put([]byte("future_metadata"), []byte("invalid"))
	})
	if closeErr := database.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	before := snapshotTrafficMigrationFixture(t, path)
	configPath := filepath.Join(directory, "config.json")
	writeTrafficMigrationPostgresConfig(t, configPath, directory)
	_, _, err = runTrafficMigrationCommandForTest(
		t,
		[]string{configPath},
		context.Background(),
		trafficStatisticsMigrateCLIOptions{
			Source:     path,
			InstanceID: "corrupt-source",
			BatchSize:  500,
			DryRun:     true,
		},
	)
	if !errors.Is(err, trafficcontrol.ErrTrafficMigrationSourceInvalid) {
		t.Fatalf("unexpected corrupt-source error: %v", err)
	}
	assertTrafficMigrationFixtureState(t, path, before)
}

func TestTrafficStatisticsMigrateCommandRejectsUnknownBoltBucketWithoutMutation(
	t *testing.T,
) {
	directory := t.TempDir()
	path := filepath.Join(directory, "traffic.db")
	writeTrafficMigrationFixture(t, path)
	database, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = database.Update(func(tx *bbolt.Tx) error {
		_, createErr := tx.CreateBucket([]byte("traffic_statistics_unexpected"))
		return createErr
	})
	closeErr := database.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	before := snapshotTrafficMigrationFixture(t, path)
	configPath := filepath.Join(directory, "config.json")
	writeTrafficMigrationPostgresConfig(t, configPath, directory)
	_, _, err = runTrafficMigrationCommandForTest(
		t,
		[]string{configPath},
		context.Background(),
		trafficStatisticsMigrateCLIOptions{
			Source:     path,
			InstanceID: "unknown-bucket",
			BatchSize:  500,
			DryRun:     true,
		},
	)
	if !errors.Is(err, trafficcontrol.ErrTrafficMigrationSourceInvalid) {
		t.Fatalf("unknown Bolt bucket was accepted: %v", err)
	}
	assertTrafficMigrationFixtureState(t, path, before)
}

func TestTrafficStatisticsMigrateCommandIgnoresUnrelatedBoltBucket(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "traffic.db")
	writeTrafficMigrationFixture(t, path)
	database, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = database.Update(func(tx *bbolt.Tx) error {
		bucket, createErr := tx.CreateBucket([]byte("unrelated"))
		if createErr != nil {
			return createErr
		}
		return bucket.Put([]byte("preserved"), []byte("value"))
	})
	closeErr := database.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	before := snapshotTrafficMigrationFixture(t, path)
	configPath := filepath.Join(directory, "config.json")
	writeTrafficMigrationPostgresConfig(t, configPath, directory)

	previousMigrate := migrateBoltTrafficStatisticsToPostgres
	defer func() {
		migrateBoltTrafficStatisticsToPostgres = previousMigrate
	}()
	sourceValidated := errors.New("source validated")
	migrateBoltTrafficStatisticsToPostgres = func(
		ctx context.Context,
		logger log.ContextLogger,
		options trafficcontrol.TrafficStatisticsMigrationOptions,
	) (trafficcontrol.TrafficStatisticsMigrationResult, error) {
		options.Progress = func(progress trafficcontrol.TrafficStatisticsMigrationProgress) error {
			if progress.Phase == "source_validated" {
				return sourceValidated
			}
			return nil
		}
		return previousMigrate(ctx, logger, options)
	}

	_, _, err = runTrafficMigrationCommandForTest(
		t,
		[]string{configPath},
		context.Background(),
		trafficStatisticsMigrateCLIOptions{
			Source:     path,
			InstanceID: "unrelated-bucket",
			BatchSize:  500,
			DryRun:     true,
		},
	)
	if !errors.Is(err, sourceValidated) {
		t.Fatalf("unrelated Bolt bucket was not ignored: %v", err)
	}
	assertTrafficMigrationFixtureState(t, path, before)
}

func TestTrafficStatisticsMigrateCommandCancellationBeforeTargetDial(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "traffic.db")
	writeTrafficMigrationFixture(t, path)
	configPath := filepath.Join(directory, "config.json")
	writeTrafficMigrationPostgresConfig(t, configPath, directory)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, stderr, err := runTrafficMigrationCommandForTest(
		t,
		[]string{configPath},
		ctx,
		trafficStatisticsMigrateCLIOptions{
			Source:     path,
			InstanceID: "canceled-source",
			BatchSize:  500,
			DryRun:     true,
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation was not preserved: %v", err)
	}
	if strings.Contains(stderr, `"phase":"source_validated"`) {
		t.Fatalf("canceled source scan emitted completed validation: %s", stderr)
	}
}

func TestTrafficStatisticsMigrateCommandDryRunDoesNotCreateIdentityOnTargetFailure(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "traffic.db")
	writeTrafficMigrationFixture(t, path)
	configPath := filepath.Join(directory, "config.json")
	writeTrafficMigrationPostgresConfig(t, configPath, directory)
	_, _, err := runTrafficMigrationCommandForTest(
		t,
		[]string{configPath},
		context.Background(),
		trafficStatisticsMigrateCLIOptions{
			Source:     path,
			InstanceID: "dry-run-source",
			BatchSize:  500,
			DryRun:     true,
		},
	)
	if err == nil {
		t.Fatal("unreachable target unexpectedly succeeded")
	}
	if _, statErr := os.Lstat(filepath.Join(directory, "identity.json")); !errors.Is(
		statErr,
		os.ErrNotExist,
	) {
		t.Fatalf("dry-run target failure created identity state: %v", statErr)
	}
}

func requireTrafficStatisticsMigrateCommand(t *testing.T) *cobra.Command {
	t.Helper()
	command, _, err := mainCommand.Find([]string{
		"tools",
		"traffic-statistics",
		"migrate",
	})
	if err != nil ||
		command == nil ||
		command.Name() != "migrate" ||
		command.Parent() == nil ||
		command.Parent().Name() != "traffic-statistics" {
		t.Fatalf("traffic statistics migrate command is absent: %v", err)
	}
	var output bytes.Buffer
	command.SetOut(&output)
	command.SetErr(&output)
	return command
}

func runTrafficMigrationCommandForTest(
	t *testing.T,
	paths []string,
	ctx context.Context,
	options trafficStatisticsMigrateCLIOptions,
) (string, string, error) {
	t.Helper()
	previousGlobalContext := globalCtx
	previousConfigPaths := configPaths
	previousConfigDirectories := configDirectories
	previousWorkingDirectory := workingDir
	previousOutbound := commandToolsFlagOutbound
	defer func() {
		globalCtx = previousGlobalContext
		configPaths = previousConfigPaths
		configDirectories = previousConfigDirectories
		workingDir = previousWorkingDirectory
		commandToolsFlagOutbound = previousOutbound
	}()
	globalCtx = ctx
	configPaths = append([]string(nil), paths...)
	configDirectories = nil
	workingDir = ""
	command := newTrafficStatisticsMigrateCommand()
	command.SetContext(ctx)
	var stdout lockedTrafficMigrationBuffer
	var stderr lockedTrafficMigrationBuffer
	command.SetOut(&stdout)
	command.SetErr(&stderr)
	err := runTrafficStatisticsMigrate(ctx, command, options)
	return stdout.String(), stderr.String(), err
}

type lockedTrafficMigrationBuffer struct {
	access sync.Mutex
	buffer bytes.Buffer
}

func (b *lockedTrafficMigrationBuffer) Write(content []byte) (int, error) {
	b.access.Lock()
	defer b.access.Unlock()
	return b.buffer.Write(content)
}

func (b *lockedTrafficMigrationBuffer) String() string {
	b.access.Lock()
	defer b.access.Unlock()
	return b.buffer.String()
}

func writeTrafficMigrationTestConfig(t *testing.T, path string, content string) {
	t.Helper()
	var value any
	if err := json.Unmarshal([]byte(content), &value); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeTrafficMigrationPostgresConfig(
	t *testing.T,
	path string,
	directory string,
) {
	t.Helper()
	content := `{
  "experimental": {
    "traffic_statistics": {
      "enabled": true,
      "identity_path": "` + filepath.ToSlash(filepath.Join(directory, "identity.json")) + `",
      "storage": {
        "type": "postgres",
        "dsn": "postgres://user:password-sentinel@127.0.0.1:1/not_traffic?sslmode=disable",
        "schema": "migration_test",
        "schema_management": "validate",
        "max_open_connections": 1,
        "min_idle_connections": 0,
        "connect_timeout": "100ms",
        "statement_timeout": "1s"
      }
    }
  }
}`
	writeTrafficMigrationTestConfig(t, path, content)
}

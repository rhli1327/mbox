package box_test

import (
	"bytes"
	"context"
	stdjson "encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/common/trafficcontrol"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	singjson "github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/filemanager"
)

const (
	postgresBoxTestSecret = "P4D_SECRET_MUST_NOT_APPEAR_d938c5f1"
	postgresBoxTestDSN    = "postgres://traffic_user:" + postgresBoxTestSecret +
		"@127.0.0.1:1/arbitrary_database?sslmode=disable"
)

type postgresBoxConfig struct {
	DSN              string
	Schema           string
	SchemaManagement string
	StartupPolicy    string
	InstanceID       string
	IdentityPath     string
	SpoolPath        string
}

func TestPostgresBoxConstructionCreatesResolvedLocalState(t *testing.T) {
	basePath := t.TempDir()
	_, instance, err := constructPostgresBox(
		t,
		basePath,
		postgresBoxConfig{
			DSN:           postgresBoxTestDSN,
			Schema:        "p4d_unit",
			StartupPolicy: option.TrafficStatisticsStartupPolicyDegraded,
		},
		nil,
	)
	if err != nil {
		t.Fatal("construct PostgreSQL Box:", err)
	}
	if instance == nil {
		t.Fatal("construct PostgreSQL Box returned nil")
	}
	if err = instance.Close(); err != nil {
		t.Fatal("close unstarted PostgreSQL Box:", err)
	}

	assertRegularPrivateFile(t, filepath.Join(basePath, "state", "traffic-instance.json"))
	assertRegularPrivateFile(t, filepath.Join(basePath, "state", "traffic-spool.db"))
	if _, err = os.Stat(filepath.Join(basePath, "traffic.db")); !os.IsNotExist(err) {
		t.Fatalf("PostgreSQL selection created Bolt history file: %v", err)
	}
}

func TestPostgresBoxLocalStateFailuresAreFatal(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation is not reliably available on Windows")
	}
	basePath := t.TempDir()
	statePath := filepath.Join(basePath, "state")
	if err := os.MkdirAll(statePath, 0o755); err != nil {
		t.Fatal(err)
	}
	targetPath := filepath.Join(basePath, "identity-target")
	if err := os.WriteFile(targetPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(targetPath, filepath.Join(statePath, "traffic-instance.json")); err != nil {
		t.Fatal(err)
	}

	_, instance, err := constructPostgresBox(
		t,
		basePath,
		postgresBoxConfig{
			DSN:           postgresBoxTestDSN,
			Schema:        "p4d_unit",
			StartupPolicy: option.TrafficStatisticsStartupPolicyDegraded,
		},
		nil,
	)
	if instance != nil {
		_ = instance.Close()
		t.Fatal("local identity failure returned a Box")
	}
	if err == nil {
		t.Fatal("local identity failure unexpectedly succeeded")
	}
	if errors.Is(err, trafficcontrol.ErrPostgresRequiresDurableSpool) {
		t.Fatalf("legacy runtime gate masked local identity failure: %v", err)
	}
	if !strings.Contains(err.Error(), "regular non-symlink file") {
		t.Fatalf("unexpected local identity failure: %v", err)
	}
}

func TestPostgresBoxDegradedStartupSurvivesTransientInitialFailure(t *testing.T) {
	basePath := t.TempDir()
	ctx, instance, err := constructPostgresBox(
		t,
		basePath,
		postgresBoxConfig{
			DSN:           postgresBoxTestDSN,
			Schema:        "p4d_unit",
			StartupPolicy: option.TrafficStatisticsStartupPolicyDegraded,
		},
		nil,
	)
	if err != nil {
		t.Fatal("construct degraded PostgreSQL Box:", err)
	}
	if err = instance.Start(); err != nil {
		t.Fatalf("degraded startup did not survive transient initial failure: %v", err)
	}
	if service.PtrFromContext[trafficcontrol.History](ctx) == nil {
		t.Fatal("degraded PostgreSQL History was not registered")
	}
	if err = instance.Close(); err != nil {
		t.Fatal("close degraded PostgreSQL Box:", err)
	}
}

func TestPostgresBoxStrictStartupReturnsInitialFailure(t *testing.T) {
	basePath := t.TempDir()
	_, instance, err := constructPostgresBox(
		t,
		basePath,
		postgresBoxConfig{
			DSN:           postgresBoxTestDSN,
			Schema:        "p4d_unit",
			StartupPolicy: option.TrafficStatisticsStartupPolicyStrict,
		},
		nil,
	)
	if err != nil {
		t.Fatal("construct strict PostgreSQL Box:", err)
	}
	err = instance.Start()
	if err == nil {
		_ = instance.Close()
		t.Fatal("strict startup survived transient initial failure")
	}
	if !errors.Is(err, trafficcontrol.ErrPostgresTransientNetwork) &&
		!errors.Is(err, trafficcontrol.ErrPostgresTimeout) {
		t.Fatalf("strict startup returned unclassified failure: %v", err)
	}
	if strings.Contains(err.Error(), postgresBoxTestSecret) {
		t.Fatalf("strict startup reported DSN secret: %v", err)
	}
}

func TestPostgresBoxCloseCancelsRetryWorker(t *testing.T) {
	basePath := t.TempDir()
	_, instance, err := constructPostgresBox(
		t,
		basePath,
		postgresBoxConfig{
			DSN:           postgresBoxTestDSN,
			Schema:        "p4d_unit",
			StartupPolicy: option.TrafficStatisticsStartupPolicyDegraded,
		},
		nil,
	)
	if err != nil {
		t.Fatal("construct degraded PostgreSQL Box:", err)
	}
	if err = instance.Start(); err != nil {
		t.Fatal("start degraded PostgreSQL Box:", err)
	}

	closeResult := make(chan error, 1)
	go func() {
		closeResult <- instance.Close()
	}()
	select {
	case err = <-closeResult:
		if err != nil {
			t.Fatal("close degraded PostgreSQL Box:", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("PostgreSQL retry worker did not stop during Box.Close")
	}
}

func TestPostgresBoxSecretsAreNeverPersistedOrReported(t *testing.T) {
	basePath := t.TempDir()
	_, instance, err := constructPostgresBox(
		t,
		basePath,
		postgresBoxConfig{
			DSN:           postgresBoxTestDSN,
			Schema:        "p4d_unit",
			StartupPolicy: option.TrafficStatisticsStartupPolicyStrict,
		},
		nil,
	)
	if err != nil {
		t.Fatal("construct strict PostgreSQL Box:", err)
	}
	startErr := instance.Start()
	if startErr == nil {
		_ = instance.Close()
		t.Fatal("strict startup unexpectedly succeeded")
	}
	reported := startErr.Error()
	if strings.Contains(reported, postgresBoxTestSecret) {
		t.Fatalf("DSN secret was reported: %s", reported)
	}
	assertTreeDoesNotContain(t, basePath, []byte(postgresBoxTestSecret))
}

func TestTrafficStatisticsStorageSelection(t *testing.T) {
	t.Run("bolt", func(t *testing.T) {
		basePath := t.TempDir()
		ctx := newBoxTestContext(basePath)
		var options option.Options
		if err := singjson.UnmarshalContext(
			ctx,
			[]byte(`{"experimental":{"traffic_statistics":{"enabled":true}}}`),
			&options,
		); err != nil {
			t.Fatal(err)
		}
		instance, err := box.New(box.Options{Context: ctx, Options: options})
		if err != nil {
			t.Fatal("construct Bolt Box:", err)
		}
		if err = instance.Start(); err != nil {
			_ = instance.Close()
			t.Fatal("start Bolt Box:", err)
		}
		if err = instance.Close(); err != nil {
			t.Fatal("close Bolt Box:", err)
		}
		assertRegularPrivateFile(t, filepath.Join(basePath, "traffic.db"))
		if _, err = os.Stat(filepath.Join(basePath, "state", "traffic-spool.db")); !os.IsNotExist(err) {
			t.Fatalf("Bolt selection created PostgreSQL spool: %v", err)
		}
	})

	t.Run("postgres", func(t *testing.T) {
		basePath := t.TempDir()
		_, instance, err := constructPostgresBox(
			t,
			basePath,
			postgresBoxConfig{
				DSN:           postgresBoxTestDSN,
				Schema:        "p4d_unit",
				StartupPolicy: option.TrafficStatisticsStartupPolicyDegraded,
			},
			nil,
		)
		if err != nil {
			t.Fatal("construct PostgreSQL Box:", err)
		}
		if err = instance.Close(); err != nil {
			t.Fatal("close PostgreSQL Box:", err)
		}
		assertRegularPrivateFile(t, filepath.Join(basePath, "state", "traffic-spool.db"))
		if _, err = os.Stat(filepath.Join(basePath, "traffic.db")); !os.IsNotExist(err) {
			t.Fatalf("PostgreSQL selection created Bolt history file: %v", err)
		}
	})
}

func newBoxTestContext(basePath string) context.Context {
	ctx := filemanager.WithDefault(
		context.Background(),
		basePath,
		"",
		os.Getuid(),
		os.Getgid(),
	)
	ctx = service.ContextWithDefaultRegistry(ctx)
	return include.Context(ctx)
}

func constructPostgresBox(
	t *testing.T,
	basePath string,
	config postgresBoxConfig,
	writer log.PlatformWriter,
) (context.Context, *box.Box, error) {
	t.Helper()
	ctx := newBoxTestContext(basePath)
	if config.DSN == "" {
		config.DSN = postgresBoxTestDSN
	}
	if config.Schema == "" {
		config.Schema = "public"
	}
	if config.StartupPolicy == "" {
		config.StartupPolicy = option.TrafficStatisticsStartupPolicyDegraded
	}
	if config.IdentityPath == "" {
		config.IdentityPath = "state/traffic-instance.json"
	}
	if config.SpoolPath == "" {
		config.SpoolPath = "state/traffic-spool.db"
	}
	storage := map[string]any{
		"type":                 option.TrafficStatisticsStorageTypePostgres,
		"dsn":                  config.DSN,
		"schema":               config.Schema,
		"max_open_connections": 2,
		"min_idle_connections": 0,
		"connect_timeout":      "250ms",
		"statement_timeout":    "2s",
	}
	if config.SchemaManagement != "" {
		storage["schema_management"] = config.SchemaManagement
	}
	traffic := map[string]any{
		"enabled":        true,
		"identity_path":  config.IdentityPath,
		"storage":        storage,
		"spool":          map[string]any{"path": config.SpoolPath},
		"startup_policy": config.StartupPolicy,
	}
	if config.InstanceID != "" {
		traffic["instance_id"] = config.InstanceID
	}
	content, err := stdjson.Marshal(map[string]any{
		"experimental": map[string]any{
			"traffic_statistics": traffic,
		},
	})
	if err != nil {
		t.Fatal("marshal PostgreSQL test configuration:", err)
	}
	var options option.Options
	if err = singjson.UnmarshalContext(ctx, content, &options); err != nil {
		t.Fatal("decode PostgreSQL test configuration:", err)
	}
	instance, err := box.New(box.Options{
		Context:           ctx,
		Options:           options,
		PlatformLogWriter: writer,
	})
	return ctx, instance, err
}

func assertRegularPrivateFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("%q is not a regular file: %s", path, info.Mode())
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("%q permissions = %o, want 600", path, info.Mode().Perm())
	}
}

func assertTreeDoesNotContain(t *testing.T, root string, secret []byte) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if bytes.Contains(content, secret) {
			t.Errorf("secret persisted in %q", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal("scan local state for secret:", err)
	}
}

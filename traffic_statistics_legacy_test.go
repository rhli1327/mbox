package box_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/service/filemanager"

	"github.com/sagernet/sing-box/common/trafficcontrol"
)

func TestTrafficStatisticsLegacyConfigurationUsesExpectedBoltPath(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		content      string
		expectedPath string
		unexpected   string
	}{
		{
			name:         "explicit legacy path",
			content:      `{"experimental":{"traffic_statistics":{"enabled":true,"path":"custom-traffic.db"}}}`,
			expectedPath: "custom-traffic.db",
			unexpected:   "traffic.db",
		},
		{
			name:         "default legacy path",
			content:      `{"experimental":{"traffic_statistics":{"enabled":true}}}`,
			expectedPath: "traffic.db",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			basePath := t.TempDir()
			ctx := include.Context(filemanager.WithDefault(
				context.Background(),
				basePath,
				"",
				os.Getuid(),
				os.Getgid(),
			))
			var options option.Options
			if err := json.UnmarshalContext(ctx, []byte(testCase.content), &options); err != nil {
				t.Fatal("decode legacy traffic statistics configuration:", err)
			}
			if options.Experimental == nil ||
				options.Experimental.TrafficStatistics == nil ||
				!options.Experimental.TrafficStatistics.Enabled {
				t.Fatalf("legacy traffic statistics options were not decoded: %#v", options.Experimental)
			}

			instance, err := box.New(box.Options{
				Context: ctx,
				Options: options,
			})
			if err != nil {
				t.Fatal("create box from legacy traffic statistics configuration:", err)
			}
			if err = instance.Start(); err != nil {
				_ = instance.Close()
				t.Fatal("start box from legacy traffic statistics configuration:", err)
			}
			if err = instance.Close(); err != nil {
				t.Fatal("close box from legacy traffic statistics configuration:", err)
			}

			expectedPath := filepath.Join(basePath, testCase.expectedPath)
			info, err := os.Stat(expectedPath)
			if err != nil {
				t.Fatalf("stat expected traffic database %q: %v", expectedPath, err)
			}
			if !info.Mode().IsRegular() {
				t.Fatalf("traffic database is not a regular file: %s", info.Mode())
			}
			permissions := info.Mode().Perm()
			if runtime.GOOS == "windows" {
				if permissions&0o200 == 0 {
					t.Fatalf(
						"traffic database is not writable on Windows: got mode %o",
						permissions,
					)
				}
			} else if permissions != 0o600 {
				t.Fatalf(
					"unexpected traffic database permissions: got %o, want 600",
					permissions,
				)
			}
			if testCase.unexpected != "" {
				unexpectedPath := filepath.Join(basePath, testCase.unexpected)
				if _, err = os.Stat(unexpectedPath); !os.IsNotExist(err) {
					t.Fatalf("unexpected traffic database created at %q: %v", unexpectedPath, err)
				}
			}
		})
	}
}

func TestPostgresRuntimeRequiresDurableSpool(t *testing.T) {
	basePath := t.TempDir()
	ctx := include.Context(filemanager.WithDefault(
		context.Background(),
		basePath,
		"",
		os.Getuid(),
		os.Getgid(),
	))
	var options option.Options
	err := json.UnmarshalContext(ctx, []byte(`{
		"experimental": {
			"traffic_statistics": {
				"enabled": true,
				"identity_path": "state/traffic-instance.json",
				"storage": {
					"type": "postgres",
					"dsn": "postgres://user:P2_SECRET_MUST_NOT_APPEAR_7f6e8a3c@does-not-exist.invalid/arbitrary"
				},
				"spool": {"path": "state/traffic-spool.db"}
			}
		}
	}`), &options)
	if err != nil {
		t.Fatal("decode PostgreSQL traffic statistics configuration:", err)
	}
	instance, err := box.New(box.Options{
		Context: ctx,
		Options: options,
	})
	if instance != nil {
		_ = instance.Close()
		t.Fatal("PostgreSQL traffic statistics unexpectedly created a Box")
	}
	if !errors.Is(err, trafficcontrol.ErrPostgresRequiresDurableSpool) {
		t.Fatalf("unexpected PostgreSQL runtime gate error: %v", err)
	}
	if err.Error() != "traffic statistics PostgreSQL recording requires durable spool support" {
		t.Fatalf("unstable PostgreSQL runtime gate text: %q", err)
	}
	entries, readErr := os.ReadDir(basePath)
	if readErr != nil {
		t.Fatal("read isolated base path:", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("PostgreSQL runtime gate created local state: %#v", entries)
	}
}

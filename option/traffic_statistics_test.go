package option

import (
	"context"
	"testing"
	"time"

	"github.com/sagernet/sing/common/byteformats"
	"github.com/sagernet/sing/common/json"
)

func TestTrafficStatisticsStorageOptions(t *testing.T) {
	t.Run("legacy default", func(t *testing.T) {
		resolved := resolveTrafficStatisticsJSON(t, `{"enabled":true}`)
		if resolved.StorageType != TrafficStatisticsStorageTypeBolt ||
			resolved.Path != "traffic.db" {
			t.Fatalf("unexpected legacy defaults: %#v", resolved)
		}
	})

	t.Run("legacy explicit path", func(t *testing.T) {
		resolved := resolveTrafficStatisticsJSON(
			t,
			`{"enabled":true,"path":"custom-traffic.db"}`,
		)
		if resolved.StorageType != TrafficStatisticsStorageTypeBolt ||
			resolved.Path != "custom-traffic.db" {
			t.Fatalf("unexpected legacy resolution: %#v", resolved)
		}
	})

	t.Run("explicit bolt defaults", func(t *testing.T) {
		resolved := resolveTrafficStatisticsJSON(
			t,
			`{"enabled":true,"storage":{"type":"bolt"}}`,
		)
		if resolved.StorageType != TrafficStatisticsStorageTypeBolt ||
			resolved.Path != "traffic.db" {
			t.Fatalf("unexpected explicit Bolt defaults: %#v", resolved)
		}
	})

	t.Run("postgres defaults", func(t *testing.T) {
		resolved := resolveTrafficStatisticsJSON(
			t,
			`{"enabled":true,"storage":{"type":"postgres","dsn":"postgres://db.invalid/arbitrary"}}`,
		)
		if resolved.StorageType != TrafficStatisticsStorageTypePostgres ||
			resolved.DSN != "postgres://db.invalid/arbitrary" ||
			resolved.Schema != "public" ||
			resolved.SchemaManagement != TrafficStatisticsSchemaManagementAuto ||
			resolved.MaxOpenConnections != 4 ||
			resolved.MinIdleConnections != 1 ||
			resolved.ConnectTimeout != 10*time.Second ||
			resolved.StatementTimeout != 30*time.Second ||
			resolved.IdentityPath != "traffic-instance.json" ||
			resolved.SpoolPath != "traffic-spool.db" ||
			resolved.SpoolMaxSize != 256*byteformats.MiByte ||
			resolved.SpoolOverflow != TrafficStatisticsSpoolOverflowDropOldest ||
			resolved.StartupPolicy != TrafficStatisticsStartupPolicyDegraded {
			t.Fatalf("unexpected PostgreSQL defaults: %#v", resolved)
		}
	})

	t.Run("postgres explicit values", func(t *testing.T) {
		resolved := resolveTrafficStatisticsJSON(t, `{
			"enabled": true,
			"instance_id": "edge-1",
			"identity_path": "identity/custom.json",
			"storage": {
				"type": "postgres",
				"dsn": "postgres://db.invalid/not-traffic",
				"schema": "tenant_42",
				"schema_management": "validate",
				"dialer": {"detour": "hy2-out"},
				"max_open_connections": 9,
				"min_idle_connections": 0,
				"connect_timeout": "3s",
				"statement_timeout": "7s"
			},
			"spool": {
				"path": "spool/custom.db",
				"max_size": "8MB",
				"overflow": "drop_oldest"
			},
			"startup_policy": "strict"
		}`)
		if resolved.InstanceID != "edge-1" ||
			resolved.IdentityPath != "identity/custom.json" ||
			resolved.Schema != "tenant_42" ||
			resolved.SchemaManagement != TrafficStatisticsSchemaManagementValidate ||
			resolved.Dialer.Detour != "hy2-out" ||
			resolved.MaxOpenConnections != 9 ||
			resolved.MinIdleConnections != 0 ||
			resolved.ConnectTimeout != 3*time.Second ||
			resolved.StatementTimeout != 7*time.Second ||
			resolved.SpoolPath != "spool/custom.db" ||
			resolved.SpoolMaxSize != 8*byteformats.MiByte ||
			resolved.StartupPolicy != TrafficStatisticsStartupPolicyStrict {
			t.Fatalf("unexpected explicit PostgreSQL resolution: %#v", resolved)
		}
	})

	for _, testCase := range []struct {
		name    string
		content string
		message string
	}{
		{
			"ambiguous legacy and typed storage",
			`{"enabled":true,"path":"traffic.db","storage":{"type":"bolt"}}`,
			"traffic statistics legacy path and storage are mutually exclusive",
		},
		{
			"missing storage type",
			`{"enabled":true,"storage":{}}`,
			"traffic statistics storage type is required",
		},
		{
			"unknown storage type",
			`{"enabled":true,"storage":{"type":"sqlite"}}`,
			"unsupported traffic statistics storage type: sqlite",
		},
		{
			"missing postgres DSN",
			`{"enabled":true,"storage":{"type":"postgres"}}`,
			"traffic statistics PostgreSQL DSN is required",
		},
		{
			"invalid schema",
			`{"enabled":true,"storage":{"type":"postgres","dsn":"postgres://db.invalid/db","schema":"bad.name"}}`,
			"invalid traffic statistics PostgreSQL schema: bad.name",
		},
		{
			"invalid instance identifier",
			`{"enabled":true,"instance_id":" edge","storage":{"type":"postgres","dsn":"postgres://db.invalid/db"}}`,
			"invalid traffic statistics instance ID",
		},
		{
			"invalid max pool size",
			`{"enabled":true,"storage":{"type":"postgres","dsn":"postgres://db.invalid/db","max_open_connections":0}}`,
			"traffic statistics max_open_connections must be at least 1",
		},
		{
			"invalid min pool size",
			`{"enabled":true,"storage":{"type":"postgres","dsn":"postgres://db.invalid/db","max_open_connections":2,"min_idle_connections":3}}`,
			"traffic statistics min_idle_connections must be between 0 and max_open_connections",
		},
		{
			"invalid connect timeout",
			`{"enabled":true,"storage":{"type":"postgres","dsn":"postgres://db.invalid/db","connect_timeout":"0s"}}`,
			"traffic statistics connect_timeout must be positive",
		},
		{
			"invalid statement timeout",
			`{"enabled":true,"storage":{"type":"postgres","dsn":"postgres://db.invalid/db","statement_timeout":"0s"}}`,
			"traffic statistics statement_timeout must be positive",
		},
		{
			"bolt with postgres fields",
			`{"enabled":true,"storage":{"type":"bolt","dsn":"postgres://db.invalid/db"}}`,
			"traffic statistics Bolt storage does not accept PostgreSQL options",
		},
		{
			"bolt with identity",
			`{"enabled":true,"instance_id":"edge","storage":{"type":"bolt"}}`,
			"traffic statistics Bolt storage does not accept PostgreSQL options",
		},
		{
			"legacy bolt with identity",
			`{"enabled":true,"instance_id":"edge"}`,
			"traffic statistics Bolt storage does not accept PostgreSQL options",
		},
		{
			"postgres with storage path",
			`{"enabled":true,"storage":{"type":"postgres","path":"traffic.db","dsn":"postgres://db.invalid/db"}}`,
			"traffic statistics PostgreSQL storage does not accept a Bolt path",
		},
		{
			"unknown schema mode",
			`{"enabled":true,"storage":{"type":"postgres","dsn":"postgres://db.invalid/db","schema_management":"repair"}}`,
			"unsupported traffic statistics schema management mode: repair",
		},
		{
			"unknown startup policy",
			`{"enabled":true,"storage":{"type":"postgres","dsn":"postgres://db.invalid/db"},"startup_policy":"online_only"}`,
			"unsupported traffic statistics startup policy: online_only",
		},
		{
			"unknown overflow policy",
			`{"enabled":true,"storage":{"type":"postgres","dsn":"postgres://db.invalid/db"},"spool":{"overflow":"reject"}}`,
			"unsupported traffic statistics spool overflow policy: reject",
		},
		{
			"zero spool size",
			`{"enabled":true,"storage":{"type":"postgres","dsn":"postgres://db.invalid/db"},"spool":{"max_size":0}}`,
			"traffic statistics spool max_size must be positive",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			options := decodeTrafficStatisticsOptions(t, testCase.content)
			_, err := ResolveTrafficStatisticsOptions(&options)
			if err == nil || err.Error() != testCase.message {
				t.Fatalf("unexpected resolution error: got %v, want %q", err, testCase.message)
			}
		})
	}
}

func resolveTrafficStatisticsJSON(t *testing.T, content string) ResolvedTrafficStatisticsOptions {
	t.Helper()
	options := decodeTrafficStatisticsOptions(t, content)
	resolved, err := ResolveTrafficStatisticsOptions(&options)
	if err != nil {
		t.Fatal("resolve traffic statistics options:", err)
	}
	return resolved
}

func decodeTrafficStatisticsOptions(t *testing.T, content string) TrafficStatisticsOptions {
	t.Helper()
	var options TrafficStatisticsOptions
	if err := json.UnmarshalContext(context.Background(), []byte(content), &options); err != nil {
		t.Fatal("decode traffic statistics options:", err)
	}
	return options
}

package trafficcontrol

import (
	"bytes"
	"context"
	"testing"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/byteformats"
	"github.com/sagernet/sing/common/json/badoption"
)

func TestTrafficStatisticsRevisionInput(t *testing.T) {
	revisionKey := bytes.Repeat([]byte{0x5a}, 32)
	base := postgresRevisionTestOptions([]string{"node-a", "node-b"}, 1080)
	baseline, err := buildPostgresRevisionInput(context.Background(), base, revisionKey)
	if err != nil {
		t.Fatal("build baseline revision:", err)
	}
	repeated, err := buildPostgresRevisionInput(context.Background(), base, revisionKey)
	if err != nil {
		t.Fatal("repeat baseline revision:", err)
	}
	if !bytes.Equal(baseline.CanonicalContent, repeated.CanonicalContent) ||
		baseline.RoutingFingerprint != repeated.RoutingFingerprint ||
		baseline.ConfigRevision != repeated.ConfigRevision {
		t.Fatal("identical routing configuration did not produce a stable revision")
	}
	if len(baseline.ConfigRevision) != 32 {
		t.Fatalf("config revision length = %d, want 32", len(baseline.ConfigRevision))
	}

	operational := postgresRevisionTestOptions([]string{"node-a", "node-b"}, 1080)
	operational.Experimental.TrafficStatistics = &option.TrafficStatisticsOptions{
		Enabled:      true,
		InstanceID:   "different-instance",
		IdentityPath: "different-identity.json",
		Path:         "legacy-ignored.db",
		Storage: &option.TrafficStatisticsStorageOptions{
			Type:               option.TrafficStatisticsStorageTypePostgres,
			DSN:                "postgres://user:P2_SECRET_MUST_NOT_APPEAR_7f6e8a3c@other.example/arbitrary",
			Schema:             "tenant_other",
			SchemaManagement:   option.TrafficStatisticsSchemaManagementValidate,
			Dialer:             option.DialerOptions{Detour: "other-detour"},
			MaxOpenConnections: intPointer(12),
			MinIdleConnections: intPointer(2),
			ConnectTimeout:     durationPointer(15),
			StatementTimeout:   durationPointer(45),
		},
		Spool: &option.TrafficStatisticsSpoolOptions{
			Path:     "other-spool.db",
			MaxSize:  memoryBytesPointer(t, `"8MB"`),
			Overflow: option.TrafficStatisticsSpoolOverflowDropOldest,
		},
		StartupPolicy: option.TrafficStatisticsStartupPolicyStrict,
	}
	operationalRevision, err := buildPostgresRevisionInput(
		context.Background(),
		operational,
		revisionKey,
	)
	if err != nil {
		t.Fatal("build operational revision:", err)
	}
	if baseline.RoutingFingerprint != operationalRevision.RoutingFingerprint ||
		baseline.ConfigRevision != operationalRevision.ConfigRevision {
		t.Fatal("traffic storage operational options changed routing attribution revision")
	}
	if bytes.Contains(
		operationalRevision.CanonicalContent,
		[]byte("P2_SECRET_MUST_NOT_APPEAR_7f6e8a3c"),
	) {
		t.Fatal("canonical routing content leaked the PostgreSQL password")
	}

	changedRoute := postgresRevisionTestOptions([]string{"node-a", "node-c"}, 1080)
	routeRevision, err := buildPostgresRevisionInput(context.Background(), changedRoute, revisionKey)
	if err != nil {
		t.Fatal("build changed route revision:", err)
	}
	if baseline.RoutingFingerprint == routeRevision.RoutingFingerprint ||
		baseline.ConfigRevision == routeRevision.ConfigRevision {
		t.Fatal("routing selection change did not change attribution revision")
	}

	changedOutbound := postgresRevisionTestOptions([]string{"node-a", "node-b"}, 2080)
	outboundRevision, err := buildPostgresRevisionInput(
		context.Background(),
		changedOutbound,
		revisionKey,
	)
	if err != nil {
		t.Fatal("build changed outbound revision:", err)
	}
	if baseline.RoutingFingerprint == outboundRevision.RoutingFingerprint ||
		baseline.ConfigRevision == outboundRevision.ConfigRevision {
		t.Fatal("outbound configuration change did not change attribution revision")
	}
}

func postgresRevisionTestOptions(groupMembers []string, serverPort uint16) option.Options {
	return option.Options{
		Experimental: &option.ExperimentalOptions{
			TrafficStatistics: &option.TrafficStatisticsOptions{
				Enabled: true,
			},
		},
		Outbounds: []option.Outbound{
			{
				Type: C.TypeSelector,
				Tag:  "Proxy",
				Options: &option.SelectorOutboundOptions{
					Outbounds: append([]string(nil), groupMembers...),
				},
			},
			{
				Type: C.TypeSOCKS,
				Tag:  "node-a",
				Options: &option.SOCKSOutboundOptions{
					ServerOptions: option.ServerOptions{
						Server:     "127.0.0.1",
						ServerPort: serverPort,
					},
					Version: "5",
				},
			},
		},
	}
}

func intPointer(value int) *int {
	return &value
}

func durationPointer(seconds int64) *badoption.Duration {
	value := badoption.Duration(seconds * 1_000_000_000)
	return &value
}

func memoryBytesPointer(t *testing.T, content string) *byteformats.MemoryBytes {
	t.Helper()
	var value byteformats.MemoryBytes
	if err := value.UnmarshalJSON([]byte(content)); err != nil {
		t.Fatal(err)
	}
	return &value
}

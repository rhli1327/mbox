package trafficcontrol

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgresBatchEncodingIsCanonical(t *testing.T) {
	var _ historyStore = (*postgresStore)(nil)
	firstKey := historyKey{
		Bucket:             60,
		ConfigRevision:     "00112233445566778899aabbccddeeff",
		RouteTag:           "route",
		GroupPath:          `["group"]`,
		DestinationDomain:  "example.com",
		ActualOutboundTag:  "proxy",
		ActualOutboundType: "mieru",
		Network:            "tcp",
	}
	secondKey := firstKey
	secondKey.DestinationDomain = ""
	secondKey.DestinationIP = "192.0.2.1"
	first := historyBatch{
		firstKey:  {UplinkBytes: 1, DownlinkBytes: 2, Connections: 3},
		secondKey: {UplinkBytes: 4, DownlinkBytes: 5, Connections: 6},
	}
	second := historyBatch{
		secondKey: {UplinkBytes: 4, DownlinkBytes: 5, Connections: 6},
		firstKey:  {UplinkBytes: 1, DownlinkBytes: 2, Connections: 3},
	}
	firstRecords, firstIdentity, err := encodePostgresBatch(first)
	if err != nil {
		t.Fatal(err)
	}
	secondRecords, secondIdentity, err := encodePostgresBatch(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstIdentity.Checksum != secondIdentity.Checksum ||
		firstIdentity.Count != secondIdentity.Count ||
		!equalPostgresBatchTime(
			firstIdentity.FirstBucket,
			secondIdentity.FirstBucket,
		) ||
		!equalPostgresBatchTime(
			firstIdentity.LastBucket,
			secondIdentity.LastBucket,
		) {
		t.Fatal("batch identity depends on map iteration order")
	}
	if len(firstRecords) != len(secondRecords) {
		t.Fatal("batch record count changed")
	}
	for index := range firstRecords {
		if !bytes.Equal(firstRecords[index].Encoded, secondRecords[index].Encoded) {
			t.Fatal("batch ordering is not canonical")
		}
	}
	if firstIdentity.FirstBucket == nil ||
		firstIdentity.LastBucket == nil ||
		!firstIdentity.FirstBucket.Equal(time.Unix(60, 0)) ||
		!firstIdentity.LastBucket.Equal(time.Unix(60, 0)) {
		t.Fatal("batch bucket range is not stable")
	}
	if _, err = uuid.FromString("00000000-0000-0000-0000-000000000001"); err != nil {
		t.Fatal(err)
	}

	nonCanonicalKey := firstKey
	nonCanonicalKey.DestinationDomain = "Example.COM."
	canonicalKey := firstKey
	canonicalKey.DestinationDomain = "example.com"
	canonicalRecords, canonicalIdentity, err := encodePostgresBatch(historyBatch{
		nonCanonicalKey: {
			UplinkBytes:   ^uint64(0),
			DownlinkBytes: 2,
			Connections:   3,
		},
		canonicalKey: {
			UplinkBytes:   1,
			DownlinkBytes: 4,
			Connections:   5,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(canonicalRecords) != 1 ||
		canonicalIdentity.Count != 1 ||
		canonicalRecords[0].Key.DestinationDomain != "example.com" ||
		canonicalRecords[0].Key.DestinationIP != "" ||
		canonicalRecords[0].Counters != (historyCounters{
			UplinkBytes:   ^uint64(0),
			DownlinkBytes: 6,
			Connections:   8,
		}) {
		t.Fatalf(
			"non-canonical batch keys did not collapse with saturation: %#v %#v",
			canonicalRecords,
			canonicalIdentity,
		)
	}
}

func TestPostgresStoreRedactsSecrets(t *testing.T) {
	secret := "never-print-this-password"
	err := classifyPostgresError(
		context.Background(),
		"connect traffic statistics database",
		&pgconn.PgError{
			Code:    "28P01",
			Message: "password authentication failed for " + secret,
			Detail:  "postgres://user:" + secret + "@db.example/traffic",
		},
	)
	if !errors.Is(err, ErrPostgresAuthentication) {
		t.Fatalf("unexpected classification: %v", err)
	}
	if strings.Contains(err.Error(), secret) ||
		strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("secret leaked in classified error: %v", err)
	}
}

func TestPostgresMigrationManifest(t *testing.T) {
	migrations, err := loadPostgresMigrations()
	if err != nil {
		t.Fatal(err)
	}
	if len(migrations) != currentPostgresSchemaVersion {
		t.Fatalf("unexpected migration count: %d", len(migrations))
	}
	for index, migration := range migrations {
		if migration.Version != index+1 {
			t.Fatalf("unexpected migration version: %d", migration.Version)
		}
		if !strings.Contains(migration.SQL, "{{schema}}") {
			t.Fatalf("migration %d does not use the quoted schema placeholder", index+1)
		}
	}
	if postgresSchemaLockKey("traffic_a") == postgresSchemaLockKey("traffic_b") {
		t.Fatal("distinct schemas unexpectedly share an advisory lock key")
	}
}

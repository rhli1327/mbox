package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"testing"
	"time"

	"github.com/sagernet/bbolt"
)

var (
	testTrafficSummaryBucket = []byte("traffic_statistics_v1")
	testTrafficTargetBucket  = []byte("traffic_statistics_targets_v1")
	testTrafficMetadata      = []byte("traffic_statistics_metadata_v1")
)

type testTrafficMigrationRecord struct {
	Bucket             int64  `json:"bucket"`
	ConfigRevision     string `json:"config_revision"`
	RouteTag           string `json:"route_tag"`
	GroupPath          string `json:"group_path"`
	DestinationDomain  string `json:"destination_domain,omitempty"`
	DestinationIP      string `json:"destination_ip,omitempty"`
	ActualOutboundTag  string `json:"actual_outbound_tag"`
	ActualOutboundType string `json:"actual_outbound_type"`
	Network            string `json:"network"`
	UplinkBytes        uint64 `json:"uplink_bytes"`
	DownlinkBytes      uint64 `json:"downlink_bytes"`
	Connections        uint64 `json:"connections"`
}

type testTrafficMigrationKey struct {
	Bucket             int64
	ConfigRevision     string
	RouteTag           string
	GroupPath          string
	DestinationDomain  string `json:",omitempty"`
	DestinationIP      string `json:",omitempty"`
	ActualOutboundTag  string
	ActualOutboundType string
	Network            string
}

type trafficMigrationFixtureState struct {
	fingerprint [sha256.Size]byte
	size        int64
	mode        os.FileMode
	modTime     time.Time
}

func writeTrafficMigrationFixture(t *testing.T, path string) {
	t.Helper()
	database, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	revisionKey := make([]byte, sha256.Size)
	for index := range revisionKey {
		revisionKey[index] = byte(index)
	}
	targetsFrom := time.Date(2025, 1, 2, 3, 0, 0, 0, time.UTC)
	destinationsFrom := time.Date(2025, 1, 2, 3, 2, 0, 0, time.UTC)
	buckets := []time.Time{
		time.Date(2025, 1, 2, 3, 4, 0, 0, time.UTC),
		time.Date(2025, 1, 2, 3, 5, 0, 0, time.UTC),
		time.Date(2025, 1, 2, 3, 6, 0, 0, time.UTC),
	}
	firstRevision := "00112233445566778899aabbccddeeff"
	secondRevision := "ffeeddccbbaa99887766554433221100"
	err = database.Update(func(tx *bbolt.Tx) error {
		metadata, createErr := tx.CreateBucket(testTrafficMetadata)
		if createErr != nil {
			return createErr
		}
		if createErr = metadata.Put([]byte("config_revision_hmac_key"), revisionKey); createErr != nil {
			return createErr
		}
		availability := make([]byte, 8)
		binary.BigEndian.PutUint64(availability, uint64(targetsFrom.Unix()))
		if createErr = metadata.Put([]byte("target_available_from"), availability); createErr != nil {
			return createErr
		}
		binary.BigEndian.PutUint64(availability, uint64(destinationsFrom.Unix()))
		if createErr = metadata.Put([]byte("destination_available_from"), availability); createErr != nil {
			return createErr
		}
		summary, createErr := tx.CreateBucket(testTrafficSummaryBucket)
		if createErr != nil {
			return createErr
		}
		targets, createErr := tx.CreateBucket(testTrafficTargetBucket)
		if createErr != nil {
			return createErr
		}
		summaries := []testTrafficMigrationRecord{
			{
				Bucket:             buckets[2].Unix(),
				ConfigRevision:     secondRevision,
				RouteTag:           "hysteria-route",
				GroupPath:          `["urltest","hy2"]`,
				ActualOutboundTag:  "hy2",
				ActualOutboundType: "hysteria2",
				Network:            "tcp",
				UplinkBytes:        101,
				DownlinkBytes:      202,
				Connections:        3,
			},
			{
				Bucket:             buckets[0].Unix(),
				ConfigRevision:     firstRevision,
				RouteTag:           "direct-route",
				GroupPath:          `[]`,
				ActualOutboundTag:  "direct",
				ActualOutboundType: "direct",
				Network:            "tcp",
				UplinkBytes:        math.MaxUint64,
				DownlinkBytes:      2,
				Connections:        1,
			},
			{
				Bucket:             buckets[1].Unix(),
				ConfigRevision:     firstRevision,
				RouteTag:           "selector-route",
				GroupPath:          `["selector","proxy-a"]`,
				ActualOutboundTag:  "proxy-a",
				ActualOutboundType: "mieru",
				Network:            "udp",
				UplinkBytes:        33,
				DownlinkBytes:      44,
				Connections:        5,
			},
		}
		for _, record := range summaries {
			if createErr = putTrafficMigrationFixtureRecord(summary, record); createErr != nil {
				return createErr
			}
		}
		targetRecords := []testTrafficMigrationRecord{
			{
				Bucket:             buckets[1].Unix(),
				ConfigRevision:     firstRevision,
				RouteTag:           "selector-route",
				GroupPath:          `["selector","proxy-a"]`,
				ActualOutboundTag:  "proxy-a",
				ActualOutboundType: "mieru",
				Network:            "udp",
				DestinationIP:      "192.0.2.9",
				UplinkBytes:        7,
				DownlinkBytes:      8,
				Connections:        1,
			},
			{
				Bucket:             buckets[2].Unix(),
				ConfigRevision:     secondRevision,
				RouteTag:           "hysteria-route",
				GroupPath:          `["urltest","hy2"]`,
				ActualOutboundTag:  "hy2",
				ActualOutboundType: "hysteria2",
				Network:            "tcp",
				DestinationDomain:  "example.com",
				UplinkBytes:        9,
				DownlinkBytes:      10,
				Connections:        1,
			},
			{
				Bucket:             buckets[0].Unix(),
				ConfigRevision:     firstRevision,
				RouteTag:           "direct-route",
				GroupPath:          `[]`,
				ActualOutboundTag:  "direct",
				ActualOutboundType: "direct",
				Network:            "tcp",
				DestinationDomain:  "example.com",
				UplinkBytes:        11,
				DownlinkBytes:      12,
				Connections:        1,
			},
			{
				Bucket:             buckets[1].Unix(),
				ConfigRevision:     firstRevision,
				RouteTag:           "empty-destination",
				GroupPath:          `[]`,
				ActualOutboundTag:  "direct",
				ActualOutboundType: "direct",
				Network:            "tcp",
				UplinkBytes:        13,
				DownlinkBytes:      14,
				Connections:        1,
			},
		}
		for _, record := range targetRecords {
			if createErr = putTrafficMigrationFixtureRecord(targets, record); createErr != nil {
				return createErr
			}
		}
		return nil
	})
	closeErr := database.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
}

func putTrafficMigrationFixtureRecord(
	bucket *bbolt.Bucket,
	record testTrafficMigrationRecord,
) error {
	keyContent, err := json.Marshal(testTrafficMigrationKey{
		Bucket:             record.Bucket,
		ConfigRevision:     record.ConfigRevision,
		RouteTag:           record.RouteTag,
		GroupPath:          record.GroupPath,
		DestinationDomain:  record.DestinationDomain,
		DestinationIP:      record.DestinationIP,
		ActualOutboundTag:  record.ActualOutboundTag,
		ActualOutboundType: record.ActualOutboundType,
		Network:            record.Network,
	})
	if err != nil {
		return err
	}
	key := make([]byte, 8+sha256.Size)
	binary.BigEndian.PutUint64(key[:8], uint64(record.Bucket))
	sum := sha256.Sum256(keyContent)
	copy(key[8:], sum[:])
	value, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return bucket.Put(key, value)
}

func trafficMigrationFixtureBytes(t *testing.T, path string) []byte {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return content
}

func snapshotTrafficMigrationFixture(
	t *testing.T,
	path string,
) trafficMigrationFixtureState {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	content := trafficMigrationFixtureBytes(t, path)
	return trafficMigrationFixtureState{
		fingerprint: sha256.Sum256(content),
		size:        info.Size(),
		mode:        info.Mode(),
		modTime:     info.ModTime(),
	}
}

func assertTrafficMigrationFixtureState(
	t *testing.T,
	path string,
	expected trafficMigrationFixtureState,
) {
	t.Helper()
	actual := snapshotTrafficMigrationFixture(t, path)
	if actual.fingerprint != expected.fingerprint ||
		actual.size != expected.size ||
		actual.mode != expected.mode ||
		!actual.modTime.Equal(expected.modTime) {
		t.Fatalf(
			"traffic migration source changed: before=%+v after=%+v",
			expected,
			actual,
		)
	}
}

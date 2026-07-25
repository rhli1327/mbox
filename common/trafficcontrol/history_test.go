package trafficcontrol

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	M "github.com/sagernet/sing/common/metadata"
)

func TestDestinationDomainNormalizationPriorityAndFreeze(t *testing.T) {
	for name, testCase := range map[string]struct {
		metadata adapter.InboundContext
		expected string
	}{
		"destination fqdn wins": {
			metadata: adapter.InboundContext{
				Destination: M.Socksaddr{Fqdn: "API.Example.COM."},
				Domain:      "fallback.example",
			},
			expected: "api.example.com",
		},
		"sniffed domain fallback": {
			metadata: adapter.InboundContext{Domain: "ChatGPT.COM."},
			expected: "chatgpt.com",
		},
		"ip is not a domain": {
			metadata: adapter.InboundContext{Destination: M.Socksaddr{Fqdn: "192.0.2.1"}},
		},
		"invalid destination falls back": {
			metadata: adapter.InboundContext{
				Destination: M.Socksaddr{Fqdn: "192.0.2.1"},
				Domain:      "Sniffed.Example.",
			},
			expected: "sniffed.example",
		},
		"invalid host is not a domain": {
			metadata: adapter.InboundContext{Domain: "bad host"},
		},
		"multiple trailing dots are invalid": {
			metadata: adapter.InboundContext{Domain: "example.com.."},
		},
	} {
		t.Run(name, func(t *testing.T) {
			frozen := destinationDomainFromMetadata(testCase.metadata)
			testCase.metadata.Destination.Fqdn = "changed.example"
			testCase.metadata.Domain = "changed.example"
			if frozen != testCase.expected {
				t.Fatalf("unexpected frozen domain: got %q, want %q", frozen, testCase.expected)
			}
		})
	}
}

func TestTargetAvailableFromRespectsRetention(t *testing.T) {
	now := time.Date(2026, time.July, 25, 12, 34, 56, 0, time.UTC)
	history := &History{targetsFrom: now.Add(-2 * HistoryRetention)}
	expected := now.Add(-HistoryRetention).Truncate(HistoryBucketInterval)
	if actual := history.targetAvailableFromAt(now); !actual.Equal(expected) {
		t.Fatalf("unexpected retention-clamped target availability: got %v, want %v", actual, expected)
	}
	recent := now.Add(-time.Hour)
	history.targetsFrom = recent
	if actual := history.targetAvailableFromAt(now); !actual.Equal(recent) {
		t.Fatalf("recent target availability changed: got %v, want %v", actual, recent)
	}
}

func TestHistoryPersistenceReopenAndFilters(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "traffic.db")
	history := openTestHistory(t, databasePath, "revision-a")
	revisionA := history.configRevision

	aiMetadata := resolvedMetadata(
		"tcp",
		"AI",
		[]groupSelection{
			{parent: "AI", selected: "AI-Auto", selectedType: "urltest", selectedIsGroup: true},
			{parent: "AI-Auto", selected: "ai-node", selectedType: "vmess"},
		},
	)
	history.RecordDelta(aiMetadata, 100, 40, true)
	history.RecordDelta(aiMetadata, 50, 10, false)

	proxyMetadata := resolvedMetadata(
		"udp",
		"Proxy",
		[]groupSelection{
			{parent: "Proxy", selected: "jp-node", selectedType: "trojan"},
		},
	)
	history.RecordDelta(proxyMetadata, 20, 70, true)

	if err := history.Close(); err != nil {
		t.Fatal("close initial history:", err)
	}

	reopened := openTestHistory(t, databasePath, "revision-b")
	revisionB := reopened.configRevision
	if revisionA == revisionB {
		t.Fatal("different configuration inputs produced the same revision")
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error("close reopened history:", err)
		}
	}()

	result, err := reopened.Query(context.Background(), HistoryQuery{})
	if err != nil {
		t.Fatal("query reopened history:", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected two persisted rows, got %d: %#v", len(result.Rows), result.Rows)
	}
	if result.ActualFrom.IsZero() || result.ActualTo.IsZero() || !result.ActualFrom.Before(result.ActualTo) {
		t.Fatalf("unexpected actual range: %v to %v", result.ActualFrom, result.ActualTo)
	}

	aiRow := findHistoryRow(t, result.Rows, revisionA, "AI", "ai-node")
	if !reflect.DeepEqual(aiRow.GroupPath, []string{"AI", "AI-Auto"}) {
		t.Fatalf("unexpected AI group path: %#v", aiRow.GroupPath)
	}
	if aiRow.ActualOutboundType != "vmess" || aiRow.Network != "tcp" {
		t.Fatalf("unexpected AI dimensions: %#v", aiRow)
	}
	if aiRow.UplinkBytes != 150 || aiRow.DownlinkBytes != 50 || aiRow.Connections != 1 {
		t.Fatalf("unexpected AI counters: %#v", aiRow)
	}

	proxyRow := findHistoryRow(t, result.Rows, revisionA, "Proxy", "jp-node")
	if proxyRow.UplinkBytes != 20 || proxyRow.DownlinkBytes != 70 || proxyRow.Connections != 1 {
		t.Fatalf("unexpected Proxy counters: %#v", proxyRow)
	}

	filtered, err := reopened.Query(context.Background(), HistoryQuery{
		RouteTags:          []string{"AI"},
		ActualOutboundTags: []string{"ai-node"},
		Networks:           []string{"tcp"},
	})
	if err != nil {
		t.Fatal("query filtered history:", err)
	}
	if len(filtered.Rows) != 1 || filtered.Rows[0].RouteTag != "AI" {
		t.Fatalf("unexpected filtered rows: %#v", filtered.Rows)
	}

	empty, err := reopened.Query(context.Background(), HistoryQuery{
		RouteTags: []string{"AI"},
		Networks:  []string{"udp"},
	})
	if err != nil {
		t.Fatal("query non-matching history:", err)
	}
	if len(empty.Rows) != 0 {
		t.Fatalf("expected no rows for intersected filters, got %#v", empty.Rows)
	}

	reopened.RecordDelta(aiMetadata, 7, 9, true)
	withNewRevision, err := reopened.Query(context.Background(), HistoryQuery{
		RouteTags: []string{"AI"},
	})
	if err != nil {
		t.Fatal("query pending revision:", err)
	}
	if len(withNewRevision.Rows) != 2 {
		t.Fatalf("expected revisions to remain separate, got %#v", withNewRevision.Rows)
	}
	findHistoryRow(t, withNewRevision.Rows, revisionA, "AI", "ai-node")
	revisionBRow := findHistoryRow(t, withNewRevision.Rows, revisionB, "AI", "ai-node")
	if revisionBRow.UplinkBytes != 7 || revisionBRow.DownlinkBytes != 9 || revisionBRow.Connections != 1 {
		t.Fatalf("unexpected revision-b counters: %#v", revisionBRow)
	}

	limited, err := reopened.Query(context.Background(), HistoryQuery{PageSize: 1})
	if err != nil {
		t.Fatal("query limited history:", err)
	}
	if len(limited.Rows) != 1 || limited.Rows[0].ConfigRevision != revisionA || limited.Rows[0].RouteTag != "AI" {
		t.Fatalf("unexpected top row: %#v", limited.Rows)
	}
	if limited.TotalRows != 3 || limited.Page != 1 || limited.PageSize != 1 {
		t.Fatalf("unexpected pagination metadata: %#v", limited)
	}
	if limited.Totals != (HistoryCounters{UplinkBytes: 177, DownlinkBytes: 129, Connections: 3}) {
		t.Fatalf("limited query totals were truncated: %#v", limited.Totals)
	}
}

func TestHistoryRecordsUntaggedDirectLeafWithEmptyGroupPath(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-direct")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close direct history:", err)
		}
	}()
	history.RecordDelta(&TrackerMetadata{
		Metadata: adapter.InboundContext{Network: "tcp"},
		Trace:    NewRouteTrace("", "direct", false),
	}, 1, 2, true)
	result, err := history.Query(context.Background(), HistoryQuery{})
	if err != nil {
		t.Fatal("query direct history:", err)
	}
	if len(result.Rows) != 1 {
		t.Fatalf("expected one direct row, got %#v", result.Rows)
	}
	row := result.Rows[0]
	if row.ActualOutboundTag != "" || row.ActualOutboundType != "direct" {
		t.Fatalf("unexpected direct leaf: %#v", row)
	}
	if row.GroupPath == nil || len(row.GroupPath) != 0 {
		t.Fatalf("direct group path must be an empty array: %#v", row.GroupPath)
	}
}

func TestHistoryKeepsLegacySummarySeparateFromTargetDetails(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "traffic.db")
	history := openTestHistory(t, databasePath, "revision-dual-bucket")
	targetsFrom := history.TargetAvailableFrom()
	if targetsFrom.IsZero() {
		t.Fatal("target availability was not initialized")
	}

	groupPath, err := json.Marshal([]string{"Proxy"})
	if err != nil {
		t.Fatal(err)
	}
	legacyKey := historyKey{
		Bucket:             targetsFrom.Add(-HistoryBucketInterval).Unix(),
		ConfigRevision:     history.configRevision,
		RouteTag:           "Proxy",
		GroupPath:          string(groupPath),
		ActualOutboundTag:  "proxy-node",
		ActualOutboundType: "vmess",
		Network:            "tcp",
	}
	legacyKeyContent, err := json.Marshal(struct {
		Bucket             int64
		ConfigRevision     string
		RouteTag           string
		GroupPath          string
		ActualOutboundTag  string
		ActualOutboundType string
		Network            string
	}{
		Bucket:             legacyKey.Bucket,
		ConfigRevision:     legacyKey.ConfigRevision,
		RouteTag:           legacyKey.RouteTag,
		GroupPath:          legacyKey.GroupPath,
		ActualOutboundTag:  legacyKey.ActualOutboundTag,
		ActualOutboundType: legacyKey.ActualOutboundType,
		Network:            legacyKey.Network,
	})
	if err != nil {
		t.Fatal("encode legacy key shape:", err)
	}
	currentKeyContent, err := json.Marshal(legacyKey)
	if err != nil {
		t.Fatal("encode current summary key:", err)
	}
	if !bytes.Equal(currentKeyContent, legacyKeyContent) {
		t.Fatalf("empty-domain summary key changed legacy encoding: %s != %s", currentKeyContent, legacyKeyContent)
	}
	err = history.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(historyBucket)
		if err != nil {
			return err
		}
		return putHistoryDelta(bucket, legacyKey, historyCounters{
			UplinkBytes:   100,
			DownlinkBytes: 200,
			Connections:   1,
		})
	})
	if err != nil {
		t.Fatal("insert legacy summary:", err)
	}

	legacySummary, err := history.Query(context.Background(), HistoryQuery{})
	if err != nil {
		t.Fatal("query legacy summary:", err)
	}
	if legacySummary.Totals != (HistoryCounters{UplinkBytes: 100, DownlinkBytes: 200, Connections: 1}) {
		t.Fatalf("legacy summary was not preserved: %#v", legacySummary.Totals)
	}
	legacyTargets, err := history.Query(context.Background(), HistoryQuery{
		GroupBy: HistoryGroupByDestinationDomain,
	})
	if err != nil {
		t.Fatal("query legacy target details:", err)
	}
	if legacyTargets.TotalRows != 0 || legacyTargets.Totals != (HistoryCounters{}) {
		t.Fatalf("legacy summary was exposed as unknown target traffic: %#v", legacyTargets)
	}

	partialTargetKey := legacyKey
	partialTargetKey.DestinationDomain = "partial.example"
	history.access.Lock()
	history.pending[partialTargetKey] = historyCounters{
		UplinkBytes:   10,
		DownlinkBytes: 20,
		Connections:   1,
	}
	history.access.Unlock()

	combinedSummary, err := history.Query(context.Background(), HistoryQuery{})
	if err != nil {
		t.Fatal("query combined summary:", err)
	}
	if combinedSummary.Totals != (HistoryCounters{UplinkBytes: 110, DownlinkBytes: 220, Connections: 2}) {
		t.Fatalf("summary did not combine legacy and target-era traffic: %#v", combinedSummary.Totals)
	}
	partialTargets, err := history.Query(context.Background(), HistoryQuery{
		GroupBy: HistoryGroupByDestinationDomain,
	})
	if err != nil {
		t.Fatal("query partial target details:", err)
	}
	if partialTargets.TotalRows != 0 || partialTargets.Totals != (HistoryCounters{}) {
		t.Fatalf("partial first target bucket was exposed: %#v", partialTargets)
	}
	if err = history.flush(); err != nil {
		t.Fatal("flush partial target bucket:", err)
	}

	fullTargetKey := legacyKey
	fullTargetKey.Bucket = targetsFrom.Unix()
	fullTargetKey.DestinationDomain = "new.example"
	history.access.Lock()
	history.pending[fullTargetKey] = historyCounters{
		UplinkBytes:   30,
		DownlinkBytes: 40,
		Connections:   1,
	}
	history.access.Unlock()

	newTargets, err := history.Query(context.Background(), HistoryQuery{
		GroupBy: HistoryGroupByDestinationDomain,
	})
	if err != nil {
		t.Fatal("query complete target details:", err)
	}
	if newTargets.TotalRows != 1 || newTargets.Rows[0].DestinationDomain != "new.example" ||
		newTargets.Totals != (HistoryCounters{UplinkBytes: 30, DownlinkBytes: 40, Connections: 1}) {
		t.Fatalf("unexpected complete target result: %#v", newTargets)
	}
	if err = history.flush(); err != nil {
		t.Fatal("flush complete target bucket:", err)
	}
	err = history.db.View(func(tx *bbolt.Tx) error {
		summary := tx.Bucket(historyBucket)
		targets := tx.Bucket(historyTargetsBucket)
		if summary == nil || targets == nil {
			t.Fatalf("missing dual buckets: summary=%v targets=%v", summary != nil, targets != nil)
		}
		if summary.Stats().KeyN != 2 || targets.Stats().KeyN != 1 {
			t.Fatalf("unexpected bucket cardinality: summary=%d targets=%d", summary.Stats().KeyN, targets.Stats().KeyN)
		}
		summaryCursor := summary.Cursor()
		for _, summaryContent := summaryCursor.First(); summaryContent != nil; _, summaryContent = summaryCursor.Next() {
			if bytes.Contains(summaryContent, []byte(`"destination_domain"`)) {
				t.Fatalf("summary record unexpectedly contains a target dimension: %s", summaryContent)
			}
		}
		_, targetContent := targets.Cursor().First()
		if !bytes.Contains(targetContent, []byte(`"destination_domain":"new.example"`)) {
			t.Fatalf("target record is missing its domain: %s", targetContent)
		}
		return nil
	})
	if err != nil {
		t.Fatal("inspect dual buckets:", err)
	}
	if err = history.Close(); err != nil {
		t.Fatal("close dual-bucket history:", err)
	}

	reopened := openTestHistory(t, databasePath, "revision-dual-bucket")
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error("close reopened dual-bucket history:", err)
		}
	}()
	if !reopened.TargetAvailableFrom().Equal(targetsFrom) {
		t.Fatalf("target availability changed across reopen: %v != %v", reopened.TargetAvailableFrom(), targetsFrom)
	}
	reopenedTargets, err := reopened.Query(context.Background(), HistoryQuery{
		GroupBy: HistoryGroupByDestinationDomain,
	})
	if err != nil {
		t.Fatal("query reopened targets:", err)
	}
	if reopenedTargets.TotalRows != 1 || reopenedTargets.Rows[0].DestinationDomain != "new.example" {
		t.Fatalf("target details did not survive reopen: %#v", reopenedTargets)
	}
}

func TestHistoryGroupingsFilteringSearchSortingAndPagination(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-query-v2")
	history.targetsFrom = time.Now().UTC().Truncate(HistoryBucketInterval)
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close v2 query history:", err)
		}
	}()
	record := func(
		network string,
		routeTag string,
		selections []groupSelection,
		domain string,
		uplink int64,
		downlink int64,
	) {
		metadata := resolvedMetadata(network, routeTag, selections)
		metadata.DestinationDomain = domain
		history.RecordDelta(metadata, uplink, downlink, true)
	}
	record("tcp", "AI", []groupSelection{
		{parent: "AI", selected: "AI-Auto", selectedType: "urltest", selectedIsGroup: true},
		{parent: "AI-Auto", selected: "shared-node", selectedType: "vmess"},
	}, "a.example", 100, 20)
	record("udp", "AI", []groupSelection{
		{parent: "AI", selected: "AI-Auto", selectedType: "urltest", selectedIsGroup: true},
		{parent: "AI-Auto", selected: "shared-node", selectedType: "trojan"},
	}, "b.example", 30, 10)
	record("tcp", "Proxy", []groupSelection{
		{parent: "Proxy", selected: "node-b", selectedType: "trojan"},
	}, "a.example", 5, 5)
	history.RecordDelta(&TrackerMetadata{
		Metadata: adapter.InboundContext{Network: "udp"},
		Trace:    NewRouteTrace("direct", "direct", false),
	}, 1, 1, true)
	record("tcp", "C", []groupSelection{
		{parent: "C", selected: "node-c", selectedType: "vmess"},
	}, "c.example", 4, 3)
	record("tcp", "D", []groupSelection{
		{parent: "D", selected: "node-d", selectedType: "vmess"},
	}, "d.example", 4, 3)

	routeRows, err := history.Query(context.Background(), HistoryQuery{
		GroupBy:  HistoryGroupByRoutePath,
		PageSize: 20,
	})
	if err != nil {
		t.Fatal("query route paths:", err)
	}
	if routeRows.TotalRows != 6 {
		t.Fatalf("unexpected route-path rows: %#v", routeRows.Rows)
	}
	for _, row := range routeRows.Rows {
		if row.DestinationDomain != "" {
			t.Fatalf("route_path did not fold destination domain: %#v", row)
		}
		if len(row.GroupPath) > 0 && row.OutboundGroup != row.GroupPath[len(row.GroupPath)-1] {
			t.Fatalf("route_path has inconsistent outbound group: %#v", row)
		}
	}

	domains, err := history.Query(context.Background(), HistoryQuery{
		GroupBy:  HistoryGroupByDestinationDomain,
		PageSize: 20,
	})
	if err != nil {
		t.Fatal("query domains:", err)
	}
	if domains.TotalRows != 5 {
		t.Fatalf("unexpected domain rows: %#v", domains.Rows)
	}
	aDomain := findHistoryRowByLabel(t, domains.Rows, HistoryGroupByDestinationDomain, "a.example")
	if aDomain.UplinkBytes != 105 || aDomain.DownlinkBytes != 25 || aDomain.Connections != 2 {
		t.Fatalf("unexpected a.example aggregate: %#v", aDomain)
	}

	groups, err := history.Query(context.Background(), HistoryQuery{
		GroupBy:  HistoryGroupByOutboundGroup,
		PageSize: 20,
	})
	if err != nil {
		t.Fatal("query outbound groups:", err)
	}
	aiAuto := findHistoryRowByLabel(t, groups.Rows, HistoryGroupByOutboundGroup, "AI-Auto")
	if aiAuto.UplinkBytes != 130 || aiAuto.DownlinkBytes != 30 || aiAuto.Connections != 2 {
		t.Fatalf("unexpected AI-Auto aggregate: %#v", aiAuto)
	}

	nodes, err := history.Query(context.Background(), HistoryQuery{
		GroupBy:  HistoryGroupByActualOutbound,
		PageSize: 20,
	})
	if err != nil {
		t.Fatal("query actual outbounds:", err)
	}
	shared := findHistoryRowByLabel(t, nodes.Rows, HistoryGroupByActualOutbound, "shared-node")
	if shared.UplinkBytes != 130 || shared.DownlinkBytes != 30 || shared.Connections != 2 {
		t.Fatalf("unexpected shared-node aggregate: %#v", shared)
	}
	if shared.ActualOutboundType != "" {
		t.Fatalf("mixed types for one node tag must be represented deterministically as empty: %#v", shared)
	}

	filtered, err := history.Query(context.Background(), HistoryQuery{
		GroupBy:            HistoryGroupByDestinationDomain,
		GroupTags:          []string{"AI-Auto"},
		ActualOutboundTags: []string{"shared-node"},
		DestinationDomains: []string{"A.EXAMPLE."},
		Networks:           []string{"tcp"},
	})
	if err != nil {
		t.Fatal("query exact filters:", err)
	}
	if filtered.TotalRows != 1 ||
		filtered.Totals != (HistoryCounters{UplinkBytes: 100, DownlinkBytes: 20, Connections: 1}) {
		t.Fatalf("unexpected exact-filter result: %#v", filtered)
	}

	searched, err := history.Query(context.Background(), HistoryQuery{
		GroupBy: HistoryGroupByDestinationDomain,
		Search:  "B.EXA",
	})
	if err != nil {
		t.Fatal("query search:", err)
	}
	if searched.TotalRows != 1 || searched.Rows[0].DestinationDomain != "b.example" ||
		searched.Totals != (HistoryCounters{UplinkBytes: 30, DownlinkBytes: 10, Connections: 1}) {
		t.Fatalf("search was not applied before totals: %#v", searched)
	}

	firstPage, err := history.Query(context.Background(), HistoryQuery{
		GroupBy:            HistoryGroupByDestinationDomain,
		DestinationDomains: []string{"c.example", "d.example"},
		Page:               1,
		PageSize:           1,
		SortBy:             HistorySortByTotalBytes,
		SortOrder:          HistorySortOrderDescending,
	})
	if err != nil {
		t.Fatal("query first tied page:", err)
	}
	if firstPage.TotalRows != 2 || len(firstPage.Rows) != 1 ||
		firstPage.Rows[0].DestinationDomain != "c.example" ||
		firstPage.Totals != (HistoryCounters{UplinkBytes: 8, DownlinkBytes: 6, Connections: 2}) {
		t.Fatalf("unexpected stable first page: %#v", firstPage)
	}
	secondPage, err := history.Query(context.Background(), HistoryQuery{
		GroupBy:            HistoryGroupByDestinationDomain,
		DestinationDomains: []string{"c.example", "d.example"},
		Page:               2,
		PageSize:           1,
		SortBy:             HistorySortByTotalBytes,
		SortOrder:          HistorySortOrderDescending,
	})
	if err != nil {
		t.Fatal("query second tied page:", err)
	}
	if len(secondPage.Rows) != 1 || secondPage.Rows[0].DestinationDomain != "d.example" {
		t.Fatalf("unexpected stable second page: %#v", secondPage)
	}
	beyond, err := history.Query(context.Background(), HistoryQuery{
		GroupBy:            HistoryGroupByDestinationDomain,
		DestinationDomains: []string{"c.example", "d.example"},
		Page:               3,
		PageSize:           1,
	})
	if err != nil {
		t.Fatal("query page beyond end:", err)
	}
	if len(beyond.Rows) != 0 || beyond.TotalRows != 2 ||
		beyond.Totals != (HistoryCounters{UplinkBytes: 8, DownlinkBytes: 6, Connections: 2}) {
		t.Fatalf("unexpected page beyond end: %#v", beyond)
	}

	nameSorted, err := history.Query(context.Background(), HistoryQuery{
		GroupBy:            HistoryGroupByDestinationDomain,
		DestinationDomains: []string{"a.example", "b.example"},
		SortBy:             HistorySortByName,
		SortOrder:          HistorySortOrderDescending,
	})
	if err != nil {
		t.Fatal("query name sort:", err)
	}
	if len(nameSorted.Rows) != 2 || nameSorted.Rows[0].DestinationDomain != "b.example" ||
		nameSorted.Rows[1].DestinationDomain != "a.example" {
		t.Fatalf("unexpected name sort: %#v", nameSorted.Rows)
	}
}

func TestHistorySortFields(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-sort-fields")
	history.targetsFrom = time.Now().UTC().Truncate(HistoryBucketInterval)
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close sort-fields history:", err)
		}
	}()
	x := resolvedMetadata("tcp", "X", []groupSelection{
		{parent: "X", selected: "x-node", selectedType: "vmess"},
	})
	x.DestinationDomain = "x.example"
	y := resolvedMetadata("tcp", "Y", []groupSelection{
		{parent: "Y", selected: "y-node", selectedType: "vmess"},
	})
	y.DestinationDomain = "y.example"
	history.RecordDelta(x, 100, 1, true)
	history.RecordDelta(y, 1, 200, true)
	history.RecordDelta(y, 0, 0, true)

	for _, testCase := range []struct {
		sortBy    string
		sortOrder string
		expected  string
	}{
		{HistorySortByName, HistorySortOrderAscending, "x.example"},
		{HistorySortByTotalBytes, HistorySortOrderDescending, "y.example"},
		{HistorySortByTotalBytes, HistorySortOrderAscending, "x.example"},
		{HistorySortByUplinkBytes, HistorySortOrderDescending, "x.example"},
		{HistorySortByDownlinkBytes, HistorySortOrderDescending, "y.example"},
		{HistorySortByConnections, HistorySortOrderDescending, "y.example"},
	} {
		result, err := history.Query(context.Background(), HistoryQuery{
			GroupBy:   HistoryGroupByDestinationDomain,
			SortBy:    testCase.sortBy,
			SortOrder: testCase.sortOrder,
		})
		if err != nil {
			t.Fatalf("query sort %s/%s: %v", testCase.sortBy, testCase.sortOrder, err)
		}
		if len(result.Rows) != 2 || result.Rows[0].DestinationDomain != testCase.expected {
			t.Fatalf(
				"unexpected sort %s/%s: got %#v, want first %q",
				testCase.sortBy,
				testCase.sortOrder,
				result.Rows,
				testCase.expected,
			)
		}
	}
}

func TestHistoryHTTPUsesDecimalStrings(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-http")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close HTTP history:", err)
		}
	}()

	const (
		uplink   int64 = 9_007_199_254_740_993
		downlink int64 = 9_007_199_254_740_995
	)
	metadata := resolvedMetadata(
		"tcp",
		"AI",
		[]groupSelection{
			{parent: "AI", selected: "ai-node", selectedType: "vmess"},
		},
	)
	history.RecordDelta(metadata, uplink, downlink, true)

	request := httptest.NewRequest(
		http.MethodPost,
		"/query",
		strings.NewReader(`{"route_tags":["AI"],"actual_outbound_tags":["ai-node"],"networks":["tcp"]}`),
	)
	response := httptest.NewRecorder()
	NewHistoryHTTPHandler(history).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("unexpected response status %d: %s", response.Code, response.Body.String())
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("unexpected content type: %q", contentType)
	}

	var body struct {
		Totals map[string]json.RawMessage   `json:"totals"`
		Rows   []map[string]json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal("decode query response:", err)
	}
	if len(body.Rows) != 1 {
		t.Fatalf("expected one response row, got %#v", body.Rows)
	}
	assertJSONString(t, body.Rows[0], "uplink_bytes", strconv.FormatInt(uplink, 10))
	assertJSONString(t, body.Rows[0], "downlink_bytes", strconv.FormatInt(downlink, 10))
	assertJSONString(t, body.Rows[0], "connections", "1")
	for _, field := range []string{
		"config_revision",
		"route_tag",
		"group_path",
		"destination_domain",
		"outbound_group",
		"actual_outbound_tag",
		"actual_outbound_type",
		"network",
		"uplink_bytes",
		"downlink_bytes",
		"connections",
	} {
		if _, loaded := body.Rows[0][field]; !loaded {
			t.Fatalf("response row omitted unified field %q: %#v", field, body.Rows[0])
		}
	}
	assertJSONString(t, body.Totals, "uplink_bytes", strconv.FormatInt(uplink, 10))
	assertJSONString(t, body.Totals, "downlink_bytes", strconv.FormatInt(downlink, 10))
	assertJSONString(t, body.Totals, "connections", "1")
}

func TestHistoryHTTPCapabilitiesV2(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-capabilities-v2")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close capabilities history:", err)
		}
	}()
	request := httptest.NewRequest(http.MethodGet, "/capabilities", nil)
	response := httptest.NewRecorder()
	NewHistoryHTTPHandler(history).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("unexpected response status %d: %s", response.Code, response.Body.String())
	}
	var body struct {
		APIVersion          string   `json:"api_version"`
		Groupings           []string `json:"groupings"`
		SortFields          []string `json:"sort_fields"`
		MaxPageSize         int      `json:"max_page_size"`
		TargetAvailableFrom string   `json:"target_available_from"`
		Features            struct {
			Targets    bool `json:"targets"`
			Pagination bool `json:"pagination"`
			Sorting    bool `json:"sorting"`
			Filtering  bool `json:"filtering"`
		} `json:"features"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal("decode capabilities:", err)
	}
	if body.APIVersion != "2" ||
		!reflect.DeepEqual(body.Groupings, []string{
			HistoryGroupByRoutePath,
			HistoryGroupByDestinationDomain,
			HistoryGroupByOutboundGroup,
			HistoryGroupByActualOutbound,
		}) ||
		!reflect.DeepEqual(body.SortFields, []string{
			HistorySortByName,
			HistorySortByTotalBytes,
			HistorySortByUplinkBytes,
			HistorySortByDownlinkBytes,
			HistorySortByConnections,
		}) ||
		body.MaxPageSize != HistoryPageSizeMax ||
		body.TargetAvailableFrom == "" ||
		!body.Features.Targets || !body.Features.Pagination ||
		!body.Features.Sorting || !body.Features.Filtering {
		t.Fatalf("unexpected v2 capabilities: %#v", body)
	}
}

func TestHistoryHTTPAlwaysReturnsFullTotals(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-http-totals")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close HTTP totals history:", err)
		}
	}()
	history.RecordDelta(
		resolvedMetadata("tcp", "AI", []groupSelection{
			{parent: "AI", selected: "ai-node", selectedType: "vmess"},
		}),
		100,
		200,
		true,
	)
	history.RecordDelta(
		resolvedMetadata("udp", "Proxy", []groupSelection{
			{parent: "Proxy", selected: "proxy-node", selectedType: "trojan"},
		}),
		10,
		20,
		true,
	)

	request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{"page_size":1}`))
	response := httptest.NewRecorder()
	NewHistoryHTTPHandler(history).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("unexpected response status %d: %s", response.Code, response.Body.String())
	}
	var body struct {
		Totals    map[string]json.RawMessage   `json:"totals"`
		Rows      []map[string]json.RawMessage `json:"rows"`
		TotalRows int                          `json:"total_rows"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal("decode limited query response:", err)
	}
	if len(body.Rows) != 1 || body.TotalRows != 2 {
		t.Fatalf("unexpected paged response: rows=%d total_rows=%d", len(body.Rows), body.TotalRows)
	}
	assertJSONString(t, body.Totals, "uplink_bytes", "110")
	assertJSONString(t, body.Totals, "downlink_bytes", "220")
	assertJSONString(t, body.Totals, "connections", "2")
}

func TestHistoryHTTPEmptyResultTotalsAreZero(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-http-empty")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close empty HTTP history:", err)
		}
	}()
	request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	NewHistoryHTTPHandler(history).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("unexpected response status %d: %s", response.Code, response.Body.String())
	}
	var body struct {
		ActualFrom string                     `json:"actual_from"`
		ActualTo   string                     `json:"actual_to"`
		GroupBy    string                     `json:"group_by"`
		Page       int                        `json:"page"`
		PageSize   int                        `json:"page_size"`
		TotalRows  int                        `json:"total_rows"`
		Totals     map[string]json.RawMessage `json:"totals"`
		Rows       []json.RawMessage          `json:"rows"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal("decode empty query response:", err)
	}
	if body.ActualFrom != "" || body.ActualTo != "" || len(body.Rows) != 0 ||
		body.GroupBy != HistoryGroupByRoutePath || body.Page != 1 ||
		body.PageSize != HistoryPageSizeDefault || body.TotalRows != 0 {
		t.Fatalf("unexpected empty response: %#v", body)
	}
	assertJSONString(t, body.Totals, "uplink_bytes", "0")
	assertJSONString(t, body.Totals, "downlink_bytes", "0")
	assertJSONString(t, body.Totals, "connections", "0")
}

func TestHistoryHTTPRejectsInvalidQuery(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-http-invalid")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close invalid-query history:", err)
		}
	}()
	for name, body := range map[string]string{
		"zero page":       `{"page":0}`,
		"negative page":   `{"page":-1}`,
		"zero page size":  `{"page_size":0}`,
		"large page size": `{"page_size":201}`,
		"group by":        `{"group_by":"server"}`,
		"sort by":         `{"sort_by":"rate"}`,
		"sort order":      `{"sort_order":"sideways"}`,
		"network":         `{"networks":["icmp"]}`,
		"invalid domain":  `{"destination_domains":["192.0.2.1"]}`,
		"invalid time":    `{"from":"yesterday"}`,
		"time range":      `{"from":"2026-07-25T01:00:00Z","to":"2026-07-25T00:00:00Z"}`,
		"long search":     `{"search":"` + strings.Repeat("x", historyQuerySearchSize+1) + `"}`,
		"long filter":     `{"route_tags":["` + strings.Repeat("x", historyQueryFilterValueSize+1) + `"]}`,
		"unknown field":   `{"outbound_tags":["node"]}`,
		"trailing value":  `{} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader(body))
			response := httptest.NewRecorder()
			NewHistoryHTTPHandler(history).ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("unexpected response status %d: %s", response.Code, response.Body.String())
			}
		})
	}
}

func TestHistoryConfigRevisionIsStableOpaqueAndDatabaseLocal(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "traffic.db")
	first := openTestHistory(t, databasePath, "secret-config-a")
	revisionA := first.configRevision
	if len(first.configContent) != 0 {
		t.Fatal("history retained canonical configuration content after deriving its revision")
	}
	if _, err := hex.DecodeString(revisionA); err != nil || len(revisionA) != historyRevisionSize*2 {
		t.Fatalf("configuration revision is not an opaque 128-bit hex identifier: %q", revisionA)
	}
	if err := first.Close(); err != nil {
		t.Fatal("close first history:", err)
	}

	same := openTestHistory(t, databasePath, "secret-config-a")
	if same.configRevision != revisionA {
		t.Fatalf("same database and configuration changed revision: %q != %q", same.configRevision, revisionA)
	}
	if err := same.Close(); err != nil {
		t.Fatal("close same-config history:", err)
	}

	changed := openTestHistory(t, databasePath, "secret-config-b")
	if changed.configRevision == revisionA {
		t.Fatal("changed configuration retained the same revision")
	}
	if err := changed.Close(); err != nil {
		t.Fatal("close changed-config history:", err)
	}

	otherDatabase := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "secret-config-a")
	if otherDatabase.configRevision == revisionA {
		t.Fatal("different databases reused the same keyed configuration revision")
	}
	if err := otherDatabase.Close(); err != nil {
		t.Fatal("close other database:", err)
	}
}

func TestHistoryQueryHonorsCancellation(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-cancel")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close canceled-query history:", err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := history.Query(ctx, HistoryQuery{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected canceled query error: %v", err)
	}
}

func TestHistoryQueryRejectsInvalidOptions(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-query-invalid")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close invalid query history:", err)
		}
	}()
	for name, query := range map[string]HistoryQuery{
		"time range": {
			From: time.Unix(2, 0),
			To:   time.Unix(1, 0),
		},
		"group":     {GroupBy: "invalid"},
		"page":      {Page: -1},
		"page size": {PageSize: HistoryPageSizeMax + 1},
		"sort":      {SortBy: "invalid"},
		"order":     {SortOrder: "invalid"},
		"network":   {Networks: []string{"icmp"}},
		"domain":    {DestinationDomains: []string{"192.0.2.1"}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := history.Query(context.Background(), query); err == nil {
				t.Fatal("invalid query was accepted")
			}
		})
	}
}

func TestHistoryConcurrentFlushQueryHasExactSnapshot(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-snapshot")
	defer func() {
		if err := history.Close(); err != nil {
			t.Error("close snapshot history:", err)
		}
	}()
	metadata := resolvedMetadata(
		"tcp",
		"AI",
		[]groupSelection{
			{parent: "AI", selected: "ai-node", selectedType: "vmess"},
		},
	)
	for iteration := 1; iteration <= 25; iteration++ {
		history.RecordDelta(metadata, 1, 2, iteration == 1)
		start := make(chan struct{})
		queryResult := make(chan HistoryQueryResult, 1)
		queryError := make(chan error, 1)
		flushError := make(chan error, 1)
		go func() {
			<-start
			result, err := history.Query(context.Background(), HistoryQuery{})
			queryResult <- result
			queryError <- err
		}()
		go func() {
			<-start
			flushError <- history.flush()
		}()
		close(start)
		result := <-queryResult
		if err := <-queryError; err != nil {
			t.Fatal("query concurrent snapshot:", err)
		}
		if err := <-flushError; err != nil {
			t.Fatal("flush concurrent snapshot:", err)
		}
		expected := HistoryCounters{
			UplinkBytes:   uint64(iteration),
			DownlinkBytes: uint64(iteration * 2),
			Connections:   1,
		}
		if result.Totals != expected {
			t.Fatalf("iteration %d produced duplicate or missing counters: got %#v, want %#v", iteration, result.Totals, expected)
		}
	}
}

func TestSaturatingAdd(t *testing.T) {
	if result := saturatingAdd(^uint64(0)-1, 2); result != ^uint64(0) {
		t.Fatalf("overflow wrapped to %d", result)
	}
	if result := saturatingAdd(40, 2); result != 42 {
		t.Fatalf("ordinary addition returned %d", result)
	}
}

func TestHistoryCloseRejectsLateDeltaAndClearsPending(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "traffic.db")
	history := openTestHistory(t, databasePath, "revision-close")
	revision := history.configRevision
	metadata := resolvedMetadata(
		"tcp",
		"Proxy",
		[]groupSelection{
			{parent: "Proxy", selected: "proxy-node", selectedType: "vmess"},
		},
	)
	history.RecordDelta(metadata, 10, 20, true)
	if err := history.Close(); err != nil {
		t.Fatal("close history:", err)
	}

	history.RecordDelta(metadata, 100, 200, true)
	history.access.Lock()
	accepting := history.accepting
	pendingCount := len(history.pending)
	history.access.Unlock()
	if accepting {
		t.Fatal("closed history still accepts deltas")
	}
	if pendingCount != 0 {
		t.Fatalf("closed history retained %d pending deltas", pendingCount)
	}

	reopened := openTestHistory(t, databasePath, "revision-reopen")
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Error("close reopened history:", err)
		}
	}()
	result, err := reopened.Query(context.Background(), HistoryQuery{})
	if err != nil {
		t.Fatal("query reopened history:", err)
	}
	row := findHistoryRow(t, result.Rows, revision, "Proxy", "proxy-node")
	if row.UplinkBytes != 10 || row.DownlinkBytes != 20 || row.Connections != 1 {
		t.Fatalf("late delta reached the closed history: %#v", row)
	}
}

func TestHistoryConcurrentCloseQueryAndRecord(t *testing.T) {
	history := openTestHistory(t, filepath.Join(t.TempDir(), "traffic.db"), "revision-race")
	if err := history.Start(adapter.StartStateStart); err != nil {
		t.Fatal("start history loop:", err)
	}
	metadata := resolvedMetadata(
		"udp",
		"Proxy",
		[]groupSelection{
			{parent: "Proxy", selected: "proxy-node", selectedType: "vmess"},
		},
	)
	history.RecordDelta(metadata, 1, 1, true)

	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 500 {
				history.RecordDelta(metadata, 1, 1, false)
			}
		}()
	}
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 100 {
				_, _ = history.Query(context.Background(), HistoryQuery{})
			}
		}()
	}
	closeResults := make(chan error, 4)
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			closeResults <- history.Close()
		}()
	}
	close(start)
	workers.Wait()
	close(closeResults)
	for err := range closeResults {
		if err != nil {
			t.Fatal("concurrent close history:", err)
		}
	}

	history.RecordDelta(metadata, 1, 1, false)
	history.access.Lock()
	accepting := history.accepting
	pendingCount := len(history.pending)
	history.access.Unlock()
	if accepting || pendingCount != 0 {
		t.Fatalf("unexpected closed state: accepting=%v pending=%d", accepting, pendingCount)
	}
	history.flushAccess.Lock()
	databaseOpen := history.db != nil
	history.flushAccess.Unlock()
	if databaseOpen {
		t.Fatal("history database remained open after concurrent Close")
	}
}

func openTestHistory(t *testing.T, path string, revision string) *History {
	t.Helper()
	history := NewHistory(
		context.Background(),
		log.NewNOPFactory().NewLogger("traffic-test"),
		HistoryOptions{
			Path:          path,
			ConfigContent: []byte(revision),
		},
	)
	if err := history.Start(adapter.StartStateInitialize); err != nil {
		t.Fatal("open history:", err)
	}
	return history
}

type groupSelection struct {
	parent          string
	selected        string
	selectedType    string
	selectedIsGroup bool
}

func resolvedMetadata(network string, routeTag string, selections []groupSelection) *TrackerMetadata {
	trace := NewRouteTrace(routeTag, "selector", true)
	for _, selection := range selections {
		trace.RecordSelection(selection.parent, selection.selected, selection.selectedType, selection.selectedIsGroup)
	}
	return &TrackerMetadata{
		Metadata: adapter.InboundContext{
			Network: network,
		},
		Trace: trace,
	}
}

func findHistoryRow(t *testing.T, rows []HistoryRow, revision string, routeTag string, outboundTag string) HistoryRow {
	t.Helper()
	for _, row := range rows {
		if row.ConfigRevision == revision && row.RouteTag == routeTag && row.ActualOutboundTag == outboundTag {
			return row
		}
	}
	t.Fatalf("missing history row revision=%q route=%q outbound=%q in %#v", revision, routeTag, outboundTag, rows)
	return HistoryRow{}
}

func findHistoryRowByLabel(t *testing.T, rows []HistoryRow, groupBy string, label string) HistoryRow {
	t.Helper()
	for _, row := range rows {
		if historyRowLabel(groupBy, row) == label {
			return row
		}
	}
	t.Fatalf("missing history row label=%q in %#v", label, rows)
	return HistoryRow{}
}

func assertJSONString(t *testing.T, row map[string]json.RawMessage, key string, expected string) {
	t.Helper()
	content, loaded := row[key]
	if !loaded {
		t.Fatalf("missing JSON field %q", key)
	}
	var actual string
	if err := json.Unmarshal(content, &actual); err != nil {
		t.Fatalf("field %q is not a JSON string: %s", key, content)
	}
	if actual != expected {
		t.Fatalf("unexpected %s: got %q, want %q", key, actual, expected)
	}
}

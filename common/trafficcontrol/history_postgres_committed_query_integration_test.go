//go:build with_postgres

package trafficcontrol

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
)

func TestPostgresCommittedQueryPG18ExactBoundary(t *testing.T) {
	var serverVersion int
	if err := testPostgresQueryRow(
		t,
		testPostgresDSN,
		"SELECT current_setting('server_version_num')::integer",
	).Scan(&serverVersion); err != nil {
		t.Fatal(err)
	}
	if serverVersion < 180000 || serverVersion >= 190000 {
		t.Fatalf("PostgreSQL server version = %d, want 18.x", serverVersion)
	}

	history, store, schema, instanceID := newPostgresCommittedQueryIntegrationHistory(
		t,
		nil,
	)
	addCommittedQueryPending(history, 7)
	result, err := history.Query(context.Background(), HistoryQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Totals.Connections != 7 {
		t.Fatalf("boundary query result = %#v", result)
	}

	var committed string
	testPostgresAdminQueryRow(t, fmt.Sprintf(`
SELECT COALESCE(sum(connections), 0)::text
FROM %s.mbox_traffic_minute_summary
WHERE instance_id = $1
`, pgx.Identifier{schema}.Sanitize()), instanceID).Scan(&committed)
	if committed != "7" {
		t.Fatalf("query returned before PostgreSQL commit: database=%s", committed)
	}
	status := store.spool.Status()
	if status.ResolvedSequence < 1 || status.QueueDepth != 0 {
		t.Fatalf("query boundary was not resolved: %#v", status)
	}
}

func TestPostgresCommittedQueryTimeout503(t *testing.T) {
	applyStarted := make(chan struct{}, 1)
	history, _, _, _ := newPostgresCommittedQueryIntegrationHistory(
		t,
		func(remote postgresSpoolRemote) postgresSpoolRemote {
			return &blockingCommittedQueryRemote{
				postgresSpoolRemote: remote,
				started:             applyStarted,
			}
		},
	)
	addCommittedQueryPending(history, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	request := httptest.NewRequest(
		http.MethodPost,
		"/query",
		bytes.NewBufferString(`{}`),
	).WithContext(ctx)
	response := httptest.NewRecorder()
	NewHistoryHTTPHandler(history).ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("timed-out committed query status = %d, want 503; body=%s",
			response.Code,
			response.Body.String(),
		)
	}
	select {
	case <-applyStarted:
	default:
		t.Fatal("timed-out HTTP query did not attempt boundary delivery")
	}
}

func TestPostgresCommittedQueryPaginationTotalsStable(t *testing.T) {
	history, _, schema, instanceID := newPostgresCommittedQueryIntegrationHistory(
		t,
		nil,
	)
	addCommittedQueryIntegrationBatch(history, "route-c", 3)
	addCommittedQueryIntegrationBatch(history, "route-a", 1)
	addCommittedQueryIntegrationBatch(history, "route-b", 2)

	first, err := history.Query(context.Background(), HistoryQuery{
		Page:      1,
		PageSize:  1,
		SortBy:    HistorySortByName,
		SortOrder: HistorySortOrderAscending,
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := history.Query(context.Background(), HistoryQuery{
		Page:      2,
		PageSize:  1,
		SortBy:    HistorySortByName,
		SortOrder: HistorySortOrderAscending,
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.TotalRows != 3 || second.TotalRows != 3 ||
		len(first.Rows) != 1 || len(second.Rows) != 1 ||
		first.Totals.Connections != 6 || second.Totals.Connections != 6 {
		t.Fatalf("unstable committed pagination:\nfirst=%#v\nsecond=%#v", first, second)
	}

	var rows int
	var committed string
	testPostgresAdminQueryRow(t, fmt.Sprintf(`
SELECT count(*), COALESCE(sum(connections), 0)::text
FROM %s.mbox_traffic_minute_summary
WHERE instance_id = $1
`, pgx.Identifier{schema}.Sanitize()), instanceID).Scan(&rows, &committed)
	if rows != 3 || committed != "6" {
		t.Fatalf("pagination used uncommitted overlay: rows=%d connections=%s", rows, committed)
	}
}

type blockingCommittedQueryRemote struct {
	postgresSpoolRemote
	started chan<- struct{}
}

func (r *blockingCommittedQueryRemote) Apply(
	ctx context.Context,
	_ uuid.UUID,
	_ historyBatch,
) error {
	select {
	case r.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

func newPostgresCommittedQueryIntegrationHistory(
	t *testing.T,
	decorate func(postgresSpoolRemote) postgresSpoolRemote,
) (*History, *postgresSpoolStore, string, string) {
	t.Helper()
	schema := newPostgresTestSchema(t)
	provisionPostgresTestSchema(t, schema)
	instanceID := newPostgresTestID(t)
	options := testPostgresStoreOptions(schema, instanceID, bytesOf(73))
	postgres, err := newPostgresStore(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	spool, initial, err := openHistorySpool(historySpoolOptions{
		Context:            context.Background(),
		Path:               t.TempDir() + "/traffic-spool.db",
		MaxSize:            1 << 20,
		Identity:           options.Identity,
		RoutingFingerprint: options.Revision.RoutingFingerprint,
		ConfigRevision:     options.Revision.ConfigRevision,
	})
	if err != nil {
		t.Fatal(err)
	}
	remote := newPostgresSpoolRemote(postgres)
	if decorate != nil {
		remote = decorate(remote)
	}
	store := newPostgresSpoolStore(
		spool,
		initial,
		remote,
		postgresSpoolStoreOptions{},
	)
	history := newHistory(
		context.Background(),
		log.NewNOPFactory().NewLogger("committed-query-history"),
		store,
	)
	if err = history.Start(adapter.StartStateInitialize); err != nil {
		_ = history.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if closeErr := history.Close(); closeErr != nil {
			t.Error(closeErr)
		}
	})
	return history, store, schema, instanceID
}

func addCommittedQueryIntegrationBatch(
	history *History,
	routeTag string,
	connections uint64,
) {
	history.access.Lock()
	defer history.access.Unlock()
	key := historySpoolTestKey(routeTag + ".example")
	key.RouteTag = routeTag
	current := history.pending[key]
	current.Connections = saturatingAdd(current.Connections, connections)
	history.pending[key] = current
}

//go:build with_postgres

package trafficcontrol

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/log"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
)

func TestPostgresSpoolStorePG14ExactOnceUnknownCommit(t *testing.T) {
	var serverVersion int
	if err := testPostgresQueryRow(
		t,
		testPostgresDSN,
		"SELECT current_setting('server_version_num')::integer",
	).Scan(&serverVersion); err != nil {
		t.Fatal(err)
	}
	if serverVersion < 140000 || serverVersion >= 150000 {
		t.Fatalf("PostgreSQL server version = %d, want 14.x", serverVersion)
	}
	schema := newPostgresTestSchema(t)
	provisionPostgresTestSchema(t, schema)
	instanceID := newPostgresTestID(t)
	revisionKey := bytesOf(42)
	postgresOptions := testPostgresStoreOptions(
		schema,
		instanceID,
		revisionKey,
	)
	spoolOptions := historySpoolOptions{
		Context:            context.Background(),
		Path:               t.TempDir() + "/traffic-spool.db",
		MaxSize:            1 << 20,
		Identity:           postgresOptions.Identity,
		RoutingFingerprint: postgresOptions.Revision.RoutingFingerprint,
		ConfigRevision:     postgresOptions.Revision.ConfigRevision,
	}

	firstPostgres, err := newPostgresStore(context.Background(), postgresOptions)
	if err != nil {
		t.Fatal(err)
	}
	firstSpool, firstState, err := openHistorySpool(spoolOptions)
	if err != nil {
		t.Fatal(err)
	}
	committed := make(chan uuid.UUID, 1)
	firstRemote := &unknownCommitPostgresSpoolRemote{
		postgresSpoolRemote: newPostgresSpoolRemote(firstPostgres),
		committed:           committed,
	}
	first := newPostgresSpoolStore(
		firstSpool,
		firstState,
		firstRemote,
		postgresSpoolStoreOptions{
			Wait: func(ctx context.Context, _ time.Duration) error {
				<-ctx.Done()
				return ctx.Err()
			},
		},
	)
	if _, err = first.Open(); err != nil {
		t.Fatal(err)
	}
	key := testPostgresHistoryKey(time.Now())
	key.ConfigRevision = postgresOptions.Revision.ConfigRevision
	if err = first.Write(historyBatch{
		key: {
			UplinkBytes:   17,
			DownlinkBytes: 19,
			Connections:   23,
		},
	}); err != nil {
		t.Fatal(err)
	}
	var batchID uuid.UUID
	select {
	case batchID = <-committed:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for uncertain PostgreSQL commit")
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}

	secondPostgres, err := newPostgresStore(context.Background(), postgresOptions)
	if err != nil {
		t.Fatal(err)
	}
	secondSpool, secondState, err := openHistorySpool(spoolOptions)
	if err != nil {
		t.Fatal(err)
	}
	secondRemote := &recordingPostgresSpoolRemote{
		postgresSpoolRemote: newPostgresSpoolRemote(secondPostgres),
	}
	second := newPostgresSpoolStore(
		secondSpool,
		secondState,
		secondRemote,
		postgresSpoolStoreOptions{},
	)
	if _, err = second.Open(); err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	waitForPostgresSpoolCondition(t, func() bool {
		return second.Status().QueueDepth == 0 &&
			len(secondRemote.batchIDsCopy()) == 1
	})
	if got := secondRemote.batchIDsCopy()[0]; got != batchID {
		t.Fatalf("restart changed batch ID: %s != %s", got, batchID)
	}

	schemaIdentifier := pgx.Identifier{schema}.Sanitize()
	var markers int
	var uplink string
	var downlink string
	var connections string
	testPostgresAdminQueryRow(t, fmt.Sprintf(`
SELECT count(*)
FROM %s.mbox_traffic_ingest_batches
WHERE instance_id = $1 AND batch_id = $2
`, schemaIdentifier), instanceID, batchID).Scan(&markers)
	testPostgresAdminQueryRow(t, fmt.Sprintf(`
SELECT
    sum(uplink_bytes)::text,
    sum(downlink_bytes)::text,
    sum(connections)::text
FROM %s.mbox_traffic_minute_summary
WHERE instance_id = $1
`, schemaIdentifier), instanceID).Scan(&uplink, &downlink, &connections)
	if markers != 1 ||
		uplink != "17" ||
		downlink != "19" ||
		connections != "23" {
		t.Fatalf(
			"unknown commit was not exactly once: markers=%d counters=%s/%s/%s",
			markers,
			uplink,
			downlink,
			connections,
		)
	}
}

type unknownCommitPostgresSpoolRemote struct {
	postgresSpoolRemote
	once      sync.Once
	committed chan<- uuid.UUID
}

func (r *unknownCommitPostgresSpoolRemote) Apply(
	ctx context.Context,
	batchID uuid.UUID,
	batch historyBatch,
) error {
	if err := r.postgresSpoolRemote.Apply(ctx, batchID, batch); err != nil {
		return err
	}
	unknown := false
	r.once.Do(func() {
		unknown = true
		r.committed <- batchID
	})
	if unknown {
		return fmt.Errorf("commit result was lost: %w", ErrPostgresTimeout)
	}
	return nil
}

type recordingPostgresSpoolRemote struct {
	postgresSpoolRemote
	access   sync.Mutex
	batchIDs []uuid.UUID
}

func (r *recordingPostgresSpoolRemote) Apply(
	ctx context.Context,
	batchID uuid.UUID,
	batch historyBatch,
) error {
	err := r.postgresSpoolRemote.Apply(ctx, batchID, batch)
	if err == nil || errors.Is(err, ErrPostgresTimeout) {
		r.access.Lock()
		r.batchIDs = append(r.batchIDs, batchID)
		r.access.Unlock()
	}
	return err
}

func (r *recordingPostgresSpoolRemote) batchIDsCopy() []uuid.UUID {
	r.access.Lock()
	defer r.access.Unlock()
	return append([]uuid.UUID(nil), r.batchIDs...)
}

//go:build with_postgres

package box_test

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/trafficcontrol"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/service"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgresBoxPG14StartupRegistersAndQueriesCommittedData(t *testing.T) {
	schema := newPostgresBoxPG14Schema(t, "p4d_start_")
	instanceID := newPostgresBoxPG14InstanceID(t)
	basePath := t.TempDir()
	config := postgresBoxConfig{
		DSN:           requirePostgresBoxPG14Environment(t, "MBOX_TEST_POSTGRES_SCHEMA_DSN"),
		Schema:        schema,
		StartupPolicy: option.TrafficStatisticsStartupPolicyStrict,
		InstanceID:    instanceID,
	}
	ctx, instance := startPostgresBoxPG14(t, basePath, config)
	setPostgresBoxPG14Availability(t, schema, instanceID, time.Now().Add(-time.Minute))
	if err := instance.Close(); err != nil {
		t.Fatal("close initial PostgreSQL Box:", err)
	}

	ctx, instance = startPostgresBoxPG14(t, basePath, config)
	history := requirePostgresBoxPG14History(t, ctx)
	history.RecordDelta(postgresBoxPG14Metadata("committed.example"), 23, 47, true)
	result, err := queryPostgresBoxPG14History(t, history)
	if err != nil {
		_ = instance.Close()
		t.Fatal("query committed PostgreSQL data:", err)
	}
	if result.TotalRows != 1 ||
		len(result.Rows) != 1 ||
		result.Rows[0].DestinationDomain != "committed.example" ||
		result.Totals != (trafficcontrol.HistoryCounters{
			UplinkBytes:   23,
			DownlinkBytes: 47,
			Connections:   1,
		}) {
		_ = instance.Close()
		t.Fatalf("unexpected committed PostgreSQL result: %#v", result)
	}
	if err = instance.Close(); err != nil {
		t.Fatal("close PostgreSQL Box:", err)
	}
}

func TestPostgresBoxPG14UsesConfiguredDatabaseAndSchema(t *testing.T) {
	schema := newPostgresBoxPG14Schema(t, "p4d_db_")
	instanceID := newPostgresBoxPG14InstanceID(t)
	dsn := requirePostgresBoxPG14Environment(t, "MBOX_TEST_POSTGRES_SCHEMA_DSN")
	_, instance := startPostgresBoxPG14(t, t.TempDir(), postgresBoxConfig{
		DSN:           dsn,
		Schema:        schema,
		StartupPolicy: option.TrafficStatisticsStartupPolicyStrict,
		InstanceID:    instanceID,
	})
	defer func() {
		if err := instance.Close(); err != nil {
			t.Error("close PostgreSQL Box:", err)
		}
	}()

	var database string
	var registered bool
	query := fmt.Sprintf(
		`SELECT current_database(), EXISTS (
			SELECT 1 FROM %s.mbox_traffic_instances WHERE instance_id = $1
		)`,
		pgx.Identifier{schema}.Sanitize(),
	)
	if err := queryPostgresBoxPG14Row(t, dsn, query, instanceID).Scan(
		&database,
		&registered,
	); err != nil {
		t.Fatal("verify configured database and schema:", err)
	}
	if database == "" || database == "traffic" {
		t.Fatalf("runtime used forbidden assumed database name: %q", database)
	}
	if !registered {
		t.Fatalf("instance %q was not registered in schema %q", instanceID, schema)
	}
}

func TestPostgresBoxPG14RestartDrainsQueuedBatchExactlyOnce(t *testing.T) {
	schema := newPostgresBoxPG14Schema(t, "p4d_restart_")
	instanceID := newPostgresBoxPG14InstanceID(t)
	basePath := t.TempDir()
	validConfig := postgresBoxConfig{
		DSN:           requirePostgresBoxPG14Environment(t, "MBOX_TEST_POSTGRES_SCHEMA_DSN"),
		Schema:        schema,
		StartupPolicy: option.TrafficStatisticsStartupPolicyStrict,
		InstanceID:    instanceID,
	}
	_, bootstrap := startPostgresBoxPG14(t, basePath, validConfig)
	setPostgresBoxPG14Availability(t, schema, instanceID, time.Now().Add(-time.Minute))
	if err := bootstrap.Close(); err != nil {
		t.Fatal("close PostgreSQL bootstrap Box:", err)
	}

	offlineConfig := validConfig
	offlineConfig.DSN = postgresBoxTestDSN
	offlineConfig.StartupPolicy = option.TrafficStatisticsStartupPolicyDegraded
	offlineCtx, offline := startPostgresBoxPG14(t, basePath, offlineConfig)
	requirePostgresBoxPG14History(t, offlineCtx).RecordDelta(
		postgresBoxPG14Metadata("restart.example"),
		31,
		37,
		true,
	)
	if err := offline.Close(); err != nil {
		t.Fatal("close offline PostgreSQL Box:", err)
	}

	restartCtx, restarted := startPostgresBoxPG14(t, basePath, validConfig)
	history := requirePostgresBoxPG14History(t, restartCtx)
	result, err := queryPostgresBoxPG14History(t, history)
	if err != nil {
		_ = restarted.Close()
		t.Fatal("query drained PostgreSQL data:", err)
	}
	if result.TotalRows != 1 ||
		len(result.Rows) != 1 ||
		result.Rows[0].DestinationDomain != "restart.example" ||
		result.Totals != (trafficcontrol.HistoryCounters{
			UplinkBytes:   31,
			DownlinkBytes: 37,
			Connections:   1,
		}) {
		_ = restarted.Close()
		t.Fatalf("queued batch was not drained exactly once: %#v", result)
	}
	if err = restarted.Close(); err != nil {
		t.Fatal("close restarted PostgreSQL Box:", err)
	}
}

func TestPostgresBoxPG14StartupPoliciesClassifyPermanentFailure(t *testing.T) {
	validDSN := requirePostgresBoxPG14Environment(t, "MBOX_TEST_POSTGRES_SCHEMA_DSN")
	missingURL, err := url.Parse(validDSN)
	if err != nil {
		t.Fatal("parse PostgreSQL test DSN:", err)
	}
	missingURL.Path = "/p4d_missing_" + strings.ReplaceAll(
		newPostgresBoxPG14InstanceID(t),
		"-",
		"",
	)
	missingDSN := missingURL.String()

	t.Run("strict", func(t *testing.T) {
		_, instance, constructErr := constructPostgresBox(
			t,
			t.TempDir(),
			postgresBoxConfig{
				DSN:           missingDSN,
				Schema:        "public",
				StartupPolicy: option.TrafficStatisticsStartupPolicyStrict,
				InstanceID:    newPostgresBoxPG14InstanceID(t),
			},
			nil,
		)
		if constructErr != nil {
			t.Fatal("construct strict PostgreSQL Box:", constructErr)
		}
		startErr := instance.Start()
		if !errors.Is(startErr, trafficcontrol.ErrPostgresDatabaseMissing) {
			t.Fatalf("strict permanent startup error = %v", startErr)
		}
	})

	t.Run("degraded", func(t *testing.T) {
		ctx, instance := startPostgresBoxPG14(t, t.TempDir(), postgresBoxConfig{
			DSN:           missingDSN,
			Schema:        "public",
			StartupPolicy: option.TrafficStatisticsStartupPolicyDegraded,
			InstanceID:    newPostgresBoxPG14InstanceID(t),
		})
		history := requirePostgresBoxPG14History(t, ctx)
		history.RecordDelta(postgresBoxPG14Metadata("permanent.example"), 1, 2, true)
		queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, queryErr := history.Query(queryCtx, trafficcontrol.HistoryQuery{})
		cancel()
		if !errors.Is(queryErr, trafficcontrol.ErrHistoryUnavailable) {
			_ = instance.Close()
			t.Fatalf("degraded permanent query error = %v", queryErr)
		}
		if err := instance.Close(); err != nil {
			t.Fatal("close degraded permanent PostgreSQL Box:", err)
		}
	})
}

func TestPostgresBoxPG14CloseCancelsActiveQuery(t *testing.T) {
	schema := newPostgresBoxPG14Schema(t, "p4d_cancel_")
	instanceID := newPostgresBoxPG14InstanceID(t)
	dsn := requirePostgresBoxPG14Environment(t, "MBOX_TEST_POSTGRES_SCHEMA_DSN")
	ctx, instance := startPostgresBoxPG14(t, t.TempDir(), postgresBoxConfig{
		DSN:           dsn,
		Schema:        schema,
		StartupPolicy: option.TrafficStatisticsStartupPolicyStrict,
		InstanceID:    instanceID,
	})
	history := requirePostgresBoxPG14History(t, ctx)

	lockConnection, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		_ = instance.Close()
		t.Fatal("connect PostgreSQL table locker:", err)
	}
	defer lockConnection.Close(context.Background())
	lockTransaction, err := lockConnection.Begin(context.Background())
	if err != nil {
		_ = instance.Close()
		t.Fatal("begin PostgreSQL table lock:", err)
	}
	defer lockTransaction.Rollback(context.Background())
	if _, err = lockTransaction.Exec(
		context.Background(),
		"LOCK TABLE "+pgx.Identifier{schema, "mbox_traffic_minute_summary"}.Sanitize()+
			" IN ACCESS EXCLUSIVE MODE",
	); err != nil {
		_ = instance.Close()
		t.Fatal("lock PostgreSQL records table:", err)
	}

	queryResult := make(chan error, 1)
	go func() {
		_, queryErr := history.Query(context.Background(), trafficcontrol.HistoryQuery{})
		queryResult <- queryErr
	}()
	waitForPostgresBoxPG14LockWait(t, dsn)

	closeResult := make(chan error, 1)
	go func() {
		closeResult <- instance.Close()
	}()
	select {
	case err = <-closeResult:
		if err != nil {
			t.Fatal("close PostgreSQL Box with active query:", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Box.Close did not cancel active PostgreSQL query")
	}
	select {
	case err = <-queryResult:
		if !errors.Is(err, trafficcontrol.ErrHistoryUnavailable) {
			t.Fatalf("active query close error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("active PostgreSQL query did not return after Box.Close")
	}
}

func startPostgresBoxPG14(
	t *testing.T,
	basePath string,
	config postgresBoxConfig,
) (context.Context, *box.Box) {
	t.Helper()
	ctx, instance, err := constructPostgresBox(t, basePath, config, nil)
	if err != nil {
		t.Fatal("construct PostgreSQL Box:", err)
	}
	if err = instance.Start(); err != nil {
		t.Fatal("start PostgreSQL Box:", err)
	}
	return ctx, instance
}

func requirePostgresBoxPG14History(
	t *testing.T,
	ctx context.Context,
) *trafficcontrol.History {
	t.Helper()
	history := service.PtrFromContext[trafficcontrol.History](ctx)
	if history == nil {
		t.Fatal("PostgreSQL History was not registered")
	}
	return history
}

func postgresBoxPG14Metadata(domain string) *trafficcontrol.TrackerMetadata {
	return &trafficcontrol.TrackerMetadata{
		Metadata: adapter.InboundContext{
			Network: "tcp",
		},
		Outbound:          "direct",
		OutboundType:      "direct",
		DestinationDomain: domain,
	}
}

func queryPostgresBoxPG14History(
	t *testing.T,
	history *trafficcontrol.History,
) (trafficcontrol.HistoryQueryResult, error) {
	t.Helper()
	queryCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return history.Query(queryCtx, trafficcontrol.HistoryQuery{
		GroupBy: trafficcontrol.HistoryGroupByDestinationDomain,
	})
}

func newPostgresBoxPG14Schema(t *testing.T, prefix string) string {
	t.Helper()
	schema := prefix + strings.ReplaceAll(newPostgresBoxPG14InstanceID(t), "-", "")
	t.Cleanup(func() {
		dsn := requirePostgresBoxPG14Environment(t, "MBOX_TEST_POSTGRES_ADMIN_DSN")
		connection, err := pgx.Connect(context.Background(), dsn)
		if err != nil {
			t.Errorf("connect PostgreSQL schema cleanup: %v", err)
			return
		}
		defer connection.Close(context.Background())
		if _, err = connection.Exec(
			context.Background(),
			"DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE",
		); err != nil {
			t.Errorf("drop PostgreSQL test schema %q: %v", schema, err)
		}
	})
	return schema
}

func newPostgresBoxPG14InstanceID(t *testing.T) string {
	t.Helper()
	value, err := uuid.NewV4()
	if err != nil {
		t.Fatal("generate PostgreSQL test instance ID:", err)
	}
	return value.String()
}

func setPostgresBoxPG14Availability(
	t *testing.T,
	schema string,
	instanceID string,
	availableFrom time.Time,
) {
	t.Helper()
	dsn := requirePostgresBoxPG14Environment(t, "MBOX_TEST_POSTGRES_ADMIN_DSN")
	query := fmt.Sprintf(`
UPDATE %s.mbox_traffic_instances
SET target_available_from = $2, destination_available_from = $2
WHERE instance_id = $1
`, pgx.Identifier{schema}.Sanitize())
	commandTag := execPostgresBoxPG14(t, dsn, query, instanceID, availableFrom.UTC())
	if commandTag.RowsAffected() != 1 {
		t.Fatalf("updated PostgreSQL instance rows = %d, want 1", commandTag.RowsAffected())
	}
}

func waitForPostgresBoxPG14LockWait(t *testing.T, dsn string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting int
		err := queryPostgresBoxPG14Row(
			t,
			dsn,
			`SELECT count(*) FROM pg_stat_activity
			 WHERE application_name = 'mbox-traffic-statistics'
			   AND wait_event_type = 'Lock'`,
		).Scan(&waiting)
		if err != nil {
			t.Fatal("inspect active PostgreSQL query:", err)
		}
		if waiting > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("PostgreSQL query did not reach a lock wait")
		case <-ticker.C:
		}
	}
}

func execPostgresBoxPG14(
	t *testing.T,
	dsn string,
	query string,
	arguments ...any,
) pgconn.CommandTag {
	t.Helper()
	connection, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal("connect PostgreSQL test database:", err)
	}
	defer connection.Close(context.Background())
	commandTag, err := connection.Exec(context.Background(), query, arguments...)
	if err != nil {
		t.Fatal("execute PostgreSQL test statement:", err)
	}
	return commandTag
}

func queryPostgresBoxPG14Row(
	t *testing.T,
	dsn string,
	query string,
	arguments ...any,
) pgx.Row {
	t.Helper()
	connection, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal("connect PostgreSQL test database:", err)
	}
	t.Cleanup(func() {
		if err := connection.Close(context.Background()); err != nil {
			t.Error("close PostgreSQL test connection:", err)
		}
	})
	return connection.QueryRow(context.Background(), query, arguments...)
}

func requirePostgresBoxPG14Environment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("with_postgres requires %s", name)
	}
	return value
}

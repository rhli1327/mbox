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

func TestPostgresBoxIntegrationStartupRegistersAndQueriesCommittedData(t *testing.T) {
	schema := newPostgresBoxSchema(t, "p4d_start_")
	instanceID := newPostgresBoxInstanceID(t)
	basePath := t.TempDir()
	config := postgresBoxConfig{
		DSN:           requirePostgresBoxEnvironment(t, "MBOX_TEST_POSTGRES_SCHEMA_DSN"),
		Schema:        schema,
		StartupPolicy: option.TrafficStatisticsStartupPolicyStrict,
		InstanceID:    instanceID,
	}
	ctx, instance := startPostgresBox(t, basePath, config)
	setPostgresBoxAvailability(t, schema, instanceID, time.Now().Add(-time.Minute))
	if err := instance.Close(); err != nil {
		t.Fatal("close initial PostgreSQL Box:", err)
	}

	ctx, instance = startPostgresBox(t, basePath, config)
	history := requirePostgresBoxHistory(t, ctx)
	history.RecordDelta(postgresBoxMetadata("committed.example"), 23, 47, true)
	result, err := queryPostgresBoxHistory(t, history)
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

func TestPostgresBoxIntegrationUsesConfiguredDatabaseAndSchema(t *testing.T) {
	schema := newPostgresBoxSchema(t, "p4d_db_")
	instanceID := newPostgresBoxInstanceID(t)
	dsn := requirePostgresBoxEnvironment(t, "MBOX_TEST_POSTGRES_SCHEMA_DSN")
	_, instance := startPostgresBox(t, t.TempDir(), postgresBoxConfig{
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
	if err := queryPostgresBoxRow(t, dsn, query, instanceID).Scan(
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

func TestPostgresBoxIntegrationRestartDrainsQueuedBatchExactlyOnce(t *testing.T) {
	schema := newPostgresBoxSchema(t, "p4d_restart_")
	instanceID := newPostgresBoxInstanceID(t)
	basePath := t.TempDir()
	validConfig := postgresBoxConfig{
		DSN:           requirePostgresBoxEnvironment(t, "MBOX_TEST_POSTGRES_SCHEMA_DSN"),
		Schema:        schema,
		StartupPolicy: option.TrafficStatisticsStartupPolicyStrict,
		InstanceID:    instanceID,
	}
	_, bootstrap := startPostgresBox(t, basePath, validConfig)
	setPostgresBoxAvailability(t, schema, instanceID, time.Now().Add(-time.Minute))
	if err := bootstrap.Close(); err != nil {
		t.Fatal("close PostgreSQL bootstrap Box:", err)
	}

	offlineConfig := validConfig
	offlineConfig.DSN = postgresBoxTestDSN
	offlineConfig.StartupPolicy = option.TrafficStatisticsStartupPolicyDegraded
	offlineCtx, offline := startPostgresBox(t, basePath, offlineConfig)
	requirePostgresBoxHistory(t, offlineCtx).RecordDelta(
		postgresBoxMetadata("restart.example"),
		31,
		37,
		true,
	)
	if err := offline.Close(); err != nil {
		t.Fatal("close offline PostgreSQL Box:", err)
	}

	restartCtx, restarted := startPostgresBox(t, basePath, validConfig)
	history := requirePostgresBoxHistory(t, restartCtx)
	result, err := queryPostgresBoxHistory(t, history)
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

func TestPostgresBoxIntegrationStartupPoliciesClassifyPermanentFailure(t *testing.T) {
	validDSN := requirePostgresBoxEnvironment(t, "MBOX_TEST_POSTGRES_SCHEMA_DSN")
	missingURL, err := url.Parse(validDSN)
	if err != nil {
		t.Fatal("parse PostgreSQL test DSN:", err)
	}
	missingURL.Path = "/p4d_missing_" + strings.ReplaceAll(
		newPostgresBoxInstanceID(t),
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
				InstanceID:    newPostgresBoxInstanceID(t),
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
		ctx, instance := startPostgresBox(t, t.TempDir(), postgresBoxConfig{
			DSN:           missingDSN,
			Schema:        "public",
			StartupPolicy: option.TrafficStatisticsStartupPolicyDegraded,
			InstanceID:    newPostgresBoxInstanceID(t),
		})
		history := requirePostgresBoxHistory(t, ctx)
		history.RecordDelta(postgresBoxMetadata("permanent.example"), 1, 2, true)
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

func TestPostgresBoxIntegrationCloseCancelsActiveQuery(t *testing.T) {
	schema := newPostgresBoxSchema(t, "p4d_cancel_")
	instanceID := newPostgresBoxInstanceID(t)
	dsn := requirePostgresBoxEnvironment(t, "MBOX_TEST_POSTGRES_SCHEMA_DSN")
	ctx, instance := startPostgresBox(t, t.TempDir(), postgresBoxConfig{
		DSN:           dsn,
		Schema:        schema,
		StartupPolicy: option.TrafficStatisticsStartupPolicyStrict,
		InstanceID:    instanceID,
	})
	history := requirePostgresBoxHistory(t, ctx)

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
	waitForPostgresBoxLockWait(t, dsn)

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

func startPostgresBox(
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

func requirePostgresBoxHistory(
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

func postgresBoxMetadata(domain string) *trafficcontrol.TrackerMetadata {
	return &trafficcontrol.TrackerMetadata{
		Metadata: adapter.InboundContext{
			Network: "tcp",
		},
		Outbound:          "direct",
		OutboundType:      "direct",
		DestinationDomain: domain,
	}
}

func queryPostgresBoxHistory(
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

func newPostgresBoxSchema(t *testing.T, prefix string) string {
	t.Helper()
	schema := prefix + strings.ReplaceAll(newPostgresBoxInstanceID(t), "-", "")
	t.Cleanup(func() {
		dsn := requirePostgresBoxEnvironment(t, "MBOX_TEST_POSTGRES_ADMIN_DSN")
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

func newPostgresBoxInstanceID(t *testing.T) string {
	t.Helper()
	value, err := uuid.NewV4()
	if err != nil {
		t.Fatal("generate PostgreSQL test instance ID:", err)
	}
	return value.String()
}

func setPostgresBoxAvailability(
	t *testing.T,
	schema string,
	instanceID string,
	availableFrom time.Time,
) {
	t.Helper()
	dsn := requirePostgresBoxEnvironment(t, "MBOX_TEST_POSTGRES_ADMIN_DSN")
	query := fmt.Sprintf(`
UPDATE %s.mbox_traffic_instances
SET target_available_from = $2, destination_available_from = $2
WHERE instance_id = $1
`, pgx.Identifier{schema}.Sanitize())
	commandTag := execPostgresBox(t, dsn, query, instanceID, availableFrom.UTC())
	if commandTag.RowsAffected() != 1 {
		t.Fatalf("updated PostgreSQL instance rows = %d, want 1", commandTag.RowsAffected())
	}
}

func waitForPostgresBoxLockWait(t *testing.T, dsn string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting int
		err := queryPostgresBoxRow(
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

func execPostgresBox(
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

func queryPostgresBoxRow(
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

func requirePostgresBoxEnvironment(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("with_postgres requires %s", name)
	}
	return value
}

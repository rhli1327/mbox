package trafficcontrol

import (
	"context"
	"crypto/subtle"
	"fmt"
	"sync"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const postgresApplicationName = "mbox-traffic-statistics"

var _ historyStore = (*postgresStore)(nil)

type postgresHistoryOptions struct {
	DSN                string
	Schema             string
	SchemaManagement   string
	Dialer             option.DialerOptions
	MaxOpenConnections int
	MinIdleConnections int
	ConnectTimeout     time.Duration
	StatementTimeout   time.Duration
	Identity           trafficIdentity
	Revision           postgresRevisionInput
}

type PostgresSchemaOptions struct {
	DSN                string
	Schema             string
	Dialer             option.DialerOptions
	MaxOpenConnections int
	MinIdleConnections int
	ConnectTimeout     time.Duration
	StatementTimeout   time.Duration
}

type postgresStore struct {
	ctx              context.Context
	schema           string
	schemaIdentifier string
	schemaManagement string
	identity         trafficIdentity
	revision         postgresRevisionInput
	poolFactory      *postgresPoolFactory
	pool             *pgxpool.Pool

	access           sync.RWMutex
	opened           bool
	closed           bool
	configRevision   string
	targetsFrom      time.Time
	destinationsFrom time.Time
}

func newPostgresStore(
	ctx context.Context,
	options postgresHistoryOptions,
) (*postgresStore, error) {
	if err := option.ValidateTrafficStatisticsSchema(options.Schema); err != nil {
		return nil, fmt.Errorf("%w", ErrPostgresConfiguration)
	}
	if options.SchemaManagement != option.TrafficStatisticsSchemaManagementAuto &&
		options.SchemaManagement != option.TrafficStatisticsSchemaManagementValidate {
		return nil, fmt.Errorf("%w", ErrPostgresConfiguration)
	}
	if options.Identity.Version != trafficIdentityVersion ||
		options.Identity.InstanceID == "" ||
		len(options.Identity.RevisionKey) != 32 ||
		len(options.Revision.RoutingFingerprint) != 32 ||
		len(options.Revision.ConfigRevision) != 32 {
		return nil, fmt.Errorf("%w", ErrPostgresConfiguration)
	}
	poolFactory, err := newPostgresPoolFactory(ctx, postgresConnectionOptions{
		DSN:                options.DSN,
		Dialer:             options.Dialer,
		MaxOpenConnections: options.MaxOpenConnections,
		MinIdleConnections: options.MinIdleConnections,
		ConnectTimeout:     options.ConnectTimeout,
		StatementTimeout:   options.StatementTimeout,
	})
	if err != nil {
		return nil, err
	}
	return &postgresStore{
		ctx:              ctx,
		schema:           options.Schema,
		schemaIdentifier: pgx.Identifier{options.Schema}.Sanitize(),
		schemaManagement: options.SchemaManagement,
		identity: trafficIdentity{
			Version:     options.Identity.Version,
			InstanceID:  options.Identity.InstanceID,
			RevisionKey: append([]byte(nil), options.Identity.RevisionKey...),
		},
		revision: postgresRevisionInput{
			CanonicalContent:   append([]byte(nil), options.Revision.CanonicalContent...),
			RoutingFingerprint: options.Revision.RoutingFingerprint,
			ConfigRevision:     options.Revision.ConfigRevision,
		},
		poolFactory: poolFactory,
	}, nil
}

func (s *postgresStore) Open() (historyStoreState, error) {
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed {
		return historyStoreState{}, fmt.Errorf("traffic statistics PostgreSQL store is closed")
	}
	if s.opened {
		return historyStoreState{
			configRevision:   s.configRevision,
			targetsFrom:      s.targetsFrom,
			destinationsFrom: s.destinationsFrom,
		}, nil
	}
	pool, err := s.poolFactory.Open(s.ctx)
	if err != nil {
		return historyStoreState{}, err
	}
	s.pool = pool
	opened := false
	defer func() {
		if !opened {
			pool.Close()
			s.pool = nil
		}
	}()
	if err = pool.Ping(s.ctx); err != nil {
		return historyStoreState{}, classifyPostgresError(s.ctx, "connect traffic statistics database", err)
	}
	if err := managePostgresSchema(
		s.ctx,
		s.pool,
		s.schema,
		s.schemaManagement,
	); err != nil {
		return historyStoreState{}, err
	}
	state, err := s.registerInstance(s.ctx)
	if err != nil {
		return historyStoreState{}, err
	}
	s.configRevision = state.configRevision
	s.targetsFrom = state.targetsFrom
	s.destinationsFrom = state.destinationsFrom
	s.opened = true
	opened = true
	return state, nil
}

func (s *postgresStore) registerInstance(ctx context.Context) (historyStoreState, error) {
	transaction, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return historyStoreState{}, classifyPostgresError(ctx, "begin traffic instance registration", err)
	}
	defer transaction.Rollback(context.Background())
	collectionFrom := time.Now().UTC().Truncate(HistoryBucketInterval).Add(HistoryBucketInterval)
	_, err = transaction.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s.mbox_traffic_instances (
    instance_id,
    revision_key,
    target_available_from,
    destination_available_from
) VALUES ($1, $2, $3, $3)
ON CONFLICT (instance_id) DO NOTHING
`, s.schemaIdentifier), s.identity.InstanceID, s.identity.RevisionKey, collectionFrom)
	if err != nil {
		return historyStoreState{}, classifyPostgresError(ctx, "register traffic instance", err)
	}

	var storedRevisionKey []byte
	var targetsFrom time.Time
	var destinationsFrom time.Time
	err = transaction.QueryRow(ctx, fmt.Sprintf(`
SELECT revision_key, target_available_from, destination_available_from
FROM %s.mbox_traffic_instances
WHERE instance_id = $1
FOR UPDATE
`, s.schemaIdentifier), s.identity.InstanceID).Scan(
		&storedRevisionKey,
		&targetsFrom,
		&destinationsFrom,
	)
	if err != nil {
		return historyStoreState{}, classifyPostgresError(ctx, "read traffic instance", err)
	}
	if subtle.ConstantTimeCompare(storedRevisionKey, s.identity.RevisionKey) != 1 {
		return historyStoreState{}, ErrPostgresIdentityConflict
	}

	_, err = transaction.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s.mbox_traffic_config_revisions (
    instance_id,
    routing_fingerprint,
    config_revision
) VALUES ($1, $2, $3)
ON CONFLICT (instance_id, routing_fingerprint) DO NOTHING
`, s.schemaIdentifier),
		s.identity.InstanceID,
		s.revision.RoutingFingerprint[:],
		s.revision.ConfigRevision,
	)
	if err != nil {
		return historyStoreState{}, classifyPostgresError(ctx, "register traffic configuration revision", err)
	}
	var configRevision string
	err = transaction.QueryRow(ctx, fmt.Sprintf(`
SELECT config_revision
FROM %s.mbox_traffic_config_revisions
WHERE instance_id = $1 AND routing_fingerprint = $2
`, s.schemaIdentifier),
		s.identity.InstanceID,
		s.revision.RoutingFingerprint[:],
	).Scan(&configRevision)
	if err != nil {
		return historyStoreState{}, classifyPostgresError(ctx, "read traffic configuration revision", err)
	}
	if err = transaction.Commit(ctx); err != nil {
		return historyStoreState{}, classifyPostgresError(ctx, "commit traffic instance registration", err)
	}
	return historyStoreState{
		configRevision:   configRevision,
		targetsFrom:      targetsFrom.UTC(),
		destinationsFrom: destinationsFrom.UTC(),
	}, nil
}

func (s *postgresStore) Close() error {
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	s.opened = false
	if s.pool != nil {
		s.pool.Close()
		s.pool = nil
	}
	for index := range s.identity.RevisionKey {
		s.identity.RevisionKey[index] = 0
	}
	for index := range s.revision.CanonicalContent {
		s.revision.CanonicalContent[index] = 0
	}
	return nil
}

func MigratePostgresSchema(
	ctx context.Context,
	logger log.ContextLogger,
	options PostgresSchemaOptions,
) error {
	_ = logger
	if options.Dialer.Detour != "" {
		return ErrPostgresDetourNotReady
	}
	if err := option.ValidateTrafficStatisticsSchema(options.Schema); err != nil {
		return fmt.Errorf("%w", ErrPostgresConfiguration)
	}
	pool, err := newPostgresPool(ctx, postgresConnectionOptions{
		DSN:                options.DSN,
		Dialer:             options.Dialer,
		MaxOpenConnections: options.MaxOpenConnections,
		MinIdleConnections: options.MinIdleConnections,
		ConnectTimeout:     options.ConnectTimeout,
		StatementTimeout:   options.StatementTimeout,
	})
	if err != nil {
		return err
	}
	defer pool.Close()
	if err = pool.Ping(ctx); err != nil {
		return classifyPostgresError(ctx, "connect traffic statistics schema database", err)
	}
	return managePostgresSchema(
		ctx,
		pool,
		options.Schema,
		option.TrafficStatisticsSchemaManagementAuto,
	)
}

type postgresConnectionOptions struct {
	DSN                string
	Dialer             option.DialerOptions
	MaxOpenConnections int
	MinIdleConnections int
	ConnectTimeout     time.Duration
	StatementTimeout   time.Duration
}

func newPostgresPool(
	ctx context.Context,
	options postgresConnectionOptions,
) (*pgxpool.Pool, error) {
	factory, err := newPostgresPoolFactory(ctx, options)
	if err != nil {
		return nil, err
	}
	return factory.Open(ctx)
}

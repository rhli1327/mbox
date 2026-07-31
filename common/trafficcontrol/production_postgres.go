package trafficcontrol

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
)

var (
	_ historyStore                = (*postgresProductionStore)(nil)
	_ historyStoreCommittedReader = (*postgresProductionStore)(nil)
)

type postgresInitialOpenRemote struct {
	postgresSpoolRemote
	once   sync.Once
	result chan error
}

func (r *postgresInitialOpenRemote) Open() (historyStoreState, error) {
	state, err := r.postgresSpoolRemote.Open()
	r.once.Do(func() {
		r.result <- err
	})
	return state, err
}

type postgresProductionStore struct {
	*postgresSpoolStore
	initial <-chan error
	strict  bool
	once    sync.Once
	st      historyStoreState
	err     error
}

func (s *postgresProductionStore) Open() (historyStoreState, error) {
	s.once.Do(func() {
		s.st, s.err = s.postgresSpoolStore.Open()
		if s.err != nil {
			return
		}
		initialErr := <-s.initial
		if initialErr != nil && s.strict {
			_ = s.postgresSpoolStore.Close()
			s.err = initialErr
		}
	})
	return s.st, s.err
}

func NewPostgresHistory(
	ctx context.Context,
	logger log.ContextLogger,
	options option.Options,
	traffic option.ResolvedTrafficStatisticsOptions,
) (*History, error) {
	if err := validatePostgresProductionOptions(traffic); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	identity, err := resolveTrafficIdentity(
		ctx,
		traffic.IdentityPath,
		traffic.InstanceID,
	)
	if err != nil {
		return nil, err
	}
	revision, err := buildPostgresRevisionInput(
		ctx,
		options,
		identity.RevisionKey,
	)
	if err != nil {
		return nil, err
	}
	spool, initial, err := openHistorySpool(historySpoolOptions{
		Context:            ctx,
		Path:               traffic.SpoolPath,
		MaxSize:            traffic.SpoolMaxSize,
		Identity:           identity,
		RoutingFingerprint: revision.RoutingFingerprint,
		ConfigRevision:     revision.ConfigRevision,
	})
	if err != nil {
		return nil, err
	}
	closeSpool := true
	defer func() {
		if closeSpool {
			_ = spool.Close()
		}
	}()

	remoteStore, err := newPostgresStore(ctx, postgresHistoryOptions{
		DSN:                traffic.DSN,
		Schema:             traffic.Schema,
		SchemaManagement:   traffic.SchemaManagement,
		Dialer:             traffic.Dialer,
		MaxOpenConnections: traffic.MaxOpenConnections,
		MinIdleConnections: traffic.MinIdleConnections,
		ConnectTimeout:     traffic.ConnectTimeout,
		StatementTimeout:   traffic.StatementTimeout,
		Identity:           identity,
		Revision:           revision,
	})
	if err != nil {
		return nil, err
	}
	initialResult := make(chan error, 1)
	remote := &postgresInitialOpenRemote{
		postgresSpoolRemote: newPostgresSpoolRemote(remoteStore),
		result:              initialResult,
	}
	spoolStore := newPostgresSpoolStore(
		spool,
		initial,
		remote,
		postgresSpoolStoreOptions{},
	)
	store := &postgresProductionStore{
		postgresSpoolStore: spoolStore,
		initial:            initialResult,
		strict: traffic.StartupPolicy ==
			option.TrafficStatisticsStartupPolicyStrict,
	}
	closeSpool = false
	return newHistory(ctx, logger, store), nil
}

func validatePostgresProductionOptions(
	traffic option.ResolvedTrafficStatisticsOptions,
) error {
	if !traffic.Enabled ||
		traffic.StorageType != option.TrafficStatisticsStorageTypePostgres ||
		strings.TrimSpace(traffic.DSN) == "" ||
		option.ValidateTrafficStatisticsSchema(traffic.Schema) != nil ||
		traffic.MaxOpenConnections < 1 ||
		traffic.MinIdleConnections < 0 ||
		traffic.MinIdleConnections > traffic.MaxOpenConnections ||
		traffic.ConnectTimeout <= 0 ||
		traffic.StatementTimeout <= 0 ||
		!option.ValidateTrafficStatisticsInstanceID(traffic.InstanceID) ||
		traffic.SpoolMaxSize == 0 ||
		traffic.SpoolOverflow != option.TrafficStatisticsSpoolOverflowDropOldest {
		return fmt.Errorf("%w", ErrPostgresConfiguration)
	}
	switch traffic.SchemaManagement {
	case option.TrafficStatisticsSchemaManagementAuto,
		option.TrafficStatisticsSchemaManagementValidate:
	default:
		return fmt.Errorf("%w", ErrPostgresConfiguration)
	}
	switch traffic.StartupPolicy {
	case option.TrafficStatisticsStartupPolicyDegraded,
		option.TrafficStatisticsStartupPolicyStrict:
	default:
		return fmt.Errorf("%w", ErrPostgresConfiguration)
	}
	return nil
}

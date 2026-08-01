package trafficcontrol

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strconv"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/common/dialer"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresPoolFactory struct {
	ctx     context.Context
	config  *pgxpool.Config
	options postgresConnectionOptions
}

func newPostgresPoolFactory(
	ctx context.Context,
	options postgresConnectionOptions,
) (*postgresPoolFactory, error) {
	if options.DSN == "" ||
		options.MaxOpenConnections < 1 ||
		options.MinIdleConnections < 0 ||
		options.MinIdleConnections > options.MaxOpenConnections ||
		options.ConnectTimeout <= 0 ||
		options.StatementTimeout <= 0 {
		return nil, fmt.Errorf("%w", ErrPostgresConfiguration)
	}
	config, err := pgxpool.ParseConfig(options.DSN)
	if err != nil {
		return nil, fmt.Errorf(
			"parse traffic statistics PostgreSQL connection: %w",
			ErrPostgresConfiguration,
		)
	}
	config.MaxConns = int32(options.MaxOpenConnections)
	config.MinConns = int32(options.MinIdleConnections)
	config.ConnConfig.ConnectTimeout = options.ConnectTimeout
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["statement_timeout"] = strconv.FormatInt(
		options.StatementTimeout.Milliseconds(),
		10,
	)
	config.ConnConfig.RuntimeParams["application_name"] = postgresApplicationName
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	return &postgresPoolFactory{
		ctx:     ctx,
		config:  config,
		options: options,
	}, nil
}

func (f *postgresPoolFactory) Open(ctx context.Context) (*pgxpool.Pool, error) {
	config := f.config.Copy()
	if f.options.Dialer.Detour != "" {
		outboundManager := service.FromContext[adapter.OutboundManager](f.ctx)
		if outboundManager == nil {
			return nil, ErrPostgresDetourUnavailable
		}
		selectedOutbound, loaded := outboundManager.Outbound(
			f.options.Dialer.Detour,
		)
		if !loaded || selectedOutbound == nil {
			return nil, ErrPostgresDetourUnavailable
		}
		networks := selectedOutbound.Network()
		if len(networks) > 0 && !slices.Contains(networks, N.NetworkTCP) {
			return nil, ErrPostgresDetourNoStream
		}
		outboundDialer, err := dialer.NewWithOptions(dialer.Options{
			Context:        f.ctx,
			Options:        f.options.Dialer,
			RemoteIsDomain: true,
		})
		if err != nil {
			return nil, ErrPostgresDetourUnavailable
		}
		config.ConnConfig.LookupFunc = postgresDetourLookup
		config.ConnConfig.DialFunc = (&postgresDetourDialer{
			dialer: outboundDialer,
		}).DialContext
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf(
			"create traffic statistics PostgreSQL pool: %w",
			ErrPostgresConfiguration,
		)
	}
	return pool, nil
}

func postgresDetourLookup(
	_ context.Context,
	host string,
) ([]string, error) {
	if host == "" {
		return nil, fmt.Errorf("%w", ErrPostgresConfiguration)
	}
	return []string{host}, nil
}

type postgresDetourDialer struct {
	dialer N.Dialer
}

func (d *postgresDetourDialer) DialContext(
	ctx context.Context,
	network string,
	address string,
) (net.Conn, error) {
	switch network {
	case N.NetworkTCP, "tcp4", "tcp6":
	default:
		return nil, fmt.Errorf("%w", ErrPostgresConfiguration)
	}
	host, portString, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return nil, fmt.Errorf("%w", ErrPostgresConfiguration)
	}
	port, err := strconv.Atoi(portString)
	if err != nil || port < 1 || port > 65535 {
		return nil, fmt.Errorf("%w", ErrPostgresConfiguration)
	}
	destination := M.ParseSocksaddrHostPortStr(host, portString)
	if !destination.IsValid() || destination.Port != uint16(port) {
		return nil, fmt.Errorf("%w", ErrPostgresConfiguration)
	}
	connection, err := d.dialer.DialContext(ctx, N.NetworkTCP, destination)
	if err != nil {
		return nil, err
	}
	return &postgresLogicalConn{
		Conn: connection,
		remote: postgresLogicalAddr{
			destination: destination.String(),
		},
	}, nil
}

type postgresLogicalConn struct {
	net.Conn
	remote net.Addr
}

func (c *postgresLogicalConn) RemoteAddr() net.Addr {
	return c.remote
}

type postgresLogicalAddr struct {
	destination string
}

func (a postgresLogicalAddr) Network() string {
	return N.NetworkTCP
}

func (a postgresLogicalAddr) String() string {
	return a.destination
}

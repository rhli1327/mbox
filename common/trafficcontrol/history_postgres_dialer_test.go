package trafficcontrol

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"
)

func TestPostgresDialFuncParsesDriverAddress(t *testing.T) {
	testCases := []struct {
		name        string
		network     string
		address     string
		destination string
	}{
		{
			name:        "domain",
			network:     "tcp",
			address:     "db.example:5432",
			destination: "db.example:5432",
		},
		{
			name:        "ipv4",
			network:     "tcp4",
			address:     "192.0.2.10:5432",
			destination: "192.0.2.10:5432",
		},
		{
			name:        "ipv6",
			network:     "tcp6",
			address:     "[2001:db8::10]:5432",
			destination: "[2001:db8::10]:5432",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			outbound := &postgresRecordingOutbound{
				tag:      "postgres-detour",
				networks: []string{N.NetworkTCP},
			}
			dialer := &postgresDetourDialer{dialer: outbound}
			connection, err := dialer.DialContext(
				context.Background(),
				testCase.network,
				testCase.address,
			)
			if err != nil {
				t.Fatal(err)
			}
			_ = connection.Close()
			records := outbound.snapshot()
			if len(records) != 1 {
				t.Fatalf("unexpected dial records: %#v", records)
			}
			if records[0].network != N.NetworkTCP ||
				records[0].destination.String() != testCase.destination {
				t.Fatalf("unexpected dial record: %#v", records[0])
			}
		})
	}

	invalidCases := []struct {
		name    string
		network string
		address string
	}{
		{name: "unsupported_network", network: "udp", address: "db.example:5432"},
		{name: "empty_network", network: "", address: "db.example:5432"},
		{name: "missing_port", network: "tcp", address: "db.example"},
		{name: "empty_host", network: "tcp", address: ":5432"},
		{name: "zero_port", network: "tcp", address: "db.example:0"},
		{name: "large_port", network: "tcp", address: "db.example:65536"},
		{name: "non_numeric_port", network: "tcp", address: "db.example:postgres"},
	}
	for _, testCase := range invalidCases {
		t.Run(testCase.name, func(t *testing.T) {
			outbound := &postgresRecordingOutbound{
				tag:      "postgres-detour",
				networks: []string{N.NetworkTCP},
			}
			dialer := &postgresDetourDialer{dialer: outbound}
			connection, err := dialer.DialContext(
				context.Background(),
				testCase.network,
				testCase.address,
			)
			if connection != nil {
				_ = connection.Close()
				t.Fatal("invalid driver address returned a connection")
			}
			if !errors.Is(err, ErrPostgresConfiguration) {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(outbound.snapshot()) != 0 {
				t.Fatal("invalid driver address reached the outbound")
			}
		})
	}
}

func TestPostgresDialFuncUsesLogicalTCPAndTargetRemoteAddr(t *testing.T) {
	outbound := &postgresRecordingOutbound{
		tag:      "postgres-detour",
		networks: []string{N.NetworkTCP},
	}
	dialer := &postgresDetourDialer{dialer: outbound}
	connection, err := dialer.DialContext(
		context.Background(),
		"tcp6",
		"[2001:db8::20]:5432",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()

	records := outbound.snapshot()
	if len(records) != 1 ||
		records[0].network != N.NetworkTCP ||
		records[0].destination.String() != "[2001:db8::20]:5432" {
		t.Fatalf("unexpected outbound dial: %#v", records)
	}
	if connection.RemoteAddr().Network() != N.NetworkTCP ||
		connection.RemoteAddr().String() != "[2001:db8::20]:5432" {
		t.Fatalf("unexpected logical remote address: %v", connection.RemoteAddr())
	}
}

func TestPostgresDetourLookupPreservesLogicalHost(t *testing.T) {
	for _, host := range []string{"db.example", "192.0.2.30", "2001:db8::30"} {
		addresses, err := postgresDetourLookup(context.Background(), host)
		if err != nil {
			t.Fatalf("lookup %q: %v", host, err)
		}
		if len(addresses) != 1 || addresses[0] != host {
			t.Fatalf("lookup %q returned %#v", host, addresses)
		}
	}
	if _, err := postgresDetourLookup(context.Background(), ""); !errors.Is(
		err,
		ErrPostgresConfiguration,
	) {
		t.Fatalf("unexpected empty-host error: %v", err)
	}
}

func TestPostgresDetourConstructionIsNetworkFree(t *testing.T) {
	manager := new(postgresRecordingOutboundManager)
	ctx := service.ContextWith[adapter.OutboundManager](
		context.Background(),
		manager,
	)
	store, err := newPostgresStore(
		ctx,
		log.StdLogger(),
		testPostgresDetourStoreOptions("postgres-detour"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if manager.lookupCalls.Load() != 0 {
		t.Fatalf("constructor performed %d outbound lookups", manager.lookupCalls.Load())
	}
	if store.pool != nil {
		t.Fatal("constructor created a PostgreSQL pool")
	}
}

func TestPostgresDetourRejectsMissingOutbound(t *testing.T) {
	manager := new(postgresRecordingOutboundManager)
	ctx := service.ContextWith[adapter.OutboundManager](
		context.Background(),
		manager,
	)
	store, err := newPostgresStore(
		ctx,
		log.StdLogger(),
		testPostgresDetourStoreOptions("missing"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.Open(); !errors.Is(err, ErrPostgresDetourUnavailable) {
		t.Fatalf("unexpected error: %v", err)
	}
	if manager.lookupCalls.Load() != 1 {
		t.Fatalf("unexpected outbound lookup count: %d", manager.lookupCalls.Load())
	}
}

func TestPostgresDetourRejectsUDPOnlyOutbound(t *testing.T) {
	outbound := &postgresRecordingOutbound{
		tag:      "udp-only",
		networks: []string{N.NetworkUDP},
	}
	manager := &postgresRecordingOutboundManager{
		outbounds: map[string]adapter.Outbound{
			outbound.Tag(): outbound,
		},
	}
	ctx := service.ContextWith[adapter.OutboundManager](
		context.Background(),
		manager,
	)
	store, err := newPostgresStore(
		ctx,
		log.StdLogger(),
		testPostgresDetourStoreOptions(outbound.Tag()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = store.Open(); !errors.Is(err, ErrPostgresDetourNoStream) {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(outbound.snapshot()) != 0 {
		t.Fatal("UDP-only outbound was dialed")
	}
}

func TestPostgresDetourHonorsDialCancellation(t *testing.T) {
	dialStarted := make(chan struct{})
	outbound := &postgresRecordingOutbound{
		tag:      "blocking",
		networks: []string{N.NetworkTCP},
		dial: func(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
			close(dialStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	ctx := service.ContextWith[adapter.OutboundManager](
		context.Background(),
		&postgresRecordingOutboundManager{
			outbounds: map[string]adapter.Outbound{
				outbound.Tag(): outbound,
			},
		},
	)
	factory, err := newPostgresPoolFactory(
		ctx,
		testPostgresConnectionOptionsWithDetour(outbound.Tag()),
	)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := factory.Open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	pingCtx, cancel := context.WithCancel(context.Background())
	pingResult := make(chan error, 1)
	go func() {
		pingResult <- pool.Ping(pingCtx)
	}()
	select {
	case <-dialStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for detour dial")
	}
	cancel()
	select {
	case err = <-pingResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected cancellation error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("detour dial ignored cancellation")
	}
}

func TestPostgresStoreRetriesOpenAfterDetourFailure(t *testing.T) {
	dialError := errors.New("detour unavailable during test")
	outbound := &postgresRecordingOutbound{
		tag:      "flaky",
		networks: []string{N.NetworkTCP},
		dial: func(context.Context, string, M.Socksaddr) (net.Conn, error) {
			return nil, dialError
		},
	}
	ctx := service.ContextWith[adapter.OutboundManager](
		context.Background(),
		&postgresRecordingOutboundManager{
			outbounds: map[string]adapter.Outbound{
				outbound.Tag(): outbound,
			},
		},
	)
	store, err := newPostgresStore(
		ctx,
		log.StdLogger(),
		testPostgresDetourStoreOptions(outbound.Tag()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for attempt := 0; attempt < 2; attempt++ {
		if _, err = store.Open(); err == nil {
			t.Fatal("detour failure unexpectedly opened the store")
		}
		if store.opened || store.pool != nil {
			t.Fatalf(
				"failed attempt retained lifecycle state: opened=%v pool=%v",
				store.opened,
				store.pool,
			)
		}
	}
	if records := outbound.snapshot(); len(records) != 2 {
		t.Fatalf("same store did not retry the detour: %#v", records)
	}
}

func TestPostgresSchemaAdminDetourRemainsDeferred(t *testing.T) {
	err := MigratePostgresSchema(
		context.Background(),
		log.StdLogger(),
		PostgresSchemaOptions{
			DSN:                "postgres://secret:do-not-connect@invalid.invalid/db",
			Schema:             "public",
			Dialer:             option.DialerOptions{Detour: "proxy"},
			MaxOpenConnections: 1,
			ConnectTimeout:     time.Second,
			StatementTimeout:   time.Second,
		},
	)
	if !errors.Is(err, ErrPostgresDetourNotReady) {
		t.Fatalf("unexpected admin error: %v", err)
	}
}

func testPostgresDetourStoreOptions(detour string) postgresHistoryOptions {
	var routingFingerprint [32]byte
	copy(routingFingerprint[:], []byte("0123456789abcdef0123456789abcdef"))
	return postgresHistoryOptions{
		DSN:                "postgres://user:secret@db.example:5432/traffic?sslmode=disable",
		Schema:             "public",
		SchemaManagement:   option.TrafficStatisticsSchemaManagementAuto,
		Dialer:             option.DialerOptions{Detour: detour},
		MaxOpenConnections: 1,
		MinIdleConnections: 0,
		ConnectTimeout:     50 * time.Millisecond,
		StatementTimeout:   time.Second,
		Identity: trafficIdentity{
			Version:     trafficIdentityVersion,
			InstanceID:  "test-instance",
			RevisionKey: []byte("0123456789abcdef0123456789abcdef"),
		},
		Revision: postgresRevisionInput{
			CanonicalContent:   []byte(`{"route":{}}`),
			RoutingFingerprint: routingFingerprint,
			ConfigRevision:     "00112233445566778899aabbccddeeff",
		},
	}
}

func testPostgresConnectionOptionsWithDetour(detour string) postgresConnectionOptions {
	return postgresConnectionOptions{
		DSN:                "postgres://user:secret@db.example:5432/traffic?sslmode=disable",
		Dialer:             option.DialerOptions{Detour: detour},
		MaxOpenConnections: 1,
		MinIdleConnections: 0,
		ConnectTimeout:     time.Second,
		StatementTimeout:   time.Second,
	}
}

type postgresDialRecord struct {
	network     string
	destination M.Socksaddr
}

type postgresRecordingOutbound struct {
	tag      string
	networks []string
	dial     func(context.Context, string, M.Socksaddr) (net.Conn, error)

	access  sync.Mutex
	records []postgresDialRecord
}

func (o *postgresRecordingOutbound) Type() string {
	return "test"
}

func (o *postgresRecordingOutbound) Tag() string {
	return o.tag
}

func (o *postgresRecordingOutbound) Network() []string {
	return o.networks
}

func (o *postgresRecordingOutbound) Dependencies() []string {
	return nil
}

func (o *postgresRecordingOutbound) DialContext(
	ctx context.Context,
	network string,
	destination M.Socksaddr,
) (net.Conn, error) {
	o.access.Lock()
	o.records = append(o.records, postgresDialRecord{
		network:     network,
		destination: destination,
	})
	o.access.Unlock()
	if o.dial != nil {
		return o.dial(ctx, network, destination)
	}
	client, server := net.Pipe()
	_ = server.Close()
	return client, nil
}

func (o *postgresRecordingOutbound) ListenPacket(
	context.Context,
	M.Socksaddr,
) (net.PacketConn, error) {
	return nil, errors.New("packet dialing is not supported by test outbound")
}

func (o *postgresRecordingOutbound) snapshot() []postgresDialRecord {
	o.access.Lock()
	defer o.access.Unlock()
	return append([]postgresDialRecord(nil), o.records...)
}

type postgresRecordingOutboundManager struct {
	adapter.OutboundManager
	outbounds   map[string]adapter.Outbound
	lookupCalls atomic.Int64
}

func (m *postgresRecordingOutboundManager) Outbound(
	tag string,
) (adapter.Outbound, bool) {
	m.lookupCalls.Add(1)
	outbound, loaded := m.outbounds[tag]
	return outbound, loaded
}

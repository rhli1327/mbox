//go:build with_postgres

package trafficcontrol

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"io"
	"math/big"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/quic-go"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresDetourUsesSelectedOutbound(t *testing.T) {
	factory, outbound := newPostgresTestDetourFactory(t, testPostgresDSN)
	pool, err := factory.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var value int
	if err = pool.QueryRow(context.Background(), "SELECT 1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != 1 {
		t.Fatalf("unexpected query value: %d", value)
	}
	records := outbound.snapshot()
	if len(records) == 0 {
		t.Fatal("PostgreSQL connection bypassed the selected outbound")
	}
	for _, record := range records {
		if record.network != N.NetworkTCP {
			t.Fatalf("unexpected outbound network: %q", record.network)
		}
	}
}

func TestPostgresDetourPreservesTLS(t *testing.T) {
	t.Run("verify_full", func(t *testing.T) {
		factory, outbound := newPostgresTestDetourFactory(t, testPostgresTLSDSN)
		pool, err := factory.Open(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		var value int
		if err = pool.QueryRow(context.Background(), "SELECT 1").Scan(&value); err != nil {
			t.Fatal(err)
		}
		if value != 1 || len(outbound.snapshot()) == 0 {
			t.Fatalf("unexpected TLS detour result: value=%d records=%#v", value, outbound.snapshot())
		}
	})
	t.Run("wrong_ca", func(t *testing.T) {
		factory, outbound := newPostgresTestDetourFactory(t, testPostgresTLSDSN)
		factory.config.ConnConfig.TLSConfig.RootCAs = x509.NewCertPool()
		pool, err := factory.Open(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		if err = pool.Ping(context.Background()); err == nil {
			t.Fatal("detour TLS accepted an untrusted certificate authority")
		}
		if len(outbound.snapshot()) == 0 {
			t.Fatal("wrong-CA test bypassed the selected outbound")
		}
	})
	t.Run("wrong_hostname", func(t *testing.T) {
		factory, outbound := newPostgresTestDetourFactory(t, testPostgresTLSDSN)
		factory.config.ConnConfig.TLSConfig.ServerName = "wrong-hostname.invalid"
		pool, err := factory.Open(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		if err = pool.Ping(context.Background()); err == nil {
			t.Fatal("detour TLS accepted a certificate for the wrong hostname")
		}
		if len(outbound.snapshot()) == 0 {
			t.Fatal("wrong-hostname test bypassed the selected outbound")
		}
	})
}

func TestPostgresDetourTriesMultipleHostsInOrder(t *testing.T) {
	actualTarget := postgresTestNetworkTarget(t, testPostgresDSN)
	outbound := &postgresRecordingOutbound{
		tag:      "postgres-multi-host",
		networks: []string{N.NetworkTCP},
		dial: func(ctx context.Context, _ string, destination M.Socksaddr) (net.Conn, error) {
			switch destination.AddrString() {
			case "first.invalid":
				return nil, &net.OpError{
					Op:  "dial",
					Net: N.NetworkTCP,
					Err: errors.New("deterministic first-host failure"),
				}
			case "second.invalid":
				return (&net.Dialer{}).DialContext(ctx, N.NetworkTCP, actualTarget)
			default:
				return nil, errors.New("unexpected logical host")
			}
		},
	}
	factory := newPostgresTestFactoryForOutbound(t, testPostgresDSN, outbound)
	directConfig, err := pgxpool.ParseConfig(testPostgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	factory.config.ConnConfig.Host = "first.invalid"
	factory.config.ConnConfig.Port = directConfig.ConnConfig.Port
	factory.config.ConnConfig.Fallbacks = []*pgconn.FallbackConfig{
		{
			Host: "second.invalid",
			Port: directConfig.ConnConfig.Port,
		},
	}
	pool, err := factory.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var value int
	if err = pool.QueryRow(context.Background(), "SELECT 1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	records := outbound.snapshot()
	if value != 1 || len(records) < 2 {
		t.Fatalf("unexpected fallback result: value=%d records=%#v", value, records)
	}
	if records[0].destination.AddrString() != "first.invalid" ||
		records[1].destination.AddrString() != "second.invalid" {
		t.Fatalf("unexpected fallback order: %#v", records)
	}
}

func TestPostgresDetourCancelRequestUsesLogicalTarget(t *testing.T) {
	factory, outbound := newPostgresTestDetourFactory(t, testPostgresDSN)
	pool, err := factory.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	connection, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Release()

	queryResult := make(chan error, 1)
	go func() {
		_, queryErr := connection.Exec(context.Background(), "SELECT pg_sleep(30)")
		queryResult <- queryErr
	}()
	waitForPostgresQuery(t, connection.Conn().PgConn().PID(), "pg_sleep")
	if err = connection.Conn().PgConn().CancelRequest(context.Background()); err != nil {
		t.Fatal("cancel PostgreSQL query:", err)
	}
	select {
	case err = <-queryResult:
		var postgresError *pgconn.PgError
		if !errors.As(err, &postgresError) || postgresError.Code != "57014" {
			t.Fatalf("unexpected canceled query result: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("PostgreSQL cancel request did not stop the query")
	}
	records := outbound.snapshot()
	if len(records) < 2 {
		t.Fatalf("cancel request did not redial through detour: %#v", records)
	}
	primary := records[0].destination.String()
	for _, record := range records {
		if record.network != N.NetworkTCP || record.destination.String() != primary {
			t.Fatalf("cancel request used a non-logical target: %#v", records)
		}
	}
}

func TestPostgresDetourDoesNotRecordTraffic(t *testing.T) {
	factory, outbound := newPostgresTestDetourFactory(t, testPostgresDSN)
	outboundManager := &postgresRecordingOutboundManager{
		outbounds: map[string]adapter.Outbound{
			outbound.Tag(): outbound,
		},
	}
	recorder := new(recordingDeltaRecorder)
	trafficManager := NewManager(outboundManager, recorder)
	history := &History{pending: make(historyBatch)}

	pool, err := factory.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var value int
	if err = pool.QueryRow(context.Background(), "SELECT 1").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != 1 || len(outbound.snapshot()) == 0 {
		t.Fatal("PostgreSQL query did not use the detour")
	}
	if trafficManager.ConnectionsLen() != 0 ||
		len(recorder.records) != 0 ||
		len(history.pending) != 0 {
		t.Fatalf(
			"database traffic entered accounting: connections=%d records=%#v pending=%#v",
			trafficManager.ConnectionsLen(),
			recorder.records,
			history.pending,
		)
	}
}

func TestPostgresLogicalTCPOverQUICOutbound(t *testing.T) {
	actualTarget := postgresTestNetworkTarget(t, testPostgresDSN)
	fixture := newPostgresQUICFixture(t, actualTarget)
	outbound := &postgresRecordingOutbound{
		tag:      "postgres-quic",
		networks: []string{N.NetworkTCP},
		dial:     fixture.DialContext,
	}
	factory := newPostgresTestFactoryForOutbound(t, testPostgresDSN, outbound)
	logicalTarget := net.JoinHostPort(
		"postgres-via-quic.invalid",
		strconv.Itoa(int(factory.config.ConnConfig.Port)),
	)
	factory.config.ConnConfig.Host = "postgres-via-quic.invalid"
	pool, err := factory.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var value int
	if err = pool.QueryRow(context.Background(), "SELECT 1").Scan(&value); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()
	if value != 1 {
		t.Fatalf("unexpected QUIC PostgreSQL query value: %d", value)
	}
	if fixture.connectionCount.Load() == 0 || fixture.streamCount.Load() == 0 {
		t.Fatalf(
			"QUIC fixture was not used: connections=%d streams=%d",
			fixture.connectionCount.Load(),
			fixture.streamCount.Load(),
		)
	}
	if len(outbound.snapshot()) == 0 {
		t.Fatal("QUIC test bypassed the selected outbound")
	}
	targets := fixture.snapshotTargets()
	if len(targets) == 0 || targets[0] != logicalTarget {
		t.Fatalf("QUIC stream did not carry the logical target: %#v", targets)
	}
}

func newPostgresTestDetourFactory(
	t *testing.T,
	dsn string,
) (*postgresPoolFactory, *postgresRecordingOutbound) {
	t.Helper()
	target := postgresTestNetworkTarget(t, dsn)
	outbound := &postgresRecordingOutbound{
		tag:      "postgres-detour",
		networks: []string{N.NetworkTCP},
		dial: func(ctx context.Context, _ string, _ M.Socksaddr) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, N.NetworkTCP, target)
		},
	}
	return newPostgresTestFactoryForOutbound(t, dsn, outbound), outbound
}

func newPostgresTestFactoryForOutbound(
	t *testing.T,
	dsn string,
	outbound *postgresRecordingOutbound,
) *postgresPoolFactory {
	t.Helper()
	ctx := service.ContextWith[adapter.OutboundManager](
		context.Background(),
		&postgresRecordingOutboundManager{
			outbounds: map[string]adapter.Outbound{
				outbound.Tag(): outbound,
			},
		},
	)
	options := testPostgresConnectionOptions(dsn)
	options.Dialer = option.DialerOptions{Detour: outbound.Tag()}
	factory, err := newPostgresPoolFactory(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	return factory
}

func postgresTestNetworkTarget(t *testing.T, dsn string) string {
	t.Helper()
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	return net.JoinHostPort(
		config.ConnConfig.Host,
		strconv.Itoa(int(config.ConnConfig.Port)),
	)
}

func waitForPostgresQuery(t *testing.T, processID uint32, queryFragment string) {
	t.Helper()
	connection, err := pgx.Connect(context.Background(), testPostgresAdminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close(context.Background())
	deadline := time.Now().Add(5 * time.Second)
	for {
		var active bool
		err = connection.QueryRow(context.Background(), `
SELECT EXISTS (
    SELECT 1
    FROM pg_stat_activity
    WHERE pid = $1
      AND state = 'active'
      AND query LIKE '%' || $2 || '%'
)
`, processID, queryFragment).Scan(&active)
		if err != nil {
			t.Fatal(err)
		}
		if active {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for PostgreSQL query to become active")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type postgresQUICFixture struct {
	listener        *quic.Listener
	tlsConfig       *tls.Config
	actualTarget    string
	connectionCount atomic.Int64
	streamCount     atomic.Int64

	targetAccess sync.Mutex
	targets      []string
}

func newPostgresQUICFixture(t *testing.T, actualTarget string) *postgresQUICFixture {
	t.Helper()
	certificate := newPostgresTestQUICCertificate(t)
	const protocol = "mbox-postgres-test"
	listener, err := quic.ListenAddr(
		"127.0.0.1:0",
		&tls.Config{
			Certificates: []tls.Certificate{certificate},
			NextProtos:   []string{protocol},
			MinVersion:   tls.VersionTLS13,
		},
		&quic.Config{
			MaxIdleTimeout: 5 * time.Second,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &postgresQUICFixture{
		listener:     listener,
		tlsConfig:    &tls.Config{InsecureSkipVerify: true, NextProtos: []string{protocol}, MinVersion: tls.VersionTLS13}, //nolint:gosec
		actualTarget: actualTarget,
	}
	serverContext, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
	})
	go fixture.serve(serverContext)
	return fixture
}

func (f *postgresQUICFixture) serve(ctx context.Context) {
	for {
		connection, err := f.listener.Accept(ctx)
		if err != nil {
			return
		}
		f.connectionCount.Add(1)
		go f.serveConnection(ctx, connection)
	}
}

func (f *postgresQUICFixture) serveConnection(
	ctx context.Context,
	connection *quic.Conn,
) {
	for {
		stream, err := connection.AcceptStream(ctx)
		if err != nil {
			return
		}
		f.streamCount.Add(1)
		go f.serveStream(stream)
	}
}

func (f *postgresQUICFixture) serveStream(stream *quic.Stream) {
	defer stream.Close()
	var destinationLength uint16
	if err := binary.Read(stream, binary.BigEndian, &destinationLength); err != nil {
		return
	}
	destination := make([]byte, destinationLength)
	if _, err := io.ReadFull(stream, destination); err != nil ||
		!strings.Contains(string(destination), ":") {
		return
	}
	f.targetAccess.Lock()
	f.targets = append(f.targets, string(destination))
	f.targetAccess.Unlock()
	backend, err := net.DialTimeout(N.NetworkTCP, f.actualTarget, 5*time.Second)
	if err != nil {
		return
	}
	defer backend.Close()
	copyDone := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(backend, stream)
		if tcpConnection, loaded := backend.(*net.TCPConn); loaded {
			_ = tcpConnection.CloseWrite()
		}
		copyDone <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(stream, backend)
		_ = stream.Close()
		copyDone <- struct{}{}
	}()
	<-copyDone
}

func (f *postgresQUICFixture) snapshotTargets() []string {
	f.targetAccess.Lock()
	defer f.targetAccess.Unlock()
	return append([]string(nil), f.targets...)
}

func (f *postgresQUICFixture) DialContext(
	ctx context.Context,
	network string,
	destination M.Socksaddr,
) (net.Conn, error) {
	if network != N.NetworkTCP {
		return nil, errors.New("QUIC fixture only supports logical TCP")
	}
	connection, err := quic.DialAddr(
		ctx,
		f.listener.Addr().String(),
		f.tlsConfig.Clone(),
		&quic.Config{MaxIdleTimeout: 5 * time.Second},
	)
	if err != nil {
		return nil, err
	}
	stream, err := connection.OpenStreamSync(ctx)
	if err != nil {
		_ = connection.CloseWithError(0, "")
		return nil, err
	}
	destinationString := destination.String()
	if len(destinationString) > int(^uint16(0)) {
		_ = stream.Close()
		_ = connection.CloseWithError(0, "")
		return nil, errors.New("logical destination is too long")
	}
	if err = binary.Write(
		stream,
		binary.BigEndian,
		uint16(len(destinationString)),
	); err != nil {
		_ = stream.Close()
		_ = connection.CloseWithError(0, "")
		return nil, err
	}
	if _, err = io.WriteString(stream, destinationString); err != nil {
		_ = stream.Close()
		_ = connection.CloseWithError(0, "")
		return nil, err
	}
	return &postgresQUICStreamConn{
		Stream:     stream,
		connection: connection,
	}, nil
}

type postgresQUICStreamConn struct {
	*quic.Stream
	connection *quic.Conn
}

func (c *postgresQUICStreamConn) Close() error {
	streamError := c.Stream.Close()
	connectionError := c.connection.CloseWithError(0, "")
	return errors.Join(streamError, connectionError)
}

func (c *postgresQUICStreamConn) LocalAddr() net.Addr {
	return c.connection.LocalAddr()
}

func (c *postgresQUICStreamConn) RemoteAddr() net.Addr {
	return c.connection.RemoteAddr()
}

func newPostgresTestQUICCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "mbox-postgres-test"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	certificateDER, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		&privateKey.PublicKey,
		privateKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{
		Certificate: [][]byte{certificateDER},
		PrivateKey:  privateKey,
	}
}

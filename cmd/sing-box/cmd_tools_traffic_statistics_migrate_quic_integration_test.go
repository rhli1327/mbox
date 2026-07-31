//go:build with_postgres && with_quic

package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/json/badoption"
	"github.com/sagernet/sing/service"
)

func TestTrafficStatisticsMigrateCommandPG14Hysteria2Detour(t *testing.T) {
	t.Run(
		"RealmHTTPClientDependencyAndCloseOrder",
		testTrafficStatisticsMigrationRuntimeHysteria2Realm,
	)
	t.Run("LogicalTCPOverQUIC", testTrafficStatisticsMigrateHysteria2Detour)
}

func testTrafficStatisticsMigrationRuntimeHysteria2Realm(
	t *testing.T,
) {
	runtime, err := newTrafficStatisticsMigrationRuntime(
		context.Background(),
		option.Options{
			Outbounds: []option.Outbound{
				{
					Type: C.TypeHysteria2,
					Tag:  "realm",
					Options: &option.Hysteria2OutboundOptions{
						OutboundTLSOptionsContainer: option.OutboundTLSOptionsContainer{
							TLS: &option.OutboundTLSOptions{
								Enabled:  true,
								Insecure: true,
							},
						},
						Realm: &option.Hysteria2Realm{
							ServerURL: "https://realm.invalid",
							Token:     "test-token",
							RealmID:   "test-realm",
						},
					},
				},
			},
		},
		&lockedTrafficMigrationBuffer{},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := runtime.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
	}()
	if service.FromContext[adapter.HTTPClientManager](runtime.Context()) == nil {
		t.Fatal("Hysteria2 realm runtime is missing its HTTP client manager")
	}
	var closeOrder []string
	for _, item := range runtime.closeLifecycleItems() {
		closeOrder = append(closeOrder, item.name)
	}
	expectedCloseOrder := []string{
		"certificate provider",
		"endpoint",
		"HTTP client",
		"outbound",
		"router",
		"connection",
		"DNS router",
		"DNS transport",
		"network",
		"network namespace manager",
	}
	if !equalTrafficMigrationStrings(closeOrder, expectedCloseOrder) {
		t.Fatalf(
			"unexpected Hysteria2 realm runtime close order: %v",
			closeOrder,
		)
	}
}

func testTrafficStatisticsMigrateHysteria2Detour(t *testing.T) {
	harness := newTrafficMigrationPGHarness(t, "auto")
	certificatePath, keyPath := createTrafficMigrationHysteriaCertificate(t)
	serverPort := reserveTrafficMigrationUDPPort(t)
	serverContext, cancelServer := context.WithCancel(
		include.Context(context.Background()),
	)
	server, err := box.New(box.Options{
		Context: serverContext,
		Options: option.Options{
			Log: &option.LogOptions{Level: "error"},
			Inbounds: []option.Inbound{{
				Type: C.TypeHysteria2,
				Tag:  "migration-hy2-in",
				Options: &option.Hysteria2InboundOptions{
					ListenOptions: option.ListenOptions{
						Listen: common.Ptr(
							badoption.Addr(netip.MustParseAddr("127.0.0.1")),
						),
						ListenPort: serverPort,
					},
					UpMbps:   100,
					DownMbps: 100,
					Users: []option.Hysteria2User{{
						Password: "phase5-local-password",
					}},
					InboundTLSOptionsContainer: option.InboundTLSOptionsContainer{
						TLS: &option.InboundTLSOptions{
							Enabled:         true,
							ServerName:      "phase5.local",
							CertificatePath: certificatePath,
							KeyPath:         keyPath,
						},
					},
				},
			}},
			Outbounds: []option.Outbound{{
				Type:    C.TypeDirect,
				Tag:     "direct",
				Options: &option.DirectOutboundOptions{},
			}},
		},
	})
	if err != nil {
		cancelServer()
		t.Fatal(err)
	}
	if err = server.Start(); err != nil {
		_ = server.Close()
		cancelServer()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = server.Close()
		cancelServer()
	})
	harness.writeHysteria2Config(serverPort, certificatePath)
	result := harness.run(t)
	if !result.Completed ||
		harness.tableCount("mbox_traffic_minute_summary") != 3 ||
		harness.tableCount("mbox_traffic_minute_targets") != 4 {
		t.Fatalf("Hysteria2 detour migration was not lossless: %+v", result)
	}
}

func (h *trafficMigrationPGHarness) writeHysteria2Config(
	serverPort uint16,
	certificatePath string,
) {
	h.t.Helper()
	targetDSN := strings.Replace(
		h.targetDSN,
		"localhost:",
		"127.0.0.1:",
		1,
	)
	content, err := json.Marshal(map[string]any{
		"outbounds": []any{map[string]any{
			"type":        "hysteria2",
			"tag":         "migration-hy2",
			"server":      "127.0.0.1",
			"server_port": serverPort,
			"up_mbps":     100,
			"down_mbps":   100,
			"password":    "phase5-local-password",
			"tls": map[string]any{
				"enabled":          true,
				"server_name":      "phase5.local",
				"certificate_path": certificatePath,
			},
		}},
		"experimental": map[string]any{
			"traffic_statistics": map[string]any{
				"enabled":       true,
				"instance_id":   h.instanceID,
				"identity_path": h.identity,
				"storage": map[string]any{
					"type":                 "postgres",
					"dsn":                  targetDSN,
					"schema":               h.schema,
					"schema_management":    "auto",
					"max_open_connections": 1,
					"min_idle_connections": 0,
					"connect_timeout":      "5s",
					"statement_timeout":    "10s",
					"dialer": map[string]any{
						"detour": "migration-hy2",
					},
				},
			},
		},
	})
	if err != nil {
		h.t.Fatal(err)
	}
	if err = os.WriteFile(h.configPath, content, 0o600); err != nil {
		h.t.Fatal(err)
	}
}

func reserveTrafficMigrationUDPPort(t *testing.T) uint16 {
	t.Helper()
	connection, err := net.ListenUDP(
		"udp",
		&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)},
	)
	if err != nil {
		t.Fatal(err)
	}
	port := uint16(connection.LocalAddr().(*net.UDPAddr).Port)
	if err = connection.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func createTrafficMigrationHysteriaCertificate(
	t *testing.T,
) (string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   "phase5.local",
			Organization: []string{"mbox Phase 5 local test"},
		},
		DNSNames:              []string{"phase5.local"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	certificate, err := x509.CreateCertificate(
		rand.Reader,
		template,
		template,
		key.Public(),
		key,
	)
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	certificatePath := filepath.Join(directory, "phase5.pem")
	keyPath := filepath.Join(directory, "phase5-key.pem")
	if err = os.WriteFile(
		certificatePath,
		pem.EncodeToMemory(&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: certificate,
		}),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(
		keyPath,
		pem.EncodeToMemory(&pem.Block{
			Type:  "PRIVATE KEY",
			Bytes: privateKey,
		}),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	return certificatePath, keyPath
}

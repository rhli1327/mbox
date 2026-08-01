package trafficcontrol

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing/service/filemanager"
)

func TestTrafficStatisticsIdentity(t *testing.T) {
	ctx, root := trafficIdentityTestContext(t)
	identity, err := resolveTrafficIdentity(ctx, "traffic-instance.json", "")
	if err != nil {
		t.Fatal("create identity:", err)
	}
	if identity.Version != trafficIdentityVersion {
		t.Fatalf("unexpected identity version: %d", identity.Version)
	}
	if !lowercaseUUIDPattern.MatchString(identity.InstanceID) {
		t.Fatalf("generated instance ID is not a canonical lowercase UUID: %q", identity.InstanceID)
	}
	if len(identity.RevisionKey) != 32 {
		t.Fatalf("unexpected revision key length: %d", len(identity.RevisionKey))
	}

	path := filepath.Join(root, "traffic-instance.json")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("read identity:", err)
	}
	expected := `{"version":1,"instance_id":"` + identity.InstanceID +
		`","revision_key":"` + hex.EncodeToString(identity.RevisionKey) + "\"}\n"
	if string(content) != expected {
		t.Fatalf("unexpected identity JSON:\n got: %q\nwant: %q", content, expected)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal("lstat identity:", err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("identity is not regular: %s", info.Mode())
	}
	if runtime.GOOS == "windows" {
		if info.Mode().Perm()&0o200 == 0 {
			t.Fatalf("identity is not owner-writable: %o", info.Mode().Perm())
		}
	} else if info.Mode().Perm() != 0o600 {
		t.Fatalf("identity mode = %o, want 600", info.Mode().Perm())
	}
}

func TestTrafficIdentityConcurrentCreate(t *testing.T) {
	ctx, _ := trafficIdentityTestContext(t)
	const count = 32
	identities := make([]trafficIdentity, count)
	errorsList := make([]error, count)
	var waitGroup sync.WaitGroup
	waitGroup.Add(count)
	for index := range count {
		go func() {
			defer waitGroup.Done()
			identities[index], errorsList[index] = resolveTrafficIdentity(
				ctx,
				"traffic-instance.json",
				"",
			)
		}()
	}
	waitGroup.Wait()
	for index, err := range errorsList {
		if err != nil {
			t.Fatalf("creator %d: %v", index, err)
		}
		if identities[index].InstanceID != identities[0].InstanceID ||
			!equalBytes(identities[index].RevisionKey, identities[0].RevisionKey) {
			t.Fatalf("creator %d observed a different identity", index)
		}
	}
}

func TestTrafficIdentityRestartStable(t *testing.T) {
	ctx, _ := trafficIdentityTestContext(t)
	first, err := resolveTrafficIdentity(ctx, "identity.json", "")
	if err != nil {
		t.Fatal("create identity:", err)
	}
	second, err := resolveTrafficIdentity(ctx, "identity.json", "")
	if err != nil {
		t.Fatal("reopen identity:", err)
	}
	if first.InstanceID != second.InstanceID || !equalBytes(first.RevisionKey, second.RevisionKey) {
		t.Fatalf("identity changed across restart: first=%#v second=%#v", first, second)
	}
}

func TestTrafficIdentityExplicitConflict(t *testing.T) {
	ctx, root := trafficIdentityTestContext(t)
	identity, err := resolveTrafficIdentity(ctx, "identity.json", "edge-a")
	if err != nil {
		t.Fatal("create explicit identity:", err)
	}
	if identity.InstanceID != "edge-a" {
		t.Fatalf("unexpected explicit ID: %q", identity.InstanceID)
	}
	before, err := os.ReadFile(filepath.Join(root, "identity.json"))
	if err != nil {
		t.Fatal("read identity before conflict:", err)
	}
	_, err = resolveTrafficIdentity(ctx, "identity.json", "edge-b")
	if !errors.Is(err, ErrPostgresIdentityConflict) {
		t.Fatalf("unexpected conflict error: %v", err)
	}
	after, err := os.ReadFile(filepath.Join(root, "identity.json"))
	if err != nil {
		t.Fatal("read identity after conflict:", err)
	}
	if string(before) != string(after) {
		t.Fatal("explicit conflict overwrote the identity file")
	}
}

func TestBoltDoesNotCreateIdentity(t *testing.T) {
	ctx, root := trafficIdentityTestContext(t)
	history := NewHistory(ctx, nil, HistoryOptions{
		Path:          "traffic.db",
		ConfigContent: []byte("bolt-does-not-create-identity"),
	})
	if err := history.Start(adapter.StartStateInitialize); err != nil {
		t.Fatal("initialize Bolt history:", err)
	}
	if err := history.Close(); err != nil {
		t.Fatal("close Bolt history:", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "traffic-instance.json")); !os.IsNotExist(err) {
		t.Fatalf("Bolt touched PostgreSQL identity: %v", err)
	}
}

func TestTrafficIdentityRejectsUnsafeFiles(t *testing.T) {
	t.Run("symlink", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("symlink creation is not generally available to unprivileged Windows tests")
		}
		ctx, root := trafficIdentityTestContext(t)
		target := filepath.Join(root, "target")
		if err := os.WriteFile(target, []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(root, "identity.json")); err != nil {
			t.Fatal(err)
		}
		if _, err := resolveTrafficIdentity(ctx, "identity.json", ""); err == nil {
			t.Fatal("accepted symlink identity")
		}
	})

	for _, testCase := range []struct {
		name    string
		content string
		mode    os.FileMode
	}{
		{"corrupt JSON", "{", 0o600},
		{"future version", `{"version":2,"instance_id":"edge","revision_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`, 0o600},
		{"short revision key", `{"version":1,"instance_id":"edge","revision_key":"aa"}`, 0o600},
		{"loose permissions", `{"version":1,"instance_id":"edge","revision_key":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`, 0o644},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.name == "loose permissions" && runtime.GOOS == "windows" {
				t.Skip("Windows mode bits do not express Unix 0600")
			}
			ctx, root := trafficIdentityTestContext(t)
			if err := os.WriteFile(
				filepath.Join(root, "identity.json"),
				[]byte(testCase.content),
				testCase.mode,
			); err != nil {
				t.Fatal(err)
			}
			if _, err := resolveTrafficIdentity(ctx, "identity.json", ""); err == nil {
				t.Fatal("accepted unsafe identity")
			}
		})
	}
}

func trafficIdentityTestContext(t *testing.T) (context.Context, string) {
	t.Helper()
	root := t.TempDir()
	return filemanager.WithDefault(
		context.Background(),
		root,
		"",
		os.Getuid(),
		os.Getgid(),
	), root
}

func equalBytes(left []byte, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var different byte
	for index := range left {
		different |= left[index] ^ right[index]
	}
	return different == 0
}

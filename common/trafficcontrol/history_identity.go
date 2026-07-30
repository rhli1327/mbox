package trafficcontrol

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/gofrs/uuid/v5"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/service/filemanager"
)

const trafficIdentityVersion = 1

var lowercaseUUIDPattern = regexp.MustCompile(
	`^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`,
)

type trafficIdentity struct {
	Version     int
	InstanceID  string
	RevisionKey []byte
}

type trafficIdentityFile struct {
	Version     int    `json:"version"`
	InstanceID  string `json:"instance_id"`
	RevisionKey string `json:"revision_key"`
}

func resolveTrafficIdentity(
	ctx context.Context,
	path string,
	configuredInstanceID string,
) (trafficIdentity, error) {
	if !option.ValidateTrafficStatisticsInstanceID(configuredInstanceID) {
		return trafficIdentity{}, fmt.Errorf("invalid traffic statistics instance ID")
	}
	resolvedPath := filemanager.BasePath(ctx, os.ExpandEnv(path))
	parent := filepath.Dir(resolvedPath)
	if parent != "." {
		if err := filemanager.MkdirAll(ctx, parent, 0o755); err != nil {
			return trafficIdentity{}, fmt.Errorf("create traffic identity directory: %w", err)
		}
	}
	identity, err := readTrafficIdentity(resolvedPath)
	if err == nil {
		return checkConfiguredTrafficIdentity(identity, configuredInstanceID)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return trafficIdentity{}, err
	}

	instanceID := configuredInstanceID
	if instanceID == "" {
		generatedID, generateErr := uuid.NewV4()
		if generateErr != nil {
			return trafficIdentity{}, fmt.Errorf("generate traffic instance ID: %w", generateErr)
		}
		instanceID = generatedID.String()
	}
	revisionKey := make([]byte, 32)
	if _, err = rand.Read(revisionKey); err != nil {
		return trafficIdentity{}, fmt.Errorf("generate traffic revision key: %w", err)
	}
	candidate := trafficIdentity{
		Version:     trafficIdentityVersion,
		InstanceID:  instanceID,
		RevisionKey: revisionKey,
	}
	won, err := writeTrafficIdentityCandidate(ctx, resolvedPath, candidate)
	if err != nil {
		return trafficIdentity{}, err
	}
	if won {
		return candidate, nil
	}
	winner, err := readTrafficIdentity(resolvedPath)
	if err != nil {
		return trafficIdentity{}, fmt.Errorf("read concurrently created traffic identity: %w", err)
	}
	return checkConfiguredTrafficIdentity(winner, configuredInstanceID)
}

func readTrafficIdentity(path string) (trafficIdentity, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return trafficIdentity{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return trafficIdentity{}, fmt.Errorf("traffic identity must be a regular non-symlink file")
	}
	if err = validateTrafficIdentityPermissions(info.Mode()); err != nil {
		return trafficIdentity{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return trafficIdentity{}, fmt.Errorf("open traffic identity: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var content trafficIdentityFile
	if err = decoder.Decode(&content); err != nil {
		return trafficIdentity{}, fmt.Errorf("decode traffic identity: %w", err)
	}
	if err = ensureTrafficIdentityEOF(decoder); err != nil {
		return trafficIdentity{}, err
	}
	if content.Version != trafficIdentityVersion {
		return trafficIdentity{}, fmt.Errorf(
			"unsupported traffic identity version: %d",
			content.Version,
		)
	}
	if !option.ValidateTrafficStatisticsInstanceID(content.InstanceID) ||
		content.InstanceID == "" {
		return trafficIdentity{}, fmt.Errorf("invalid traffic identity instance ID")
	}
	if len(content.RevisionKey) != 64 ||
		content.RevisionKey != bytesToLowerHex(content.RevisionKey) {
		return trafficIdentity{}, fmt.Errorf("invalid traffic identity revision key")
	}
	revisionKey, err := hex.DecodeString(content.RevisionKey)
	if err != nil || len(revisionKey) != 32 {
		return trafficIdentity{}, fmt.Errorf("invalid traffic identity revision key")
	}
	return trafficIdentity{
		Version:     content.Version,
		InstanceID:  content.InstanceID,
		RevisionKey: revisionKey,
	}, nil
}

func writeTrafficIdentityCandidate(
	ctx context.Context,
	path string,
	identity trafficIdentity,
) (bool, error) {
	content, err := json.Marshal(trafficIdentityFile{
		Version:     identity.Version,
		InstanceID:  identity.InstanceID,
		RevisionKey: hex.EncodeToString(identity.RevisionKey),
	})
	if err != nil {
		return false, fmt.Errorf("encode traffic identity: %w", err)
	}
	content = append(content, '\n')
	parent := filepath.Dir(path)
	temporary, err := os.CreateTemp(parent, ".traffic-instance-*.tmp")
	if err != nil {
		return false, fmt.Errorf("create temporary traffic identity: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		if temporaryPath != "" {
			_ = os.Remove(temporaryPath)
		}
	}()
	writeErr := writeAndSyncTrafficIdentity(temporary, content)
	if writeErr != nil {
		return false, writeErr
	}
	if err = filemanager.Chown(ctx, temporaryPath); err != nil {
		return false, fmt.Errorf("set traffic identity ownership: %w", err)
	}
	won, err := publishTrafficIdentity(temporaryPath, path)
	if err != nil {
		return false, err
	}
	err = os.Remove(temporaryPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("remove temporary traffic identity: %w", err)
	}
	temporaryPath = ""
	if won {
		if err = syncTrafficIdentityParent(parent); err != nil {
			return false, err
		}
	}
	return won, nil
}

func writeAndSyncTrafficIdentity(file *os.File, content []byte) error {
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return fmt.Errorf("set temporary traffic identity permissions: %w", err)
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return fmt.Errorf("write temporary traffic identity: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync temporary traffic identity: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temporary traffic identity: %w", err)
	}
	return nil
}

func ensureTrafficIdentityEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("traffic identity contains multiple JSON values")
	}
	return fmt.Errorf("decode trailing traffic identity content: %w", err)
}

func checkConfiguredTrafficIdentity(
	identity trafficIdentity,
	configuredInstanceID string,
) (trafficIdentity, error) {
	if configuredInstanceID != "" && configuredInstanceID != identity.InstanceID {
		return trafficIdentity{}, ErrPostgresIdentityConflict
	}
	return identity, nil
}

func bytesToLowerHex(value string) string {
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(decoded)
}

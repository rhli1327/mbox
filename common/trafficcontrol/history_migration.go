package trafficcontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/filemanager"
)

var (
	ErrTrafficMigrationSourceInUse = errors.New(
		"traffic statistics migration source is in use",
	)
	ErrTrafficMigrationSourceChanged = errors.New(
		"traffic statistics migration source changed during migration",
	)
	ErrTrafficMigrationSourceInvalid = errors.New(
		"traffic statistics migration source is invalid",
	)
	ErrTrafficMigrationIdentityConflict = errors.New(
		"traffic statistics migration identity conflicts with existing state",
	)
	ErrTrafficMigrationResumeMismatch = errors.New(
		"traffic statistics migration resume selection does not match the source",
	)
	ErrTrafficMigrationConflict = errors.New(
		"traffic statistics migration record conflicts with PostgreSQL",
	)
	ErrTrafficMigrationTargetInUse = errors.New(
		"traffic statistics migration target is already in use",
	)
)

var (
	trafficMigrationIDPattern       = regexp.MustCompile(`^[0-9a-f]{64}$`)
	trafficMigrationRevisionPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

type TrafficStatisticsMigrationOptions struct {
	SourcePath           string
	Configuration        option.Options
	Traffic              option.ResolvedTrafficStatisticsOptions
	InstanceID           string
	BatchSize            int
	From                 time.Time
	To                   time.Time
	ResumeMigrationID    string
	ActiveConfigRevision string
	DryRun               bool
	Progress             func(TrafficStatisticsMigrationProgress) error
}

type TrafficStatisticsMigrationProgress struct {
	Phase       string `json:"phase"`
	MigrationID string `json:"migration_id,omitempty"`
	Kind        string `json:"kind,omitempty"`
	Cursor      string `json:"cursor,omitempty"`
	Scanned     uint64 `json:"scanned"`
	Inserted    uint64 `json:"inserted"`
	Skipped     uint64 `json:"skipped"`
}

type TrafficStatisticsMigrationCounts struct {
	Scanned  uint64 `json:"scanned"`
	Inserted uint64 `json:"inserted"`
	Skipped  uint64 `json:"skipped"`
}

type TrafficStatisticsMigrationResult struct {
	MigrationID       string                           `json:"migration_id"`
	SourceFingerprint string                           `json:"source_fingerprint"`
	InstanceID        string                           `json:"instance_id"`
	DryRun            bool                             `json:"dry_run"`
	Completed         bool                             `json:"completed"`
	Summary           TrafficStatisticsMigrationCounts `json:"summary"`
	Targets           TrafficStatisticsMigrationCounts `json:"targets"`
}

type trafficMigrationIdentityState struct {
	path     string
	identity trafficIdentity
	missing  bool
}

func MigrateBoltTrafficStatisticsToPostgres(
	ctx context.Context,
	logger log.ContextLogger,
	options TrafficStatisticsMigrationOptions,
) (result TrafficStatisticsMigrationResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err = validateTrafficMigrationOptions(options); err != nil {
		return result, err
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	sourcePath := options.SourcePath
	if sourcePath == "" {
		sourcePath = "traffic.db"
	}
	sourcePath = filemanager.BasePath(ctx, os.ExpandEnv(sourcePath))
	source, err := openTrafficMigrationSource(ctx, sourcePath)
	if err != nil {
		return result, err
	}
	defer func() {
		closeErr := source.closeAndVerify()
		if err == nil && closeErr != nil {
			err = closeErr
			result.Completed = false
		}
	}()
	if err = emitTrafficMigrationProgress(options.Progress, TrafficStatisticsMigrationProgress{
		Phase:   "source_validated",
		Scanned: source.summaryRecords + source.targetRecords,
	}); err != nil {
		return result, err
	}

	identityState, err := inspectTrafficMigrationIdentity(ctx, source, options)
	if err != nil {
		return result, err
	}
	revisionInput, err := buildPostgresRevisionInput(
		ctx,
		options.Configuration,
		source.revisionKey,
	)
	if err != nil {
		return result, err
	}
	defer clearTrafficMigrationRevisionInput(&revisionInput)
	activeRevision, err := source.activeRevision(options.ActiveConfigRevision, revisionInput.ConfigRevision)
	if err != nil {
		return result, err
	}
	fromBucket, toBucket := trafficMigrationBounds(options.From, options.To)
	migrationID := buildTrafficMigrationID(
		source.fingerprint,
		source.size,
		identityState.identity.InstanceID,
		options.Traffic.Schema,
		revisionInput.RoutingFingerprint,
		fromBucket,
		toBucket,
		activeRevision,
	)
	if options.ResumeMigrationID != "" && options.ResumeMigrationID != migrationID {
		return result, ErrTrafficMigrationResumeMismatch
	}
	result = TrafficStatisticsMigrationResult{
		MigrationID:       migrationID,
		SourceFingerprint: hex.EncodeToString(source.fingerprint[:]),
		InstanceID:        identityState.identity.InstanceID,
		DryRun:            options.DryRun,
	}
	startRuntime := service.FromContext[func() (
		context.Context,
		log.ContextLogger,
		func() error,
		error,
	)](ctx)
	if startRuntime != nil {
		runtimeCtx, runtimeLogger, closeRuntime, startErr := startRuntime()
		if startErr != nil {
			return result, startErr
		}
		ctx = runtimeCtx
		logger = runtimeLogger
		if closeRuntime != nil {
			defer func() {
				closeErr := closeRuntime()
				if err == nil && closeErr != nil {
					err = closeErr
					result.Completed = false
				}
			}()
		}
	}
	target, err := openTrafficMigrationTarget(
		ctx,
		logger,
		source,
		identityState.identity,
		revisionInput,
		activeRevision,
		migrationID,
		fromBucket,
		toBucket,
		options,
	)
	if err != nil {
		return result, err
	}
	defer func() {
		closeErr := target.Close()
		if err == nil && closeErr != nil {
			err = closeErr
			result.Completed = false
		}
	}()
	if err = emitTrafficMigrationProgress(options.Progress, TrafficStatisticsMigrationProgress{
		Phase:       "target_validated",
		MigrationID: migrationID,
	}); err != nil {
		return result, err
	}
	if !options.DryRun && identityState.missing {
		if err = persistTrafficMigrationIdentity(ctx, identityState); err != nil {
			return result, err
		}
	}
	result.Summary, err = target.restoreKind(
		ctx,
		"summary",
		options.BatchSize,
		options.DryRun,
		options.Progress,
	)
	if err != nil {
		return result, err
	}
	result.Targets, err = target.restoreKind(
		ctx,
		"target",
		options.BatchSize,
		options.DryRun,
		options.Progress,
	)
	if err != nil {
		return result, err
	}
	result.Completed = true
	if err = emitTrafficMigrationProgress(options.Progress, TrafficStatisticsMigrationProgress{
		Phase:       "completed",
		MigrationID: migrationID,
		Scanned:     result.Summary.Scanned + result.Targets.Scanned,
		Inserted:    result.Summary.Inserted + result.Targets.Inserted,
		Skipped:     result.Summary.Skipped + result.Targets.Skipped,
	}); err != nil {
		result.Completed = false
		return result, err
	}
	return result, nil
}

func validateTrafficMigrationOptions(options TrafficStatisticsMigrationOptions) error {
	if options.BatchSize < 1 || options.BatchSize > 10000 {
		return fmt.Errorf("traffic statistics migration batch size must be between 1 and 10000")
	}
	if !options.Traffic.Enabled {
		return errors.New("traffic statistics are not enabled")
	}
	if options.Traffic.StorageType != option.TrafficStatisticsStorageTypePostgres {
		return errors.New("traffic statistics migration requires PostgreSQL storage")
	}
	if err := option.ValidateTrafficStatisticsSchema(options.Traffic.Schema); err != nil {
		return fmt.Errorf("%w", ErrPostgresConfiguration)
	}
	if options.ResumeMigrationID != "" &&
		!trafficMigrationIDPattern.MatchString(options.ResumeMigrationID) {
		return ErrTrafficMigrationResumeMismatch
	}
	if options.ActiveConfigRevision != "" &&
		!trafficMigrationRevisionPattern.MatchString(options.ActiveConfigRevision) {
		return fmt.Errorf("%w: invalid active config revision", ErrTrafficMigrationSourceInvalid)
	}
	if err := validateTrafficMigrationBound(options.From); err != nil {
		return err
	}
	if err := validateTrafficMigrationBound(options.To); err != nil {
		return err
	}
	if !options.From.IsZero() && !options.To.IsZero() && !options.From.Before(options.To) {
		return errors.New("traffic statistics migration --from must be before --to")
	}
	return nil
}

func validateTrafficMigrationBound(value time.Time) error {
	if value.IsZero() {
		return nil
	}
	utc := value.UTC()
	if utc.Second() != 0 || utc.Nanosecond() != 0 {
		return errors.New("traffic statistics migration bounds must be minute-aligned RFC3339")
	}
	return nil
}

func trafficMigrationBounds(from time.Time, to time.Time) (int64, int64) {
	fromBucket := int64(math.MinInt64)
	toBucket := int64(math.MaxInt64)
	if !from.IsZero() {
		fromBucket = from.UTC().Unix()
	}
	if !to.IsZero() {
		toBucket = to.UTC().Unix()
	}
	return fromBucket, toBucket
}

func inspectTrafficMigrationIdentity(
	ctx context.Context,
	source *trafficMigrationSource,
	options TrafficStatisticsMigrationOptions,
) (trafficMigrationIdentityState, error) {
	path := filemanager.BasePath(ctx, os.ExpandEnv(options.Traffic.IdentityPath))
	existing, readErr := readTrafficIdentity(path)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return trafficMigrationIdentityState{}, fmt.Errorf(
			"%w",
			ErrTrafficMigrationIdentityConflict,
		)
	}
	instanceID := options.InstanceID
	if instanceID == "" {
		instanceID = options.Traffic.InstanceID
	}
	if instanceID == "" && readErr == nil {
		instanceID = existing.InstanceID
	}
	if instanceID == "" || !option.ValidateTrafficStatisticsInstanceID(instanceID) {
		return trafficMigrationIdentityState{}, fmt.Errorf(
			"%w",
			ErrTrafficMigrationIdentityConflict,
		)
	}
	identity := trafficIdentity{
		Version:     trafficIdentityVersion,
		InstanceID:  instanceID,
		RevisionKey: append([]byte(nil), source.revisionKey...),
	}
	if readErr == nil &&
		(existing.InstanceID != identity.InstanceID ||
			!bytes.Equal(existing.RevisionKey, identity.RevisionKey)) {
		return trafficMigrationIdentityState{}, ErrTrafficMigrationIdentityConflict
	}
	return trafficMigrationIdentityState{
		path:     path,
		identity: identity,
		missing:  errors.Is(readErr, os.ErrNotExist),
	}, nil
}

func persistTrafficMigrationIdentity(
	ctx context.Context,
	state trafficMigrationIdentityState,
) error {
	parent := filepath.Dir(state.path)
	if parent != "." {
		if err := filemanager.MkdirAll(ctx, parent, 0o755); err != nil {
			return fmt.Errorf("create traffic identity directory: %w", err)
		}
	}
	won, err := writeTrafficIdentityCandidate(ctx, state.path, state.identity)
	if err != nil {
		return err
	}
	if won {
		return nil
	}
	existing, err := readTrafficIdentity(state.path)
	if err != nil ||
		existing.InstanceID != state.identity.InstanceID ||
		!bytes.Equal(existing.RevisionKey, state.identity.RevisionKey) {
		return ErrTrafficMigrationIdentityConflict
	}
	return nil
}

func buildTrafficMigrationID(
	sourceFingerprint [sha256.Size]byte,
	sourceSize int64,
	instanceID string,
	schema string,
	routingFingerprint [sha256.Size]byte,
	fromBucket int64,
	toBucket int64,
	activeRevision string,
) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("mbox/traffic-statistics/bolt-restore/v1"))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(sourceFingerprint[:])
	var integer [8]byte
	binary.BigEndian.PutUint64(integer[:], uint64(sourceSize))
	_, _ = hash.Write(integer[:])
	writeTrafficMigrationString(hash, instanceID)
	writeTrafficMigrationString(hash, schema)
	_, _ = hash.Write(routingFingerprint[:])
	binary.BigEndian.PutUint64(integer[:], uint64(fromBucket))
	_, _ = hash.Write(integer[:])
	binary.BigEndian.PutUint64(integer[:], uint64(toBucket))
	_, _ = hash.Write(integer[:])
	revision, _ := hex.DecodeString(activeRevision)
	_, _ = hash.Write(revision)
	return hex.EncodeToString(hash.Sum(nil))
}

func writeTrafficMigrationString(
	writer interface{ Write([]byte) (int, error) },
	value string,
) {
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write([]byte(value))
}

func emitTrafficMigrationProgress(
	progress func(TrafficStatisticsMigrationProgress) error,
	event TrafficStatisticsMigrationProgress,
) error {
	if progress == nil {
		return nil
	}
	return progress(event)
}

func clearTrafficMigrationRevisionInput(input *postgresRevisionInput) {
	for index := range input.CanonicalContent {
		input.CanonicalContent[index] = 0
	}
	input.CanonicalContent = nil
}

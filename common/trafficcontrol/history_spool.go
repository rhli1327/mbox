package trafficcontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gofrs/uuid/v5"
	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing/service/filemanager"
)

const (
	historySpoolFormatVersion   uint32 = 1
	historySpoolSafeLogicalSize        = uint64(math.MaxInt)
)

var (
	historySpoolMetadataBucket = []byte(
		"traffic_statistics_spool_metadata_v1",
	)
	historySpoolPendingBucket = []byte(
		"traffic_statistics_spool_pending_v1",
	)
	historySpoolInflightBucket = []byte(
		"traffic_statistics_spool_inflight_v1",
	)
	historySpoolRetiredBucket = []byte(
		"traffic_statistics_spool_retired_v1",
	)

	historySpoolFormatVersionKey  = []byte("format_version")
	historySpoolIDKey             = []byte("spool_id")
	historySpoolInstanceIDKey     = []byte("instance_id")
	historySpoolRevisionHashKey   = []byte("revision_key_sha256")
	historySpoolRoutingKey        = []byte("routing_fingerprint")
	historySpoolConfigRevisionKey = []byte(
		"config_revision",
	)
	historySpoolRemoteRevisionConfirmedKey = []byte(
		"remote_revision_confirmed",
	)
	historySpoolTargetAvailableFromKey = []byte(
		"target_available_from",
	)
	historySpoolDestinationAvailableFromKey = []byte(
		"destination_available_from",
	)
	historySpoolNextSequenceKey        = []byte("next_sequence")
	historySpoolResolvedSequenceKey    = []byte("resolved_sequence")
	historySpoolQueuedBatchesKey       = []byte("queued_batches")
	historySpoolQueuedRecordsKey       = []byte("queued_records")
	historySpoolQueuedBytesKey         = []byte("queued_bytes")
	historySpoolDroppedBatchesKey      = []byte("dropped_batches")
	historySpoolDroppedRecordsKey      = []byte("dropped_records")
	historySpoolDroppedBytesKey        = []byte("dropped_bytes")
	historySpoolLossGenerationKey      = []byte("loss_generation")
	historySpoolLastSuccessfulFlushKey = []byte(
		"last_successful_flush_unix_nano",
	)

	historySpoolMetadataKeys = [][]byte{
		historySpoolFormatVersionKey,
		historySpoolIDKey,
		historySpoolInstanceIDKey,
		historySpoolRevisionHashKey,
		historySpoolRoutingKey,
		historySpoolConfigRevisionKey,
		historySpoolRemoteRevisionConfirmedKey,
		historySpoolTargetAvailableFromKey,
		historySpoolDestinationAvailableFromKey,
		historySpoolNextSequenceKey,
		historySpoolResolvedSequenceKey,
		historySpoolQueuedBatchesKey,
		historySpoolQueuedRecordsKey,
		historySpoolQueuedBytesKey,
		historySpoolDroppedBatchesKey,
		historySpoolDroppedRecordsKey,
		historySpoolDroppedBytesKey,
		historySpoolLossGenerationKey,
		historySpoolLastSuccessfulFlushKey,
	}
	historySpoolBuckets = [][]byte{
		historySpoolMetadataBucket,
		historySpoolPendingBucket,
		historySpoolInflightBucket,
		historySpoolRetiredBucket,
	}
)

type historyCommitBoundary struct {
	Sequence        uint64
	LossGeneration  uint64
	Durable         bool
	LossDuringWrite bool
}

type historySpoolOptions struct {
	Context            context.Context
	Path               string
	MaxSize            uint64
	Identity           trafficIdentity
	RoutingFingerprint [sha256.Size]byte
	ConfigRevision     string
	Now                func() time.Time
	Faults             *historySpoolFaults
}

type historySpoolFaults struct {
	BeforeEnvelopeAllocation      func() error
	BeforeEnvelopeMaterialization func() error
	BeforeAppendCommit            func() error
	AfterAppendCommit             func() error
	BeforeOverflowCommit          func() error
	AfterLeaseCommit              func() error
	BeforeAcknowledgeCommit       func() error
	AfterAcknowledgeCommit        func() error
	FileSync                      func() error
	ParentSync                    func() error
	OpenDatabase                  func() error
	TransactionWrite              func(operation string) error
	View                          func(operation string) error
}

type historySpoolStatus struct {
	FormatVersion            uint32
	SpoolID                  uuid.UUID
	InstanceID               string
	RoutingFingerprint       [sha256.Size]byte
	ConfigRevision           string
	RemoteRevisionConfirmed  bool
	TargetAvailableFrom      time.Time
	DestinationAvailableFrom time.Time
	NextSequence             uint64
	ResolvedSequence         uint64
	QueueDepth               uint64
	QueueRecords             uint64
	QueueBytes               uint64
	InFlight                 bool
	DroppedBatches           uint64
	DroppedRecords           uint64
	DroppedBytes             uint64
	LossGeneration           uint64
	LastSuccessfulFlush      time.Time
}

type historySpool struct {
	access  sync.Mutex
	ctx     context.Context
	path    string
	maxSize uint64
	now     func() time.Time
	faults  *historySpoolFaults

	configRevision string
	db             *bbolt.DB
	closed         bool
}

type historySpoolMigration func(
	*bbolt.Tx,
	historySpoolOptions,
) error

var historySpoolMigrations = []historySpoolMigration{
	initializeHistorySpoolV1,
}

func openHistorySpool(
	options historySpoolOptions,
) (*historySpool, historyStoreState, error) {
	if options.Context == nil {
		options.Context = context.Background()
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.MaxSize == 0 {
		return nil, historyStoreState{}, fmt.Errorf(
			"%w: configured capacity is zero",
			ErrSpoolIO,
		)
	}
	if !isHistoryConfigRevision(options.ConfigRevision) {
		return nil, historyStoreState{}, fmt.Errorf(
			"%w: config revision is invalid",
			ErrSpoolCorrupt,
		)
	}
	if options.Identity.Version != trafficIdentityVersion ||
		options.Identity.InstanceID == "" ||
		!utf8.ValidString(options.Identity.InstanceID) ||
		len(options.Identity.RevisionKey) != sha256.Size {
		return nil, historyStoreState{}, ErrSpoolIdentityConflict
	}
	path := filemanager.BasePath(options.Context, os.ExpandEnv(options.Path))
	if path == "" {
		return nil, historyStoreState{}, fmt.Errorf(
			"%w: path is empty",
			ErrSpoolIO,
		)
	}
	absolutePath, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return nil, historyStoreState{}, fmt.Errorf("%w: resolve path", ErrSpoolIO)
	}
	path = absolutePath
	parent := filepath.Dir(path)
	if err = filemanager.MkdirAll(options.Context, parent, 0o755); err != nil {
		return nil, historyStoreState{}, fmt.Errorf(
			"%w: create parent directory",
			ErrSpoolIO,
		)
	}
	_, err = prepareHistorySpoolFile(
		options.Context,
		path,
		options.Faults,
	)
	if err != nil {
		return nil, historyStoreState{}, err
	}
	if err = runHistorySpoolFault(
		options.Faults,
		func(value *historySpoolFaults) func() error {
			return value.OpenDatabase
		},
	); err != nil {
		return nil, historyStoreState{}, classifyHistorySpoolError(err)
	}
	database, err := bbolt.Open(path, 0o600, &bbolt.Options{
		Timeout:        time.Second,
		NoSync:         false,
		NoFreelistSync: false,
	})
	if err != nil {
		return nil, historyStoreState{}, classifyHistorySpoolError(err)
	}
	closeOnError := func(openErr error) (*historySpool, historyStoreState, error) {
		_ = database.Close()
		return nil, historyStoreState{}, openErr
	}
	if err = filemanager.Chown(options.Context, path); err != nil {
		return closeOnError(fmt.Errorf("%w: set ownership", ErrSpoolIO))
	}
	if err = migrateHistorySpool(database, options); err != nil {
		return closeOnError(err)
	}
	spool := &historySpool{
		ctx:            options.Context,
		path:           path,
		maxSize:        options.MaxSize,
		now:            options.Now,
		faults:         options.Faults,
		configRevision: options.ConfigRevision,
		db:             database,
	}
	if err = spool.validateAndPrepare(options); err != nil {
		return closeOnError(err)
	}
	status, err := spool.readStatus()
	if err != nil {
		return closeOnError(err)
	}
	spool.configRevision = status.ConfigRevision
	if err = runHistorySpoolFault(
		options.Faults,
		func(value *historySpoolFaults) func() error {
			return value.ParentSync
		},
	); err == nil {
		err = syncHistorySpoolParent(parent)
	}
	if err != nil {
		return closeOnError(classifyHistorySpoolError(err))
	}
	return spool, historyStoreState{
		configRevision:   status.ConfigRevision,
		targetsFrom:      status.TargetAvailableFrom,
		destinationsFrom: status.DestinationAvailableFrom,
	}, nil
}

func prepareHistorySpoolFile(
	ctx context.Context,
	path string,
	faults *historySpoolFaults,
) (bool, error) {
	info, err := os.Lstat(path)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return false, fmt.Errorf(
				"%w: spool must be a regular non-symlink file",
				ErrSpoolIO,
			)
		}
		if err = validateHistorySpoolPermissions(info.Mode()); err != nil {
			return false, fmt.Errorf("%w: %v", ErrSpoolIO, err)
		}
		return false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("%w: inspect spool", ErrSpoolIO)
	}
	file, err := filemanager.OpenFile(
		ctx,
		path,
		os.O_RDWR|os.O_CREATE|os.O_EXCL,
		0o600,
	)
	if err != nil {
		return false, fmt.Errorf("%w: create spool", ErrSpoolIO)
	}
	if err = file.Chmod(0o600); err != nil {
		_ = file.Close()
		return false, fmt.Errorf("%w: set spool permissions", ErrSpoolIO)
	}
	if err = runHistorySpoolFault(
		faults,
		func(value *historySpoolFaults) func() error {
			return value.FileSync
		},
	); err == nil {
		err = file.Sync()
	}
	if err != nil {
		_ = file.Close()
		return false, classifyHistorySpoolError(err)
	}
	if err = file.Close(); err != nil {
		return false, fmt.Errorf("%w: close new spool", ErrSpoolIO)
	}
	if err = filemanager.Chown(ctx, path); err != nil {
		return false, fmt.Errorf("%w: set spool ownership", ErrSpoolIO)
	}
	return true, nil
}

func migrateHistorySpool(database *bbolt.DB, options historySpoolOptions) error {
	return updateHistorySpoolDatabase(
		database,
		options.Faults,
		"migrate",
		func(tx *bbolt.Tx) error {
			version, pristine, err := readHistorySpoolVersion(tx)
			if err != nil {
				return err
			}
			if version > historySpoolFormatVersion {
				return ErrSpoolFuture
			}
			for version < historySpoolFormatVersion {
				if version != 0 || !pristine || int(version) >= len(historySpoolMigrations) {
					return fmt.Errorf("%w: unsupported migration state", ErrSpoolCorrupt)
				}
				if err = historySpoolMigrations[version](tx, options); err != nil {
					return fmt.Errorf("%w: initialize format", ErrSpoolIO)
				}
				version++
				pristine = false
			}
			return nil
		},
	)
}

func readHistorySpoolVersion(tx *bbolt.Tx) (uint32, bool, error) {
	metadata := tx.Bucket(historySpoolMetadataBucket)
	if metadata == nil {
		pristine := true
		err := tx.ForEach(func([]byte, *bbolt.Bucket) error {
			pristine = false
			return nil
		})
		if err != nil {
			return 0, false, fmt.Errorf("%w: inspect buckets", ErrSpoolCorrupt)
		}
		if !pristine {
			return 0, false, fmt.Errorf(
				"%w: metadata is missing",
				ErrSpoolCorrupt,
			)
		}
		return 0, true, nil
	}
	value := metadata.Get(historySpoolFormatVersionKey)
	if len(value) != 4 {
		return 0, false, fmt.Errorf(
			"%w: format version is invalid",
			ErrSpoolCorrupt,
		)
	}
	return binary.BigEndian.Uint32(value), false, nil
}

func initializeHistorySpoolV1(
	tx *bbolt.Tx,
	options historySpoolOptions,
) error {
	for _, name := range historySpoolBuckets {
		if _, err := tx.CreateBucket(name); err != nil {
			return err
		}
	}
	spoolID, err := uuid.NewV4()
	if err != nil {
		return err
	}
	availableFrom := options.Now().UTC().
		Truncate(HistoryBucketInterval).
		Add(HistoryBucketInterval)
	revisionHash := sha256.Sum256(options.Identity.RevisionKey)
	metadata := tx.Bucket(historySpoolMetadataBucket)
	values := []struct {
		key   []byte
		value []byte
	}{
		{historySpoolFormatVersionKey, encodeHistorySpoolUint32(historySpoolFormatVersion)},
		{historySpoolIDKey, spoolID[:]},
		{historySpoolInstanceIDKey, []byte(options.Identity.InstanceID)},
		{historySpoolRevisionHashKey, revisionHash[:]},
		{historySpoolRoutingKey, options.RoutingFingerprint[:]},
		{historySpoolConfigRevisionKey, []byte(options.ConfigRevision)},
		{historySpoolRemoteRevisionConfirmedKey, []byte{0}},
		{historySpoolTargetAvailableFromKey, encodeHistorySpoolInt64(availableFrom.Unix())},
		{historySpoolDestinationAvailableFromKey, encodeHistorySpoolInt64(availableFrom.Unix())},
		{historySpoolNextSequenceKey, encodeHistorySpoolUint64(1)},
		{historySpoolResolvedSequenceKey, encodeHistorySpoolUint64(0)},
		{historySpoolQueuedBatchesKey, encodeHistorySpoolUint64(0)},
		{historySpoolQueuedRecordsKey, encodeHistorySpoolUint64(0)},
		{historySpoolQueuedBytesKey, encodeHistorySpoolUint64(0)},
		{historySpoolDroppedBatchesKey, encodeHistorySpoolUint64(0)},
		{historySpoolDroppedRecordsKey, encodeHistorySpoolUint64(0)},
		{historySpoolDroppedBytesKey, encodeHistorySpoolUint64(0)},
		{historySpoolLossGenerationKey, encodeHistorySpoolUint64(0)},
		{historySpoolLastSuccessfulFlushKey, encodeHistorySpoolInt64(0)},
	}
	for _, item := range values {
		if err = metadata.Put(item.key, item.value); err != nil {
			return err
		}
	}
	return nil
}

func (s *historySpool) validateAndPrepare(options historySpoolOptions) error {
	return updateHistorySpoolDatabase(
		s.db,
		s.faults,
		"startup",
		func(tx *bbolt.Tx) error {
			status, err := validateHistorySpoolTransaction(
				tx,
				historySpoolSafeLogicalSize,
				false,
				s.faults,
			)
			if err != nil {
				return err
			}
			revisionHash := sha256.Sum256(options.Identity.RevisionKey)
			if status.InstanceID != options.Identity.InstanceID ||
				!bytes.Equal(
					tx.Bucket(historySpoolMetadataBucket).Get(historySpoolRevisionHashKey),
					revisionHash[:],
				) {
				return ErrSpoolIdentityConflict
			}
			metadata := tx.Bucket(historySpoolMetadataBucket)
			if status.RoutingFingerprint == options.RoutingFingerprint {
				if !status.RemoteRevisionConfirmed &&
					status.ConfigRevision != options.ConfigRevision {
					return ErrPostgresRevisionConflict
				}
			} else {
				if err = metadata.Put(
					historySpoolRoutingKey,
					options.RoutingFingerprint[:],
				); err != nil {
					return fmt.Errorf("%w: update routing fingerprint", ErrSpoolIO)
				}
				if err = metadata.Put(
					historySpoolConfigRevisionKey,
					[]byte(options.ConfigRevision),
				); err != nil {
					return fmt.Errorf("%w: update config revision", ErrSpoolIO)
				}
				if err = metadata.Put(
					historySpoolRemoteRevisionConfirmedKey,
					[]byte{0},
				); err != nil {
					return fmt.Errorf("%w: reset revision confirmation", ErrSpoolIO)
				}
			}
			if err = enforceHistorySpoolCapacity(tx, s.maxSize); err != nil {
				return err
			}
			if err = requeueHistorySpoolInflight(tx); err != nil {
				return err
			}
			_, err = validateHistorySpoolTransaction(
				tx,
				historySpoolSafeLogicalSize,
				true,
				s.faults,
			)
			return err
		},
	)
}

func validateHistorySpoolTransaction(
	tx *bbolt.Tx,
	maxLogicalSize uint64,
	materialize bool,
	faults *historySpoolFaults,
) (historySpoolStatus, error) {
	if err := validateHistorySpoolBuckets(tx); err != nil {
		return historySpoolStatus{}, err
	}
	metadata := tx.Bucket(historySpoolMetadataBucket)
	if err := validateHistorySpoolMetadataKeys(metadata); err != nil {
		return historySpoolStatus{}, err
	}
	status, err := decodeHistorySpoolStatus(metadata)
	if err != nil {
		return historySpoolStatus{}, err
	}
	type sequenceEntry struct {
		sequence uint64
		kind     byte
	}
	var entries []sequenceEntry
	var queuedBatches uint64
	var queuedRecords uint64
	var queuedBytes uint64
	inflightCount := 0
	for _, bucketSpec := range []struct {
		name []byte
		kind byte
	}{
		{historySpoolPendingBucket, 'p'},
		{historySpoolInflightBucket, 'i'},
		{historySpoolRetiredBucket, 'r'},
	} {
		bucket := tx.Bucket(bucketSpec.name)
		err = bucket.ForEach(func(key []byte, value []byte) error {
			if len(key) != 8 || value == nil {
				return fmt.Errorf("%w: queue key is invalid", ErrSpoolCorrupt)
			}
			sequence := binary.BigEndian.Uint64(key)
			if sequence == 0 {
				return fmt.Errorf("%w: queue sequence is zero", ErrSpoolCorrupt)
			}
			entries = append(entries, sequenceEntry{sequence, bucketSpec.kind})
			if bucketSpec.kind == 'r' {
				if !bytes.Equal(value, []byte{1}) {
					return fmt.Errorf(
						"%w: retired reason is invalid",
						ErrSpoolCorrupt,
					)
				}
				return nil
			}
			inspection, decodeErr := inspectHistoryBatchEnvelope(
				value,
				status.SpoolID,
				maxLogicalSize,
			)
			if decodeErr != nil {
				return decodeErr
			}
			if inspection.Sequence != sequence {
				return fmt.Errorf(
					"%w: queue key does not match envelope",
					ErrSpoolCorrupt,
				)
			}
			if materialize {
				if _, decodeErr = decodeHistoryBatchEnvelopeWithFaults(
					value,
					status.SpoolID,
					maxLogicalSize,
					faults,
				); decodeErr != nil {
					return decodeErr
				}
			}
			var addErr error
			queuedBatches, addErr = addHistorySpoolCounter(queuedBatches, 1)
			if addErr != nil {
				return addErr
			}
			queuedRecords, addErr = addHistorySpoolCounter(
				queuedRecords,
				uint64(inspection.RecordCount),
			)
			if addErr != nil {
				return addErr
			}
			queuedBytes, addErr = addHistorySpoolCounter(
				queuedBytes,
				inspection.LogicalSize,
			)
			if addErr != nil {
				return addErr
			}
			if bucketSpec.kind == 'i' {
				inflightCount++
			}
			return nil
		})
		if err != nil {
			return historySpoolStatus{}, err
		}
	}
	if inflightCount > 1 {
		return historySpoolStatus{}, fmt.Errorf(
			"%w: multiple inflight batches",
			ErrSpoolCorrupt,
		)
	}
	slices.SortFunc(entries, func(left, right sequenceEntry) int {
		switch {
		case left.sequence < right.sequence:
			return -1
		case left.sequence > right.sequence:
			return 1
		default:
			return 0
		}
	})
	unresolved := status.NextSequence - status.ResolvedSequence - 1
	if uint64(len(entries)) != unresolved {
		return historySpoolStatus{}, fmt.Errorf(
			"%w: queue sequence count is inconsistent",
			ErrSpoolCorrupt,
		)
	}
	for index, entry := range entries {
		want := status.ResolvedSequence + uint64(index) + 1
		if entry.sequence != want || entry.sequence >= status.NextSequence {
			return historySpoolStatus{}, fmt.Errorf(
				"%w: queue sequence range is inconsistent",
				ErrSpoolCorrupt,
			)
		}
	}
	if queuedBatches != status.QueueDepth ||
		queuedRecords != status.QueueRecords ||
		queuedBytes != status.QueueBytes {
		return historySpoolStatus{}, fmt.Errorf(
			"%w: queue counters are inconsistent",
			ErrSpoolCorrupt,
		)
	}
	status.InFlight = inflightCount == 1
	return status, nil
}

func validateHistorySpoolBuckets(tx *bbolt.Tx) error {
	seen := make(map[string]bool, len(historySpoolBuckets))
	err := tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
		for _, known := range historySpoolBuckets {
			if bytes.Equal(name, known) {
				seen[string(known)] = true
				return nil
			}
		}
		return fmt.Errorf("%w: unknown top-level bucket", ErrSpoolCorrupt)
	})
	if err != nil {
		return err
	}
	for _, known := range historySpoolBuckets {
		if !seen[string(known)] {
			return fmt.Errorf("%w: required bucket is missing", ErrSpoolCorrupt)
		}
	}
	return nil
}

func validateHistorySpoolMetadataKeys(metadata *bbolt.Bucket) error {
	seen := make(map[string]bool, len(historySpoolMetadataKeys))
	err := metadata.ForEach(func(key []byte, value []byte) error {
		if value == nil {
			return fmt.Errorf("%w: nested metadata bucket", ErrSpoolCorrupt)
		}
		for _, known := range historySpoolMetadataKeys {
			if bytes.Equal(key, known) {
				seen[string(known)] = true
				return nil
			}
		}
		return fmt.Errorf("%w: unknown metadata key", ErrSpoolCorrupt)
	})
	if err != nil {
		return err
	}
	for _, known := range historySpoolMetadataKeys {
		if !seen[string(known)] {
			return fmt.Errorf("%w: metadata key is missing", ErrSpoolCorrupt)
		}
	}
	return nil
}

func decodeHistorySpoolStatus(
	metadata *bbolt.Bucket,
) (historySpoolStatus, error) {
	readUint64 := func(key []byte) (uint64, error) {
		value := metadata.Get(key)
		if len(value) != 8 {
			return 0, fmt.Errorf("%w: metadata integer is invalid", ErrSpoolCorrupt)
		}
		return binary.BigEndian.Uint64(value), nil
	}
	versionValue := metadata.Get(historySpoolFormatVersionKey)
	if len(versionValue) != 4 {
		return historySpoolStatus{}, fmt.Errorf(
			"%w: format version is invalid",
			ErrSpoolCorrupt,
		)
	}
	version := binary.BigEndian.Uint32(versionValue)
	if version > historySpoolFormatVersion {
		return historySpoolStatus{}, ErrSpoolFuture
	}
	if version != historySpoolFormatVersion {
		return historySpoolStatus{}, fmt.Errorf(
			"%w: format version is unsupported",
			ErrSpoolCorrupt,
		)
	}
	spoolIDValue := metadata.Get(historySpoolIDKey)
	spoolID, err := uuid.FromBytes(spoolIDValue)
	if err != nil || len(spoolIDValue) != uuid.Size || spoolID.Version() != uuid.V4 {
		return historySpoolStatus{}, fmt.Errorf(
			"%w: spool ID is invalid",
			ErrSpoolCorrupt,
		)
	}
	instanceID := metadata.Get(historySpoolInstanceIDKey)
	if len(instanceID) == 0 || !utf8.Valid(instanceID) {
		return historySpoolStatus{}, fmt.Errorf(
			"%w: instance ID is invalid",
			ErrSpoolCorrupt,
		)
	}
	if len(metadata.Get(historySpoolRevisionHashKey)) != sha256.Size {
		return historySpoolStatus{}, fmt.Errorf(
			"%w: revision-key hash is invalid",
			ErrSpoolCorrupt,
		)
	}
	routing := metadata.Get(historySpoolRoutingKey)
	if len(routing) != sha256.Size {
		return historySpoolStatus{}, fmt.Errorf(
			"%w: routing fingerprint is invalid",
			ErrSpoolCorrupt,
		)
	}
	var routingFingerprint [sha256.Size]byte
	copy(routingFingerprint[:], routing)
	configRevision := string(metadata.Get(historySpoolConfigRevisionKey))
	if !isHistoryConfigRevision(configRevision) {
		return historySpoolStatus{}, fmt.Errorf(
			"%w: config revision is invalid",
			ErrSpoolCorrupt,
		)
	}
	confirmed := metadata.Get(historySpoolRemoteRevisionConfirmedKey)
	if len(confirmed) != 1 || confirmed[0] > 1 {
		return historySpoolStatus{}, fmt.Errorf(
			"%w: remote revision state is invalid",
			ErrSpoolCorrupt,
		)
	}
	targetSeconds, err := readUint64(historySpoolTargetAvailableFromKey)
	if err != nil {
		return historySpoolStatus{}, err
	}
	destinationSeconds, err := readUint64(
		historySpoolDestinationAvailableFromKey,
	)
	if err != nil {
		return historySpoolStatus{}, err
	}
	targetAvailableFrom := time.Unix(int64(targetSeconds), 0).UTC()
	destinationAvailableFrom := time.Unix(int64(destinationSeconds), 0).UTC()
	if destinationAvailableFrom.Before(targetAvailableFrom) {
		return historySpoolStatus{}, fmt.Errorf(
			"%w: availability order is invalid",
			ErrSpoolCorrupt,
		)
	}
	nextSequence, err := readUint64(historySpoolNextSequenceKey)
	if err != nil {
		return historySpoolStatus{}, err
	}
	resolvedSequence, err := readUint64(historySpoolResolvedSequenceKey)
	if err != nil {
		return historySpoolStatus{}, err
	}
	if nextSequence == 0 || resolvedSequence >= nextSequence {
		return historySpoolStatus{}, fmt.Errorf(
			"%w: sequence watermarks are invalid",
			ErrSpoolCorrupt,
		)
	}
	queueDepth, err := readUint64(historySpoolQueuedBatchesKey)
	if err != nil {
		return historySpoolStatus{}, err
	}
	queueRecords, err := readUint64(historySpoolQueuedRecordsKey)
	if err != nil {
		return historySpoolStatus{}, err
	}
	queueBytes, err := readUint64(historySpoolQueuedBytesKey)
	if err != nil {
		return historySpoolStatus{}, err
	}
	droppedBatches, err := readUint64(historySpoolDroppedBatchesKey)
	if err != nil {
		return historySpoolStatus{}, err
	}
	droppedRecords, err := readUint64(historySpoolDroppedRecordsKey)
	if err != nil {
		return historySpoolStatus{}, err
	}
	droppedBytes, err := readUint64(historySpoolDroppedBytesKey)
	if err != nil {
		return historySpoolStatus{}, err
	}
	lossGeneration, err := readUint64(historySpoolLossGenerationKey)
	if err != nil {
		return historySpoolStatus{}, err
	}
	lastSuccessfulNanos, err := readUint64(historySpoolLastSuccessfulFlushKey)
	if err != nil {
		return historySpoolStatus{}, err
	}
	var lastSuccessfulFlush time.Time
	if lastSuccessfulNanos != 0 {
		lastSuccessfulFlush = time.Unix(0, int64(lastSuccessfulNanos)).UTC()
	}
	return historySpoolStatus{
		FormatVersion:            version,
		SpoolID:                  spoolID,
		InstanceID:               string(instanceID),
		RoutingFingerprint:       routingFingerprint,
		ConfigRevision:           configRevision,
		RemoteRevisionConfirmed:  confirmed[0] == 1,
		TargetAvailableFrom:      targetAvailableFrom,
		DestinationAvailableFrom: destinationAvailableFrom,
		NextSequence:             nextSequence,
		ResolvedSequence:         resolvedSequence,
		QueueDepth:               queueDepth,
		QueueRecords:             queueRecords,
		QueueBytes:               queueBytes,
		DroppedBatches:           droppedBatches,
		DroppedRecords:           droppedRecords,
		DroppedBytes:             droppedBytes,
		LossGeneration:           lossGeneration,
		LastSuccessfulFlush:      lastSuccessfulFlush,
	}, nil
}

func requeueHistorySpoolInflight(tx *bbolt.Tx) error {
	inflight := tx.Bucket(historySpoolInflightBucket)
	cursor := inflight.Cursor()
	key, value := cursor.First()
	if key == nil {
		return nil
	}
	keyCopy := append([]byte(nil), key...)
	valueCopy := append([]byte(nil), value...)
	if next, _ := cursor.Next(); next != nil {
		return fmt.Errorf("%w: multiple inflight batches", ErrSpoolCorrupt)
	}
	if err := tx.Bucket(historySpoolPendingBucket).Put(keyCopy, valueCopy); err != nil {
		return fmt.Errorf("%w: requeue inflight batch", ErrSpoolIO)
	}
	if err := inflight.Delete(keyCopy); err != nil {
		return fmt.Errorf("%w: clear inflight batch", ErrSpoolIO)
	}
	return nil
}

func enforceHistorySpoolCapacity(
	tx *bbolt.Tx,
	maxSize uint64,
) error {
	metadata := tx.Bucket(historySpoolMetadataBucket)
	queuedBytes, err := readHistorySpoolUint64(metadata, historySpoolQueuedBytesKey)
	if err != nil {
		return err
	}
	if queuedBytes <= maxSize {
		return nil
	}
	pending := tx.Bucket(historySpoolPendingBucket)
	droppedBatches := uint64(0)
	droppedRecords := uint64(0)
	droppedBytes := uint64(0)
	for queuedBytes > maxSize {
		key, value := pending.Cursor().First()
		if key == nil {
			break
		}
		inspection, decodeErr := inspectHistoryBatchEnvelope(
			value,
			mustHistorySpoolID(metadata),
			historySpoolSafeLogicalSize,
		)
		if decodeErr != nil {
			return decodeErr
		}
		logicalSize := inspection.LogicalSize
		keyCopy := append([]byte(nil), key...)
		if err = pending.Delete(keyCopy); err != nil {
			return fmt.Errorf("%w: evict pending batch", ErrSpoolIO)
		}
		if err = tx.Bucket(historySpoolRetiredBucket).Put(keyCopy, []byte{1}); err != nil {
			return fmt.Errorf("%w: record retired batch", ErrSpoolIO)
		}
		queuedBytes -= logicalSize
		droppedBatches++
		droppedRecords, err = addHistorySpoolCounter(
			droppedRecords,
			uint64(inspection.RecordCount),
		)
		if err != nil {
			return err
		}
		droppedBytes, err = addHistorySpoolCounter(droppedBytes, logicalSize)
		if err != nil {
			return err
		}
	}
	if droppedBatches == 0 {
		return nil
	}
	if err = updateHistorySpoolQueueCounters(
		metadata,
		false,
		droppedBatches,
		droppedRecords,
		droppedBytes,
	); err != nil {
		return err
	}
	_, err = addHistorySpoolDrops(
		metadata,
		droppedBatches,
		droppedRecords,
		droppedBytes,
	)
	return err
}

func (s *historySpool) Append(
	batch historyBatch,
) (historyCommitBoundary, error) {
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed || s.db == nil {
		return historyCommitBoundary{}, ErrSpoolClosed
	}
	prepared, oversize, err := prepareHistoryBatchEnvelope(
		batch,
		s.configRevision,
		s.maxSize,
	)
	if err != nil {
		return historyCommitBoundary{}, err
	}
	if len(prepared.Records) == 0 {
		status, statusErr := s.readStatusUnlocked()
		if statusErr != nil {
			return historyCommitBoundary{}, statusErr
		}
		return historyCommitBoundary{
			Sequence:       status.NextSequence - 1,
			LossGeneration: status.LossGeneration,
			Durable:        true,
		}, nil
	}
	var boundary historyCommitBoundary
	err = updateHistorySpoolDatabase(
		s.db,
		s.faults,
		"append",
		func(tx *bbolt.Tx) error {
			metadata := tx.Bucket(historySpoolMetadataBucket)
			nextSequence, readErr := readHistorySpoolUint64(
				metadata,
				historySpoolNextSequenceKey,
			)
			if readErr != nil {
				return readErr
			}
			spoolID := mustHistorySpoolID(metadata)
			queuedBytes, readErr := readHistorySpoolUint64(
				metadata,
				historySpoolQueuedBytesKey,
			)
			if readErr != nil {
				return readErr
			}
			var droppedBatches uint64
			var droppedRecords uint64
			var droppedBytes uint64
			rejectIncoming := oversize
			pending := tx.Bucket(historySpoolPendingBucket)
			for !rejectIncoming && queuedBytes > s.maxSize-prepared.LogicalSize {
				key, value := pending.Cursor().First()
				if key == nil {
					rejectIncoming = true
					break
				}
				evicted, decodeErr := inspectHistoryBatchEnvelope(
					value,
					spoolID,
					historySpoolSafeLogicalSize,
				)
				if decodeErr != nil {
					return decodeErr
				}
				evictedSize := evicted.LogicalSize
				keyCopy := append([]byte(nil), key...)
				if err = pending.Delete(keyCopy); err != nil {
					return fmt.Errorf("%w: evict pending batch", ErrSpoolIO)
				}
				if err = tx.Bucket(historySpoolRetiredBucket).Put(
					keyCopy,
					[]byte{1},
				); err != nil {
					return fmt.Errorf("%w: record retired batch", ErrSpoolIO)
				}
				queuedBytes -= evictedSize
				droppedBatches++
				droppedRecords, err = addHistorySpoolCounter(
					droppedRecords,
					uint64(evicted.RecordCount),
				)
				if err != nil {
					return err
				}
				droppedBytes, err = addHistorySpoolCounter(
					droppedBytes,
					evictedSize,
				)
				if err != nil {
					return err
				}
			}
			if droppedBatches > 0 {
				if err = updateHistorySpoolQueueCounters(
					metadata,
					false,
					droppedBatches,
					droppedRecords,
					droppedBytes,
				); err != nil {
					return err
				}
			}
			if rejectIncoming {
				droppedBatches++
				droppedRecords, err = addHistorySpoolCounter(
					droppedRecords,
					uint64(len(prepared.Records)),
				)
				if err != nil {
					return err
				}
				droppedBytes, err = addHistorySpoolCounter(
					droppedBytes,
					prepared.LogicalSize,
				)
				if err != nil {
					return err
				}
				lossGeneration, dropErr := addHistorySpoolDrops(
					metadata,
					droppedBatches,
					droppedRecords,
					droppedBytes,
				)
				if dropErr != nil {
					return dropErr
				}
				boundary = historyCommitBoundary{
					Sequence:        nextSequence - 1,
					LossGeneration:  lossGeneration,
					Durable:         false,
					LossDuringWrite: true,
				}
				if err = runHistorySpoolFault(
					s.faults,
					func(value *historySpoolFaults) func() error {
						return value.BeforeOverflowCommit
					},
				); err != nil {
					return err
				}
				return runHistorySpoolFault(
					s.faults,
					func(value *historySpoolFaults) func() error {
						return value.BeforeAppendCommit
					},
				)
			}
			if nextSequence == math.MaxUint64 {
				return ErrSpoolSequenceExhausted
			}
			var routingFingerprint [sha256.Size]byte
			copy(routingFingerprint[:], metadata.Get(historySpoolRoutingKey))
			configRevision := string(metadata.Get(historySpoolConfigRevisionKey))
			encoded, envelope, encodeErr := encodePreparedHistoryBatchEnvelope(
				spoolID,
				nextSequence,
				s.now().UTC(),
				routingFingerprint,
				configRevision,
				prepared,
				s.faults,
			)
			if encodeErr != nil {
				return encodeErr
			}
			key := encodeHistorySpoolUint64(nextSequence)
			if err = pending.Put(key, encoded); err != nil {
				return fmt.Errorf("%w: append pending batch", ErrSpoolIO)
			}
			if err = metadata.Put(
				historySpoolNextSequenceKey,
				encodeHistorySpoolUint64(nextSequence+1),
			); err != nil {
				return fmt.Errorf("%w: advance sequence", ErrSpoolIO)
			}
			if err = updateHistorySpoolQueueCounters(
				metadata,
				true,
				1,
				uint64(envelope.RecordCount),
				prepared.LogicalSize,
			); err != nil {
				return err
			}
			lossGeneration, readErr := readHistorySpoolUint64(
				metadata,
				historySpoolLossGenerationKey,
			)
			if readErr != nil {
				return readErr
			}
			lossDuringWrite := droppedBatches > 0
			if lossDuringWrite {
				lossGeneration, readErr = addHistorySpoolDrops(
					metadata,
					droppedBatches,
					droppedRecords,
					droppedBytes,
				)
				if readErr != nil {
					return readErr
				}
			}
			boundary = historyCommitBoundary{
				Sequence:        nextSequence,
				LossGeneration:  lossGeneration,
				Durable:         true,
				LossDuringWrite: lossDuringWrite,
			}
			if lossDuringWrite {
				if err = runHistorySpoolFault(
					s.faults,
					func(value *historySpoolFaults) func() error {
						return value.BeforeOverflowCommit
					},
				); err != nil {
					return err
				}
			}
			return runHistorySpoolFault(
				s.faults,
				func(value *historySpoolFaults) func() error {
					return value.BeforeAppendCommit
				},
			)
		},
	)
	if err != nil {
		return historyCommitBoundary{}, err
	}
	if err = runHistorySpoolFault(
		s.faults,
		func(value *historySpoolFaults) func() error {
			return value.AfterAppendCommit
		},
	); err != nil {
		return historyCommitBoundary{}, classifyHistorySpoolError(err)
	}
	return boundary, nil
}

func (s *historySpool) Lease() (*historyBatchEnvelope, error) {
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed || s.db == nil {
		return nil, ErrSpoolClosed
	}
	var leased *historyBatchEnvelope
	err := updateHistorySpoolDatabase(
		s.db,
		s.faults,
		"lease",
		func(tx *bbolt.Tx) error {
			metadata := tx.Bucket(historySpoolMetadataBucket)
			if err := advanceHistorySpoolRetired(tx); err != nil {
				return err
			}
			inflight := tx.Bucket(historySpoolInflightBucket)
			key, value := inflight.Cursor().First()
			if key != nil {
				envelope, err := decodeHistoryBatchEnvelopeWithFaults(
					value,
					mustHistorySpoolID(metadata),
					historySpoolSafeLogicalSize,
					s.faults,
				)
				if err != nil {
					return err
				}
				leased = &envelope
				return nil
			}
			pending := tx.Bucket(historySpoolPendingBucket)
			key, value = pending.Cursor().First()
			if key == nil {
				return nil
			}
			resolved, err := readHistorySpoolUint64(
				metadata,
				historySpoolResolvedSequenceKey,
			)
			if err != nil {
				return err
			}
			sequence := binary.BigEndian.Uint64(key)
			if sequence != resolved+1 {
				return fmt.Errorf("%w: delivery order is inconsistent", ErrSpoolCorrupt)
			}
			keyCopy := append([]byte(nil), key...)
			valueCopy := append([]byte(nil), value...)
			envelope, err := decodeHistoryBatchEnvelopeWithFaults(
				valueCopy,
				mustHistorySpoolID(metadata),
				historySpoolSafeLogicalSize,
				s.faults,
			)
			if err != nil {
				return err
			}
			if err = inflight.Put(keyCopy, valueCopy); err != nil {
				return fmt.Errorf("%w: lease batch", ErrSpoolIO)
			}
			if err = pending.Delete(keyCopy); err != nil {
				return fmt.Errorf("%w: remove leased batch", ErrSpoolIO)
			}
			leased = &envelope
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	if err = runHistorySpoolFault(
		s.faults,
		func(value *historySpoolFaults) func() error {
			return value.AfterLeaseCommit
		},
	); err != nil {
		return nil, classifyHistorySpoolError(err)
	}
	return leased, nil
}

func (s *historySpool) Acknowledge(sequence uint64, now time.Time) error {
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed || s.db == nil {
		return ErrSpoolClosed
	}
	err := updateHistorySpoolDatabase(
		s.db,
		s.faults,
		"acknowledge",
		func(tx *bbolt.Tx) error {
			metadata := tx.Bucket(historySpoolMetadataBucket)
			key := encodeHistorySpoolUint64(sequence)
			inflight := tx.Bucket(historySpoolInflightBucket)
			value := inflight.Get(key)
			if value == nil {
				return fmt.Errorf("%w: acknowledged batch is not inflight", ErrSpoolCorrupt)
			}
			envelope, err := decodeHistoryBatchEnvelopeWithFaults(
				value,
				mustHistorySpoolID(metadata),
				historySpoolSafeLogicalSize,
				s.faults,
			)
			if err != nil {
				return err
			}
			resolved, err := readHistorySpoolUint64(
				metadata,
				historySpoolResolvedSequenceKey,
			)
			if err != nil {
				return err
			}
			if sequence != resolved+1 {
				return fmt.Errorf("%w: acknowledgment order is inconsistent", ErrSpoolCorrupt)
			}
			if err = inflight.Delete(key); err != nil {
				return fmt.Errorf("%w: acknowledge batch", ErrSpoolIO)
			}
			if err = updateHistorySpoolQueueCounters(
				metadata,
				false,
				1,
				uint64(envelope.RecordCount),
				historySpoolLogicalSize(value),
			); err != nil {
				return err
			}
			if err = metadata.Put(
				historySpoolResolvedSequenceKey,
				encodeHistorySpoolUint64(sequence),
			); err != nil {
				return fmt.Errorf("%w: advance resolved sequence", ErrSpoolIO)
			}
			if err = advanceHistorySpoolRetired(tx); err != nil {
				return err
			}
			if err = metadata.Put(
				historySpoolLastSuccessfulFlushKey,
				encodeHistorySpoolInt64(now.UTC().UnixNano()),
			); err != nil {
				return fmt.Errorf("%w: record successful flush", ErrSpoolIO)
			}
			return runHistorySpoolFault(
				s.faults,
				func(value *historySpoolFaults) func() error {
					return value.BeforeAcknowledgeCommit
				},
			)
		},
	)
	if err != nil {
		return err
	}
	if err = runHistorySpoolFault(
		s.faults,
		func(value *historySpoolFaults) func() error {
			return value.AfterAcknowledgeCommit
		},
	); err != nil {
		return classifyHistorySpoolError(err)
	}
	return nil
}

func advanceHistorySpoolRetired(tx *bbolt.Tx) error {
	metadata := tx.Bucket(historySpoolMetadataBucket)
	resolved, err := readHistorySpoolUint64(
		metadata,
		historySpoolResolvedSequenceKey,
	)
	if err != nil {
		return err
	}
	retired := tx.Bucket(historySpoolRetiredBucket)
	for resolved != math.MaxUint64 {
		key := encodeHistorySpoolUint64(resolved + 1)
		if retired.Get(key) == nil {
			break
		}
		if err = retired.Delete(key); err != nil {
			return fmt.Errorf("%w: consume retired batch", ErrSpoolIO)
		}
		resolved++
	}
	if err = metadata.Put(
		historySpoolResolvedSequenceKey,
		encodeHistorySpoolUint64(resolved),
	); err != nil {
		return fmt.Errorf("%w: advance retired sequence", ErrSpoolIO)
	}
	return nil
}

func (s *historySpool) Status() historySpoolStatus {
	s.access.Lock()
	defer s.access.Unlock()
	status, _ := s.readStatusUnlocked()
	return status
}

func (s *historySpool) readStatus() (historySpoolStatus, error) {
	s.access.Lock()
	defer s.access.Unlock()
	return s.readStatusUnlocked()
}

func (s *historySpool) readStatusUnlocked() (historySpoolStatus, error) {
	if s.closed || s.db == nil {
		return historySpoolStatus{}, ErrSpoolClosed
	}
	var status historySpoolStatus
	err := viewHistorySpoolDatabase(
		s.db,
		s.faults,
		"status",
		func(tx *bbolt.Tx) error {
			var err error
			status, err = validateHistorySpoolTransaction(
				tx,
				historySpoolSafeLogicalSize,
				true,
				s.faults,
			)
			return err
		},
	)
	return status, err
}

func (s *historySpool) Close() error {
	s.access.Lock()
	defer s.access.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	if err != nil {
		return fmt.Errorf("%w: close database", ErrSpoolIO)
	}
	return nil
}

func runHistorySpoolFault(
	faults *historySpoolFaults,
	selectFault func(*historySpoolFaults) func() error,
) error {
	if faults == nil {
		return nil
	}
	fault := selectFault(faults)
	if fault == nil {
		return nil
	}
	return fault()
}

func runHistorySpoolOperationFault(
	faults *historySpoolFaults,
	operation string,
	selectFault func(*historySpoolFaults) func(string) error,
) error {
	if faults == nil {
		return nil
	}
	fault := selectFault(faults)
	if fault == nil {
		return nil
	}
	return fault(operation)
}

func updateHistorySpoolDatabase(
	database *bbolt.DB,
	faults *historySpoolFaults,
	operation string,
	update func(*bbolt.Tx) error,
) error {
	err := database.Update(func(tx *bbolt.Tx) error {
		if updateErr := update(tx); updateErr != nil {
			return updateErr
		}
		return runHistorySpoolOperationFault(
			faults,
			operation,
			func(value *historySpoolFaults) func(string) error {
				return value.TransactionWrite
			},
		)
	})
	return classifyHistorySpoolError(err)
}

func viewHistorySpoolDatabase(
	database *bbolt.DB,
	faults *historySpoolFaults,
	operation string,
	view func(*bbolt.Tx) error,
) error {
	err := database.View(func(tx *bbolt.Tx) error {
		if viewErr := view(tx); viewErr != nil {
			return viewErr
		}
		return runHistorySpoolOperationFault(
			faults,
			operation,
			func(value *historySpoolFaults) func(string) error {
				return value.View
			},
		)
	})
	return classifyHistorySpoolError(err)
}

func classifyHistorySpoolError(err error) error {
	if err == nil {
		return nil
	}
	for _, stable := range []error{
		ErrSpoolLocked,
		ErrSpoolCorrupt,
		ErrSpoolFuture,
		ErrSpoolIdentityMissing,
		ErrSpoolIdentityConflict,
		ErrSpoolIO,
		ErrSpoolClosed,
		ErrSpoolSequenceExhausted,
		ErrPostgresRevisionConflict,
	} {
		if errors.Is(err, stable) {
			return err
		}
	}
	if errors.Is(err, bbolt.ErrTimeout) {
		return fmt.Errorf("%w: %v", ErrSpoolLocked, err)
	}
	for _, corrupt := range []error{
		bbolt.ErrInvalid,
		bbolt.ErrVersionMismatch,
		bbolt.ErrChecksum,
		bbolt.ErrBucketNotFound,
		bbolt.ErrIncompatibleValue,
	} {
		if errors.Is(err, corrupt) {
			return fmt.Errorf("%w: %v", ErrSpoolCorrupt, err)
		}
	}
	return fmt.Errorf("%w: %v", ErrSpoolIO, err)
}

func updateHistorySpoolQueueCounters(
	metadata *bbolt.Bucket,
	increase bool,
	batches uint64,
	records uint64,
	bytesCount uint64,
) error {
	for _, item := range []struct {
		key   []byte
		delta uint64
	}{
		{historySpoolQueuedBatchesKey, batches},
		{historySpoolQueuedRecordsKey, records},
		{historySpoolQueuedBytesKey, bytesCount},
	} {
		current, err := readHistorySpoolUint64(metadata, item.key)
		if err != nil {
			return err
		}
		var next uint64
		if !increase {
			if item.delta > current {
				return fmt.Errorf("%w: queue counter underflow", ErrSpoolCorrupt)
			}
			next = current - item.delta
		} else {
			next, err = addHistorySpoolCounter(current, item.delta)
			if err != nil {
				return err
			}
		}
		if err = metadata.Put(item.key, encodeHistorySpoolUint64(next)); err != nil {
			return fmt.Errorf("%w: update queue counter", ErrSpoolIO)
		}
	}
	return nil
}

func addHistorySpoolDrops(
	metadata *bbolt.Bucket,
	batches uint64,
	records uint64,
	bytesCount uint64,
) (uint64, error) {
	for _, item := range []struct {
		key   []byte
		value uint64
	}{
		{historySpoolDroppedBatchesKey, batches},
		{historySpoolDroppedRecordsKey, records},
		{historySpoolDroppedBytesKey, bytesCount},
	} {
		current, err := readHistorySpoolUint64(metadata, item.key)
		if err != nil {
			return 0, err
		}
		next, err := addHistorySpoolCounter(current, item.value)
		if err != nil {
			return 0, err
		}
		if err = metadata.Put(item.key, encodeHistorySpoolUint64(next)); err != nil {
			return 0, fmt.Errorf("%w: update drop counter", ErrSpoolIO)
		}
	}
	generation, err := readHistorySpoolUint64(
		metadata,
		historySpoolLossGenerationKey,
	)
	if err != nil {
		return 0, err
	}
	if generation == math.MaxUint64 {
		return 0, fmt.Errorf("%w: loss generation overflow", ErrSpoolCorrupt)
	}
	generation++
	if err = metadata.Put(
		historySpoolLossGenerationKey,
		encodeHistorySpoolUint64(generation),
	); err != nil {
		return 0, fmt.Errorf("%w: update loss generation", ErrSpoolIO)
	}
	return generation, nil
}

func readHistorySpoolUint64(
	metadata *bbolt.Bucket,
	key []byte,
) (uint64, error) {
	value := metadata.Get(key)
	if len(value) != 8 {
		return 0, fmt.Errorf("%w: metadata integer is invalid", ErrSpoolCorrupt)
	}
	return binary.BigEndian.Uint64(value), nil
}

func mustHistorySpoolID(metadata *bbolt.Bucket) uuid.UUID {
	value := metadata.Get(historySpoolIDKey)
	identifier, _ := uuid.FromBytes(value)
	return identifier
}

func historySpoolLogicalSize(envelope []byte) uint64 {
	return 8 + uint64(len(envelope))
}

func addHistorySpoolCounter(left uint64, right uint64) (uint64, error) {
	if math.MaxUint64-left < right {
		return 0, fmt.Errorf("%w: counter overflow", ErrSpoolCorrupt)
	}
	return left + right, nil
}

func encodeHistorySpoolUint32(value uint32) []byte {
	encoded := make([]byte, 4)
	binary.BigEndian.PutUint32(encoded, value)
	return encoded
}

func encodeHistorySpoolUint64(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return encoded
}

func encodeHistorySpoolInt64(value int64) []byte {
	return encodeHistorySpoolUint64(uint64(value))
}

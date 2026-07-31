package trafficcontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/bbolt"
)

type trafficMigrationSource struct {
	path             string
	database         *bbolt.DB
	originalInfo     os.FileInfo
	lockedInfo       os.FileInfo
	fingerprint      [sha256.Size]byte
	size             int64
	revisionKey      []byte
	targetsFrom      time.Time
	destinationsFrom time.Time
	summaryRecords   uint64
	targetRecords    uint64
	revisions        map[string]trafficMigrationRevision
	maxBucket        int64
	maxRevisions     map[string]struct{}
	closeOnce        sync.Once
	closeErr         error
}

type trafficMigrationRecord struct {
	key       []byte
	value     []byte
	record    historyRecord
	groupPath []string
}

type trafficMigrationRevision struct {
	configRevision string
	firstBucket    int64
	lastBucket     int64
	summaryRecords uint64
	targetRecords  uint64
}

var trafficMigrationBucketPrefix = []byte("traffic_statistics_")

func openTrafficMigrationSource(
	ctx context.Context,
	path string,
) (*trafficMigrationSource, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, trafficMigrationSourceInvalid()
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, trafficMigrationSourceInvalid()
	}
	beforeFingerprint, beforeSize, err := fingerprintTrafficMigrationSource(ctx, path)
	if err != nil {
		return nil, trafficMigrationSourceInvalid()
	}
	database, err := bbolt.Open(path, 0o600, &bbolt.Options{
		Timeout:  time.Second,
		ReadOnly: false,
	})
	if err != nil {
		if errors.Is(err, bbolt.ErrTimeout) {
			return nil, ErrTrafficMigrationSourceInUse
		}
		return nil, trafficMigrationSourceInvalid()
	}
	source := &trafficMigrationSource{
		path:         path,
		database:     database,
		originalInfo: info,
		fingerprint:  beforeFingerprint,
		size:         beforeSize,
		revisions:    make(map[string]trafficMigrationRevision),
		maxRevisions: make(map[string]struct{}),
	}
	loaded := false
	defer func() {
		if !loaded {
			_ = database.Close()
		}
	}()
	lockedInfo, err := os.Lstat(path)
	if err != nil ||
		lockedInfo.Mode()&os.ModeSymlink != 0 ||
		!lockedInfo.Mode().IsRegular() ||
		!os.SameFile(info, lockedInfo) {
		return nil, ErrTrafficMigrationSourceChanged
	}
	source.lockedInfo = lockedInfo
	lockedFingerprint, lockedSize, err := fingerprintTrafficMigrationSource(ctx, path)
	if err != nil ||
		lockedFingerprint != beforeFingerprint ||
		lockedSize != beforeSize ||
		lockedInfo.Size() != beforeSize ||
		lockedInfo.Mode() != info.Mode() ||
		!lockedInfo.ModTime().Equal(info.ModTime()) {
		return nil, ErrTrafficMigrationSourceChanged
	}
	if err = source.load(ctx); err != nil {
		return nil, err
	}
	loaded = true
	return source, nil
}

func (s *trafficMigrationSource) load(ctx context.Context) error {
	err := s.database.View(func(tx *bbolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := validateTrafficMigrationBuckets(tx); err != nil {
			return err
		}
		metadata := tx.Bucket(historyMetadataBucket)
		if metadata == nil {
			return trafficMigrationSourceInvalid()
		}
		if err := validateTrafficMigrationMetadata(metadata); err != nil {
			return err
		}
		s.revisionKey = append([]byte(nil), metadata.Get(historyConfigRevisionKey)...)
		if len(s.revisionKey) != sha256.Size {
			return trafficMigrationSourceInvalid()
		}
		var err error
		s.targetsFrom, err = decodeTrafficMigrationAvailability(
			metadata.Get(historyTargetsFromKey),
		)
		if err != nil {
			return err
		}
		s.destinationsFrom, err = decodeTrafficMigrationAvailability(
			metadata.Get(historyDestinationsFromKey),
		)
		if err != nil || s.destinationsFrom.Before(s.targetsFrom) {
			return trafficMigrationSourceInvalid()
		}
		s.summaryRecords, err = s.validateRecords(
			ctx,
			tx.Bucket(historyBucket),
			"summary",
		)
		if err != nil {
			return err
		}
		s.targetRecords, err = s.validateRecords(
			ctx,
			tx.Bucket(historyTargetsBucket),
			"target",
		)
		return err
	})
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if errors.Is(err, ErrTrafficMigrationSourceInvalid) {
			return err
		}
		return trafficMigrationSourceInvalid()
	}
	return nil
}

func validateTrafficMigrationBuckets(tx *bbolt.Tx) error {
	return tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
		if bytes.Equal(name, historyBucket) ||
			bytes.Equal(name, historyTargetsBucket) ||
			bytes.Equal(name, historyMetadataBucket) {
			return nil
		}
		if bytes.HasPrefix(name, trafficMigrationBucketPrefix) {
			return trafficMigrationSourceInvalid()
		}
		return nil
	})
}

func validateTrafficMigrationMetadata(metadata *bbolt.Bucket) error {
	required := map[string]bool{
		string(historyConfigRevisionKey):   false,
		string(historyTargetsFromKey):      false,
		string(historyDestinationsFromKey): false,
	}
	err := metadata.ForEach(func(key []byte, value []byte) error {
		if value == nil {
			return trafficMigrationSourceInvalid()
		}
		name := string(key)
		if _, loaded := required[name]; !loaded {
			return trafficMigrationSourceInvalid()
		}
		required[name] = true
		return nil
	})
	if err != nil {
		return err
	}
	for _, present := range required {
		if !present {
			return trafficMigrationSourceInvalid()
		}
	}
	return nil
}

func decodeTrafficMigrationAvailability(value []byte) (time.Time, error) {
	if len(value) != 8 {
		return time.Time{}, trafficMigrationSourceInvalid()
	}
	availability := time.Unix(int64(binary.BigEndian.Uint64(value)), 0).UTC()
	if availability.Second() != 0 || availability.Nanosecond() != 0 {
		return time.Time{}, trafficMigrationSourceInvalid()
	}
	return availability, nil
}

func (s *trafficMigrationSource) validateRecords(
	ctx context.Context,
	bucket *bbolt.Bucket,
	kind string,
) (uint64, error) {
	if bucket == nil {
		return 0, nil
	}
	var count uint64
	cursor := bucket.Cursor()
	for rawKey, rawValue := cursor.First(); rawKey != nil; rawKey, rawValue = cursor.Next() {
		if count&255 == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		record, err := decodeTrafficMigrationRecord(rawKey, rawValue, kind)
		if err != nil {
			return 0, err
		}
		revision := s.revisions[record.record.ConfigRevision]
		revision.configRevision = record.record.ConfigRevision
		if revision.summaryRecords+revision.targetRecords == 0 ||
			record.record.Bucket < revision.firstBucket {
			revision.firstBucket = record.record.Bucket
		}
		if revision.summaryRecords+revision.targetRecords == 0 ||
			record.record.Bucket > revision.lastBucket {
			revision.lastBucket = record.record.Bucket
		}
		if kind == "summary" {
			revision.summaryRecords++
		} else {
			revision.targetRecords++
		}
		s.revisions[record.record.ConfigRevision] = revision
		if len(s.maxRevisions) == 0 || record.record.Bucket > s.maxBucket {
			s.maxBucket = record.record.Bucket
			clear(s.maxRevisions)
			s.maxRevisions[record.record.ConfigRevision] = struct{}{}
		} else if record.record.Bucket == s.maxBucket {
			s.maxRevisions[record.record.ConfigRevision] = struct{}{}
		}
		count++
	}
	return count, nil
}

func decodeTrafficMigrationRecord(
	rawKey []byte,
	rawValue []byte,
	kind string,
) (trafficMigrationRecord, error) {
	if len(rawKey) != 8+sha256.Size || rawValue == nil {
		return trafficMigrationRecord{}, trafficMigrationSourceInvalid()
	}
	decoder := json.NewDecoder(bytes.NewReader(rawValue))
	decoder.DisallowUnknownFields()
	var record historyRecord
	if err := decoder.Decode(&record); err != nil {
		return trafficMigrationRecord{}, trafficMigrationSourceInvalid()
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return trafficMigrationRecord{}, trafficMigrationSourceInvalid()
	}
	if record.Bucket != int64(binary.BigEndian.Uint64(rawKey[:8])) ||
		record.Bucket%int64(HistoryBucketInterval/time.Second) != 0 ||
		!trafficMigrationRevisionPattern.MatchString(record.ConfigRevision) {
		return trafficMigrationRecord{}, trafficMigrationSourceInvalid()
	}
	var groupPath []string
	groupDecoder := json.NewDecoder(strings.NewReader(record.GroupPath))
	if err := groupDecoder.Decode(&groupPath); err != nil {
		return trafficMigrationRecord{}, trafficMigrationSourceInvalid()
	}
	if err := groupDecoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return trafficMigrationRecord{}, trafficMigrationSourceInvalid()
	}
	if groupPath == nil {
		groupPath = []string{}
	}
	if kind == "summary" {
		if record.DestinationDomain != "" || record.DestinationIP != "" {
			return trafficMigrationRecord{}, trafficMigrationSourceInvalid()
		}
	} else if err := validateTrafficMigrationDestination(record); err != nil {
		return trafficMigrationRecord{}, err
	}
	key, err := encodeHistoryKey(historyKey{
		Bucket:             record.Bucket,
		ConfigRevision:     record.ConfigRevision,
		RouteTag:           record.RouteTag,
		GroupPath:          record.GroupPath,
		DestinationDomain:  record.DestinationDomain,
		DestinationIP:      record.DestinationIP,
		ActualOutboundTag:  record.ActualOutboundTag,
		ActualOutboundType: record.ActualOutboundType,
		Network:            record.Network,
	})
	if err != nil || !bytes.Equal(key, rawKey) {
		return trafficMigrationRecord{}, trafficMigrationSourceInvalid()
	}
	return trafficMigrationRecord{
		key:       append([]byte(nil), rawKey...),
		value:     append([]byte(nil), rawValue...),
		record:    record,
		groupPath: groupPath,
	}, nil
}

func validateTrafficMigrationDestination(record historyRecord) error {
	if record.DestinationDomain != "" && record.DestinationIP != "" {
		return trafficMigrationSourceInvalid()
	}
	if record.DestinationDomain != "" &&
		normalizeDestinationDomain(record.DestinationDomain) != record.DestinationDomain {
		return trafficMigrationSourceInvalid()
	}
	if record.DestinationIP != "" {
		address, err := netip.ParseAddr(record.DestinationIP)
		if err != nil || address.Unmap().String() != record.DestinationIP {
			return trafficMigrationSourceInvalid()
		}
	}
	return nil
}

func (s *trafficMigrationSource) readBatch(
	ctx context.Context,
	kind string,
	cursorStart []byte,
	from int64,
	to int64,
	limit int,
) ([]trafficMigrationRecord, bool, error) {
	var records []trafficMigrationRecord
	final := true
	err := s.database.View(func(tx *bbolt.Tx) error {
		bucketName := historyBucket
		if kind == "target" {
			bucketName = historyTargetsBucket
		}
		bucket := tx.Bucket(bucketName)
		if bucket == nil {
			return nil
		}
		cursor := bucket.Cursor()
		var rawKey []byte
		var rawValue []byte
		switch {
		case len(cursorStart) != 0:
			rawKey, rawValue = cursor.Seek(cursorStart)
			if bytes.Equal(rawKey, cursorStart) {
				rawKey, rawValue = cursor.Next()
			}
		case from < 0 && to <= 0:
			rawKey, rawValue = cursor.Seek(historyBucketKeyPrefix(from))
		case from < 0:
			rawKey, rawValue = cursor.First()
		default:
			rawKey, rawValue = cursor.Seek(historyBucketKeyPrefix(from))
		}
		for scanned := 0; rawKey != nil; rawKey, rawValue = cursor.Next() {
			if scanned&255 == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			scanned++
			if len(rawKey) != 8+sha256.Size {
				return trafficMigrationSourceInvalid()
			}
			bucketValue := int64(binary.BigEndian.Uint64(rawKey[:8]))
			if bucketValue >= to {
				if from >= 0 || to <= 0 {
					break
				}
				continue
			}
			if bucketValue < from {
				continue
			}
			record, err := decodeTrafficMigrationRecord(rawKey, rawValue, kind)
			if err != nil {
				return err
			}
			records = append(records, record)
			if len(records) > limit {
				records = records[:limit]
				final = false
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return records, final, nil
}

func (s *trafficMigrationSource) activeRevision(
	override string,
	emptyRevision string,
) (string, error) {
	if override != "" {
		if _, loaded := s.revisions[override]; !loaded {
			return "", trafficMigrationSourceInvalid()
		}
		return override, nil
	}
	if len(s.revisions) == 0 {
		return emptyRevision, nil
	}
	if len(s.maxRevisions) != 1 {
		return "", fmt.Errorf(
			"%w: latest bucket contains multiple config revisions",
			ErrTrafficMigrationSourceInvalid,
		)
	}
	for revision := range s.maxRevisions {
		return revision, nil
	}
	return "", trafficMigrationSourceInvalid()
}

func (s *trafficMigrationSource) closeAndVerify() error {
	s.closeOnce.Do(func() {
		if s.database != nil {
			s.closeErr = s.database.Close()
			s.database = nil
		}
		if s.closeErr != nil {
			s.closeErr = ErrTrafficMigrationSourceChanged
			return
		}
		info, err := os.Lstat(s.path)
		if err != nil ||
			info.Mode()&os.ModeSymlink != 0 ||
			!info.Mode().IsRegular() ||
			!os.SameFile(s.originalInfo, info) ||
			info.Size() != s.size ||
			info.Mode() != s.originalInfo.Mode() ||
			!info.ModTime().Equal(s.originalInfo.ModTime()) {
			s.closeErr = ErrTrafficMigrationSourceChanged
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		fingerprint, size, err := fingerprintTrafficMigrationSource(ctx, s.path)
		cancel()
		if err != nil || size != s.size || fingerprint != s.fingerprint {
			s.closeErr = ErrTrafficMigrationSourceChanged
		}
	})
	return s.closeErr
}

func fingerprintTrafficMigrationSource(
	ctx context.Context,
	path string,
) ([sha256.Size]byte, int64, error) {
	var fingerprint [sha256.Size]byte
	file, err := os.Open(path)
	if err != nil {
		return fingerprint, 0, err
	}
	defer file.Close()
	hash := sha256.New()
	buffer := make([]byte, 64*1024)
	var size int64
	for {
		if err = ctx.Err(); err != nil {
			return fingerprint, 0, err
		}
		var read int
		read, err = file.Read(buffer)
		if read > 0 {
			_, _ = hash.Write(buffer[:read])
			size += int64(read)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fingerprint, 0, err
		}
	}
	copy(fingerprint[:], hash.Sum(nil))
	return fingerprint, size, nil
}

func trafficMigrationSourceInvalid() error {
	return ErrTrafficMigrationSourceInvalid
}

func trafficMigrationRevisionBytes(revision string) []byte {
	value, _ := hex.DecodeString(revision)
	return value
}

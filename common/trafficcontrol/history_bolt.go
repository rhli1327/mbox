package trafficcontrol

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing/service/filemanager"
)

var (
	historyBucket              = []byte("traffic_statistics_v1")
	historyTargetsBucket       = []byte("traffic_statistics_targets_v1")
	historyMetadataBucket      = []byte("traffic_statistics_metadata_v1")
	historyConfigRevisionKey   = []byte("config_revision_hmac_key")
	historyTargetsFromKey      = []byte("target_available_from")
	historyDestinationsFromKey = []byte("destination_available_from")
)

var _ historyStore = (*boltStore)(nil)

type boltStore struct {
	ctx              context.Context
	path             string
	configContent    []byte
	configRevision   string
	targetsFrom      time.Time
	destinationsFrom time.Time
	db               *bbolt.DB
}

func newBoltStore(ctx context.Context, options HistoryOptions) *boltStore {
	path := options.Path
	if path == "" {
		path = "traffic.db"
	}
	return &boltStore{
		ctx:           ctx,
		path:          filemanager.BasePath(ctx, os.ExpandEnv(path)),
		configContent: append([]byte(nil), options.ConfigContent...),
	}
}

func (s *boltStore) Open() (historyStoreState, error) {
	parent := filepath.Dir(s.path)
	if parent != "." {
		err := filemanager.MkdirAll(s.ctx, parent, 0o755)
		if err != nil {
			return historyStoreState{}, err
		}
	}
	const fileMode = 0o600
	file, err := filemanager.OpenFile(s.ctx, s.path, os.O_RDWR|os.O_CREATE, fileMode)
	if err != nil {
		return historyStoreState{}, err
	}
	err = file.Close()
	if err != nil {
		return historyStoreState{}, err
	}
	db, err := bbolt.Open(s.path, fileMode, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return historyStoreState{}, err
	}
	err = filemanager.Chown(s.ctx, s.path)
	if err != nil {
		_ = db.Close()
		return historyStoreState{}, err
	}
	s.db = db
	err = s.initializeConfigRevision()
	if err == nil {
		err = s.initializeAvailability()
	}
	if err == nil {
		err = s.Cleanup(time.Now())
	}
	if err != nil {
		_ = db.Close()
		s.db = nil
		return historyStoreState{}, err
	}
	return historyStoreState{
		configRevision:   s.configRevision,
		targetsFrom:      s.targetsFrom,
		destinationsFrom: s.destinationsFrom,
	}, nil
}

func (s *boltStore) Close() error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func (s *boltStore) initializeConfigRevision() error {
	defer func() {
		for index := range s.configContent {
			s.configContent[index] = 0
		}
		s.configContent = nil
	}()
	var revisionKey []byte
	err := s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(historyMetadataBucket)
		if err != nil {
			return err
		}
		revisionKey = append([]byte(nil), bucket.Get(historyConfigRevisionKey)...)
		if len(revisionKey) == 0 {
			revisionKey = make([]byte, sha256.Size)
			_, err = rand.Read(revisionKey)
			if err != nil {
				return err
			}
			return bucket.Put(historyConfigRevisionKey, revisionKey)
		}
		if len(revisionKey) != sha256.Size {
			return errors.New("invalid traffic statistics revision key")
		}
		return nil
	})
	if err != nil {
		return err
	}
	revisionMAC := hmac.New(sha256.New, revisionKey)
	_, _ = revisionMAC.Write(s.configContent)
	s.configRevision = hex.EncodeToString(revisionMAC.Sum(nil)[:historyRevisionSize])
	return nil
}

func (s *boltStore) initializeAvailability() error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(historyMetadataBucket)
		if err != nil {
			return err
		}
		collectionFrom := time.Now().UTC().Truncate(HistoryBucketInterval).Add(HistoryBucketInterval)
		s.targetsFrom, err = initializeHistoryAvailability(
			bucket,
			historyTargetsFromKey,
			collectionFrom,
			"target",
		)
		if err != nil {
			return err
		}
		destinationCollectionFrom := collectionFrom
		if s.targetsFrom.After(destinationCollectionFrom) {
			destinationCollectionFrom = s.targetsFrom
		}
		s.destinationsFrom, err = initializeHistoryAvailability(
			bucket,
			historyDestinationsFromKey,
			destinationCollectionFrom,
			"destination",
		)
		if err != nil {
			return err
		}
		if s.destinationsFrom.Before(s.targetsFrom) {
			return errors.New("invalid traffic statistics destination availability time")
		}
		return nil
	})
}

func initializeHistoryAvailability(bucket *bbolt.Bucket, key []byte, fallback time.Time, name string) (time.Time, error) {
	content := bucket.Get(key)
	if len(content) == 0 {
		err := bucket.Put(key, historyBucketKeyPrefix(fallback.Unix()))
		return fallback, err
	}
	if len(content) != 8 {
		return time.Time{}, errors.New("invalid traffic statistics " + name + " availability time")
	}
	return time.Unix(int64(binary.BigEndian.Uint64(content)), 0).UTC(), nil
}

func (s *boltStore) Write(batch historyBatch) error {
	if s.db == nil {
		return errors.New("traffic statistics database is not open")
	}
	return s.db.Batch(func(tx *bbolt.Tx) error {
		summaryBucket, err := tx.CreateBucketIfNotExists(historyBucket)
		if err != nil {
			return err
		}
		targetsBucket, err := tx.CreateBucketIfNotExists(historyTargetsBucket)
		if err != nil {
			return err
		}
		for key, delta := range batch {
			summaryKey := key
			summaryKey.DestinationDomain = ""
			summaryKey.DestinationIP = ""
			err = putHistoryDelta(summaryBucket, summaryKey, delta)
			if err != nil {
				return err
			}
			if s.targetsFrom.IsZero() || key.Bucket >= s.targetsFrom.Unix() {
				err = putHistoryDelta(targetsBucket, key, delta)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func putHistoryDelta(bucket *bbolt.Bucket, key historyKey, delta historyCounters) error {
	diskKey, err := encodeHistoryKey(key)
	if err != nil {
		return err
	}
	record := historyRecordFrom(key, delta)
	if previous := bucket.Get(diskKey); previous != nil {
		err = json.Unmarshal(previous, &record)
		if err != nil {
			return err
		}
		record.UplinkBytes = saturatingAdd(record.UplinkBytes, delta.UplinkBytes)
		record.DownlinkBytes = saturatingAdd(record.DownlinkBytes, delta.DownlinkBytes)
		record.Connections = saturatingAdd(record.Connections, delta.Connections)
	}
	content, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return bucket.Put(diskKey, content)
}

func (s *boltStore) Cleanup(now time.Time) error {
	if s.db == nil {
		return nil
	}
	cutoff := now.Add(-HistoryRetention).UTC().Truncate(HistoryBucketInterval).Unix()
	return s.db.Update(func(tx *bbolt.Tx) error {
		for _, bucketName := range [][]byte{historyBucket, historyTargetsBucket} {
			bucket := tx.Bucket(bucketName)
			if bucket == nil {
				continue
			}
			cursor := bucket.Cursor()
			for key, _ := cursor.First(); key != nil; key, _ = cursor.Next() {
				if len(key) < 8 {
					continue
				}
				if int64(binary.BigEndian.Uint64(key[:8])) >= cutoff {
					break
				}
				err := cursor.Delete()
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *boltStore) BeginRead(context.Context) (historyStoreSnapshot, error) {
	if s.db == nil {
		return nil, errors.New("traffic statistics database is not open")
	}
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, err
	}
	return &boltStoreSnapshot{tx: tx}, nil
}

type boltStoreSnapshot struct {
	tx *bbolt.Tx
}

func (s *boltStoreSnapshot) Close() {
	_ = s.tx.Rollback()
}

func (s *boltStoreSnapshot) Query(
	ctx context.Context,
	query HistoryQuery,
	overlay historyQueryOverlay,
) (HistoryQueryResult, error) {
	routeTags := filterSet(query.RouteTags)
	groupTags := filterSet(query.GroupTags)
	actualOutboundTags := filterSet(query.ActualOutboundTags)
	destinations := filterSet(query.Destinations)
	destinationDomains := filterSet(query.DestinationDomains)
	networks := filterSet(query.Networks)
	useDestinations := query.GroupBy == HistoryGroupByDestination || len(query.Destinations) > 0
	useTargets := useDestinations ||
		query.GroupBy == HistoryGroupByDestinationDomain ||
		len(query.DestinationDomains) > 0
	detailAvailableFrom := overlay.targetAvailableFrom
	if useDestinations {
		detailAvailableFrom = overlay.destinationAvailableFrom
	}
	rows := make(map[historyDimensions]historyAggregate)
	merge := func(record historyRecord) error {
		bucketStart := time.Unix(record.Bucket, 0).UTC()
		bucketEnd := bucketStart.Add(HistoryBucketInterval)
		if useTargets && bucketStart.Before(detailAvailableFrom) {
			return nil
		}
		if !query.From.IsZero() && !bucketEnd.After(query.From) {
			return nil
		}
		if !query.To.IsZero() && !bucketStart.Before(query.To) {
			return nil
		}
		var groupPath []string
		err := json.Unmarshal([]byte(record.GroupPath), &groupPath)
		if err != nil {
			return err
		}
		if groupPath == nil {
			groupPath = []string{}
		}
		outboundGroup := lastGroupTag(groupPath)
		destinationDomain := normalizeDestinationDomain(record.DestinationDomain)
		destination, destinationType := preferredDestination(destinationDomain, record.DestinationIP)
		if !matchesFilterSet(record.RouteTag, routeTags) ||
			!matchesFilterSet(outboundGroup, groupTags) ||
			!matchesFilterSet(record.ActualOutboundTag, actualOutboundTags) ||
			!matchesFilterSet(destination, destinations) ||
			!matchesFilterSet(destinationDomain, destinationDomains) ||
			!matchesFilterSet(record.Network, networks) {
			return nil
		}

		dimensions, row := aggregateHistoryRecord(
			query.GroupBy,
			record,
			groupPath,
			destination,
			destinationType,
			destinationDomain,
			outboundGroup,
		)
		aggregate, loaded := rows[dimensions]
		if !loaded {
			aggregate.Row = row
		} else if query.GroupBy == HistoryGroupByActualOutbound &&
			aggregate.Row.ActualOutboundType != record.ActualOutboundType {
			aggregate.Row.ActualOutboundType = ""
		}
		aggregate.Row.UplinkBytes = saturatingAdd(aggregate.Row.UplinkBytes, record.UplinkBytes)
		aggregate.Row.DownlinkBytes = saturatingAdd(aggregate.Row.DownlinkBytes, record.DownlinkBytes)
		aggregate.Row.Connections = saturatingAdd(aggregate.Row.Connections, record.Connections)
		if aggregate.ActualFrom.IsZero() || bucketStart.Before(aggregate.ActualFrom) {
			aggregate.ActualFrom = bucketStart
		}
		if bucketEnd.After(aggregate.ActualTo) {
			aggregate.ActualTo = bucketEnd
		}
		rows[dimensions] = aggregate
		return nil
	}

	selectedBucket := historyBucket
	if useTargets {
		selectedBucket = historyTargetsBucket
	}
	bucket := s.tx.Bucket(selectedBucket)
	if bucket != nil {
		cursor := bucket.Cursor()
		var key, content []byte
		scanFrom := query.From
		if useTargets && (scanFrom.IsZero() || detailAvailableFrom.After(scanFrom)) {
			scanFrom = detailAvailableFrom
		}
		if scanFrom.IsZero() || scanFrom.Unix() < 0 {
			key, content = cursor.First()
		} else {
			key, content = cursor.Seek(historyBucketKeyPrefix(scanFrom.UTC().Truncate(HistoryBucketInterval).Unix()))
		}
		for scanned := 0; key != nil; key, content = cursor.Next() {
			if scanned&255 == 0 {
				if err := ctx.Err(); err != nil {
					return HistoryQueryResult{}, err
				}
			}
			scanned++
			if len(key) < 8 {
				continue
			}
			bucketTime := time.Unix(int64(binary.BigEndian.Uint64(key[:8])), 0)
			if !query.To.IsZero() && !bucketTime.Before(query.To) {
				break
			}
			var record historyRecord
			err := json.Unmarshal(content, &record)
			if err != nil {
				return HistoryQueryResult{}, err
			}
			err = merge(record)
			if err != nil {
				return HistoryQueryResult{}, err
			}
		}
	}
	scanned := 0
	for key, counters := range overlay.pending {
		if scanned&255 == 0 {
			if err := ctx.Err(); err != nil {
				return HistoryQueryResult{}, err
			}
		}
		scanned++
		if useTargets && key.Bucket < detailAvailableFrom.Unix() {
			continue
		}
		record := historyRecordFrom(key, counters)
		if !useTargets {
			record.DestinationDomain = ""
			record.DestinationIP = ""
		}
		err := merge(record)
		if err != nil {
			return HistoryQueryResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return HistoryQueryResult{}, err
	}
	resultRows := make([]HistoryRow, 0, len(rows))
	var totals HistoryCounters
	var actualFrom, actualTo time.Time
	search := strings.ToLower(query.Search)
	visited := 0
	for _, aggregate := range rows {
		if visited&255 == 0 {
			if err := ctx.Err(); err != nil {
				return HistoryQueryResult{}, err
			}
		}
		visited++
		row := aggregate.Row
		if search != "" && !strings.Contains(strings.ToLower(historyRowLabel(query.GroupBy, row)), search) {
			continue
		}
		resultRows = append(resultRows, row)
		totals.UplinkBytes = saturatingAdd(totals.UplinkBytes, row.UplinkBytes)
		totals.DownlinkBytes = saturatingAdd(totals.DownlinkBytes, row.DownlinkBytes)
		totals.Connections = saturatingAdd(totals.Connections, row.Connections)
		if actualFrom.IsZero() || aggregate.ActualFrom.Before(actualFrom) {
			actualFrom = aggregate.ActualFrom
		}
		if aggregate.ActualTo.After(actualTo) {
			actualTo = aggregate.ActualTo
		}
	}
	sort.Slice(resultRows, func(i, j int) bool {
		primary := compareHistoryRows(query.GroupBy, query.SortBy, resultRows[i], resultRows[j])
		if primary != 0 {
			if query.SortOrder == HistorySortOrderDescending {
				return primary > 0
			}
			return primary < 0
		}
		return compareHistoryRowsCanonical(query.GroupBy, resultRows[i], resultRows[j]) < 0
	})

	totalRows := len(resultRows)
	if query.Page > 1 && query.Page-1 > len(resultRows)/query.PageSize {
		resultRows = []HistoryRow{}
	} else {
		pageStart := (query.Page - 1) * query.PageSize
		pageEnd := min(pageStart+query.PageSize, len(resultRows))
		resultRows = resultRows[pageStart:pageEnd]
	}
	return HistoryQueryResult{
		TargetAvailableFrom:      overlay.targetAvailableFrom,
		DestinationAvailableFrom: overlay.destinationAvailableFrom,
		ActualFrom:               actualFrom,
		ActualTo:                 actualTo,
		GroupBy:                  query.GroupBy,
		Page:                     query.Page,
		PageSize:                 query.PageSize,
		TotalRows:                totalRows,
		Totals:                   totals,
		Rows:                     resultRows,
	}, nil
}

func encodeHistoryKey(key historyKey) ([]byte, error) {
	content, err := json.Marshal(key)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(content)
	encoded := make([]byte, 8+len(hash))
	binary.BigEndian.PutUint64(encoded, uint64(key.Bucket))
	copy(encoded[8:], hash[:])
	return encoded, nil
}

func historyBucketKeyPrefix(bucket int64) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(bucket))
}

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
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/service/filemanager"
)

const (
	HistoryBucketInterval = time.Minute
	HistoryRetention      = 30 * 24 * time.Hour
	historyFlushInterval  = 5 * time.Second
	historyQueryLimit     = 500
	historyQueryLimitMax  = 5000
	historyQueryParallel  = 2
	historyRevisionSize   = 16
)

var (
	historyBucket            = []byte("traffic_statistics_v1")
	historyMetadataBucket    = []byte("traffic_statistics_metadata_v1")
	historyConfigRevisionKey = []byte("config_revision_hmac_key")
)

type HistoryOptions struct {
	Path          string
	ConfigContent []byte
}

type HistoryQuery struct {
	From               time.Time
	To                 time.Time
	RouteTags          []string
	ActualOutboundTags []string
	Networks           []string
	Limit              int
}

type HistoryRow struct {
	ConfigRevision     string
	RouteTag           string
	GroupPath          []string
	ActualOutboundTag  string
	ActualOutboundType string
	Network            string
	UplinkBytes        uint64
	DownlinkBytes      uint64
	Connections        uint64
}

type HistoryQueryResult struct {
	ActualFrom time.Time
	ActualTo   time.Time
	Totals     HistoryCounters
	Rows       []HistoryRow
	Truncated  bool
}

type HistoryCounters struct {
	UplinkBytes   uint64
	DownlinkBytes uint64
	Connections   uint64
}

type historyKey struct {
	Bucket             int64
	ConfigRevision     string
	RouteTag           string
	GroupPath          string
	ActualOutboundTag  string
	ActualOutboundType string
	Network            string
}

type historyCounters struct {
	UplinkBytes   uint64 `json:"uplink_bytes"`
	DownlinkBytes uint64 `json:"downlink_bytes"`
	Connections   uint64 `json:"connections"`
}

type historyDimensions struct {
	ConfigRevision     string
	RouteTag           string
	GroupPath          string
	ActualOutboundTag  string
	ActualOutboundType string
	Network            string
}

type historyRecord struct {
	Bucket             int64  `json:"bucket"`
	ConfigRevision     string `json:"config_revision"`
	RouteTag           string `json:"route_tag"`
	GroupPath          string `json:"group_path"`
	ActualOutboundTag  string `json:"actual_outbound_tag"`
	ActualOutboundType string `json:"actual_outbound_type"`
	Network            string `json:"network"`
	historyCounters
}

var _ adapter.LifecycleService = (*History)(nil)

type History struct {
	ctx            context.Context
	logger         log.ContextLogger
	path           string
	configContent  []byte
	configRevision string

	databaseAccess sync.RWMutex
	flushAccess    sync.Mutex
	access         sync.Mutex
	accepting      bool
	pending        map[historyKey]historyCounters
	db             *bbolt.DB
	queryAccess    chan struct{}

	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
	loopWait  sync.WaitGroup
}

func NewHistory(ctx context.Context, logger log.ContextLogger, options HistoryOptions) *History {
	path := options.Path
	if path == "" {
		path = "traffic.db"
	}
	return &History{
		ctx:           ctx,
		logger:        logger,
		path:          filemanager.BasePath(ctx, os.ExpandEnv(path)),
		configContent: append([]byte(nil), options.ConfigContent...),
		accepting:     true,
		pending:       make(map[historyKey]historyCounters),
		queryAccess:   make(chan struct{}, historyQueryParallel),
		done:          make(chan struct{}),
	}
}

func (h *History) Name() string {
	return "traffic statistics"
}

func (h *History) Start(stage adapter.StartStage) error {
	switch stage {
	case adapter.StartStateInitialize:
		return h.open()
	case adapter.StartStateStart:
		h.loopWait.Add(1)
		go h.loop()
	}
	return nil
}

func (h *History) open() error {
	parent := filepath.Dir(h.path)
	if parent != "." {
		err := filemanager.MkdirAll(h.ctx, parent, 0o755)
		if err != nil {
			return err
		}
	}
	const fileMode = 0o600
	file, err := filemanager.OpenFile(h.ctx, h.path, os.O_RDWR|os.O_CREATE, fileMode)
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return err
	}
	db, err := bbolt.Open(h.path, fileMode, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		return err
	}
	err = filemanager.Chown(h.ctx, h.path)
	if err != nil {
		db.Close()
		return err
	}
	h.db = db
	err = h.initializeConfigRevision()
	if err == nil {
		err = h.cleanup(time.Now())
	}
	if err != nil {
		db.Close()
		h.db = nil
		return err
	}
	return nil
}

func (h *History) initializeConfigRevision() error {
	defer func() {
		for index := range h.configContent {
			h.configContent[index] = 0
		}
		h.configContent = nil
	}()
	var revisionKey []byte
	err := h.db.Update(func(tx *bbolt.Tx) error {
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
	_, _ = revisionMAC.Write(h.configContent)
	h.configRevision = hex.EncodeToString(revisionMAC.Sum(nil)[:historyRevisionSize])
	return nil
}

func (h *History) loop() {
	defer h.loopWait.Done()
	flushTicker := time.NewTicker(historyFlushInterval)
	cleanupTicker := time.NewTicker(time.Hour)
	defer flushTicker.Stop()
	defer cleanupTicker.Stop()
	for {
		select {
		case <-flushTicker.C:
			if err := h.flush(); err != nil {
				h.logger.Error("flush traffic statistics: ", err)
			}
		case now := <-cleanupTicker.C:
			if err := h.cleanup(now); err != nil {
				h.logger.Error("clean traffic statistics: ", err)
			}
		case <-h.done:
			return
		}
	}
}

func (h *History) Close() error {
	h.closeOnce.Do(func() {
		h.closeErr = h.close()
	})
	return h.closeErr
}

func (h *History) close() error {
	close(h.done)
	h.loopWait.Wait()

	h.databaseAccess.Lock()
	defer h.databaseAccess.Unlock()
	h.flushAccess.Lock()
	defer h.flushAccess.Unlock()
	h.access.Lock()
	h.accepting = false
	h.access.Unlock()

	// Do not restore a failed final batch: after Close returns the history has
	// no writable database and must not retain an unreachable pending tail.
	flushErr := h.flushLocked(false)
	if h.db == nil {
		return flushErr
	}
	closeErr := h.db.Close()
	h.db = nil
	h.access.Lock()
	clear(h.pending)
	h.access.Unlock()
	return errors.Join(flushErr, closeErr)
}

func (h *History) RecordDelta(metadata *TrackerMetadata, uplink int64, downlink int64, newConnection bool) {
	if metadata == nil || uplink < 0 || downlink < 0 || !newConnection && uplink == 0 && downlink == 0 {
		return
	}
	var (
		routeTag           string
		groupPath          []string
		actualOutboundTag  string
		actualOutboundType string
	)
	if metadata.Trace != nil {
		trace := metadata.Trace.Snapshot()
		routeTag = trace.RouteTag
		groupPath = trace.GroupPath
		actualOutboundTag = trace.ActualOutboundTag
		actualOutboundType = trace.ActualOutboundType
	} else {
		routeTag = metadata.Outbound
		actualOutboundTag = metadata.Outbound
		actualOutboundType = metadata.OutboundType
	}
	if groupPath == nil {
		groupPath = []string{}
	}
	groupPathContent, _ := json.Marshal(groupPath)
	key := historyKey{
		Bucket:             time.Now().UTC().Truncate(HistoryBucketInterval).Unix(),
		ConfigRevision:     h.configRevision,
		RouteTag:           routeTag,
		GroupPath:          string(groupPathContent),
		ActualOutboundTag:  actualOutboundTag,
		ActualOutboundType: actualOutboundType,
		Network:            metadata.Metadata.Network,
	}
	h.access.Lock()
	if !h.accepting {
		h.access.Unlock()
		return
	}
	counters := h.pending[key]
	counters.UplinkBytes = saturatingAdd(counters.UplinkBytes, uint64(uplink))
	counters.DownlinkBytes = saturatingAdd(counters.DownlinkBytes, uint64(downlink))
	if newConnection {
		counters.Connections = saturatingAdd(counters.Connections, 1)
	}
	h.pending[key] = counters
	h.access.Unlock()
}

func (h *History) flush() error {
	h.databaseAccess.RLock()
	defer h.databaseAccess.RUnlock()
	h.flushAccess.Lock()
	defer h.flushAccess.Unlock()
	return h.flushLocked(true)
}

func (h *History) flushLocked(restoreOnError bool) error {
	h.access.Lock()
	if len(h.pending) == 0 {
		h.access.Unlock()
		return nil
	}
	pending := h.pending
	h.pending = make(map[historyKey]historyCounters)
	h.access.Unlock()
	if h.db == nil {
		if restoreOnError {
			h.restorePending(pending)
		}
		return errors.New("traffic statistics database is not open")
	}
	err := h.db.Batch(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(historyBucket)
		if err != nil {
			return err
		}
		for key, delta := range pending {
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
			err = bucket.Put(diskKey, content)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil && restoreOnError {
		h.restorePending(pending)
	}
	return err
}

func (h *History) restorePending(pending map[historyKey]historyCounters) {
	h.access.Lock()
	defer h.access.Unlock()
	if !h.accepting {
		return
	}
	for key, counters := range pending {
		current := h.pending[key]
		current.UplinkBytes = saturatingAdd(current.UplinkBytes, counters.UplinkBytes)
		current.DownlinkBytes = saturatingAdd(current.DownlinkBytes, counters.DownlinkBytes)
		current.Connections = saturatingAdd(current.Connections, counters.Connections)
		h.pending[key] = current
	}
}

func (h *History) Query(ctx context.Context, query HistoryQuery) (HistoryQueryResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case h.queryAccess <- struct{}{}:
		defer func() { <-h.queryAccess }()
	case <-ctx.Done():
		return HistoryQueryResult{}, ctx.Err()
	}

	h.databaseAccess.RLock()
	defer h.databaseAccess.RUnlock()

	// Taking the Bolt read transaction and copying pending deltas under the
	// flush lock establishes one consistent query boundary. Once the
	// transaction exists, a later flush can proceed without changing its
	// database snapshot.
	h.flushAccess.Lock()
	if h.db == nil {
		h.flushAccess.Unlock()
		return HistoryQueryResult{}, errors.New("traffic statistics database is not open")
	}
	tx, err := h.db.Begin(false)
	if err != nil {
		h.flushAccess.Unlock()
		return HistoryQueryResult{}, err
	}
	h.access.Lock()
	pending := make(map[historyKey]historyCounters, len(h.pending))
	for key, counters := range h.pending {
		pending[key] = counters
	}
	h.access.Unlock()
	h.flushAccess.Unlock()
	defer tx.Rollback()

	routeTags := filterSet(query.RouteTags)
	actualOutboundTags := filterSet(query.ActualOutboundTags)
	networks := filterSet(query.Networks)
	rows := make(map[historyDimensions]HistoryRow)
	var totals HistoryCounters
	var actualFrom, actualTo time.Time
	merge := func(record historyRecord) error {
		bucketStart := time.Unix(record.Bucket, 0).UTC()
		bucketEnd := bucketStart.Add(HistoryBucketInterval)
		if !query.From.IsZero() && !bucketEnd.After(query.From) {
			return nil
		}
		if !query.To.IsZero() && !bucketStart.Before(query.To) {
			return nil
		}
		if !matchesFilterSet(record.RouteTag, routeTags) ||
			!matchesFilterSet(record.ActualOutboundTag, actualOutboundTags) ||
			!matchesFilterSet(record.Network, networks) {
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
		dimensions := historyDimensions{
			ConfigRevision:     record.ConfigRevision,
			RouteTag:           record.RouteTag,
			GroupPath:          record.GroupPath,
			ActualOutboundTag:  record.ActualOutboundTag,
			ActualOutboundType: record.ActualOutboundType,
			Network:            record.Network,
		}
		row, loaded := rows[dimensions]
		if !loaded {
			row = HistoryRow{
				ConfigRevision:     record.ConfigRevision,
				RouteTag:           record.RouteTag,
				GroupPath:          groupPath,
				ActualOutboundTag:  record.ActualOutboundTag,
				ActualOutboundType: record.ActualOutboundType,
				Network:            record.Network,
			}
		}
		row.UplinkBytes = saturatingAdd(row.UplinkBytes, record.UplinkBytes)
		row.DownlinkBytes = saturatingAdd(row.DownlinkBytes, record.DownlinkBytes)
		row.Connections = saturatingAdd(row.Connections, record.Connections)
		rows[dimensions] = row
		totals.UplinkBytes = saturatingAdd(totals.UplinkBytes, record.UplinkBytes)
		totals.DownlinkBytes = saturatingAdd(totals.DownlinkBytes, record.DownlinkBytes)
		totals.Connections = saturatingAdd(totals.Connections, record.Connections)
		if actualFrom.IsZero() || bucketStart.Before(actualFrom) {
			actualFrom = bucketStart
		}
		if bucketEnd.After(actualTo) {
			actualTo = bucketEnd
		}
		return nil
	}

	bucket := tx.Bucket(historyBucket)
	if bucket != nil {
		cursor := bucket.Cursor()
		var key, content []byte
		if query.From.IsZero() || query.From.Unix() < 0 {
			key, content = cursor.First()
		} else {
			key, content = cursor.Seek(historyBucketKeyPrefix(query.From.UTC().Truncate(HistoryBucketInterval).Unix()))
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
	for key, counters := range pending {
		if scanned&255 == 0 {
			if err := ctx.Err(); err != nil {
				return HistoryQueryResult{}, err
			}
		}
		scanned++
		err = merge(historyRecordFrom(key, counters))
		if err != nil {
			return HistoryQueryResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return HistoryQueryResult{}, err
	}
	resultRows := make([]HistoryRow, 0, len(rows))
	materialized := 0
	for _, row := range rows {
		if materialized&255 == 0 {
			if err := ctx.Err(); err != nil {
				return HistoryQueryResult{}, err
			}
		}
		resultRows = append(resultRows, row)
		materialized++
	}
	sort.Slice(resultRows, func(i, j int) bool {
		leftTotal := saturatingAdd(resultRows[i].UplinkBytes, resultRows[i].DownlinkBytes)
		rightTotal := saturatingAdd(resultRows[j].UplinkBytes, resultRows[j].DownlinkBytes)
		if leftTotal != rightTotal {
			return leftTotal > rightTotal
		}
		if resultRows[i].RouteTag != resultRows[j].RouteTag {
			return resultRows[i].RouteTag < resultRows[j].RouteTag
		}
		if groupPathOrder := slices.Compare(resultRows[i].GroupPath, resultRows[j].GroupPath); groupPathOrder != 0 {
			return groupPathOrder < 0
		}
		if resultRows[i].ActualOutboundTag != resultRows[j].ActualOutboundTag {
			return resultRows[i].ActualOutboundTag < resultRows[j].ActualOutboundTag
		}
		if resultRows[i].ActualOutboundType != resultRows[j].ActualOutboundType {
			return resultRows[i].ActualOutboundType < resultRows[j].ActualOutboundType
		}
		if resultRows[i].Network != resultRows[j].Network {
			return resultRows[i].Network < resultRows[j].Network
		}
		return resultRows[i].ConfigRevision < resultRows[j].ConfigRevision
	})
	limit := query.Limit
	if limit <= 0 {
		limit = historyQueryLimit
	} else if limit > historyQueryLimitMax {
		limit = historyQueryLimitMax
	}
	truncated := len(resultRows) > limit
	if truncated {
		resultRows = resultRows[:limit]
	}
	return HistoryQueryResult{
		ActualFrom: actualFrom,
		ActualTo:   actualTo,
		Totals:     totals,
		Rows:       resultRows,
		Truncated:  truncated,
	}, nil
}

func (h *History) cleanup(now time.Time) error {
	h.databaseAccess.RLock()
	defer h.databaseAccess.RUnlock()
	h.flushAccess.Lock()
	defer h.flushAccess.Unlock()
	if h.db == nil {
		return nil
	}
	cutoff := now.Add(-HistoryRetention).UTC().Truncate(HistoryBucketInterval).Unix()
	return h.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(historyBucket)
		if bucket == nil {
			return nil
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
		return nil
	})
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

func historyRecordFrom(key historyKey, counters historyCounters) historyRecord {
	return historyRecord{
		Bucket:             key.Bucket,
		ConfigRevision:     key.ConfigRevision,
		RouteTag:           key.RouteTag,
		GroupPath:          key.GroupPath,
		ActualOutboundTag:  key.ActualOutboundTag,
		ActualOutboundType: key.ActualOutboundType,
		Network:            key.Network,
		historyCounters:    counters,
	}
}

func filterSet(filter []string) map[string]struct{} {
	if len(filter) == 0 {
		return nil
	}
	result := make(map[string]struct{}, len(filter))
	for _, value := range filter {
		result[value] = struct{}{}
	}
	return result
}

func matchesFilterSet(value string, filter map[string]struct{}) bool {
	if len(filter) == 0 {
		return true
	}
	_, loaded := filter[value]
	return loaded
}

func historyBucketKeyPrefix(bucket int64) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(bucket))
}

func saturatingAdd(left uint64, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

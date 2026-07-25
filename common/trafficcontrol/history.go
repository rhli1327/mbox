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
	"strings"
	"sync"
	"time"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/service/filemanager"
)

const (
	HistoryBucketInterval  = time.Minute
	HistoryRetention       = 30 * 24 * time.Hour
	historyFlushInterval   = 5 * time.Second
	HistoryPageSizeDefault = 50
	HistoryPageSizeMax     = 200
	historyQueryParallel   = 2
	historyRevisionSize    = 16

	HistoryGroupByRoutePath         = "route_path"
	HistoryGroupByDestinationDomain = "destination_domain"
	HistoryGroupByOutboundGroup     = "outbound_group"
	HistoryGroupByActualOutbound    = "actual_outbound"

	HistorySortByName          = "name"
	HistorySortByTotalBytes    = "total_bytes"
	HistorySortByUplinkBytes   = "uplink_bytes"
	HistorySortByDownlinkBytes = "downlink_bytes"
	HistorySortByConnections   = "connections"

	HistorySortOrderAscending  = "asc"
	HistorySortOrderDescending = "desc"
)

var (
	historyBucket            = []byte("traffic_statistics_v1")
	historyTargetsBucket     = []byte("traffic_statistics_targets_v1")
	historyMetadataBucket    = []byte("traffic_statistics_metadata_v1")
	historyConfigRevisionKey = []byte("config_revision_hmac_key")
	historyTargetsFromKey    = []byte("target_available_from")
)

type HistoryOptions struct {
	Path          string
	ConfigContent []byte
}

type HistoryQuery struct {
	From               time.Time
	To                 time.Time
	RouteTags          []string
	GroupTags          []string
	ActualOutboundTags []string
	DestinationDomains []string
	Networks           []string
	GroupBy            string
	Page               int
	PageSize           int
	SortBy             string
	SortOrder          string
	Search             string
}

type HistoryRow struct {
	ConfigRevision     string
	RouteTag           string
	GroupPath          []string
	DestinationDomain  string
	OutboundGroup      string
	ActualOutboundTag  string
	ActualOutboundType string
	Network            string
	UplinkBytes        uint64
	DownlinkBytes      uint64
	Connections        uint64
}

type HistoryQueryResult struct {
	TargetAvailableFrom time.Time
	ActualFrom          time.Time
	ActualTo            time.Time
	GroupBy             string
	Page                int
	PageSize            int
	TotalRows           int
	Totals              HistoryCounters
	Rows                []HistoryRow
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
	DestinationDomain  string `json:",omitempty"`
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
	DestinationDomain  string
	OutboundGroup      string
	ActualOutboundTag  string
	ActualOutboundType string
	Network            string
}

type historyRecord struct {
	Bucket             int64  `json:"bucket"`
	ConfigRevision     string `json:"config_revision"`
	RouteTag           string `json:"route_tag"`
	GroupPath          string `json:"group_path"`
	DestinationDomain  string `json:"destination_domain,omitempty"`
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
	targetsFrom    time.Time

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
		err = h.initializeTargetsFrom()
	}
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

func (h *History) initializeTargetsFrom() error {
	return h.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(historyMetadataBucket)
		if err != nil {
			return err
		}
		content := bucket.Get(historyTargetsFromKey)
		if len(content) == 0 {
			h.targetsFrom = time.Now().UTC().Truncate(HistoryBucketInterval).Add(HistoryBucketInterval)
			return bucket.Put(historyTargetsFromKey, historyBucketKeyPrefix(h.targetsFrom.Unix()))
		}
		if len(content) != 8 {
			return errors.New("invalid traffic statistics target availability time")
		}
		h.targetsFrom = time.Unix(int64(binary.BigEndian.Uint64(content)), 0).UTC()
		return nil
	})
}

func (h *History) TargetAvailableFrom() time.Time {
	return h.targetAvailableFromAt(time.Now())
}

func (h *History) targetAvailableFromAt(now time.Time) time.Time {
	if h.targetsFrom.IsZero() {
		return time.Time{}
	}
	retentionStart := now.Add(-HistoryRetention).UTC().Truncate(HistoryBucketInterval)
	if h.targetsFrom.Before(retentionStart) {
		return retentionStart
	}
	return h.targetsFrom
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
		DestinationDomain:  normalizeDestinationDomain(metadata.DestinationDomain),
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
		summaryBucket, err := tx.CreateBucketIfNotExists(historyBucket)
		if err != nil {
			return err
		}
		targetsBucket, err := tx.CreateBucketIfNotExists(historyTargetsBucket)
		if err != nil {
			return err
		}
		for key, delta := range pending {
			summaryKey := key
			summaryKey.DestinationDomain = ""
			err = putHistoryDelta(summaryBucket, summaryKey, delta)
			if err != nil {
				return err
			}
			if h.targetsFrom.IsZero() || key.Bucket >= h.targetsFrom.Unix() {
				err = putHistoryDelta(targetsBucket, key, delta)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil && restoreOnError {
		h.restorePending(pending)
	}
	return err
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

type historyAggregate struct {
	Row        HistoryRow
	ActualFrom time.Time
	ActualTo   time.Time
}

func (h *History) Query(ctx context.Context, query HistoryQuery) (HistoryQueryResult, error) {
	query, err := normalizeHistoryQuery(query)
	if err != nil {
		return HistoryQueryResult{}, err
	}
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
	targetAvailableFrom := h.targetAvailableFromAt(time.Now())

	routeTags := filterSet(query.RouteTags)
	groupTags := filterSet(query.GroupTags)
	actualOutboundTags := filterSet(query.ActualOutboundTags)
	destinationDomains := filterSet(query.DestinationDomains)
	networks := filterSet(query.Networks)
	useTargets := query.GroupBy == HistoryGroupByDestinationDomain || len(query.DestinationDomains) > 0
	rows := make(map[historyDimensions]historyAggregate)
	merge := func(record historyRecord) error {
		bucketStart := time.Unix(record.Bucket, 0).UTC()
		bucketEnd := bucketStart.Add(HistoryBucketInterval)
		if useTargets && bucketStart.Before(targetAvailableFrom) {
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
		if !matchesFilterSet(record.RouteTag, routeTags) ||
			!matchesFilterSet(outboundGroup, groupTags) ||
			!matchesFilterSet(record.ActualOutboundTag, actualOutboundTags) ||
			!matchesFilterSet(destinationDomain, destinationDomains) ||
			!matchesFilterSet(record.Network, networks) {
			return nil
		}

		dimensions, row := aggregateHistoryRecord(query.GroupBy, record, groupPath, destinationDomain, outboundGroup)
		aggregate, loaded := rows[dimensions]
		if !loaded {
			aggregate.Row = row
		} else if query.GroupBy == HistoryGroupByActualOutbound &&
			aggregate.Row.ActualOutboundType != record.ActualOutboundType {
			// Node aggregation is keyed by tag. An empty type explicitly marks
			// that the tag represented more than one type in the selected range.
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
	bucket := tx.Bucket(selectedBucket)
	if bucket != nil {
		cursor := bucket.Cursor()
		var key, content []byte
		scanFrom := query.From
		if useTargets && (scanFrom.IsZero() || targetAvailableFrom.After(scanFrom)) {
			scanFrom = targetAvailableFrom
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
	for key, counters := range pending {
		if scanned&255 == 0 {
			if err := ctx.Err(); err != nil {
				return HistoryQueryResult{}, err
			}
		}
		scanned++
		if useTargets && key.Bucket < targetAvailableFrom.Unix() {
			continue
		}
		record := historyRecordFrom(key, counters)
		if !useTargets {
			record.DestinationDomain = ""
		}
		err = merge(record)
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
		TargetAvailableFrom: targetAvailableFrom,
		ActualFrom:          actualFrom,
		ActualTo:            actualTo,
		GroupBy:             query.GroupBy,
		Page:                query.Page,
		PageSize:            query.PageSize,
		TotalRows:           totalRows,
		Totals:              totals,
		Rows:                resultRows,
	}, nil
}

func normalizeHistoryQuery(query HistoryQuery) (HistoryQuery, error) {
	if !query.From.IsZero() && !query.To.IsZero() && !query.From.Before(query.To) {
		return HistoryQuery{}, errors.New("from must be before to")
	}
	if query.GroupBy == "" {
		query.GroupBy = HistoryGroupByRoutePath
	}
	switch query.GroupBy {
	case HistoryGroupByRoutePath,
		HistoryGroupByDestinationDomain,
		HistoryGroupByOutboundGroup,
		HistoryGroupByActualOutbound:
	default:
		return HistoryQuery{}, errors.New("invalid group_by")
	}
	if query.Page < 0 {
		return HistoryQuery{}, errors.New("page must be at least 1")
	}
	if query.Page == 0 {
		query.Page = 1
	}
	if query.PageSize < 0 || query.PageSize > HistoryPageSizeMax {
		return HistoryQuery{}, errors.New("page_size must be between 1 and 200")
	}
	if query.PageSize == 0 {
		query.PageSize = HistoryPageSizeDefault
	}
	if query.SortBy == "" {
		query.SortBy = HistorySortByTotalBytes
	}
	switch query.SortBy {
	case HistorySortByName,
		HistorySortByTotalBytes,
		HistorySortByUplinkBytes,
		HistorySortByDownlinkBytes,
		HistorySortByConnections:
	default:
		return HistoryQuery{}, errors.New("invalid sort_by")
	}
	if query.SortOrder == "" {
		query.SortOrder = HistorySortOrderDescending
	}
	switch query.SortOrder {
	case HistorySortOrderAscending, HistorySortOrderDescending:
	default:
		return HistoryQuery{}, errors.New("invalid sort_order")
	}
	for _, network := range query.Networks {
		if network != "tcp" && network != "udp" {
			return HistoryQuery{}, errors.New("network must be tcp or udp")
		}
	}
	query.DestinationDomains = append([]string(nil), query.DestinationDomains...)
	for index, domain := range query.DestinationDomains {
		normalized := normalizeDestinationDomain(domain)
		if domain != "" && normalized == "" {
			return HistoryQuery{}, errors.New("invalid destination_domain")
		}
		query.DestinationDomains[index] = normalized
	}
	return query, nil
}

func aggregateHistoryRecord(
	groupBy string,
	record historyRecord,
	groupPath []string,
	destinationDomain string,
	outboundGroup string,
) (historyDimensions, HistoryRow) {
	emptyPath := []string{}
	switch groupBy {
	case HistoryGroupByDestinationDomain:
		return historyDimensions{
				DestinationDomain: destinationDomain,
			}, HistoryRow{
				GroupPath:         emptyPath,
				DestinationDomain: destinationDomain,
			}
	case HistoryGroupByOutboundGroup:
		return historyDimensions{
				OutboundGroup: outboundGroup,
			}, HistoryRow{
				GroupPath:     emptyPath,
				OutboundGroup: outboundGroup,
			}
	case HistoryGroupByActualOutbound:
		return historyDimensions{
				ActualOutboundTag: record.ActualOutboundTag,
			}, HistoryRow{
				GroupPath:          emptyPath,
				ActualOutboundTag:  record.ActualOutboundTag,
				ActualOutboundType: record.ActualOutboundType,
			}
	default:
		return historyDimensions{
				ConfigRevision:     record.ConfigRevision,
				RouteTag:           record.RouteTag,
				GroupPath:          record.GroupPath,
				OutboundGroup:      outboundGroup,
				ActualOutboundTag:  record.ActualOutboundTag,
				ActualOutboundType: record.ActualOutboundType,
				Network:            record.Network,
			}, HistoryRow{
				ConfigRevision:     record.ConfigRevision,
				RouteTag:           record.RouteTag,
				GroupPath:          groupPath,
				OutboundGroup:      outboundGroup,
				ActualOutboundTag:  record.ActualOutboundTag,
				ActualOutboundType: record.ActualOutboundType,
				Network:            record.Network,
			}
	}
}

func lastGroupTag(groupPath []string) string {
	if len(groupPath) == 0 {
		return ""
	}
	return groupPath[len(groupPath)-1]
}

func historyRowLabel(groupBy string, row HistoryRow) string {
	switch groupBy {
	case HistoryGroupByDestinationDomain:
		return row.DestinationDomain
	case HistoryGroupByOutboundGroup:
		return row.OutboundGroup
	case HistoryGroupByActualOutbound:
		return row.ActualOutboundTag
	default:
		path := make([]string, 0, len(row.GroupPath)+2)
		if row.RouteTag != "" {
			path = append(path, row.RouteTag)
		}
		for _, group := range row.GroupPath {
			if group != "" && (len(path) == 0 || path[len(path)-1] != group) {
				path = append(path, group)
			}
		}
		if row.ActualOutboundTag != "" &&
			(len(path) == 0 || path[len(path)-1] != row.ActualOutboundTag) {
			path = append(path, row.ActualOutboundTag)
		}
		return strings.Join(path, " > ")
	}
}

func compareHistoryRows(groupBy string, sortBy string, left HistoryRow, right HistoryRow) int {
	switch sortBy {
	case HistorySortByName:
		leftName := historyRowLabel(groupBy, left)
		rightName := historyRowLabel(groupBy, right)
		if order := strings.Compare(strings.ToLower(leftName), strings.ToLower(rightName)); order != 0 {
			return order
		}
		return strings.Compare(leftName, rightName)
	case HistorySortByUplinkBytes:
		return compareUint64(left.UplinkBytes, right.UplinkBytes)
	case HistorySortByDownlinkBytes:
		return compareUint64(left.DownlinkBytes, right.DownlinkBytes)
	case HistorySortByConnections:
		return compareUint64(left.Connections, right.Connections)
	default:
		return compareUint64(
			saturatingAdd(left.UplinkBytes, left.DownlinkBytes),
			saturatingAdd(right.UplinkBytes, right.DownlinkBytes),
		)
	}
}

func compareHistoryRowsCanonical(groupBy string, left HistoryRow, right HistoryRow) int {
	if order := strings.Compare(historyRowLabel(groupBy, left), historyRowLabel(groupBy, right)); order != 0 {
		return order
	}
	if order := strings.Compare(left.ConfigRevision, right.ConfigRevision); order != 0 {
		return order
	}
	if order := strings.Compare(left.RouteTag, right.RouteTag); order != 0 {
		return order
	}
	if order := slices.Compare(left.GroupPath, right.GroupPath); order != 0 {
		return order
	}
	for _, values := range [][2]string{
		{left.DestinationDomain, right.DestinationDomain},
		{left.OutboundGroup, right.OutboundGroup},
		{left.ActualOutboundTag, right.ActualOutboundTag},
		{left.ActualOutboundType, right.ActualOutboundType},
		{left.Network, right.Network},
	} {
		if order := strings.Compare(values[0], values[1]); order != 0 {
			return order
		}
	}
	return 0
}

func compareUint64(left uint64, right uint64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
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
		DestinationDomain:  key.DestinationDomain,
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

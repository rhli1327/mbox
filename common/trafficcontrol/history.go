package trafficcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/log"
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
	HistoryGroupByDestination       = "destination"
	HistoryGroupByDestinationDomain = "destination_domain"
	HistoryGroupByOutboundGroup     = "outbound_group"
	HistoryGroupByActualOutbound    = "actual_outbound"

	HistoryDestinationTypeDomain = "domain"
	HistoryDestinationTypeIP     = "ip"

	HistorySortByName          = "name"
	HistorySortByTotalBytes    = "total_bytes"
	HistorySortByUplinkBytes   = "uplink_bytes"
	HistorySortByDownlinkBytes = "downlink_bytes"
	HistorySortByConnections   = "connections"

	HistorySortOrderAscending  = "asc"
	HistorySortOrderDescending = "desc"
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
	Destinations       []string
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
	Destination        string
	DestinationType    string
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
	TargetAvailableFrom      time.Time
	DestinationAvailableFrom time.Time
	ActualFrom               time.Time
	ActualTo                 time.Time
	GroupBy                  string
	Page                     int
	PageSize                 int
	TotalRows                int
	Totals                   HistoryCounters
	Rows                     []HistoryRow
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
	DestinationIP      string `json:",omitempty"`
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
	Destination        string
	DestinationType    string
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
	DestinationIP      string `json:"destination_ip,omitempty"`
	ActualOutboundTag  string `json:"actual_outbound_tag"`
	ActualOutboundType string `json:"actual_outbound_type"`
	Network            string `json:"network"`
	historyCounters
}

var (
	_ adapter.LifecycleService = (*History)(nil)
	_ DeltaRecorder            = (*History)(nil)
	_ HistoryReader            = (*History)(nil)
)

type History struct {
	ctx              context.Context
	logger           log.ContextLogger
	configRevision   string
	targetsFrom      time.Time
	destinationsFrom time.Time
	store            historyStore

	storeAccess sync.RWMutex
	flushAccess sync.Mutex
	access      sync.Mutex
	accepting   bool
	pending     historyBatch
	queryAccess chan struct{}

	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
	loopWait  sync.WaitGroup
}

func NewHistory(ctx context.Context, logger log.ContextLogger, options HistoryOptions) *History {
	return newHistory(ctx, logger, newBoltStore(ctx, options))
}

func newHistory(ctx context.Context, logger log.ContextLogger, store historyStore) *History {
	return &History{
		ctx:         ctx,
		logger:      logger,
		store:       store,
		accepting:   true,
		pending:     make(historyBatch),
		queryAccess: make(chan struct{}, historyQueryParallel),
		done:        make(chan struct{}),
	}
}

func (h *History) Name() string {
	return "traffic statistics"
}

func (h *History) Start(stage adapter.StartStage) error {
	switch stage {
	case adapter.StartStateInitialize:
		h.storeAccess.Lock()
		defer h.storeAccess.Unlock()
		state, err := h.store.Open()
		if err != nil {
			return err
		}
		h.configRevision = state.configRevision
		h.targetsFrom = state.targetsFrom
		h.destinationsFrom = state.destinationsFrom
	case adapter.StartStateStart:
		h.loopWait.Add(1)
		go h.loop()
	}
	return nil
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

func (h *History) DestinationAvailableFrom() time.Time {
	return h.destinationAvailableFromAt(time.Now())
}

func (h *History) destinationAvailableFromAt(now time.Time) time.Time {
	if h.destinationsFrom.IsZero() {
		return time.Time{}
	}
	retentionStart := now.Add(-HistoryRetention).UTC().Truncate(HistoryBucketInterval)
	if h.destinationsFrom.Before(retentionStart) {
		return retentionStart
	}
	return h.destinationsFrom
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

	h.storeAccess.Lock()
	defer h.storeAccess.Unlock()
	h.flushAccess.Lock()
	defer h.flushAccess.Unlock()
	h.access.Lock()
	h.accepting = false
	h.access.Unlock()

	// Do not restore a failed final batch: after Close returns the history has
	// no writable database and must not retain an unreachable pending tail.
	flushErr := h.flushLocked(false)
	closeErr := h.store.Close()
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
	destinationDomain := normalizeDestinationDomain(metadata.DestinationDomain)
	var destinationIP string
	if destinationDomain == "" {
		destinationIP = normalizeDestinationIP(metadata.DestinationIP)
	}
	key := historyKey{
		Bucket:             time.Now().UTC().Truncate(HistoryBucketInterval).Unix(),
		ConfigRevision:     h.configRevision,
		RouteTag:           routeTag,
		GroupPath:          string(groupPathContent),
		DestinationDomain:  destinationDomain,
		DestinationIP:      destinationIP,
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
	h.storeAccess.RLock()
	defer h.storeAccess.RUnlock()
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
	h.pending = make(historyBatch)
	h.access.Unlock()
	err := h.store.Write(pending)
	if err != nil && restoreOnError {
		h.restorePending(pending)
	}
	return err
}

func (h *History) restorePending(pending historyBatch) {
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

	h.storeAccess.RLock()
	defer h.storeAccess.RUnlock()

	// Creating the store snapshot and copying pending deltas under the flush
	// lock establishes one consistent query boundary. Once the snapshot exists,
	// a later flush can proceed without changing the persisted side of it.
	h.flushAccess.Lock()
	snapshot, err := h.store.BeginRead(ctx)
	if err != nil {
		h.flushAccess.Unlock()
		return HistoryQueryResult{}, err
	}
	h.access.Lock()
	pending := make(historyBatch, len(h.pending))
	for key, counters := range h.pending {
		pending[key] = counters
	}
	h.access.Unlock()
	h.flushAccess.Unlock()
	defer snapshot.Close()
	return snapshot.Query(ctx, query, historyQueryOverlay{
		pending:                  pending,
		targetAvailableFrom:      h.targetAvailableFromAt(time.Now()),
		destinationAvailableFrom: h.destinationAvailableFromAt(time.Now()),
	})
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
		HistoryGroupByDestination,
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
	query.Destinations = append([]string(nil), query.Destinations...)
	for index, destination := range query.Destinations {
		normalized, _ := normalizeDestination(destination)
		if destination != "" && normalized == "" {
			return HistoryQuery{}, errors.New("invalid destination")
		}
		query.Destinations[index] = normalized
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

func normalizeDestination(destination string) (string, string) {
	if domain := normalizeDestinationDomain(destination); domain != "" {
		return domain, HistoryDestinationTypeDomain
	}
	if address := normalizeDestinationIP(destination); address != "" {
		return address, HistoryDestinationTypeIP
	}
	return "", ""
}

func preferredDestination(domain string, address string) (string, string) {
	if domain = normalizeDestinationDomain(domain); domain != "" {
		return domain, HistoryDestinationTypeDomain
	}
	if address = normalizeDestinationIP(address); address != "" {
		return address, HistoryDestinationTypeIP
	}
	return "", ""
}

func destinationTypeForDomain(domain string) string {
	if domain == "" {
		return ""
	}
	return HistoryDestinationTypeDomain
}

func aggregateHistoryRecord(
	groupBy string,
	record historyRecord,
	groupPath []string,
	destination string,
	destinationType string,
	destinationDomain string,
	outboundGroup string,
) (historyDimensions, HistoryRow) {
	emptyPath := []string{}
	switch groupBy {
	case HistoryGroupByDestination:
		return historyDimensions{
				Destination:     destination,
				DestinationType: destinationType,
			}, HistoryRow{
				GroupPath:         emptyPath,
				Destination:       destination,
				DestinationType:   destinationType,
				DestinationDomain: destinationDomain,
			}
	case HistoryGroupByDestinationDomain:
		return historyDimensions{
				DestinationDomain: destinationDomain,
			}, HistoryRow{
				GroupPath:         emptyPath,
				Destination:       destinationDomain,
				DestinationType:   destinationTypeForDomain(destinationDomain),
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
	case HistoryGroupByDestination:
		return row.Destination
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
		{left.Destination, right.Destination},
		{left.DestinationType, right.DestinationType},
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
	h.storeAccess.RLock()
	defer h.storeAccess.RUnlock()
	h.flushAccess.Lock()
	defer h.flushAccess.Unlock()
	return h.store.Cleanup(now)
}

func historyRecordFrom(key historyKey, counters historyCounters) historyRecord {
	return historyRecord{
		Bucket:             key.Bucket,
		ConfigRevision:     key.ConfigRevision,
		RouteTag:           key.RouteTag,
		GroupPath:          key.GroupPath,
		DestinationDomain:  key.DestinationDomain,
		DestinationIP:      key.DestinationIP,
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

func saturatingAdd(left uint64, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

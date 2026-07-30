package trafficcontrol

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type postgresStoreSnapshot struct {
	connection       *pgxpool.Conn
	transaction      pgx.Tx
	instanceID       string
	schemaIdentifier string
	closeOnce        sync.Once
}

type postgresPersistedRecord struct {
	Record     historyRecord
	ActualFrom time.Time
	ActualTo   time.Time
}

func (s *postgresStore) BeginRead(ctx context.Context) (historyStoreSnapshot, error) {
	s.access.RLock()
	opened := s.opened && !s.closed
	s.access.RUnlock()
	if !opened {
		return nil, errors.New("traffic statistics PostgreSQL store is not open")
	}
	connection, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, classifyPostgresError(ctx, "acquire traffic statistics snapshot", err)
	}
	transaction, err := connection.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		connection.Release()
		return nil, classifyPostgresError(ctx, "begin traffic statistics snapshot", err)
	}
	var snapshotID string
	err = transaction.QueryRow(ctx, "SELECT txid_current_snapshot()::text").Scan(
		&snapshotID,
	)
	if err != nil {
		_ = transaction.Rollback(context.Background())
		connection.Release()
		return nil, classifyPostgresError(ctx, "establish traffic statistics snapshot", err)
	}
	return &postgresStoreSnapshot{
		connection:       connection,
		transaction:      transaction,
		instanceID:       s.identity.InstanceID,
		schemaIdentifier: s.schemaIdentifier,
	}, nil
}

func (s *postgresStoreSnapshot) Close() {
	s.closeOnce.Do(func() {
		_ = s.transaction.Rollback(context.Background())
		s.connection.Release()
	})
}

func (s *postgresStoreSnapshot) Query(
	ctx context.Context,
	query HistoryQuery,
	overlay historyQueryOverlay,
) (HistoryQueryResult, error) {
	useDestinations := query.GroupBy == HistoryGroupByDestination ||
		len(query.Destinations) > 0
	useTargets := useDestinations ||
		query.GroupBy == HistoryGroupByDestinationDomain ||
		len(query.DestinationDomains) > 0
	detailAvailableFrom := overlay.targetAvailableFrom
	if useDestinations {
		detailAvailableFrom = overlay.destinationAvailableFrom
	}
	records, err := s.queryPersisted(
		ctx,
		query,
		useTargets,
		detailAvailableFrom,
	)
	if err != nil {
		return HistoryQueryResult{}, err
	}
	return aggregatePostgresHistory(
		ctx,
		query,
		overlay,
		useTargets,
		useDestinations,
		detailAvailableFrom,
		records,
	)
}

func (s *postgresStoreSnapshot) queryPersisted(
	ctx context.Context,
	query HistoryQuery,
	useTargets bool,
	detailAvailableFrom time.Time,
) ([]postgresPersistedRecord, error) {
	table := "mbox_traffic_minute_summary"
	destinationColumns := "''::text AS destination_domain, ''::text AS destination_ip"
	destinationGroup := "''::text, ''::text"
	if useTargets {
		table = "mbox_traffic_minute_targets"
		destinationColumns = "destination_domain, destination_ip"
		destinationGroup = "destination_domain, destination_ip"
	}
	arguments := []any{s.instanceID}
	conditions := []string{"instance_id = $1"}
	if useTargets && !detailAvailableFrom.IsZero() {
		arguments = append(arguments, detailAvailableFrom)
		conditions = append(conditions, fmt.Sprintf(
			"bucket_start >= $%d",
			len(arguments),
		))
	}
	if !query.From.IsZero() {
		arguments = append(arguments, query.From.UTC().Truncate(HistoryBucketInterval))
		conditions = append(conditions, fmt.Sprintf(
			"bucket_start >= $%d",
			len(arguments),
		))
	}
	if !query.To.IsZero() {
		arguments = append(arguments, query.To.UTC())
		conditions = append(conditions, fmt.Sprintf(
			"bucket_start < $%d",
			len(arguments),
		))
	}
	addTextArrayFilter := func(column string, values []string) {
		if len(values) == 0 {
			return
		}
		arguments = append(arguments, values)
		conditions = append(conditions, fmt.Sprintf(
			"%s = ANY($%d::text[])",
			column,
			len(arguments),
		))
	}
	addTextArrayFilter("route_tag", query.RouteTags)
	addTextArrayFilter("actual_outbound_tag", query.ActualOutboundTags)
	addTextArrayFilter("network", query.Networks)
	if len(query.GroupTags) > 0 {
		arguments = append(arguments, query.GroupTags)
		conditions = append(conditions, fmt.Sprintf(
			"COALESCE(group_path[array_length(group_path, 1)], '') = ANY($%d::text[])",
			len(arguments),
		))
	}
	if useTargets && len(query.DestinationDomains) > 0 {
		addTextArrayFilter("destination_domain", query.DestinationDomains)
	}
	if useTargets && len(query.Destinations) > 0 {
		var domains []string
		var addresses []string
		var destinationConditions []string
		for _, destination := range query.Destinations {
			if destination == "" {
				arguments = append(arguments, destination)
				destinationConditions = append(destinationConditions, fmt.Sprintf(
					"(destination_domain = $%d AND destination_ip = $%d)",
					len(arguments),
					len(arguments),
				))
				continue
			}
			if normalized := normalizeDestinationDomain(destination); normalized != "" {
				domains = append(domains, normalized)
			} else if normalized = normalizeDestinationIP(destination); normalized != "" {
				addresses = append(addresses, normalized)
			}
		}
		if len(domains) > 0 {
			arguments = append(arguments, domains)
			destinationConditions = append(destinationConditions, fmt.Sprintf(
				"destination_domain = ANY($%d::text[])",
				len(arguments),
			))
		}
		if len(addresses) > 0 {
			arguments = append(arguments, addresses)
			destinationConditions = append(destinationConditions, fmt.Sprintf(
				"(destination_domain = '' AND destination_ip = ANY($%d::text[]))",
				len(arguments),
			))
		}
		conditions = append(
			conditions,
			"("+strings.Join(destinationConditions, " OR ")+")",
		)
	}
	statement := fmt.Sprintf(`
SELECT
    MIN(bucket_start),
    MAX(bucket_start) + interval '1 minute',
    config_revision,
    route_tag,
    group_path,
    %s,
    actual_outbound_tag,
    actual_outbound_type,
    network,
    LEAST(%s::numeric, SUM(uplink_bytes))::text,
    LEAST(%s::numeric, SUM(downlink_bytes))::text,
    LEAST(%s::numeric, SUM(connections))::text
FROM %s.%s
WHERE %s
GROUP BY
    config_revision,
    route_tag,
    group_path,
    %s,
    actual_outbound_tag,
    actual_outbound_type,
    network
`, destinationColumns,
		postgresUint64Maximum,
		postgresUint64Maximum,
		postgresUint64Maximum,
		s.schemaIdentifier,
		table,
		strings.Join(conditions, " AND "),
		destinationGroup,
	)
	rows, err := s.transaction.Query(ctx, statement, arguments...)
	if err != nil {
		return nil, classifyPostgresError(ctx, "query persisted traffic statistics", err)
	}
	defer rows.Close()
	records := make([]postgresPersistedRecord, 0)
	for rows.Next() {
		var bucketStart time.Time
		var bucketEnd time.Time
		var groupPath []string
		var uplink string
		var downlink string
		var connections string
		var record historyRecord
		err = rows.Scan(
			&bucketStart,
			&bucketEnd,
			&record.ConfigRevision,
			&record.RouteTag,
			&groupPath,
			&record.DestinationDomain,
			&record.DestinationIP,
			&record.ActualOutboundTag,
			&record.ActualOutboundType,
			&record.Network,
			&uplink,
			&downlink,
			&connections,
		)
		if err != nil {
			return nil, classifyPostgresError(ctx, "decode persisted traffic statistics", err)
		}
		record.Bucket = bucketStart.UTC().Unix()
		record.GroupPath = mustMarshalPostgresGroupPath(groupPath)
		if record.UplinkBytes, err = strconv.ParseUint(uplink, 10, 64); err != nil {
			return nil, ErrPostgresSchemaIncompatible
		}
		if record.DownlinkBytes, err = strconv.ParseUint(downlink, 10, 64); err != nil {
			return nil, ErrPostgresSchemaIncompatible
		}
		if record.Connections, err = strconv.ParseUint(connections, 10, 64); err != nil {
			return nil, ErrPostgresSchemaIncompatible
		}
		records = append(records, postgresPersistedRecord{
			Record:     record,
			ActualFrom: bucketStart.UTC(),
			ActualTo:   bucketEnd.UTC(),
		})
	}
	if err = rows.Err(); err != nil {
		return nil, classifyPostgresError(ctx, "query persisted traffic statistics", err)
	}
	return records, nil
}

func mustMarshalPostgresGroupPath(groupPath []string) string {
	if groupPath == nil {
		groupPath = []string{}
	}
	content, err := json.Marshal(groupPath)
	if err != nil {
		panic(err)
	}
	return string(content)
}

func aggregatePostgresHistory(
	ctx context.Context,
	query HistoryQuery,
	overlay historyQueryOverlay,
	useTargets bool,
	useDestinations bool,
	detailAvailableFrom time.Time,
	persisted []postgresPersistedRecord,
) (HistoryQueryResult, error) {
	routeTags := filterSet(query.RouteTags)
	groupTags := filterSet(query.GroupTags)
	actualOutboundTags := filterSet(query.ActualOutboundTags)
	destinations := filterSet(query.Destinations)
	destinationDomains := filterSet(query.DestinationDomains)
	networks := filterSet(query.Networks)
	rows := make(map[historyDimensions]historyAggregate)
	merge := func(record historyRecord, bucketStart time.Time, bucketEnd time.Time) error {
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
		if err := json.Unmarshal([]byte(record.GroupPath), &groupPath); err != nil {
			return err
		}
		if groupPath == nil {
			groupPath = []string{}
		}
		outboundGroup := lastGroupTag(groupPath)
		destinationDomain := normalizeDestinationDomain(record.DestinationDomain)
		destination, destinationType := preferredDestination(
			destinationDomain,
			record.DestinationIP,
		)
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
		aggregate.Row.UplinkBytes = saturatingAdd(
			aggregate.Row.UplinkBytes,
			record.UplinkBytes,
		)
		aggregate.Row.DownlinkBytes = saturatingAdd(
			aggregate.Row.DownlinkBytes,
			record.DownlinkBytes,
		)
		aggregate.Row.Connections = saturatingAdd(
			aggregate.Row.Connections,
			record.Connections,
		)
		if aggregate.ActualFrom.IsZero() || bucketStart.Before(aggregate.ActualFrom) {
			aggregate.ActualFrom = bucketStart
		}
		if bucketEnd.After(aggregate.ActualTo) {
			aggregate.ActualTo = bucketEnd
		}
		rows[dimensions] = aggregate
		return nil
	}
	for index, persistedRecord := range persisted {
		if index&255 == 0 {
			if err := ctx.Err(); err != nil {
				return HistoryQueryResult{}, err
			}
		}
		if err := merge(
			persistedRecord.Record,
			persistedRecord.ActualFrom,
			persistedRecord.ActualTo,
		); err != nil {
			return HistoryQueryResult{}, err
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
		bucketStart := time.Unix(record.Bucket, 0).UTC()
		if err := merge(
			record,
			bucketStart,
			bucketStart.Add(HistoryBucketInterval),
		); err != nil {
			return HistoryQueryResult{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return HistoryQueryResult{}, err
	}
	resultRows := make([]HistoryRow, 0, len(rows))
	var totals HistoryCounters
	var actualFrom time.Time
	var actualTo time.Time
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
		if search != "" &&
			!strings.Contains(
				strings.ToLower(historyRowLabel(query.GroupBy, row)),
				search,
			) {
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
	sort.Slice(resultRows, func(left int, right int) bool {
		primary := compareHistoryRows(
			query.GroupBy,
			query.SortBy,
			resultRows[left],
			resultRows[right],
		)
		if primary != 0 {
			if query.SortOrder == HistorySortOrderDescending {
				return primary > 0
			}
			return primary < 0
		}
		return compareHistoryRowsCanonical(
			query.GroupBy,
			resultRows[left],
			resultRows[right],
		) < 0
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

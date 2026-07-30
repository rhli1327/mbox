package trafficcontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/jackc/pgx/v5"
)

const postgresUint64Maximum = "18446744073709551615"

type postgresBatchRecord struct {
	Key      historyKey
	Counters historyCounters
	Encoded  []byte
}

type postgresBatchIdentity struct {
	Checksum    [sha256.Size]byte
	Count       int
	FirstBucket *time.Time
	LastBucket  *time.Time
}

func (s *postgresStore) Write(batch historyBatch) error {
	// Phase 2's direct historyStore adapter has no durable batch identity.
	// Phase 4 replaces this generated ID with the stable spool record ID.
	batchID, err := uuid.NewV4()
	if err != nil {
		return fmt.Errorf("generate traffic statistics PostgreSQL batch ID: %w", err)
	}
	return s.applyBatch(s.ctx, batchID, batch)
}

func (s *postgresStore) applyBatch(
	ctx context.Context,
	batchID uuid.UUID,
	batch historyBatch,
) error {
	s.access.RLock()
	opened := s.opened && !s.closed
	s.access.RUnlock()
	if !opened {
		return errors.New("traffic statistics PostgreSQL store is not open")
	}
	records, identity, err := encodePostgresBatch(batch)
	if err != nil {
		return err
	}
	transaction, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return classifyPostgresError(ctx, "begin traffic statistics batch", err)
	}
	defer transaction.Rollback(context.Background())
	inserted := false
	err = transaction.QueryRow(ctx, fmt.Sprintf(`
INSERT INTO %s.mbox_traffic_ingest_batches (
    instance_id,
    batch_id,
    payload_sha256,
    record_count,
    first_bucket,
    last_bucket
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (instance_id, batch_id) DO NOTHING
RETURNING true`, s.schemaIdentifier),
		s.identity.InstanceID,
		batchID,
		identity.Checksum[:],
		identity.Count,
		identity.FirstBucket,
		identity.LastBucket,
	).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.verifyExistingPostgresBatch(ctx, transaction, batchID, identity)
	}
	if err != nil {
		return classifyPostgresError(ctx, "record traffic statistics batch", err)
	}
	for _, record := range records {
		if err = s.upsertPostgresBatchRecord(ctx, transaction, record); err != nil {
			return err
		}
	}
	if err = transaction.Commit(ctx); err != nil {
		return classifyPostgresError(ctx, "commit traffic statistics batch", err)
	}
	return nil
}

func (s *postgresStore) verifyExistingPostgresBatch(
	ctx context.Context,
	transaction pgx.Tx,
	batchID uuid.UUID,
	expected postgresBatchIdentity,
) error {
	var checksum []byte
	var count int
	var firstBucket *time.Time
	var lastBucket *time.Time
	err := transaction.QueryRow(ctx, fmt.Sprintf(`
SELECT payload_sha256, record_count, first_bucket, last_bucket
FROM %s.mbox_traffic_ingest_batches
WHERE instance_id = $1 AND batch_id = $2
FOR UPDATE`, s.schemaIdentifier),
		s.identity.InstanceID,
		batchID,
	).Scan(&checksum, &count, &firstBucket, &lastBucket)
	if err != nil {
		return classifyPostgresError(ctx, "read traffic statistics batch marker", err)
	}
	if count != expected.Count ||
		!bytes.Equal(checksum, expected.Checksum[:]) ||
		!equalPostgresBatchTime(firstBucket, expected.FirstBucket) ||
		!equalPostgresBatchTime(lastBucket, expected.LastBucket) {
		return ErrPostgresBatchCollision
	}
	return nil
}

func equalPostgresBatchTime(left *time.Time, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.UTC().Equal(right.UTC())
}

func encodePostgresBatch(
	batch historyBatch,
) ([]postgresBatchRecord, postgresBatchIdentity, error) {
	normalizedBatch := normalizePostgresBatch(batch)
	records := make([]postgresBatchRecord, 0, len(normalizedBatch))
	for key, counters := range normalizedBatch {
		encoded, err := encodePostgresBatchRecord(key, counters)
		if err != nil {
			return nil, postgresBatchIdentity{}, err
		}
		records = append(records, postgresBatchRecord{
			Key:      key,
			Counters: counters,
			Encoded:  encoded,
		})
	}
	slices.SortFunc(records, func(left postgresBatchRecord, right postgresBatchRecord) int {
		return bytes.Compare(left.Encoded, right.Encoded)
	})
	hasher := sha256.New()
	for _, record := range records {
		_, _ = hasher.Write(record.Encoded)
	}
	var checksum [sha256.Size]byte
	copy(checksum[:], hasher.Sum(nil))
	identity := postgresBatchIdentity{
		Checksum: checksum,
		Count:    len(records),
	}
	if len(records) > 0 {
		first := time.Unix(records[0].Key.Bucket, 0).UTC()
		last := first
		for _, record := range records[1:] {
			bucket := time.Unix(record.Key.Bucket, 0).UTC()
			if bucket.Before(first) {
				first = bucket
			}
			if bucket.After(last) {
				last = bucket
			}
		}
		identity.FirstBucket = &first
		identity.LastBucket = &last
	}
	return records, identity, nil
}

func normalizePostgresBatch(batch historyBatch) historyBatch {
	normalized := make(historyBatch, len(batch))
	for key, counters := range batch {
		domain := normalizeDestinationDomain(key.DestinationDomain)
		if domain != "" {
			key.DestinationDomain = domain
			key.DestinationIP = ""
		} else {
			key.DestinationDomain = ""
			key.DestinationIP = normalizeDestinationIP(key.DestinationIP)
		}
		current := normalized[key]
		current.UplinkBytes = saturatingAdd(
			current.UplinkBytes,
			counters.UplinkBytes,
		)
		current.DownlinkBytes = saturatingAdd(
			current.DownlinkBytes,
			counters.DownlinkBytes,
		)
		current.Connections = saturatingAdd(
			current.Connections,
			counters.Connections,
		)
		normalized[key] = current
	}
	return normalized
}

func encodePostgresBatchRecord(
	key historyKey,
	counters historyCounters,
) ([]byte, error) {
	var encoded bytes.Buffer
	if err := binary.Write(&encoded, binary.BigEndian, key.Bucket); err != nil {
		return nil, err
	}
	for _, value := range []string{
		key.ConfigRevision,
		key.RouteTag,
		key.GroupPath,
		key.DestinationDomain,
		key.DestinationIP,
		key.ActualOutboundTag,
		key.ActualOutboundType,
		key.Network,
	} {
		if uint64(len(value)) > uint64(math.MaxUint32) {
			return nil, errors.New("traffic statistics batch dimension is too large")
		}
		if err := binary.Write(&encoded, binary.BigEndian, uint32(len(value))); err != nil {
			return nil, err
		}
		_, _ = encoded.WriteString(value)
	}
	for _, counter := range []uint64{
		counters.UplinkBytes,
		counters.DownlinkBytes,
		counters.Connections,
	} {
		if err := binary.Write(&encoded, binary.BigEndian, counter); err != nil {
			return nil, err
		}
	}
	return encoded.Bytes(), nil
}

func (s *postgresStore) upsertPostgresBatchRecord(
	ctx context.Context,
	transaction pgx.Tx,
	record postgresBatchRecord,
) error {
	var groupPath []string
	if err := json.Unmarshal([]byte(record.Key.GroupPath), &groupPath); err != nil {
		return fmt.Errorf("decode traffic statistics group path: %w", err)
	}
	if groupPath == nil {
		groupPath = []string{}
	}
	bucketStart := time.Unix(record.Key.Bucket, 0).UTC()
	if !bucketStart.Equal(bucketStart.Truncate(HistoryBucketInterval)) {
		return errors.New("traffic statistics PostgreSQL bucket is not minute aligned")
	}
	counterArguments := []any{
		strconv.FormatUint(record.Counters.UplinkBytes, 10),
		strconv.FormatUint(record.Counters.DownlinkBytes, 10),
		strconv.FormatUint(record.Counters.Connections, 10),
	}
	_, err := transaction.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s.mbox_traffic_minute_summary (
    instance_id,
    bucket_start,
    config_revision,
    route_tag,
    group_path,
    actual_outbound_tag,
    actual_outbound_type,
    network,
    uplink_bytes,
    downlink_bytes,
    connections
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::numeric, $10::numeric, $11::numeric)
ON CONFLICT (
    instance_id,
    bucket_start,
    config_revision,
    route_tag,
    group_path,
    actual_outbound_tag,
    actual_outbound_type,
    network
) DO UPDATE SET
    uplink_bytes = LEAST(%s::numeric, mbox_traffic_minute_summary.uplink_bytes + EXCLUDED.uplink_bytes),
    downlink_bytes = LEAST(%s::numeric, mbox_traffic_minute_summary.downlink_bytes + EXCLUDED.downlink_bytes),
    connections = LEAST(%s::numeric, mbox_traffic_minute_summary.connections + EXCLUDED.connections)
`, s.schemaIdentifier, postgresUint64Maximum, postgresUint64Maximum, postgresUint64Maximum),
		s.identity.InstanceID,
		bucketStart,
		record.Key.ConfigRevision,
		record.Key.RouteTag,
		groupPath,
		record.Key.ActualOutboundTag,
		record.Key.ActualOutboundType,
		record.Key.Network,
		counterArguments[0],
		counterArguments[1],
		counterArguments[2],
	)
	if err != nil {
		return classifyPostgresError(ctx, "write traffic statistics summary", err)
	}
	if !s.targetsFrom.IsZero() && bucketStart.Before(s.targetsFrom) {
		return nil
	}
	_, err = transaction.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s.mbox_traffic_minute_targets (
    instance_id,
    bucket_start,
    config_revision,
    route_tag,
    group_path,
    actual_outbound_tag,
    actual_outbound_type,
    network,
    destination_domain,
    destination_ip,
    uplink_bytes,
    downlink_bytes,
    connections
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11::numeric, $12::numeric, $13::numeric
)
ON CONFLICT (
    instance_id,
    bucket_start,
    config_revision,
    route_tag,
    group_path,
    actual_outbound_tag,
    actual_outbound_type,
    network,
    destination_domain,
    destination_ip
) DO UPDATE SET
    uplink_bytes = LEAST(%s::numeric, mbox_traffic_minute_targets.uplink_bytes + EXCLUDED.uplink_bytes),
    downlink_bytes = LEAST(%s::numeric, mbox_traffic_minute_targets.downlink_bytes + EXCLUDED.downlink_bytes),
    connections = LEAST(%s::numeric, mbox_traffic_minute_targets.connections + EXCLUDED.connections)
`, s.schemaIdentifier, postgresUint64Maximum, postgresUint64Maximum, postgresUint64Maximum),
		s.identity.InstanceID,
		bucketStart,
		record.Key.ConfigRevision,
		record.Key.RouteTag,
		groupPath,
		record.Key.ActualOutboundTag,
		record.Key.ActualOutboundType,
		record.Key.Network,
		record.Key.DestinationDomain,
		record.Key.DestinationIP,
		counterArguments[0],
		counterArguments[1],
		counterArguments[2],
	)
	if err != nil {
		return classifyPostgresError(ctx, "write traffic statistics targets", err)
	}
	return nil
}

func (s *postgresStore) Cleanup(now time.Time) error {
	s.access.RLock()
	opened := s.opened && !s.closed
	s.access.RUnlock()
	if !opened {
		return nil
	}
	cutoff := now.Add(-HistoryRetention).UTC().Truncate(HistoryBucketInterval)
	transaction, err := s.pool.BeginTx(s.ctx, pgx.TxOptions{})
	if err != nil {
		return classifyPostgresError(s.ctx, "begin traffic statistics retention", err)
	}
	defer transaction.Rollback(context.Background())
	for _, table := range []string{
		"mbox_traffic_minute_summary",
		"mbox_traffic_minute_targets",
	} {
		_, err = transaction.Exec(s.ctx, fmt.Sprintf(`
DELETE FROM %s.%s
WHERE instance_id = $1 AND bucket_start < $2
`, s.schemaIdentifier, table), s.identity.InstanceID, cutoff)
		if err != nil {
			return classifyPostgresError(s.ctx, "delete expired traffic statistics", err)
		}
	}
	if err = transaction.Commit(s.ctx); err != nil {
		return classifyPostgresError(s.ctx, "commit traffic statistics retention", err)
	}
	return nil
}

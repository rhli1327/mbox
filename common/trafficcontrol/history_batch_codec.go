package trafficcontrol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"time"
	"unicode/utf8"
)

type historyBatchRecord struct {
	Key      historyKey
	Counters historyCounters
	Encoded  []byte
}

type historyBatchIdentity struct {
	Checksum    [sha256.Size]byte
	Count       int
	FirstBucket *time.Time
	LastBucket  *time.Time
}

type postgresBatchRecord = historyBatchRecord
type postgresBatchIdentity = historyBatchIdentity

func encodeHistoryBatch(
	batch historyBatch,
) ([]historyBatchRecord, historyBatchIdentity, error) {
	normalizedBatch := normalizeHistoryBatch(batch)
	records := make([]historyBatchRecord, 0, len(normalizedBatch))
	for key, counters := range normalizedBatch {
		encoded, err := encodeHistoryBatchRecord(key, counters)
		if err != nil {
			return nil, historyBatchIdentity{}, err
		}
		records = append(records, historyBatchRecord{
			Key:      key,
			Counters: counters,
			Encoded:  encoded,
		})
	}
	slices.SortFunc(records, func(left historyBatchRecord, right historyBatchRecord) int {
		return bytes.Compare(left.Encoded, right.Encoded)
	})
	hasher := sha256.New()
	for _, record := range records {
		_, _ = hasher.Write(record.Encoded)
	}
	var checksum [sha256.Size]byte
	copy(checksum[:], hasher.Sum(nil))
	identity := historyBatchIdentity{
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

func encodePostgresBatch(
	batch historyBatch,
) ([]postgresBatchRecord, postgresBatchIdentity, error) {
	return encodeHistoryBatch(batch)
}

func normalizeHistoryBatch(batch historyBatch) historyBatch {
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

func normalizePostgresBatch(batch historyBatch) historyBatch {
	return normalizeHistoryBatch(batch)
}

func encodeHistoryBatchRecord(
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
		if !utf8.ValidString(value) {
			return nil, errors.New("traffic statistics batch dimension is not UTF-8")
		}
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

func encodePostgresBatchRecord(
	key historyKey,
	counters historyCounters,
) ([]byte, error) {
	return encodeHistoryBatchRecord(key, counters)
}

func decodeHistoryBatchRecord(encoded []byte) (historyKey, historyCounters, error) {
	const fixedSize = 8 + 8*4 + 3*8
	if len(encoded) < fixedSize {
		return historyKey{}, historyCounters{}, errors.New(
			"traffic statistics batch record is truncated",
		)
	}
	offset := 0
	readUint64 := func() (uint64, bool) {
		if len(encoded)-offset < 8 {
			return 0, false
		}
		value := binary.BigEndian.Uint64(encoded[offset : offset+8])
		offset += 8
		return value, true
	}
	bucket, ok := readUint64()
	if !ok {
		return historyKey{}, historyCounters{}, errors.New(
			"traffic statistics batch record is truncated",
		)
	}
	values := make([]string, 8)
	for index := range values {
		if len(encoded)-offset < 4 {
			return historyKey{}, historyCounters{}, errors.New(
				"traffic statistics batch record is truncated",
			)
		}
		length := uint64(binary.BigEndian.Uint32(encoded[offset : offset+4]))
		offset += 4
		if length > uint64(len(encoded)-offset) {
			return historyKey{}, historyCounters{}, errors.New(
				"traffic statistics batch dimension length is invalid",
			)
		}
		value := encoded[offset : offset+int(length)]
		if !utf8.Valid(value) {
			return historyKey{}, historyCounters{}, errors.New(
				"traffic statistics batch dimension is not UTF-8",
			)
		}
		values[index] = string(value)
		offset += int(length)
	}
	counters := make([]uint64, 3)
	for index := range counters {
		value, available := readUint64()
		if !available {
			return historyKey{}, historyCounters{}, errors.New(
				"traffic statistics batch counters are truncated",
			)
		}
		counters[index] = value
	}
	if offset != len(encoded) {
		return historyKey{}, historyCounters{}, errors.New(
			"traffic statistics batch record contains trailing data",
		)
	}
	return historyKey{
			Bucket:             int64(bucket),
			ConfigRevision:     values[0],
			RouteTag:           values[1],
			GroupPath:          values[2],
			DestinationDomain:  values[3],
			DestinationIP:      values[4],
			ActualOutboundTag:  values[5],
			ActualOutboundType: values[6],
			Network:            values[7],
		}, historyCounters{
			UplinkBytes:   counters[0],
			DownlinkBytes: counters[1],
			Connections:   counters[2],
		}, nil
}

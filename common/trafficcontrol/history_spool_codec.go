package trafficcontrol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/gofrs/uuid/v5"
)

const (
	historySpoolEnvelopeVersion = 1
	historySpoolEnvelopeHeader  = 172
	historySpoolEnvelopeTrailer = sha256.Size
)

var historySpoolEnvelopeMagic = [8]byte{'M', 'B', 'T', 'S', 'P', 'L', '0', '1'}

type historyBatchEnvelope struct {
	BatchID            uuid.UUID
	Sequence           uint64
	CreatedAt          time.Time
	RoutingFingerprint [sha256.Size]byte
	ConfigRevision     string
	RecordCount        uint32
	FirstBucket        int64
	LastBucket         int64
	PayloadSHA256      [sha256.Size]byte
	EnvelopeSHA256     [sha256.Size]byte
	Batch              historyBatch
	Encoded            []byte
}

type preparedHistoryBatchEnvelope struct {
	Records       []historyBatchRecord
	Identity      historyBatchIdentity
	PayloadLength uint64
	LogicalSize   uint64
}

type historyBatchEnvelopeInspection struct {
	BatchID            uuid.UUID
	Sequence           uint64
	CreatedAt          time.Time
	RoutingFingerprint [sha256.Size]byte
	ConfigRevision     string
	RecordCount        uint32
	FirstBucket        int64
	LastBucket         int64
	PayloadLength      uint64
	PayloadSHA256      [sha256.Size]byte
	EnvelopeSHA256     [sha256.Size]byte
	LogicalSize        uint64
}

func historyBatchID(spoolID uuid.UUID, sequence uint64) uuid.UUID {
	name := make([]byte, len("mbox-traffic-batch-v1\x00")+8)
	copy(name, "mbox-traffic-batch-v1\x00")
	binary.BigEndian.PutUint64(name[len(name)-8:], sequence)
	return uuid.NewV5(spoolID, string(name))
}

func prepareHistoryBatchEnvelope(
	batch historyBatch,
	configRevision string,
	maxLogicalSize uint64,
) (preparedHistoryBatchEnvelope, bool, error) {
	if !isHistoryConfigRevision(configRevision) {
		return preparedHistoryBatchEnvelope{}, false, errors.New(
			"traffic statistics spool config revision is invalid",
		)
	}
	records, identity, err := encodeHistoryBatch(batch)
	if err != nil {
		return preparedHistoryBatchEnvelope{}, false, err
	}
	if len(records) == 0 {
		return preparedHistoryBatchEnvelope{}, false, nil
	}
	if len(records) > math.MaxUint32 {
		return preparedHistoryBatchEnvelope{}, false, errors.New(
			"traffic statistics spool batch has too many records",
		)
	}
	var payloadLength uint64
	for _, record := range records {
		if record.Key.ConfigRevision != configRevision {
			return preparedHistoryBatchEnvelope{}, false, errors.New(
				"traffic statistics spool batch config revision does not match envelope",
			)
		}
		if uint64(len(record.Encoded)) > math.MaxUint32 {
			return preparedHistoryBatchEnvelope{}, false, errors.New(
				"traffic statistics spool record is too large",
			)
		}
		framedLength, addErr := addHistorySpoolSize(
			4,
			uint64(len(record.Encoded)),
		)
		if addErr != nil {
			return preparedHistoryBatchEnvelope{}, false, addErr
		}
		payloadLength, addErr = addHistorySpoolSize(
			payloadLength,
			framedLength,
		)
		if addErr != nil {
			return preparedHistoryBatchEnvelope{}, false, addErr
		}
	}
	envelopeLength, err := addHistorySpoolSize(
		historySpoolEnvelopeHeader,
		payloadLength,
	)
	if err != nil {
		return preparedHistoryBatchEnvelope{}, false, err
	}
	envelopeLength, err = addHistorySpoolSize(
		envelopeLength,
		historySpoolEnvelopeTrailer,
	)
	if err != nil {
		return preparedHistoryBatchEnvelope{}, false, err
	}
	logicalSize, err := addHistorySpoolSize(8, envelopeLength)
	if err != nil {
		return preparedHistoryBatchEnvelope{}, false, err
	}
	if logicalSize > historySpoolSafeLogicalSize {
		return preparedHistoryBatchEnvelope{}, false, errors.New(
			"traffic statistics spool batch exceeds safe allocation range",
		)
	}
	prepared := preparedHistoryBatchEnvelope{
		Records:       records,
		Identity:      identity,
		PayloadLength: payloadLength,
		LogicalSize:   logicalSize,
	}
	return prepared, maxLogicalSize > 0 && logicalSize > maxLogicalSize, nil
}

func encodeHistoryBatchEnvelope(
	spoolID uuid.UUID,
	sequence uint64,
	createdAt time.Time,
	routingFingerprint [sha256.Size]byte,
	configRevision string,
	batch historyBatch,
) ([]byte, historyBatchEnvelope, error) {
	prepared, oversize, err := prepareHistoryBatchEnvelope(
		batch,
		configRevision,
		historySpoolSafeLogicalSize,
	)
	if err != nil {
		return nil, historyBatchEnvelope{}, err
	}
	if oversize {
		return nil, historyBatchEnvelope{}, errors.New(
			"traffic statistics spool batch exceeds safe allocation range",
		)
	}
	return encodePreparedHistoryBatchEnvelope(
		spoolID,
		sequence,
		createdAt,
		routingFingerprint,
		configRevision,
		prepared,
		nil,
	)
}

func encodePreparedHistoryBatchEnvelope(
	spoolID uuid.UUID,
	sequence uint64,
	createdAt time.Time,
	routingFingerprint [sha256.Size]byte,
	configRevision string,
	prepared preparedHistoryBatchEnvelope,
	faults *historySpoolFaults,
) ([]byte, historyBatchEnvelope, error) {
	if sequence == 0 {
		return nil, historyBatchEnvelope{}, errors.New(
			"traffic statistics spool sequence must be nonzero",
		)
	}
	if !isHistoryConfigRevision(configRevision) {
		return nil, historyBatchEnvelope{}, errors.New(
			"traffic statistics spool config revision is invalid",
		)
	}
	if len(prepared.Records) == 0 {
		return nil, historyBatchEnvelope{}, errors.New(
			"traffic statistics spool batch is empty",
		)
	}
	if err := runHistorySpoolFault(
		faults,
		func(value *historySpoolFaults) func() error {
			return value.BeforeEnvelopeAllocation
		},
	); err != nil {
		return nil, historyBatchEnvelope{}, err
	}
	batchID := historyBatchID(spoolID, sequence)
	firstBucket := prepared.Identity.FirstBucket.Unix()
	lastBucket := prepared.Identity.LastBucket.Unix()
	encoded := make(
		[]byte,
		int(prepared.LogicalSize)-8,
	)
	copy(encoded[0:8], historySpoolEnvelopeMagic[:])
	binary.BigEndian.PutUint16(encoded[8:10], historySpoolEnvelopeVersion)
	binary.BigEndian.PutUint16(encoded[10:12], 0)
	copy(encoded[12:28], batchID[:])
	binary.BigEndian.PutUint64(encoded[28:36], sequence)
	binary.BigEndian.PutUint64(encoded[36:44], uint64(createdAt.UnixNano()))
	copy(encoded[44:76], routingFingerprint[:])
	copy(encoded[76:108], configRevision)
	binary.BigEndian.PutUint32(encoded[108:112], uint32(len(prepared.Records)))
	binary.BigEndian.PutUint32(encoded[112:116], 0)
	binary.BigEndian.PutUint64(encoded[116:124], uint64(firstBucket))
	binary.BigEndian.PutUint64(encoded[124:132], uint64(lastBucket))
	binary.BigEndian.PutUint64(encoded[132:140], prepared.PayloadLength)
	copy(encoded[140:172], prepared.Identity.Checksum[:])
	offset := historySpoolEnvelopeHeader
	for _, record := range prepared.Records {
		binary.BigEndian.PutUint32(
			encoded[offset:offset+4],
			uint32(len(record.Encoded)),
		)
		offset += 4
		copy(encoded[offset:offset+len(record.Encoded)], record.Encoded)
		offset += len(record.Encoded)
	}
	envelopeChecksum := sha256.Sum256(encoded[:len(encoded)-sha256.Size])
	copy(encoded[len(encoded)-sha256.Size:], envelopeChecksum[:])
	envelope := historyBatchEnvelope{
		BatchID:            batchID,
		Sequence:           sequence,
		CreatedAt:          createdAt,
		RoutingFingerprint: routingFingerprint,
		ConfigRevision:     configRevision,
		RecordCount:        uint32(len(prepared.Records)),
		FirstBucket:        firstBucket,
		LastBucket:         lastBucket,
		PayloadSHA256:      prepared.Identity.Checksum,
		EnvelopeSHA256:     envelopeChecksum,
		Batch:              normalizedRecordsToBatch(prepared.Records),
		Encoded:            encoded,
	}
	return encoded, envelope, nil
}

func decodeHistoryBatchEnvelope(
	encoded []byte,
	spoolID uuid.UUID,
	maxLogicalSize uint64,
) (historyBatchEnvelope, error) {
	return decodeHistoryBatchEnvelopeWithFaults(
		encoded,
		spoolID,
		maxLogicalSize,
		nil,
	)
}

func inspectHistoryBatchEnvelope(
	encoded []byte,
	spoolID uuid.UUID,
	maxLogicalSize uint64,
) (historyBatchEnvelopeInspection, error) {
	corrupt := func(reason string) (historyBatchEnvelope, error) {
		return historyBatchEnvelope{}, fmt.Errorf("%w: %s", ErrSpoolCorrupt, reason)
	}
	inspectionCorrupt := func(reason string) (historyBatchEnvelopeInspection, error) {
		_, err := corrupt(reason)
		return historyBatchEnvelopeInspection{}, err
	}
	if len(encoded) < historySpoolEnvelopeHeader+historySpoolEnvelopeTrailer {
		return inspectionCorrupt("batch envelope is truncated")
	}
	logicalSize, err := addHistorySpoolSize(8, uint64(len(encoded)))
	if err != nil || logicalSize > historySpoolSafeLogicalSize {
		return inspectionCorrupt("batch envelope exceeds safe allocation range")
	}
	if maxLogicalSize > 0 && logicalSize > maxLogicalSize {
		return inspectionCorrupt("batch envelope exceeds configured capacity")
	}
	if !bytes.Equal(encoded[0:8], historySpoolEnvelopeMagic[:]) {
		return inspectionCorrupt("batch envelope magic is invalid")
	}
	if binary.BigEndian.Uint16(encoded[8:10]) != historySpoolEnvelopeVersion {
		return inspectionCorrupt("batch envelope version is invalid")
	}
	if binary.BigEndian.Uint16(encoded[10:12]) != 0 ||
		binary.BigEndian.Uint32(encoded[112:116]) != 0 {
		return inspectionCorrupt("batch envelope reserved fields are nonzero")
	}
	batchID, err := uuid.FromBytes(encoded[12:28])
	if err != nil {
		return inspectionCorrupt("batch ID is invalid")
	}
	sequence := binary.BigEndian.Uint64(encoded[28:36])
	if sequence == 0 || batchID != historyBatchID(spoolID, sequence) {
		return inspectionCorrupt("batch ID does not match spool sequence")
	}
	var routingFingerprint [sha256.Size]byte
	copy(routingFingerprint[:], encoded[44:76])
	configRevision := string(encoded[76:108])
	if !isHistoryConfigRevision(configRevision) {
		return inspectionCorrupt("batch config revision is invalid")
	}
	recordCount := binary.BigEndian.Uint32(encoded[108:112])
	if recordCount == 0 {
		return inspectionCorrupt("batch record count is zero")
	}
	firstBucket := int64(binary.BigEndian.Uint64(encoded[116:124]))
	lastBucket := int64(binary.BigEndian.Uint64(encoded[124:132]))
	if firstBucket > lastBucket {
		return inspectionCorrupt("batch bucket range is invalid")
	}
	payloadLength := binary.BigEndian.Uint64(encoded[132:140])
	if payloadLength > uint64(math.MaxInt) ||
		payloadLength != uint64(len(encoded)-historySpoolEnvelopeHeader-
			historySpoolEnvelopeTrailer) {
		return inspectionCorrupt("batch payload length is invalid")
	}
	const minimumFramedRecordSize = 4 + 8 + 8*4 + 3*8
	if uint64(recordCount) > payloadLength/minimumFramedRecordSize {
		return inspectionCorrupt("batch record count exceeds payload capacity")
	}
	var payloadChecksum [sha256.Size]byte
	copy(payloadChecksum[:], encoded[140:172])
	var envelopeChecksum [sha256.Size]byte
	copy(envelopeChecksum[:], encoded[len(encoded)-sha256.Size:])
	actualEnvelopeChecksum := sha256.Sum256(encoded[:len(encoded)-sha256.Size])
	if envelopeChecksum != actualEnvelopeChecksum {
		return inspectionCorrupt("batch envelope checksum does not match")
	}
	payload := encoded[historySpoolEnvelopeHeader : len(encoded)-sha256.Size]
	offset := 0
	payloadHasher := sha256.New()
	var previous []byte
	for index := uint32(0); index < recordCount; index++ {
		if len(payload)-offset < 4 {
			return inspectionCorrupt("batch record frame is truncated")
		}
		recordLength := uint64(binary.BigEndian.Uint32(payload[offset : offset+4]))
		offset += 4
		if recordLength == 0 || recordLength > uint64(len(payload)-offset) {
			return inspectionCorrupt("batch record frame length is invalid")
		}
		record := payload[offset : offset+int(recordLength)]
		offset += int(recordLength)
		if previous != nil && bytes.Compare(previous, record) >= 0 {
			return inspectionCorrupt("batch records are not in canonical order")
		}
		previous = record
		_, _ = payloadHasher.Write(record)
	}
	if offset != len(payload) {
		return inspectionCorrupt("batch payload contains trailing data")
	}
	var actualPayloadChecksum [sha256.Size]byte
	copy(actualPayloadChecksum[:], payloadHasher.Sum(nil))
	if payloadChecksum != actualPayloadChecksum {
		return inspectionCorrupt("batch payload checksum does not match")
	}
	return historyBatchEnvelopeInspection{
		BatchID:            batchID,
		Sequence:           sequence,
		CreatedAt:          time.Unix(0, int64(binary.BigEndian.Uint64(encoded[36:44]))).UTC(),
		RoutingFingerprint: routingFingerprint,
		ConfigRevision:     configRevision,
		RecordCount:        recordCount,
		FirstBucket:        firstBucket,
		LastBucket:         lastBucket,
		PayloadLength:      payloadLength,
		PayloadSHA256:      payloadChecksum,
		EnvelopeSHA256:     envelopeChecksum,
		LogicalSize:        logicalSize,
	}, nil
}

func decodeHistoryBatchEnvelopeWithFaults(
	encoded []byte,
	spoolID uuid.UUID,
	maxLogicalSize uint64,
	faults *historySpoolFaults,
) (historyBatchEnvelope, error) {
	corrupt := func(reason string) (historyBatchEnvelope, error) {
		return historyBatchEnvelope{}, fmt.Errorf("%w: %s", ErrSpoolCorrupt, reason)
	}
	inspection, err := inspectHistoryBatchEnvelope(
		encoded,
		spoolID,
		maxLogicalSize,
	)
	if err != nil {
		return historyBatchEnvelope{}, err
	}
	if err = runHistorySpoolFault(
		faults,
		func(value *historySpoolFaults) func() error {
			return value.BeforeEnvelopeMaterialization
		},
	); err != nil {
		return historyBatchEnvelope{}, err
	}
	payload := encoded[historySpoolEnvelopeHeader : len(encoded)-sha256.Size]
	batch := make(historyBatch)
	var recordBytes [][]byte
	offset := 0
	for index := uint32(0); index < inspection.RecordCount; index++ {
		recordLength := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		offset += 4
		record := payload[offset : offset+recordLength]
		offset += recordLength
		key, counters, decodeErr := decodeHistoryBatchRecord(record)
		if decodeErr != nil {
			return corrupt(decodeErr.Error())
		}
		if key.ConfigRevision != inspection.ConfigRevision {
			return corrupt("batch record config revision does not match envelope")
		}
		if _, exists := batch[key]; exists {
			return corrupt("batch contains a duplicate record")
		}
		batch[key] = counters
		recordBytes = append(recordBytes, record)
	}
	records, identity, err := encodeHistoryBatch(batch)
	if err != nil {
		return corrupt(err.Error())
	}
	if identity.Count != int(inspection.RecordCount) ||
		identity.Checksum != inspection.PayloadSHA256 ||
		identity.FirstBucket == nil ||
		identity.LastBucket == nil ||
		identity.FirstBucket.Unix() != inspection.FirstBucket ||
		identity.LastBucket.Unix() != inspection.LastBucket ||
		len(records) != len(recordBytes) {
		return corrupt("batch canonical identity does not match envelope")
	}
	for index := range records {
		if !bytes.Equal(records[index].Encoded, recordBytes[index]) {
			return corrupt("batch record encoding is not canonical")
		}
	}
	return historyBatchEnvelope{
		BatchID:            inspection.BatchID,
		Sequence:           inspection.Sequence,
		CreatedAt:          inspection.CreatedAt,
		RoutingFingerprint: inspection.RoutingFingerprint,
		ConfigRevision:     inspection.ConfigRevision,
		RecordCount:        inspection.RecordCount,
		FirstBucket:        inspection.FirstBucket,
		LastBucket:         inspection.LastBucket,
		PayloadSHA256:      inspection.PayloadSHA256,
		EnvelopeSHA256:     inspection.EnvelopeSHA256,
		Batch:              batch,
		Encoded:            append([]byte(nil), encoded...),
	}, nil
}

func addHistorySpoolSize(left uint64, right uint64) (uint64, error) {
	if math.MaxUint64-left < right {
		return 0, errors.New("traffic statistics spool size overflows")
	}
	return left + right, nil
}

func normalizedRecordsToBatch(records []historyBatchRecord) historyBatch {
	batch := make(historyBatch, len(records))
	for _, record := range records {
		batch[record.Key] = record.Counters
	}
	return batch
}

func isHistoryConfigRevision(revision string) bool {
	if len(revision) != 32 {
		return false
	}
	decoded := make([]byte, 16)
	if _, err := hex.Decode(decoded, []byte(revision)); err != nil {
		return false
	}
	return hex.EncodeToString(decoded) == revision
}

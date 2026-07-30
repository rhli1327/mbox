package trafficcontrol

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
)

func TestHistoryBatchEnvelopeIsCanonical(t *testing.T) {
	spoolID := uuid.Must(uuid.FromString("c8643e5c-3b3c-4fcb-9d4d-e6ad9c42c597"))
	fingerprint := sha256.Sum256([]byte("routing"))
	createdAt := time.Unix(123, 456).UTC()
	first := historyBatch{
		historySpoolTestKey("Example.COM."): {
			UplinkBytes:   1,
			DownlinkBytes: 2,
			Connections:   3,
		},
		historySpoolTestKey("example.com"): {
			UplinkBytes:   4,
			DownlinkBytes: 5,
			Connections:   6,
		},
	}
	second := historyBatch{
		historySpoolTestKey("example.com"): {
			UplinkBytes:   4,
			DownlinkBytes: 5,
			Connections:   6,
		},
		historySpoolTestKey("Example.COM."): {
			UplinkBytes:   1,
			DownlinkBytes: 2,
			Connections:   3,
		},
	}
	firstEncoded, firstEnvelope, err := encodeHistoryBatchEnvelope(
		spoolID,
		7,
		createdAt,
		fingerprint,
		historySpoolTestRevision,
		first,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondEncoded, secondEnvelope, err := encodeHistoryBatchEnvelope(
		spoolID,
		7,
		createdAt,
		fingerprint,
		historySpoolTestRevision,
		second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstEncoded, secondEncoded) {
		t.Fatal("envelope depends on map iteration order")
	}
	if firstEnvelope.BatchID != secondEnvelope.BatchID ||
		firstEnvelope.PayloadSHA256 != secondEnvelope.PayloadSHA256 ||
		firstEnvelope.RecordCount != 1 {
		t.Fatalf("unexpected canonical identity: %#v %#v", firstEnvelope, secondEnvelope)
	}
	decoded, err := decodeHistoryBatchEnvelope(
		firstEncoded,
		spoolID,
		historySpoolLogicalSize(firstEncoded),
	)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.BatchID.Version() != uuid.V5 ||
		decoded.Sequence != 7 ||
		decoded.ConfigRevision != historySpoolTestRevision ||
		decoded.BatchID != historyBatchID(spoolID, 7) {
		t.Fatalf("unexpected decoded envelope: %#v", decoded)
	}
	records, identity, err := encodeHistoryBatch(decoded.Batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 ||
		identity.Checksum != decoded.PayloadSHA256 ||
		identity.Count != int(decoded.RecordCount) {
		t.Fatalf("decoded payload changed identity: %#v %#v", decoded, identity)
	}
}

func TestHistoryBatchEnvelopeAllowsIdenticalPayloadsAtDifferentSequences(
	t *testing.T,
) {
	spoolID := uuid.Must(uuid.FromString("54d49da6-2294-4207-8fe0-da1ee9a3c814"))
	fingerprint := sha256.Sum256([]byte("routing"))
	batch := historySpoolTestBatch(1)
	firstBytes, first, err := encodeHistoryBatchEnvelope(
		spoolID,
		1,
		time.Unix(10, 0),
		fingerprint,
		historySpoolTestRevision,
		batch,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, second, err := encodeHistoryBatchEnvelope(
		spoolID,
		2,
		time.Unix(10, 0),
		fingerprint,
		historySpoolTestRevision,
		batch,
	)
	if err != nil {
		t.Fatal(err)
	}
	if first.BatchID == second.BatchID {
		t.Fatal("different sequences reused a batch ID")
	}
	if first.PayloadSHA256 != second.PayloadSHA256 {
		t.Fatal("identical payload changed PostgreSQL checksum")
	}
	if bytes.Equal(firstBytes, secondBytes) {
		t.Fatal("different sequences produced identical envelopes")
	}
}

func TestHistoryBatchEnvelopeRejectsCorruption(t *testing.T) {
	spoolID := uuid.Must(uuid.FromString("54d49da6-2294-4207-8fe0-da1ee9a3c814"))
	encoded, _, err := encodeHistoryBatchEnvelope(
		spoolID,
		1,
		time.Unix(10, 0),
		sha256.Sum256([]byte("routing")),
		historySpoolTestRevision,
		historySpoolTestBatch(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{0, len(encoded) / 2, len(encoded) - 1} {
		corrupt := append([]byte(nil), encoded...)
		corrupt[index] ^= 0xff
		if _, err = decodeHistoryBatchEnvelope(
			corrupt,
			spoolID,
			historySpoolLogicalSize(corrupt),
		); err == nil {
			t.Fatalf("corruption at byte %d was accepted", index)
		}
	}
	if _, err = decodeHistoryBatchEnvelope(
		append(encoded, 0),
		spoolID,
		historySpoolLogicalSize(encoded)+1,
	); err == nil {
		t.Fatal("trailing data was accepted")
	}
}

func TestHistoryBatchEnvelopeRejectsUnsafeAllocationLengths(t *testing.T) {
	spoolID := uuid.Must(uuid.FromString("54d49da6-2294-4207-8fe0-da1ee9a3c814"))
	encoded, _, err := encodeHistoryBatchEnvelope(
		spoolID,
		1,
		time.Unix(10, 0),
		sha256.Sum256([]byte("routing")),
		historySpoolTestRevision,
		historySpoolTestBatch(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	logicalSize := historySpoolLogicalSize(encoded)
	if _, err = decodeHistoryBatchEnvelope(
		encoded,
		spoolID,
		logicalSize-1,
	); !errors.Is(err, ErrSpoolCorrupt) {
		t.Fatalf("logical capacity excluding the key was accepted: %v", err)
	}

	unsafe := append([]byte(nil), encoded...)
	binary.BigEndian.PutUint64(unsafe[132:140], math.MaxUint64)
	checksum := sha256.Sum256(unsafe[:len(unsafe)-sha256.Size])
	copy(unsafe[len(unsafe)-sha256.Size:], checksum[:])
	if _, err = decodeHistoryBatchEnvelope(
		unsafe,
		spoolID,
		uint64(math.MaxInt),
	); !errors.Is(err, ErrSpoolCorrupt) {
		t.Fatalf("unsafe allocation length was accepted: %v", err)
	}
}

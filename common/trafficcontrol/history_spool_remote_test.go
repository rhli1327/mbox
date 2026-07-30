package trafficcontrol

import (
	"errors"
	"testing"
	"time"
)

const historySpoolRemoteAliasRevision = "ffeeddccbbaa99887766554433221100"

func TestPostgresSpoolStoreAliasAdoptionEmptyQueue(t *testing.T) {
	spool := openHistorySpoolForTest(t, historySpoolTestOptions(t, 1<<20))
	defer spool.Close()

	remoteState := historySpoolRemoteTestState(historySpoolRemoteAliasRevision)
	if err := spool.reconcileRemote(remoteState); err != nil {
		t.Fatal(err)
	}
	status := spool.Status()
	if status.ConfigRevision != historySpoolRemoteAliasRevision ||
		!status.RemoteRevisionConfirmed ||
		!status.TargetAvailableFrom.Equal(remoteState.targetsFrom) ||
		!status.DestinationAvailableFrom.Equal(remoteState.destinationsFrom) {
		t.Fatalf("unexpected reconciled status: %#v", status)
	}
}

func TestPostgresSpoolStoreAliasConflictNonEmpty(t *testing.T) {
	spool := openHistorySpoolForTest(t, historySpoolTestOptions(t, 1<<20))
	defer spool.Close()
	if _, err := spool.Append(historySpoolTestBatch(1)); err != nil {
		t.Fatal(err)
	}

	err := spool.reconcileRemote(
		historySpoolRemoteTestState(historySpoolRemoteAliasRevision),
	)
	if !errors.Is(err, ErrPostgresRevisionConflict) {
		t.Fatalf("unexpected reconciliation error: %v", err)
	}
	status := spool.Status()
	if status.ConfigRevision != historySpoolTestRevision ||
		status.RemoteRevisionConfirmed {
		t.Fatalf("conflict changed spool metadata: %#v", status)
	}
}

func TestPostgresSpoolStoreAliasConflictAfterConfirmed(t *testing.T) {
	spool := openHistorySpoolForTest(t, historySpoolTestOptions(t, 1<<20))
	defer spool.Close()
	if err := spool.reconcileRemote(
		historySpoolRemoteTestState(historySpoolTestRevision),
	); err != nil {
		t.Fatal(err)
	}

	err := spool.reconcileRemote(
		historySpoolRemoteTestState(historySpoolRemoteAliasRevision),
	)
	if !errors.Is(err, ErrPostgresRevisionConflict) {
		t.Fatalf("unexpected reconciliation error: %v", err)
	}
	status := spool.Status()
	if status.ConfigRevision != historySpoolTestRevision ||
		!status.RemoteRevisionConfirmed {
		t.Fatalf("confirmed conflict changed spool metadata: %#v", status)
	}
}

func TestPostgresSpoolStoreAliasPersistence(t *testing.T) {
	options := historySpoolTestOptions(t, 1<<20)
	spool := openHistorySpoolForTest(t, options)
	remoteState := historySpoolRemoteTestState(historySpoolRemoteAliasRevision)
	if err := spool.reconcileRemote(remoteState); err != nil {
		t.Fatal(err)
	}
	if err := spool.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, state, err := openHistorySpool(options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	status := reopened.Status()
	if state.configRevision != historySpoolRemoteAliasRevision ||
		status.ConfigRevision != historySpoolRemoteAliasRevision ||
		!status.RemoteRevisionConfirmed {
		t.Fatalf("alias was not persisted: %#v %#v", state, status)
	}
}

func historySpoolRemoteTestState(revision string) historyStoreState {
	return historyStoreState{
		configRevision: revision,
		targetsFrom: time.Date(
			2026, 7, 29, 12, 40, 0, 0, time.UTC,
		),
		destinationsFrom: time.Date(
			2026, 7, 29, 12, 41, 0, 0, time.UTC,
		),
	}
}

package trafficcontrol

import (
	"fmt"

	"github.com/sagernet/bbolt"
)

func (s *historySpool) reconcileRemote(state historyStoreState) error {
	if !isHistoryConfigRevision(state.configRevision) ||
		state.targetsFrom.IsZero() ||
		state.destinationsFrom.Before(state.targetsFrom) {
		return fmt.Errorf(
			"%w: remote state is invalid",
			ErrPostgresRevisionConflict,
		)
	}

	s.access.Lock()
	defer s.access.Unlock()
	if s.closed || s.db == nil {
		return ErrSpoolClosed
	}
	err := updateHistorySpoolDatabase(
		s.db,
		s.faults,
		"reconcile_remote",
		func(tx *bbolt.Tx) error {
			status, validateErr := validateHistorySpoolTransaction(
				tx,
				historySpoolSafeLogicalSize,
				true,
				s.faults,
			)
			if validateErr != nil {
				return validateErr
			}
			if status.ConfigRevision != state.configRevision &&
				(status.RemoteRevisionConfirmed || status.QueueDepth != 0) {
				return ErrPostgresRevisionConflict
			}
			metadata := tx.Bucket(historySpoolMetadataBucket)
			for _, item := range []struct {
				key   []byte
				value []byte
			}{
				{
					historySpoolConfigRevisionKey,
					[]byte(state.configRevision),
				},
				{
					historySpoolRemoteRevisionConfirmedKey,
					[]byte{1},
				},
				{
					historySpoolTargetAvailableFromKey,
					encodeHistorySpoolInt64(state.targetsFrom.UTC().Unix()),
				},
				{
					historySpoolDestinationAvailableFromKey,
					encodeHistorySpoolInt64(
						state.destinationsFrom.UTC().Unix(),
					),
				},
			} {
				if putErr := metadata.Put(item.key, item.value); putErr != nil {
					return putErr
				}
			}
			_, validateErr = validateHistorySpoolTransaction(
				tx,
				historySpoolSafeLogicalSize,
				true,
				s.faults,
			)
			return validateErr
		},
	)
	if err != nil {
		return err
	}
	s.configRevision = state.configRevision
	return nil
}

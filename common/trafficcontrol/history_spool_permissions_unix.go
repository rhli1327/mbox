//go:build !windows

package trafficcontrol

import (
	"fmt"
	"os"
)

func validateHistorySpoolPermissions(mode os.FileMode) error {
	if mode.Perm() != 0o600 {
		return fmt.Errorf(
			"traffic statistics spool permissions must be 0600, got %04o",
			mode.Perm(),
		)
	}
	return nil
}

func syncHistorySpoolParent(parent string) error {
	directory, err := os.Open(parent)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

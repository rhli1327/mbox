//go:build windows

package trafficcontrol

import (
	"fmt"
	"os"
)

func validateHistorySpoolPermissions(mode os.FileMode) error {
	if mode.Perm()&0o200 == 0 {
		return fmt.Errorf("traffic statistics spool must be owner-writable")
	}
	return nil
}

func syncHistorySpoolParent(string) error {
	return nil
}

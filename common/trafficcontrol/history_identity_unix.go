//go:build !windows

package trafficcontrol

import (
	"fmt"
	"os"
)

func publishTrafficIdentity(temporaryPath string, path string) (bool, error) {
	err := os.Link(temporaryPath, path)
	if err == nil {
		return true, nil
	}
	if os.IsExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("publish traffic identity: %w", err)
}

func syncTrafficIdentityParent(parent string) error {
	directory, err := os.Open(parent)
	if err != nil {
		return fmt.Errorf("open traffic identity directory for sync: %w", err)
	}
	defer directory.Close()
	if err = directory.Sync(); err != nil {
		return fmt.Errorf("sync traffic identity directory: %w", err)
	}
	return nil
}

func validateTrafficIdentityPermissions(mode os.FileMode) error {
	if mode.Perm() != 0o600 {
		return fmt.Errorf(
			"traffic identity permissions must be 0600, got %04o",
			mode.Perm(),
		)
	}
	return nil
}

//go:build windows

package trafficcontrol

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func publishTrafficIdentity(temporaryPath string, path string) (bool, error) {
	temporaryPathPointer, err := windows.UTF16PtrFromString(temporaryPath)
	if err != nil {
		return false, fmt.Errorf("encode temporary traffic identity path: %w", err)
	}
	pathPointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return false, fmt.Errorf("encode traffic identity path: %w", err)
	}
	err = windows.MoveFileEx(
		temporaryPathPointer,
		pathPointer,
		windows.MOVEFILE_WRITE_THROUGH,
	)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, windows.ERROR_FILE_EXISTS) ||
		errors.Is(err, windows.ERROR_ALREADY_EXISTS) {
		return false, nil
	}
	return false, fmt.Errorf("publish traffic identity: %w", err)
}

func syncTrafficIdentityParent(string) error {
	return nil
}

func validateTrafficIdentityPermissions(mode os.FileMode) error {
	if mode.Perm()&0o200 == 0 {
		return fmt.Errorf("traffic identity must be owner-writable")
	}
	return nil
}

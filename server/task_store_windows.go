//go:build windows

package server

import (
	"golang.org/x/sys/windows"
)

func replaceTaskStoreFile(tempPath, targetPath string) error {
	source, err := windows.UTF16PtrFromString(tempPath)
	if err != nil {
		return err
	}
	target, err := windows.UTF16PtrFromString(targetPath)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(
		source,
		target,
		windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH,
	)
}

func syncTaskStoreDirectory(string) error {
	// MoveFileEx with WRITE_THROUGH flushes the replacement on Windows.
	return nil
}

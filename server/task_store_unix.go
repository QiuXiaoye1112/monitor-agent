//go:build !windows

package server

import (
	"os"
)

func replaceTaskStoreFile(tempPath, targetPath string) error {
	return os.Rename(tempPath, targetPath)
}

func syncTaskStoreDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

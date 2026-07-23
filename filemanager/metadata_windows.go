//go:build windows

package filemanager

import (
	"errors"
	"io/fs"
)

func fileOwnerGroup(info fs.FileInfo) (owner, group string, uid, gid int64) {
	return "--", "--", -1, -1
}

func lookupUserID(raw string) (int, error) {
	return -1, newRequestError("unsupported", "Windows Agent 暂不支持修改所有者", nil, errors.New("chown is not supported on Windows"))
}

func lookupGroupID(raw string) (int, error) {
	return -1, newRequestError("unsupported", "Windows Agent 暂不支持修改用户组", nil, errors.New("chgrp is not supported on Windows"))
}

func changeOwner(path string, uid, gid int) error {
	return newRequestError("unsupported", "Windows Agent 暂不支持修改所有者或用户组", nil, errors.New("chown is not supported on Windows"))
}

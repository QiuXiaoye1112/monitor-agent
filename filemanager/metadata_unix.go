//go:build !windows

package filemanager

import (
	"fmt"
	"io/fs"
	"os"
	"os/user"
	"strconv"
	"strings"
	"syscall"
)

func fileOwnerGroup(info fs.FileInfo) (owner, group string, uid, gid int64) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "--", "--", -1, -1
	}
	uid, gid = int64(stat.Uid), int64(stat.Gid)
	owner, group = strconv.FormatInt(uid, 10), strconv.FormatInt(gid, 10)
	if value, err := user.LookupId(owner); err == nil && value.Username != "" {
		owner = value.Username
	}
	if value, err := user.LookupGroupId(group); err == nil && value.Name != "" {
		group = value.Name
	}
	return owner, group, uid, gid
}

func lookupUserID(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return -1, newRequestError("invalid_owner", "用户不能为空", nil, nil)
	}
	if id, err := strconv.Atoi(raw); err == nil && id >= 0 {
		return id, nil
	}
	value, err := user.Lookup(raw)
	if err != nil {
		return -1, newRequestError("invalid_owner", "用户不存在", nil, err)
	}
	id, err := strconv.Atoi(value.Uid)
	if err != nil {
		return -1, fmt.Errorf("parse user id: %w", err)
	}
	return id, nil
}

func lookupGroupID(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return -1, newRequestError("invalid_group", "用户组不能为空", nil, nil)
	}
	if id, err := strconv.Atoi(raw); err == nil && id >= 0 {
		return id, nil
	}
	value, err := user.LookupGroup(raw)
	if err != nil {
		return -1, newRequestError("invalid_group", "用户组不存在", nil, err)
	}
	id, err := strconv.Atoi(value.Gid)
	if err != nil {
		return -1, fmt.Errorf("parse group id: %w", err)
	}
	return id, nil
}

func changeOwner(path string, uid, gid int) error { return os.Lchown(path, uid, gid) }

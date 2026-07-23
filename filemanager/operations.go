package filemanager

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"
)

func ensureDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return newRequestError("symlink_not_supported", "不能直接在软链接目录中写入，请进入其实际目标目录后再操作", nil, nil)
	}
	if !info.IsDir() {
		return newRequestError("not_a_directory", "目标位置不是文件夹", nil, nil)
	}
	return nil
}

func isUTF8Text(content []byte) bool {
	if !utf8.Valid(content) || bytes.IndexByte(content, 0) >= 0 {
		return false
	}
	controls := 0
	for _, b := range content {
		if b < 0x09 || (b > 0x0d && b < 0x20) {
			controls++
		}
	}
	return len(content) == 0 || controls*100 < len(content)
}

func removePath(path string, info fs.FileInfo, recursive bool) error {
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return os.Remove(path)
	}
	if !recursive {
		return os.Remove(path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		child := filepath.Join(path, entry.Name())
		childInfo, err := os.Lstat(child)
		if err != nil {
			return err
		}
		if err := removePath(child, childInfo, true); err != nil {
			return err
		}
	}
	return os.Remove(path)
}

func normalizeConflict(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "overwrite", "skip", "rename":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return "ask"
	}
}

func nextAvailablePath(path string) (string, error) {
	dir, name := filepath.Dir(path), filepath.Base(path)
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	for i := 1; i <= 10000; i++ {
		candidate := filepath.Join(dir, fmt.Sprintf("%s (%d)%s", base, i, ext))
		if _, err := os.Lstat(candidate); os.IsNotExist(err) {
			return candidate, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", newRequestError("conflict", "无法生成不冲突的文件名", nil, nil)
}

func (s *service) copyMany(paths []string, destination, conflict string) (interface{}, error) {
	return s.transferMany(paths, destination, conflict, false)
}

func (s *service) moveMany(paths []string, destination, conflict string) (interface{}, error) {
	return s.transferMany(paths, destination, conflict, true)
}

func (s *service) transferMany(paths []string, destination, conflict string, move bool) (interface{}, error) {
	if len(paths) == 0 {
		return nil, newRequestError("invalid_path", "请先选择要操作的文件或目录", nil, nil)
	}
	results := make([]map[string]interface{}, 0, len(paths))
	for _, path := range paths {
		var data interface{}
		var err error
		if move {
			data, err = s.moveWithConflict(path, destination, conflict)
		} else {
			data, err = s.copyWithConflict(path, destination, conflict)
		}
		result := map[string]interface{}{"path": path, "ok": err == nil}
		if err != nil {
			message, code, _ := publicError(err)
			result["error"] = message
			result["code"] = code
		} else {
			result["data"] = data
		}
		results = append(results, result)
	}
	return map[string]interface{}{"results": results}, nil
}

func (s *service) removeMany(paths []string, recursive bool) (interface{}, error) {
	if len(paths) == 0 {
		return nil, newRequestError("invalid_path", "请先选择要删除的文件或目录", nil, nil)
	}
	results := make([]map[string]interface{}, 0, len(paths))
	for _, path := range paths {
		err := s.removeWithOptions(path, recursive)
		result := map[string]interface{}{"path": path, "ok": err == nil}
		if err != nil {
			message, code, _ := publicError(err)
			result["error"] = message
			result["code"] = code
		}
		results = append(results, result)
	}
	return map[string]interface{}{"results": results}, nil
}

func (s *service) properties(raw string) (interface{}, error) {
	path, err := s.cleanPath(raw)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	entry, err := makeFileEntry(filepath.Dir(path), info)
	if err != nil {
		return nil, err
	}
	return entry.toMap(), nil
}

func (s *service) chmod(raw, rawMode string) error {
	path, err := s.cleanPath(raw)
	if err != nil {
		return err
	}
	if err := s.ensureMutablePath(path); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return newRequestError("symlink_not_supported", "不能修改软链接权限，请修改其目标文件", nil, nil)
	}
	rawMode = strings.TrimSpace(strings.TrimPrefix(rawMode, "0"))
	if len(rawMode) != 3 || strings.ContainsAny(rawMode, "89") {
		return newRequestError("invalid_mode", "权限格式无效，请输入 755、644 或 600", nil, nil)
	}
	mode, err := strconv.ParseUint(rawMode, 8, 32)
	if err != nil || mode > 0o777 {
		return newRequestError("invalid_mode", "权限格式无效，请输入 755、644 或 600", nil, err)
	}
	return os.Chmod(path, os.FileMode(mode))
}

func (s *service) chown(raw, owner string) error {
	path, err := s.cleanPath(raw)
	if err != nil {
		return err
	}
	if err := s.ensureMutablePath(path); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err != nil {
		return err
	}
	uid, err := lookupUserID(owner)
	if err != nil {
		return err
	}
	return changeOwner(path, uid, -1)
}

func (s *service) chgrp(raw, group string) error {
	path, err := s.cleanPath(raw)
	if err != nil {
		return err
	}
	if err := s.ensureMutablePath(path); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err != nil {
		return err
	}
	gid, err := lookupGroupID(group)
	if err != nil {
		return err
	}
	return changeOwner(path, -1, gid)
}

package filemanager

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type fileEntry struct {
	name          string
	path          string
	typeName      string
	size          int64
	isDir         bool
	isLink        bool
	linkIsDir     bool
	linkTarget    string
	mode          string
	permissions   string
	owner         string
	group         string
	uid           int64
	gid           int64
	modified      time.Time
	protectedPath bool
	textCandidate bool
}

func makeFileEntry(parent string, info fs.FileInfo) (fileEntry, error) {
	full := filepath.Join(parent, info.Name())
	entry := fileEntry{
		name:          info.Name(),
		path:          full,
		size:          info.Size(),
		isDir:         info.IsDir(),
		isLink:        info.Mode()&os.ModeSymlink != 0,
		mode:          info.Mode().String(),
		permissions:   formatPermissions(info.Mode()),
		modified:      info.ModTime(),
		protectedPath: isSensitivePath(full),
	}
	entry.typeName = fileType(info)
	entry.owner, entry.group, entry.uid, entry.gid = fileOwnerGroup(info)
	if entry.isLink {
		if target, err := os.Readlink(full); err == nil {
			entry.linkTarget = target
		}
		if targetInfo, err := os.Stat(full); err == nil {
			entry.linkIsDir = targetInfo.IsDir()
		}
	}
	entry.textCandidate = !entry.isDir && !entry.isLink && isLikelyTextName(entry.name) && entry.size <= maxTextSize
	return entry, nil
}

func (e fileEntry) toMap() map[string]interface{} {
	return map[string]interface{}{
		"name": e.name, "path": e.path, "type": e.typeName, "size": e.size,
		"is_dir": e.isDir, "is_link": e.isLink, "link_is_dir": e.linkIsDir, "link_target": e.linkTarget,
		"mode": e.mode, "permissions": e.permissions, "owner": e.owner, "group": e.group,
		"uid": e.uid, "gid": e.gid, "modified": e.modified.Format(time.RFC3339),
		"protected": e.protectedPath, "text_candidate": e.textCandidate,
	}
}

func fileType(info fs.FileInfo) string {
	switch {
	case info.Mode()&os.ModeSymlink != 0:
		return "软链接"
	case info.IsDir():
		return "目录"
	case info.Mode().IsRegular():
		return "文件"
	default:
		return "特殊文件"
	}
}

func formatPermissions(mode fs.FileMode) string {
	return "0" + strconv.FormatUint(uint64(mode.Perm()), 8)
}

func sortEntries(entries []fileEntry, sortBy, order string) {
	desc := strings.EqualFold(order, "desc")
	sort.SliceStable(entries, func(i, j int) bool {
		left, right := entries[i], entries[j]
		if left.isDir != right.isDir {
			return left.isDir
		}
		var comparison int
		switch sortBy {
		case "size":
			comparison = compareInt64(left.size, right.size)
		case "modified":
			comparison = compareTime(left.modified, right.modified)
		default:
			comparison = strings.Compare(strings.ToLower(left.name), strings.ToLower(right.name))
		}
		if desc {
			return comparison > 0
		}
		return comparison < 0
	})
}

func compareInt64(left, right int64) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func compareTime(left, right time.Time) int {
	if left.Before(right) {
		return -1
	}
	if left.After(right) {
		return 1
	}
	return 0
}

func isSensitivePath(path string) bool {
	clean := filepath.Clean(path)
	for _, protected := range []string{"/", "/boot", "/etc", "/usr", "/var", "/root", "/home", "/opt"} {
		if clean == protected {
			return true
		}
	}
	return false
}

func isLikelyTextName(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	switch ext {
	case ".conf", ".ini", ".json", ".yaml", ".yml", ".toml", ".env", ".service", ".sh", ".py", ".js", ".ts", ".md", ".txt", ".log", ".xml", ".html", ".css", ".csv":
		return true
	}
	return strings.HasPrefix(name, ".") || strings.Contains(strings.ToLower(name), "readme")
}

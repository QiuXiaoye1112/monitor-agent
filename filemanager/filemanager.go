package filemanager

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
	flagsPkg "monitor-agent/cmd/flags"
)

const (
	maxMessageSize  = 3 << 20
	maxTextSize     = 2 << 20
	maxChunkSize    = 256 << 10
	maxTransferSize = int64(1 << 30)
	maxTransfers    = 4
	maxListEntries  = 1000
	defaultPageSize = 200
)

type request struct {
	ID          string   `json:"id"`
	Type        string   `json:"type"`
	Path        string   `json:"path,omitempty"`
	Paths       []string `json:"paths,omitempty"`
	Destination string   `json:"destination,omitempty"`
	Name        string   `json:"name,omitempty"`
	NewName     string   `json:"new_name,omitempty"`
	Content     string   `json:"content,omitempty"`
	ContentB64  string   `json:"content_base64,omitempty"`
	Data        string   `json:"data,omitempty"`
	UploadID    string   `json:"upload_id,omitempty"`
	DownloadID  string   `json:"download_id,omitempty"`
	Offset      int64    `json:"offset,omitempty"`
	Size        int64    `json:"size,omitempty"`
	Limit       int      `json:"limit,omitempty"`
	Overwrite   bool     `json:"overwrite,omitempty"`
	Recursive   bool     `json:"recursive,omitempty"`
	Force       bool     `json:"force,omitempty"`
	ShowHidden  bool     `json:"show_hidden,omitempty"`
	Sort        string   `json:"sort,omitempty"`
	Order       string   `json:"order,omitempty"`
	Conflict    string   `json:"conflict,omitempty"`
	Mode        string   `json:"mode,omitempty"`
	Owner       string   `json:"owner,omitempty"`
	Group       string   `json:"group,omitempty"`
	Format      string   `json:"format,omitempty"`
}

type response struct {
	Type    string      `json:"type"`
	ID      string      `json:"id,omitempty"`
	OK      bool        `json:"ok"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
	Code    string      `json:"code,omitempty"`
	Details interface{} `json:"details,omitempty"`
}

type uploadState struct {
	file      *os.File
	tempPath  string
	target    string
	size      int64
	written   int64
	overwrite bool
}

type downloadState struct {
	file   *os.File
	path   string
	size   int64
	offset int64
}

type service struct {
	home      string
	root      string
	uploads   map[string]*uploadState
	downloads map[string]*downloadState
}

type Session struct {
	service *service
}

func NewSession() (*Session, error) {
	if flagsPkg.GlobalConfig.DisableWebSsh {
		return nil, errors.New("远程控制已在 Agent 中禁用")
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		home, _ = os.Getwd()
	}
	return &Session{service: &service{
		home:      home,
		root:      filesystemRoot(home),
		uploads:   make(map[string]*uploadState),
		downloads: make(map[string]*downloadState),
	}}, nil
}

func (s *Session) InitialMessage() []byte {
	if s == nil || s.service == nil {
		return nil
	}
	data, _ := json.Marshal(response{Type: "system", OK: true, Data: map[string]interface{}{"home": s.service.home}})
	return data
}

func (s *Session) HandleMessage(payload []byte) []byte {
	if s == nil || s.service == nil {
		return nil
	}
	var req request
	if err := json.Unmarshal(payload, &req); err != nil {
		data, _ := json.Marshal(response{Type: "response", OK: false, Error: "请求格式错误"})
		return data
	}
	data, err := s.service.handle(req)
	resp := response{Type: "response", ID: req.ID, OK: err == nil, Data: data}
	if err != nil {
		log.Printf("file manager operation %q failed: %v", req.Type, err)
		resp.Error, resp.Code, resp.Details = publicError(err)
		resp.Data = nil
	}
	encoded, _ := json.Marshal(resp)
	return encoded
}

func (s *Session) Close() {
	if s != nil && s.service != nil {
		s.service.close()
	}
}

func Start(conn *websocket.Conn) {
	conn.SetReadLimit(maxMessageSize)
	if flagsPkg.GlobalConfig.DisableWebSsh {
		_ = conn.WriteJSON(response{Type: "system", OK: false, Error: "远程控制已在 Agent 中禁用"})
		_ = conn.Close()
		return
	}
	session, err := NewSession()
	if err != nil {
		_ = conn.WriteJSON(response{Type: "system", OK: false, Error: err.Error()})
		_ = conn.Close()
		return
	}
	defer session.Close()
	defer conn.Close()
	_ = conn.WriteMessage(websocket.TextMessage, session.InitialMessage())
	for {
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if messageType != websocket.TextMessage {
			_ = conn.WriteJSON(response{Type: "response", OK: false, Error: "只接受 JSON 请求"})
			continue
		}
		if err := conn.WriteMessage(websocket.TextMessage, session.HandleMessage(payload)); err != nil {
			return
		}
	}
}

func (s *service) close() {
	for id, state := range s.uploads {
		_ = state.file.Close()
		_ = os.Remove(state.tempPath)
		delete(s.uploads, id)
	}
	for id, state := range s.downloads {
		_ = state.file.Close()
		delete(s.downloads, id)
	}
}

func (s *service) handle(req request) (interface{}, error) {
	switch req.Type {
	case "ping":
		return map[string]interface{}{"time": time.Now().Unix()}, nil
	case "list":
		return s.list(req.Path, req.Offset, req.Limit, req.ShowHidden, req.Sort, req.Order)
	case "read":
		return s.readText(req.Path, req.Force)
	case "write":
		content := req.Content
		if req.ContentB64 != "" {
			decoded, err := base64.StdEncoding.DecodeString(req.ContentB64)
			if err != nil || !utf8.Valid(decoded) {
				return nil, errors.New("文本内容不是有效的 UTF-8 编码")
			}
			content = string(decoded)
		}
		return nil, s.writeText(req.Path, content)
	case "create":
		return nil, s.create(req.Path, req.Name)
	case "mkdir":
		return nil, s.mkdir(req.Path, req.Name)
	case "rename":
		return nil, s.rename(req.Path, req.NewName)
	case "copy":
		return s.copyWithConflict(req.Path, req.Destination, req.Conflict)
	case "copy_many":
		return s.copyMany(req.Paths, req.Destination, req.Conflict)
	case "move":
		return s.moveWithConflict(req.Path, req.Destination, req.Conflict)
	case "move_many":
		return s.moveMany(req.Paths, req.Destination, req.Conflict)
	case "delete":
		return nil, s.removeWithOptions(req.Path, req.Recursive)
	case "delete_many":
		return s.removeMany(req.Paths, req.Recursive)
	case "properties":
		return s.properties(req.Path)
	case "chmod":
		return nil, s.chmod(req.Path, req.Mode)
	case "chown":
		return nil, s.chown(req.Path, req.Owner)
	case "chgrp":
		return nil, s.chgrp(req.Path, req.Group)
	case "archive":
		return s.archive(req.Paths, req.Path, req.Name, req.Format)
	case "extract":
		return s.extract(req.Path, req.Destination)
	case "upload_start":
		return s.uploadStartWithConflict(req.Path, req.Name, req.Size, req.Overwrite, req.Conflict)
	case "upload_chunk":
		return s.uploadChunk(req.UploadID, req.Offset, req.Data)
	case "upload_finish":
		return nil, s.uploadFinish(req.UploadID)
	case "upload_cancel":
		return nil, s.uploadCancel(req.UploadID)
	case "download_start":
		return s.downloadStart(req.Path)
	case "download_chunk":
		return s.downloadChunk(req.DownloadID, req.Offset)
	case "download_cancel":
		return nil, s.downloadCancel(req.DownloadID)
	default:
		return nil, fmt.Errorf("不支持的文件操作: %s", req.Type)
	}
}

func (s *service) cleanPath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", newRequestError("invalid_path", "路径不能为空", nil, nil)
	}
	for _, part := range strings.FieldsFunc(raw, func(r rune) bool { return r == '/' || r == '\\' }) {
		if part == ".." {
			return "", newRequestError("invalid_path", "路径中不允许包含 ..，请使用规范的绝对路径", nil, nil)
		}
	}
	cleaned := filepath.Clean(raw)
	if !filepath.IsAbs(cleaned) {
		cleaned = filepath.Join(s.home, cleaned)
	}
	abs, err := filepath.Abs(cleaned)
	if err != nil {
		return "", newRequestError("invalid_path", "路径无效", nil, err)
	}
	if !isPathInsideOrEqual(s.root, abs) {
		return "", newRequestError("path_outside_scope", "路径不在文件管理器允许范围内", nil, nil)
	}
	return abs, nil
}

func (s *service) cleanPathOrHome(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return filepath.Clean(s.home), nil
	}
	return s.cleanPath(raw)
}

func filesystemRoot(path string) string {
	volume := filepath.VolumeName(path)
	if volume != "" {
		return volume + string(filepath.Separator)
	}
	return string(filepath.Separator)
}

func isPathInsideOrEqual(root, path string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func (s *service) ensureMutablePath(path string) error {
	if filepath.Clean(path) == filepath.Clean(s.root) || filepath.Dir(path) == path {
		return newRequestError("protected_path", "禁止操作文件系统根目录 /", nil, nil)
	}
	return nil
}

func validName(name string) error {
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name || strings.ContainsAny(name, `/\\`) {
		return errors.New("名称无效")
	}
	return nil
}

func (s *service) list(raw string, offset int64, limit int, showHidden bool, sortBy, order string) (interface{}, error) {
	path, err := s.cleanPathOrHome(raw)
	if err != nil {
		return nil, err
	}
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	infos, err := dir.Readdir(maxListEntries + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	if !showHidden {
		visible := infos[:0]
		for _, info := range infos {
			if !strings.HasPrefix(info.Name(), ".") {
				visible = append(visible, info)
			}
		}
		infos = visible
	}
	truncated := len(infos) > maxListEntries
	if truncated {
		infos = infos[:maxListEntries]
	}
	entries := make([]fileEntry, 0, len(infos))
	for _, info := range infos {
		entry, entryErr := makeFileEntry(path, info)
		if entryErr != nil {
			return nil, entryErr
		}
		entries = append(entries, entry)
	}
	sortEntries(entries, sortBy, order)
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = defaultPageSize
	} else if limit > maxListEntries {
		limit = maxListEntries
	}
	start := int(offset)
	if start > len(entries) {
		start = len(entries)
	}
	end := start + limit
	if end > len(entries) {
		end = len(entries)
	}
	items := make([]map[string]interface{}, 0, end-start)
	for _, entry := range entries[start:end] {
		items = append(items, entry.toMap())
	}
	parent := filepath.Dir(path)
	return map[string]interface{}{
		"path": path, "parent": parent, "items": items, "offset": start, "total": len(entries),
		"has_more": end < len(entries), "truncated": truncated,
	}, nil
}

func (s *service) readText(raw string, force bool) (interface{}, error) {
	path, err := s.cleanPath(raw)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, newRequestError("symlink_not_editable", "不能直接编辑软链接，请编辑其目标文件", nil, nil)
	}
	if !info.Mode().IsRegular() {
		return nil, newRequestError("not_regular_file", "只能编辑普通文件", nil, nil)
	}
	if info.Size() > maxTextSize {
		return nil, newRequestError("text_too_large", "文件超过 2 MiB，不能直接编辑", map[string]interface{}{"size": info.Size(), "limit": maxTextSize}, nil)
	}
	if info.Size() > 512<<10 && !force {
		return nil, newRequestError("text_confirmation_required", "文件较大，加载可能导致页面卡顿，是否继续打开？", map[string]interface{}{"size": info.Size(), "requires_confirmation": true}, nil)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if !isUTF8Text(content) {
		return nil, newRequestError("binary_file", "该文件看起来是二进制文件，不能用文本编辑器打开", nil, nil)
	}
	return map[string]interface{}{
		"path": path, "content_base64": base64.StdEncoding.EncodeToString(content), "encoding": "utf-8", "size": len(content),
	}, nil
}

func (s *service) writeText(raw, content string) error {
	if !utf8.ValidString(content) {
		return newRequestError("invalid_text", "文本内容不是有效的 UTF-8 编码", nil, nil)
	}
	if len(content) > maxTextSize {
		return newRequestError("text_too_large", "文本内容超过 2 MiB 限制", nil, nil)
	}
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
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return newRequestError("not_regular_file", "只能保存到普通文件，不能覆盖目录或软链接", nil, nil)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err = file.WriteString(content); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func (s *service) create(rawDir, name string) error {
	if err := validName(name); err != nil {
		return err
	}
	dir, err := s.cleanPath(rawDir)
	if err != nil {
		return err
	}
	if err := ensureDirectory(dir); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	return file.Close()
}

func (s *service) mkdir(rawDir, name string) error {
	if err := validName(name); err != nil {
		return err
	}
	dir, err := s.cleanPath(rawDir)
	if err != nil {
		return err
	}
	if err := ensureDirectory(dir); err != nil {
		return err
	}
	return os.Mkdir(filepath.Join(dir, name), 0755)
}

func (s *service) rename(raw, newName string) error {
	if err := validName(newName); err != nil {
		return err
	}
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
	target := filepath.Join(filepath.Dir(path), newName)
	if _, err := os.Lstat(target); err == nil {
		return newRequestError("conflict", "目标位置已存在同名文件", nil, nil)
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Rename(path, target)
}

func (s *service) copy(rawSource, rawDestination string) error {
	_, err := s.copyWithConflict(rawSource, rawDestination, "ask")
	return err
}

func (s *service) copyWithConflict(rawSource, rawDestination, conflict string) (interface{}, error) {
	source, target, skipped, err := s.transferPaths(rawSource, rawDestination, conflict)
	if err != nil {
		return nil, err
	}
	if skipped {
		return map[string]interface{}{"skipped": true, "path": target}, nil
	}
	if err := copyPath(source, target); err != nil {
		return nil, err
	}
	return map[string]interface{}{"path": target}, nil
}

func (s *service) move(rawSource, rawDestination string) error {
	_, err := s.moveWithConflict(rawSource, rawDestination, "ask")
	return err
}

func (s *service) moveWithConflict(rawSource, rawDestination, conflict string) (interface{}, error) {
	source, target, skipped, err := s.transferPaths(rawSource, rawDestination, conflict)
	if err != nil {
		return nil, err
	}
	if skipped {
		return map[string]interface{}{"skipped": true, "path": target}, nil
	}
	if err := os.Rename(source, target); err == nil {
		return map[string]interface{}{"path": target}, nil
	}
	if err := copyPath(source, target); err != nil {
		return nil, err
	}
	info, err := os.Lstat(source)
	if err != nil {
		_ = os.RemoveAll(target)
		return nil, err
	}
	err = removePath(source, info, true)
	if err != nil {
		return nil, fmt.Errorf("目标已创建，但无法删除原文件: %w", err)
	}
	return map[string]interface{}{"path": target}, nil
}

func (s *service) transferPaths(rawSource, rawDestination, conflict string) (string, string, bool, error) {
	source, err := s.cleanPath(rawSource)
	if err != nil {
		return "", "", false, err
	}
	if filepath.Dir(source) == source {
		return "", "", false, newRequestError("protected_path", "不能复制或移动文件系统根目录", nil, nil)
	}
	if _, err := os.Lstat(source); err != nil {
		return "", "", false, err
	}
	destination, err := s.cleanPath(rawDestination)
	if err != nil {
		return "", "", false, err
	}
	if err := ensureDirectory(destination); err != nil {
		return "", "", false, err
	}
	target := filepath.Join(destination, filepath.Base(source))
	if target == source {
		return "", "", false, newRequestError("same_path", "源文件和目标位置相同", nil, nil)
	}
	if isPathWithin(source, target) {
		return "", "", false, newRequestError("path_cycle", "不能复制或移动到自身目录中", nil, nil)
	}
	if targetInfo, err := os.Lstat(target); err == nil {
		switch normalizeConflict(conflict) {
		case "skip":
			return source, target, true, nil
		case "rename":
			target, err = nextAvailablePath(target)
			if err != nil {
				return "", "", false, err
			}
		case "overwrite":
			if targetInfo.IsDir() && targetInfo.Mode()&os.ModeSymlink == 0 {
				return "", "", false, newRequestError("conflict", "不能覆盖已有目录，请先选择其他名称", nil, nil)
			}
			if err := os.Remove(target); err != nil {
				return "", "", false, err
			}
		default:
			return "", "", false, newRequestError("conflict", "目标位置已存在同名文件", map[string]bool{"requires_conflict_choice": true}, nil)
		}
	} else if !os.IsNotExist(err) {
		return "", "", false, err
	}
	return source, target, false, nil
}

func isPathWithin(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	if err != nil || relative == "." || relative == ".." {
		return false
	}
	return !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func copyPath(source, target string) (err error) {
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	created := false
	defer func() {
		if err != nil && created {
			_ = os.RemoveAll(target)
		}
	}()

	switch {
	case info.Mode()&os.ModeSymlink != 0:
		linkTarget, readErr := os.Readlink(source)
		if readErr != nil {
			return readErr
		}
		if err = os.Symlink(linkTarget, target); err != nil {
			return err
		}
		created = true
		return nil
	case info.IsDir():
		if err = os.Mkdir(target, info.Mode().Perm()); err != nil {
			return err
		}
		created = true
		entries, readErr := os.ReadDir(source)
		if readErr != nil {
			return readErr
		}
		for _, entry := range entries {
			if err = copyPath(filepath.Join(source, entry.Name()), filepath.Join(target, entry.Name())); err != nil {
				return err
			}
		}
		return os.Chtimes(target, info.ModTime(), info.ModTime())
	case info.Mode().IsRegular():
		input, openErr := os.Open(source)
		if openErr != nil {
			return openErr
		}
		defer input.Close()
		output, createErr := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, info.Mode().Perm())
		if createErr != nil {
			return createErr
		}
		created = true
		if _, err = io.Copy(output, input); err == nil {
			err = output.Sync()
		}
		closeErr := output.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		return os.Chtimes(target, info.ModTime(), info.ModTime())
	default:
		return errors.New("不支持复制特殊设备文件")
	}
}

func (s *service) remove(raw string) error {
	return s.removeWithOptions(raw, false)
}

func (s *service) removeWithOptions(raw string, recursive bool) error {
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
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return os.Remove(path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	if len(entries) > 0 && !recursive {
		return newRequestError("directory_not_empty", "目录包含文件，请确认后递归删除", map[string]bool{"requires_confirmation": true}, syscall.ENOTEMPTY)
	}
	return removePath(path, info, recursive)
}

func (s *service) uploadStart(rawDir, name string, size int64, overwrite bool) (interface{}, error) {
	conflict := "ask"
	if overwrite {
		conflict = "overwrite"
	}
	return s.uploadStartWithConflict(rawDir, name, size, overwrite, conflict)
}

func (s *service) uploadStartWithConflict(rawDir, name string, size int64, overwrite bool, conflict string) (interface{}, error) {
	if len(s.uploads) >= maxTransfers {
		return nil, errors.New("同时上传任务过多")
	}
	if size < 0 || size > maxTransferSize {
		return nil, errors.New("文件超过 1 GiB 传输限制")
	}
	if err := validName(name); err != nil {
		return nil, err
	}
	dir, err := s.cleanPath(rawDir)
	if err != nil {
		return nil, err
	}
	if err := ensureDirectory(dir); err != nil {
		return nil, err
	}
	target := filepath.Join(dir, name)
	if targetInfo, err := os.Lstat(target); err == nil {
		switch normalizeConflict(conflict) {
		case "skip":
			return map[string]interface{}{"skipped": true, "path": target}, nil
		case "rename":
			target, err = nextAvailablePath(target)
			if err != nil {
				return nil, err
			}
			overwrite = false
		case "overwrite":
			if targetInfo.IsDir() && targetInfo.Mode()&os.ModeSymlink == 0 {
				return nil, newRequestError("conflict", "不能用上传文件覆盖已有目录", nil, nil)
			}
			overwrite = true
		default:
			return nil, newRequestError("conflict", "目标文件已存在", map[string]bool{"requires_conflict_choice": true}, nil)
		}
	} else if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	file, err := os.CreateTemp(dir, "."+name+".monitor-upload-*")
	if err != nil {
		return nil, err
	}
	id := randomID()
	s.uploads[id] = &uploadState{file: file, tempPath: file.Name(), target: target, size: size, overwrite: overwrite}
	return map[string]interface{}{"upload_id": id, "chunk_size": maxChunkSize, "path": target}, nil
}

func (s *service) uploadChunk(id string, offset int64, encoded string) (interface{}, error) {
	state, ok := s.uploads[id]
	if !ok {
		return nil, errors.New("上传任务不存在")
	}
	if offset != state.written {
		return nil, errors.New("上传分块偏移不匹配")
	}
	chunk, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, errors.New("上传分块编码无效")
	}
	if len(chunk) > maxChunkSize || state.written+int64(len(chunk)) > state.size {
		return nil, errors.New("上传分块超过限制")
	}
	n, err := state.file.Write(chunk)
	state.written += int64(n)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"written": state.written, "size": state.size}, nil
}

func (s *service) uploadFinish(id string) error {
	state, ok := s.uploads[id]
	if !ok {
		return errors.New("上传任务不存在")
	}
	defer delete(s.uploads, id)
	if state.written != state.size {
		_ = state.file.Close()
		_ = os.Remove(state.tempPath)
		return errors.New("上传数据不完整")
	}
	if err := state.file.Sync(); err != nil {
		_ = state.file.Close()
		_ = os.Remove(state.tempPath)
		return err
	}
	if err := state.file.Close(); err != nil {
		_ = os.Remove(state.tempPath)
		return err
	}
	if state.overwrite {
		if err := os.Remove(state.target); err != nil && !os.IsNotExist(err) {
			_ = os.Remove(state.tempPath)
			return err
		}
	}
	if err := os.Rename(state.tempPath, state.target); err != nil {
		_ = os.Remove(state.tempPath)
		return err
	}
	return os.Chmod(state.target, 0644)
}

func (s *service) uploadCancel(id string) error {
	state, ok := s.uploads[id]
	if !ok {
		return nil
	}
	delete(s.uploads, id)
	_ = state.file.Close()
	return os.Remove(state.tempPath)
}

func (s *service) downloadStart(raw string) (interface{}, error) {
	if len(s.downloads) >= maxTransfers {
		return nil, errors.New("同时下载任务过多")
	}
	path, err := s.cleanPath(raw)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("只能下载普通文件")
	}
	if info.Size() > maxTransferSize {
		return nil, errors.New("文件超过 1 GiB 传输限制")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	id := randomID()
	s.downloads[id] = &downloadState{file: file, path: path, size: info.Size()}
	return map[string]interface{}{"download_id": id, "name": filepath.Base(path), "size": info.Size(), "chunk_size": maxChunkSize}, nil
}

func (s *service) downloadChunk(id string, offset int64) (interface{}, error) {
	state, ok := s.downloads[id]
	if !ok {
		return nil, errors.New("下载任务不存在")
	}
	if offset != state.offset {
		return nil, errors.New("下载分块偏移不匹配")
	}
	remaining := state.size - state.offset
	length := int64(maxChunkSize)
	if remaining < length {
		length = remaining
	}
	buf := make([]byte, int(length))
	n, err := io.ReadFull(state.file, buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	buf = buf[:n]
	state.offset += int64(n)
	eof := state.offset >= state.size
	if eof {
		_ = state.file.Close()
		delete(s.downloads, id)
	}
	return map[string]interface{}{
		"data": base64.StdEncoding.EncodeToString(buf), "offset": offset, "next_offset": state.offset,
		"size": state.size, "eof": eof,
	}, nil
}

func (s *service) downloadCancel(id string) error {
	state, ok := s.downloads[id]
	if !ok {
		return nil
	}
	delete(s.downloads, id)
	return state.file.Close()
}

func randomID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

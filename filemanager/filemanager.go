package filemanager

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	ID         string `json:"id"`
	Type       string `json:"type"`
	Path       string `json:"path,omitempty"`
	Name       string `json:"name,omitempty"`
	NewName    string `json:"new_name,omitempty"`
	Content    string `json:"content,omitempty"`
	ContentB64 string `json:"content_base64,omitempty"`
	Data       string `json:"data,omitempty"`
	UploadID   string `json:"upload_id,omitempty"`
	DownloadID string `json:"download_id,omitempty"`
	Offset     int64  `json:"offset,omitempty"`
	Size       int64  `json:"size,omitempty"`
	Limit      int    `json:"limit,omitempty"`
	Overwrite  bool   `json:"overwrite,omitempty"`
}

type response struct {
	Type  string      `json:"type"`
	ID    string      `json:"id,omitempty"`
	OK    bool        `json:"ok"`
	Data  interface{} `json:"data,omitempty"`
	Error string      `json:"error,omitempty"`
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
	uploads   map[string]*uploadState
	downloads map[string]*downloadState
}

func Start(conn *websocket.Conn) {
	defer conn.Close()
	conn.SetReadLimit(maxMessageSize)
	if flagsPkg.GlobalConfig.DisableWebSsh {
		_ = conn.WriteJSON(response{Type: "system", OK: false, Error: "远程控制已在 Agent 中禁用"})
		return
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		home, _ = os.Getwd()
	}
	svc := &service{home: home, uploads: make(map[string]*uploadState), downloads: make(map[string]*downloadState)}
	defer svc.close()
	_ = conn.WriteJSON(response{Type: "system", OK: true, Data: map[string]interface{}{"home": home}})
	for {
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if messageType != websocket.TextMessage {
			_ = conn.WriteJSON(response{Type: "response", OK: false, Error: "只接受 JSON 请求"})
			continue
		}
		var req request
		if err := json.Unmarshal(payload, &req); err != nil {
			_ = conn.WriteJSON(response{Type: "response", OK: false, Error: "请求格式错误"})
			continue
		}
		data, err := svc.handle(req)
		resp := response{Type: "response", ID: req.ID, OK: err == nil, Data: data}
		if err != nil {
			resp.Error = err.Error()
			resp.Data = nil
		}
		if err := conn.WriteJSON(resp); err != nil {
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
		return s.list(req.Path, req.Offset, req.Limit)
	case "read":
		return s.readText(req.Path)
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
	case "delete":
		return nil, s.remove(req.Path)
	case "upload_start":
		return s.uploadStart(req.Path, req.Name, req.Size, req.Overwrite)
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
	if strings.TrimSpace(raw) == "" {
		return filepath.Clean(s.home), nil
	}
	cleaned := filepath.Clean(raw)
	if !filepath.IsAbs(cleaned) {
		cleaned = filepath.Join(s.home, cleaned)
	}
	abs, err := filepath.Abs(cleaned)
	if err != nil {
		return "", err
	}
	return abs, nil
}

func validName(name string) error {
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name || strings.ContainsAny(name, `/\\`) {
		return errors.New("名称无效")
	}
	return nil
}

func (s *service) list(raw string, offset int64, limit int) (interface{}, error) {
	path, err := s.cleanPath(raw)
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
	truncated := len(infos) > maxListEntries
	if truncated {
		infos = infos[:maxListEntries]
	}
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].IsDir() != infos[j].IsDir() {
			return infos[i].IsDir()
		}
		return strings.ToLower(infos[i].Name()) < strings.ToLower(infos[j].Name())
	})
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = defaultPageSize
	} else if limit > maxListEntries {
		limit = maxListEntries
	}
	start := int(offset)
	if start > len(infos) {
		start = len(infos)
	}
	end := start + limit
	if end > len(infos) {
		end = len(infos)
	}
	items := make([]map[string]interface{}, 0, end-start)
	for _, info := range infos[start:end] {
		full := filepath.Join(path, info.Name())
		isLink := info.Mode()&os.ModeSymlink != 0
		displayInfo := info
		if isLink {
			if target, statErr := os.Stat(full); statErr == nil {
				displayInfo = target
			}
		}
		items = append(items, map[string]interface{}{
			"name": info.Name(), "path": full, "size": displayInfo.Size(), "is_dir": displayInfo.IsDir(),
			"is_link": isLink, "mode": info.Mode().String(), "modified": info.ModTime().Format(time.RFC3339),
		})
	}
	parent := filepath.Dir(path)
	return map[string]interface{}{
		"path": path, "parent": parent, "items": items, "offset": start, "total": len(infos),
		"has_more": end < len(infos), "truncated": truncated,
	}, nil
}

func (s *service) readText(raw string) (interface{}, error) {
	path, err := s.cleanPath(raw)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("只能编辑普通文件")
	}
	if info.Size() > maxTextSize {
		return nil, errors.New("文本文件超过 2 MiB 限制")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(content) {
		return nil, errors.New("文件不是 UTF-8 文本")
	}
	return map[string]interface{}{
		"path": path, "content_base64": base64.StdEncoding.EncodeToString(content), "encoding": "utf-8", "size": len(content),
	}, nil
}

func (s *service) writeText(raw, content string) error {
	if len(content) > maxTextSize {
		return errors.New("文本内容超过 2 MiB 限制")
	}
	path, err := s.cleanPath(raw)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
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
	return os.Rename(path, filepath.Join(filepath.Dir(path), newName))
}

func (s *service) remove(raw string) error {
	path, err := s.cleanPath(raw)
	if err != nil {
		return err
	}
	if path == filepath.Clean(s.home) || filepath.Dir(path) == path {
		return errors.New("不能删除主目录或文件系统根目录")
	}
	return os.Remove(path)
}

func (s *service) uploadStart(rawDir, name string, size int64, overwrite bool) (interface{}, error) {
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
	target := filepath.Join(dir, name)
	if _, err := os.Lstat(target); err == nil && !overwrite {
		return nil, errors.New("目标文件已存在")
	} else if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	file, err := os.CreateTemp(dir, "."+name+".monitor-upload-*")
	if err != nil {
		return nil, err
	}
	id := randomID()
	s.uploads[id] = &uploadState{file: file, tempPath: file.Name(), target: target, size: size, overwrite: overwrite}
	return map[string]interface{}{"upload_id": id, "chunk_size": maxChunkSize}, nil
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
		_ = os.Remove(state.target)
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

package filemanager

import (
	"archive/zip"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newTestService(t *testing.T) *service {
	t.Helper()
	home := t.TempDir()
	svc := &service{home: home, root: home, uploads: make(map[string]*uploadState), downloads: make(map[string]*downloadState)}
	t.Cleanup(svc.close)
	return svc
}

func TestSessionMultiplexesFileRequestResponses(t *testing.T) {
	session := &Session{service: newTestService(t)}
	initial := session.InitialMessage()
	if len(initial) == 0 || !strings.Contains(string(initial), `"type":"system"`) {
		t.Fatalf("unexpected initial session message: %s", initial)
	}
	response := session.HandleMessage([]byte(`{"id":"request-1","type":"ping"}`))
	if !strings.Contains(string(response), `"type":"response"`) || !strings.Contains(string(response), `"id":"request-1"`) {
		t.Fatalf("unexpected multiplexed response: %s", response)
	}
}

func TestFileOperations(t *testing.T) {
	svc := newTestService(t)
	if _, err := svc.handle(request{Type: "ping"}); err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	if err := svc.mkdir(svc.home, "docs"); err != nil {
		t.Fatal(err)
	}
	if err := svc.create(filepath.Join(svc.home, "docs"), "note.txt"); err != nil {
		t.Fatal(err)
	}
	note := filepath.Join(svc.home, "docs", "note.txt")
	if err := svc.writeText(note, "hello 世界"); err != nil {
		t.Fatal(err)
	}
	read, err := svc.readText(note, true)
	if err != nil {
		t.Fatal(err)
	}
	encoded := read.(map[string]interface{})["content_base64"].(string)
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || string(decoded) != "hello 世界" {
		t.Fatalf("unexpected content: %#v, err: %v", read, err)
	}
	if err := svc.rename(note, "renamed.txt"); err != nil {
		t.Fatal(err)
	}
	list, err := svc.list(filepath.Join(svc.home, "docs"), 0, 10, true, "name", "asc")
	if err != nil {
		t.Fatal(err)
	}
	items := list.(map[string]interface{})["items"].([]map[string]interface{})
	if len(items) != 1 || items[0]["name"] != "renamed.txt" {
		t.Fatalf("unexpected list: %#v", items)
	}
	if err := svc.remove(filepath.Join(svc.home, "docs", "renamed.txt")); err != nil {
		t.Fatal(err)
	}
	if err := svc.remove(filepath.Join(svc.home, "docs")); err != nil {
		t.Fatal(err)
	}
}

func TestChunkedUploadDownload(t *testing.T) {
	svc := newTestService(t)
	want := []byte("chunked file data")
	started, err := svc.uploadStart(svc.home, "data.bin", int64(len(want)), false)
	if err != nil {
		t.Fatal(err)
	}
	uploadID := started.(map[string]interface{})["upload_id"].(string)
	if _, err := svc.uploadChunk(uploadID, 0, base64.StdEncoding.EncodeToString(want[:7])); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.uploadChunk(uploadID, 7, base64.StdEncoding.EncodeToString(want[7:])); err != nil {
		t.Fatal(err)
	}
	if err := svc.uploadFinish(uploadID); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(svc.home, "data.bin")
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(want) {
		t.Fatalf("uploaded data = %q, err = %v", got, err)
	}

	download, err := svc.downloadStart(path)
	if err != nil {
		t.Fatal(err)
	}
	downloadID := download.(map[string]interface{})["download_id"].(string)
	chunk, err := svc.downloadChunk(downloadID, 0)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := base64.StdEncoding.DecodeString(chunk.(map[string]interface{})["data"].(string))
	if err != nil || string(decoded) != string(want) {
		t.Fatalf("downloaded data = %q, err = %v", decoded, err)
	}
}

func TestFileSafetyLimits(t *testing.T) {
	svc := newTestService(t)
	if err := svc.create(svc.home, "../escape"); err == nil {
		t.Fatal("expected invalid name to fail")
	}
	if err := svc.remove(svc.home); err == nil {
		t.Fatal("expected deleting home to fail")
	}
	if _, err := svc.uploadStart(svc.home, "large.bin", maxTransferSize+1, false); err == nil {
		t.Fatal("expected oversized upload to fail")
	}
	if err := svc.writeText(filepath.Join(svc.home, "large.txt"), string(make([]byte, maxTextSize+1))); err == nil {
		t.Fatal("expected oversized text to fail")
	}
}

func TestCopyAndMoveOperations(t *testing.T) {
	svc := newTestService(t)
	sourceDir := filepath.Join(svc.home, "source")
	destinationDir := filepath.Join(svc.home, "destination")
	if err := os.MkdirAll(filepath.Join(sourceDir, "nested"), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(destinationDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "nested", "note.txt"), []byte("copied content"), 0640); err != nil {
		t.Fatal(err)
	}

	if _, err := svc.handle(request{Type: "copy", Path: sourceDir, Destination: destinationDir}); err != nil {
		t.Fatalf("copy failed: %v", err)
	}
	copied := filepath.Join(destinationDir, "source", "nested", "note.txt")
	content, err := os.ReadFile(copied)
	if err != nil || string(content) != "copied content" {
		t.Fatalf("copied content = %q, err = %v", content, err)
	}

	moveTarget := filepath.Join(svc.home, "moved")
	if err := os.Mkdir(moveTarget, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.handle(request{Type: "move", Path: copied, Destination: moveTarget}); err != nil {
		t.Fatalf("move failed: %v", err)
	}
	if _, err := os.Stat(copied); !os.IsNotExist(err) {
		t.Fatalf("source still exists after move: %v", err)
	}
	movedContent, err := os.ReadFile(filepath.Join(moveTarget, "note.txt"))
	if err != nil || string(movedContent) != "copied content" {
		t.Fatalf("moved content = %q, err = %v", movedContent, err)
	}
}

func TestCopyAndMoveSafety(t *testing.T) {
	svc := newTestService(t)
	sourceDir := filepath.Join(svc.home, "source")
	if err := os.MkdirAll(filepath.Join(sourceDir, "nested"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "file.txt"), []byte("data"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := svc.copy(sourceDir, filepath.Join(sourceDir, "nested")); err == nil {
		t.Fatal("expected copying a directory into itself to fail")
	}
	if err := svc.copy(filepath.Join(sourceDir, "file.txt"), sourceDir); err == nil {
		t.Fatal("expected copying over an existing file to fail")
	}
	if err := svc.move(svc.home, sourceDir); err == nil {
		t.Fatal("expected moving the home directory to fail")
	}
}

func TestListHonorsRequestedLimitUpToMaximum(t *testing.T) {
	svc := newTestService(t)
	for i := 0; i < 250; i++ {
		name := filepath.Join(svc.home, fmt.Sprintf("file-%03d.txt", i))
		if err := os.WriteFile(name, []byte("test"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	result, err := svc.list("", 0, 1000, true, "name", "asc")
	if err != nil {
		t.Fatal(err)
	}
	items := result.(map[string]interface{})["items"].([]map[string]interface{})
	if len(items) != 250 {
		t.Fatalf("got %d items, want 250", len(items))
	}
}

func TestDeleteNonEmptyDirectoryRequiresRecursiveConfirmation(t *testing.T) {
	svc := newTestService(t)
	dir := filepath.Join(svc.home, "nested")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "note.txt"), []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := svc.removeWithOptions(dir, false); err == nil {
		t.Fatal("expected non-empty directory to require confirmation")
	} else if _, code, _ := publicError(err); code != "directory_not_empty" {
		t.Fatalf("unexpected error code: %s (%v)", code, err)
	}
	if err := svc.removeWithOptions(dir, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(dir); !os.IsNotExist(err) {
		t.Fatalf("directory still exists: %v", err)
	}
}

func TestDeleteSymlinkDoesNotDeleteTarget(t *testing.T) {
	svc := newTestService(t)
	target := filepath.Join(svc.home, "target")
	link := filepath.Join(svc.home, "link")
	if err := os.Mkdir(target, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target, "keep.txt"), []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := svc.removeWithOptions(link, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(link); !os.IsNotExist(err) {
		t.Fatalf("link still exists: %v", err)
	}
	if content, err := os.ReadFile(filepath.Join(target, "keep.txt")); err != nil || string(content) != "keep" {
		t.Fatalf("symlink target changed: %q, %v", content, err)
	}
}

func TestBatchDeleteAndPathScope(t *testing.T) {
	svc := newTestService(t)
	first := filepath.Join(svc.home, "first.txt")
	second := filepath.Join(svc.home, "second")
	if err := os.WriteFile(first, []byte("first"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(second, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(second, "child"), []byte("second"), 0644); err != nil {
		t.Fatal(err)
	}
	result, err := svc.removeMany([]string{first, second}, true)
	if err != nil {
		t.Fatal(err)
	}
	entries := result.(map[string]interface{})["results"].([]map[string]interface{})
	if len(entries) != 2 || !entries[0]["ok"].(bool) || !entries[1]["ok"].(bool) {
		t.Fatalf("unexpected batch result: %#v", result)
	}
	if _, err := svc.cleanPath("/etc/passwd"); err == nil {
		t.Fatal("expected path outside allowed root to fail")
	}
	if _, err := svc.cleanPath("../../etc/passwd"); err == nil {
		t.Fatal("expected traversal path to fail")
	}
	if err := svc.removeWithOptions("", true); err == nil {
		t.Fatal("expected empty delete path to fail")
	}
}

func TestBinaryFilesAreNotOpenedAsText(t *testing.T) {
	svc := newTestService(t)
	path := filepath.Join(svc.home, "binary.bin")
	if err := os.WriteFile(path, []byte{0, 1, 2, 3}, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.readText(path, true); err == nil {
		t.Fatal("expected binary file to be rejected")
	} else if _, code, _ := publicError(err); code != "binary_file" {
		t.Fatalf("unexpected error code: %s (%v)", code, err)
	}
}

func TestArchiveExtractionRejectsTraversal(t *testing.T) {
	svc := newTestService(t)
	source := filepath.Join(svc.home, "source.txt")
	if err := os.WriteFile(source, []byte("archive"), 0644); err != nil {
		t.Fatal(err)
	}
	archived, err := svc.archive([]string{source}, svc.home, "bundle", "zip")
	if err != nil {
		t.Fatal(err)
	}
	archivePath := archived.(map[string]interface{})["path"].(string)
	destination := filepath.Join(svc.home, "out")
	if err := os.Mkdir(destination, 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.extract(archivePath, destination); err != nil {
		t.Fatal(err)
	}
	if content, err := os.ReadFile(filepath.Join(destination, "source.txt")); err != nil || string(content) != "archive" {
		t.Fatalf("unexpected extracted content %q, %v", content, err)
	}
	malicious := filepath.Join(svc.home, "malicious.zip")
	output, err := os.Create(malicious)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(output)
	entry, err := writer.Create("../../outside.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Write([]byte("must not escape")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.extract(malicious, destination); err == nil {
		t.Fatal("expected malicious archive extraction to fail")
	} else if _, code, _ := publicError(err); code != "archive_path_traversal" {
		t.Fatalf("unexpected error code: %s (%v)", code, err)
	}
	if _, err := os.Stat(filepath.Join(svc.home, "outside.txt")); !os.IsNotExist(err) {
		t.Fatalf("archive wrote outside destination: %v", err)
	}
	escape := t.TempDir()
	if err := os.Symlink(escape, filepath.Join(destination, "linked")); err != nil {
		t.Fatal(err)
	}
	symlinkArchive := filepath.Join(svc.home, "symlink.zip")
	symlinkOutput, err := os.Create(symlinkArchive)
	if err != nil {
		t.Fatal(err)
	}
	symlinkWriter := zip.NewWriter(symlinkOutput)
	symlinkEntry, err := symlinkWriter.Create("linked/escaped.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := symlinkEntry.Write([]byte("must not follow link")); err != nil {
		t.Fatal(err)
	}
	if err := symlinkWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := symlinkOutput.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.extract(symlinkArchive, destination); err == nil {
		t.Fatal("expected symlink traversal extraction to fail")
	}
	if _, err := os.Stat(filepath.Join(escape, "escaped.txt")); !os.IsNotExist(err) {
		t.Fatalf("archive followed destination symlink: %v", err)
	}
}

func TestConflictRenameAndChmod(t *testing.T) {
	svc := newTestService(t)
	source := filepath.Join(svc.home, "note.txt")
	destination := filepath.Join(svc.home, "destination")
	if err := os.WriteFile(source, []byte("source"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(destination, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destination, "note.txt"), []byte("existing"), 0644); err != nil {
		t.Fatal(err)
	}
	result, err := svc.copyWithConflict(source, destination, "rename")
	if err != nil {
		t.Fatal(err)
	}
	if got := filepath.Base(result.(map[string]interface{})["path"].(string)); got != "note (1).txt" {
		t.Fatalf("unexpected renamed copy: %q", got)
	}
	if err := svc.chmod(source, "600"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("permissions = %o, want 600", info.Mode().Perm())
	}
}

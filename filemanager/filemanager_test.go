package filemanager

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func newTestService(t *testing.T) *service {
	t.Helper()
	home := t.TempDir()
	svc := &service{home: home, uploads: make(map[string]*uploadState), downloads: make(map[string]*downloadState)}
	t.Cleanup(svc.close)
	return svc
}

func TestFileOperations(t *testing.T) {
	svc := newTestService(t)
	if _, err := svc.handle(request{Type: "ping"}); err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	if err := svc.mkdir("", "docs"); err != nil {
		t.Fatal(err)
	}
	if err := svc.create(filepath.Join(svc.home, "docs"), "note.txt"); err != nil {
		t.Fatal(err)
	}
	note := filepath.Join(svc.home, "docs", "note.txt")
	if err := svc.writeText(note, "hello 世界"); err != nil {
		t.Fatal(err)
	}
	read, err := svc.readText(note)
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
	list, err := svc.list(filepath.Join(svc.home, "docs"), 0, 10)
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
	started, err := svc.uploadStart("", "data.bin", int64(len(want)), false)
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
	if err := svc.create("", "../escape"); err == nil {
		t.Fatal("expected invalid name to fail")
	}
	if err := svc.remove(svc.home); err == nil {
		t.Fatal("expected deleting home to fail")
	}
	if _, err := svc.uploadStart("", "large.bin", maxTransferSize+1, false); err == nil {
		t.Fatal("expected oversized upload to fail")
	}
	if err := svc.writeText(filepath.Join(svc.home, "large.txt"), string(make([]byte, maxTextSize+1))); err == nil {
		t.Fatal("expected oversized text to fail")
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

	result, err := svc.list("", 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	items := result.(map[string]interface{})["items"].([]map[string]interface{})
	if len(items) != 250 {
		t.Fatalf("got %d items, want 250", len(items))
	}
}

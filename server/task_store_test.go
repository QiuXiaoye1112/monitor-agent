package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v2 "monitor-agent/protocol/v2"
)

func TestTaskStorePersistsLifecycleAndDeduplicates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "tasks.json")
	store, err := newTaskStore(path)
	if err != nil {
		t.Fatalf("new task store: %v", err)
	}
	if err := store.AcceptTask("task-1", "first command"); err != nil {
		t.Fatalf("accept task: %v", err)
	}
	if err := store.AcceptTask("task-1", "must not replace"); err != nil {
		t.Fatalf("accept duplicate task: %v", err)
	}
	task, ok := store.Get("task-1")
	if !ok || task.Command != "first command" || task.Status != taskStatusAccepted {
		t.Fatalf("unexpected accepted task: %+v, exists=%t", task, ok)
	}
	if err := store.MarkTaskRunning("task-1"); err != nil {
		t.Fatalf("mark running: %v", err)
	}
	finishedAt := time.Now().UTC().Round(0)
	if err := store.CompleteTask("task-1", "done", 7, finishedAt); err != nil {
		t.Fatalf("complete task: %v", err)
	}

	reloaded, err := newTaskStore(path)
	if err != nil {
		t.Fatalf("reload task store: %v", err)
	}
	task, ok = reloaded.Get("task-1")
	if !ok || task.Status != taskStatusCompleted || task.Result != "done" ||
		task.ExitCode != 7 || !task.FinishedAt.Equal(finishedAt) {
		t.Fatalf("unexpected reloaded task: %+v, exists=%t", task, ok)
	}
	if err := reloaded.DeleteTask("task-1"); err != nil {
		t.Fatalf("delete task: %v", err)
	}
	again, err := newTaskStore(path)
	if err != nil {
		t.Fatalf("reload empty task store: %v", err)
	}
	if _, ok := again.Get("task-1"); ok {
		t.Fatal("deleted task was restored")
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary task file remains: %v", err)
	}
}

func TestTaskStoreRecoveryDoesNotReexecuteRunningTask(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.json")
	store, err := newTaskStore(path)
	if err != nil {
		t.Fatalf("new task store: %v", err)
	}
	if err := store.AcceptTask("accepted", "safe"); err != nil {
		t.Fatal(err)
	}
	if err := store.AcceptTask("running", "side effect"); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkTaskRunning("running"); err != nil {
		t.Fatal(err)
	}
	if err := store.AcceptTask("completed", "done"); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkTaskRunning("completed"); err != nil {
		t.Fatal(err)
	}
	oldFinishedAt := time.Now().Add(-time.Minute).UTC().Round(0)
	if err := store.CompleteTask("completed", "existing", 0, oldFinishedAt); err != nil {
		t.Fatal(err)
	}

	restartAt := time.Now().UTC().Round(0)
	accepted, err := store.RecoverAfterRestart(restartAt)
	if err != nil {
		t.Fatalf("recover tasks: %v", err)
	}
	if len(accepted) != 1 || accepted[0] != "accepted" {
		t.Fatalf("accepted tasks = %v, want [accepted]", accepted)
	}
	running, _ := store.Get("running")
	if running.Status != taskStatusCompleted || running.Result != restartedTaskResult ||
		running.ExitCode != restartedTaskExitCode || !running.FinishedAt.Equal(restartAt) {
		t.Fatalf("running task recovery = %+v", running)
	}
	completed, _ := store.Get("completed")
	if completed.Result != "existing" || !completed.FinishedAt.Equal(oldFinishedAt) {
		t.Fatalf("completed task was modified: %+v", completed)
	}
}

func TestTaskStoreConcurrentDuplicateAcceptIsSingleRecord(t *testing.T) {
	store, err := newTaskStore(filepath.Join(t.TempDir(), "tasks.json"))
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for index := 0; index < 16; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := store.AcceptTask("same", "command"); err != nil {
				t.Errorf("accept duplicate: %v", err)
			}
		}()
	}
	wait.Wait()
	if len(store.tasks) != 1 {
		t.Fatalf("task count = %d, want 1", len(store.tasks))
	}
}

func TestTaskStoreRejectsCorruptState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.json")
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newTaskStore(path); err == nil {
		t.Fatal("corrupt task store was accepted")
	}
}

func TestV2ExecIsNotAcknowledgedWhenPersistenceFails(t *testing.T) {
	blockingFile := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blockingFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}

	durableTaskState.Lock()
	previousStore := durableTaskState.store
	durableTaskState.store = &taskStore{
		path:  filepath.Join(blockingFile, "tasks.json"),
		tasks: make(map[string]DurableTask),
	}
	durableTaskState.Unlock()
	t.Cleanup(func() {
		durableTaskState.Lock()
		durableTaskState.store = previousStore
		durableTaskState.Unlock()
		forgetV2EventSeen("persist-failure")
	})

	accepted := processV2Event(
		nil,
		v2.MethodAgentExec,
		map[string]any{"task_id": "task-failure", "command": "echo unsafe"},
		"persist-failure",
	)
	if accepted {
		t.Fatal("exec event was acknowledged even though persistence failed")
	}
	if !markV2EventSeen("persist-failure") {
		t.Fatal("failed exec event remained marked as seen and could not be replayed")
	}
}

func TestCompletedTaskIsDeletedOnlyAfterHTTP200(t *testing.T) {
	store, err := newTaskStore(filepath.Join(t.TempDir(), "tasks.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AcceptTask("task-upload", "echo done"); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkTaskRunning("task-upload"); err != nil {
		t.Fatal(err)
	}
	finishedAt := time.Now().UTC().Round(0)
	if err := store.CompleteTask("task-upload", "done", 0, finishedAt); err != nil {
		t.Fatal(err)
	}

	var succeed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/clients/task/result" || r.URL.Query().Get("token") != "test-token" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload["task_id"] != "task-upload" {
			http.Error(w, "invalid payload", http.StatusBadRequest)
			return
		}
		if !succeed.Load() {
			http.Error(w, "temporary failure", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	durableTaskState.Lock()
	previousStore := durableTaskState.store
	durableTaskState.store = store
	durableTaskState.Unlock()
	previousHealth := monitorServiceHealth
	monitorServiceHealth = newServiceHealthState()
	monitorServiceHealth.recordPong()
	previousEndpoint, previousToken := flags.Endpoint, flags.Token
	previousRetries, previousPreference := flags.MaxRetries, flags.PreferIPVersion
	flags.Endpoint = server.URL
	flags.Token = "test-token"
	flags.MaxRetries = 0
	flags.PreferIPVersion = ""
	t.Cleanup(func() {
		durableTaskState.Lock()
		durableTaskState.store = previousStore
		durableTaskState.Unlock()
		monitorServiceHealth = previousHealth
		flags.Endpoint = previousEndpoint
		flags.Token = previousToken
		flags.MaxRetries = previousRetries
		flags.PreferIPVersion = previousPreference
	})

	if err := flushDurableTaskResultsOnce(); err == nil {
		t.Fatal("HTTP 500 was treated as a successful task upload")
	}
	if _, ok := store.Get("task-upload"); !ok {
		t.Fatal("task was deleted after failed upload")
	}

	succeed.Store(true)
	if err := flushDurableTaskResultsOnce(); err != nil {
		t.Fatalf("flush after recovery: %v", err)
	}
	if _, ok := store.Get("task-upload"); ok {
		t.Fatal("task remains after confirmed HTTP 200")
	}
}

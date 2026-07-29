package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"
)

const (
	taskStatusAccepted  = "accepted"
	taskStatusRunning   = "running"
	taskStatusCompleted = "completed"

	restartedTaskResult   = "Agent restarted while the command was running. Final command status is unknown."
	restartedTaskExitCode = -2
)

var (
	errTaskNotAccepted      = errors.New("task is not in accepted state")
	errDurableTasksNotReady = errors.New("durable task store is not initialized")
)

type DurableTask struct {
	TaskID     string    `json:"task_id"`
	Command    string    `json:"command"`
	Status     string    `json:"status"`
	Result     string    `json:"result,omitempty"`
	ExitCode   int       `json:"exit_code,omitempty"`
	AcceptedAt time.Time `json:"accepted_at"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

type taskStore struct {
	mu    sync.Mutex
	path  string
	tasks map[string]DurableTask
}

func newTaskStore(path string) (*taskStore, error) {
	store := &taskStore{
		path:  path,
		tasks: make(map[string]DurableTask),
	}
	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func defaultTaskStorePath() string {
	switch runtime.GOOS {
	case "windows":
		base := os.Getenv("ProgramData")
		if base == "" {
			base = `C:\ProgramData`
		}
		return filepath.Join(base, "monitor-agent", "tasks.json")
	case "linux":
		return "/var/lib/monitor-agent/tasks.json"
	default:
		base, err := os.UserConfigDir()
		if err == nil && base != "" {
			return filepath.Join(base, "monitor-agent", "tasks.json")
		}
		return filepath.Join(".", "monitor-agent-tasks.json")
	}
}

func (s *taskStore) load() error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read durable task store: %w", err)
	}
	if len(data) == 0 {
		return errors.New("durable task store is empty")
	}
	if err := json.Unmarshal(data, &s.tasks); err != nil {
		return fmt.Errorf("decode durable task store: %w", err)
	}
	if s.tasks == nil {
		s.tasks = make(map[string]DurableTask)
	}
	for id, task := range s.tasks {
		if id == "" || task.TaskID != id {
			return fmt.Errorf("durable task store contains invalid task key %q", id)
		}
		switch task.Status {
		case taskStatusAccepted, taskStatusRunning, taskStatusCompleted:
		default:
			return fmt.Errorf("durable task %q has invalid status %q", id, task.Status)
		}
	}
	return nil
}

func (s *taskStore) saveLocked() error {
	data, err := json.MarshalIndent(s.tasks, "", "  ")
	if err != nil {
		return fmt.Errorf("encode durable task store: %w", err)
	}
	directory := filepath.Dir(s.path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create durable task directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return fmt.Errorf("secure durable task directory: %w", err)
	}

	tempPath := s.path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open temporary durable task store: %w", err)
	}
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return fmt.Errorf("secure temporary durable task store: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write durable task store: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync durable task store: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close durable task store: %w", err)
	}
	if err := replaceTaskStoreFile(tempPath, s.path); err != nil {
		return fmt.Errorf("replace durable task store: %w", err)
	}
	removeTemp = false
	if err := syncTaskStoreDirectory(directory); err != nil {
		return fmt.Errorf("sync durable task directory: %w", err)
	}
	return nil
}

func (s *taskStore) AcceptTask(taskID, command string) error {
	if taskID == "" {
		return errors.New("task ID is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tasks[taskID]; exists {
		return nil
	}
	s.tasks[taskID] = DurableTask{
		TaskID:     taskID,
		Command:    command,
		Status:     taskStatusAccepted,
		AcceptedAt: time.Now(),
	}
	if err := s.saveLocked(); err != nil {
		delete(s.tasks, taskID)
		return err
	}
	return nil
}

func (s *taskStore) MarkTaskRunning(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[taskID]
	if !ok {
		return os.ErrNotExist
	}
	if task.Status != taskStatusAccepted {
		return errTaskNotAccepted
	}
	task.Status = taskStatusRunning
	s.tasks[taskID] = task
	if err := s.saveLocked(); err != nil {
		task.Status = taskStatusAccepted
		s.tasks[taskID] = task
		return err
	}
	return nil
}

func (s *taskStore) CompleteTask(taskID, result string, exitCode int, finishedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[taskID]
	if !ok {
		return os.ErrNotExist
	}
	previous := task
	task.Status = taskStatusCompleted
	task.Result = result
	task.ExitCode = exitCode
	task.FinishedAt = finishedAt
	s.tasks[taskID] = task
	if err := s.saveLocked(); err != nil {
		s.tasks[taskID] = previous
		return err
	}
	return nil
}

func (s *taskStore) DeleteTask(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[taskID]
	if !ok {
		return nil
	}
	delete(s.tasks, taskID)
	if err := s.saveLocked(); err != nil {
		s.tasks[taskID] = task
		return err
	}
	return nil
}

func (s *taskStore) Get(taskID string) (DurableTask, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[taskID]
	return task, ok
}

func (s *taskStore) CompletedTasks() []DurableTask {
	s.mu.Lock()
	defer s.mu.Unlock()
	completed := make([]DurableTask, 0)
	for _, task := range s.tasks {
		if task.Status == taskStatusCompleted {
			completed = append(completed, task)
		}
	}
	sort.Slice(completed, func(i, j int) bool {
		if completed[i].FinishedAt.Equal(completed[j].FinishedAt) {
			return completed[i].TaskID < completed[j].TaskID
		}
		return completed[i].FinishedAt.Before(completed[j].FinishedAt)
	})
	return completed
}

// RecoverAfterRestart converts indeterminate running tasks into reportable
// completed records and returns accepted tasks that are safe to start.
func (s *taskStore) RecoverAfterRestart(now time.Time) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	accepted := make([]string, 0)
	changed := false
	for id, task := range s.tasks {
		switch task.Status {
		case taskStatusAccepted:
			accepted = append(accepted, id)
		case taskStatusRunning:
			task.Status = taskStatusCompleted
			task.Result = restartedTaskResult
			task.ExitCode = restartedTaskExitCode
			task.FinishedAt = now
			s.tasks[id] = task
			changed = true
		}
	}
	sort.Strings(accepted)
	if changed {
		if err := s.saveLocked(); err != nil {
			return nil, err
		}
	}
	return accepted, nil
}

var durableTaskState struct {
	sync.RWMutex
	store *taskStore
}

func initializeDurableTasks(path string) error {
	store, err := newTaskStore(path)
	if err != nil {
		return err
	}
	accepted, err := store.RecoverAfterRestart(time.Now())
	if err != nil {
		return err
	}
	durableTaskState.Lock()
	durableTaskState.store = store
	durableTaskState.Unlock()
	for _, taskID := range accepted {
		go RunDurableTask(taskID)
	}
	return nil
}

func InitializeDurableTasks() error {
	return initializeDurableTasks(defaultTaskStorePath())
}

func currentDurableTaskStore() (*taskStore, error) {
	durableTaskState.RLock()
	defer durableTaskState.RUnlock()
	if durableTaskState.store == nil {
		return nil, errDurableTasksNotReady
	}
	return durableTaskState.store, nil
}

func AcceptDurableTask(taskID, command string) error {
	store, err := currentDurableTaskStore()
	if err != nil {
		return err
	}
	return store.AcceptTask(taskID, command)
}

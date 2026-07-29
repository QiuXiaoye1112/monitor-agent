package server

import (
	"fmt"
	"testing"
	"time"
)

func TestPendingResultsAreBoundedAndDeduplicated(t *testing.T) {
	pendingResults.Lock()
	originalItems := pendingResults.items
	originalFlushing := pendingResults.flushing
	pendingResults.items = nil
	pendingResults.flushing = false
	pendingResults.Unlock()
	t.Cleanup(func() {
		pendingResults.Lock()
		pendingResults.items = originalItems
		pendingResults.flushing = originalFlushing
		pendingResults.Unlock()
	})

	first := pendingResult{
		kind:       pendingTaskResult,
		taskID:     "same-task",
		taskOutput: "old",
		finishedAt: time.Now(),
	}
	enqueuePendingResult(first)
	first.taskOutput = "new"
	enqueuePendingResult(first)

	pendingResults.Lock()
	if len(pendingResults.items) != 1 || pendingResults.items[0].taskOutput != "new" {
		pendingResults.Unlock()
		t.Fatal("duplicate task result was not replaced")
	}
	pendingResults.items = nil
	pendingResults.Unlock()

	for index := 0; index < maxPendingResults+1; index++ {
		enqueuePendingResult(pendingResult{
			kind:       pendingTaskResult,
			taskID:     fmt.Sprintf("task-%d", index),
			finishedAt: time.Now(),
		})
	}

	pendingResults.Lock()
	defer pendingResults.Unlock()
	if len(pendingResults.items) != maxPendingResults {
		t.Fatalf("pending result count = %d, want %d", len(pendingResults.items), maxPendingResults)
	}
	if pendingResults.items[0].taskID != "task-1" {
		t.Fatalf("oldest retained task = %q, want task-1", pendingResults.items[0].taskID)
	}
}

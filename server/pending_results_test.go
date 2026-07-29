package server

import (
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
		kind:       pendingPingResult,
		pingTaskID: 42,
		pingValue:  1,
		finishedAt: time.Now(),
	}
	enqueuePendingResult(first)
	first.pingValue = 2
	enqueuePendingResult(first)

	pendingResults.Lock()
	if len(pendingResults.items) != 1 || pendingResults.items[0].pingValue != 2 {
		pendingResults.Unlock()
		t.Fatal("duplicate ping result was not replaced")
	}
	pendingResults.items = nil
	pendingResults.Unlock()

	for index := 0; index < maxPendingResults+1; index++ {
		enqueuePendingResult(pendingResult{
			kind:       pendingPingResult,
			pingTaskID: uint(index + 1),
			finishedAt: time.Now(),
		})
	}

	pendingResults.Lock()
	defer pendingResults.Unlock()
	if len(pendingResults.items) != maxPendingResults {
		t.Fatalf("pending result count = %d, want %d", len(pendingResults.items), maxPendingResults)
	}
	if pendingResults.items[0].pingTaskID != 2 {
		t.Fatalf("oldest retained ping task = %d, want 2", pendingResults.items[0].pingTaskID)
	}
}

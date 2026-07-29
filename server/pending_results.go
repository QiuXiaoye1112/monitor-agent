package server

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	v2 "monitor-agent/protocol/v2"
	"monitor-agent/ws"
)

const maxPendingResults = 128

type pendingResultKind uint8

const (
	pendingPingResult pendingResultKind = iota + 1
)

type pendingResult struct {
	kind       pendingResultKind
	pingTaskID uint
	pingValue  int
	finishedAt time.Time
}

var pendingResults struct {
	sync.Mutex
	items    []pendingResult
	flushing bool
}

func enqueuePendingResult(result pendingResult) {
	pendingResults.Lock()
	defer pendingResults.Unlock()

	for index := range pendingResults.items {
		if samePendingResult(pendingResults.items[index], result) {
			pendingResults.items[index] = result
			return
		}
	}
	if len(pendingResults.items) >= maxPendingResults {
		dropped := pendingResults.items[0]
		pendingResults.items = pendingResults.items[1:]
		log.Printf("Pending result queue is full; dropping oldest %s", pendingResultDescription(dropped))
	}
	pendingResults.items = append(pendingResults.items, result)
}

func samePendingResult(left, right pendingResult) bool {
	if left.kind != right.kind {
		return false
	}
	return left.pingTaskID == right.pingTaskID
}

func pendingResultDescription(result pendingResult) string {
	return "ping result"
}

func flushPendingResults(conn *ws.SafeConn, protocolVersion int) {
	pendingResults.Lock()
	if pendingResults.flushing || len(pendingResults.items) == 0 {
		pendingResults.Unlock()
		return
	}
	pendingResults.flushing = true
	pendingResults.Unlock()

	go func() {
		for {
			reportContext, ok := monitorServiceHealth.context()
			if !ok {
				pendingResults.Lock()
				pendingResults.flushing = false
				pendingResults.Unlock()
				return
			}

			pendingResults.Lock()
			if len(pendingResults.items) == 0 {
				pendingResults.flushing = false
				pendingResults.Unlock()
				return
			}
			result := pendingResults.items[0]
			pendingResults.items = pendingResults.items[1:]
			pendingResults.Unlock()

			var err error
			switch result.kind {
			case pendingPingResult:
				if contextErr := reportContext.Err(); contextErr != nil {
					err = contextErr
				} else if conn == nil {
					err = errors.New("WebSocket is not connected")
				} else {
					payload := any(map[string]interface{}{
						"type":        "ping_result",
						"task_id":     result.pingTaskID,
						"value":       result.pingValue,
						"finished_at": result.finishedAt,
					})
					if protocolVersion >= 2 {
						payload = v2.BuildPingResultPayload(result.pingTaskID, result.pingValue, result.finishedAt)
					}
					err = conn.WriteJSON(payload)
				}
			}
			if err == nil {
				continue
			}

			pendingResults.Lock()
			pendingResults.items = append([]pendingResult{result}, pendingResults.items...)
			pendingResults.flushing = false
			pendingResults.Unlock()
			log.Printf("Failed to flush pending %s: %v", pendingResultDescription(result), err)
			return
		}
	}()
}

var durableTaskFlushState struct {
	sync.Mutex
	flushing bool
}

func flushDurableTaskResults() {
	durableTaskFlushState.Lock()
	if durableTaskFlushState.flushing {
		durableTaskFlushState.Unlock()
		return
	}
	durableTaskFlushState.flushing = true
	durableTaskFlushState.Unlock()

	go func() {
		defer func() {
			durableTaskFlushState.Lock()
			durableTaskFlushState.flushing = false
			durableTaskFlushState.Unlock()
		}()
		if err := flushDurableTaskResultsOnce(); err != nil {
			log.Printf("Failed to flush durable task results: %v", err)
		}
	}()
}

func flushDurableTaskResultsOnce() error {
	store, err := currentDurableTaskStore()
	if err != nil {
		return err
	}
	for _, task := range store.CompletedTasks() {
		reportContext, ok := monitorServiceHealth.context()
		if !ok {
			return nil
		}
		if err := sendTaskResult(reportContext, task); err != nil {
			return fmt.Errorf("upload persisted task %s: %w", task.TaskID, err)
		}
		if err := store.DeleteTask(task.TaskID); err != nil {
			return fmt.Errorf("delete uploaded durable task %s: %w", task.TaskID, err)
		}
	}
	return nil
}

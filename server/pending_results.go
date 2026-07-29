package server

import (
	"errors"
	"log"
	"sync"
	"time"

	v2 "monitor-agent/protocol/v2"
	"monitor-agent/ws"
)

const maxPendingResults = 128

type pendingResultKind uint8

const (
	pendingTaskResult pendingResultKind = iota + 1
	pendingPingResult
)

type pendingResult struct {
	kind       pendingResultKind
	taskID     string
	taskOutput string
	exitCode   int
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
	if left.kind == pendingTaskResult {
		return left.taskID == right.taskID
	}
	return left.pingTaskID == right.pingTaskID
}

func pendingResultDescription(result pendingResult) string {
	if result.kind == pendingTaskResult {
		return "task result " + result.taskID
	}
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
			case pendingTaskResult:
				err = sendTaskResult(reportContext, result)
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

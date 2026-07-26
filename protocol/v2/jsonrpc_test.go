package v2

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBuildPingResultPayloadOmitsRedundantPingType(t *testing.T) {
	finishedAt := time.Date(2026, time.July, 25, 1, 2, 3, 0, time.UTC)
	payload := BuildPingResultPayload(7, 25, finishedAt)
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal ping result: %v", err)
	}

	var request struct {
		Params map[string]interface{} `json:"params"`
	}
	if err := json.Unmarshal(encoded, &request); err != nil {
		t.Fatalf("decode ping result: %v", err)
	}
	if _, exists := request.Params["ping_type"]; exists {
		t.Fatalf("ping_type must not be reported: %s", encoded)
	}
	for _, key := range []string{"task_id", "value", "finished_at"} {
		if _, exists := request.Params[key]; !exists {
			t.Fatalf("%s missing from ping result: %s", key, encoded)
		}
	}
}

func TestBuildHistoryReportPayloadUsesIndependentMethod(t *testing.T) {
	payload := BuildHistoryReportPayload([]byte(`{"cpu":{"usage":8.5}}`))
	var request Request
	if err := json.Unmarshal(payload, &request); err != nil {
		t.Fatalf("decode history request: %v", err)
	}
	if request.Method != MethodAgentHistory {
		t.Fatalf("method = %q, want %q", request.Method, MethodAgentHistory)
	}
}

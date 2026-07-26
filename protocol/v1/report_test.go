package v1

import (
	"encoding/json"
	"testing"
)

func TestBuildHistoryReportPayload(t *testing.T) {
	payload := BuildHistoryReportPayload([]byte(`{"cpu":{"usage":12.5}}`))
	var decoded map[string]interface{}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode history payload: %v", err)
	}
	if decoded["type"] != "history_report" {
		t.Fatalf("type = %v, want history_report", decoded["type"])
	}
}

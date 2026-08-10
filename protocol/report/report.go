package report

import "encoding/json"

// ReportPayload is the raw JSON report payload shared by the v2 RPC builders.
// It is an internal data representation, not a legacy transport protocol.
type ReportPayload = []byte

// BuildHistoryReportPayload adds the history marker before the payload is
// wrapped in the v2 JSON-RPC notification.
func BuildHistoryReportPayload(report ReportPayload) []byte {
	var payload map[string]interface{}
	if err := json.Unmarshal(report, &payload); err != nil {
		return report
	}
	payload["type"] = "history_report"
	result, err := json.Marshal(payload)
	if err != nil {
		return report
	}
	return result
}

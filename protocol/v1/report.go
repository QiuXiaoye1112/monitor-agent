package v1

import "encoding/json"

// ReportPayload is the raw JSON payload used by protocol v1 report upload.
// The agent still builds v1 reports from monitoring data dynamically so third-party
// fields can pass through without requiring a shared server dependency.
type ReportPayload = []byte

// BuildHistoryReportPayload adds the v1 message discriminator without changing
// the report shape used by older panel versions.
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

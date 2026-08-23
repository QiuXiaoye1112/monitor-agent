package v2

import (
	"encoding/json"
	"time"

	report "monitor-agent/protocol/report"
)

const (
	Version                          = "2.0"
	MethodAgentReport                = "agent.report"
	MethodAgentHistory               = "agent.historyReport"
	MethodAgentBasicInfo             = "agent.basicInfo"
	MethodAgentPingResult            = "agent.pingResult"
	MethodAgentTaskResult            = "agent.taskResult"
	MethodAgentExec                  = "agent.exec"
	MethodAgentPing                  = "agent.ping"
	MethodAgentMessage               = "agent.message"
	MethodAgentEvent                 = "agent.event"
	MethodAgentTerminal              = "agent.terminal.request"
	MethodAgentFile                  = "agent.file.request"
	MethodAgentPull                  = "agent.pull"
	MethodAgentTrafficSnapshot       = "agent.trafficSnapshot"
	MethodAgentTrafficSnapshotResult = "agent.trafficSnapshotResult"
	MethodAgentTrafficReset          = "agent.trafficReset"
	MethodAgentTrafficResetResult    = "agent.trafficResetResult"
)

type Request struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
	ID      interface{} `json:"id,omitempty"`
	EventID string      `json:"event_id,omitempty"`
}

type Response struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id,omitempty"`
	Result  interface{} `json:"result,omitempty"`
	Error   *RPCError   `json:"error,omitempty"`
}

type RPCError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type Event struct {
	ID        string      `json:"id"`
	Method    string      `json:"method"`
	Params    interface{} `json:"params,omitempty"`
	CreatedAt string      `json:"created_at,omitempty"`
	ExpiresAt string      `json:"expires_at,omitempty"`
}

type EventResult struct {
	Status string  `json:"status,omitempty"`
	Events []Event `json:"events,omitempty"`
}

type TrafficSnapshotParams struct {
	OperationID string `json:"operation_id"`
}

type TrafficSnapshotResultParams struct {
	OperationID    string `json:"operation_id"`
	CapturedAt     string `json:"captured_at"`
	CycleID        string `json:"cycle_id"`
	CycleStartedAt string `json:"cycle_started_at"`
	TotalUp        int64  `json:"total_up"`
	TotalDown      int64  `json:"total_down"`
}

type TrafficResetParams struct {
	OperationID string `json:"operation_id"`
}

type TrafficResetResultParams = TrafficSnapshotResultParams

func NewNotification(method string, params interface{}) []byte {
	payload, _ := json.Marshal(Request{JSONRPC: Version, Method: method, Params: params})
	return payload
}

func NewRequest(id interface{}, method string, params interface{}) []byte {
	payload, _ := json.Marshal(Request{JSONRPC: Version, Method: method, Params: params, ID: id})
	return payload
}

func BuildReportPayload(report report.ReportPayload, ackEventIDs []string) []byte {
	return NewNotification(MethodAgentReport, reportParams{Report: json.RawMessage(report), AckEventIDs: ackEventIDs})
}

func BuildReportRequest(id interface{}, report report.ReportPayload, ackEventIDs []string) []byte {
	return NewRequest(id, MethodAgentReport, reportParams{Report: json.RawMessage(report), AckEventIDs: ackEventIDs})
}

func BuildHistoryReportPayload(report report.ReportPayload) []byte {
	return NewNotification(MethodAgentHistory, reportParams{Report: json.RawMessage(report)})
}

func BuildHistoryReportRequest(id interface{}, report report.ReportPayload) []byte {
	return NewRequest(id, MethodAgentHistory, reportParams{Report: json.RawMessage(report)})
}

func BuildBasicInfoPayload(info map[string]interface{}) []byte {
	return NewNotification(MethodAgentBasicInfo, map[string]interface{}{"info": info})
}

type reportParams struct {
	Report      json.RawMessage `json:"report"`
	AckEventIDs []string        `json:"ack_event_ids,omitempty"`
}

func BuildPingResultPayload(taskID uint, value int, finishedAt time.Time) interface{} {
	return Request{
		JSONRPC: Version,
		Method:  MethodAgentPingResult,
		Params: map[string]interface{}{
			"task_id":     taskID,
			"value":       value,
			"finished_at": finishedAt.Format(time.RFC3339Nano),
		},
	}
}

func BindParams(raw interface{}, target interface{}) error {
	b, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, target)
}

func BindResult(raw interface{}, target interface{}) error {
	return BindParams(raw, target)
}

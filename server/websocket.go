package server

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"monitor-agent/dnsresolver"
	"monitor-agent/filemanager"
	"monitor-agent/monitoring"
	v2 "monitor-agent/protocol/v2"
	"monitor-agent/terminal"
	"monitor-agent/utils"
	"monitor-agent/ws"
)

var (
	v2AckMu                 sync.Mutex
	v2AckEventIDs           []string
	v2SeenEvents            = make(map[string]struct{})
	v2SeenOrder             []string
	v2ResultMu              sync.Mutex
	v2EventResults          = make(map[string]v2EventResult)
	trafficReportBoundaryMu sync.Mutex
)

const (
	maxRememberedV2Events     = 1024
	maxPendingInboundMessages = 128
	heartbeatInterval         = 30 * time.Second
	heartbeatTimeout          = 30 * time.Second
	heartbeatWriteTimeout     = 5 * time.Second
	websocketConnectTimeout   = 30 * time.Second
	maxV2InboundMessageSize   = 4 << 20
	v2EventResultTTL          = 5 * time.Minute
)

type heartbeatPong struct {
	payload    string
	receivedAt time.Time
}

type websocketInboundMessage struct {
	conn    *ws.SafeConn
	payload []byte
}

type v2EventResult struct {
	payload   []byte
	expiresAt time.Time
}

type connectionRetryState struct {
	connectionVerified          bool
	immediateReconnectAvailable bool
}

func (state *connectionRetryState) connected() {
	state.connectionVerified = false
}

func (state *connectionRetryState) pongReceived() {
	state.connectionVerified = true
	state.immediateReconnectAvailable = true
}

func (state *connectionRetryState) connectionFailed() bool {
	reconnectImmediately := state.connectionVerified && state.immediateReconnectAvailable
	state.connectionVerified = false
	if reconnectImmediately {
		state.immediateReconnectAvailable = false
	}
	return reconnectImmediately
}

func EstablishWebSocketConnection() {
	var conn *ws.SafeConn
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	var err error
	interval := math.Max(1, flags.Interval)
	historyInterval := math.Max(1, flags.HistoryInterval)

	dataTicker := time.NewTicker(time.Duration(interval * float64(time.Second)))
	defer dataTicker.Stop()
	historyTicker := time.NewTicker(time.Duration(historyInterval * float64(time.Second)))
	defer historyTicker.Stop()

	heartbeatTimer := time.NewTimer(0)
	defer heartbeatTimer.Stop()

	var readDone <-chan struct{}
	var pongEvents <-chan heartbeatPong
	var pendingHeartbeat string
	var heartbeatDeadline time.Time
	var latestReport []byte
	var retryState connectionRetryState
	inboundMessages := make(chan websocketInboundMessage, maxPendingInboundMessages)
	pendingInboundMessages := make([]websocketInboundMessage, 0, maxPendingInboundMessages)

	resetHeartbeatTimer := func(delay time.Duration) {
		if !heartbeatTimer.Stop() {
			select {
			case <-heartbeatTimer.C:
			default:
			}
		}
		heartbeatTimer.Reset(delay)
	}

	clearPendingHeartbeat := func() {
		pendingHeartbeat = ""
		heartbeatDeadline = time.Time{}
	}

	closeConnection := func() {
		clearPendingHeartbeat()
		if conn != nil {
			conn.Close()
			conn = nil
		}
		readDone = nil
		pongEvents = nil
		pendingInboundMessages = pendingInboundMessages[:0]
	}

	activateConnection := func(newConn *ws.SafeConn) {
		clearPendingHeartbeat()
		conn = newConn
		retryState.connected()

		pongChannel := make(chan heartbeatPong, 4)
		configureHeartbeat(conn, pongChannel)
		pongEvents = pongChannel

		done := make(chan struct{})
		readDone = done
		go handleWebSocketMessages(conn, inboundMessages, done)
	}

	handleConnectionFailure := func(reason string, countMiss bool) {
		canReconnectImmediately := retryState.connectionFailed()
		closeConnection()
		if canReconnectImmediately {
			log.Printf("WebSocket disconnected (%s); attempting one immediate reconnect", reason)
			resetHeartbeatTimer(0)
			return
		}
		if countMiss {
			recordHeartbeatMiss(reason)
		}
		resetHeartbeatTimer(heartbeatInterval)
	}

	processPendingInboundMessages := func() {
		for _, inbound := range pendingInboundMessages {
			processWebSocketMessage(inbound.conn, inbound.payload)
		}
		pendingInboundMessages = pendingInboundMessages[:0]
	}

	acceptHeartbeatPong := func(pong heartbeatPong) bool {
		if !isTimelyHeartbeatPong(pong, pendingHeartbeat, heartbeatDeadline) {
			return false
		}
		clearPendingHeartbeat()
		retryState.pongReceived()
		recordHeartbeatPong()
		flushPendingResults(conn)
		flushDurableTaskResults()
		processPendingInboundMessages()
		resetHeartbeatTimer(heartbeatInterval)
		return true
	}

	takeQueuedTimelyPong := func() bool {
		for pongEvents != nil {
			select {
			case pong := <-pongEvents:
				if acceptHeartbeatPong(pong) {
					return true
				}
			default:
				return false
			}
		}
		return false
	}

	for {
		select {
		case <-dataTicker.C:
			if conn == nil || !monitorServiceHealth.canReport() {
				continue
			}
			trafficReportBoundaryMu.Lock()
			data := monitoring.GenerateReport()
			latestReport = append(latestReport[:0], data...)
			ackIDs := snapshotV2AckEventIDs()
			data = v2.BuildReportPayload(data, ackIDs)
			err = conn.WriteMessage(websocket.TextMessage, data)
			if err == nil {
				clearV2AckEventIDs(ackIDs)
			}
			trafficReportBoundaryMu.Unlock()
			if err != nil {
				log.Println("Failed to send WebSocket message:", err)
				handleConnectionFailure(err.Error(), true)
				continue
			}
			// 基础信息随普通数据上报一起检查；只有内容发生变化时才会上传。
			checkAndUploadBasicInfo(false)
		case <-historyTicker.C:
			if !monitorServiceHealth.canReport() || conn == nil || len(latestReport) == 0 {
				continue
			}
			data := v2.BuildHistoryReportPayload(latestReport)
			if err = conn.WriteMessage(websocket.TextMessage, data); err != nil {
				log.Println("Failed to send history report:", err)
				handleConnectionFailure(err.Error(), true)
			}
		case <-heartbeatTimer.C:
			missRecorded := false
			if pendingHeartbeat != "" {
				if takeQueuedTimelyPong() {
					continue
				}
				timedOutHeartbeat := pendingHeartbeat
				clearPendingHeartbeat()
				recordHeartbeatMiss(fmt.Sprintf("%s did not receive Pong within %s", timedOutHeartbeat, heartbeatTimeout))
				missRecorded = true
			}
			if conn == nil {
				websocketEndpoint := buildWebSocketEndpoint()
				newConn, connectErr := connectWebSocket(websocketEndpoint)
				if connectErr != nil {
					if !missRecorded {
						recordHeartbeatMiss(connectErr.Error())
					}
					resetHeartbeatTimer(heartbeatInterval)
					continue
				}
				activateConnection(newConn)
				log.Printf("WebSocket heartbeat probe connected using v2 protocol")
			}

			payload := fmt.Sprintf("heartbeat-%d", time.Now().UnixNano())
			if err = sendHeartbeat(conn, payload); err != nil {
				log.Println("Failed to send heartbeat:", err)
				handleConnectionFailure(err.Error(), !missRecorded)
				continue
			}
			pendingHeartbeat = payload
			heartbeatDeadline = time.Now().Add(heartbeatTimeout)
			resetHeartbeatTimer(heartbeatTimeout)
		case pong := <-pongEvents:
			acceptHeartbeatPong(pong)
		case inbound := <-inboundMessages:
			if inbound.conn != conn {
				continue
			}
			if monitorServiceHealth.canReport() {
				processWebSocketMessage(inbound.conn, inbound.payload)
				continue
			}
			if len(pendingInboundMessages) >= maxPendingInboundMessages {
				handleConnectionFailure("inbound event buffer is full", true)
				continue
			}
			pendingInboundMessages = append(pendingInboundMessages, inbound)
		case <-readDone:
			handleConnectionFailure("read loop ended", true)
		}
	}
}

func configureHeartbeat(conn *ws.SafeConn, pongEvents chan<- heartbeatPong) {
	conn.SetPongHandler(func(payload string) error {
		pong := heartbeatPong{payload: payload, receivedAt: time.Now()}
		select {
		case pongEvents <- pong:
		default:
		}
		return nil
	})
}

func sendHeartbeat(conn *ws.SafeConn, payload string) error {
	return conn.WriteControl(
		websocket.PingMessage,
		[]byte(payload),
		time.Now().Add(heartbeatWriteTimeout),
	)
}

func isTimelyHeartbeatPong(pong heartbeatPong, pendingPayload string, deadline time.Time) bool {
	return pendingPayload != "" &&
		pong.payload == pendingPayload &&
		!pong.receivedAt.After(deadline)
}

func buildWebSocketEndpoint() string {
	websocketEndpoint := strings.TrimSuffix(flags.Endpoint, "/") + "/api/clients/v2/rpc?token=" + flags.Token
	websocketEndpoint = "ws" + strings.TrimPrefix(websocketEndpoint, "http")
	if convertedEndpoint, err := utils.ConvertIDNToASCII(websocketEndpoint); err == nil {
		return convertedEndpoint
	} else {
		log.Printf("Warning: Failed to convert WebSocket IDN to ASCII: %v", err)
	}
	return websocketEndpoint
}

func snapshotV2AckEventIDs() []string {
	v2AckMu.Lock()
	defer v2AckMu.Unlock()
	return append([]string{}, v2AckEventIDs...)
}

func clearV2AckEventIDs(sent []string) {
	if len(sent) == 0 {
		return
	}
	sentSet := make(map[string]struct{}, len(sent))
	for _, id := range sent {
		sentSet[id] = struct{}{}
	}
	v2AckMu.Lock()
	defer v2AckMu.Unlock()
	remaining := v2AckEventIDs[:0]
	for _, id := range v2AckEventIDs {
		if _, ok := sentSet[id]; !ok {
			remaining = append(remaining, id)
		}
	}
	v2AckEventIDs = remaining
}

func addV2AckEventID(id string) {
	if id == "" {
		return
	}
	v2AckMu.Lock()
	defer v2AckMu.Unlock()
	v2AckEventIDs = append(v2AckEventIDs, id)
}

func rememberV2EventResult(id string, payload []byte) {
	if id == "" || len(payload) == 0 {
		return
	}
	v2ResultMu.Lock()
	defer v2ResultMu.Unlock()
	now := time.Now()
	for eventID, result := range v2EventResults {
		if !result.expiresAt.After(now) {
			delete(v2EventResults, eventID)
		}
	}
	if len(v2EventResults) >= maxRememberedV2Events {
		return
	}
	v2EventResults[id] = v2EventResult{
		payload:   append([]byte(nil), payload...),
		expiresAt: now.Add(v2EventResultTTL),
	}
}

func takeV2EventResult(id string) ([]byte, bool) {
	if id == "" {
		return nil, false
	}
	v2ResultMu.Lock()
	defer v2ResultMu.Unlock()
	result, ok := v2EventResults[id]
	if !ok {
		return nil, false
	}
	if !result.expiresAt.After(time.Now()) {
		delete(v2EventResults, id)
		return nil, false
	}
	return append([]byte(nil), result.payload...), true
}

func markV2EventSeen(id string) bool {
	if id == "" {
		return true
	}
	v2AckMu.Lock()
	defer v2AckMu.Unlock()
	if _, ok := v2SeenEvents[id]; ok {
		return false
	}
	v2SeenEvents[id] = struct{}{}
	v2SeenOrder = append(v2SeenOrder, id)
	if len(v2SeenOrder) > maxRememberedV2Events {
		oldest := v2SeenOrder[0]
		delete(v2SeenEvents, oldest)
		v2SeenOrder = v2SeenOrder[1:]
	}
	return true
}

func connectWebSocket(websocketEndpoint string) (*ws.SafeConn, error) {
	dialer := newWSDialer()

	headers := newWSHeaders()

	conn, resp, err := dialer.Dial(websocketEndpoint, headers)
	if err != nil {
		if resp != nil && resp.StatusCode != 101 {
			return nil, &httpStatusError{StatusCode: resp.StatusCode, Status: resp.Status}
		}
		return nil, err
	}

	safeConn := ws.NewSafeConn(conn)
	safeConn.SetReadLimit(maxV2InboundMessageSize)
	return safeConn, nil
}

func handleWebSocketMessages(
	conn *ws.SafeConn,
	inboundMessages chan<- websocketInboundMessage,
	done chan<- struct{},
) {
	defer close(done)
	for {
		_, message_raw, err := conn.ReadMessage()
		if err != nil {
			log.Println("WebSocket read error:", err)
			return
		}
		message := websocketInboundMessage{
			conn:    conn,
			payload: append([]byte(nil), message_raw...),
		}
		select {
		case inboundMessages <- message:
		default:
			log.Println("WebSocket inbound event buffer is full; reconnecting so unacknowledged events can be replayed")
			return
		}
	}
}

func processWebSocketMessage(conn *ws.SafeConn, messageRaw []byte) {
	var message struct {
		JSONRPC string      `json:"jsonrpc,omitempty"`
		Method  string      `json:"method,omitempty"`
		Params  interface{} `json:"params,omitempty"`
		EventID string      `json:"event_id,omitempty"`
	}
	err := json.Unmarshal(messageRaw, &message)
	if err != nil {
		log.Println("Bad ws message:", err)
		return
	}
	if message.JSONRPC != v2.Version {
		log.Printf("Bad v2 WebSocket message version: %q", message.JSONRPC)
		return
	}
	if processV2Event(conn, message.Method, message.Params, message.EventID) {
		addV2AckEventID(message.EventID)
	}
}

func processV2Event(conn *ws.SafeConn, method string, params interface{}, eventID string) (processed bool) {
	if !markV2EventSeen(eventID) {
		if payload, ok := takeV2EventResult(eventID); ok {
			if conn == nil {
				return false
			}
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				log.Printf("failed to replay v2 event result %s: %v", eventID, err)
				return false
			}
		}
		return true
	}
	defer func() {
		if !processed {
			forgetV2EventSeen(eventID)
		}
	}()
	switch method {
	case v2.MethodAgentExec:
		var p struct {
			TaskID  string `json:"task_id"`
			Command string `json:"command"`
		}
		if err := v2.BindParams(params, &p); err == nil {
			if err := AcceptDurableTask(p.TaskID, p.Command); err != nil {
				log.Printf("failed to persist task %s before ACK: %v", p.TaskID, err)
				forgetV2EventSeen(eventID)
				return false
			}
			go RunDurableTask(p.TaskID)
			return true
		} else {
			log.Printf("bad v2 exec params: %v", err)
		}
	case v2.MethodAgentPing:
		var p struct {
			TaskID uint   `json:"ping_task_id"`
			Type   string `json:"ping_type"`
			Target string `json:"ping_target"`
		}
		if err := v2.BindParams(params, &p); err == nil {
			go NewPingTask(conn, p.TaskID, p.Type, p.Target)
			return true
		} else {
			log.Printf("bad v2 ping params: %v", err)
		}
	case v2.MethodAgentTerminal:
		var p struct {
			RequestID string `json:"request_id"`
		}
		if err := v2.BindParams(params, &p); err == nil {
			go establishWorkspaceConnection(flags.Token, p.RequestID, flags.Endpoint)
			return true
		} else {
			log.Printf("bad v2 terminal params: %v", err)
		}
	case v2.MethodAgentFile:
		var p struct {
			RequestID string `json:"request_id"`
		}
		if err := v2.BindParams(params, &p); err == nil {
			go establishFileConnection(flags.Token, p.RequestID, flags.Endpoint)
			return true
		} else {
			log.Printf("bad v2 file params: %v", err)
		}
	case v2.MethodAgentTrafficSnapshot:
		var p v2.TrafficSnapshotParams
		if err := v2.BindParams(params, &p); err != nil || p.OperationID == "" {
			log.Printf("bad v2 traffic snapshot params: %v", err)
			return false
		}
		trafficReportBoundaryMu.Lock()
		snapshot, err := monitoring.GenerateTrafficSnapshot()
		if err != nil {
			trafficReportBoundaryMu.Unlock()
			log.Printf("failed to capture traffic snapshot: %v", err)
			forgetV2EventSeen(eventID)
			return false
		}
		result := v2.TrafficSnapshotResultParams{
			OperationID:    p.OperationID,
			CapturedAt:     snapshot.CapturedAt.Format(time.RFC3339Nano),
			CycleID:        snapshot.CycleID,
			CycleStartedAt: snapshot.CycleStartedAt.Format(time.RFC3339Nano),
			TotalUp:        snapshot.TotalUp,
			TotalDown:      snapshot.TotalDown,
		}
		payload := v2.NewNotification(v2.MethodAgentTrafficSnapshotResult, result)
		if conn == nil {
			trafficReportBoundaryMu.Unlock()
			forgetV2EventSeen(eventID)
			return false
		}
		err = conn.WriteMessage(websocket.TextMessage, payload)
		trafficReportBoundaryMu.Unlock()
		if err != nil {
			log.Printf("failed to return traffic snapshot: %v", err)
			forgetV2EventSeen(eventID)
			return false
		}
		rememberV2EventResult(eventID, payload)
		return true
	case v2.MethodAgentTrafficReset:
		var p v2.TrafficResetParams
		if err := v2.BindParams(params, &p); err != nil || p.OperationID == "" {
			log.Printf("bad v2 traffic reset params: %v", err)
			return false
		}
		trafficReportBoundaryMu.Lock()
		snapshot, err := monitoring.ResetTraffic(p.OperationID)
		if err != nil {
			trafficReportBoundaryMu.Unlock()
			log.Printf("failed to reset traffic ledger: %v", err)
			forgetV2EventSeen(eventID)
			return false
		}
		result := v2.TrafficResetResultParams{
			OperationID:    p.OperationID,
			CapturedAt:     snapshot.CapturedAt.Format(time.RFC3339Nano),
			CycleID:        snapshot.CycleID,
			CycleStartedAt: snapshot.CycleStartedAt.Format(time.RFC3339Nano),
			TotalUp:        snapshot.TotalUp,
			TotalDown:      snapshot.TotalDown,
		}
		payload := v2.NewNotification(v2.MethodAgentTrafficResetResult, result)
		if conn == nil {
			trafficReportBoundaryMu.Unlock()
			forgetV2EventSeen(eventID)
			return false
		}
		err = conn.WriteMessage(websocket.TextMessage, payload)
		trafficReportBoundaryMu.Unlock()
		if err != nil {
			log.Printf("failed to return traffic reset result: %v", err)
			forgetV2EventSeen(eventID)
			return false
		}
		rememberV2EventResult(eventID, payload)
		return true
	case v2.MethodAgentMessage, v2.MethodAgentEvent:
		log.Printf("received v2 %s: %+v", method, params)
		return true
	default:
		log.Printf("unknown v2 event method %s", method)
	}
	return false
}

func forgetV2EventSeen(id string) {
	if id == "" {
		return
	}
	v2AckMu.Lock()
	defer v2AckMu.Unlock()
	delete(v2SeenEvents, id)
	filtered := v2SeenOrder[:0]
	for _, seenID := range v2SeenOrder {
		if seenID != id {
			filtered = append(filtered, seenID)
		}
	}
	v2SeenOrder = filtered
}

// connectWebSocket attempts to establish a WebSocket connection and upload basic info

// establishTerminalConnection 建立终端连接并使用terminal包处理终端操作
func establishTerminalConnection(token, id, endpoint string) {
	endpoint = strings.TrimSuffix(endpoint, "/") + "/api/clients/terminal?token=" + token + "&id=" + id
	endpoint = "ws" + strings.TrimPrefix(endpoint, "http")

	// 转换中文域名为 ASCII 兼容编码
	if convertedEndpoint, err := utils.ConvertIDNToASCII(endpoint); err == nil {
		endpoint = convertedEndpoint
	} else {
		log.Printf("Warning: Failed to convert Terminal WebSocket IDN to ASCII: %v", err)
	}

	// 使用与主 WS 相同的拨号策略
	dialer := newWSDialer()

	headers := newWSHeaders()

	conn, _, err := dialer.Dial(endpoint, headers)
	if err != nil {
		log.Println("Failed to establish terminal connection:", err)
		return
	}

	// 单连接承载终端命令和文件管理消息，由 Agent 统一分发。
	terminal.StartTerminalWithFileManager(conn)
	if conn != nil {
		conn.Close()
	}
}

func establishWorkspaceConnection(token, id, endpoint string) {
	establishTerminalConnection(token, id, endpoint)
}

func establishFileConnection(token, id, endpoint string) {
	endpoint = strings.TrimSuffix(endpoint, "/") + "/api/clients/files?token=" + token + "&id=" + id
	endpoint = "ws" + strings.TrimPrefix(endpoint, "http")
	if convertedEndpoint, err := utils.ConvertIDNToASCII(endpoint); err == nil {
		endpoint = convertedEndpoint
	} else {
		log.Printf("Warning: Failed to convert file WebSocket IDN to ASCII: %v", err)
	}
	conn, _, err := newWSDialer().Dial(endpoint, newWSHeaders())
	if err != nil {
		log.Println("Failed to establish file manager connection:", err)
		return
	}
	filemanager.Start(conn)
	_ = conn.Close()
}

// newWSDialer 构造统一的 WebSocket 拨号器（自定义解析、IPv4/IPv6 动态排序、可选 TLS 忽略）
func newWSDialer() *websocket.Dialer {
	d := &websocket.Dialer{
		HandshakeTimeout:  websocketConnectTimeout,
		NetDialContext:    dnsresolver.GetDialContextWithPreference(websocketConnectTimeout, flags.PreferIPVersion),
		Proxy:             http.ProxyFromEnvironment,
		EnableCompression: !flags.DisableCompression,
	}
	if flags.IgnoreUnsafeCert {
		d.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return d
}

// newWSHeaders 统一构造 WS 请求头（含 Cloudflare Access 头）
func newWSHeaders() http.Header {
	headers := http.Header{}
	if flags.CFAccessClientID != "" && flags.CFAccessClientSecret != "" {
		headers.Set("CF-Access-Client-Id", flags.CFAccessClientID)
		headers.Set("CF-Access-Client-Secret", flags.CFAccessClientSecret)
	}
	return headers
}

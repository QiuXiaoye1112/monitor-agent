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
	v1 "monitor-agent/protocol/v1"
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
	trafficReportBoundaryMu sync.Mutex
)

const (
	maxRememberedV2Events     = 1024
	maxPendingInboundMessages = 128
	heartbeatInterval         = 30 * time.Second
	heartbeatTimeout          = 30 * time.Second
	websocketConnectTimeout   = 30 * time.Second
)

type heartbeatPong struct {
	payload    string
	receivedAt time.Time
}

type websocketInboundMessage struct {
	conn            *ws.SafeConn
	protocolVersion int
	payload         []byte
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
		resetConnectionProtocolVersion()
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

	nextProtocol := requestedProtocolVersion()
	activeProtocol := 0
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
		activeProtocol = 0
		pendingInboundMessages = pendingInboundMessages[:0]
		resetConnectionProtocolVersion()
		if requestedProtocolVersion() >= 2 {
			nextProtocol = 2
		}
	}

	activateConnection := func(newConn *ws.SafeConn, protocolVersion int) {
		clearPendingHeartbeat()
		conn = newConn
		retryState.connected()
		activeProtocol = protocolVersion
		nextProtocol = protocolVersion
		setConnectionProtocolVersion(activeProtocol)

		pongChannel := make(chan heartbeatPong, 4)
		configureHeartbeat(conn, pongChannel)
		pongEvents = pongChannel

		done := make(chan struct{})
		readDone = done
		go handleWebSocketMessages(conn, activeProtocol, inboundMessages, done)
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
			processWebSocketMessage(inbound.conn, inbound.protocolVersion, inbound.payload)
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
		flushPendingResults(conn, activeProtocol)
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
			var ackIDs []string
			if activeProtocol >= 2 {
				ackIDs = snapshotV2AckEventIDs()
				data = v2.BuildReportPayload(data, ackIDs)
			}
			err = conn.WriteMessage(websocket.TextMessage, data)
			if err == nil && activeProtocol >= 2 {
				clearV2AckEventIDs(ackIDs)
			}
			trafficReportBoundaryMu.Unlock()
			if err != nil {
				log.Println("Failed to send WebSocket message:", err)
				handleConnectionFailure(err.Error(), true)
				continue
			}
		case <-historyTicker.C:
			if !monitorServiceHealth.canReport() || conn == nil || len(latestReport) == 0 {
				continue
			}
			data := v1.BuildHistoryReportPayload(latestReport)
			if activeProtocol >= 2 {
				data = v2.BuildHistoryReportPayload(latestReport)
			}
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
				connectProtocol := nextProtocol
				websocketEndpoint := buildWebSocketEndpoint(connectProtocol)
				newConn, connectErr := connectWebSocket(websocketEndpoint)
				if connectErr != nil && shouldFallbackToV1(connectProtocol, connectErr) {
					connectProtocol = 1
					websocketEndpoint = buildWebSocketEndpoint(connectProtocol)
					newConn, connectErr = connectWebSocket(websocketEndpoint)
				}
				if connectErr != nil {
					if !missRecorded {
						recordHeartbeatMiss(connectErr.Error())
					}
					resetHeartbeatTimer(heartbeatInterval)
					continue
				}
				activateConnection(newConn, connectProtocol)
				log.Printf("WebSocket heartbeat probe connected using v%d protocol", activeProtocol)
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
				processWebSocketMessage(inbound.conn, inbound.protocolVersion, inbound.payload)
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
	return conn.WriteMessage(websocket.PingMessage, []byte(payload))
}

func isTimelyHeartbeatPong(pong heartbeatPong, pendingPayload string, deadline time.Time) bool {
	return pendingPayload != "" &&
		pong.payload == pendingPayload &&
		!pong.receivedAt.After(deadline)
}

func buildWebSocketEndpoint(protocolVersion int) string {
	path := "/api/clients/report?token=" + flags.Token
	if protocolVersion >= 2 {
		path = "/api/clients/v2/rpc?token=" + flags.Token
	}
	websocketEndpoint := strings.TrimSuffix(flags.Endpoint, "/") + path
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
	return safeConn, nil
}

func handleWebSocketMessages(
	conn *ws.SafeConn,
	protocolVersion int,
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
			conn:            conn,
			protocolVersion: protocolVersion,
			payload:         append([]byte(nil), message_raw...),
		}
		select {
		case inboundMessages <- message:
		default:
			log.Println("WebSocket inbound event buffer is full; reconnecting so unacknowledged events can be replayed")
			return
		}
	}
}

func processWebSocketMessage(conn *ws.SafeConn, protocolVersion int, messageRaw []byte) {
	var message struct {
		JSONRPC string      `json:"jsonrpc,omitempty"`
		Method  string      `json:"method,omitempty"`
		Params  interface{} `json:"params,omitempty"`
		EventID string      `json:"event_id,omitempty"`
		Message string      `json:"message"`
		// Terminal
		TerminalId string `json:"request_id,omitempty"`
		// Remote Exec
		ExecCommand string `json:"command,omitempty"`
		ExecTaskID  string `json:"task_id,omitempty"`
		// Ping
		PingTaskID uint   `json:"ping_task_id,omitempty"`
		PingType   string `json:"ping_type,omitempty"`
		PingTarget string `json:"ping_target,omitempty"`
	}
	err := json.Unmarshal(messageRaw, &message)
	if err != nil {
		log.Println("Bad ws message:", err)
		return
	}
	if message.JSONRPC == v2.Version && protocolVersion >= 2 {
		if processV2Event(conn, message.Method, message.Params, message.EventID) {
			addV2AckEventID(message.EventID)
		}
		return
	}

	if message.Message == "file" {
		go establishFileConnection(flags.Token, message.TerminalId, flags.Endpoint)
		return
	}
	if message.Message == "terminal" || message.TerminalId != "" {
		go establishTerminalConnection(flags.Token, message.TerminalId, flags.Endpoint)
		return
	}
	if message.Message == "exec" {
		go NewTask(message.ExecTaskID, message.ExecCommand)
		return
	}
	if message.Message == "ping" || message.PingTaskID != 0 || message.PingType != "" || message.PingTarget != "" {
		go NewPingTask(conn, protocolVersion, message.PingTaskID, message.PingType, message.PingTarget)
		return
	}
}

func processV2Event(conn *ws.SafeConn, method string, params interface{}, eventID string) bool {
	if !markV2EventSeen(eventID) {
		return true
	}
	switch method {
	case v2.MethodAgentExec:
		var p struct {
			TaskID  string `json:"task_id"`
			Command string `json:"command"`
		}
		if err := v2.BindParams(params, &p); err == nil {
			go NewTask(p.TaskID, p.Command)
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
			go NewPingTask(conn, 2, p.TaskID, p.Type, p.Target)
			return true
		} else {
			log.Printf("bad v2 ping params: %v", err)
		}
	case v2.MethodAgentTerminal:
		var p struct {
			RequestID string `json:"request_id"`
		}
		if err := v2.BindParams(params, &p); err == nil {
			go establishTerminalConnection(flags.Token, p.RequestID, flags.Endpoint)
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
			OperationID: p.OperationID,
			CapturedAt:  snapshot.CapturedAt.Format(time.RFC3339Nano),
			TotalUp:     snapshot.TotalUp,
			TotalDown:   snapshot.TotalDown,
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

	// 启动终端
	terminal.StartTerminal(conn)
	if conn != nil {
		conn.Close()
	}
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

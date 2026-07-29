package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"monitor-agent/ws"
)

func TestHeartbeatTiming(t *testing.T) {
	if heartbeatInterval != 30*time.Second {
		t.Fatalf("heartbeat interval = %s, want 30s", heartbeatInterval)
	}
	if heartbeatTimeout != 30*time.Second {
		t.Fatalf("heartbeat timeout = %s, want 30s", heartbeatTimeout)
	}
	if websocketConnectTimeout != 30*time.Second {
		t.Fatalf("websocket connect timeout = %s, want 30s", websocketConnectTimeout)
	}
	if maxPendingInboundMessages != 128 {
		t.Fatalf("inbound message buffer = %d, want 128", maxPendingInboundMessages)
	}
}

func TestConnectionRetryStateAllowsOnlyOneImmediateReconnect(t *testing.T) {
	var state connectionRetryState
	state.connected()
	if state.connectionFailed() {
		t.Fatal("unverified connection received an immediate reconnect")
	}

	state.connected()
	state.pongReceived()
	if !state.connectionFailed() {
		t.Fatal("verified connection did not receive its immediate reconnect")
	}

	state.connected()
	if state.connectionFailed() {
		t.Fatal("failed replacement connection received another immediate reconnect")
	}

	state.connected()
	state.pongReceived()
	if !state.connectionFailed() {
		t.Fatal("a new Pong did not restore the one immediate reconnect allowance")
	}
}

func TestHeartbeatReceivesMatchingPong(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	rawConn, _, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(server.URL, "http"),
		nil,
	)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	conn := ws.NewSafeConn(rawConn)
	defer conn.Close()
	pongEvents := make(chan heartbeatPong, 1)
	configureHeartbeat(conn, pongEvents)

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		_, _, _ = conn.ReadMessage()
	}()

	const payload = "heartbeat-test"
	if err := sendHeartbeat(conn, payload); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}
	select {
	case pong := <-pongEvents:
		if pong.payload != payload {
			t.Fatalf("pong payload = %q, want %q", pong.payload, payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Pong")
	}

	_ = conn.Close()
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("reader did not stop after closing connection")
	}
}

func TestReaderForwardsMessageBeforeFirstPong(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"message":"exec","task_id":"queued"}`))
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()

	rawConn, _, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(server.URL, "http"),
		nil,
	)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	conn := ws.NewSafeConn(rawConn)
	defer conn.Close()

	inboundMessages := make(chan websocketInboundMessage, 1)
	readDone := make(chan struct{})
	go handleWebSocketMessages(conn, 2, inboundMessages, readDone)

	select {
	case inbound := <-inboundMessages:
		if !strings.Contains(string(inbound.payload), `"task_id":"queued"`) {
			t.Fatalf("unexpected inbound payload: %s", inbound.payload)
		}
	case <-time.After(time.Second):
		t.Fatal("message sent before the first Pong was not forwarded for buffering")
	}

	_ = conn.Close()
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("reader did not stop")
	}
}

func TestHeartbeatOnlyAcceptsMatchingPongWithinDeadline(t *testing.T) {
	deadline := time.Now()
	tests := []struct {
		name    string
		pong    heartbeatPong
		pending string
		want    bool
	}{
		{
			name:    "matching and timely",
			pong:    heartbeatPong{payload: "expected", receivedAt: deadline},
			pending: "expected",
			want:    true,
		},
		{
			name:    "late",
			pong:    heartbeatPong{payload: "expected", receivedAt: deadline.Add(time.Nanosecond)},
			pending: "expected",
		},
		{
			name:    "wrong payload",
			pong:    heartbeatPong{payload: "old", receivedAt: deadline},
			pending: "expected",
		},
		{
			name: "no pending heartbeat",
			pong: heartbeatPong{payload: "expected", receivedAt: deadline},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isTimelyHeartbeatPong(test.pong, test.pending, deadline); got != test.want {
				t.Fatalf("isTimelyHeartbeatPong() = %t, want %t", got, test.want)
			}
		})
	}
}

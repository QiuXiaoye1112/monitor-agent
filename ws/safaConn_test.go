package ws

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWriteControlBypassesDataWriteMutex(t *testing.T) {
	receivedPing := make(chan string, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetPingHandler(func(payload string) error {
			receivedPing <- payload
			return nil
		})
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
	conn := NewSafeConn(rawConn)
	defer conn.Close()

	conn.mu.Lock()
	err = conn.WriteControl(
		websocket.PingMessage,
		[]byte("independent"),
		time.Now().Add(time.Second),
	)
	conn.mu.Unlock()
	if err != nil {
		t.Fatalf("write control while data mutex held: %v", err)
	}
	select {
	case payload := <-receivedPing:
		if payload != "independent" {
			t.Fatalf("ping payload = %q", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("control frame was blocked by data write mutex")
	}
}

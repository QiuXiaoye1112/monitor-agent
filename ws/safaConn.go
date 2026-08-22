package ws

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const writeTimeout = 30 * time.Second

type SafeConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func NewSafeConn(conn *websocket.Conn) *SafeConn {
	return &SafeConn{
		conn: conn,
		mu:   sync.Mutex{},
	}
}

func (sc *SafeConn) WriteMessage(messageType int, data []byte) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if err := sc.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	err := sc.conn.WriteMessage(messageType, data)
	_ = sc.conn.SetWriteDeadline(time.Time{})
	return err
}

func (sc *SafeConn) WriteJSON(v interface{}) error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if err := sc.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return err
	}
	err := sc.conn.WriteJSON(v)
	_ = sc.conn.SetWriteDeadline(time.Time{})
	return err
}

// WriteControl bypasses the data-frame mutex so Ping/Pong/Close control frames
// can still meet their short deadline while a normal report write is blocked.
// Gorilla WebSocket explicitly permits WriteControl concurrently with other
// connection methods.
func (sc *SafeConn) WriteControl(messageType int, data []byte, deadline time.Time) error {
	return sc.conn.WriteControl(messageType, data, deadline)
}

func (sc *SafeConn) Close() error {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.conn.Close()
}
func (sc *SafeConn) ReadMessage() (int, []byte, error) {
	// sc.mu.Lock()
	// defer sc.mu.Unlock()
	return sc.conn.ReadMessage()
}
func (sc *SafeConn) ReadJSON(v interface{}) error {
	// sc.mu.Lock()
	// defer sc.mu.Unlock()
	return sc.conn.ReadJSON(v)
}
func (sc *SafeConn) SetReadDeadline(t time.Time) error {
	// sc.mu.Lock()
	// defer sc.mu.Unlock()
	return sc.conn.SetReadDeadline(t)
}

func (sc *SafeConn) SetReadLimit(limit int64) {
	sc.conn.SetReadLimit(limit)
}

func (sc *SafeConn) SetPongHandler(h func(appData string) error) {
	sc.conn.SetPongHandler(h)
}
func (sc *SafeConn) GetConn() *websocket.Conn {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.conn
}

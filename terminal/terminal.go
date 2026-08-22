package terminal

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	pkg_flags "monitor-agent/cmd/flags"
	"monitor-agent/filemanager"
)

var flags = pkg_flags.GlobalConfig

// Terminal 接口定义平台特定的终端操作
type Terminal interface {
	Close() error
	Read(p []byte) (int, error)
	Write(p []byte) (int, error)
	Resize(cols, rows int) error
	Wait() error
}

// terminalImpl 封装终端和平台特定逻辑
type terminalImpl struct {
	shell      string
	workingDir string
	term       Terminal
}

// StartTerminal 启动终端并处理 WebSocket 通信
func StartTerminal(conn *websocket.Conn) {
	startTerminal(conn, nil)
}

func StartTerminalWithFileManager(conn *websocket.Conn) {
	files, err := filemanager.NewSession()
	if err != nil {
		_ = conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("Error: %v\r\n", err)))
		_ = conn.Close()
		return
	}
	startTerminal(conn, files)
}

func startTerminal(conn *websocket.Conn, files *filemanager.Session) {
	if flags.DisableWebSsh {
		conn.WriteMessage(websocket.TextMessage, []byte("\n\nWeb SSH is disabled. Enable it by running without the --disable-web-ssh flag."))
		conn.Close()
		return
	}
	if files != nil {
		defer files.Close()
	}
	impl, err := newTerminalImpl()
	if err != nil {
		conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("Error: %v\r\n", err)))
		return
	}

	errChan := make(chan error, 3) // 增加容量以容纳多个错误源
	done := make(chan struct{})
	var writeMu sync.Mutex
	var fileQueue chan []byte
	var fileWorkerDone chan struct{}
	if files != nil {
		fileQueue = make(chan []byte, 8)
		fileWorkerDone = make(chan struct{})
		go func() {
			defer close(fileWorkerDone)
			for payload := range fileQueue {
				response := files.HandleMessage(payload)
				if len(response) == 0 {
					continue
				}
				writeMu.Lock()
				err := conn.WriteMessage(websocket.TextMessage, response)
				writeMu.Unlock()
				if err != nil {
					select {
					case errChan <- err:
					default:
					}
					return
				}
			}
		}()
		defer func() {
			close(fileQueue)
			<-fileWorkerDone
		}()
	}
	if files != nil {
		writeMu.Lock()
		_ = conn.WriteMessage(websocket.TextMessage, files.InitialMessage())
		writeMu.Unlock()
	}

	defer func() {
		gracefulShutdown(impl.term)
		impl.term.Close()
		conn.Close()
		close(done)
	}()

	// 从 WebSocket 读取消息并写入终端
	go handleWebSocketInput(conn, impl.term, fileQueue, errChan, done)

	// 从终端读取输出并写入 WebSocket
	go handleTerminalOutput(conn, impl.term, &writeMu, errChan, done)

	// 等待终端进程结束或出现错误
	select {
	case err := <-errChan:
		// WebSocket 连接断开或出现错误
		if err != nil {
			conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("\r\nConnection error: %v\r\n", err)))
		}
	case <-done:
		// 已经被其他地方关闭
	}
}

// gracefulShutdown 尝试优雅地关闭终端
func gracefulShutdown(term Terminal) {
	//  Ctrl+C
	for i := 0; i < 3; i++ {
		if _, err := term.Write([]byte{3}); err != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(200 * time.Millisecond)

	//  Ctrl+D (EOF)
	term.Write([]byte{4})
	time.Sleep(100 * time.Millisecond)

	term.Write([]byte("exit\n"))
	time.Sleep(100 * time.Millisecond)
}

// handleWebSocketInput 处理 WebSocket 输入
func handleWebSocketInput(conn *websocket.Conn, term Terminal, fileQueue chan<- []byte, errChan chan<- error, done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		default:
		}

		t, p, err := conn.ReadMessage()
		if err != nil {
			select {
			case errChan <- err:
			default:
			}
			return
		}
		if t == websocket.TextMessage {
			var cmd struct {
				Type  string `json:"type"`
				Cols  int    `json:"cols,omitempty"`
				Rows  int    `json:"rows,omitempty"`
				Input string `json:"input,omitempty"`
			}
			if err := json.Unmarshal(p, &cmd); err == nil {
				switch cmd.Type {
				case "resize":
					if cmd.Cols > 0 && cmd.Rows > 0 {
						term.Resize(cmd.Cols, cmd.Rows)
					}
				case "input":
					if cmd.Input != "" {
						term.Write([]byte(cmd.Input))
					}
				default:
					enqueueFileMessage(fileQueue, p, done)
				}
			} else {
				if fileQueue != nil {
					enqueueFileMessage(fileQueue, p, done)
				} else {
					term.Write(p)
				}
			}
		}
		if t == websocket.BinaryMessage {
			term.Write(p)
		}
	}
}

func enqueueFileMessage(fileQueue chan<- []byte, payload []byte, done <-chan struct{}) bool {
	if fileQueue == nil {
		return false
	}
	copyPayload := append([]byte(nil), payload...)
	select {
	case fileQueue <- copyPayload:
		return true
	case <-done:
		return false
	}
}

// handleTerminalOutput 处理终端输出
func handleTerminalOutput(conn *websocket.Conn, term Terminal, writeMu *sync.Mutex, errChan chan<- error, done <-chan struct{}) {
	buf := make([]byte, 4096)
	for {
		select {
		case <-done:
			return
		default:
		}

		n, err := term.Read(buf)
		if err != nil {
			select {
			case errChan <- err:
			default:
			}
			return
		}
		writeMu.Lock()
		err = conn.WriteMessage(websocket.BinaryMessage, buf[:n])
		writeMu.Unlock()
		if err != nil {
			select {
			case errChan <- err:
			default:
			}
			return
		}
	}
}

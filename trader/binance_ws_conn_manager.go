package trader

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"nofx/logger"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type wsConnManager interface {
	Start()
	Stop()
	WaitReady(timeout time.Duration) error
	Send(method string, params map[string]interface{}, signed bool, timeout time.Duration) (map[string]interface{}, error)
}

type wsPendingResp struct {
	resp map[string]interface{}
	err  error
}

type wsAPIConnManager struct {
	name     string
	endpoint string
	apiKey   string
	secret   string

	timeOffsetFn func() int64

	mu         sync.Mutex
	cond       *sync.Cond
	conn       *websocket.Conn
	connected  bool
	connecting bool
	closed     bool

	readDone chan struct{}

	pending map[string]chan wsPendingResp
	writeMu sync.Mutex
}

func newWSAPIConnManager(name, endpoint, apiKey, secret string, timeOffsetFn func() int64) *wsAPIConnManager {
	m := &wsAPIConnManager{
		name:         name,
		endpoint:     endpoint,
		apiKey:       apiKey,
		secret:       secret,
		timeOffsetFn: timeOffsetFn,
		pending:      make(map[string]chan wsPendingResp),
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

func (m *wsAPIConnManager) Start() {
	// Lazy-connect. Real dial happens in WaitReady/Send.
}

func (m *wsAPIConnManager) Stop() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	conn := m.conn
	m.conn = nil
	m.connected = false
	m.mu.Unlock()
	m.cond.Broadcast()
	if conn != nil {
		_ = conn.Close()
	}
	m.failAllPending(fmt.Errorf("ws manager %s stopped", m.name))
}

func (m *wsAPIConnManager) WaitReady(timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	_, err := m.getConn(timeout)
	return err
}

func (m *wsAPIConnManager) Send(method string, params map[string]interface{}, signed bool, timeout time.Duration) (map[string]interface{}, error) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	if params == nil {
		params = make(map[string]interface{})
	}
	if signed {
		params = cloneParams(params)
		params["apiKey"] = m.apiKey
		offset := int64(0)
		if m.timeOffsetFn != nil {
			offset = m.timeOffsetFn()
		}
		params["timestamp"] = time.Now().UnixMilli() - offset
		params["signature"] = signQuery(m.secret, params)
	}

	id := uuid.NewString()
	ch := make(chan wsPendingResp, 1)
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, fmt.Errorf("ws manager %s is closed", m.name)
	}
	m.pending[id] = ch
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		delete(m.pending, id)
		m.mu.Unlock()
	}()

	req := map[string]interface{}{
		"id":     id,
		"method": method,
		"params": params,
	}

	conn, err := m.getConn(timeout)
	if err != nil {
		return nil, err
	}
	if err := m.writeJSON(conn, req); err != nil {
		m.onConnError(err)
		// one quick retry after reconnect window
		conn2, err2 := m.getConn(2 * time.Second)
		if err2 != nil {
			return nil, err
		}
		if err = m.writeJSON(conn2, req); err != nil {
			m.onConnError(err)
			return nil, err
		}
	}

	select {
	case out := <-ch:
		if out.err != nil {
			return nil, out.err
		}
		return out.resp, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("ws api request timeout (%s %s)", m.name, method)
	}
}

func (m *wsAPIConnManager) getConn(timeout time.Duration) (*websocket.Conn, error) {
	deadline := time.Now().Add(timeout)
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, fmt.Errorf("ws manager %s is closed", m.name)
		}
		if m.connected && m.conn != nil {
			conn := m.conn
			m.mu.Unlock()
			return conn, nil
		}
		if !m.connecting {
			m.connecting = true
			m.mu.Unlock()

			conn, err := m.dial()
			m.mu.Lock()
			m.connecting = false
			if err == nil {
				m.conn = conn
				m.connected = true
				m.readDone = make(chan struct{})
				go m.readLoop(conn, m.readDone)
				m.mu.Unlock()
				m.cond.Broadcast()
				return conn, nil
			}
			m.connected = false
			m.conn = nil
			m.mu.Unlock()
			m.cond.Broadcast()
			if time.Now().After(deadline) {
				return nil, fmt.Errorf("ws dial failed (%s): %w", m.name, err)
			}
			logger.Infof("⚠️ Binance WS API dial failed (%s): %v", m.name, err)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			m.mu.Unlock()
			return nil, fmt.Errorf("ws connect timeout (%s)", m.name)
		}
		m.mu.Unlock()
		time.Sleep(minDuration(150*time.Millisecond, remaining))
	}
}

func (m *wsAPIConnManager) dial() (*websocket.Conn, error) {
	conn, _, err := websocket.DefaultDialer.Dial(m.endpoint, nil)
	if err != nil {
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Time{})
	_ = conn.SetWriteDeadline(time.Time{})
	return conn, nil
}

func (m *wsAPIConnManager) writeJSON(conn *websocket.Conn, payload interface{}) error {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	err := conn.WriteJSON(payload)
	_ = conn.SetWriteDeadline(time.Time{})
	return err
}

func (m *wsAPIConnManager) readLoop(conn *websocket.Conn, done chan struct{}) {
	defer close(done)
	for {
		var resp map[string]interface{}
		if err := conn.ReadJSON(&resp); err != nil {
			m.onConnError(err)
			return
		}

		id := fmt.Sprintf("%v", resp["id"])
		if id == "" || id == "<nil>" {
			continue
		}

		m.mu.Lock()
		ch := m.pending[id]
		m.mu.Unlock()
		if ch == nil {
			continue
		}

		if errNode, ok := resp["error"].(map[string]interface{}); ok {
			code := fmt.Sprintf("%v", errNode["code"])
			msg := fmt.Sprintf("%v", errNode["msg"])
			select {
			case ch <- wsPendingResp{err: fmt.Errorf("binance ws api error code=%s msg=%s", code, msg)}:
			default:
			}
			continue
		}
		if status, ok := resp["status"].(float64); ok && int(status) >= 400 {
			select {
			case ch <- wsPendingResp{err: fmt.Errorf("binance ws api status=%d", int(status))}:
			default:
			}
			continue
		}

		select {
		case ch <- wsPendingResp{resp: resp}:
		default:
		}
	}
}

func (m *wsAPIConnManager) onConnError(err error) {
	m.mu.Lock()
	if m.conn != nil {
		_ = m.conn.Close()
	}
	m.conn = nil
	m.connected = false
	m.mu.Unlock()
	m.cond.Broadcast()
	m.failAllPending(fmt.Errorf("ws connection lost (%s): %w", m.name, err))
	logger.Infof("⚠️ Binance WS API connection lost (%s): %v", m.name, err)
}

func (m *wsAPIConnManager) failAllPending(err error) {
	m.mu.Lock()
	pending := m.pending
	m.pending = make(map[string]chan wsPendingResp)
	m.mu.Unlock()
	for _, ch := range pending {
		select {
		case ch <- wsPendingResp{err: err}:
		default:
		}
	}
}

func cloneParams(in map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func minDuration(a, b time.Duration) time.Duration {
	if a <= b {
		return a
	}
	return b
}

// Marshal helper to keep response maps deterministic in tests/logging if needed.
func normalizeMap(in map[string]interface{}) map[string]interface{} {
	if in == nil {
		return nil
	}
	b, _ := json.Marshal(in)
	var out map[string]interface{}
	_ = json.Unmarshal(b, &out)
	return out
}

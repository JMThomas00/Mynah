package wire

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Client is a minimal Concord plugin-protocol client: dial, identify, send,
// and a blocking read loop. Standard WebSocket ping/pong keepalive is
// handled automatically by gorilla/websocket's default PingHandler — no
// application-level heartbeat message needed, confirmed by Tukan's own
// server-mode client (d:\Tukan\internal\wire\client.go), which relies on
// the same default.
type Client struct {
	conn *websocket.Conn
	mu   sync.Mutex // serializes writes; gorilla/websocket requires this for concurrent senders
}

// Dial opens the WebSocket connection. It does not identify — call Identify
// separately so a failed handshake is a clear, distinct error from a failed
// dial.
func Dial(wsURL string) (*Client, error) {
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", wsURL, err)
	}
	return &Client{conn: conn}, nil
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Send marshals and writes one message.
func (c *Client) Send(op OpCode, data any) error {
	msg, err := NewMessage(op, data)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteMessage(websocket.TextMessage, raw)
}

// readOne blocks for the next message on the connection. Not safe to call
// from more than one goroutine — only ReadLoop and Identify call it, and
// Identify always completes (or fails) before ReadLoop starts.
func (c *Client) readOne() (*Message, error) {
	_, data, err := c.conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("unmarshal message: %w", err)
	}
	return &msg, nil
}

// maxIdentifyReads bounds how many non-Ready messages Identify will skip
// past before giving up — a safety net against a connection that never
// sends OpReady at all, not a number expected to matter in practice.
const maxIdentifyReads = 10

// Identify sends OpIdentify with ClientType "plugin" and waits for
// Concord's OpReady acknowledgment, returning this connection's own
// service-account user ID (from ReadyPayload.User.ID) — needed to recognize
// and ignore this plugin's own posted messages if they ever come back
// around through mention-mode relay.
//
// Concord sends OpHello immediately upon connection, independent of when
// (or whether) the client has sent OpIdentify yet — a strict "the next
// message must be OpReady" read fails against a real server, which sends
// Hello first. This loops reading and only reacts to OpReady/OpInvalidSession,
// silently ignoring everything else including Hello — the same tolerant
// approach Tukan's own client (and Concord's reference cmd/testplugin) use.
func (c *Client) Identify(token string) (uuid.UUID, error) {
	if err := c.Send(OpIdentify, IdentifyPayload{Token: token, ClientType: "plugin"}); err != nil {
		return uuid.Nil, fmt.Errorf("send identify: %w", err)
	}
	for i := 0; i < maxIdentifyReads; i++ {
		msg, err := c.readOne()
		if err != nil {
			return uuid.Nil, fmt.Errorf("read identify response: %w", err)
		}
		switch msg.Op {
		case OpReady:
			var ready ReadyPayload
			if err := json.Unmarshal(msg.Data, &ready); err != nil {
				return uuid.Nil, fmt.Errorf("unmarshal ready payload: %w", err)
			}
			if ready.User == nil {
				return uuid.Nil, fmt.Errorf("ready payload missing user")
			}
			return ready.User.ID, nil
		case OpInvalidSession:
			return uuid.Nil, fmt.Errorf("identify rejected: invalid session")
		}
	}
	return uuid.Nil, fmt.Errorf("identify failed: did not receive OpReady after %d messages", maxIdentifyReads)
}

// ReadLoop blocks, calling handle for every message received, until the
// connection closes or a read fails — at which point it returns that error.
// Only ever call after a successful Identify.
func (c *Client) ReadLoop(handle func(*Message)) error {
	for {
		msg, err := c.readOne()
		if err != nil {
			return err
		}
		handle(msg)
	}
}

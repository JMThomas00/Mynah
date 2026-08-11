package wire

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// testServer spins up a minimal local WebSocket server that speaks just
// enough of Concord's handshake to exercise Client end-to-end — mirrors
// Tukan's own internal/wire/client_test.go (d:\Tukan), the precedent this
// plugin's whole wire package is modeled on. Not a mock of Concord's actual
// server logic, a stand-in for the wire shape, since there's no running
// Concord instance to test against from this environment.
func testServer(t *testing.T, selfID uuid.UUID, onIdentify func(IdentifyPayload) bool, afterReady func(*websocket.Conn)) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		hello, _ := NewMessage(OpHello, nil)
		helloRaw, _ := json.Marshal(hello)
		if err := conn.WriteMessage(websocket.TextMessage, helloRaw); err != nil {
			return
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var msg Message
		if err := json.Unmarshal(data, &msg); err != nil {
			return
		}
		if msg.Op != OpIdentify {
			return
		}
		var identify IdentifyPayload
		_ = json.Unmarshal(msg.Data, &identify)

		if onIdentify != nil && !onIdentify(identify) {
			// Simulate a rejected identify: close without replying OpReady.
			return
		}

		ready, _ := NewMessage(OpReady, ReadyPayload{User: &Author{ID: selfID, Username: "test-plugin"}})
		raw, _ := json.Marshal(ready)
		if err := conn.WriteMessage(websocket.TextMessage, raw); err != nil {
			return
		}

		if afterReady != nil {
			afterReady(conn)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

// TestClientIdentifySucceeds drives the real Dial -> Identify sequence
// against a local server that replies OpReady, confirming Identify returns
// the service account's own user ID and sent ClientType "plugin" (not the
// default "user").
func TestClientIdentifySucceeds(t *testing.T) {
	wantID := uuid.New()
	var gotToken string
	var gotClientType string
	srv := testServer(t, wantID, func(p IdentifyPayload) bool {
		gotToken = p.Token
		gotClientType = p.ClientType
		return true
	}, nil)

	c, err := Dial(wsURL(srv.URL))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	gotID, err := c.Identify("secret-token")
	if err != nil {
		t.Fatalf("identify: %v", err)
	}
	if gotID != wantID {
		t.Fatalf("Identify returned %v, want %v", gotID, wantID)
	}
	if gotToken != "secret-token" {
		t.Fatalf("server saw token = %q, want %q", gotToken, "secret-token")
	}
	if gotClientType != "plugin" {
		t.Fatalf("server saw client_type = %q, want %q", gotClientType, "plugin")
	}
}

// TestClientIdentifyToleratesLeadingHello confirms Identify doesn't require
// the very next message after OpIdentify to be OpReady — Concord sends
// OpHello immediately on connect, independent of identify timing (the same
// real bug Tukan's own client hit against a live server).
func TestClientIdentifyToleratesLeadingHello(t *testing.T) {
	srv := testServer(t, uuid.New(), func(IdentifyPayload) bool { return true }, nil)

	c, err := Dial(wsURL(srv.URL))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if _, err := c.Identify("secret-token"); err != nil {
		t.Fatalf("Identify should tolerate a leading OpHello, got: %v", err)
	}
}

// TestClientIdentifyFailsWithoutReady confirms a connection that never gets
// an OpReady (the server just hangs up) surfaces as a clear error, not a
// silent hang or a panic.
func TestClientIdentifyFailsWithoutReady(t *testing.T) {
	srv := testServer(t, uuid.New(), func(IdentifyPayload) bool { return false }, nil)

	c, err := Dial(wsURL(srv.URL))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if _, err := c.Identify("secret-token"); err == nil {
		t.Fatal("expected Identify to fail when the server never sends OpReady")
	}
}

// TestClientReadLoopDispatchesEvents confirms ReadLoop correctly unmarshals
// and hands off a dispatched event (the shape every relayed MESSAGE_CREATE
// arrives as: OpDispatch with Type set) after a successful identify.
func TestClientReadLoopDispatchesEvents(t *testing.T) {
	channelID := uuid.New()
	srv := testServer(t, uuid.New(), nil, func(conn *websocket.Conn) {
		payload := MessageCreatePayload{
			ChatMessage: &ChatMessage{ChannelID: channelID, Content: "hello"},
			Author:      &Author{ID: uuid.New(), Username: "someone"},
		}
		raw, _ := json.Marshal(payload)
		dispatch := Message{Op: OpDispatch, Type: EventMessageCreate, Data: raw}
		data, _ := json.Marshal(dispatch)
		_ = conn.WriteMessage(websocket.TextMessage, data)
		time.Sleep(50 * time.Millisecond) // give the client time to read before the handler closes the conn
	})

	c, err := Dial(wsURL(srv.URL))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if _, err := c.Identify("token"); err != nil {
		t.Fatalf("identify: %v", err)
	}

	received := make(chan *Message, 1)
	go func() {
		_ = c.ReadLoop(func(msg *Message) {
			received <- msg
		})
	}()

	select {
	case msg := <-received:
		if msg.Type != EventMessageCreate {
			t.Fatalf("Type = %v, want EventMessageCreate", msg.Type)
		}
		var payload MessageCreatePayload
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			t.Fatalf("unmarshal payload: %v", err)
		}
		if payload.ChannelID != channelID || payload.Content != "hello" {
			t.Fatalf("payload = %+v, want ChannelID=%v Content=hello", payload, channelID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ReadLoop to dispatch the event")
	}
}

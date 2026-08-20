package relay

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/JMThomas00/mynah/internal/wire"
)

// fakeConfigurableResponder implements both Responder and
// ConfigurableResponder, recording the last map it was handed.
type fakeConfigurableResponder struct {
	lastValues map[string]string
}

func (f *fakeConfigurableResponder) Complete(_ context.Context, _, _ string) (string, error) {
	return "", nil
}

func (f *fakeConfigurableResponder) UpdateConfig(values map[string]string) {
	f.lastValues = values
}

func TestHandleConfigUpdateCallsUpdateConfigOnConfigurableResponder(t *testing.T) {
	responder := &fakeConfigurableResponder{}
	s := &Server{pluginID: "test-plugin", responder: responder}

	payload := wire.PluginConfigListPayload{
		Plugins: []wire.PluginInfo{
			{
				ID: "test-plugin",
				ConfigValues: map[string]string{
					"gateway_endpoint": "http://example.invalid/v1/chat/completions",
					"gateway_model":    "some-model",
					"mention_enabled":  "true",
				},
			},
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	s.handleConfigUpdate(data)

	if responder.lastValues["gateway_endpoint"] != "http://example.invalid/v1/chat/completions" {
		t.Errorf("UpdateConfig gateway_endpoint = %q, want the configured endpoint", responder.lastValues["gateway_endpoint"])
	}
	if responder.lastValues["gateway_model"] != "some-model" {
		t.Errorf("UpdateConfig gateway_model = %q, want %q", responder.lastValues["gateway_model"], "some-model")
	}
}

// fakePlainResponder implements only Responder, not ConfigurableResponder.
type fakePlainResponder struct{}

func (fakePlainResponder) Complete(_ context.Context, _, _ string) (string, error) {
	return "", nil
}

func TestHandleConfigUpdateDoesNotPanicOnPlainResponder(t *testing.T) {
	s := &Server{pluginID: "test-plugin", responder: fakePlainResponder{}}

	payload := wire.PluginConfigListPayload{
		Plugins: []wire.PluginInfo{
			{ID: "test-plugin", ConfigValues: map[string]string{"gateway_endpoint": "http://example.invalid"}},
		},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	s.handleConfigUpdate(data) // must not panic
}

func TestStripTrigger(t *testing.T) {
	tests := []struct {
		name    string
		content string
		trigger string
		want    string
	}{
		{"trigger at start", "@burt hello", "burt", "hello"},
		{"trigger at end", "hello @burt", "burt", "hello"},
		{"trigger in middle", "hey @burt what's up", "burt", "hey  what's up"},
		{"case insensitive", "hey @BURT", "burt", "hey"},
		{"no trigger present", "just a message", "burt", "just a message"},
		{"trigger with trailing punctuation", "@burt, can you help?", "burt", ", can you help?"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stripTrigger(tt.content, tt.trigger)
			if got != tt.want {
				t.Errorf("stripTrigger(%q, %q) = %q, want %q", tt.content, tt.trigger, got, tt.want)
			}
		})
	}
}

func TestParseIntOr(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		fallback int
		want     int
	}{
		{"valid positive number", "42", 10, 42},
		{"empty string uses fallback", "", 10, 10},
		{"non-numeric uses fallback", "not-a-number", 10, 10},
		{"zero uses fallback", "0", 10, 10},
		{"negative uses fallback", "-5", 10, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseIntOr(tt.input, tt.fallback)
			if got != tt.want {
				t.Errorf("parseIntOr(%q, %d) = %d, want %d", tt.input, tt.fallback, got, tt.want)
			}
		})
	}
}

// slowResponder blocks for delay before replying, simulating a real
// backend's cold-model-load or long-agentic-task latency.
type slowResponder struct {
	delay time.Duration
}

func (r slowResponder) Complete(ctx context.Context, _, content string) (string, error) {
	select {
	case <-time.After(r.delay):
		return "reply to: " + content, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// fakeConcordServer is a minimal stand-in for Concord's own plugin
// WebSocket endpoint. It hands the test the accepted server-side *Conn (so
// the test can push EVENT dispatches "from Concord," the same direction a
// real relayed message arrives on) and drains that same connection for
// OpSendMessage frames the plugin sends back, forwarding their payloads on
// repliesCh.
func fakeConcordServer(t *testing.T) (srv *httptest.Server, connCh chan *websocket.Conn, replies chan wire.SendMessagePayload) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	connCh = make(chan *websocket.Conn, 1)
	replies = make(chan wire.SendMessagePayload, 8)

	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		connCh <- conn
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var msg wire.Message
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}
			if msg.Op == wire.OpSendMessage {
				var p wire.SendMessagePayload
				if err := json.Unmarshal(msg.Data, &p); err == nil {
					replies <- p
				}
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv, connCh, replies
}

func writeMessageCreateDispatch(t *testing.T, conn *websocket.Conn, channelID uuid.UUID, content string) {
	t.Helper()
	payload := wire.MessageCreatePayload{
		ChatMessage: &wire.ChatMessage{ChannelID: channelID, Content: content},
		Author:      &wire.Author{ID: uuid.New(), Username: "tester"},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal MessageCreatePayload: %v", err)
	}
	dispatch := wire.Message{Op: wire.OpDispatch, Type: wire.EventMessageCreate, Data: raw}
	data, err := json.Marshal(dispatch)
	if err != nil {
		t.Fatalf("marshal dispatch: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		t.Fatalf("write dispatch: %v", err)
	}
}

// blockingResponder blocks until the test explicitly releases it — lets a
// test assert something happened (or didn't) before the reply is available,
// then let the worker finish cleanly so no goroutine outlives the test.
type blockingResponder struct {
	release chan struct{}
}

func (r *blockingResponder) Complete(ctx context.Context, _, content string) (string, error) {
	select {
	case <-r.release:
		return "reply to: " + content, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// TestHandleReturnsImmediatelyEvenWithASlowResponder is the regression test
// for the 2026-08-19 root cause: handleMessageCreate previously ran inline
// on ReadLoop's own goroutine, so a slow Complete() call left
// conn.ReadMessage() uncalled for the whole wait — starving
// gorilla/websocket's automatic pong replies and letting Concord's 60s
// pongWait kill the connection before a slow reply could ever be sent.
// handle() must return near-instantly regardless of how long the
// configured Responder takes, since it's called directly from ReadLoop's
// own loop — enqueueing onto messageQueue, not waiting on the worker, is
// what makes that true.
func TestHandleReturnsImmediatelyEvenWithASlowResponder(t *testing.T) {
	srv, connCh, replies := fakeConcordServer(t)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	c, err := wire.Dial(wsURL)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	select {
	case <-connCh:
	case <-time.After(time.Second):
		t.Fatal("server never accepted the connection")
	}

	responder := &blockingResponder{release: make(chan struct{})}
	s := New(c, "test-plugin", uuid.Nil, responder)
	channelID := uuid.New()
	s.RegisterChannel(channelID, nil)

	payload := wire.MessageCreatePayload{
		ChatMessage: &wire.ChatMessage{ChannelID: channelID, Content: "hello"},
		Author:      &wire.Author{ID: uuid.New(), Username: "tester"},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	start := time.Now()
	s.handle(&wire.Message{Type: wire.EventMessageCreate, Data: raw})
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Errorf("handle() took %v against a still-blocked Responder, want near-instant — looks like it's blocking on the worker instead of just enqueueing", elapsed)
	}

	// Let the worker finish so it doesn't outlive the test.
	close(responder.release)
	select {
	case <-replies:
	case <-time.After(time.Second):
		t.Fatal("worker never processed the queued message after release")
	}
}

// TestQueuedMessagesAreProcessedOneAtATimeInOrder is the regression test for
// 2026-08-19's second finding: naively backgrounding every message onto its
// own goroutine (the first fix attempt) let multiple real Complete() calls
// run concurrently against the same backend for the first time ever — Alice's
// plugin process crashed shortly after a live test drove exactly that (2-4
// concurrent slow replies at once), losing every reply in flight with no
// trace. messageQueue's single worker must serialize handling: two replies
// answered after a real delay should complete in roughly 2x the delay
// (proving they ran one at a time, not concurrently) and, since content is
// deterministic here, in the order they were sent.
func TestQueuedMessagesAreProcessedOneAtATimeInOrder(t *testing.T) {
	srv, connCh, replies := fakeConcordServer(t)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	c, err := wire.Dial(wsURL)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	var serverConn *websocket.Conn
	select {
	case serverConn = <-connCh:
	case <-time.After(time.Second):
		t.Fatal("server never accepted the connection")
	}

	const delay = 150 * time.Millisecond
	s := New(c, "test-plugin", uuid.Nil, slowResponder{delay: delay})
	channelID := uuid.New()
	s.RegisterChannel(channelID, nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.Run()
	}()
	defer func() {
		c.Close()
		wg.Wait()
	}()

	start := time.Now()
	writeMessageCreateDispatch(t, serverConn, channelID, "first")
	writeMessageCreateDispatch(t, serverConn, channelID, "second")

	var got []string
	deadline := time.After(2 * time.Second)
	for len(got) < 2 {
		select {
		case p := <-replies:
			got = append(got, p.Content)
		case <-deadline:
			t.Fatalf("timed out waiting for replies, got %d of 2", len(got))
		}
	}
	elapsed := time.Since(start)

	// Serialized handling takes ~2*delay (300ms) plus scheduling slack;
	// concurrent handling (the bug) would land close to 1*delay (150ms).
	// 250ms cleanly separates the two without being flaky under normal
	// test-machine jitter.
	if elapsed < 250*time.Millisecond {
		t.Errorf("both replies took only %v, want close to 2x the %v delay — looks like messages are running concurrently against the backend again", elapsed, delay)
	}
	if got[0] != "reply to: first" || got[1] != "reply to: second" {
		t.Errorf("replies arrived as %v, want [\"reply to: first\", \"reply to: second\"] in send order", got)
	}
}

// panicOnceResponder panics on its first call and replies normally after —
// stands in for any future bug in this path (a malformed backend response,
// a nil dereference, anything), while staying the one Responder instance
// for the whole test (swapping s.responder mid-test would itself be an
// unsynchronized data race against the worker goroutine reading it).
type panicOnceResponder struct {
	panicked atomic.Bool
}

func (r *panicOnceResponder) Complete(_ context.Context, _, content string) (string, error) {
	if !r.panicked.Swap(true) {
		panic("simulated bug in Complete")
	}
	return "reply to: " + content, nil
}

// TestWorkerSurvivesAPanicAndSendsFallbackReply is the regression test for
// the other half of 2026-08-19's finding: this plugin had zero panic
// recovery anywhere, so any bug in handleMessageCreate's path — not just
// the concurrency-under-load scenario messageQueue's own doc comment
// describes — took the whole process down, silently losing every reply in
// flight. A panic handling one message must produce the same
// "...(no response, try again)" fallback a normal Complete() error would,
// and the worker must still be alive and able to process the next message
// afterward — proven here with a normal responder following the
// panicking one, both sent before either is processed.
func TestWorkerSurvivesAPanicAndSendsFallbackReply(t *testing.T) {
	srv, connCh, replies := fakeConcordServer(t)
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	c, err := wire.Dial(wsURL)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	var serverConn *websocket.Conn
	select {
	case serverConn = <-connCh:
	case <-time.After(time.Second):
		t.Fatal("server never accepted the connection")
	}

	s := New(c, "test-plugin", uuid.Nil, &panicOnceResponder{})
	channelID := uuid.New()
	s.RegisterChannel(channelID, nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.Run()
	}()
	defer func() {
		c.Close()
		wg.Wait()
	}()

	writeMessageCreateDispatch(t, serverConn, channelID, "trigger the panic")

	select {
	case p := <-replies:
		if p.Content != "...(no response, try again)" {
			t.Errorf("reply after panic = %q, want the standard fallback text", p.Content)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the post-panic fallback reply — did the worker die instead of recovering?")
	}

	// Prove the process (and its one worker) is still alive: a second
	// message must still be processed normally by the same Responder.
	writeMessageCreateDispatch(t, serverConn, channelID, "still alive")

	select {
	case p := <-replies:
		if p.Content != "reply to: still alive" {
			t.Errorf("reply after recovery = %q, want a normal reply", p.Content)
		}
	case <-time.After(time.Second):
		t.Fatal("worker never processed a message after recovering from the panic")
	}
}

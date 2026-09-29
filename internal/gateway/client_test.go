package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func collect(t *testing.T, c *Client, content string) (string, error) {
	t.Helper()
	var got strings.Builder
	err := c.Complete(context.Background(), content, func(s string) { got.WriteString(s) })
	return got.String(), err
}

func TestEchoesWithoutAnEndpoint(t *testing.T) {
	got, err := collect(t, New(Config{}), "hi")
	if err != nil || got != "echo: hi" {
		t.Fatalf("got %q, %v", got, err)
	}
}

// A streaming backend's pieces arrive as they're sent, and the request
// carries the settings: model, key, persona, stream.
func TestStreamsServerSentEvents(t *testing.T) {
	var req chatRequest
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "text/event-stream")
		for _, piece := range []string{"Here", "'s **bold**", " and\n```go\nx := 1\n```"} {
			b, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"delta": map[string]string{"content": piece}}}})
			fmt.Fprintf(w, "data: %s\n\n", b)
			w.(http.Flusher).Flush()
		}
		fmt.Fprint(w, ": a comment\n\ndata: {\"choices\":[{\"delta\":{}}]}\n\ndata: [DONE]\n\n")
	}))
	defer srv.Close()

	c := New(Config{APIKey: "env-key", SystemPrompt: "from the file"})
	c.UpdateConfig(map[string]string{"gateway_endpoint": srv.URL, "gateway_model": "m1", "gateway_api_key": "secret-key", "persona": "You are Burt."})
	var pieces []string
	err := c.Complete(context.Background(), "hello", func(s string) { pieces = append(pieces, s) })
	if err != nil {
		t.Fatal(err)
	}
	if len(pieces) != 3 || strings.Join(pieces, "") != "Here's **bold** and\n```go\nx := 1\n```" {
		t.Fatalf("pieces %q", pieces)
	}
	if !req.Stream || req.Model != "m1" || auth != "Bearer secret-key" ||
		len(req.Messages) != 2 || req.Messages[0].Content != "You are Burt." || req.Messages[1].Content != "hello" {
		t.Fatalf("request %+v, auth %q", req, auth)
	}

	// Clearing the settings falls back to the environment's key and file.
	c.UpdateConfig(map[string]string{"gateway_endpoint": srv.URL})
	_, _ = collect(t, c, "again")
	if auth != "Bearer env-key" || req.Messages[0].Content != "from the file" {
		t.Fatalf("fallbacks not used: auth %q, system %q", auth, req.Messages[0].Content)
	}
}

// A backend that ignores "stream" and sends one JSON reply still works.
func TestAcceptsANonStreamingReply(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"all at once"}}]}`)
	}))
	defer srv.Close()
	c := New(Config{Endpoint: srv.URL})
	if got, err := collect(t, c, "q"); err != nil || got != "all at once" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestReportsFailures(t *testing.T) {
	for name, handler := range map[string]http.HandlerFunc{
		"status": func(w http.ResponseWriter, r *http.Request) { http.Error(w, "bad key", http.StatusUnauthorized) },
		"empty": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: [DONE]\n\n")
		},
		"error event": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: {\"error\":{\"message\":\"model overloaded\"}}\n\n")
		},
	} {
		srv := httptest.NewServer(handler)
		_, err := collect(t, New(Config{Endpoint: srv.URL}), "q")
		srv.Close()
		if err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

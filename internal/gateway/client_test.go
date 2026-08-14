package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCompleteSendsOpenAICompatibleRequest(t *testing.T) {
	var gotAuth string
	var gotReq chatCompletionRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		json.NewEncoder(w).Encode(chatCompletionResponse{
			Choices: []struct {
				Message chatMessage `json:"message"`
			}{
				{Message: chatMessage{Role: "assistant", Content: "hello back"}},
			},
		})
	}))
	defer srv.Close()

	c := New(Config{Endpoint: srv.URL, APIKey: "test-key", Model: "burt-model"})
	reply, err := c.Complete(context.Background(), "channel-123", "hello")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if reply != "hello back" {
		t.Errorf("reply = %q, want %q", reply, "hello back")
	}
	if gotAuth != "Bearer test-key" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer test-key")
	}
	if gotReq.Model != "burt-model" {
		t.Errorf("request model = %q, want %q", gotReq.Model, "burt-model")
	}
	if len(gotReq.Messages) != 1 || gotReq.Messages[0].Role != "user" || gotReq.Messages[0].Content != "hello" {
		t.Errorf("request messages = %+v, want one user message \"hello\"", gotReq.Messages)
	}
}

func TestCompletePrependsSystemPromptWhenConfigured(t *testing.T) {
	var gotReq chatCompletionRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotReq)
		json.NewEncoder(w).Encode(chatCompletionResponse{
			Choices: []struct {
				Message chatMessage `json:"message"`
			}{
				{Message: chatMessage{Role: "assistant", Content: "hi"}},
			},
		})
	}))
	defer srv.Close()

	c := New(Config{Endpoint: srv.URL, SystemPrompt: "You are Burt, a homelab assistant."})
	if _, err := c.Complete(context.Background(), "channel-123", "hello"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if len(gotReq.Messages) != 2 {
		t.Fatalf("got %d messages, want 2 (system + user): %+v", len(gotReq.Messages), gotReq.Messages)
	}
	if gotReq.Messages[0].Role != "system" || gotReq.Messages[0].Content != "You are Burt, a homelab assistant." {
		t.Errorf("first message = %+v, want system prompt", gotReq.Messages[0])
	}
	if gotReq.Messages[1].Role != "user" || gotReq.Messages[1].Content != "hello" {
		t.Errorf("second message = %+v, want user \"hello\"", gotReq.Messages[1])
	}
}

func TestCompleteOmitsSystemPromptWhenNotConfigured(t *testing.T) {
	var gotReq chatCompletionRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotReq)
		json.NewEncoder(w).Encode(chatCompletionResponse{
			Choices: []struct {
				Message chatMessage `json:"message"`
			}{
				{Message: chatMessage{Role: "assistant", Content: "hi"}},
			},
		})
	}))
	defer srv.Close()

	c := New(Config{Endpoint: srv.URL})
	if _, err := c.Complete(context.Background(), "channel-123", "hello"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if len(gotReq.Messages) != 1 || gotReq.Messages[0].Role != "user" {
		t.Errorf("got %+v, want exactly one user message", gotReq.Messages)
	}
}

func TestCompleteErrorsOnNonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"invalid api key"}`))
	}))
	defer srv.Close()

	c := New(Config{Endpoint: srv.URL, APIKey: "wrong-key"})
	if _, err := c.Complete(context.Background(), "channel-123", "hello"); err == nil {
		t.Fatal("expected an error on 401, got nil")
	}
}

func TestCompleteErrorsOnEmptyChoices(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(chatCompletionResponse{})
	}))
	defer srv.Close()

	c := New(Config{Endpoint: srv.URL})
	if _, err := c.Complete(context.Background(), "channel-123", "hello"); err == nil {
		t.Fatal("expected an error on empty choices, got nil")
	}
}

func TestCompleteReturnsEchoWhenEndpointUnset(t *testing.T) {
	c := New(Config{})
	reply, err := c.Complete(context.Background(), "channel-123", "hello")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if reply != "echo: hello" {
		t.Errorf("reply = %q, want %q", reply, "echo: hello")
	}
}

func TestUpdateConfigChangesEndpointAndModel(t *testing.T) {
	var gotReq chatCompletionRequest

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotReq)
		json.NewEncoder(w).Encode(chatCompletionResponse{
			Choices: []struct {
				Message chatMessage `json:"message"`
			}{
				{Message: chatMessage{Role: "assistant", Content: "hi"}},
			},
		})
	}))
	defer srv.Close()

	c := New(Config{})
	// Before any config push, it's echo mode.
	if reply, err := c.Complete(context.Background(), "channel-123", "hello"); err != nil || reply != "echo: hello" {
		t.Fatalf("before UpdateConfig: reply=%q err=%v, want echo", reply, err)
	}

	c.UpdateConfig(map[string]string{"gateway_endpoint": srv.URL, "gateway_model": "new-model"})

	if _, err := c.Complete(context.Background(), "channel-123", "hello"); err != nil {
		t.Fatalf("after UpdateConfig: Complete: %v", err)
	}
	if gotReq.Model != "new-model" {
		t.Errorf("request model = %q, want %q", gotReq.Model, "new-model")
	}
}

func TestUpdateConfigMissingKeyFallsBackToEcho(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("gateway endpoint should not have been called after UpdateConfig with no gateway_endpoint key")
	}))
	defer srv.Close()

	c := New(Config{Endpoint: srv.URL})
	c.UpdateConfig(map[string]string{"mention_enabled": "true"}) // no gateway_endpoint key at all

	reply, err := c.Complete(context.Background(), "channel-123", "hello")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if reply != "echo: hello" {
		t.Errorf("reply = %q, want echo fallback after a config push with no gateway_endpoint key", reply)
	}
}

// Package gateway is the HTTP client to an external AI backend — any
// endpoint speaking the OpenAI-compatible /v1/chat/completions shape
// (Hermes' own API server, OpenClaw if it exposes one, OpenAI itself,
// OpenRouter, most local LLM servers). Deliberately not tied to one
// specific backend — see 10 Projects/AI Passthrough/AI Passthrough - Plan.md
// in the Obsidian vault for the full design history (this package started
// life as "internal/hermes," renamed once the plugin's own scope was
// confirmed to be backend-agnostic).
//
// Request/response shape ({model, messages[]} -> {choices[0].message.content})
// is the single most-copied HTTP API shape there is — grounded in Hermes'
// own documented API server (30 Resources/AI & Agents/Hermes Agent Docs/07
// Integrations/MCP, ACP & API Server.md), which is itself just one
// implementation of this same OpenAI-compatible contract. Still genuinely
// unverified against any real running gateway — that needs live infra
// (Plan Part 2's step 3), not something fixable from here.
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Config seeds a new Client. Endpoint and Model are expected to start
// empty and arrive later via UpdateConfig — see the Client doc below for
// why. APIKey and SystemPrompt are set once here and never change for the
// life of the process (APIKey is a local secret read from this install's
// own [process.env], SystemPrompt is the persona doc's contents read once
// at startup).
type Config struct {
	Endpoint     string // full URL to the backend's chat endpoint, e.g. http://127.0.0.1:8642/v1/chat/completions — now sourced from the gateway_endpoint server_config_field (Settings > Plugins), not env
	APIKey       string // this install's own secret — GATEWAY_API_KEY in [process.env], deliberately never exposed through Concord's admin UI (no masking/encryption exists there — see the Plan doc)
	Model        string // optional; some OpenAI-compatible servers require a non-empty "model" field even when the endpoint only ever serves one backend — gateway_model server_config_field
	SystemPrompt string // persona doc content, read from PERSONA_DOC_PATH by cmd/mynah-server/main.go; sent as a system-role message on every request. Empty means no persona injection.
}

// Client calls an OpenAI-compatible chat completions endpoint.
//
// Endpoint/Model are mutable after construction, guarded by mu, because
// they're no longer known at process-startup time — they live in
// Concord's DB as server_config_field values and only arrive over the
// wire after this Client already has to exist (relay.New needs a
// concrete Responder in hand before any config push can arrive). See
// UpdateConfig. APIKey/SystemPrompt never change post-construction, so
// they're plain fields read without locking.
type Client struct {
	apiKey       string
	systemPrompt string

	mu       sync.RWMutex
	endpoint string
	model    string

	http *http.Client
}

func New(cfg Config) *Client {
	return &Client{
		apiKey:       cfg.APIKey,
		systemPrompt: cfg.SystemPrompt,
		endpoint:     cfg.Endpoint,
		model:        cfg.Model,
		http:         &http.Client{Timeout: 20*time.Minute + 30*time.Second}, // slightly above relay.completeTimeout's 20min so our own context deadline (passed via NewRequestWithContext) always fires first — this is a backstop, not the real deadline
	}
}

// UpdateConfig implements relay.ConfigurableResponder — applies a live
// push of this plugin's server_config_field values (both at connect time
// and on every admin edit in Settings > Plugins). Only gateway_endpoint/
// gateway_model are consulted; everything else in values (mention_enabled
// etc.) is relay.Server's own concern, not this client's. A key missing
// from values is treated the same as an explicit empty string — in
// practice Concord always includes every declared field in this map, so
// this only matters for hand-built test fixtures.
func (c *Client) UpdateConfig(values map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.endpoint = values["gateway_endpoint"]
	c.model = values["gateway_model"]
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatCompletionRequest struct {
	Model    string        `json:"model,omitempty"`
	Messages []chatMessage `json:"messages"`
}

type chatCompletionResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
}

// Complete implements relay.Responder. Stateless per call, matching
// /v1/chat/completions' documented "full history per request" contract —
// channelID isn't sent (v1 limitation: no cross-message memory yet).
//
// Falls back to a trivial echo when no endpoint is configured yet (a
// fresh process before its first config push, or an admin simply hasn't
// filled in gateway_endpoint) — this is the same dev/test-friendly
// default the plugin has always had, just now driven by live config
// instead of a startup-time env var check.
func (c *Client) Complete(ctx context.Context, channelID, content string) (string, error) {
	c.mu.RLock()
	endpoint, model := c.endpoint, c.model
	c.mu.RUnlock()

	if endpoint == "" {
		return "echo: " + content, nil
	}

	var messages []chatMessage
	if c.systemPrompt != "" {
		messages = append(messages, chatMessage{Role: "system", Content: c.systemPrompt})
	}
	messages = append(messages, chatMessage{Role: "user", Content: content})

	reqBody, err := json.Marshal(chatCompletionRequest{
		Model:    model,
		Messages: messages,
	})
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("gateway returned %d: %s", resp.StatusCode, string(body))
	}

	var result chatCompletionResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("unmarshal response: %w", err)
	}
	if len(result.Choices) == 0 || result.Choices[0].Message.Content == "" {
		return "", fmt.Errorf("empty reply from gateway")
	}
	return result.Choices[0].Message.Content, nil
}

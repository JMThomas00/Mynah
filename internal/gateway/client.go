// Package gateway is the HTTP client to an AI backend: any endpoint that
// speaks the OpenAI-compatible /v1/chat/completions shape (OpenAI,
// OpenRouter, Hermes' API server, most local LLM servers).
//
// Replies are streamed ("stream": true, server-sent events) so Mynah can
// show them being written. A backend that ignores stream and answers with
// one JSON body works too: the whole reply arrives as one piece.
package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Config seeds a Client with what's known at startup. The endpoint, model,
// API key and persona can all be (re)set later from Concord's settings;
// see UpdateConfig.
type Config struct {
	APIKey       string // fallback key (GATEWAY_API_KEY), for installs from before the key was a setting
	SystemPrompt string // fallback persona (PERSONA_DOC_PATH's contents)
	Endpoint     string // e.g. http://127.0.0.1:8642/v1/chat/completions
	Model        string
}

// Client calls an OpenAI-compatible chat completions endpoint. Its
// settings can change at any time (an admin saving the plugin's settings),
// so they're read under a lock at the start of each request.
type Client struct {
	mu           sync.RWMutex
	endpoint     string
	model        string
	apiKey       string
	systemPrompt string

	fallbackKey    string
	fallbackPrompt string

	http *http.Client
}

func New(cfg Config) *Client {
	return &Client{
		endpoint: cfg.Endpoint, model: cfg.Model,
		apiKey: cfg.APIKey, systemPrompt: cfg.SystemPrompt,
		fallbackKey: cfg.APIKey, fallbackPrompt: cfg.SystemPrompt,
		// A backstop only: each request carries its own deadline (the
		// relay's), which fires first.
		http: &http.Client{Timeout: 21 * time.Minute},
	}
}

// UpdateConfig applies the plugin's server settings: gateway_endpoint,
// gateway_model, gateway_api_key (a secret) and persona. An empty key or
// persona falls back to the one from the environment/persona file.
func (c *Client) UpdateConfig(values map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.endpoint = strings.TrimSpace(values["gateway_endpoint"])
	c.model = strings.TrimSpace(values["gateway_model"])
	c.apiKey = c.fallbackKey
	if k := strings.TrimSpace(values["gateway_api_key"]); k != "" {
		c.apiKey = k
	}
	c.systemPrompt = c.fallbackPrompt
	if p := strings.TrimSpace(values["persona"]); p != "" {
		c.systemPrompt = p
	}
}

// Configured reports whether an endpoint is set (without one, Complete
// echoes).
func (c *Client) Configured() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.endpoint != ""
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string        `json:"model,omitempty"`
	Messages []chatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
}

// ErrEmptyReply means the backend answered without any text.
var ErrEmptyReply = errors.New("the AI backend sent an empty reply")

// Complete answers content, calling onText with each piece of the reply
// as it arrives (in order, from this goroutine). Stateless per call: the
// persona is sent as a system message, then the user's message.
//
// With no endpoint configured it echoes, so the relay can be tried with no
// AI backend at all.
func (c *Client) Complete(ctx context.Context, content string, onText func(string)) error {
	c.mu.RLock()
	endpoint, model, key, prompt := c.endpoint, c.model, c.apiKey, c.systemPrompt
	c.mu.RUnlock()

	if endpoint == "" {
		onText("echo: " + content)
		return nil
	}

	var messages []chatMessage
	if prompt != "" {
		messages = append(messages, chatMessage{Role: "system", Content: prompt})
	}
	messages = append(messages, chatMessage{Role: "user", Content: content})
	body, err := json.Marshal(chatRequest{Model: model, Messages: messages, Stream: true})
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream, application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 500))
		return fmt.Errorf("gateway returned %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	wrote := false
	emit := func(s string) {
		if s != "" {
			wrote = true
			onText(s)
		}
	}
	if strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		err = readEvents(resp.Body, emit)
	} else {
		err = readWhole(resp.Body, emit)
	}
	if err != nil {
		return err
	}
	if !wrote {
		return ErrEmptyReply
	}
	return nil
}

// streamChunk is one server-sent event of a streamed completion (or, with
// Message set, a whole non-streamed reply).
type streamChunk struct {
	Choices []struct {
		Delta   chatMessage `json:"delta"`
		Message chatMessage `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

// readEvents reads "data: {...}" lines until "data: [DONE]" or the end of
// the body, emitting each piece of content.
func readEvents(r io.Reader, emit func(string)) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		data, ok := strings.CutPrefix(strings.TrimSpace(sc.Text()), "data:")
		if !ok {
			continue // comments, "event:" lines, blank separators
		}
		data = strings.TrimSpace(data)
		if data == "[DONE]" {
			return nil
		}
		var chunk streamChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue // tolerate one malformed event rather than lose the reply
		}
		if chunk.Error != nil {
			return fmt.Errorf("gateway error: %s", chunk.Error.Message)
		}
		for _, ch := range chunk.Choices {
			emit(ch.Delta.Content)
			emit(ch.Message.Content)
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("reading the reply: %w", err)
	}
	return nil
}

// readWhole handles a backend that answered with one JSON body.
func readWhole(r io.Reader, emit func(string)) error {
	var result streamChunk
	if err := json.NewDecoder(r).Decode(&result); err != nil {
		return fmt.Errorf("unmarshal response: %w", err)
	}
	if result.Error != nil {
		return fmt.Errorf("gateway error: %s", result.Error.Message)
	}
	for _, ch := range result.Choices {
		emit(ch.Message.Content)
	}
	return nil
}

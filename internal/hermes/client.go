// Package hermes is the HTTP client to a restricted Hermes gateway profile
// — see 10 Projects/AI Passthrough/AI Passthrough - Plan.md (Part 2b/2g) in
// the Obsidian vault for the full design: one profile per persona,
// toolset-restricted, no MCP/homelab access, SSRF protection left at its
// secure-by-default setting.
package hermes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// allowedToolsets is a hard-coded allowlist of toolset names this plugin
// will ever pass through to Hermes, independent of what's toggled in
// Concord's plugin_server_config — so a misconfigured or tampered config
// row can never smuggle "terminal"/"code_execution"/an "mcp-*" set through.
// See the Plan doc's Part 2b for why this is a curated static list rather
// than a fully dynamic query of Hermes' own toolset catalog.
var allowedToolsets = map[string]bool{
	"web":       true,
	"search":    true,
	"image_gen": true,
	"tts":       true,
}

// Config is this client's connection info — read from the plugin process's
// own environment ([process.env] in plugin.toml), never from Concord's DB.
// See cmd/mynah-server/main.go.
type Config struct {
	Endpoint string   // e.g. http://127.0.0.1:PORT/v1/chat — HERMES_ENDPOINT
	APIKey   string   // HERMES_API_KEY
	Toolsets []string // requested toolset names, filtered through allowedToolsets before every request
}

// Client calls a restricted Hermes gateway's chat-completion endpoint.
type Client struct {
	cfg      Config
	http     *http.Client
	toolsets []string // cfg.Toolsets, pre-filtered through allowedToolsets once at construction
}

func New(cfg Config) *Client {
	var toolsets []string
	for _, t := range cfg.Toolsets {
		if allowedToolsets[t] {
			toolsets = append(toolsets, t)
		}
	}
	return &Client{
		cfg:      cfg,
		http:     &http.Client{Timeout: 65 * time.Second}, // slightly above relay.completeTimeout's 60s so our own context deadline fires first
		toolsets: toolsets,
	}
}

// completeRequest is this client's best-documented guess at Hermes' chat
// gateway request shape (message + a conversation/session identifier +
// requested toolsets), modeled on `hermes chat --toolsets "web,terminal"`'s
// CLI-level parameter per 30 Resources/AI & Agents/Hermes Agent Docs/03
// Core Features/Tools & Terminal Backends.md. NOT yet verified against a
// real running Hermes gateway — confirm and adjust this shape (and
// completeResponse below) during Part 2's step 3, once a real restricted
// profile exists to test against. See the Plan doc's open question #7.
type completeRequest struct {
	Message        string   `json:"message"`
	ConversationID string   `json:"conversation_id"`
	Toolsets       []string `json:"toolsets,omitempty"`
}

type completeResponse struct {
	Reply string `json:"reply"`
}

// Complete implements relay.Responder.
func (c *Client) Complete(ctx context.Context, channelID, content string) (string, error) {
	reqBody, err := json.Marshal(completeRequest{
		Message:        content,
		ConversationID: channelID,
		Toolsets:       c.toolsets,
	})
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.Endpoint, bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
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
		return "", fmt.Errorf("hermes gateway returned %d: %s", resp.StatusCode, string(body))
	}

	var result completeResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("unmarshal response: %w", err)
	}
	if result.Reply == "" {
		return "", fmt.Errorf("empty reply from hermes gateway")
	}
	return result.Reply, nil
}

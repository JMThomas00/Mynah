// Command aipassthrough-server is the AI Passthrough plugin's process: it
// speaks Concord's plugin wire protocol and relays chat messages to a
// restricted Hermes gateway (or, with no HERMES_ENDPOINT configured, a
// trivial echo responder — useful on its own for verifying the relay path
// end-to-end before any Hermes backend exists, per the Plan doc's Part 2
// sequencing). One install per persona (e.g. Plugins/Burt/, Plugins/Alice/),
// same binary, different [process.env] in each install's plugin.toml.
package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/JMThomas00/aipassthrough/internal/hermes"
	"github.com/JMThomas00/aipassthrough/internal/relay"
	"github.com/JMThomas00/aipassthrough/internal/wire"
)

// echoResponder is the step-1 smoke-test backend: no Hermes, no LLM, just
// proves the whole Concord-relay -> plugin -> reply path works end-to-end.
// Used automatically whenever HERMES_ENDPOINT isn't set.
type echoResponder struct{}

func (echoResponder) Complete(ctx context.Context, channelID, content string) (string, error) {
	return "echo: " + content, nil
}

func main() {
	wsURL := os.Getenv("CONCORD_WS_URL")
	pluginID := os.Getenv("CONCORD_PLUGIN_ID")
	token := os.Getenv("CONCORD_PLUGIN_TOKEN")
	if wsURL == "" || pluginID == "" || token == "" {
		log.Fatal("aipassthrough-server: CONCORD_WS_URL, CONCORD_PLUGIN_ID, and CONCORD_PLUGIN_TOKEN must all be set")
	}

	client, err := wire.Dial(wsURL)
	if err != nil {
		log.Fatalf("aipassthrough-server: dial %s: %v", wsURL, err)
	}
	defer client.Close()

	selfUserID, err := client.Identify(token)
	if err != nil {
		log.Fatalf("aipassthrough-server: identify: %v", err)
	}
	log.Printf("aipassthrough-server: connected to %s as plugin %s (user %s)", wsURL, pluginID, selfUserID)

	responder := buildResponder()

	srv := relay.New(client, pluginID, selfUserID, responder)
	if err := srv.Run(); err != nil {
		log.Fatalf("aipassthrough-server: %v", err)
	}
}

// buildResponder picks the real Hermes-backed responder when configured,
// falling back to the echo responder otherwise — see the package doc.
func buildResponder() relay.Responder {
	endpoint := os.Getenv("HERMES_ENDPOINT")
	if endpoint == "" {
		log.Print("aipassthrough-server: no HERMES_ENDPOINT configured — running in echo mode")
		return echoResponder{}
	}

	label := os.Getenv("PERSONA_LABEL")
	log.Printf("aipassthrough-server: %s persona backed by Hermes gateway at %s", label, endpoint)

	return hermes.New(hermes.Config{
		Endpoint: endpoint,
		APIKey:   os.Getenv("HERMES_API_KEY"),
		Toolsets: parseToolsets(os.Getenv("HERMES_TOOLSETS")),
	})
}

// parseToolsets reads a comma-separated HERMES_TOOLSETS env var (e.g.
// "web,search") — set from this install's own plugin.toml [process.env],
// itself built from Concord admin-toggled tool_* server_config_fields once
// Part 2 wires that translation up. Still filtered a second time through
// hermes.allowedToolsets before ever reaching a real request, regardless of
// what's in this env var.
func parseToolsets(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

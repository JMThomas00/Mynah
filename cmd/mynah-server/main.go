// Command mynah-server is Mynah's process (a Concord plugin for chatting
// with an LLM-backed persona, formerly named "AI Passthrough"): it speaks
// Concord's plugin wire protocol and relays chat messages to an external
// AI gateway (any OpenAI-compatible /v1/chat/completions endpoint — see
// internal/gateway), starting in a trivial echo mode until an admin fills
// in that gateway's endpoint via Concord's own Settings > Plugins UI (no
// [process.env]/restart needed for that part anymore — only the API key
// and persona doc path still live there, see buildResponder below). One
// install per persona (e.g. Plugins/Burt/, Plugins/Alice/), same binary,
// different [process.env] in each install's plugin.toml.
package main

import (
	"log"
	"os"

	"github.com/JMThomas00/mynah/internal/gateway"
	"github.com/JMThomas00/mynah/internal/relay"
	"github.com/JMThomas00/mynah/internal/wire"
)

func main() {
	wsURL := os.Getenv("CONCORD_WS_URL")
	pluginID := os.Getenv("CONCORD_PLUGIN_ID")
	token := os.Getenv("CONCORD_PLUGIN_TOKEN")
	if wsURL == "" || pluginID == "" || token == "" {
		log.Fatal("mynah-server: CONCORD_WS_URL, CONCORD_PLUGIN_ID, and CONCORD_PLUGIN_TOKEN must all be set")
	}

	client, err := wire.Dial(wsURL)
	if err != nil {
		log.Fatalf("mynah-server: dial %s: %v", wsURL, err)
	}
	defer client.Close()

	selfUserID, err := client.Identify(token)
	if err != nil {
		log.Fatalf("mynah-server: identify: %v", err)
	}
	log.Printf("mynah-server: connected to %s as plugin %s (user %s)", wsURL, pluginID, selfUserID)

	responder := buildResponder()

	srv := relay.New(client, pluginID, selfUserID, responder)
	if err := srv.Run(); err != nil {
		log.Fatalf("mynah-server: %v", err)
	}
}

// buildResponder always constructs one gateway.Client, seeded only with
// the two things that still have to be local secrets/files
// (GATEWAY_API_KEY, the persona doc) — endpoint and model are deliberately
// left empty here. They arrive live over the wire as this plugin's
// gateway_endpoint/gateway_model server_config_field values (Settings >
// Plugins), the first push landing shortly after Identify above. Until
// then, gateway.Client runs in its own built-in echo mode — see its
// Complete doc. This is why there's no echo/real branch here anymore: the
// endpoint literally isn't knowable at this point in startup.
func buildResponder() relay.Responder {
	label := os.Getenv("PERSONA_LABEL")
	log.Printf("mynah-server: %s persona starting — gateway endpoint/model come from Settings > Plugins; starting in echo mode until configured", label)

	return gateway.New(gateway.Config{
		APIKey:       os.Getenv("GATEWAY_API_KEY"),
		SystemPrompt: loadPersonaDoc(),
	})
}

// loadPersonaDoc reads PERSONA_DOC_PATH (relative to this install's own
// folder — the plugin process's working directory is set to its manifest
// dir by Concord's supervisor, e.g. Plugins/Burt/) if configured. Missing
// or unset is not fatal: the persona doc is content-writing work that may
// not exist yet (Plan Part 2f), and this process should still run — just
// without persona injection — rather than refuse to start.
func loadPersonaDoc() string {
	path := os.Getenv("PERSONA_DOC_PATH")
	if path == "" {
		return ""
	}
	content, err := os.ReadFile(path)
	if err != nil {
		log.Printf("mynah-server: PERSONA_DOC_PATH=%s set but unreadable (%v) — running without persona injection", path, err)
		return ""
	}
	return string(content)
}

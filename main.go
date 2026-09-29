// Mynah: an AI persona for Concord. It answers chat messages with any
// OpenAI-compatible model (OpenAI, OpenRouter, a local LLM server...),
// streaming each reply into the channel as it's written.
//
// Launched by a Concord server (which sets CONCORD_* variables) it runs as
// a plugin; everything is configured in Server Settings > Plugins. Run it
// from a terminal instead to chat with the persona directly:
//
//	mynah -endpoint http://127.0.0.1:11434/v1/chat/completions -model llama3.2
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/JMThomas00/Concord/sdk/plugin"

	"github.com/JMThomas00/mynah/internal/gateway"
	"github.com/JMThomas00/mynah/internal/relay"
)

func main() {
	responder := gateway.New(gateway.Config{
		APIKey:       os.Getenv("GATEWAY_API_KEY"),
		SystemPrompt: loadPersonaDoc(),
		Endpoint:     os.Getenv("GATEWAY_ENDPOINT"),
		Model:        os.Getenv("GATEWAY_MODEL"),
	})

	cfg, underConcord := plugin.ConfigFromEnv()
	if !underConcord {
		standalone(responder)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := plugin.Run(ctx, cfg, relay.New(responder).Handler()); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

// standalone chats with the persona in this terminal.
func standalone(responder *gateway.Client) {
	endpoint := flag.String("endpoint", os.Getenv("GATEWAY_ENDPOINT"), "chat completions URL (or GATEWAY_ENDPOINT); empty echoes")
	model := flag.String("model", os.Getenv("GATEWAY_MODEL"), "model name (or GATEWAY_MODEL)")
	key := flag.String("key", "", "API key (or GATEWAY_API_KEY)")
	persona := flag.String("persona", "", "persona instructions (or PERSONA_DOC_PATH, a file)")
	flag.Parse()
	responder.UpdateConfig(map[string]string{
		"gateway_endpoint": *endpoint, "gateway_model": *model,
		"gateway_api_key": *key, "persona": *persona,
	})
	if responder.Configured() {
		fmt.Printf("Mynah: talking to %s. Type a message; Ctrl+C to quit.\n", *endpoint)
	} else {
		fmt.Println("Mynah: no -endpoint given, so replies just echo. Type a message; Ctrl+C to quit.")
	}
	in := bufio.NewScanner(os.Stdin)
	for fmt.Print("> "); in.Scan(); fmt.Print("> ") {
		if in.Text() == "" {
			continue
		}
		err := responder.Complete(context.Background(), in.Text(), func(text string) { fmt.Print(text) })
		fmt.Println()
		if err != nil {
			fmt.Println("(error:", err, ")")
		}
	}
}

// loadPersonaDoc reads PERSONA_DOC_PATH (relative to the plugin's folder)
// if set. The persona setting in Concord takes precedence; this is for
// installs from before it existed.
func loadPersonaDoc() string {
	path := os.Getenv("PERSONA_DOC_PATH")
	if path == "" {
		return ""
	}
	content, err := os.ReadFile(path)
	if err != nil {
		log.Printf("mynah: PERSONA_DOC_PATH=%s unreadable (%v); running without it", path, err)
		return ""
	}
	return string(content)
}

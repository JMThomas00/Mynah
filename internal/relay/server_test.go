package relay

import (
	"context"
	"encoding/json"
	"testing"

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

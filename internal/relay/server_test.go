package relay

import "testing"

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

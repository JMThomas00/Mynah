// Package wire is this plugin's own hand-mirrored copy of the subset of
// Concord's plugin wire protocol it actually speaks. It cannot import
// github.com/concord-chat/concord/internal/protocol directly — Go's
// internal/ visibility rules only allow imports from within the same module
// tree, and this plugin (github.com/JMThomas00/mynah) and Concord
// are separate modules — so these types are hand-mirrored against
// d:\Concord\internal\protocol\messages.go's actual field tags, not
// guessed. Only what this plugin needs is included; see Tukan's own
// internal/wire package (d:\Tukan) for the precedent this follows.
package wire

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// OpCode mirrors protocol.OpCode's underlying representation exactly — a
// plain int, and these values must match Concord's numbering byte-for-byte
// since they're the actual wire values, not a private enum.
type OpCode int

const (
	OpIdentify       OpCode = 0
	OpSendMessage    OpCode = 3
	OpTypingStart    OpCode = 4
	OpDispatch       OpCode = 10
	OpHello          OpCode = 12
	OpReady          OpCode = 13
	OpInvalidSession OpCode = 14
)

// EventType mirrors protocol.EventType — the values Concord dispatches to a
// plugin's own connection, identified by Message.Type once Op == OpDispatch.
type EventType string

const (
	EventMessageCreate      EventType = "MESSAGE_CREATE"
	EventPluginConfigUpdate EventType = "PLUGIN_CONFIG_UPDATE"
	EventChannelCreate      EventType = "CHANNEL_CREATE"
	EventChannelUpdate      EventType = "CHANNEL_UPDATE"
)

// Message is the wire envelope — field names/tags/omitempty must match
// protocol.Message exactly, since both ends marshal/unmarshal the same JSON.
type Message struct {
	Op   OpCode          `json:"op"`
	Data json.RawMessage `json:"d,omitempty"`
	Seq  *int64          `json:"s,omitempty"`
	Type EventType       `json:"t,omitempty"`
}

// NewMessage mirrors protocol.NewMessage.
func NewMessage(op OpCode, data any) (*Message, error) {
	var raw json.RawMessage
	if data != nil {
		var err error
		raw, err = json.Marshal(data)
		if err != nil {
			return nil, fmt.Errorf("marshal payload: %w", err)
		}
	}
	return &Message{Op: op, Data: raw}, nil
}

// IdentifyPayload — this plugin only ever sends ClientType "plugin", so
// ConnectionProperties (present on Concord's own struct) is omitted
// entirely rather than mirrored unused.
type IdentifyPayload struct {
	Token      string `json:"token"`
	ClientType string `json:"client_type,omitempty"`
}

// SendMessagePayload posts a chat message — used both to send this plugin's
// own reply (Author is implied server-side from the connection's own
// service-account identity) and, mirrored the other direction, to read off
// what a human sent.
type SendMessagePayload struct {
	ChannelID uuid.UUID  `json:"channel_id"`
	Content   string     `json:"content"`
	ReplyToID *uuid.UUID `json:"reply_to_id,omitempty"`
	Nonce     string     `json:"nonce,omitempty"`
}

// TypingStartPayload triggers Concord's "X is typing..." indicator — used
// here to show "thinking..." while waiting on the LLM backend.
type TypingStartPayload struct {
	ChannelID uuid.UUID `json:"channel_id"`
}

// Message is a minimal mirror of models.Message — only the fields this
// plugin actually reads off a relayed MESSAGE_CREATE event.
type ChatMessage struct {
	ID        uuid.UUID `json:"id"`
	ChannelID uuid.UUID `json:"channel_id"`
	AuthorID  uuid.UUID `json:"author_id"`
	Content   string    `json:"content"`
}

// Author is a minimal mirror of models.User — enough to identify who sent a
// relayed message (in particular, to recognize and ignore this plugin's own
// posts, preventing an echo loop).
type Author struct {
	ID       uuid.UUID `json:"id"`
	Username string    `json:"username"`
}

// MessageCreatePayload mirrors protocol.MessageCreatePayload's embedding of
// *models.Message — Go's JSON encoding promotes an embedded struct
// pointer's own fields to the parent object, so embedding *ChatMessage here
// (not a named field) is required to unmarshal Concord's actual JSON shape.
type MessageCreatePayload struct {
	*ChatMessage
	Author *Author `json:"author"`
}

// PluginInfo mirrors protocol.PluginInfo — only the fields this plugin
// reads off its own PLUGIN_CONFIG_UPDATE push (its current
// server_config_field values).
type PluginInfo struct {
	ID           string            `json:"id"`
	ConfigValues map[string]string `json:"config_values,omitempty"`
}

// PluginConfigListPayload mirrors protocol.PluginConfigListPayload.
type PluginConfigListPayload struct {
	Plugins []PluginInfo `json:"plugins"`
}

// Channel is a minimal mirror of models.Channel — only the fields this
// plugin actually reads off a CHANNEL_CREATE/CHANNEL_UPDATE event
// (identifying it as one of this plugin's own "ai_passthrough" channels, and
// its current create_field values).
type Channel struct {
	ID           uuid.UUID         `json:"id"`
	Name         string            `json:"name"`
	PluginID     string            `json:"plugin_id,omitempty"`
	PluginConfig map[string]string `json:"plugin_config,omitempty"`
}

// ChannelCreatePayload mirrors protocol.ChannelCreatePayload's embedding of
// *models.Channel — Go's JSON encoding promotes an embedded struct
// pointer's own fields to the parent object, so embedding *Channel here
// (not a named field) is required to unmarshal Concord's actual JSON shape.
// Concord's ChannelCreatePayload additionally declares its own top-level
// PluginConfig field (distinct from Channel.PluginConfig, which Concord's
// Channel model didn't carry when this event was first wired up) — that
// field, not Channel.PluginConfig, is what actually carries the value here.
type ChannelCreatePayload struct {
	*Channel
	PluginConfig map[string]string `json:"plugin_config,omitempty"`
}

// ChannelUpdatePayload mirrors protocol.ChannelUpdatePayload, which embeds
// *models.Channel with no separate field of its own — models.Channel now
// carries PluginConfig directly, so it's promoted straight through here too.
type ChannelUpdatePayload struct {
	*Channel
}

// ReadyPayload is a minimal mirror of protocol.ReadyPayload — only User.ID,
// which this plugin needs to recognize (and ignore) its own posted replies
// when they come back around through mention-mode relay.
type ReadyPayload struct {
	User *Author `json:"user"`
}

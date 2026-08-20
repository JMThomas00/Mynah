// Package relay is the actual plugin logic: relay a chat message to a
// Responder (an LLM backend) and post the reply back, in either of two
// modes — a dedicated channel this plugin owns, or an @mention anywhere.
// Structurally mirrors Tukan's own internal/pluginserver.Server (one
// wire.Client, a handle(msg) dispatch on event type, a channels map keyed
// by Concord channel ID) — see d:\Tukan\internal\pluginserver\server.go.
package relay

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/JMThomas00/mynah/internal/ratelimit"
	"github.com/JMThomas00/mynah/internal/wire"
)

// Responder answers one message with reply text. internal/gateway's Client
// implements this (falling back to an echo reply of its own when it has no
// endpoint configured yet) — Server itself doesn't know or care which
// backend a Responder actually talks to.
type Responder interface {
	Complete(ctx context.Context, channelID, content string) (string, error)
}

// ConfigurableResponder is implemented by a Responder that wants to learn
// about this plugin's server_config_field values whenever they change —
// checked via type assertion in handleConfigUpdate, right alongside the
// mention-field handling below. Entirely optional; a Responder that
// doesn't need live config (e.g. a test fake) simply doesn't implement it.
type ConfigurableResponder interface {
	UpdateConfig(values map[string]string)
}

const (
	defaultBurst         = 3
	defaultRefillPerHour = 20
	// completeTimeout was 60s originally, matched to the echo bot's
	// near-instant reply and never revisited once a real backend existed,
	// then 3 minutes after 2026-08-13's live Alice test showed a real
	// agentic reply can genuinely take over a minute. 2026-08-19 root
	// cause: raising this number was never actually the fix for the
	// "no reply at all" failure mode — see handle()'s comment on why a
	// cold model load or a long, complex task can silently kill the
	// connection well before this deadline even matters. This is now a
	// generous outer ceiling (cold model load + a genuinely long agentic
	// task), not the thing standing between a slow reply and silence.
	completeTimeout = 20 * time.Minute

	// typingRefreshInterval must stay comfortably under Concord's own
	// server-side typing timeout (5s, internal/server/hub.go's
	// TypingTimeout) — a single OpTypingStart goes stale long before a real
	// backend call finishes, so this re-sends it periodically for as long
	// as Complete() is still running.
	typingRefreshInterval = 3 * time.Second

	// messageQueueSize is a generous backlog, not a real expected depth —
	// one worker processes messages in order (see New()'s worker
	// goroutine), so under normal use this rarely holds more than one or
	// two entries. It exists so a burst of messages arriving faster than
	// the backend can answer them enqueues instead of ReadLoop ever
	// blocking on a full channel.
	messageQueueSize = 64
)

// dedicatedChannel is what Server tracks per channel it owns.
type dedicatedChannel struct {
	limiter *ratelimit.Limiter
}

// Server is the top-level orchestrator: one wire.Client connection, routing
// relayed messages (both this plugin's own dedicated channels and
// @mention matches elsewhere) to the configured Responder and posting its
// reply back.
type Server struct {
	client     *wire.Client
	pluginID   string
	selfUserID uuid.UUID
	responder  Responder

	mu       sync.Mutex
	channels map[string]*dedicatedChannel // keyed by Concord channel ID (string form of its UUID)

	mentionMu      sync.RWMutex
	mentionEnabled bool
	mentionTrigger string
	mentionLimiter *ratelimit.Limiter

	// messageQueue decouples "received off the socket" from "handed to the
	// backend." One worker (see New()) drains it in order, so this backend
	// only ever sees one Complete() call in flight at a time — deliberate:
	// 2026-08-19's live test ran 3-4 real, slow (web-search-backed) replies
	// concurrently for the first time (this plugin's own dedicated channel
	// plus a mention, from two accounts, arriving within a couple minutes
	// of each other) and the process crashed shortly after — a single
	// local model backend genuinely can't be assumed to handle concurrent
	// generation requests safely. Queuing here keeps ReadLoop's own read
	// (and therefore Concord's ping/pong keepalive, handle()'s original
	// fix) free regardless of how long the backend takes, without ever
	// sending it more than one request at once.
	messageQueue chan json.RawMessage
}

// New constructs a Server. selfUserID is this connection's own
// service-account user ID (from wire.Client.Identify's return value) —
// needed to recognize and ignore this plugin's own posted replies.
func New(client *wire.Client, pluginID string, selfUserID uuid.UUID, responder Responder) *Server {
	s := &Server{
		client:         client,
		pluginID:       pluginID,
		selfUserID:     selfUserID,
		responder:      responder,
		channels:       make(map[string]*dedicatedChannel),
		mentionLimiter: ratelimit.New(defaultBurst, defaultRefillPerHour),
		messageQueue:   make(chan json.RawMessage, messageQueueSize),
	}
	go s.processMessageQueue()
	return s
}

// processMessageQueue is the single worker that ever calls
// handleMessageCreate — see messageQueue's doc comment on why this is
// deliberately not one-goroutine-per-message.
func (s *Server) processMessageQueue() {
	for data := range s.messageQueue {
		s.handleMessageCreateSafely(data)
	}
}

// handleMessageCreateSafely recovers a panic in handleMessageCreate rather
// than letting it take down the whole process. Concrete motivation, not
// defensive boilerplate: this plugin has never had panic recovery anywhere,
// and 2026-08-19's crash (see messageQueue's doc comment) demonstrated
// exactly what that costs — every in-flight reply silently lost, with the
// only symptom being permanent silence in Concord. A panic partway through
// handleMessageCreate skips straight past its own error-fallback logic (the
// "...(no response, try again)" reply), so this sends that same fallback
// itself from the recovered state — best-effort: if the payload doesn't
// even parse, there's no channel to reply into, and that's fine, since
// handleMessageCreate's own early unmarshal-failure return doesn't reply
// either.
func (s *Server) handleMessageCreateSafely(data json.RawMessage) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("relay: recovered panic in handleMessageCreate: %v", r)
			var payload wire.MessageCreatePayload
			if err := json.Unmarshal(data, &payload); err == nil && payload.ChatMessage != nil {
				if err := s.client.Send(wire.OpSendMessage, wire.SendMessagePayload{
					ChannelID: payload.ChannelID,
					Content:   "...(no response, try again)",
				}); err != nil {
					log.Printf("relay: failed to send post-panic fallback reply: %v", err)
				}
			}
		}
	}()
	s.handleMessageCreate(data)
}

// Run blocks, dispatching events until the connection closes or fails.
func (s *Server) Run() error {
	return s.client.ReadLoop(s.handle)
}

func (s *Server) handle(msg *wire.Message) {
	switch msg.Type {
	case wire.EventMessageCreate:
		// Must not run inline on this goroutine, and must not run on its
		// own ad-hoc goroutine either (see messageQueue's doc comment for
		// why the second part matters, found the hard way 2026-08-19).
		// ReadLoop only calls handle() between conn.ReadMessage() calls,
		// and it's conn.ReadMessage() — not any explicit heartbeat code —
		// that makes gorilla/websocket answer Concord's server-side pings
		// with pongs. Concord (internal/server/client.go) kills any
		// connection that goes 60s without one, and handleMessageCreate
		// can legitimately block for many minutes inside Complete() (a
		// cold model load, a long agentic task) — root-caused 2026-08-19
		// after Alice's connection kept dying mid-reply well inside the
		// (then 3-minute) completeTimeout: it wasn't completeTimeout
		// firing, it was this same connection's read loop going silent
		// long enough that Concord gave up on it first. Enqueueing here
		// is instant and never blocks ReadLoop; messageQueue's single
		// worker is what actually keeps the connection alive for as long
		// as a reply takes, without also sending the backend more than
		// one request at a time.
		select {
		case s.messageQueue <- msg.Data:
		default:
			log.Printf("relay: message queue full (%d), dropping a message", messageQueueSize)
		}
	case wire.EventPluginConfigUpdate:
		s.handleConfigUpdate(msg.Data)
	case wire.EventChannelCreate:
		s.handleChannelCreate(msg.Data)
	case wire.EventChannelUpdate:
		s.handleChannelUpdate(msg.Data)
	}
}

// handleChannelCreate is how this plugin's connection learns about a
// dedicated channel at all — mirrors Tukan's own handleChannelCreate. Same
// known gap Tukan's integration documents: a plugin only ever learns about
// a channel created while it's online (a targeted send at creation time,
// not something a plugin can query for pre-existing channels) — acceptable
// here for the same reason it was acceptable there.
func (s *Server) handleChannelCreate(data json.RawMessage) {
	var payload wire.ChannelCreatePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		log.Printf("relay: unmarshal channel create: %v", err)
		return
	}
	if payload.Channel == nil || payload.PluginID != s.pluginID {
		return
	}
	s.RegisterChannel(payload.Channel.ID, payload.PluginConfig)
}

// handleChannelUpdate re-registers a dedicated channel's rate-limit config
// when an admin edits its create_field values (burst / refill-per-hour)
// after creation — without this, an edit would silently have no effect
// until this plugin's process next restarted. Rebuilding via RegisterChannel
// resets the bucket to full on any update, not just a rate-limit change
// (Concord doesn't distinguish which fields actually changed) — an
// acceptable, simple tradeoff, not a correctness issue.
func (s *Server) handleChannelUpdate(data json.RawMessage) {
	var payload wire.ChannelUpdatePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		log.Printf("relay: unmarshal channel update: %v", err)
		return
	}
	if payload.Channel == nil || payload.PluginID != s.pluginID {
		return
	}
	s.RegisterChannel(payload.Channel.ID, payload.PluginConfig)
}

// handleConfigUpdate applies a live (or identify-time) push of this
// plugin's own server_config_field values — see Concord's
// identifyAsPlugin (pushes once right after Ready) and HandleSetPluginConfig
// (pushes again on every admin change), both targeted sends via the same
// EventPluginConfigUpdate this plugin's own connection receives.
func (s *Server) handleConfigUpdate(data json.RawMessage) {
	var payload wire.PluginConfigListPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		log.Printf("relay: unmarshal config update: %v", err)
		return
	}
	for _, info := range payload.Plugins {
		if info.ID != s.pluginID {
			continue
		}
		s.mentionMu.Lock()
		s.mentionEnabled = info.ConfigValues["mention_enabled"] == "true"
		s.mentionTrigger = info.ConfigValues["mention_trigger"]
		burst := parseIntOr(info.ConfigValues["mention_rate_limit_burst"], defaultBurst)
		refill := parseIntOr(info.ConfigValues["mention_rate_limit_refill_per_hour"], defaultRefillPerHour)
		s.mentionLimiter = ratelimit.New(burst, refill)
		s.mentionMu.Unlock()

		// Let the Responder itself pick out whatever it cares about
		// (gateway_endpoint/gateway_model today) — Server stays backend-
		// agnostic, never importing internal/gateway to do this.
		if cr, ok := s.responder.(ConfigurableResponder); ok {
			cr.UpdateConfig(info.ConfigValues)
		}
	}
}

func parseIntOr(s string, fallback int) int {
	if s == "" {
		return fallback
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

// handleMessageCreate is the core relay: figure out which mode this message
// arrived under, rate-limit, show typing, call the Responder, post the
// reply.
func (s *Server) handleMessageCreate(data json.RawMessage) {
	var payload wire.MessageCreatePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		log.Printf("relay: unmarshal message create: %v", err)
		return
	}
	if payload.ChatMessage == nil || payload.Author == nil {
		return
	}
	// Never respond to our own posted replies — the one thing standing
	// between this and an infinite bot-to-bot loop.
	if payload.Author.ID == s.selfUserID {
		return
	}

	channelID := payload.ChannelID
	content := payload.Content

	s.mu.Lock()
	dc, owned := s.channels[channelID.String()]
	s.mu.Unlock()

	var limiter *ratelimit.Limiter
	if owned {
		limiter = dc.limiter
	} else {
		// Not one of our own channels — Concord only ever relays a message
		// to this plugin for one of two reasons (see relayMessageToPlugins
		// on Concord's side), so if it's not owned-channel mode, it must be
		// a mention match, and Concord has already gated on mention_enabled
		// before ever sending it. Re-checking mentionEnabled here too is
		// belt-and-suspenders, not load-bearing — it costs nothing and
		// guards against a message that was already in flight the instant
		// an admin flips mention mode back off.
		s.mentionMu.RLock()
		enabled := s.mentionEnabled
		trigger := s.mentionTrigger
		limiter = s.mentionLimiter
		s.mentionMu.RUnlock()
		if !enabled {
			return
		}
		if trigger != "" {
			content = stripTrigger(content, trigger)
		}
	}

	// Silent drop on rate-limit rejection, not a "slow down" reply — a
	// reply would itself be more bot traffic, working against the point of
	// throttling in the first place.
	if limiter != nil && !limiter.Allow(payload.Author.ID) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), completeTimeout)
	go s.keepTyping(ctx, channelID)
	reply, err := s.responder.Complete(ctx, channelID.String(), content)
	cancel()
	if err != nil {
		log.Printf("relay: responder error for channel %s: %v", channelID, err)
		reply = "...(no response, try again)"
	}

	if err := s.client.Send(wire.OpSendMessage, wire.SendMessagePayload{ChannelID: channelID, Content: reply}); err != nil {
		log.Printf("relay: failed to send reply: %v", err)
	}
}

// keepTyping sends an immediate OpTypingStart, then re-sends it every
// typingRefreshInterval until ctx is done (Complete returns, or
// completeTimeout fires) — a real backend call can legitimately take up to
// completeTimeout, far longer than Concord's 5s typing-indicator expiry, so
// one send at the start isn't enough to keep it showing for the whole wait.
func (s *Server) keepTyping(ctx context.Context, channelID uuid.UUID) {
	_ = s.client.Send(wire.OpTypingStart, wire.TypingStartPayload{ChannelID: channelID})
	ticker := time.NewTicker(typingRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.client.Send(wire.OpTypingStart, wire.TypingStartPayload{ChannelID: channelID})
		}
	}
}

// stripTrigger removes the first @-mention of trigger from content
// (case-insensitive), trimming the leftover whitespace it leaves behind.
func stripTrigger(content, trigger string) string {
	lower := strings.ToLower(content)
	target := "@" + strings.ToLower(trigger)
	idx := strings.Index(lower, target)
	if idx < 0 {
		return content
	}
	stripped := content[:idx] + content[idx+len(target):]
	return strings.TrimSpace(stripped)
}

// RegisterChannel records a dedicated channel this plugin owns, with its
// own rate-limit configuration read off the channel's create_field values
// (rate_limit_burst / rate_limit_refill_per_hour) — called from
// EventChannelCreate handling in cmd/mynah-server.
func (s *Server) RegisterChannel(channelID uuid.UUID, pluginConfig map[string]string) {
	burst := parseIntOr(pluginConfig["rate_limit_burst"], defaultBurst)
	refill := parseIntOr(pluginConfig["rate_limit_refill_per_hour"], defaultRefillPerHour)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.channels[channelID.String()] = &dedicatedChannel{limiter: ratelimit.New(burst, refill)}
}

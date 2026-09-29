// Package relay is Mynah's plugin logic: answer chat messages with a
// Responder (an AI backend), streaming the reply into the channel as it's
// written. Messages arrive two ways: every message in a channel Mynah owns,
// or an @mention of its trigger word anywhere (when an admin enables it).
package relay

import (
	"context"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JMThomas00/Concord/sdk/plugin"
	"github.com/JMThomas00/Concord/sdk/wire"
	"github.com/google/uuid"

	"github.com/JMThomas00/mynah/internal/ratelimit"
)

// Responder answers content, calling onText with each piece of the reply
// as it's produced. internal/gateway's Client is the real one.
type Responder interface {
	Complete(ctx context.Context, content string, onText func(string)) error
}

// ConfigurableResponder is a Responder that wants the plugin's server
// settings (the gateway endpoint, model, key and persona).
type ConfigurableResponder interface {
	UpdateConfig(values map[string]string)
}

const (
	defaultBurst         = 3
	defaultRefillPerHour = 20

	// completeTimeout is an outer ceiling for one reply: a cold model load
	// plus a genuinely long agentic task can take many minutes.
	completeTimeout = 20 * time.Minute

	// typingRefreshInterval stays under Concord's 5s typing timeout; the
	// indicator is kept up until the reply starts appearing.
	typingRefreshInterval = 3 * time.Second

	// queueSize is a backlog for bursts; one worker answers in order.
	queueSize = 64

	// FailedReply is posted when the backend fails before writing anything.
	FailedReply = "...(no response, try again)"
	// cutOffNote ends a reply the backend stopped partway through.
	cutOffNote = "\n\n*…(the reply was cut off)*"
)

// Server answers messages. Handler callbacks only record work; one worker
// goroutine talks to the backend, so it only ever gets one request at a
// time (a single local model can't be assumed to handle concurrent
// generation), and the connection stays responsive however long a reply
// takes.
type Server struct {
	responder Responder

	mu             sync.Mutex
	channels       map[uuid.UUID]*ratelimit.Limiter // the channels Mynah owns
	mentionEnabled bool
	mentionTrigger string
	mentionLimiter *ratelimit.Limiter

	queue chan job
}

type job struct {
	c   *plugin.Conn
	msg wire.MessageCreatePayload
}

// New starts a Server's worker.
func New(responder Responder) *Server {
	s := &Server{
		responder:      responder,
		channels:       map[uuid.UUID]*ratelimit.Limiter{},
		mentionLimiter: ratelimit.New(defaultBurst, defaultRefillPerHour),
		queue:          make(chan job, queueSize),
	}
	go func() {
		for j := range s.queue {
			s.answerSafely(j)
		}
	}()
	return s
}

// Handler is what plugin.Run needs.
func (s *Server) Handler() plugin.Handler {
	return plugin.Handler{
		OnConfig:        s.onConfig,
		OnChannel:       s.onChannel,
		OnChannelDelete: s.onChannelDelete,
		OnMessage:       s.onMessage,
	}
}

func (s *Server) onConfig(_ *plugin.Conn, info wire.PluginInfo) {
	v := info.ConfigValues
	s.mu.Lock()
	s.mentionEnabled = v["mention_enabled"] == "true"
	s.mentionTrigger = strings.TrimPrefix(strings.TrimSpace(v["mention_trigger"]), "@")
	s.mentionLimiter = ratelimit.New(intOr(v["mention_rate_limit_burst"], defaultBurst), intOr(v["mention_rate_limit_refill_per_hour"], defaultRefillPerHour))
	s.mu.Unlock()
	if cr, ok := s.responder.(ConfigurableResponder); ok {
		cr.UpdateConfig(v)
	}
}

// onChannel registers (or, after an edit, re-registers) a channel Mynah
// owns with its rate limit. Concord sends each on connect too.
func (s *Server) onChannel(_ *plugin.Conn, ch wire.Channel) {
	limiter := ratelimit.New(intOr(ch.PluginConfig["rate_limit_burst"], defaultBurst), intOr(ch.PluginConfig["rate_limit_refill_per_hour"], defaultRefillPerHour))
	s.mu.Lock()
	s.channels[ch.ID] = limiter
	s.mu.Unlock()
}

func (s *Server) onChannelDelete(_ *plugin.Conn, e wire.ChannelDeletePayload) {
	s.mu.Lock()
	delete(s.channels, e.ChannelID)
	s.mu.Unlock()
}

func (s *Server) onMessage(c *plugin.Conn, m wire.MessageCreatePayload) {
	if m.ChatMessage == nil || m.Author == nil {
		return
	}
	if self := c.Self(); self != nil && m.AuthorID == self.ID {
		return // never answer ourselves
	}
	select {
	case s.queue <- job{c, m}:
	default:
		log.Printf("relay: queue full (%d), dropping a message", queueSize)
	}
}

// answerSafely keeps one bad reply from taking the process (and every
// queued reply) down with it.
func (s *Server) answerSafely(j job) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("relay: recovered panic answering a message: %v", r)
			_ = j.c.SendMessage(j.msg.ChannelID, FailedReply, nil)
		}
	}()
	s.answer(j)
}

// answer decides whether to reply, then streams the reply.
func (s *Server) answer(j job) {
	m, c := j.msg, j.c
	content := m.Content

	s.mu.Lock()
	limiter, owned := s.channels[m.ChannelID]
	if !owned {
		// Concord only relays other channels' messages for a mention of our
		// trigger, and only while mentions are enabled; re-check in case
		// an admin just turned them off.
		if !s.mentionEnabled {
			s.mu.Unlock()
			return
		}
		limiter = s.mentionLimiter
		if s.mentionTrigger != "" {
			content = stripTrigger(content, s.mentionTrigger)
		}
	}
	s.mu.Unlock()

	// Over the limit: stay quiet (a "slow down" reply is more bot traffic).
	if limiter != nil && !limiter.Allow(m.AuthorID) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), completeTimeout)
	defer cancel()
	typing, stopTyping := context.WithCancel(ctx)
	go keepTyping(typing, c, m.ChannelID)

	var replyTo *uuid.UUID
	if !owned {
		replyTo = &m.ID // in a busy channel, show what's being answered
	}
	stream := c.Stream(ctx, m.ChannelID, replyTo)
	var writeErr error
	err := s.responder.Complete(ctx, content, func(text string) {
		stopTyping()
		if writeErr == nil {
			writeErr = stream.Write(text)
		}
	})
	stopTyping()
	switch {
	case err != nil && strings.TrimSpace(stream.Text()) == "" && writeErr == nil:
		log.Printf("relay: no reply for channel %s: %v", m.ChannelID, err)
		_ = c.SendMessage(m.ChannelID, FailedReply, replyTo)
	case err != nil:
		log.Printf("relay: reply for channel %s cut off: %v", m.ChannelID, err)
		_ = stream.Write(cutOffNote)
	}
	if err := stream.Close(); err != nil {
		log.Printf("relay: couldn't finish the reply in %s: %v", m.ChannelID, err)
	}
	if writeErr != nil {
		log.Printf("relay: streaming the reply in %s failed: %v", m.ChannelID, writeErr)
	}
}

// keepTyping shows "typing…" until ctx ends (the reply starts appearing,
// or the request finishes), refreshing it before Concord's 5s expiry.
func keepTyping(ctx context.Context, c *plugin.Conn, channelID uuid.UUID) {
	_ = c.Typing(channelID, true)
	t := time.NewTicker(typingRefreshInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = c.Typing(channelID, false)
			return
		case <-t.C:
			_ = c.Typing(channelID, true)
		}
	}
}

// stripTrigger removes the first @-mention of trigger (any case) and the
// space it leaves.
func stripTrigger(content, trigger string) string {
	target := "@" + strings.ToLower(trigger)
	i := strings.Index(strings.ToLower(content), target)
	if i < 0 {
		return content
	}
	return strings.TrimSpace(content[:i] + content[i+len(target):])
}

func intOr(s string, fallback int) int {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n <= 0 {
		return fallback
	}
	return n
}

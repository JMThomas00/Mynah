package relay

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JMThomas00/Concord/sdk/plugin"
	"github.com/JMThomas00/Concord/sdk/plugintest"
	"github.com/JMThomas00/Concord/sdk/wire"
	"github.com/google/uuid"
)

// fakeResponder replies with pieces, or fails after them.
type fakeResponder struct {
	mu      sync.Mutex
	pieces  []string
	err     error
	asked   []string
	configs []map[string]string
}

func (f *fakeResponder) Complete(_ context.Context, content string, onText func(string)) error {
	f.mu.Lock()
	f.asked = append(f.asked, content)
	pieces, err := f.pieces, f.err
	f.mu.Unlock()
	for _, p := range pieces {
		onText(p)
	}
	return err
}

func (f *fakeResponder) UpdateConfig(v map[string]string) {
	f.mu.Lock()
	f.configs = append(f.configs, v)
	f.mu.Unlock()
}

func (f *fakeResponder) questions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.asked...)
}

// rig runs Mynah against a fake Concord with one owned channel.
func rig(t *testing.T, r *fakeResponder) (*plugintest.Server, uuid.UUID) {
	t.Helper()
	srv := plugintest.NewServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go plugin.Run(ctx, srv.Config(), New(r).Handler())
	srv.WaitReady()
	owned := uuid.New()
	srv.Channel(wire.Channel{ID: owned, Name: "ask-mynah", PluginConfig: map[string]string{"rate_limit_burst": "2"}})
	return srv, owned
}

// finalReply waits for the next message Mynah posts and returns it once
// finished.
func finalReply(t *testing.T, srv *plugintest.Server) plugintest.Posted {
	t.Helper()
	srv.NextChat()
	posted := srv.Posted()
	return srv.WaitStreamDone(posted[len(posted)-1].ID)
}

func TestAnswersInItsChannelByStreaming(t *testing.T) {
	r := &fakeResponder{pieces: []string{"Sure", ", here's code:\n```go\n", "fmt.Println(1)\n```"}}
	srv, owned := rig(t, r)
	srv.ChatMessage(owned, "alex", "how do I print?")
	got := finalReply(t, srv)
	if got.ChannelID != owned || got.Content != "Sure, here's code:\n```go\nfmt.Println(1)\n```" {
		t.Fatalf("reply %+v", got)
	}
	if got.ReplyToID != nil {
		t.Error("a reply in Mynah's own channel shouldn't quote the question")
	}
	if q := r.questions(); len(q) != 1 || q[0] != "how do I print?" {
		t.Fatalf("asked %q", q)
	}
}

func TestMentionsOnlyWhenEnabledAndTriggerIsStripped(t *testing.T) {
	r := &fakeResponder{pieces: []string{"hello!"}}
	srv, _ := rig(t, r)
	general := uuid.New()

	srv.ChatMessage(general, "alex", "@mynah are you there?")
	time.Sleep(200 * time.Millisecond)
	if len(r.questions()) != 0 {
		t.Fatal("answered a mention with mentions turned off")
	}

	srv.Settings(map[string]string{"mention_enabled": "true", "mention_trigger": "Mynah", "gateway_endpoint": "http://x"})
	time.Sleep(100 * time.Millisecond)
	srv.ChatMessage(general, "alex", "@mynah are you there?")
	got := finalReply(t, srv)
	if got.ChannelID != general || got.Content != "hello!" || got.ReplyToID == nil {
		t.Fatalf("mention reply %+v", got)
	}
	if q := r.questions(); len(q) != 1 || q[0] != "are you there?" {
		t.Fatalf("asked %q", q)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.configs) == 0 || r.configs[len(r.configs)-1]["gateway_endpoint"] != "http://x" {
		t.Fatal("the gateway settings weren't passed on")
	}
}

func TestRateLimitIsPerChannelSetting(t *testing.T) {
	r := &fakeResponder{pieces: []string{"ok"}}
	srv, owned := rig(t, r) // burst 2
	for i := 0; i < 3; i++ {
		srv.ChatMessage(owned, "alex", "again")
	}
	time.Sleep(500 * time.Millisecond)
	if n := len(r.questions()); n != 2 {
		t.Fatalf("answered %d of 3 messages with a burst of 2", n)
	}
}

func TestFailuresSayWhatHappened(t *testing.T) {
	r := &fakeResponder{err: errors.New("backend down")}
	srv, owned := rig(t, r)
	srv.ChatMessage(owned, "alex", "hello?")
	if got := finalReply(t, srv); got.Content != FailedReply {
		t.Fatalf("with nothing written: %q", got.Content)
	}

	r.mu.Lock()
	r.pieces, r.err = []string{"The answer is"}, errors.New("connection reset")
	r.mu.Unlock()
	srv.ChatMessage(owned, "sam", "and?")
	got := finalReply(t, srv)
	if !strings.HasPrefix(got.Content, "The answer is") || !strings.Contains(got.Content, "cut off") || got.Stream != wire.StreamDone {
		t.Fatalf("cut-off reply %+v", got)
	}
}

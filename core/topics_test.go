package core

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type tfContext struct{ chat, thread, root string }
type tfPlatform struct {
	stubPlatformEngine
	limit   int
	creates atomic.Int32
	fail    bool
}

func (p *tfPlatform) TopicSession(ctx any) (string, string, int) {
	r, _ := ctx.(tfContext)
	return r.chat, r.thread, p.limit
}
func (p *tfPlatform) TopicInfo(ctx any) TopicInfo {
	r, _ := ctx.(tfContext)
	return TopicInfo{ChatID: r.chat, ThreadID: r.thread, RootID: r.root}
}
func (p *tfPlatform) TopicTitle(context.Context, string) (string, error) { return "Fetched title", nil }
func (p *tfPlatform) TopicLatest(context.Context, string) (TopicLatest, error) {
	return TopicLatest{Sender: "executor", Time: "123"}, nil
}
func (p *tfPlatform) CreateTopic(context.Context, any, string, string, string, string) (TopicReceipt, error) {
	p.creates.Add(1)
	if p.fail {
		return TopicReceipt{RootID: "root", Status: "failed"}, fmt.Errorf("reply failed")
	}
	return TopicReceipt{RootID: "root", ThreadID: "thread-created", MessageID: "reply", Status: "sent"}, nil
}

type tfStart struct {
	s      *tfSession
	prompt string
	at     time.Time
}
type tfAgent struct {
	beforeStart func()
	stubAgent
	started chan tfStart
	count   atomic.Int32
}

func (a *tfAgent) StartSession(context.Context, string) (AgentSession, error) {
	if a.beforeStart != nil {
		a.beforeStart()
	}
	return &tfSession{id: fmt.Sprint(a.count.Add(1)), events: make(chan Event, 16), a: a}, nil
}

type tfSession struct {
	id     string
	a      *tfAgent
	events chan Event
	mu     sync.Mutex
	closed bool
}

func (s *tfSession) Send(text, id string, _ []ImageAttachment, _ []FileAttachment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("closed")
	}
	s.a.started <- tfStart{s, text, time.Now()}
	return nil
}
func (s *tfSession) RespondPermission(string, PermissionResult) error { return nil }
func (s *tfSession) Events() <-chan Event                             { return s.events }
func (s *tfSession) CurrentSessionID() string                         { return s.id }
func (s *tfSession) Alive() bool                                      { s.mu.Lock(); defer s.mu.Unlock(); return !s.closed }
func (s *tfSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.events)
	}
	return nil
}
func (s *tfSession) finish(text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.events <- Event{Type: EventResult, Content: text, Done: true}
	}
}
func tfEnv(t *testing.T, limit int) (*Engine, *tfPlatform, *tfAgent) {
	t.Helper()
	p := &tfPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}, limit: limit}
	a := &tfAgent{started: make(chan tfStart, 32)}
	e := NewEngine("test", a, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	e.dataDir = t.TempDir()
	t.Cleanup(func() { _ = e.Stop() })
	return e, p, a
}
func tfSend(e *Engine, p *tfPlatform, thread, text string) {
	key := "test:chat:commander"
	if thread != "" {
		key = "test:chat:thread:" + thread
	}
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: key, MessageID: text, Content: text, UserID: "commander", ReplyCtx: tfContext{"chat", thread, "root"}})
}
func tfNext(t *testing.T, a *tfAgent, contains string) tfStart {
	t.Helper()
	select {
	case s := <-a.started:
		if !strings.Contains(s.prompt, contains) {
			t.Fatalf("started %q, want %q", s.prompt, contains)
		}
		return s
	case <-time.After(4 * time.Second):
		t.Fatalf("no start for %s", contains)
		return tfStart{}
	}
}
func tfWait(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not reached")
}
func tfSaw(p *tfPlatform, text string) bool {
	for _, s := range p.getSent() {
		if strings.Contains(s, text) {
			return true
		}
	}
	return false
}
func TestTopicLimitFIFOAndSingleStop(t *testing.T) {
	e, p, a := tfEnv(t, 1)
	tfSend(e, p, "one", "ONE")
	one := tfNext(t, a, "ONE")
	tfSend(e, p, "two", "TWO")
	tfSend(e, p, "three", "THREE")
	tfWait(t, func() bool { return tfSaw(p, "0") && tfSaw(p, "1") })
	select {
	case s := <-a.started:
		t.Fatalf("over limit: %s", s.prompt)
	default:
	}
	one.s.finish("ONE-DONE")
	two := tfNext(t, a, "TWO")
	tfSend(e, p, "two", "/stop")
	three := tfNext(t, a, "THREE")
	tfWait(t, func() bool { return !two.s.Alive() })
	if !three.s.Alive() {
		t.Fatal("single stop killed other topic")
	}
	three.s.finish("THREE-DONE")
	tfWait(t, func() bool { return tfSaw(p, "THREE-DONE") })
}
func TestTopicStopAllClearsWaitingAndDoesNotResurrect(t *testing.T) {
	e, p, a := tfEnv(t, 1)
	tfSend(e, p, "one", "ONE")
	one := tfNext(t, a, "ONE")
	tfSend(e, p, "two", "NEVER")
	out := e.StopTopics("test", "chat")
	if out.Cleared != 1 || out.Stopped != 1 {
		t.Fatalf("stop=%+v", out)
	}
	tfWait(t, func() bool { return !one.s.Alive() })
	tfWait(t, func() bool {
		for _, s := range e.TopicSessions("test", "chat") {
			if s.State != "idle" {
				return false
			}
		}
		return true
	})
	select {
	case s := <-a.started:
		t.Fatalf("resurrected %s", s.prompt)
	default:
	}
	tfSend(e, p, "two", "AFTER-STOP")
	next := tfNext(t, a, "AFTER-STOP")
	next.s.finish("AFTER-DONE")
}
func TestTopicSameThreadPendingMessageHoldsSlot(t *testing.T) {
	e, p, a := tfEnv(t, 1)
	tfSend(e, p, "one", "FIRST")
	first := tfNext(t, a, "FIRST")
	tfSend(e, p, "one", "SECOND")
	tfSend(e, p, "two", "THIRD")
	first.s.finish("FIRST-DONE")
	second := tfNext(t, a, "SECOND")
	if first.s != second.s {
		t.Fatal("same topic lost agent")
	}
	second.s.finish("SECOND-DONE")
	third := tfNext(t, a, "THIRD")
	third.s.finish("THIRD-DONE")
}
func TestTopicCreatePersistenceIdempotencyAndOrigin(t *testing.T) {
	e, p, _ := tfEnv(t, 4)
	key := "test:chat:commander"
	e.sessions.GetOrCreateActive(key)
	e.interactiveStates[key] = &interactiveState{platform: p, replyCtx: tfContext{chat: "chat"}}
	req := TopicRequest{SessionKey: key, RequestID: strings.Repeat("a", 32), To: "executor", Title: "Title", Body: "spec"}
	r, err := e.CreateTopic(req)
	if err != nil || r.Status != "sent" {
		t.Fatalf("create=%+v %v", r, err)
	}
	if _, err = e.CreateTopic(req); err != nil || p.creates.Load() != 1 {
		t.Fatal("duplicate send", err)
	}
	req.Body = "changed"
	if _, err = e.CreateTopic(req); err == nil {
		t.Fatal("id reused with changed body")
	}
	req.SessionKey = "test:unknown"
	if _, err = e.CreateTopic(req); err == nil {
		t.Fatal("unknown key accepted")
	}
	e.topicRecordsLoaded = false
	records, err := e.topicRecordsForSession(key)
	if err != nil || len(records) != 1 {
		t.Fatalf("restart records: %v %v", records, err)
	}
	msg := &Message{SessionKey: key, Content: "DELIVERY", ReplyCtx: tfContext{"chat", "thread-created", "root"}}
	e.prefixTopicOrigin(p, msg)
	if msg.SessionKey != key || !strings.Contains(msg.ExtraContent, "[话题 Fetched title thread=thread-created]") {
		t.Fatalf("prefix=%+v", msg)
	}
	untouched := &Message{Content: "ordinary", ReplyCtx: tfContext{"chat", "unrecorded", "root"}}
	e.prefixTopicOrigin(p, untouched)
	if untouched.ExtraContent != "" {
		t.Fatal("old behavior changed")
	}
}
func TestTopicShutdownClearsMemoryQueue(t *testing.T) {
	e, p, a := tfEnv(t, 1)
	tfSend(e, p, "one", "ONE")
	tfNext(t, a, "ONE")
	tfSend(e, p, "two", "TWO")
	e.closeTopics()
	e.topicMu.Lock()
	defer e.topicMu.Unlock()
	g := e.topicGroups[topicGroupID("test", "chat")]
	if len(g.waiting) != 0 || len(g.order) != 0 || !g.stopping {
		t.Fatal("shutdown retained queues")
	}
}

type tfCommander struct{ *tfPlatform }

func (p *tfCommander) TopicSession(any) (string, string, int) { return "", "", 0 }

func TestTopicStopDuringStartupCannotSendLater(t *testing.T) {
	e, p, a := tfEnv(t, 1)
	entered, release := make(chan struct{}), make(chan struct{})
	a.beforeStart = func() { close(entered); <-release }
	tfSend(e, p, "one", "MUST-NOT-SEND")
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("no startup")
	}
	stopped := make(chan struct{})
	go func() { tfSend(e, p, "one", "/stop"); close(stopped) }()
	tfWait(t, func() bool {
		e.topicMu.Lock()
		defer e.topicMu.Unlock()
		return e.topicGroups[topicGroupID("test", "chat")].running["test:chat:thread:one"].cancelled.Load()
	})
	close(release)
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("stop stuck")
	}
	tfWait(t, func() bool {
		for _, s := range e.TopicSessions("test", "chat") {
			if s.State != "idle" {
				return false
			}
		}
		return true
	})
	select {
	case s := <-a.started:
		t.Fatalf("late send after stop: %s", s.prompt)
	default:
	}
}

type tfBlockingClose struct {
	tfSession
	release chan struct{}
}

func (s *tfBlockingClose) Close() error { <-s.release; return s.tfSession.Close() }
func TestTopicStopVerificationWaitsForCloseReceipt(t *testing.T) {
	e, p, _ := tfEnv(t, 1)
	key := "test:chat:thread:one"
	backend := &tfBlockingClose{tfSession: tfSession{events: make(chan Event)}, release: make(chan struct{})}
	e.interactiveStates[key] = &interactiveState{platform: p, agentSession: backend}
	e.topicGroups = map[string]*topicGroup{topicGroupID("test", "chat"): {known: map[string]struct{}{key: {}}, running: map[string]*topicRun{}, waiting: map[string][]topicEnvelope{}}}
	e.StopTopics("test", "chat")
	status := e.TopicSessions("test", "chat")
	if len(status) != 1 || status[0].State != "closing" || !status[0].ProcessAlive {
		t.Fatalf("premature stopped receipt: %+v", status)
	}
	close(backend.release)
	tfWait(t, func() bool { s := e.TopicSessions("test", "chat"); return s[0].State == "idle" && !s[0].ProcessAlive })
}

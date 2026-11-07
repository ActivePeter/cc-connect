package core

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type rotAgent struct {
	mu            sync.Mutex
	sessions      []*rotAgentSession
	candidateGate <-chan struct{}
	startErr      error
	gateFirst     bool
	resumeIDs     []string
}

func (a *rotAgent) Name() string { return "rotation-test" }
func (a *rotAgent) Stop() error  { return nil }
func (a *rotAgent) ListSessions(context.Context) ([]AgentSessionInfo, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := []AgentSessionInfo{}
	for _, s := range a.sessions {
		out = append(out, AgentSessionInfo{ID: s.id, Summary: s.id})
	}
	return out, nil
}
func (a *rotAgent) StartSession(_ context.Context, id string) (AgentSession, error) {
	a.mu.Lock()
	index := len(a.sessions)
	err := a.startErr
	if err != nil {
		a.mu.Unlock()
		return nil, err
	}
	s := &rotAgentSession{id: fmt.Sprintf("backend-%d", index), events: make(chan Event, 20), alive: true}
	a.sessions = append(a.sessions, s)
	a.resumeIDs = append(a.resumeIDs, id)
	gate := a.candidateGate
	a.mu.Unlock()
	if (index > 0 || a.gateFirst) && gate != nil {
		<-gate
	}
	return s, nil
}
func (a *rotAgent) count() int { a.mu.Lock(); defer a.mu.Unlock(); return len(a.sessions) }
func (a *rotAgent) at(i int) *rotAgentSession {
	a.mu.Lock()
	defer a.mu.Unlock()
	if i >= len(a.sessions) {
		return nil
	}
	return a.sessions[i]
}

type rotAgentSession struct {
	mu         sync.Mutex
	id         string
	events     chan Event
	inputs     []string
	alive      bool
	messageIDs []string
}

func (s *rotAgentSession) Send(text, id string, _ []ImageAttachment, _ []FileAttachment) error {
	s.mu.Lock()
	s.inputs = append(s.inputs, text)
	s.messageIDs = append(s.messageIDs, id)
	s.mu.Unlock()
	if s.id != "backend-0" {
		s.events <- Event{Type: EventResult, Done: true, Content: "HANDOFF_DONE " + text}
	}
	return nil
}
func (s *rotAgentSession) RespondPermission(string, PermissionResult) error { return nil }
func (s *rotAgentSession) Events() <-chan Event                             { return s.events }
func (s *rotAgentSession) CurrentSessionID() string                         { return s.id }
func (s *rotAgentSession) Alive() bool                                      { s.mu.Lock(); defer s.mu.Unlock(); return s.alive }
func (s *rotAgentSession) Close() error                                     { s.mu.Lock(); s.alive = false; s.mu.Unlock(); return nil }
func (s *rotAgentSession) sent() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.inputs...)
}

type rotPlatform struct {
	stubPlatformEngine
	entered     chan struct{}
	release     chan struct{}
	once        sync.Once
	sendErr     bool
	onNotice    func()
	failSegment bool
}

func (p *rotPlatform) Send(ctx context.Context, rc any, content string) error {
	if p.failSegment && strings.Contains(content, "UNDELIVERED_SEGMENT") {
		return fmt.Errorf("segment send failed")
	}
	if strings.Contains(content, "Session switched") && p.onNotice != nil {
		p.onNotice()
	}
	if strings.Contains(content, "A_DONE") && p.entered != nil {
		p.once.Do(func() { close(p.entered) })
		<-p.release
	}
	if p.sendErr && strings.Contains(content, "A_DONE") {
		return fmt.Errorf("platform unavailable")
	}
	return p.stubPlatformEngine.Send(ctx, rc, content)
}
func rotWait(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if fn() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("rotation timeout")
		case <-tick.C:
		}
	}
}

func rotStart(t *testing.T) (*Engine, *rotAgent, *rotPlatform, *Session) {
	t.Helper()
	a := &rotAgent{}
	p := &rotPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("rotation", a, []Platform{p}, "", LangEnglish)
	e.streamPreview.Enabled = false
	t.Cleanup(func() { e.cancel() })
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: "test:chat:user", Content: "ORIGINAL", ReplyCtx: "chat"})
	rotWait(t, func() bool { return a.at(0) != nil && len(a.at(0).sent()) == 1 })
	return e, a, p, e.sessions.LookupActive("test:chat:user")
}

func TestRotationPlatformFailureRetainsOld(t *testing.T) {
	e, a, p, old := rotStart(t)
	p.sendErr = true
	_, err := e.ScheduleRotation(RotateRequest{SessionKey: "test:chat:user", ExpectedSessionID: old.ID, RequestID: "r", Handoff: "handoff"})
	if err != nil {
		t.Fatal(err)
	}
	a.at(0).events <- Event{Type: EventResult, Done: true, Content: "A_DONE"}
	rotWait(t, func() bool { op, _ := e.sessions.Rotation("r"); return op.Status == "FAILED" })
	if e.sessions.LookupActive("test:chat:user") != old || a.count() != 1 {
		t.Fatal("failed reply switched session")
	}
}

func TestRotationLatestRegistrationDuringPreparation(t *testing.T) {
	e, a, _, old := rotStart(t)
	gate := make(chan struct{})
	a.mu.Lock()
	a.candidateGate = gate
	a.mu.Unlock()
	req := RotateRequest{SessionKey: "test:chat:user", ExpectedSessionID: old.ID, RequestID: "first", Handoff: "STALE"}
	if _, err := e.ScheduleRotation(req); err != nil {
		t.Fatal(err)
	}
	a.at(0).events <- Event{Type: EventResult, Done: true, Content: "A_DONE"}
	rotWait(t, func() bool { return a.count() == 2 })
	req.RequestID = "last"
	req.Handoff = "LATEST"
	if _, err := e.ScheduleRotation(req); err != nil {
		t.Fatal(err)
	}
	close(gate)
	rotWait(t, func() bool { return a.at(2) != nil && len(a.at(2).sent()) > 0 })
	if !strings.Contains(a.at(2).sent()[0], "LATEST") {
		t.Fatal("latest request lost")
	}
}

func TestRotationStopCancelsPending(t *testing.T) {
	e, _, p, old := rotStart(t)
	if _, err := e.ScheduleRotation(RotateRequest{SessionKey: "test:chat:user", ExpectedSessionID: old.ID, RequestID: "r", Handoff: "handoff"}); err != nil {
		t.Fatal(err)
	}
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: "test:chat:user", Content: "/stop", ReplyCtx: "chat"})
	rotWait(t, func() bool { op, _ := e.sessions.Rotation("r"); return op.Status == "CANCELLED" })
}

func TestRotationStopAfterCommitDoesNotResurrectHandoff(t *testing.T) {
	e, a, p, old := rotStart(t)
	done := make(chan struct{})
	p.onNotice = func() {
		e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: "test:chat:user", Content: "/stop", ReplyCtx: "chat"})
		close(done)
	}
	if _, err := e.ScheduleRotation(RotateRequest{SessionKey: "test:chat:user", ExpectedSessionID: old.ID, RequestID: "r-stop-after", Handoff: "DO_NOT_SEND"}); err != nil {
		t.Fatal(err)
	}
	a.at(0).events <- Event{Type: EventResult, Done: true, Content: "A_DONE"}
	<-done
	fresh := e.sessions.LookupActive("test:chat:user")
	rotWait(t, func() bool { return !fresh.Busy() })
	if a.count() != 2 || len(a.at(1).sent()) != 0 {
		t.Fatal("stop resurrected handoff")
	}
	if fresh.FirstInput().State != "pending" {
		t.Fatal("unsent handoff was consumed")
	}
}
func TestRotationCancelDuringPrepareKeepsCancelledReceipt(t *testing.T) {
	e, a, p, old := rotStart(t)
	gate := make(chan struct{})
	a.mu.Lock()
	a.candidateGate = gate
	a.mu.Unlock()
	if _, err := e.ScheduleRotation(RotateRequest{SessionKey: "test:chat:user", ExpectedSessionID: old.ID, RequestID: "r-cancel", Handoff: "DO_NOT_SEND"}); err != nil {
		t.Fatal(err)
	}
	a.at(0).events <- Event{Type: EventResult, Done: true, Content: "A_DONE"}
	rotWait(t, func() bool { return a.count() == 2 })
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: "test:chat:user", Content: "/stop", ReplyCtx: "chat"})
	close(gate)
	rotWait(t, func() bool { return !old.Busy() })
	op, _ := e.sessions.Rotation("r-cancel")
	if op.Status != "CANCELLED" || e.sessions.LookupActive("test:chat:user") != old || len(a.at(1).sent()) != 0 {
		t.Fatal(op)
	}
}

func TestRotationUnsentEarlierSegmentPreventsCutover(t *testing.T) {
	e, a, p, old := rotStart(t)
	p.failSegment = true
	if _, err := e.ScheduleRotation(RotateRequest{SessionKey: "test:chat:user", ExpectedSessionID: old.ID, RequestID: "segment", Handoff: "handoff"}); err != nil {
		t.Fatal(err)
	}
	a.at(0).events <- Event{Type: EventText, Content: "UNDELIVERED_SEGMENT"}
	a.at(0).events <- Event{Type: EventToolUse, ToolName: "test-tool"}
	a.at(0).events <- Event{Type: EventText, Content: "A_DONE"}
	a.at(0).events <- Event{Type: EventResult, Done: true, Content: "A_DONE"}
	rotWait(t, func() bool { op, _ := e.sessions.Rotation("segment"); return op.Status == "FAILED" })
	if e.sessions.LookupActive("test:chat:user") != old || a.count() != 1 {
		t.Fatal("cut over despite failed reply fragment")
	}
}
func TestRotationNoReplyIsAnIntentionalDeliveryBoundary(t *testing.T) {
	e, a, p, old := rotStart(t)
	if _, err := e.ScheduleRotation(RotateRequest{SessionKey: "test:chat:user", ExpectedSessionID: old.ID, RequestID: "silent", Handoff: "handoff"}); err != nil {
		t.Fatal(err)
	}
	a.at(0).events <- Event{Type: EventResult, Done: true, Content: "NO_REPLY"}
	rotWait(t, func() bool { return a.at(1) != nil && len(a.at(1).sent()) > 0 })
	if strings.Contains(strings.Join(p.getSent(), "\n"), "NO_REPLY") {
		t.Fatal("silent marker leaked")
	}
}
func TestRotationFinalizingFailureCannotRemainScheduled(t *testing.T) {
	e, a, _, old := rotStart(t)
	a.at(0).events <- Event{Type: EventResult, Done: true, Content: "A_DONE"}
	rotWait(t, func() bool { return !old.Busy() })
	e.rotationMu.Lock()
	e.rotationWorkers = map[string]bool{"test:chat:user": true}
	e.rotationMu.Unlock()
	if _, err := e.ScheduleRotation(RotateRequest{SessionKey: "test:chat:user", ExpectedSessionID: old.ID, RequestID: "overlap", Handoff: "handoff"}); err == nil {
		t.Fatal("missing explicit failure")
	}
	op, _ := e.sessions.Rotation("overlap")
	if op.Status != "FAILED" || e.sessions.LookupActive("test:chat:user") != old {
		t.Fatal(op)
	}
}

type rotCardPlatform struct {
	rotPlatform
	cardCount    int
	finalEntered chan struct{}
	finalRelease chan struct{}
}
type rotCard struct {
	platform *rotCardPlatform
	index    int
}

func (p *rotCardPlatform) CreateStreamingCard(context.Context, any) (StreamingCard, error) {
	p.mu.Lock()
	index := p.cardCount
	p.cardCount++
	p.mu.Unlock()
	return &rotCard{platform: p, index: index}, nil
}
func (c *rotCard) Update(context.Context, string) error { return nil }
func (c *rotCard) Failed() bool                         { return false }
func (c *rotCard) Finalize(ctx context.Context, text string) error {
	if c.index == 0 {
		close(c.platform.finalEntered)
		<-c.platform.finalRelease
		return fmt.Errorf("card finalization refused")
	}
	return c.platform.stubPlatformEngine.Send(ctx, "chat", text)
}
func TestRotationStreamingCardWaitsForFinalizeAndFallback(t *testing.T) {
	for _, fallbackFails := range []bool{false, true} {
		t.Run(fmt.Sprint(fallbackFails), func(t *testing.T) {
			a := &rotAgent{}
			p := &rotCardPlatform{rotPlatform: rotPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}, sendErr: fallbackFails}, finalEntered: make(chan struct{}), finalRelease: make(chan struct{})}
			e := NewEngine("rotation", a, []Platform{p}, "", LangEnglish)
			e.streamPreview.Enabled = false
			defer e.cancel()
			key := "test:chat:user"
			e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: key, Content: "ORIGINAL", ReplyCtx: "chat"})
			rotWait(t, func() bool { return a.at(0) != nil && len(a.at(0).sent()) == 1 })
			old := e.sessions.LookupActive(key)
			if _, err := e.ScheduleRotation(RotateRequest{SessionKey: key, ExpectedSessionID: old.ID, RequestID: "card", Handoff: "handoff"}); err != nil {
				t.Fatal(err)
			}
			a.at(0).events <- Event{Type: EventResult, Done: true, Content: "A_DONE"}
			<-p.finalEntered
			if a.count() != 1 || e.sessions.LookupActive(key) != old {
				t.Fatal("cut over before card finalization")
			}
			close(p.finalRelease)
			wanted := "SWITCHED"
			if fallbackFails {
				wanted = "FAILED"
			}
			rotWait(t, func() bool { op, _ := e.sessions.Rotation("card"); return op.Status == wanted })
			if fallbackFails && e.sessions.LookupActive(key) != old {
				t.Fatal("failed fallback retired source")
			}
		})
	}
}

func TestRotationLatestIdleRegistrationIsNotStranded(t *testing.T) {
	e, a, _, old := rotStart(t)
	a.at(0).events <- Event{Type: EventResult, Done: true, Content: "A_DONE"}
	rotWait(t, func() bool { return !old.Busy() })
	gate := make(chan struct{})
	a.mu.Lock()
	a.candidateGate = gate
	a.mu.Unlock()
	first := RotateRequest{SessionKey: "test:chat:user", ExpectedSessionID: old.ID, RequestID: "idle-first", Handoff: "STALE"}
	returned := make(chan RotationOperation, 1)
	errs := make(chan error, 1)
	go func() { op, err := e.ScheduleRotation(first); returned <- op; errs <- err }()
	rotWait(t, func() bool { return a.count() == 2 })
	last := first
	last.RequestID = "idle-last"
	last.Handoff = "LATEST"
	if op, err := e.ScheduleRotation(last); err != nil || op.Status != "SCHEDULED" {
		t.Fatal(op, err)
	}
	close(gate)
	op := <-returned
	if err := <-errs; err != nil || op.Status != "SUPERSEDED" {
		t.Fatal(op, err)
	}
	rotWait(t, func() bool { return a.at(2) != nil && len(a.at(2).sent()) > 0 })
	if !strings.Contains(a.at(2).sent()[0], "LATEST") {
		t.Fatal("idle replacement lost")
	}
}

type rotColdPlatform struct{ rotPlatform }

func (p *rotColdPlatform) ReconstructReplyCtx(string) (any, error) { return "cold-chat", nil }
func TestRotationColdSessionPreservesMessagesQueuedDuringPrepare(t *testing.T) {
	gate := make(chan struct{})
	a := &rotAgent{candidateGate: gate, gateFirst: true}
	p := &rotColdPlatform{rotPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}}
	e := NewEngine("rotation", a, []Platform{p}, "", LangEnglish)
	e.streamPreview.Enabled = false
	defer e.cancel()
	key := "test:chat:user"
	old := e.sessions.GetOrCreateActive(key)
	old.SetAgentSessionID("cold-old-native", a.Name())
	returned := make(chan error, 1)
	go func() {
		_, err := e.ScheduleRotation(RotateRequest{SessionKey: key, ExpectedSessionID: old.ID, RequestID: "cold", Handoff: "COLD_HANDOFF"})
		returned <- err
	}()
	rotWait(t, func() bool { return a.count() == 1 })
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: key, Content: "KEEP_COLD_QUEUE", ReplyCtx: "chat"})
	close(gate)
	if err := <-returned; err != nil {
		t.Fatal(err)
	}
	rotWait(t, func() bool { return len(a.at(0).sent()) >= 1 })
	if a.at(0).sent()[0] != "COLD_HANDOFF" {
		t.Fatal("handoff duplicated or overtaken", a.at(0).sent())
	}
	a.at(0).events <- Event{Type: EventResult, Done: true, Content: "handoff read"}
	rotWait(t, func() bool { return len(a.at(0).sent()) >= 2 })
	if !strings.Contains(a.at(0).sent()[1], "KEEP_COLD_QUEUE") {
		t.Fatal("cold queue lost")
	}
}
func TestRotationCommitFailureProcessesQueueOnSource(t *testing.T) {
	e, a, p, old := rotStart(t)
	gate := make(chan struct{})
	a.mu.Lock()
	a.candidateGate = gate
	a.mu.Unlock()
	if _, err := e.ScheduleRotation(RotateRequest{SessionKey: "test:chat:user", ExpectedSessionID: old.ID, RequestID: "fail-write", Handoff: "FAILED_HANDOFF"}); err != nil {
		t.Fatal(err)
	}
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: "test:chat:user", Content: "KEEP_ORIGINAL_QUEUE", ReplyCtx: "chat"})
	a.at(0).events <- Event{Type: EventResult, Done: true, Content: "A_DONE"}
	rotWait(t, func() bool { return a.count() == 2 })
	bad := t.TempDir()
	e.sessions.mu.Lock()
	e.sessions.storePath = bad
	e.sessions.mu.Unlock()
	close(gate)
	rotWait(t, func() bool { return a.at(2) != nil && len(a.at(2).sent()) > 0 })
	if e.sessions.LookupActive("test:chat:user") != old || e.sessions.IsRetiredAgentID("backend-0") {
		t.Fatal("failed commit retired source")
	}
	a.mu.Lock()
	resumed := a.resumeIDs[2]
	a.mu.Unlock()
	if resumed != "backend-0" || !strings.Contains(a.at(2).sent()[0], "KEEP_ORIGINAL_QUEUE") || strings.Contains(a.at(2).sent()[0], "FAILED_HANDOFF") {
		t.Fatal("queue not resumed on original context")
	}
	op, _ := e.sessions.Rotation("fail-write")
	if op.Status != "FAILED" {
		t.Fatal("failed receipt left scheduled", op)
	}
}

func TestRotationScopeAndLifecycleRejectStaleRequests(t *testing.T) {
	e, _, _, old := rotStart(t)
	req := RotateRequest{SessionKey: "test:chat:user", ExpectedSessionID: old.ID, ExpectedScope: "other-workspace", RequestID: "scope", Handoff: "handoff"}
	if _, err := e.ScheduleRotation(req); err != ErrRotationStale {
		t.Fatal(err)
	}
	req.ExpectedScope = e.sessions.RotationScope()
	done := e.beginRotationLifecycle(req.SessionKey)
	if _, err := e.ScheduleRotation(req); err != ErrRotationStale {
		t.Fatal(err)
	}
	done()
	if _, err := e.sessions.Rotation(req.RequestID); err != ErrRotationNotFound {
		t.Fatal("invalid request was registered", err)
	}
}

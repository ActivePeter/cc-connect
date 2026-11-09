package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type goalTestSession struct {
	*queuingAgentSession
	mu           sync.Mutex
	info         GoalInfo
	sets         int
	clears       int
	resumeNotice string
	running      bool
	setStarted   chan struct{}
	allowSet     chan struct{}
}

func (s *goalTestSession) Alive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queuingAgentSession.Alive()
}
func (s *goalTestSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queuingAgentSession.Close()
}

func (s *goalTestSession) GoalResumeNotice() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resumeNotice
}
func (s *goalTestSession) AckGoalResumeNotice(n string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.resumeNotice == n {
		s.resumeNotice = ""
	}
}

func (s *goalTestSession) GoalTurnRunning() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.running }

func (s *goalTestSession) SetGoal(o string) (GoalInfo, error) {
	if s.setStarted != nil {
		close(s.setStarted)
		<-s.allowSet
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sets++
	s.info = GoalInfo{Objective: o, Status: "active"}
	return s.info, nil
}

func (s *goalTestSession) GetGoal() GoalInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.info
}

func (s *goalTestSession) ClearGoal() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clears++
	s.info = GoalInfo{}
	return nil
}

func (s *goalTestSession) state() (GoalInfo, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.info, s.clears, s.sets
}

type goalAwareTestAgent struct {
	*stubAgent
	session  *goalTestSession
	started  string
	startErr error
}

func (a *goalAwareTestAgent) SupportsGoalMode() bool { return true }
func (a *goalAwareTestAgent) StartSession(_ context.Context, id string) (AgentSession, error) {
	a.started = id
	if a.startErr != nil {
		return nil, a.startErr
	}
	return a.session, nil
}

// newGoalEngine wires one live goal session (agent session id "goal-live").
func newGoalEngine(t *testing.T) (*Engine, *stubPlatformEngine, *goalTestSession, string) {
	t.Helper()
	p := &stubPlatformEngine{n: "test"}
	as := &goalTestSession{queuingAgentSession: newQueuingSession("goal-live")}
	e := NewEngine("test", &goalAwareTestAgent{stubAgent: &stubAgent{}, session: as}, []Platform{p}, t.TempDir()+"/s.json", LangEnglish)
	key := "test:room:user"
	t.Cleanup(func() {
		e.cancel()
		waitFor(t, func() bool { return !e.sessions.GetOrCreateActive(key).Busy() }, 2*time.Second)
		_ = e.Stop()
	})
	e.sessions.GetOrCreateActive(key).SetAgentSessionID("goal-live", "test")
	e.interactiveMu.Lock()
	e.interactiveStates[key] = &interactiveState{agentSession: as, platform: p, replyCtx: "ctx"}
	e.interactiveMu.Unlock()
	return e, p, as, key
}

func sendText(e *Engine, p Platform, key, text string) {
	e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", UserID: "u", UserName: "U", MessageID: "m" + text, Content: text, ReplyCtx: "r"})
}

func TestCmdGoal_SetStatusClear(t *testing.T) {
	e, p, as, key := newGoalEngine(t)
	sendText(e, p, key, "/goal make tests pass")
	if _, _, sets := as.state(); sets != 1 {
		t.Fatalf("sets = %d", sets)
	}
	if got := p.getSent(); len(got) != 1 || !strings.Contains(got[0], "make tests pass") {
		t.Fatalf("set reply = %#v", got)
	}
	p.clearSent()
	sendText(e, p, key, "/goal")
	if got := p.getSent(); len(got) != 1 || !strings.Contains(got[0], "active") {
		t.Fatalf("status reply = %#v", got)
	}
	p.clearSent()
	sendText(e, p, key, "/goal clear")
	if info, clears, _ := as.state(); clears != 1 || info.Status != "" {
		t.Fatalf("clears = %d info = %#v", clears, info)
	}
	if got := p.getSent(); len(got) != 1 || !strings.Contains(got[0], "goal 已清除") {
		t.Fatalf("clear reply = %#v", got)
	}
}

// Agents with their own /goal (Claude Code) get the text verbatim.
func TestCmdGoal_ForwardedWhenAgentHasOwnGoal(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	as := newQueuingSession("plain")
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	defer e.Stop()
	key := "test:room:user"
	e.sessions.GetOrCreateActive(key).SetAgentSessionID("plain", "test")
	e.interactiveMu.Lock()
	e.interactiveStates[key] = &interactiveState{agentSession: as, platform: p, replyCtx: "ctx"}
	e.interactiveMu.Unlock()

	sendText(e, p, key, "/goal keep the build green")
	waitFor(t, func() bool { return queuedSendCount(as) == 1 }, 2*time.Second)
	as.sendMu.Lock()
	got := as.sendCalls[0]
	as.sendMu.Unlock()
	if !strings.Contains(got, "/goal keep the build green") || len(p.getSent()) != 0 {
		t.Fatalf("forwarded = %q, replies = %#v", got, p.getSent())
	}
	as.events <- Event{Type: EventResult, Done: true}
	waitFor(t, func() bool { return !e.sessions.GetOrCreateActive(key).Busy() }, 2*time.Second)
}

func TestGoalActiveBlocksMessagesUntilClear(t *testing.T) {
	e, p, as, key := newGoalEngine(t)
	sendText(e, p, key, "/goal finish the migration")
	p.clearSent()
	sendText(e, p, key, "rename the config key")
	if got := p.getSent(); len(got) != 1 || !strings.Contains(got[0], "goal 进行中") {
		t.Fatalf("reply = %#v", got)
	}
	if queuedSendCount(as.queuingAgentSession) != 0 {
		t.Fatal("message reached the agent while a goal was active")
	}
	sendText(e, p, key, "/goal clear")
	as.events <- Event{Type: EventResult, Done: true}
	waitFor(t, func() bool { return !e.sessions.GetOrCreateActive(key).Busy() }, 2*time.Second)
	sendText(e, p, key, "rename the config key")
	waitFor(t, func() bool { return queuedSendCount(as.queuingAgentSession) == 1 }, 2*time.Second)
}

// Codex emits each round as EventText(s) plus an empty EventResult; round one
// is the foreground turn, later rounds come via the unsolicited reader. Each
// round's text must reach the platform exactly once.
func TestGoalRoundsSendEachTextOnce(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	defer e.Stop()
	key := "test:goal-rounds"
	session := e.sessions.GetOrCreateActive(key)
	as := newControllableSession("goal-rounds")
	state := &interactiveState{agentSession: as, platform: p, replyCtx: "ctx"}
	e.interactiveStates[key] = state

	as.events <- Event{Type: EventText, Content: "DONE"}
	as.events <- Event{Type: EventResult, Done: true}
	e.processInteractiveEvents(state, session, e.sessions, key, "m1", time.Now(), nil, nil, state.replyCtx)
	if got := p.getSent(); len(got) != 1 || got[0] != "DONE" {
		t.Fatalf("round 1 = %#v", got)
	}

	e.startUnsolicitedReader(state, session, e.sessions, key, "")
	defer e.stopUnsolicitedReader(state)
	// A codex check turn can carry two assistant messages; they must land in
	// one platform message, not two.
	as.events <- Event{Type: EventGoal, Content: "goal 已达成：ship it"}
	as.events <- Event{Type: EventText, Content: "DONE"}
	as.events <- Event{Type: EventText, Content: "DONE"}
	as.events <- Event{Type: EventResult, Done: true}
	waitFor(t, func() bool { return len(p.getSent()) == 3 }, 2*time.Second)
	time.Sleep(100 * time.Millisecond)
	got := p.getSent()
	if len(got) != 3 || got[1] != "goal 已达成：ship it" || got[2] != "DONEDONE" {
		t.Fatalf("round 2 = %#v", got)
	}
}

// /stop tears the session down without touching the goal; the goal is cleared
// on the next resume (see the codex resume test).
func TestStopLeavesGoalAlone(t *testing.T) {
	e, p, as, key := newGoalEngine(t)
	sendText(e, p, key, "/goal keep going")
	sendText(e, p, key, "/stop")
	e.interactiveMu.Lock()
	_, live := e.interactiveStates[key]
	e.interactiveMu.Unlock()
	if live {
		t.Fatal("state survived /stop")
	}
	if info, clears, _ := as.state(); clears != 0 || info.Status != "active" {
		t.Fatalf("clears = %d info = %#v", clears, info)
	}
}

func queuedSendCount(as *queuingAgentSession) int {
	as.sendMu.Lock()
	defer as.sendMu.Unlock()
	return len(as.sendCalls)
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func TestCmdGoal_QueuedUntilTurnEnds(t *testing.T) {
	e, p, as, key := newGoalEngine(t)
	sendText(e, p, key, "working")
	waitFor(t, func() bool { return queuedSendCount(as.queuingAgentSession) == 1 }, 2*time.Second)
	sendText(e, p, key, "/goal make tests pass")
	if _, _, sets := as.state(); sets != 0 {
		t.Fatalf("sets = %d while running", sets)
	}
	as.events <- Event{Type: EventResult, Done: true}
	waitFor(t, func() bool { _, _, n := as.state(); return n == 1 }, 2*time.Second)
}

// Regression: /goal uses the same new/resume path as ordinary messages.
func TestGoalColdStartAndStatus(t *testing.T) {
	for _, saved := range []string{"", "goal-live"} {
		t.Run("saved="+saved, func(t *testing.T) {
			e, p, as, key := newGoalEngine(t)
			delete(e.interactiveStates, key)
			e.sessions.GetOrCreateActive(key).SetAgentSessionID(saved, "test")
			sendText(e, p, key, "/goal make tests pass")
			if _, _, n := as.state(); n != 1 {
				t.Fatalf("sets = %d", n)
			}
			if got := e.agent.(*goalAwareTestAgent).started; got != saved {
				t.Fatalf("started %q, want %q", got, saved)
			}
		})
	}
	e, p, _, key := newGoalEngine(t)
	delete(e.interactiveStates, key)
	e.sessions.GetOrCreateActive(key).SetAgentSessionID("", "test")
	sendText(e, p, key, "/goal")
	sendText(e, p, key, "/goal clear")
	if got := p.getSent(); len(got) != 2 || !strings.Contains(got[0], "没有 goal") || !strings.Contains(got[1], "没有 goal") {
		t.Fatal(got)
	}
}

func TestGoalModeAlways(t *testing.T) {
	e, p, as, key := newGoalEngine(t)
	e.SetGoalModeAlways(true)
	sendText(e, p, key, "finish the task")
	if info, _, n := as.state(); n != 1 || info.Objective != "finish the task" {
		t.Fatalf("goal = %#v, sets = %d", info, n)
	}
	if queuedSendCount(as.queuingAgentSession) != 0 {
		t.Fatal("ordinary Send called")
	}
}

func TestGoalResumeNoticeBeforeSetOrStatus(t *testing.T) {
	for _, command := range []string{"/goal", "/goal clear", "/goal new", "new"} {
		t.Run(command, func(t *testing.T) {
			e, p, as, key := newGoalEngine(t)
			delete(e.interactiveStates, key)
			as.resumeNotice = "old"
			if command == "new" {
				e.SetGoalModeAlways(true)
			}
			sendText(e, p, key, command)
			got := p.getSent()
			want := 2
			if command == "/goal" || command == "/goal clear" {
				want = 1
			}
			if len(got) != want || !strings.Contains(got[0], "old") {
				t.Fatalf("replies = %v", got)
			}
			if want == 2 && !strings.Contains(got[1], "new") {
				t.Fatalf("replies = %v", got)
			}
		})
	}
}

func TestGoalActiveRejectsNewTarget(t *testing.T) {
	e, p, as, key := newGoalEngine(t)
	as.mu.Lock()
	as.info = GoalInfo{Objective: "old", Status: "active"}
	as.running = true
	as.mu.Unlock()
	sendText(e, p, key, "/goal new")
	if _, _, sets := as.state(); sets != 0 {
		t.Fatalf("sets = %d", sets)
	}
	if got := p.getSent(); len(got) != 1 || !strings.Contains(got[0], "/goal clear") {
		t.Fatalf("reply = %v", got)
	}
}

func TestQueuedGoalRechecksActive(t *testing.T) {
	e, p, as, key := newGoalEngine(t)
	sendText(e, p, key, "working")
	waitFor(t, func() bool { return queuedSendCount(as.queuingAgentSession) == 1 }, 2*time.Second)
	sendText(e, p, key, "/goal new")
	as.mu.Lock()
	as.info = GoalInfo{Objective: "other", Status: "active"}
	as.mu.Unlock()
	as.events <- Event{Type: EventResult, Done: true}
	waitFor(t, func() bool { return len(p.getSent()) >= 2 }, 2*time.Second)
	if _, _, sets := as.state(); sets != 0 {
		t.Fatalf("sets = %d", sets)
	}
	if got := p.getSent(); !strings.Contains(got[len(got)-1], "重发") {
		t.Fatal(got)
	}
}

func TestGoalStatusResumeFailurePreservesThread(t *testing.T) {
	e, p, _, key := newGoalEngine(t)
	delete(e.interactiveStates, key)
	e.agent.(*goalAwareTestAgent).startErr = errors.New("offline")
	sendText(e, p, key, "/goal")
	if id := e.sessions.GetOrCreateActive(key).GetAgentSessionID(); id != "goal-live" {
		t.Fatal(id)
	}
	if got := p.getSent(); len(got) != 1 || strings.Contains(got[0], "没有 goal") {
		t.Fatal(got)
	}
}

func TestGoalResumePausedStatusAndClear(t *testing.T) {
	for _, command := range []string{"/goal", "/goal clear"} {
		e, p, as, key := newGoalEngine(t)
		delete(e.interactiveStates, key)
		as.info = GoalInfo{Objective: "old", Status: "paused"}
		sendText(e, p, key, command)
		if command == "/goal" && !strings.Contains(p.getSent()[0], "old") {
			t.Fatal(p.getSent())
		}
		if command == "/goal clear" {
			_, clears, _ := as.state()
			if clears != 1 {
				t.Fatal(clears)
			}
		}
	}
}

func TestGoalClearCancelsQueuedTarget(t *testing.T) {
	e, p, as, key := newGoalEngine(t)
	sendText(e, p, key, "working")
	waitFor(t, func() bool { return queuedSendCount(as.queuingAgentSession) == 1 }, 2*time.Second)
	sendText(e, p, key, "/goal queued")
	sendText(e, p, key, "/goal clear")
	as.events <- Event{Type: EventResult, Done: true}
	waitFor(t, func() bool { return !e.sessions.GetOrCreateActive(key).Busy() }, 2*time.Second)
	if _, _, sets := as.state(); sets != 0 {
		t.Fatalf("cleared queued goal was set %d times", sets)
	}
	if got := strings.Join(p.getSent(), "|"); !strings.Contains(got, "取消") {
		t.Fatal(got)
	}
}

func TestGoalClearWaitsForInFlightQueuedSet(t *testing.T) {
	e, p, as, key := newGoalEngine(t)
	as.setStarted, as.allowSet = make(chan struct{}), make(chan struct{})
	sendText(e, p, key, "working")
	waitFor(t, func() bool { return queuedSendCount(as.queuingAgentSession) == 1 }, 2*time.Second)
	sendText(e, p, key, "/goal queued")
	as.events <- Event{Type: EventResult, Done: true}
	select {
	case <-as.setStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("goal did not start")
	}
	done := make(chan struct{})
	go func() { sendText(e, p, key, "/goal clear"); close(done) }()
	close(as.allowSet)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("clear did not finish")
	}
	info, clears, sets := as.state()
	if sets != 1 || clears != 1 || info.Objective != "" {
		t.Fatalf("goal=%#v clears=%d sets=%d", info, clears, sets)
	}
}

type blockingGoalPlatform struct {
	*stubPlatformEngine
	entered, release chan struct{}
}

func (p *blockingGoalPlatform) Reply(ctx context.Context, replyCtx any, content string) error {
	if strings.Contains(content, "当前没有 goal") {
		close(p.entered)
		<-p.release
	}
	return p.stubPlatformEngine.Reply(ctx, replyCtx, content)
}
func TestGoalStatusResumeDoesNotLoseQueuedMessage(t *testing.T) {
	e, p, as, key := newGoalEngine(t)
	delete(e.interactiveStates, key)
	blocked := &blockingGoalPlatform{stubPlatformEngine: p, entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() { sendText(e, blocked, key, "/goal"); close(done) }()
	select {
	case <-blocked.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("status did not resume")
	}
	sendText(e, blocked, key, "after status")
	close(blocked.release)
	<-done
	waitFor(t, func() bool { return queuedSendCount(as.queuingAgentSession) == 1 }, 2*time.Second)
	as.events <- Event{Type: EventResult, Done: true}
	waitFor(t, func() bool { return !e.sessions.GetOrCreateActive(key).Busy() }, 2*time.Second)
}

type failNoticePlatform struct {
	*stubPlatformEngine
	failed bool
}

func (p *failNoticePlatform) Reply(ctx context.Context, replyCtx any, content string) error {
	if !p.failed && strings.Contains(content, "重启时清除") {
		p.failed = true
		return errors.New("send failed")
	}
	return p.stubPlatformEngine.Reply(ctx, replyCtx, content)
}
func TestGoalResumeNoticeFailureDoesNotSetOrLoseNotice(t *testing.T) {
	e, p, as, key := newGoalEngine(t)
	delete(e.interactiveStates, key)
	as.resumeNotice = "old"
	fp := &failNoticePlatform{stubPlatformEngine: p}
	sendText(e, fp, key, "/goal new")
	waitFor(t, func() bool { return !e.sessions.GetOrCreateActive(key).Busy() }, 2*time.Second)
	if _, _, n := as.state(); n != 0 {
		t.Fatalf("set after failed notice: %d", n)
	}
	sendText(e, fp, key, "/goal new")
	if _, _, n := as.state(); n != 1 {
		t.Fatalf("retry sets = %d", n)
	}
	if got := strings.Join(p.getSent(), "|"); !strings.Contains(got, "old") {
		t.Fatal(got)
	}
}

func TestGoalQueueGenerationSurvivesSessionStartup(t *testing.T) {
	old := &interactiveState{goalSeq: 3, pendingMessages: []queuedMessage{{goalObjective: true, goalSeq: 3}}}
	next := &interactiveState{}
	adoptPendingFromPlaceholder(old, next)
	if next.goalSeq != 3 || len(next.pendingMessages) != 1 || next.pendingMessages[0].goalSeq != next.goalSeq {
		t.Fatal("queued goal became stale during session startup")
	}
}

func TestGoalModeDoesNotDiscardAttachments(t *testing.T) {
	e, p, as, key := newGoalEngine(t)
	e.SetGoalModeAlways(true)
	e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", Content: "look", ReplyCtx: "r", Files: []FileAttachment{{FileName: "a.txt"}}})
	if _, _, n := as.state(); n != 0 {
		t.Fatalf("goal set without attachment: %d", n)
	}
	if got := strings.Join(p.getSent(), "|"); !strings.Contains(got, "附件") {
		t.Fatal(got)
	}
}

func TestGoalPreservesQuotedContext(t *testing.T) {
	e, p, as, key := newGoalEngine(t)
	e.ReceiveMessage(p, &Message{SessionKey: key, Platform: "test", Content: "/goal finish", ExtraContent: "quoted context", ReplyCtx: "r"})
	if info, _, _ := as.state(); !strings.Contains(info.Objective, "quoted context") {
		t.Fatal(info)
	}
}

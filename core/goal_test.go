package core

import (
	"strings"
	"sync"
	"testing"
	"time"
)

type goalTestSession struct {
	*queuingAgentSession
	mu     sync.Mutex
	info   GoalInfo
	sets   int
	clears int
}

func (s *goalTestSession) SetGoal(o string) (GoalInfo, error) {
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

type goalAwareTestAgent struct{ *stubAgent }

func (a *goalAwareTestAgent) SupportsGoalMode() bool { return true }

// newGoalEngine wires one live goal session (agent session id "goal-live").
func newGoalEngine(t *testing.T) (*Engine, *stubPlatformEngine, *goalTestSession, string) {
	t.Helper()
	p := &stubPlatformEngine{n: "test"}
	as := &goalTestSession{queuingAgentSession: newQueuingSession("goal-live")}
	e := NewEngine("test", &goalAwareTestAgent{&stubAgent{}}, []Platform{p}, t.TempDir()+"/s.json", LangEnglish)
	t.Cleanup(func() { _ = e.Stop() })
	key := "test:room:user"
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

// /goal <condition> must not fire while a foreground turn is in flight:
// goal/set starts its own turn, and the two event streams would mix.
func TestCmdGoalRejectedWhileTurnRunning(t *testing.T) {
	e, p, as, key := newGoalEngine(t)
	session := e.sessions.GetOrCreateActive(key)
	if !session.TryLock() {
		t.Fatal("lock")
	}
	sendText(e, p, key, "/goal make tests pass")
	if _, _, sets := as.state(); sets != 0 {
		t.Fatalf("sets = %d while a turn was running", sets)
	}
	if got := p.getSent(); len(got) != 1 || !strings.Contains(got[0], "还在跑") {
		t.Fatalf("reply = %#v", got)
	}
	session.UnlockWithoutUpdate()
}

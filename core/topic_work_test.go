package core

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Fake native app-server boundary: automatic turns, goal state and deferred
// work remain independent of Engine's foreground Session lock.
type twSession struct {
	mu                         sync.Mutex
	id                         string
	events                     chan Event
	agent                      *twAgent
	closed, running, steerFail bool
	goal                       GoalInfo
	queue                      []DeferredMessage
	inFlight                   int
	injected                   []string
	callback                   func()
	last                       time.Time
}
type twStart struct {
	s    *twSession
	text string
}
type twAgent struct {
	stubAgent
	mu       sync.Mutex
	sessions []*twSession
	started  chan twStart
}

func (a *twAgent) SupportsGoalMode() bool { return true }
func (a *twAgent) StartSession(context.Context, string) (AgentSession, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := &twSession{id: fmt.Sprintf("native-%d", len(a.sessions)), agent: a, events: make(chan Event, 32)}
	a.sessions = append(a.sessions, s)
	return s, nil
}
func (s *twSession) SetWorkStateCallback(f func()) { s.mu.Lock(); s.callback = f; s.mu.Unlock() }
func (s *twSession) notify() {
	s.mu.Lock()
	f := s.callback
	s.mu.Unlock()
	if f != nil {
		f()
	}
}
func (s *twSession) LastNativeTurnActivity() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}
func (s *twSession) GoalTurnRunning() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.running }
func (s *twSession) GetGoal() GoalInfo     { s.mu.Lock(); defer s.mu.Unlock(); return s.goal }
func (s *twSession) SetGoal(text string) (GoalInfo, error) {
	s.mu.Lock()
	s.goal = GoalInfo{Objective: text, Status: "active"}
	g := s.goal
	s.running = true
	s.last = time.Now()
	s.mu.Unlock()
	s.notify()
	s.agent.started <- twStart{s, text}
	return g, nil
}
func (s *twSession) ClearGoal() error {
	s.mu.Lock()
	s.goal = GoalInfo{}
	s.mu.Unlock()
	s.notify()
	return nil
}
func (s *twSession) Send(text, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	s.mu.Lock()
	s.running = true
	s.last = time.Now()
	s.mu.Unlock()
	s.notify()
	s.agent.started <- twStart{s, text}
	return nil
}
func (s *twSession) SendMidTurn(text, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running || s.steerFail {
		return fmt.Errorf("no steerable native turn")
	}
	s.injected = append(s.injected, text)
	return nil
}
func (s *twSession) EnqueueMidTurn(m DeferredMessage) error {
	s.mu.Lock()
	s.queue = append(s.queue, m)
	s.mu.Unlock()
	s.notify()
	return nil
}
func (s *twSession) PendingMidTurnMessages() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue) + s.inFlight
}
func (s *twSession) ClearMidTurnMessages() int {
	s.mu.Lock()
	q := s.queue
	s.queue = nil
	s.mu.Unlock()
	s.notify()
	for _, m := range q {
		if m.Notify != nil {
			m.Notify(DeferredCanceled, nil)
		}
	}
	return len(q)
}
func (s *twSession) RespondPermission(string, PermissionResult) error { return nil }
func (s *twSession) Events() <-chan Event                             { return s.events }
func (s *twSession) CurrentSessionID() string                         { return s.id }
func (s *twSession) Alive() bool                                      { s.mu.Lock(); defer s.mu.Unlock(); return !s.closed }
func (s *twSession) Close() error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.events)
	}
	s.mu.Unlock()
	s.notify()
	return nil
}
func (s *twSession) complete(text, status string) {
	s.mu.Lock()
	s.running = false
	s.last = time.Now()
	if status != "" {
		s.goal.Status = status
	}
	s.mu.Unlock()
	s.notify()
	s.mu.Lock()
	if !s.closed {
		s.events <- Event{Type: EventResult, Content: text, Done: true}
	}
	s.mu.Unlock()
}
func (s *twSession) nativeStart() {
	s.mu.Lock()
	s.running = true
	s.last = time.Now()
	s.mu.Unlock()
	s.notify()
}
func twCleanup(t *testing.T, e *Engine) {
	t.Helper()
	t.Cleanup(func() {
		e.interactiveMu.Lock()
		states := make([]*interactiveState, 0, len(e.interactiveStates))
		for _, state := range e.interactiveStates {
			states = append(states, state)
		}
		e.interactiveMu.Unlock()
		_ = e.Stop()
		for _, state := range states {
			e.stopUnsolicitedReader(state)
		}
		// Engine.Stop cancels foreground consumers, but does not join them. Wait
		// for their deferred unlock/save before TempDir cleanup removes the store.
		tfWait(t, func() bool {
			for _, session := range e.sessions.AllSessions() {
				if session.Busy() {
					return false
				}
			}
			e.rotationMu.Lock()
			defer e.rotationMu.Unlock()
			return len(e.rotationWorkers) == 0
		})
	})
}
func twEnv(t *testing.T, limit int) (*Engine, *tfPlatform, *twAgent) {
	t.Helper()
	p := &tfPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}, limit: limit}
	a := &twAgent{started: make(chan twStart, 32)}
	e := NewEngine("native-topics", a, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	e.dataDir = t.TempDir()
	twCleanup(t, e)
	return e, p, a
}
func twNext(t *testing.T, a *twAgent, want string) *twSession {
	t.Helper()
	select {
	case n := <-a.started:
		if !strings.Contains(n.text, want) {
			t.Fatalf("start %q want %q", n.text, want)
		}
		return n.s
	case <-time.After(3 * time.Second):
		t.Fatalf("no start %s", want)
		return nil
	}
}
func twStatus(e *Engine, key string) string {
	for _, s := range e.TopicSessions("test", "chat") {
		if s.SessionKey == "test:chat:thread:"+key {
			return s.State
		}
	}
	return "unknown"
}
func twAssertNoStart(t *testing.T, a *twAgent) {
	t.Helper()
	select {
	case n := <-a.started:
		t.Fatalf("unexpected model start %s", n.text)
	default:
	}
}

func TestTopicWorkGoalContinuationAndSteerIsolation(t *testing.T) {
	e, p, a := twEnv(t, 1)
	tfSend(e, p, "a", "/goal finish A")
	one := twNext(t, a, "finish A")
	tfSend(e, p, "b", "B-WAITING")
	one.complete("A-FIRST", "active")
	tfWait(t, func() bool { return tfSaw(p, "A-FIRST") && !e.sessions.LookupActive("test:chat:thread:a").Busy() })
	if twStatus(e, "a") != "running" || twStatus(e, "b") != "queued" {
		t.Fatal(e.TopicSessions("test", "chat"))
	}
	twAssertNoStart(t, a)
	one.nativeStart()
	tfSend(e, p, "a", "CORRECT-A")
	tfWait(t, func() bool { one.mu.Lock(); defer one.mu.Unlock(); return len(one.injected) == 1 })
	tfSend(e, p, "a", "/goal status")
	tfWait(t, func() bool { return tfSaw(p, "finish A") })
	twAssertNoStart(t, a)
	one.complete("A-AUTOMATIC-FINAL", "complete")
	two := twNext(t, a, "B-WAITING")
	if two == one {
		t.Fatal("cross-topic native session reused")
	}
	two.complete("B-DONE", "")
}
func TestTopicWorkGoalOnlyIdleTimerAndNativeProtection(t *testing.T) {
	if NativeGoalIdleTimeout != 60*time.Second {
		t.Fatal("native watchdog threshold drift")
	}
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprint(native), func(t *testing.T) {
			e, p, a := twEnv(t, 1)
			e.topicGoalIdleTimeout = 25 * time.Millisecond
			tfSend(e, p, "a", "/goal A")
			one := twNext(t, a, "A")
			one.complete("FIRST", "active")
			tfWait(t, func() bool { return !e.sessions.LookupActive("test:chat:thread:a").Busy() })
			if native {
				one.nativeStart()
			}
			tfSend(e, p, "b", "B")
			if native {
				select {
				case n := <-a.started:
					t.Fatalf("running native turn lost slot: %s", n.text)
				case <-time.After(60 * time.Millisecond):
				}
				one.complete("END", "complete")
			}
			two := twNext(t, a, "B")
			two.complete("DONE", "")
		})
	}
}
func TestTopicWorkDeferredInFlightHoldsUntilNotification(t *testing.T) {
	e, p, a := twEnv(t, 1)
	tfSend(e, p, "a", "A")
	one := twNext(t, a, "A")
	one.mu.Lock()
	one.inFlight = 1
	one.mu.Unlock()
	one.complete("A-END", "")
	tfWait(t, func() bool { return !e.sessions.LookupActive("test:chat:thread:a").Busy() })
	tfSend(e, p, "b", "B")
	if twStatus(e, "b") != "queued" {
		t.Fatal(e.TopicSessions("test", "chat"))
	}
	twAssertNoStart(t, a)
	one.mu.Lock()
	one.inFlight = 0
	one.mu.Unlock()
	one.notify()
	two := twNext(t, a, "B")
	two.complete("B-END", "")
}
func TestTopicWorkQueuedGoalClearStopAndNewBypass(t *testing.T) {
	for _, cmd := range []string{"/goal clear", "/stop", "/new"} {
		t.Run(cmd, func(t *testing.T) {
			e, p, a := twEnv(t, 1)
			tfSend(e, p, "a", "A")
			one := twNext(t, a, "A")
			tfSend(e, p, "b", "/goal NEVER-START")
			if twStatus(e, "b") != "queued" {
				t.Fatal("not queued")
			}
			tfSend(e, p, "b", cmd)
			tfWait(t, func() bool { return twStatus(e, "b") != "queued" })
			one.complete("A-DONE", "")
			tfWait(t, func() bool { return twStatus(e, "a") == "idle" })
			twAssertNoStart(t, a)
		})
	}
}
func TestTopicWorkClearDoesNotDeleteOrdinaryTextOrImage(t *testing.T) {
	e, p, a := twEnv(t, 1)
	tfSend(e, p, "a", "A")
	one := twNext(t, a, "A")
	tfSend(e, p, "b", "goal ordinary text")
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: "test:chat:thread:c", Content: "/stop", Images: []ImageAttachment{{Data: []byte("image")}}, ReplyCtx: tfContext{"chat", "c", "root"}})
	tfSend(e, p, "b", "/goal clear")
	if twStatus(e, "b") != "queued" || twStatus(e, "c") != "queued" {
		t.Fatal("control classifier bypassed model input")
	}
	one.complete("A-DONE", "")
	two := twNext(t, a, "goal ordinary text")
	two.complete("B-DONE", "")
	three := twNext(t, a, "/stop")
	three.complete("C-DONE", "")
}
func TestTopicWorkRotationRefusesGoalAndDeferredBeforeRegistration(t *testing.T) {
	e, p, a := twEnv(t, 1)
	tfSend(e, p, "a", "/goal A")
	one := twNext(t, a, "A")
	key := "test:chat:thread:a"
	old := e.sessions.LookupActive(key)
	for _, kind := range []string{"goal", "deferred"} {
		if kind == "deferred" {
			one.ClearGoal()
			one.mu.Lock()
			one.inFlight = 1
			one.mu.Unlock()
		}
		req := RotateRequest{SessionKey: key, ExpectedSessionID: old.ID, RequestID: kind, Handoff: "HANDOFF"}
		if _, err := e.ScheduleRotation(req); err == nil || !strings.Contains(err.Error(), kind) {
			t.Fatalf("missing explicit %s error: %v", kind, err)
		}
		if _, err := e.sessions.Rotation(kind); err == nil {
			t.Fatal("rejected rotation was registered")
		}
		if e.sessions.LookupActive(key) != old || !one.Alive() {
			t.Fatal("source changed")
		}
	}
}
func TestTopicWorkRotationPreservesDeferredArrivingAfterRegistration(t *testing.T) {
	e, p, a := twEnv(t, 1)
	tfSend(e, p, "a", "A")
	one := twNext(t, a, "A")
	key := "test:chat:thread:a"
	old := e.sessions.LookupActive(key)
	op, err := e.ScheduleRotation(RotateRequest{SessionKey: key, ExpectedSessionID: old.ID, RequestID: "race", Handoff: "NEVER"})
	if err != nil || op.Status != "SCHEDULED" {
		t.Fatal(op, err)
	}
	one.mu.Lock()
	one.steerFail = true
	one.mu.Unlock()
	tfSend(e, p, "a", "PRESERVE-INPUT")
	tfWait(t, func() bool { return one.PendingMidTurnMessages() == 1 })
	one.complete("A-DONE", "")
	tfWait(t, func() bool { op, _ = e.sessions.Rotation("race"); return op.Status == "FAILED" })
	if e.sessions.LookupActive(key) != old || !one.Alive() || one.PendingMidTurnMessages() != 1 {
		t.Fatal("accepted native input lost in rotation")
	}
}
func TestTopicWorkRotationHandoffKeepsTopicSlot(t *testing.T) {
	e, p, a := twEnv(t, 1)
	tfSend(e, p, "a", "A")
	one := twNext(t, a, "A")
	key := "test:chat:thread:a"
	old := e.sessions.LookupActive(key)
	op, err := e.ScheduleRotation(RotateRequest{SessionKey: key, ExpectedSessionID: old.ID, RequestID: "rotate", Handoff: "HANDOFF"})
	if err != nil || op.Status != "SCHEDULED" {
		t.Fatal(op, err)
	}
	tfSend(e, p, "b", "B")
	one.complete("SOURCE-DONE", "")
	fresh := twNext(t, a, "HANDOFF")
	if fresh == one || e.sessions.LookupActive(key) == old || twStatus(e, "b") != "queued" {
		t.Fatal("handoff lost its topic reservation")
	}
	fresh.complete("HANDOFF-DONE", "")
	two := twNext(t, a, "B")
	two.complete("B-DONE", "")
}

func TestTopicWorkRotationAPIReportsNativeConflict(t *testing.T) {
	w := httptest.NewRecorder()
	rotationAPIError(w, fmt.Errorf("%w: deferred input", ErrRotationNativeBusy))
	if w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "deferred input") {
		t.Fatal(w.Code, w.Body.String())
	}
}

type twProgressSession struct {
	*twSession
	completed                        uint64 // protected by twSession.mu
	snapshotArm                      atomic.Bool
	snapshotEntered, snapshotRelease chan struct{}
}

func (s *twProgressSession) NativeTurnProgress() (uint64, bool) {
	s.mu.Lock()
	completed, running := s.completed, s.running
	s.mu.Unlock()
	if s.snapshotArm.CompareAndSwap(true, false) {
		close(s.snapshotEntered)
		<-s.snapshotRelease
	}
	return completed, running
}
func (s *twProgressSession) finishProgress(text string, emit, notify bool) {
	s.mu.Lock()
	s.completed++
	s.running = false
	s.last = time.Now()
	s.mu.Unlock()
	if notify {
		s.notify()
	}
	if emit {
		s.emitResult(text, true)
	}
}
func (s *twProgressSession) emitResult(text string, done bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.events <- Event{Type: EventResult, Content: text, Done: done}
	}
}

type twBlockPlatform struct {
	*tfPlatform
	entered, release chan struct{}
	once             sync.Once
}

func (p *twBlockPlatform) Send(ctx context.Context, rc any, text string) error {
	if strings.Contains(text, "A1_DONE") && p.entered != nil {
		p.once.Do(func() { close(p.entered) })
		<-p.release
	}
	return p.tfPlatform.Send(ctx, rc, text)
}
func twProgressEnv(t *testing.T, topic bool) (*Engine, *twBlockPlatform, *twAgent, *twProgressSession, *Session, string) {
	t.Helper()
	p := &twBlockPlatform{tfPlatform: &tfPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}, limit: 1}}
	a := &twAgent{started: make(chan twStart, 16)}
	e := NewEngine("native-progress", a, []Platform{p}, filepath.Join(t.TempDir(), "s.json"), LangEnglish)
	e.dataDir = t.TempDir()
	twCleanup(t, e)
	key := "test:plain:user"
	rc := tfContext{"chat", "", "root"}
	if topic {
		key = "test:chat:thread:a"
		rc.thread = "a"
	}
	old := e.sessions.GetOrCreateActive(key)
	old.SetAgentSessionID("native-old", "test")
	s := &twProgressSession{twSession: &twSession{id: "native-old", agent: a, events: make(chan Event, 16), running: true, last: time.Now()}, snapshotEntered: make(chan struct{}), snapshotRelease: make(chan struct{})}
	state := &interactiveState{agentSession: s, agent: a, platform: p, replyCtx: rc, logicalSessionID: old.ID, rotationRouteKey: key}
	e.interactiveStates[key] = state
	if topic {
		e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: key, Content: "KEEP-A", ReplyCtx: rc})
		tfWait(t, func() bool { return tfSaw(p.tfPlatform, "Message added") })
	}
	e.watchTopicWork(key, state)
	e.startUnsolicitedReader(state, old, e.sessions, key, "")
	return e, p, a, s, old, key
}
func TestTopicNativeDeliveryProgressSurvivesCoalescedCallbacks(t *testing.T) {
	e, p, a, s, _, key := twProgressEnv(t, true)
	p.entered = make(chan struct{})
	p.release = make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(p.release) }) })
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: "test:chat:thread:b", Content: "B", ReplyCtx: tfContext{"chat", "b", "root"}})
	s.finishProgress("A1_DONE", true, false)
	<-p.entered
	// No completion callback was observed before the next native turn began.
	s.nativeStart()
	release.Do(func() { close(p.release) })
	state := e.interactiveStates[key]
	tfWait(t, func() bool { return state.topicWork.resultsAcked.Load() == 1 })
	s.finishProgress("A2_DONE", false, false)
	if !e.nativeDeliveryOutstanding(key) || !e.topicWorkSnapshot(key).deliveryPending || twStatus(e, "b") != "queued" {
		t.Fatal("coalesced native result lost its delivery reservation")
	}
	// A non-terminal result must not consume the second turn's delivery.
	s.emitResult("", false)
	s.notify()
	twAssertNoStart(t, a)
	s.emitResult("A2_DONE", true)
	next := twNext(t, a, "B")
	next.complete("B-DONE", "")
	tfWait(t, func() bool { return !e.nativeDeliveryOutstanding(key) })
	s.notify()
	twAssertNoStart(t, a)
}
func TestNativeRotationCompletionCannotLoseReservationWake(t *testing.T) {
	e, p, a, s, old, key := twProgressEnv(t, false)
	// Native completion exists, but its result has not reached the reader yet.
	s.finishProgress("SOURCE-DONE", false, false)
	s.snapshotArm.Store(true)
	type response struct {
		op  RotationOperation
		err error
	}
	result := make(chan response, 1)
	go func() {
		op, err := e.ScheduleRotation(RotateRequest{SessionKey: key, ExpectedSessionID: old.ID, RequestID: "native-race", Handoff: "HANDOFF"})
		result <- response{op, err}
	}()
	<-s.snapshotEntered
	locked := e.rotationMu.TryLock()
	if locked {
		e.rotationMu.Unlock()
	}
	if locked {
		close(s.snapshotRelease)
		t.Fatal("native snapshot is outside the completion/reservation lock")
	}
	s.emitResult("SOURCE-DONE", true)
	tfWait(t, func() bool { return tfSaw(p.tfPlatform, "SOURCE-DONE") })
	close(s.snapshotRelease)
	r := <-result
	if r.err != nil {
		t.Fatal(r.err)
	}
	fresh := twNext(t, a, "HANDOFF")
	fresh.complete("HANDOFF-DONE", "")
	tfWait(t, func() bool { op, _ := e.sessions.Rotation("native-race"); return op.Status == "SWITCHED" })
	if e.sessions.LookupActive(key) == old || !e.sessions.IsRetiredAgentID("native-old") {
		t.Fatal("native rotation did not retire source")
	}
	seen := strings.Join(p.getSent(), "\n")
	if strings.Index(seen, "SOURCE-DONE") > strings.Index(seen, "Session switched") {
		t.Fatal(seen)
	}
	if len(e.topicGroups) != 0 {
		t.Fatal("legacy session enabled topic admission")
	}
}

// A foreground error can require resync before a late native completion is
// consumed. The discarded terminal result must not leave a permanent delivery
// debt after the next ordinary turn finishes.
func TestTopicWorkResyncLateTerminalThenNormalTurnReleasesSlot(t *testing.T) {
	e, p, a, s, old, key := twProgressEnv(t, true)
	e.stopUnsolicitedReader(e.interactiveStates[key])
	state := e.interactiveStates[key]
	state.mu.Lock()
	state.eventsNeedResync = true // previous foreground event consumer aborted
	state.mu.Unlock()
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: "test:chat:thread:b", Content: "B-AFTER-RECOVERY", ReplyCtx: tfContext{"chat", "b", "root"}})
	s.emitResult("STALE-NONTERMINAL", false)
	s.finishProgress("STALE-LATE-DONE", true, true)
	if !e.nativeDeliveryOutstanding(key) || twStatus(e, "b") != "queued" {
		t.Fatal("fixture did not retain the late result's delivery reservation")
	}
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: key, Content: "A-RECOVERED", ReplyCtx: tfContext{"chat", "a", "root"}})
	recovered := twNext(t, a, "A-RECOVERED")
	if recovered != s.twSession {
		t.Fatal("resync unexpectedly replaced the native session")
	}
	if state.topicWork.resultsAcked.Load() != 1 || !e.nativeDeliveryOutstanding(key) || twStatus(e, "b") != "queued" {
		t.Fatalf("resync lost or over-acked native delivery: acked=%d topics=%+v", state.topicWork.resultsAcked.Load(), e.TopicSessions("test", "chat"))
	}
	if tfSaw(p.tfPlatform, "STALE-LATE-DONE") || tfSaw(p.tfPlatform, "STALE-NONTERMINAL") {
		t.Fatal("resync forwarded a stale result as a new reply")
	}
	s.finishProgress("A-RECOVERED-DONE", true, true)
	next := twNext(t, a, "B-AFTER-RECOVERY")
	if next == s.twSession {
		t.Fatal("waiting topic reused A's context")
	}
	tfWait(t, func() bool { return !e.nativeDeliveryOutstanding(key) })
	if state.topicWork.resultsAcked.Load() != 2 || e.sessions.LookupActive(key) != old {
		t.Fatal("discarded completion left delivery debt or reset the logical session")
	}
	next.complete("B-DONE", "")
}

func TestNativeDiscardedTerminalFailsScheduledRotation(t *testing.T) {
	for _, taken := range []bool{false, true} {
		t.Run(map[bool]string{false: "buffered-resync", true: "taken-after-reader-cancel"}[taken], func(t *testing.T) {
			e, p, a, s, old, key := twProgressEnv(t, false)
			state := e.interactiveStates[key]
			e.stopUnsolicitedReader(state)
			op, err := e.ScheduleRotation(RotateRequest{SessionKey: key, ExpectedSessionID: old.ID, RequestID: "discard-native", Handoff: "MUST-NOT-RUN"})
			if err != nil || op.Status != "SCHEDULED" {
				t.Fatal(op, err)
			}
			// Non-terminal metadata is neither a delivery acknowledgement nor
			// a reason to fail a pending replacement.
			e.discardNativeResult(state, old, e.sessions, key, Event{Type: EventResult, Done: false})
			op, _ = e.sessions.Rotation("discard-native")
			if op.Status != "SCHEDULED" || state.topicWork.resultsAcked.Load() != 0 {
				t.Fatal("non-terminal result changed the replacement or delivery count")
			}
			s.finishProgress("UNDISPATCHED-NATIVE-REPLY", true, false)
			if taken {
				event := <-s.Events()
				e.discardNativeResult(state, old, e.sessions, key, event)
			} else {
				e.drainAgentEvents(state, old, e.sessions, key)
			}
			op, err = e.sessions.Rotation("discard-native")
			if err != nil || op.Status != "FAILED" || !strings.Contains(op.Error, "discarded") {
				t.Fatal(op, err)
			}
			if e.sessions.LookupActive(key) != old || e.sessions.IsRetiredAgentID("native-old") || !s.Alive() {
				t.Fatal("discarded native result committed or retired the source")
			}
			if state.topicWork.resultsAcked.Load() != 1 || e.nativeDeliveryOutstanding(key) {
				t.Fatal("discarded terminal result retained delivery debt")
			}
			if tfSaw(p.tfPlatform, "UNDISPATCHED-NATIVE-REPLY") || tfSaw(p.tfPlatform, "Session switched") || !tfSaw(p.tfPlatform, "discarded") {
				t.Fatal(p.getSent())
			}
			twAssertNoStart(t, a)
			if len(e.topicGroups) != 0 {
				t.Fatal("legacy discard path enabled topic admission")
			}
		})
	}
}

func TestNativeDiscardAcknowledgesOnlyEmittedCompletedResults(t *testing.T) {
	e, _, _, s, old, key := twProgressEnv(t, false)
	state := e.interactiveStates[key]
	e.stopUnsolicitedReader(state)
	// A stale terminal event excluded by the installation baseline must not
	// consume the currently running turn's pending delivery.
	e.topicDeliveryFinished(state)
	if state.topicWork.resultsAcked.Load() != 0 || !e.nativeDeliveryOutstanding(key) {
		t.Fatal("acknowledgement consumed a running, uncompleted turn")
	}
	// completeTurn advances native progress before emit. Empty-channel resync
	// must not reset acknowledgements from this snapshot or release the slot.
	s.finishProgress("EMIT-LATER", false, false)
	e.drainAgentEvents(state, old, e.sessions, key)
	if state.topicWork.resultsAcked.Load() != 0 || !e.nativeDeliveryOutstanding(key) {
		t.Fatal("resync acknowledged a completion before its result was emitted")
	}
	s.emitResult("EMIT-LATER", true)
	e.drainAgentEvents(state, old, e.sessions, key)
	if state.topicWork.resultsAcked.Load() != 1 || e.nativeDeliveryOutstanding(key) {
		t.Fatal("explicitly discarded emitted result was not acknowledged")
	}
}

// Model the exact admission/clear race without scheduler sleeps: a topic
// dispatcher has removed an admitted goal envelope, clear completes, and only
// then is that old envelope allowed to join the foreground busy FIFO.
func TestTopicWorkClearedAdmissionGenerationSurvivesBusyQueueAndRotation(t *testing.T) {
	for _, rotate := range []bool{false, true} {
		t.Run(map[bool]string{false: "same-native-session", true: "rot2-queued-handoff"}[rotate], func(t *testing.T) {
			e, p, a := twEnv(t, 1)
			tfSend(e, p, "a", "ORDINARY-A")
			one := twNext(t, a, "ORDINARY-A")
			key := "test:chat:thread:a"
			old := e.sessions.LookupActive(key)
			state := e.interactiveStates[key]
			e.topicMu.Lock()
			generation := e.topicGroups[topicGroupID("test", "chat")].goalGeneration[key]
			e.topicMu.Unlock()
			// This is the already-popped envelope. It must preserve its old
			// generation even though clear will run before the FIFO append.
			stale := &Message{
				Platform: "test", SessionKey: key, MessageID: "stale-topic-goal",
				Content: "/goal STALE-MUST-NOT-START", ReplyCtx: tfContext{"chat", "a", "root"},
				topicAdmitted: true, topicGoalSet: true, topicGoalGeneration: generation,
			}
			if rotate {
				op, err := e.ScheduleRotation(RotateRequest{SessionKey: key, ExpectedSessionID: old.ID, RequestID: "goal-generation", Handoff: "ROT2-HANDOFF"})
				if err != nil || op.Status != "SCHEDULED" {
					t.Fatal(op, err)
				}
			}
			tfSend(e, p, "a", "/goal clear")
			state.mu.Lock()
			afterClear := state.goalSeq
			state.mu.Unlock()
			e.ReceiveMessage(p, stale)
			state.mu.Lock()
			if len(state.pendingMessages) != 1 {
				state.mu.Unlock()
				t.Fatal("stale admitted goal did not reach the intended busy FIFO")
			}
			queued := state.pendingMessages[0]
			state.mu.Unlock()
			if queued.goalSeq != afterClear || !queued.topicGoalSet || queued.topicGoalGeneration != generation {
				t.Fatalf("queue lost original admission token: %+v", queued)
			}
			// goalSeq deliberately equals the post-clear value. Only the
			// independently retained topic admission token can reject it.
			one.complete("ORDINARY-A-DONE", "")
			if rotate {
				handoff := twNext(t, a, "ROT2-HANDOFF")
				if handoff == one {
					t.Fatal("rotation did not create a distinct native session")
				}
				handoff.complete("ROT2-HANDOFF-DONE", "")
			}
			tfWait(t, func() bool {
				select {
				case started := <-a.started:
					t.Fatalf("cleared goal resurrected after FIFO/rotation: %q", started.text)
				default:
				}
				return !e.sessions.LookupActive(key).Busy()
			})
			if info := one.GetGoal(); info.Active() {
				t.Fatalf("clear was undone by old admitted goal: %+v", info)
			}
			// A genuinely new goal captured after clear must still work.
			tfSend(e, p, "a", "/goal FRESH-AFTER-CLEAR")
			fresh := twNext(t, a, "FRESH-AFTER-CLEAR")
			fresh.complete("FRESH-AFTER-CLEAR-DONE", "complete")
			tfWait(t, func() bool { return tfSaw(p, "FRESH-AFTER-CLEAR-DONE") })
		})
	}
}

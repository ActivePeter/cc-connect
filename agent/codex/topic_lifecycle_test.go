package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

type topicLifecycleReply struct {
	thread, text string
}

type topicLifecycleRPC struct {
	thread, method, expected, text string
	at                             time.Time
}

type topicLifecyclePlatform struct {
	mu      sync.Mutex
	replies []topicLifecycleReply
	changed chan struct{}
}

func (p *topicLifecyclePlatform) pulse() {
	select {
	case p.changed <- struct{}{}:
	default:
	}
}
func (*topicLifecyclePlatform) Name() string                    { return "topic-probe" }
func (*topicLifecyclePlatform) Start(core.MessageHandler) error { return nil }
func (*topicLifecyclePlatform) Stop() error                     { return nil }
func (*topicLifecyclePlatform) TopicSession(replyCtx any) (string, string, int) {
	thread, _ := replyCtx.(string)
	if thread == "" {
		return "", "", 1
	}
	return "chat", thread, 1
}
func (p *topicLifecyclePlatform) Reply(_ context.Context, replyCtx any, text string) error {
	thread, _ := replyCtx.(string)
	p.mu.Lock()
	p.replies = append(p.replies, topicLifecycleReply{thread, text})
	p.mu.Unlock()
	p.pulse()
	return nil
}
func (p *topicLifecyclePlatform) Send(ctx context.Context, replyCtx any, text string) error {
	return p.Reply(ctx, replyCtx, text)
}
func (p *topicLifecyclePlatform) saw(thread, text string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, reply := range p.replies {
		if reply.thread == thread && strings.Contains(reply.text, text) {
			return true
		}
	}
	return false
}
func (p *topicLifecyclePlatform) snapshot() []topicLifecycleReply {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]topicLifecycleReply(nil), p.replies...)
}

type topicLifecycleAgent struct {
	*Agent
	fixture *topicLifecycleFixture
}

func (a *topicLifecycleAgent) StartSession(_ context.Context, _ string) (core.AgentSession, error) {
	a.mu.RLock()
	var key string
	for _, entry := range a.sessionEnv {
		if strings.HasPrefix(entry, "CC_SESSION_KEY=") {
			key = strings.TrimPrefix(entry, "CC_SESSION_KEY=")
		}
	}
	a.mu.RUnlock()
	f := a.fixture
	f.mu.Lock()
	s := f.sessions[key]
	if s != nil {
		f.starts[key]++
	}
	f.mu.Unlock()
	if s == nil {
		return nil, fmt.Errorf("unexpected topic fixture session key %q", key)
	}
	f.p.pulse()
	return s, nil
}

type topicLifecycleFixture struct {
	e        *core.Engine
	p        *topicLifecyclePlatform
	mu       sync.Mutex
	rpcs     []topicLifecycleRPC
	starts   map[string]int
	sessions map[string]*appServerSession
}

func topicLifecycleKey(thread string) string {
	if thread == "" {
		return "topic-probe:chat:main"
	}
	return "topic-probe:chat:thread:" + thread
}

func newTopicLifecycleFixture(t *testing.T) *topicLifecycleFixture {
	t.Helper()
	dir := t.TempDir()
	p := &topicLifecyclePlatform{changed: make(chan struct{}, 1)}
	f := &topicLifecycleFixture{p: p, starts: make(map[string]int), sessions: make(map[string]*appServerSession)}
	for _, thread := range []string{"", "A", "B"} {
		thread := thread
		id := "native-" + thread
		if thread == "" {
			id = "native-main"
		}
		goal, _ := json.Marshal(threadGoalSetResponse{Goal: threadGoal{ThreadID: id, Objective: "objective-" + thread, Status: "active"}})
		turn, _ := json.Marshal(map[string]any{"turn": map[string]any{"id": id + "-input"}})
		s, _ := fakeGoalSession(t, map[string]string{
			"thread/goal/set":    string(goal),
			"thread/goal/clear":  "{\"cleared\":true}",
			"turn/start":         string(turn),
			"test/manual-notice": "true",
		}, func(req map[string]any) {
			method, _ := req["method"].(string)
			params, _ := req["params"].(map[string]any)
			expected, _ := params["expectedTurnId"].(string)
			var text string
			if input, ok := params["input"].([]any); ok && len(input) > 0 {
				if first, ok := input[0].(map[string]any); ok {
					text, _ = first["text"].(string)
				}
			}
			f.mu.Lock()
			f.rpcs = append(f.rpcs, topicLifecycleRPC{thread, method, expected, text, time.Now()})
			f.mu.Unlock()
			p.pulse()
		})
		s.ctx, s.cancel = context.WithCancel(context.Background())
		s.workDir = dir
		s.threadID.Store(id)
		f.sessions[topicLifecycleKey(thread)] = s
		t.Cleanup(func() { _ = s.Close() })
	}
	// New validates only this harmless executable. StartSession above never
	// launches it and all app-server RPCs terminate at fakeGoalSession's pipe.
	raw, err := New(map[string]any{"cmd": "true", "backend": "app_server", "work_dir": dir, "codex_home": filepath.Join(dir, "codex-home")})
	if err != nil {
		t.Fatal(err)
	}
	a := &topicLifecycleAgent{Agent: raw.(*Agent), fixture: f}
	f.e = core.NewEngine("topic-lifecycle-probe", a, []core.Platform{p}, filepath.Join(dir, "sessions.json"), core.LangEnglish)
	f.e.SetReplyFooterEnabled(false)
	f.e.SetShowContextIndicator(false)
	f.e.SetShowWorkdirIndicator(false)
	t.Cleanup(func() { _ = f.e.Stop() })
	return f
}

func (f *topicLifecycleFixture) session(thread string) *appServerSession {
	return f.sessions[topicLifecycleKey(thread)]
}
func (f *topicLifecycleFixture) send(thread, text string) {
	f.e.ReceiveMessage(f.p, &core.Message{
		SessionKey: topicLifecycleKey(thread), Platform: f.p.Name(), UserID: "tester",
		UserName: "Tester", MessageID: thread + ":" + text, Content: text, ReplyCtx: thread,
	})
}
func (f *topicLifecycleFixture) wait(t *testing.T, why string, condition func() bool) {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-f.p.changed:
		case <-timer.C:
			t.Fatalf("%s; replies=%+v; RPC=%+v", why, f.p.snapshot(), f.rpcSnapshot())
		}
	}
}
func (f *topicLifecycleFixture) rpcSnapshot() []topicLifecycleRPC {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]topicLifecycleRPC(nil), f.rpcs...)
}
func (f *topicLifecycleFixture) rpcCount(thread, method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, rpc := range f.rpcs {
		if rpc.thread == thread && rpc.method == method {
			n++
		}
	}
	return n
}
func (f *topicLifecycleFixture) startCount(thread string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts[topicLifecycleKey(thread)]
}
func (f *topicLifecycleFixture) topicState(thread string) string {
	for _, status := range f.e.TopicSessions(f.p.Name(), "chat") {
		if status.SessionKey == topicLifecycleKey(thread) {
			return status.State
		}
	}
	return ""
}
func (f *topicLifecycleFixture) startNative(thread, turn string) {
	b, _ := json.Marshal(map[string]any{"threadId": f.session(thread).CurrentSessionID(), "turn": map[string]any{"id": turn}})
	f.session(thread).handleNotification("turn/started", b)
}
func (f *topicLifecycleFixture) finishNative(thread, text string, terminal bool) {
	s := f.session(thread)
	if terminal {
		b, _ := json.Marshal(threadGoalNotification{ThreadID: s.CurrentSessionID(), Goal: threadGoal{ThreadID: s.CurrentSessionID(), Objective: "objective-" + thread, Status: "complete"}})
		s.handleNotification("thread/goal/updated", b)
	}
	s.handleItemCompleted(map[string]any{"type": "agentMessage", "text": text})
	s.completeTurn()
}

// Real Engine + actual appServerSession state/steer paths, fake only at the
// app-server JSON-RPC and platform boundaries. No CLI, auth or model is used.
func TestTopicLifecycleAppServerAutonomousGoalKeepsSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newTopicLifecycleFixture(t)
		f.send("A", "/goal objective-A")
		f.wait(t, "A goal was not set", func() bool { return f.p.saw("A", "goal 已设置") })
		f.startNative("A", "A-first")
		f.finishNative("A", "A_FIRST_DONE", false)
		f.wait(t, "A first reply was not delivered", func() bool { return f.p.saw("A", "A_FIRST_DONE") })
		synctest.Wait()
		if f.e.GetSessions().LookupActive(topicLifecycleKey("A")).Busy() {
			t.Fatal("fixture did not reach the foreground-free goal gap")
		}
		if !f.session("A").GetGoal().Active() || f.session("A").GoalTurnRunning() {
			t.Fatal("fixture did not preserve an active goal without a native turn")
		}

		f.send("B", "B_WORK")
		f.wait(t, "B was not visibly queued behind A's goal", func() bool { return f.p.saw("B", "Queued;") })
		synctest.Wait()
		if f.startCount("B") != 0 || f.topicState("B") != "queued" {
			t.Fatalf("B started in A's native goal gap: starts=%d state=%s", f.startCount("B"), f.topicState("B"))
		}
		f.startNative("A", "A-autonomous")
		f.send("A", "A_ONLY_INTERJECTION")
		f.wait(t, "A interjection did not reach native turn/steer", func() bool { return f.rpcCount("A", "turn/steer") == 1 })
		f.wait(t, "A interjection acknowledgement missing", func() bool { return f.p.saw("A", "Message added to the running task.") })
		synctest.Wait()
		if f.startCount("B") != 0 || f.topicState("B") != "queued" || f.rpcCount("A", "thread/goal/set") != 1 {
			t.Fatalf("autonomous turn lost its slot or replaced its goal: states=%+v RPC=%+v", f.e.TopicSessions(f.p.Name(), "chat"), f.rpcSnapshot())
		}
		for _, rpc := range f.rpcSnapshot() {
			if rpc.method == "turn/steer" && (rpc.thread != "A" || rpc.expected != "A-autonomous" || !strings.Contains(rpc.text, "A_ONLY_INTERJECTION")) {
				t.Fatalf("interjection crossed topic/native turn: %+v", rpc)
			}
		}

		f.finishNative("A", "A_FINAL_DONE", true)
		f.wait(t, "A terminal reply was not delivered", func() bool { return f.p.saw("A", "A_FINAL_DONE") })
		f.wait(t, "B did not start after A's goal and turn ended", func() bool { return f.rpcCount("B", "turn/start") == 1 })
		synctest.Wait()
		if f.startCount("A") != 1 || f.startCount("B") != 1 || f.session("A").GetGoal().Status != "complete" {
			t.Fatalf("unexpected final lifecycle: A=%d B=%d goal=%+v", f.startCount("A"), f.startCount("B"), f.session("A").GetGoal())
		}
		f.finishNative("B", "B_DONE", false)
		f.wait(t, "B did not deliver its own output", func() bool { return f.p.saw("B", "B_DONE") })
		synctest.Wait()
		t.Logf("A goal gap -> B queued -> A autonomous steer -> A terminal -> B native start; RPC=%+v", f.rpcSnapshot())
	})
}

func TestTopicLifecycleAppServerQueuedControlsAndStopIsolation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newTopicLifecycleFixture(t)
		f.send("", "MAIN_WORK")
		f.wait(t, "main ordinary turn did not start", func() bool { return f.rpcCount("", "turn/start") == 1 })
		synctest.Wait()
		f.send("A", "/goal objective-A")
		f.wait(t, "A goal was not set", func() bool { return f.p.saw("A", "goal 已设置") })
		f.startNative("A", "A-running")
		f.send("B", "/goal objective-B")
		f.wait(t, "B goal was not queued", func() bool { return f.p.saw("B", "Queued;") })

		f.send("B", "/goal status")
		f.wait(t, "queued B status command was not immediate", func() bool { return f.p.saw("B", "当前没有 goal") })
		f.send("B", "/goal clear")
		synctest.Wait()
		if f.startCount("B") != 0 || f.rpcCount("B", "thread/goal/set") != 0 || f.topicState("B") == "queued" {
			t.Fatalf("B clear did not cancel the queued goal: starts=%d state=%s RPC=%+v", f.startCount("B"), f.topicState("B"), f.rpcSnapshot())
		}
		if !f.session("A").GetGoal().Active() || !f.session("A").GoalTurnRunning() {
			t.Fatal("B control command changed A's goal or turn")
		}
		f.send("A", "/goal status")
		f.wait(t, "A status did not report A's objective", func() bool { return f.p.saw("A", "goal：objective-A") })
		f.send("A", "/goal clear")
		f.wait(t, "A clear did not reach A's RPC", func() bool { return f.rpcCount("A", "thread/goal/clear") == 1 })
		f.wait(t, "A clear acknowledgement missing", func() bool { return f.p.saw("A", "goal 已清除") })
		if f.session("A").GetGoal().Active() || !f.session("A").GoalTurnRunning() || !f.session("").GoalTurnRunning() {
			t.Fatal("goal clear should keep its native turn and the main turn running")
		}
		f.send("A", "/stop")
		synctest.Wait()
		if f.session("A").Alive() || !f.session("").Alive() || !f.session("").GoalTurnRunning() {
			t.Fatal("topic /stop failed to stop A alone")
		}
		if f.rpcCount("", "thread/goal/clear") != 0 || f.rpcCount("B", "thread/goal/clear") != 0 {
			t.Fatal("goal control leaked to a different session")
		}
		f.finishNative("", "MAIN_DONE", false)
		f.wait(t, "main did not survive topic stop", func() bool { return f.p.saw("", "MAIN_DONE") })
		synctest.Wait()
		t.Logf("queued B status/clear immediate; A clear preserved running native turn; A stop preserved main; RPC=%+v", f.rpcSnapshot())
	})
}

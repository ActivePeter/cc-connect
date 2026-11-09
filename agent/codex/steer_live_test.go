package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// Opt-in only. Cross-compile this test on the build host, then run the binary
// on the authenticated Codex host with CC_STEER_LIVE_DIR pointing to an isolated
// directory outside any production instance. No model calls in ordinary tests.
func TestCodexGoalSteerLive(t *testing.T)  { runCodexGoalSteerLive(t, false) }
func TestCodexQueuedGoalLive(t *testing.T) { runCodexGoalSteerLive(t, true) }
func runCodexGoalSteerLive(t *testing.T, queued bool) {
	dir := os.Getenv("CC_STEER_LIVE_DIR")
	if dir == "" {
		t.Skip("set CC_STEER_LIVE_DIR for the opt-in real-model probe")
	}
	if !filepath.IsAbs(dir) {
		t.Fatal("probe directory must be absolute")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "checkpoint")); !os.IsNotExist(err) {
		t.Fatal("probe requires a fresh directory")
	}
	if err := os.WriteFile(filepath.Join(dir, "AGENTS.md"), []byte("This is an isolated cc-connect protocol probe. Follow the current user goal exactly. Do not read other directories, use skills or agents, or do unrelated work.\n"), 0600); err != nil {
		t.Fatal(err)
	}
	raw, err := New(map[string]any{"backend": "app_server", "app_server_url": "stdio", "work_dir": dir, "model": "gpt-6.1-terra", "reasoning_effort": "low", "mode": "full-auto"})
	if err != nil {
		t.Fatal(err)
	}
	a := &steerLiveAgent{Agent: raw.(*Agent), started: make(chan *appServerSession, 1)}
	p := &steerLivePlatform{}
	e := core.NewEngine("steer-live-probe", a, []core.Platform{p}, filepath.Join(dir, "sessions.json"), core.LangEnglish)
	e.SetGoalModeAlways(true)
	defer e.Stop()
	key := "probe:isolated:user"
	objective := `This is an explicitly requested goal-mode acceptance probe. Complete exactly three checkpoints in three SEPARATE turns. Each turn run this single command exactly ONCE (no additional shell commands): python3 -c 'from pathlib import Path; import time; p=Path("checkpoint"); n=int(p.read_text())+1 if p.exists() else 1; p.write_text(str(n)); print("checkpoint",n,flush=True); time.sleep(8)'. After checkpoint 1 and 2, give a FINAL interim response and END THAT TURN while keeping the goal active; the next automatic goal turn continues. Do not do two checkpoints in one turn. After checkpoint 3 mark goal complete and give the final reply. Do not use other files, skills, agents, network, or tools except this command and goal-status tools. If a user interjects a marker, put it in the final response of that same turn and later turns. Never clear or replace the goal.`
	e.ReceiveMessage(p, &core.Message{SessionKey: key, Platform: p.Name(), UserID: "tester", MessageID: "goal", Content: objective, ReplyCtx: "original"})
	var s *appServerSession
	select {
	case s = <-a.started:
	case <-time.After(30 * time.Second):
		t.Fatal("session not started")
	}
	report := map[string]any{"threadId": s.CurrentSessionID(), "checkpoints": []any{}}
	defer func() {
		p.mu.Lock()
		report["platformMessages"] = append([]string(nil), p.sent...)
		p.mu.Unlock()
		b, _ := json.MarshalIndent(report, "", "  ")
		_ = os.WriteFile(filepath.Join(dir, "live-summary.json"), b, 0600)
	}()
	wait := func(why string, fn func() bool) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Minute)
		for time.Now().Before(deadline) {
			if fn() {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("timed out: %s; messages=%v", why, p.messages())
	}
	count := func() string { b, _ := os.ReadFile(filepath.Join(dir, "checkpoint")); return string(b) }
	limit := 2
	if queued {
		limit = 1
	}
	for n := 1; n <= limit; n++ {
		wait(fmt.Sprintf("checkpoint %d", n), func() bool { return count() == fmt.Sprint(n) })
		s.stateMu.Lock()
		turn := s.currentTurn
		s.stateMu.Unlock()
		marker := fmt.Sprintf("CC_STEER_LIVE_TURN_%d_1106", n)
		before := len(p.messages())
		if queued {
			s.writeMu.Lock()
			s.stdin = &oneExpiredSteerWriter{base: s.stdin}
			s.writeMu.Unlock()
		}
		// Drive the exact platform entrypoint, including the autonomous goal turn
		// where the foreground lock and midTurnReady have already been released.
		e.ReceiveMessage(p, &core.Message{SessionKey: key, Platform: p.Name(), UserID: "tester", MessageID: fmt.Sprintf("steer-%d", n), Content: "Interjection: add exact marker " + marker + " in the FINAL reply of THIS turn and later turns. Continue the original goal unchanged.", ReplyCtx: "interjection"})
		after := p.messages()
		ack := "Message added to the running task."
		if queued {
			ack = "Queued; will insert"
		}
		if len(after) != before+1 || !strings.Contains(after[len(after)-1], ack) {
			t.Fatalf("interjection not accepted: %v", after)
		}
		var goal threadGoalGetResponse
		if err := s.request("thread/goal/get", map[string]any{"threadId": s.CurrentSessionID()}, &goal); err != nil {
			t.Fatal(err)
		}
		if goal.Goal == nil || goal.Goal.Status != "active" {
			t.Fatalf("goal after steer: %+v", goal.Goal)
		}
		wait("current-turn final marker", func() bool {
			for _, m := range p.messages()[before+1:] {
				if strings.Contains(m, marker) {
					return true
				}
			}
			return false
		})
		// A marker in the next turn is insufficient: the target turn's command
		// must still be the only checkpoint completed when its reply is observed.
		wantCheckpoint := n
		if queued {
			wantCheckpoint = 2
		}
		if count() != fmt.Sprint(wantCheckpoint) {
			t.Fatalf("marker observed at checkpoint %s; want %d", count(), wantCheckpoint)
		}
		if queued {
			if s.PendingMidTurnMessages() != 0 {
				t.Fatal("queued marker not drained")
			}
			report["queuedMarkerInNextTurn"] = true
		}
		entry := map[string]any{"turnId": turn, "marker": marker, "goalAfterSteer": goal.Goal, "markerInSameTurn": !queued, "markerObservedAtCheckpoint": wantCheckpoint}
		if queued {
			entry["attemptedTurnId"] = turn
			delete(entry, "turnId")
		}
		report[fmt.Sprintf("steer%d", n)] = entry
		t.Logf("checkpoint=%d turn=%s marker=%s goal=active", n, turn, marker)
	}
	wait("goal complete", func() bool { return s.GetGoal().Status == "complete" && !s.GoalTurnRunning() })
	var final threadGoalGetResponse
	if err := s.request("thread/goal/get", map[string]any{"threadId": s.CurrentSessionID()}, &final); err != nil {
		t.Fatal(err)
	}
	if final.Goal == nil || final.Goal.Status != "complete" || count() != "3" {
		t.Fatalf("final=%+v count=%s", final.Goal, count())
	}
	s.queueMu.Lock()
	report["queueSteerAttempts"] = s.queueAttempts
	report["queueSteerMisses"] = s.queueMisses
	s.queueMu.Unlock()
	report["goalFinal"] = final.Goal
	report["pass"] = true
	t.Logf("goal complete after exactly 3 checkpoints; thread=%s", s.CurrentSessionID())
}

type steerLiveAgent struct {
	*Agent
	started chan *appServerSession
}

func (a *steerLiveAgent) StartSession(ctx context.Context, id string) (core.AgentSession, error) {
	s, err := a.Agent.StartSession(ctx, id)
	if err == nil {
		a.started <- s.(*appServerSession)
	}
	return s, err
}

type steerLivePlatform struct {
	mu   sync.Mutex
	sent []string
}

func (p *steerLivePlatform) Name() string                    { return "probe" }
func (p *steerLivePlatform) Start(core.MessageHandler) error { return nil }
func (p *steerLivePlatform) Stop() error                     { return nil }
func (p *steerLivePlatform) Reply(_ context.Context, _ any, text string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sent = append(p.sent, text)
	return nil
}
func (p *steerLivePlatform) Send(ctx context.Context, r any, text string) error {
	return p.Reply(ctx, r, text)
}
func (p *steerLivePlatform) messages() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.sent...)
}

// Deliberately corrupt exactly one real steer request to reproduce a stale
// expectedTurnId. Subsequent queued steers use the unmodified protocol.
type oneExpiredSteerWriter struct {
	base io.WriteCloser
	used atomic.Bool
}

func (w *oneExpiredSteerWriter) Write(b []byte) (int, error) {
	var d map[string]any
	if json.Unmarshal(b, &d) == nil && d["method"] == "turn/steer" && w.used.CompareAndSwap(false, true) {
		d["params"].(map[string]any)["expectedTurnId"] = "expired-queue-probe-id"
		changed, _ := json.Marshal(d)
		changed = append(changed, '\n')
		_, err := w.base.Write(changed)
		return len(b), err
	}
	return w.base.Write(b)
}
func (w *oneExpiredSteerWriter) Close() error { return w.base.Close() }

func TestCodexOrdinarySteerLive(t *testing.T) {
	dir := os.Getenv("CC_STEER_LIVE_DIR")
	if dir == "" {
		t.Skip("set CC_STEER_LIVE_DIR for opt-in real model probe")
	}
	if !filepath.IsAbs(dir) {
		t.Fatal("absolute directory required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ordinary")); !os.IsNotExist(err) {
		t.Fatal("fresh directory required")
	}
	_ = os.WriteFile(filepath.Join(dir, "AGENTS.md"), []byte("Isolated acceptance probe. Only follow the current prompt; no unrelated files, skills, agents or network."), 0600)
	raw, err := New(map[string]any{"backend": "app_server", "app_server_url": "stdio", "work_dir": dir, "model": "gpt-6.1-terra", "reasoning_effort": "low", "mode": "full-auto"})
	if err != nil {
		t.Fatal(err)
	}
	a := &steerLiveAgent{Agent: raw.(*Agent), started: make(chan *appServerSession, 1)}
	p := &steerLivePlatform{}
	e := core.NewEngine("ordinary-probe", a, []core.Platform{p}, filepath.Join(dir, "sessions.json"), core.LangEnglish)
	defer e.Stop()
	key := "probe:ordinary:user"
	prompt := `This is an ordinary turn, not a goal. Run exactly one command: python3 -c 'from pathlib import Path; import time; Path("ordinary").write_text("running"); time.sleep(8)'. Then FINAL answer ORDINARY_DONE and any interjected marker. Do not create a goal, call other tools or do other work.`
	e.ReceiveMessage(p, &core.Message{SessionKey: key, Platform: p.Name(), Content: prompt, MessageID: "ordinary", ReplyCtx: "original"})
	var s *appServerSession
	select {
	case s = <-a.started:
	case <-time.After(30 * time.Second):
		t.Fatal("session start timeout")
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if _, err := os.Stat(filepath.Join(dir, "ordinary")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("tool did not start")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Simulate the always instance receiving a new message during a resumed
	// ordinary turn. No active goal exists and none should be created.
	e.SetGoalModeAlways(true)
	e.ReceiveMessage(p, &core.Message{SessionKey: key, Platform: p.Name(), Content: "Interjection: FINAL reply must contain ORDINARY_STEER_MARKER_1106. Continue this ordinary turn; do not set a goal.", MessageID: "ordinary-steer", ReplyCtx: "interjection"})
	if s.GetGoal().Status != "" {
		t.Fatal("interjection created a goal")
	}
	for {
		found := false
		for _, m := range p.messages() {
			if strings.Contains(m, "ORDINARY_STEER_MARKER_1106") {
				found = true
			}
		}
		if found && !s.GoalTurnRunning() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ordinary turn did not read marker")
		}
		time.Sleep(50 * time.Millisecond)
	}
	var goal threadGoalGetResponse
	if err := s.request("thread/goal/get", map[string]any{"threadId": s.CurrentSessionID()}, &goal); err != nil {
		t.Fatal(err)
	}
	if goal.Goal != nil {
		t.Fatal("ordinary steering created a native goal")
	}
	b, _ := json.MarshalIndent(map[string]any{"pass": true, "goalAfterSteer": goal.Goal, "threadId": s.CurrentSessionID(), "platformMessages": p.messages()}, "", "  ")
	_ = os.WriteFile(filepath.Join(dir, "live-summary.json"), b, 0600)
	t.Log("ordinary marker read in current turn; no goal created")
}

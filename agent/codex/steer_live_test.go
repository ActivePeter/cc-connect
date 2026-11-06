package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// Opt-in only. Cross-compile this test on the build host, then run the binary
// on the authenticated Codex host with CC_STEER_LIVE_DIR pointing to an isolated
// directory outside any production instance. No model calls in ordinary tests.
func TestCodexGoalSteerLive(t *testing.T) {
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
	for n := 1; n <= 2; n++ {
		wait(fmt.Sprintf("checkpoint %d", n), func() bool { return count() == fmt.Sprint(n) })
		s.stateMu.Lock()
		turn := s.currentTurn
		s.stateMu.Unlock()
		marker := fmt.Sprintf("CC_STEER_LIVE_TURN_%d_1106", n)
		before := len(p.messages())
		// Drive the exact platform entrypoint, including the autonomous goal turn
		// where the foreground lock and midTurnReady have already been released.
		e.ReceiveMessage(p, &core.Message{SessionKey: key, Platform: p.Name(), UserID: "tester", MessageID: fmt.Sprintf("steer-%d", n), Content: "Interjection: add exact marker " + marker + " in the FINAL reply of THIS turn and later turns. Continue the original goal unchanged.", ReplyCtx: "interjection"})
		after := p.messages()
		if len(after) != before+1 || !strings.Contains(after[len(after)-1], "Message added to the running task.") {
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
		if count() != fmt.Sprint(n) {
			t.Fatalf("marker was delayed past checkpoint %d", n)
		}
		report[fmt.Sprintf("steer%d", n)] = map[string]any{"turnId": turn, "marker": marker, "goalAfterSteer": goal.Goal, "markerInSameTurn": true}
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

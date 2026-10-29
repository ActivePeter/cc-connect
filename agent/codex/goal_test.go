package codex

import (
	"encoding/json"
	"io"
	"sync"
	"testing"

	"github.com/chenhg5/cc-connect/core"
)

// fakeGoalSession answers JSON-RPC requests from a canned map keyed by method
// and records the methods it saw.
func fakeGoalSession(t *testing.T, replies map[string]string) (*appServerSession, func() []string) {
	t.Helper()
	pr, pw := io.Pipe()
	s := &appServerSession{events: make(chan core.Event, 16), stdin: pw}
	s.alive.Store(true)
	s.threadID.Store("thread-1")
	var mu sync.Mutex
	var methods []string
	go func() {
		dec := json.NewDecoder(pr)
		for {
			var req map[string]any
			if err := dec.Decode(&req); err != nil {
				return
			}
			method, _ := req["method"].(string)
			mu.Lock()
			methods = append(methods, method)
			mu.Unlock()
			res := replies[method]
			if res == "" {
				res = "{}"
			}
			s.handleResponse(rpcResponseEnvelope{ID: req["id"], Result: json.RawMessage(res)})
		}
	}()
	t.Cleanup(func() { _ = pw.Close(); _ = pr.Close() })
	return s, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), methods...)
	}
}

func drainEvents(s *appServerSession) []core.Event {
	var out []core.Event
	for {
		select {
		case ev := <-s.events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func TestGoalControllerSetClear(t *testing.T) {
	s, methods := fakeGoalSession(t, map[string]string{
		"thread/goal/set":   `{"goal":{"threadId":"thread-1","objective":"make tests pass","status":"active","tokensUsed":0}}`,
		"thread/goal/clear": `{"cleared":true}`,
	})
	info, err := s.SetGoal("make tests pass")
	if err != nil || !info.Active() || s.GetGoal().Objective != "make tests pass" {
		t.Fatalf("SetGoal = %#v, %v", info, err)
	}
	if err := s.ClearGoal(); err != nil || s.GetGoal().Status != "" {
		t.Fatalf("ClearGoal = %v, cache = %#v", err, s.GetGoal())
	}
	if got := methods(); len(got) != 2 || got[0] != "thread/goal/set" || got[1] != "thread/goal/clear" {
		t.Fatalf("methods = %v", got)
	}
}

// The achieved notice must wait for the running turn's result; an active goal
// found after resume must be cleared and announced on the next turn.
func TestGoalNoticeDeferredAndResumeClears(t *testing.T) {
	s, methods := fakeGoalSession(t, map[string]string{
		"thread/goal/get":   `{"goal":{"threadId":"thread-1","objective":"old goal","status":"active","tokensUsed":7}}`,
		"thread/goal/clear": `{"cleared":true}`,
	})

	s.stateMu.Lock()
	s.currentTurn = "turn-1"
	s.stateMu.Unlock()
	s.handleNotification("thread/goal/updated", json.RawMessage(
		`{"threadId":"thread-1","goal":{"threadId":"thread-1","objective":"ship it","status":"complete","tokensUsed":10019}}`))
	if evs := drainEvents(s); len(evs) != 0 {
		t.Fatalf("notice emitted mid-turn: %#v", evs)
	}
	if g := s.GetGoal(); g.Status != "complete" || g.TokensUsed != 10019 {
		t.Fatalf("cache = %#v", g)
	}
	s.completeTurn()
	if evs := drainEvents(s); len(evs) != 2 || evs[0].Type != core.EventResult || evs[1].Type != core.EventGoal {
		t.Fatalf("events = %#v, want result then goal notice", evs)
	}

	// A stale notification with no turn running is announced immediately.
	s.handleNotification("thread/goal/updated", json.RawMessage(
		`{"threadId":"thread-1","goal":{"threadId":"thread-1","objective":"ship it","status":"paused","tokensUsed":9}}`))
	if evs := drainEvents(s); len(evs) != 1 || evs[0].Type != core.EventGoal {
		t.Fatalf("idle notice = %#v", evs)
	}

	s.syncGoalAfterResume()
	if got := methods(); len(got) != 2 || got[0] != "thread/goal/get" || got[1] != "thread/goal/clear" {
		t.Fatalf("methods = %v", got)
	}
	if g := s.GetGoal(); g.Status != "" {
		t.Fatalf("active goal survived resume: %#v", g)
	}
	s.flushGoalNotice()
	if evs := drainEvents(s); len(evs) != 1 || evs[0].Type != core.EventGoal {
		t.Fatalf("resume notice = %#v", evs)
	}
}

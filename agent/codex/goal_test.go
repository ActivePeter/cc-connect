package codex

import (
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

// fakeGoalSession answers JSON-RPC requests from a canned map keyed by method
// and records the methods it saw.
func fakeGoalSession(t *testing.T, replies map[string]string, observe ...func(map[string]any)) (*appServerSession, func() []string) {
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
			if len(observe) > 0 {
				observe[0](req)
			}
			mu.Lock()
			methods = append(methods, method)
			mu.Unlock()
			res := replies[method]
			if res == "" {
				res = "{}"
			}
			s.handleResponse(rpcResponseEnvelope{ID: req["id"], Result: json.RawMessage(res)})
			if method == "thread/goal/clear" {
				if len(observe) > 0 {
					observe[0](map[string]any{"method": "after-clear-response"})
				}
				if res == `{"cleared":true}` && replies["test/manual-notice"] != "true" {
					s.handleNotification("thread/goal/cleared", json.RawMessage(`{"threadId":"thread-1"}`))
				}
			}
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
	requests := make(chan map[string]any, 1)
	s, methods := fakeGoalSession(t, map[string]string{
		"thread/goal/set":   `{"goal":{"threadId":"thread-1","objective":"make tests pass","status":"active","tokensUsed":0}}`,
		"thread/goal/clear": `{"cleared":true}`,
	}, func(req map[string]any) {
		if req["method"] == "thread/goal/set" {
			requests <- req["params"].(map[string]any)
		}
	})
	info, err := s.SetGoal("make tests pass")
	if err != nil || !info.Active() || s.GetGoal().Objective != "make tests pass" {
		t.Fatalf("SetGoal = %#v, %v", info, err)
	}
	if params := <-requests; params["status"] != "active" {
		t.Fatalf("goal/set params = %v", params)
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
	s.flushGoalNotice(true)
	if evs := drainEvents(s); len(evs) != 1 || evs[0].Type != core.EventGoal {
		t.Fatalf("resume notice = %#v", evs)
	}
}

func TestGoalResumeNoticeConsumed(t *testing.T) {
	s, _ := fakeGoalSession(t, map[string]string{
		"thread/goal/get":   `{"goal":{"threadId":"thread-1","objective":"old","status":"active"}}`,
		"thread/goal/clear": `{"cleared":true}`,
	})
	s.syncGoalAfterResume()
	s.stateMu.Lock()
	s.currentTurn = "old-turn"
	s.stateMu.Unlock()
	s.handleGoalUpdated(threadGoal{Objective: "other", Status: "paused"})
	s.completeTurn()
	if got := drainEvents(s); len(got) != 2 || got[0].Type != core.EventResult || got[1].Type != core.EventGoal {
		t.Fatalf("terminal notice should not consume resume notice: %#v", got)
	}
	if got := s.GoalResumeNotice(); got != "old" {
		t.Fatalf("notice = %q", got)
	}
	s.AckGoalResumeNotice("old")
	s.flushGoalNotice(false)
	if got := drainEvents(s); len(got) != 0 {
		t.Fatalf("duplicate notice: %#v", got)
	}
}

func TestGoalResumeReportsLookupFailure(t *testing.T) {
	s, _ := fakeGoalSession(t, map[string]string{"thread/goal/get": `{"goal":42}`})
	if err := s.syncGoalAfterResume(); err == nil {
		t.Fatal("goal lookup failure was ignored")
	}
}

func TestGoalSetDoesNotWaitForEarlierClearNotification(t *testing.T) {
	s, _ := fakeGoalSession(t, map[string]string{
		"thread/goal/clear":  `{"cleared":true}`,
		"thread/goal/set":    `{"goal":{"threadId":"thread-1","objective":"new","status":"active"}}`,
		"test/manual-notice": "true",
	})
	s.storeGoal(threadGoal{Objective: "old", Status: "active"})
	if err := s.ClearGoal(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := s.SetGoal("new"); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("set waited for a clear notification")
	}
	s.handleNotification("thread/goal/cleared", json.RawMessage(`{"threadId":"other"}`))
	s.handleNotification("thread/goal/cleared", json.RawMessage(`{"threadId":"thread-1"}`))
	if got := s.GetGoal(); !got.Active() || got.Objective != "new" {
		t.Fatal(got)
	}
}

func TestGoalClearFalseDoesNotDelaySet(t *testing.T) {
	s, _ := fakeGoalSession(t, map[string]string{
		"thread/goal/clear": `{"cleared":false}`,
		"thread/goal/set":   `{"goal":{"threadId":"thread-1","objective":"new","status":"active"}}`,
	})
	s.storeGoal(threadGoal{Objective: "old", Status: "active"})
	if err := s.ClearGoal(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := s.SetGoal("new"); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("set waited after cleared=false")
	}
	if got := s.GetGoal(); !got.Active() || got.Objective != "new" {
		t.Fatal(got)
	}
}

func TestGoalClearNotificationBeforeResponseProcessing(t *testing.T) {
	var s *appServerSession
	s, _ = fakeGoalSession(t, map[string]string{
		"thread/goal/clear":  `{"cleared":true}`,
		"thread/goal/set":    `{"goal":{"threadId":"thread-1","objective":"new","status":"active"}}`,
		"test/manual-notice": "true",
	}, func(req map[string]any) {
		if req["method"] == "thread/goal/clear" {
			s.handleNotification("thread/goal/cleared", json.RawMessage(`{"threadId":"thread-1"}`))
		}
	})
	if err := s.ClearGoal(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetGoal("new"); err != nil {
		t.Fatal(err)
	}
	// An unrelated later clear must not be mistaken for the earlier notice.
	s.handleNotification("thread/goal/cleared", json.RawMessage(`{"threadId":"thread-1"}`))
	if got := s.GetGoal(); got.Status != "" {
		t.Fatal(got)
	}
}

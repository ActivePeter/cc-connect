package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

type deferredRPC struct {
	mu             sync.Mutex
	methods, texts []string
	reject         map[string]bool
	failMethod     string
	holdMethod     string
	holdStarted    chan struct{}
	releaseReply   chan struct{}
}

func newDeferredRPC(t *testing.T) (*appServerSession, *deferredRPC) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	s := &appServerSession{ctx: ctx, cancel: cancel, workDir: t.TempDir(), events: make(chan core.Event, 128), goalObjective: "original", goalStatus: "active", currentTurn: "turn-1", turnSequence: 1}
	s.alive.Store(true)
	s.threadID.Store("thread-1")
	r := &deferredRPC{reject: map[string]bool{}}
	s.stdin = &steerRPCWriter{write: func(b []byte) {
		var q struct {
			ID     int64  `json:"id"`
			Method string `json:"method"`
			Params struct {
				Expected  string                              `json:"expectedTurnId"`
				Objective string                              `json:"objective"`
				Input     []struct{ Type, Text, Path string } `json:"input"`
			} `json:"params"`
		}
		if err := json.Unmarshal(b, &q); err != nil {
			panic(err)
		}
		r.mu.Lock()
		r.methods = append(r.methods, q.Method)
		text := ""
		if len(q.Params.Input) > 0 {
			text = q.Params.Input[0].Text
		}
		r.texts = append(r.texts, text)
		rejected := r.reject[q.Params.Expected] || q.Method == r.failMethod
		r.mu.Unlock()
		if rejected {
			s.handleResponse(rpcResponseEnvelope{ID: q.ID, Error: &rpcError{Message: "cannot steer this turn"}})
			return
		}
		result := `{}`
		if q.Method == "turn/start" {
			result = `{"turn":{"id":"sent-turn"}}`
		}
		if q.Method == "thread/goal/set" {
			b, _ := json.Marshal(threadGoalSetResponse{Goal: threadGoal{Objective: q.Params.Objective, Status: "active"}})
			result = string(b)
		}
		response := rpcResponseEnvelope{ID: q.ID, Result: json.RawMessage(result)}
		if q.Method == r.holdMethod {
			close(r.holdStarted)
			go func() {
				<-r.releaseReply
				s.handleResponse(response)
			}()
			return
		}
		s.handleResponse(response)
	}}
	t.Cleanup(func() { _ = s.Close() })
	return s, r
}
func (r *deferredRPC) snapshot() ([]string, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.methods...), append([]string(nil), r.texts...)
}
func deferredNotify(out chan string) func(string, error) {
	return func(status string, err error) {
		if err != nil {
			out <- fmt.Sprint(err)
		} else {
			out <- status
		}
	}
}
func awaitDeferred(t *testing.T, out chan string, want string) {
	t.Helper()
	select {
	case got := <-out:
		if got != want {
			t.Fatalf("got %s want %s", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deferred message not delivered")
	}
}
func startDeferredTurn(s *appServerSession, id string) {
	b, _ := json.Marshal(map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": id}})
	s.handleNotification("turn/started", b)
}
func TestDeferredQueue_GapAndFIFO(t *testing.T) {
	s, r := newDeferredRPC(t)
	s.currentTurn = ""
	out := make(chan string, 4)
	for _, text := range []string{"first", "second"} {
		if err := s.EnqueueMidTurn(core.DeferredMessage{Prompt: text, GoalObjective: text, MessageID: text, Attempted: true, Notify: deferredNotify(out)}); err != nil {
			t.Fatal(err)
		}
	}
	if methods, _ := r.snapshot(); len(methods) != 0 {
		t.Fatal(methods)
	}
	startDeferredTurn(s, "turn-2")
	awaitDeferred(t, out, core.DeferredInserted)
	awaitDeferred(t, out, core.DeferredInserted)
	methods, texts := r.snapshot()
	if strings.Join(methods, ",") != "turn/steer,turn/steer" || strings.Join(texts, ",") != "first,second" {
		t.Fatalf("%v %v", methods, texts)
	}
}
func TestDeferredQueue_RejectedReviewCompactOrExpiredTurnWaits(t *testing.T) {
	for _, kind := range []string{"review", "compact", "expired"} {
		t.Run(kind, func(t *testing.T) {
			s, r := newDeferredRPC(t)
			r.reject["blocked-turn"] = true
			out := make(chan string, 2)
			_ = s.EnqueueMidTurn(core.DeferredMessage{Prompt: "marker", Attempted: true, Notify: deferredNotify(out)})
			startDeferredTurn(s, "blocked-turn")
			if kind != "expired" {
				s.stateMu.Lock()
				s.turnUnsteerable = true
				s.stateMu.Unlock()
			}
			// A known review/compact is skipped; an early attempt before its kind is
			// announced is rejected by app-server. Neither can deliver the message.
			s.signalDeferredMessages()
			select {
			case got := <-out:
				t.Fatalf("delivered to blocked turn: %s", got)
			case <-time.After(30 * time.Millisecond):
			}
			startDeferredTurn(s, "normal-turn")
			awaitDeferred(t, out, core.DeferredInserted)
			if s.PendingMidTurnMessages() != 0 {
				t.Fatal("queue not drained")
			}
		})
	}
}
func TestDeferredQueue_TerminalGoalMergesNewInput(t *testing.T) {
	for _, always := range []bool{false, true} {
		t.Run(fmt.Sprint(always), func(t *testing.T) {
			s, r := newDeferredRPC(t)
			out := make(chan string, 4)
			for _, text := range []string{"one", "two"} {
				_ = s.EnqueueMidTurn(core.DeferredMessage{Prompt: text, GoalObjective: text, Attempted: true, AsGoal: always, Notify: deferredNotify(out)})
			}
			s.completeTurn()
			s.handleGoalUpdated(threadGoal{Objective: "original", Status: "complete"})
			awaitDeferred(t, out, core.DeferredSent)
			awaitDeferred(t, out, core.DeferredSent)
			methods, texts := r.snapshot()
			want := "turn/start"
			if always {
				want = "thread/goal/set"
			}
			if len(methods) != 1 || methods[0] != want {
				t.Fatal(methods)
			}
			if !always && texts[0] != "one\n\ntwo" {
				t.Fatal(texts)
			}
			if always && s.GetGoal().Objective != "one\n\ntwo" {
				t.Fatal(s.GetGoal())
			}
		})
	}
}
func TestDeferredQueue_AttachmentsSentAfterTurn(t *testing.T) {
	s, r := newDeferredRPC(t)
	out := make(chan string, 2)
	_ = s.EnqueueMidTurn(core.DeferredMessage{Prompt: "read this", MessageID: "attachment", Files: []core.FileAttachment{{FileName: "note.txt", Data: []byte("content")}}, AsGoal: true, Notify: deferredNotify(out)})
	s.completeTurn()
	awaitDeferred(t, out, core.DeferredSent)
	methods, texts := r.snapshot()
	if len(methods) != 1 || methods[0] != "turn/start" || !strings.Contains(texts[0], "note.txt") {
		t.Fatalf("%v %v", methods, texts)
	}
	if s.GetGoal().Objective != "original" || !s.GetGoal().Active() {
		t.Fatal("attachment replaced goal")
	}
}
func TestDeferredQueue_ClearAndCloseLogCounts(t *testing.T) {
	s, _ := newDeferredRPC(t)
	out := make(chan string, 4)
	for i := 0; i < 2; i++ {
		_ = s.EnqueueMidTurn(core.DeferredMessage{Prompt: "pending", Attempted: true, Notify: deferredNotify(out)})
	}
	if n := s.ClearMidTurnMessages(); n != 2 {
		t.Fatalf("cleared %d", n)
	}
	awaitDeferred(t, out, core.DeferredCanceled)
	awaitDeferred(t, out, core.DeferredCanceled)
	for i := 0; i < 2; i++ {
		_ = s.EnqueueMidTurn(core.DeferredMessage{Prompt: "lost on close", Attempted: true})
	}
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	_ = s.Close()
	slog.SetDefault(old)
	if !strings.Contains(buf.String(), "count=2") || !strings.Contains(buf.String(), "session close/restart") {
		t.Fatal(buf.String())
	}
}

func TestDeferredQueue_FailedFallbackReportsEveryMessage(t *testing.T) {
	s, r := newDeferredRPC(t)
	r.failMethod = "turn/start"
	out := make(chan string, 4)
	for _, id := range []string{"one", "two"} {
		_ = s.EnqueueMidTurn(core.DeferredMessage{Prompt: id, MessageID: id, Attempted: true, Notify: deferredNotify(out)})
	}
	s.completeTurn()
	s.handleGoalUpdated(threadGoal{Objective: "original", Status: "complete"})
	awaitDeferred(t, out, "codex app-server turn/start: cannot steer this turn")
	awaitDeferred(t, out, "codex app-server turn/start: cannot steer this turn")
	if s.PendingMidTurnMessages() != 0 {
		t.Fatal("failed fallback remained hung")
	}
}

// A terminal fallback can be setting a fresh goal when /goal clear arrives.
// Clear must finish after that RPC, so its result cannot resurrect the goal.
func TestDeferredQueue_ClearWaitsForInFlightFallback(t *testing.T) {
	s, r := newDeferredRPC(t)
	s.currentTurn = ""
	s.goalStatus = "complete"
	r.holdMethod = "thread/goal/set"
	r.holdStarted = make(chan struct{})
	r.releaseReply = make(chan struct{})
	out := make(chan string, 1)
	if err := s.EnqueueMidTurn(core.DeferredMessage{Prompt: "new input", GoalObjective: "new input", AsGoal: true, Notify: deferredNotify(out)}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.holdStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("fallback did not start")
	}
	clearDone := make(chan error, 1)
	go func() { clearDone <- s.ClearGoal() }()
	select {
	case err := <-clearDone:
		close(r.releaseReply)
		select {
		case <-out:
		case <-time.After(2 * time.Second):
			t.Fatal("in-flight fallback did not settle")
		}
		t.Fatalf("clear overtook in-flight fallback: %v; goal=%+v", err, s.GetGoal())
	case <-time.After(30 * time.Millisecond):
	}
	close(r.releaseReply)
	select {
	case err := <-clearDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("clear did not finish")
	}
	awaitDeferred(t, out, core.DeferredSent)
	if got := s.GetGoal(); got.Objective != "" || got.Active() {
		t.Fatalf("canceled goal reappeared: %+v", got)
	}
	methods, _ := r.snapshot()
	if strings.Join(methods, ",") != "thread/goal/set,thread/goal/clear" {
		t.Fatal(methods)
	}
}

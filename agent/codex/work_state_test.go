package codex

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

type observedWorkState struct {
	alive          bool
	running        bool
	goal           string
	pending        int
	activity       time.Time
	deliveryLocked bool
}

func observeWorkState(s *appServerSession) observedWorkState {
	return observedWorkState{
		alive: s.Alive(), running: s.GoalTurnRunning(), goal: s.GetGoal().Status,
		pending: s.PendingMidTurnMessages(), activity: s.LastNativeTurnActivity(),
	}
}

func awaitWorkState(t *testing.T, updates <-chan observedWorkState, match func(observedWorkState) bool) observedWorkState {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case got := <-updates:
			if match(got) {
				return got
			}
		case <-timer.C:
			t.Fatal("work-state callback did not publish the expected state")
			return observedWorkState{}
		}
	}
}

func TestWorkStateCallbackTurnAndGoalReadback(t *testing.T) {
	s, _ := fakeGoalSession(t, map[string]string{
		"thread/goal/set":    "{\"goal\":{\"threadId\":\"thread-1\",\"objective\":\"new goal\",\"status\":\"active\"}}",
		"thread/goal/clear":  "{\"cleared\":true}",
		"test/manual-notice": "true",
	})
	updates := make(chan observedWorkState, 16)
	s.SetWorkStateCallback(func() { updates <- observeWorkState(s) })
	select {
	case <-updates:
		t.Fatal("registration must not synchronously call back")
	default:
	}
	start := time.Now()
	s.handleNotification("turn/started", json.RawMessage("{\"turn\":{\"id\":\"native-1\"}}"))
	started := awaitWorkState(t, updates, func(g observedWorkState) bool { return g.running })
	if started.activity.Before(start) {
		t.Fatalf("start activity was not recorded: %v", started.activity)
	}
	s.completeTurn()
	completed := awaitWorkState(t, updates, func(g observedWorkState) bool { return !g.running })
	if completed.activity.Before(started.activity) {
		t.Fatalf("completion activity moved backwards: %v < %v", completed.activity, started.activity)
	}
	if _, err := s.SetGoal("new goal"); err != nil {
		t.Fatal(err)
	}
	active := awaitWorkState(t, updates, func(g observedWorkState) bool { return g.goal == "active" })
	if active.activity.Before(completed.activity) {
		t.Fatalf("new goal kept a stale idle deadline: %v < %v", active.activity, completed.activity)
	}
	s.handleNotification("thread/goal/updated", json.RawMessage("{\"goal\":{\"objective\":\"new goal\",\"status\":\"complete\"}}"))
	awaitWorkState(t, updates, func(g observedWorkState) bool { return g.goal == "complete" })
	if err := s.ClearGoal(); err != nil {
		t.Fatal(err)
	}
	awaitWorkState(t, updates, func(g observedWorkState) bool { return g.goal == "" })
	// Exercise the independent notification path too, without an RPC clear.
	s.storeGoal(threadGoal{Objective: "other", Status: "active"})
	awaitWorkState(t, updates, func(g observedWorkState) bool { return g.goal == "active" })
	s.handleNotification("thread/goal/cleared", json.RawMessage("{\"threadId\":\"thread-1\"}"))
	awaitWorkState(t, updates, func(g observedWorkState) bool { return g.goal == "" })
}

func TestWorkStateCallbackDeferredDeliveryAfterLocks(t *testing.T) {
	for _, asGoal := range []bool{false, true} {
		t.Run(map[bool]string{false: "send", true: "set-goal"}[asGoal], func(t *testing.T) {
			s, _ := newDeferredRPC(t)
			s.currentTurn, s.goalStatus = "", ""
			// Own worker scheduling in this test. Enqueue still follows the real
			// queue path but no goroutine can consume the head before assertions.
			s.queueWake = make(chan struct{}, 1)
			updates := make(chan observedWorkState, 16)
			s.SetWorkStateCallback(func() {
				// Acquiring every lock through the public readers and the delivery
				// lock proves that deferred Send/SetGoal never calls back inside it.
				state := observeWorkState(s)
				if s.queueDeliveryMu.TryLock() {
					s.queueDeliveryMu.Unlock()
				} else {
					state.deliveryLocked = true
				}
				updates <- state
			})
			if err := s.EnqueueMidTurn(core.DeferredMessage{Prompt: "next", GoalObjective: "next", AsGoal: asGoal}); err != nil {
				t.Fatal(err)
			}
			awaitWorkState(t, updates, func(g observedWorkState) bool { return g.pending == 1 })
			done := make(chan struct{})
			go func() { s.deliverDeferredHead(); close(done) }()
			terminal := awaitWorkState(t, updates, func(g observedWorkState) bool { return g.pending == 0 })
			if terminal.deliveryLocked {
				t.Fatal("deferred callback ran while queueDeliveryMu was held")
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("deferred delivery callback held a backend lock")
			}
		})
	}
}

func TestWorkStateCallbackDeferredInFlightAndClear(t *testing.T) {
	s, r := newDeferredRPC(t)
	s.currentTurn, s.goalStatus = "", ""
	s.queueWake = make(chan struct{}, 1)
	r.holdMethod, r.holdStarted, r.releaseReply = "turn/start", make(chan struct{}), make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(r.releaseReply) }) })
	updates := make(chan observedWorkState, 16)
	s.SetWorkStateCallback(func() { updates <- observeWorkState(s) })
	if err := s.EnqueueMidTurn(core.DeferredMessage{Prompt: "in flight"}); err != nil {
		t.Fatal(err)
	}
	awaitWorkState(t, updates, func(g observedWorkState) bool { return g.pending == 1 })
	done := make(chan struct{})
	go func() { s.deliverDeferredHead(); close(done) }()
	select {
	case <-r.holdStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("deferred fallback did not start")
	}
	// The head has left messageQueue but must remain visible as work until
	// turn/start settles, so topic admission cannot release a slot in this gap.
	if pending := s.PendingMidTurnMessages(); pending != 1 {
		t.Fatalf("in-flight head disappeared from work state: %d", pending)
	}
	release.Do(func() { close(r.releaseReply) })
	awaitWorkState(t, updates, func(g observedWorkState) bool { return g.pending == 0 && g.running })
	<-done
	s.completeTurn()
	awaitWorkState(t, updates, func(g observedWorkState) bool { return !g.running && g.pending == 0 })
	if err := s.EnqueueMidTurn(core.DeferredMessage{Prompt: "cancel me"}); err != nil {
		t.Fatal(err)
	}
	awaitWorkState(t, updates, func(g observedWorkState) bool { return g.pending == 1 })
	if cleared := s.ClearMidTurnMessages(); cleared != 1 {
		t.Fatalf("clear removed %d heads", cleared)
	}
	awaitWorkState(t, updates, func(g observedWorkState) bool { return g.pending == 0 && !g.running })
}

func TestWorkStateCallbackNaturalExitAndClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := &appServerSession{ctx: ctx, cancel: cancel, events: make(chan core.Event, 4)}
	s.alive.Store(true)
	updates := make(chan observedWorkState, 4)
	s.SetWorkStateCallback(func() {
		state := observeWorkState(s)
		// The callback lock must be released before calling out, including on
		// the single RPC reader's natural EOF exit.
		s.SetWorkStateCallback(nil)
		updates <- state
	})
	s.wg.Add(1)
	go s.readLoop(strings.NewReader(""))
	awaitWorkState(t, updates, func(g observedWorkState) bool { return !g.alive })
	s.wg.Wait()
	// Reinstall to verify explicit close separately from natural EOF.
	s.SetWorkStateCallback(func() { updates <- observeWorkState(s) })
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	awaitWorkState(t, updates, func(g observedWorkState) bool { return !g.alive && g.pending == 0 })
	s.SetWorkStateCallback(nil)
	s.notifyWorkStateChanged() // Removing the observer leaves a no-op path.
}

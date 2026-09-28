package core

import (
	"testing"
	"time"
)

// TestCmdStop_ReleasesBusyLockForNextTurn pins the stop-path release that fixes
// the incident of 2026-09-28 (upstream #1830). A /stop landed while the turn's
// goroutine was wedged, so that turn never reached its own session.Unlock. The
// busy flag stayed set forever, every later message — including a second /stop
// — was queued into a session nobody drained, and the bot answered nothing but
// "📬 消息已收到" until cc-connect was restarted.
//
// The stub session implements no CancelTurn, so this drives the path the
// incident actually took: the graceful stop times out and the agent process is
// closed. Before the fix the lock outlived the turn; the assertions below are
// the two properties that were violated in production.
func TestCmdStop_ReleasesBusyLockForNextTurn(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	key := "test:user1"

	session := e.sessions.GetOrCreateActive(key)
	wedgedGen, locked := session.TryLock()
	if !locked {
		t.Fatal("could not take the busy lock for the in-flight turn")
	}

	// The wedged turn: a live agent session whose Close blocks, mirroring a turn
	// stuck in an unbounded wait when /stop arrives.
	sess := newBlockingCloseSession("stop-busy-lock")
	state := &interactiveState{
		agentSession: sess,
		platform:     p,
		replyCtx:     "ctx",
		busySession:  session, // recorded by getOrCreateInteractiveStateWith
	}
	e.interactiveMu.Lock()
	e.interactiveStates[key] = state
	e.interactiveMu.Unlock()

	e.cmdStop(p, &Message{SessionKey: key, ReplyCtx: "ctx"})

	select {
	case <-sess.closeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("expected the stop path to close the agent session")
	}

	// Property 1: /stop must not leave the session busy — that is what made
	// every subsequent message queue forever.
	if session.Busy() {
		t.Fatal("session is still busy after /stop: the next message would queue forever")
	}

	// A later message must be able to start a fresh turn.
	nextGen, ok := session.TryLock()
	if !ok {
		t.Fatal("the next turn could not take the busy lock after /stop")
	}
	if nextGen == wedgedGen {
		t.Fatalf("lock generation did not advance across /stop: still %d", nextGen)
	}

	// Property 2: if the wedged turn ever unsticks, its late Unlock carries the
	// pre-stop generation and must not clear the new turn's lock.
	session.Unlock(wedgedGen)
	if !session.Busy() {
		t.Fatal("a late Unlock from the interrupted turn cleared the new turn's lock")
	}
	session.Unlock(nextGen)
	if session.Busy() {
		t.Fatal("the new turn's own Unlock did not release the lock")
	}

	close(sess.releaseClose)
	select {
	case <-sess.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish after release")
	}
}

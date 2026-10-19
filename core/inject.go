package core

import (
	"errors"
	"fmt"
	"log/slog"
)

// Inject results.
const (
	InjectInjected = "injected" // written to the running agent's stdin mid-turn
	InjectIdle     = "idle"     // no foreground turn: deliver through the normal path instead
)

// ErrInjectUnsupported is returned when the session's agent cannot take a
// message in the middle of a turn.
var ErrInjectUnsupported = errors.New("agent does not support mid-turn injection")

// InjectToBusySession writes prompt straight into the agent of a session whose
// foreground turn is running, instead of queueing it until the turn ends
// (queueMessageForBusySession) or rejecting it (timer/cron "is busy").
//
// Claude Code (stream-json input) reads a user message that arrives during a
// turn at its next tool boundary and folds it into the same turn: the turn
// still ends with a single result, so the foreground event loop is not left
// waiting for a second one. If the turn ends between the busy check and the
// write, the message starts a turn of its own that the unsolicited reader
// relays like any between-turn output.
//
// When no foreground turn is running it returns InjectIdle and writes
// nothing; the caller then delivers the message through the normal path
// (a timer), which starts a turn properly.
func (e *Engine) InjectToBusySession(sessionKey, prompt string) (string, error) {
	if sessionKey == "" || prompt == "" {
		return "", fmt.Errorf("session_key and message are required")
	}
	_, sessions := e.sessionContextForKey(sessionKey)
	session := sessions.GetOrCreateActive(sessionKey)
	if session.TryLock() {
		session.UnlockWithoutUpdate()
		return InjectIdle, nil
	}

	iKey := e.interactiveKeyForSessionKey(sessionKey)
	e.interactiveMu.Lock()
	state, ok := e.interactiveStates[iKey]
	if !ok || state == nil {
		if found := findInteractiveKeyInStatesLocked(e.interactiveStates, sessionKey); found != "" {
			iKey = found
			state = e.interactiveStates[found]
		}
	}
	e.interactiveMu.Unlock()
	if state == nil {
		return InjectIdle, nil
	}

	state.mu.Lock()
	agentSession := state.agentSession
	agent := state.agent
	state.mu.Unlock()
	if agentSession == nil || !agentSession.Alive() {
		// Busy but the agent is still starting (or just died): nothing to
		// write into yet. Report idle so the caller retries through the
		// normal path, which queues or starts a turn.
		return InjectIdle, nil
	}
	if agent != nil && agent.Name() != "claudecode" {
		return "", ErrInjectUnsupported
	}
	if err := agentSession.Send(prompt, "", nil, nil); err != nil {
		return "", fmt.Errorf("inject: %w", err)
	}
	slog.Info("message injected into running turn", "session", sessionKey, "interactive_key", iKey, "len", len(prompt))
	return InjectInjected, nil
}

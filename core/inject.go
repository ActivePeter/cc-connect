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
// When neither a foreground nor an autonomous goal turn is running it returns
// InjectIdle and writes nothing; the caller then delivers through the normal path
// (a timer), which starts a turn properly.
func (e *Engine) InjectToBusySession(sessionKey, prompt string) (string, error) {
	if sessionKey == "" || prompt == "" {
		return "", fmt.Errorf("session_key and message are required")
	}
	_, sessions := e.sessionContextForKey(sessionKey)
	session := sessions.GetOrCreateActive(sessionKey)
	foregroundBusy := !session.TryLock()
	if !foregroundBusy {
		session.UnlockWithoutUpdate()
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
	if !foregroundBusy && !activeGoalTurnRunning(state.agentSession) {
		state.mu.Unlock()
		return InjectIdle, nil
	}
	status, err := injectIntoStateLocked(state, prompt, "", nil, nil)
	state.mu.Unlock()
	if err != nil || status != InjectInjected {
		return status, err
	}
	slog.Info("message injected into running turn", "session", sessionKey, "interactive_key", iKey, "len", len(prompt))
	return InjectInjected, nil
}

// injectIntoStateLocked shares the mid-turn transport between API notifications
// and platform messages. state.mu excludes terminal-result handling and the
// first Send, so an interjection cannot overtake the turn's initial prompt.
func injectIntoStateLocked(state *interactiveState, prompt, messageID string, images []ImageAttachment, files []FileAttachment) (string, error) {
	if state.stopped || state.agentSession == nil || !state.agentSession.Alive() {
		return InjectIdle, nil
	}
	sender, ok := state.agentSession.(MidTurnMessageSender)
	if !ok {
		return "", ErrInjectUnsupported
	}
	if !state.midTurnReady && !activeGoalTurnRunning(state.agentSession) {
		return InjectIdle, nil
	}
	if err := sender.SendMidTurn(prompt, messageID, images, files); err != nil {
		return "", fmt.Errorf("inject: %w", err)
	}
	return InjectInjected, nil
}

// activeGoalTurnRunning distinguishes an autonomous goal turn from idle or
// foreground startup, without enabling injection before an ordinary first Send.
func activeGoalTurnRunning(as AgentSession) bool {
	gc, hasGoal := as.(GoalController)
	running, hasRunning := as.(GoalTurnRunning)
	return hasGoal && hasRunning && gc.GetGoal().Active() && running.GoalTurnRunning()
}

// sendForegroundMessage arms injection only after the initial input has been
// written. Agents whose Send blocks for an entire turn never take this lock.
func sendForegroundMessage(state *interactiveState, as AgentSession, prompt, messageID string, images []ImageAttachment, files []FileAttachment) error {
	if _, ok := as.(MidTurnMessageSender); !ok {
		return as.Send(prompt, messageID, images, files)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.stopped || state.agentSession != as {
		return fmt.Errorf("agent session ended before send")
	}
	err := as.Send(prompt, messageID, images, files)
	state.midTurnReady = err == nil
	return err
}

// injectUserMessageForBusySession accepts a platform message into the current
// turn without adding a queued turn or waiting for an extra result. Callers
// retain the normal queue/retry path whenever delivery is unavailable or fails.
func (e *Engine) injectUserMessageForBusySession(p Platform, msg *Message, key string, session *Session, sessions *SessionManager) bool {
	if msg.ModeOverride != "" {
		return false // per-turn permission overrides need their own turn
	}
	e.interactiveMu.Lock()
	state := e.interactiveStates[key]
	if state == nil {
		e.interactiveMu.Unlock()
		return false
	}
	state.mu.Lock()
	e.interactiveMu.Unlock()
	// Do not overtake inputs queued during startup or after a failed write.
	if len(state.pendingMessages) > 0 {
		state.mu.Unlock()
		return false
	}
	if e.isStaleUserMessageLocked(state, msg.UserMessageTimeMs) {
		snap := userMessageWatermarkSnapshotLocked(state)
		state.mu.Unlock()
		e.logStaleUserMessageDropped("reject_before_inject", msg, key, snap)
		return true
	}
	prompt := e.buildSenderPrompt(msg.Content, msg.UserID, msg.UserName, msg.Platform, msg.SessionKey, msg.ChannelKey)
	status, err := injectIntoStateLocked(state, prompt, msg.MessageID, msg.Images, msg.Files)
	if err != nil || status != InjectInjected {
		state.mu.Unlock()
		if err != nil && !errors.Is(err, ErrInjectUnsupported) {
			slog.Warn("user message injection failed; falling back to queue", "session", msg.SessionKey, "msg_id", msg.MessageID, "error", err)
		}
		return false
	}
	session.AddHistory("user", msg.Content)
	session.TouchUserActivity()
	if msg.UserMessageTimeMs > state.currentTurnUserMessageTimeMs {
		state.currentTurnUserMessageTimeMs = msg.UserMessageTimeMs
	}
	state.mu.Unlock()
	sessions.Save()
	runMessageAccepted(msg)
	slog.Info("user message injected into running turn", "session", msg.SessionKey, "interactive_key", key, "msg_id", msg.MessageID)
	e.reply(p, msg.ReplyCtx, e.i18n.T(MsgMessageInjected))
	return true
}

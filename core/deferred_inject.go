package core

import (
	"fmt"
	"log/slog"
)

// handleDeferredMessage intercepts native Codex activity before always mode
// infers a new objective. A live ordinary turn is sufficient; no active goal
// is required. Startup still waits for a real turn or the initial Send.
func (e *Engine) handleDeferredMessage(p Platform, msg *Message, key string, sessions *SessionManager) bool {
	e.interactiveMu.Lock()
	state := e.interactiveStates[key]
	e.interactiveMu.Unlock()
	if state == nil {
		return false
	}
	// A failed direct steer must enter the FIFO before later arrivals can
	// decide whether to steer; otherwise a concurrent message can overtake it.
	state.deferredMu.Lock()
	defer state.deferredMu.Unlock()
	state.mu.Lock()
	as := state.agentSession
	queue, ok := as.(DeferredMessageSender)
	ready := state.midTurnReady
	backlog := len(state.pendingMessages)
	stopped := state.stopped
	state.mu.Unlock()
	if !ok || stopped || as == nil || !as.Alive() {
		return false
	}
	active := false
	if gc, ok := as.(GoalController); ok {
		active = gc.GetGoal().Active()
	}
	running := agentTurnRunning(as)
	if !running && !active && queue.PendingMidTurnMessages() == 0 && (!sessions.GetOrCreateActive(msg.SessionKey).Busy() || !ready) {
		return false
	}
	if backlog > 0 {
		// Keep startup/backlogged input ahead of this message. goalAuto is a
		// queue-dispatch marker here; the native FIFO decides Send vs SetGoal.
		msg.goalObjective, msg.goalAuto = true, true
		return e.queueMessageForBusySession(p, msg, key)
	}
	attempted := queue.PendingMidTurnMessages() == 0
	if attempted && len(msg.Images)+len(msg.Files) == 0 && e.injectUserMessageForBusySession(p, msg, key, sessions.GetOrCreateActive(msg.SessionKey), sessions) {
		return true
	}
	if msg.ModeOverride != "" {
		e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgError, "mid-turn permission override requires a separate turn"))
		return true
	}
	return e.enqueueDeferredMessage(p, msg, key, state, queue, sessions, attempted, false)
}

func (e *Engine) enqueueDeferredMessage(p Platform, msg *Message, key string, state *interactiveState, queue DeferredMessageSender, sessions *SessionManager, attempted, accepted bool) bool {
	ready := make(chan struct{})
	defer close(ready)
	replyCtx, id := msg.ReplyCtx, msg.MessageID
	notify := func(status string, err error) {
		if status == DeferredCanceled {
			slog.Info("queued user message canceled", "session", msg.SessionKey, "msg_id", id)
			return
		}
		<-ready // the initial queued acknowledgement/history must precede delivery
		if err != nil {
			e.reply(p, replyCtx, e.i18n.Tf(MsgError, fmt.Sprintf("message %s: %v", id, err)))
			return
		}
		if status == DeferredInserted {
			e.reply(p, replyCtx, e.i18n.T(MsgMessageInjected))
		} else {
			e.reply(p, replyCtx, e.i18n.T(MsgDeferredMessageSent))
		}
	}
	prompt := e.buildSenderPrompt(msg.Content, msg.UserID, msg.UserName, msg.Platform, msg.SessionKey, msg.ChannelKey)
	if err := queue.EnqueueMidTurn(DeferredMessage{Prompt: prompt, GoalObjective: msg.Content, MessageID: id, Images: msg.Images, Files: msg.Files, AsGoal: e.goalModeAlways, Attempted: attempted, Notify: notify}); err != nil {
		e.reply(p, replyCtx, e.i18n.Tf(MsgError, fmt.Sprintf("message %s: %v", id, err)))
		return true
	}
	if !accepted {
		session := sessions.GetOrCreateActive(msg.SessionKey)
		session.AddHistory("user", msg.Content)
		session.TouchUserActivity()
		sessions.Save()
		state.mu.Lock()
		if msg.UserMessageTimeMs > state.currentTurnUserMessageTimeMs {
			state.currentTurnUserMessageTimeMs = msg.UserMessageTimeMs
		}
		state.mu.Unlock()
		runMessageAccepted(msg)
	}
	if len(msg.Images)+len(msg.Files) > 0 {
		e.reply(p, replyCtx, e.i18n.T(MsgDeferredAttachmentQueued))
	} else {
		e.reply(p, replyCtx, e.i18n.T(MsgDeferredMessageQueued))
	}
	return true
}

func (e *Engine) clearDeferredMessageQueue(key string) int {
	e.interactiveMu.Lock()
	state := e.interactiveStates[key]
	e.interactiveMu.Unlock()
	if state == nil {
		return 0
	}
	state.deferredMu.Lock()
	defer state.deferredMu.Unlock()
	state.mu.Lock()
	as := state.agentSession
	queue, ok := as.(DeferredMessageSender)
	n := 0
	if ok {
		n = len(state.pendingMessages)
		state.pendingMessages = nil
	}
	state.mu.Unlock()
	if ok {
		if n > 0 {
			slog.Info("foreground queued messages cleared", "session", key, "count", n)
		}
		return n + queue.ClearMidTurnMessages()
	}
	return 0
}

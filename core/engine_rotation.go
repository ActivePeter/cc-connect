package core

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type rotationTurnOutcome struct{ Complete, Delivered bool }

// FreshSessionValidator rejects CLI arguments that could override a fresh start.
type FreshSessionValidator interface{ ValidateFreshSession() error }

type RotateRequest struct {
	Project           string `json:"project"`
	SessionKey        string `json:"session_key"`
	ExpectedSessionID string `json:"expected_session_id"`
	ExpectedScope     string `json:"expected_scope,omitempty"`
	RequestID         string `json:"request_id"`
	Name              string `json:"name,omitempty"`
	Handoff           string `json:"handoff"`
}

func rotationSessionKey(key string) string {
	// Workspace interactive keys use an absolute path prefix; platform keys do not.
	if strings.HasPrefix(key, "/") {
		if i := strings.Index(key, ":"); i >= 0 {
			return key[i+1:]
		}
	}
	return key
}

func (e *Engine) injectRotationEnv(agent Agent, key, logicalID, scope string) {
	inj, ok := agent.(SessionEnvInjector)
	if !ok {
		return
	}
	env := []string{"CC_PROJECT=" + e.name, "CC_SESSION_KEY=" + key, "CC_SESSION_ID=" + logicalID, "CC_SESSION_SCOPE=" + scope}
	if e.dataDir != "" {
		env = append(env, "CC_DATA_DIR="+e.dataDir)
	}
	if exe, err := os.Executable(); err == nil {
		env = append(env, "PATH="+filepath.Dir(exe)+string(filepath.ListSeparator)+os.Getenv("PATH"))
	}
	inj.SetSessionEnv(env)
}

func (e *Engine) ScheduleRotation(req RotateRequest) (RotationOperation, error) {
	if strings.TrimSpace(req.Handoff) == "" || len(req.Handoff) > 65536 || req.RequestID == "" || req.ExpectedSessionID == "" || req.SessionKey == "" {
		return RotationOperation{}, fmt.Errorf("session_key, expected_session_id, request_id and nonempty handoff (max 64 KiB) required")
	}
	agent, sm := e.sessionContextForKey(req.SessionKey)
	if req.ExpectedScope != "" && req.ExpectedScope != sm.RotationScope() {
		return RotationOperation{}, ErrRotationStale
	}
	if prior, err := sm.Rotation(req.RequestID); err == nil {
		if prior.SessionKey != req.SessionKey || prior.SourceID != req.ExpectedSessionID || prior.Name != req.Name || prior.Handoff != req.Handoff {
			return RotationOperation{}, ErrRotationStale
		}
		return prior, nil
	}
	old := sm.LookupActive(req.SessionKey)
	if old == nil {
		return RotationOperation{}, ErrRotationNotFound
	}
	target, err := e.resolveSendTarget(req.SessionKey, false)
	if err != nil {
		return RotationOperation{}, err
	}
	if target.platform == nil {
		return RotationOperation{}, fmt.Errorf("no reply route for session")
	}
	if v, ok := agent.(FreshSessionValidator); ok {
		if err := v.ValidateFreshSession(); err != nil {
			return RotationOperation{}, err
		}
	}
	e.rotationMu.Lock()
	if e.rotationLifecycle[req.SessionKey] > 0 {
		e.rotationMu.Unlock()
		return RotationOperation{}, ErrRotationStale
	}
	op, err := sm.PrepareRotation(req.SessionKey, req.ExpectedSessionID, req.RequestID, req.Name, req.Handoff)
	if err != nil || op.Status != "SCHEDULED" {
		e.rotationMu.Unlock()
		return op, err
	}
	locked := old.TryLock()
	e.rotationMu.Unlock()
	if !locked {
		return op, nil
	}
	key := e.interactiveKeyForSessionKey(req.SessionKey)
	return e.dispatchReservedRotation(old, sm, key, req.SessionKey, op, target)
}

func (e *Engine) dispatchReservedRotation(old *Session, sm *SessionManager, key, route string, op RotationOperation, target sendTarget) (RotationOperation, error) {
	defer e.releaseRotationReservation(old, sm, key)
	agent, _ := e.sessionContextForKey(route)
	e.interactiveMu.Lock()
	state := e.interactiveStates[key]
	if state == nil {
		state = &interactiveState{platform: target.platform, replyCtx: target.replyCtx, agent: agent, rotationRouteKey: route, logicalSessionID: old.ID}
		e.interactiveStates[key] = state
	}
	e.interactiveMu.Unlock()
	_, err := e.runLatestRotation(state, old, sm, key, route, op)
	updated, _ := sm.Rotation(op.RequestID)
	return updated, err
}

// Called only by the foreground owner after its event loop has returned.
func (e *Engine) finishScheduledRotation(state *interactiveState, old *Session, sm *SessionManager, key, workspace, ccKey string) bool {
	if ccKey == "" {
		ccKey = rotationSessionKey(key)
	}
	op, ok := sm.PendingRotation(ccKey, old.ID)
	if !ok {
		return false
	}
	state.mu.Lock()
	outcome := state.rotationOutcome
	p := state.platform
	rc := state.replyCtx
	state.mu.Unlock()
	if !outcome.Complete || !outcome.Delivered {
		reason := "turn or final reply delivery was not confirmed; old conversation retained"
		if err := sm.FinishRotation(op.RequestID, "FAILED", reason); err != nil {
			slog.Error("rotation failure receipt", "error", err)
		}
		_ = p.Send(e.ctx, rc, e.i18n.Tf(MsgRotationFailed, reason))
		return false
	}
	committed, err := e.runLatestRotation(state, old, sm, key, ccKey, op)

	if err != nil {
		slog.Error("session rotation failed", "request_id", op.RequestID, "error", err)
		_ = p.Send(e.ctx, rc, e.i18n.Tf(MsgRotationFailed, err))
	}
	return committed
}

func (e *Engine) performRotation(state *interactiveState, old *Session, sm *SessionManager, key, ccKey string, op RotationOperation) (bool, error) {
	e.rotationMu.Lock()
	if e.rotationWorkers == nil {
		e.rotationWorkers = make(map[string]bool)
	}
	if e.rotationWorkers[key] {
		e.rotationMu.Unlock()
		err := fmt.Errorf("another rotation is finalizing; source conversation retained")
		if saveErr := sm.FinishRotation(op.RequestID, "FAILED", err.Error()); saveErr != nil {
			slog.Error("rotation failure receipt", "error", saveErr)
		}
		return false, err
	}
	e.rotationWorkers[key] = true
	if e.rotationGeneration == nil {
		e.rotationGeneration = make(map[string]uint64)
	}
	generation := e.rotationGeneration[key]
	e.rotationMu.Unlock()
	defer func() { e.rotationMu.Lock(); delete(e.rotationWorkers, key); e.rotationMu.Unlock() }()
	fail := func(err error) (bool, error) {
		if saveErr := sm.FinishRotation(op.RequestID, "FAILED", err.Error()); saveErr != nil {
			slog.Error("rotation failure receipt", "error", saveErr)
		}
		e.rotationMu.Lock()
		if e.rotationGeneration[key] == generation && sm.LookupActive(ccKey) == old {
			e.interactiveMu.Lock()
			if e.interactiveStates[key] == state {
				state.mu.Lock()
				if state.agentSession == nil || !state.agentSession.Alive() {
					state.agentSession = nil
					state.rotationRecovery = true
					state.eventsNeedResync = true
				}
				state.mu.Unlock()
			}
			e.interactiveMu.Unlock()
		}
		e.rotationMu.Unlock()
		return false, err
	}
	e.stopUnsolicitedReader(state)
	state.mu.Lock()
	agent := state.agent
	p := state.platform
	rc := state.replyCtx
	workspace := state.workspaceDir
	oldAS := state.agentSession
	state.midTurnReady = false
	state.mu.Unlock()
	if agent == nil {
		agent, _ = e.sessionContextForKey(ccKey)
	}
	if e.ctx.Err() != nil {
		return fail(e.ctx.Err())
	}
	// Startup is preparatory: no input is sent and the old durable mapping stays intact.
	e.sessionSpawnMu.Lock()
	restoreActiveProviderFromSession(agent, old)
	if injector, ok := agent.(PlatformPromptInjector); ok {
		prompt := ""
		if provider, ok := p.(FormattingInstructionProvider); ok {
			prompt = provider.FormattingInstructions()
		}
		injector.SetPlatformPrompt(prompt)
	}
	e.injectRotationEnv(agent, ccKey, op.TargetID, sm.RotationScope())
	candidate, err := agent.StartSession(e.ctx, "")
	e.sessionSpawnMu.Unlock()
	if err != nil {
		return fail(fmt.Errorf("prepare fresh session: %w", err))
	}
	discard := func() {
		if err := candidate.Close(); err != nil {
			slog.Warn("close unused rotation candidate", "error", err)
		}
	}
	current, ok := sm.PendingRotation(ccKey, old.ID)
	if !ok || current.RequestID != op.RequestID {
		discard()
		return false, nil
	} // latest registration wins
	// Close the quiescent old process before publishing the candidate. The logical
	// source remains resumable if close/commit fails.
	if oldAS != nil && oldAS.Alive() {
		closed := make(chan error, 1)
		go func() { closed <- oldAS.Close() }()
		select {
		case err = <-closed:
		case <-time.After(30 * time.Second):
			err = fmt.Errorf("old session close timed out")
		case <-e.ctx.Done():
			err = e.ctx.Err()
		}
		if err != nil {
			discard()
			return fail(fmt.Errorf("close source session: %w", err))
		}
	}
	if oldAS != nil && state.logicalSessionID == old.ID {
		if latest := oldAS.CurrentSessionID(); latest != "" {
			old.SetAgentSessionID(latest, agent.Name())
		}
	}
	e.rotationMu.Lock()
	if e.rotationGeneration[key] != generation {
		e.rotationMu.Unlock()
		discard()
		return false, nil
	}
	fresh, err := sm.CommitRotation(op)
	if err != nil {
		e.rotationMu.Unlock()
		discard()
		return fail(err)
	}
	if id := candidate.CurrentSessionID(); id != "" {
		fresh.SetAgentSessionID(id, agent.Name())
		if op.Name != "" {
			sm.SetSessionName(id, op.Name)
		}
		if err := sm.SaveError(); err != nil {
			slog.Error("save prepared backend id", "error", err)
		}
	}
	// Keep the candidate locked until its handoff turn starts; migrate the FIFO
	// under the same locks used by the normal busy-message path.
	fresh.TryLock()
	newState := &interactiveState{agentSession: candidate, agent: agent, platform: p, replyCtx: rc, workspaceDir: workspace,
		eventsNeedResync: true, rotationCandidate: true, logicalSessionID: fresh.ID, rotationRouteKey: ccKey}
	e.interactiveMu.Lock()
	state.mu.Lock()
	newState.pendingMessages = state.pendingMessages
	newState.lastCompletedUserMessageTimeMs = state.lastCompletedUserMessageTimeMs
	state.pendingMessages = nil
	state.stopped = true
	state.agentSession = nil
	if state.agentSessionIdleCancel != nil {
		state.agentSessionIdleCancel()
		state.agentSessionIdleCancel = nil
	}
	state.mu.Unlock()
	e.interactiveStates[key] = newState
	e.interactiveMu.Unlock()
	e.rotationMu.Unlock()
	if err := p.Send(e.ctx, rc, e.i18n.Tf(MsgRotationSwitched, fresh.ID)); err != nil {
		slog.Warn("rotation visible notice failed", "request_id", op.RequestID, "error", err)
	}
	slog.Info("session rotation committed", "source", old.ID, "target", fresh.ID, "request_id", op.RequestID, "revision", op.Revision)
	go e.processInteractiveMessageWith(p, &Message{SessionKey: ccKey, Platform: p.Name(), Content: op.Handoff, ReplyCtx: rc, rotationState: newState}, fresh, agent, sm, key, workspace, ccKey)
	return true, nil
}

// cancelScheduledRotation is the lifecycle gate shared with commit/publication.
func (e *Engine) cancelScheduledRotation(key, reason string) {
	e.rotationMu.Lock()
	if e.rotationGeneration == nil {
		e.rotationGeneration = make(map[string]uint64)
	}
	e.rotationGeneration[key]++
	route := rotationSessionKey(key)
	e.interactiveMu.Lock()
	if state := e.interactiveStates[key]; state != nil {
		state.mu.Lock()
		if state.rotationRouteKey != "" {
			route = state.rotationRouteKey
		}
		state.mu.Unlock()
	}
	e.interactiveMu.Unlock()
	e.rotationMu.Unlock()
	_, sm := e.sessionContextForKey(route)
	if err := sm.CancelRotations(route, reason); err != nil {
		slog.Error("cancel session rotation", "error", err)
	}
}

// runLatestRotation serializes replacement requests even when the original call
// started while idle (there may be no later foreground turn to wake a retry).
func (e *Engine) runLatestRotation(state *interactiveState, old *Session, sm *SessionManager, key, ccKey string, op RotationOperation) (bool, error) {
	for {
		committed, err := e.performRotation(state, old, sm, key, ccKey, op)
		if committed || (err != nil && err != ErrRotationStale) {
			return committed, err
		}
		next, ok := sm.PendingRotation(ccKey, old.ID)
		if !ok || next.RequestID == op.RequestID {
			return false, err
		}
		op = next
		e.interactiveMu.Lock()
		if current := e.interactiveStates[key]; current != nil {
			state = current
		}
		e.interactiveMu.Unlock()
	}
}

// processRotationFailureQueue takes the caller's existing session reservation.
// It reuses the normal startup/reader/idle/queue path and never sends the failed
// replacement handoff to the source conversation.
func (e *Engine) processRotationFailureQueue(state *interactiveState, session *Session, sm *SessionManager, key string) bool {
	e.rotationMu.Lock()
	if sm.LookupActive(sm.KeyForSession(session.ID)) != session {
		e.rotationMu.Unlock()
		return false
	}
	state.mu.Lock()
	if state.stopped || len(state.pendingMessages) == 0 {
		state.mu.Unlock()
		e.rotationMu.Unlock()
		return false
	}
	q := state.pendingMessages[0]
	state.pendingMessages = state.pendingMessages[1:]
	state.rotationRecovery = false
	agent := state.agent
	workspace := state.workspaceDir
	state.mu.Unlock()
	e.rotationMu.Unlock()
	if agent == nil {
		agent, _ = e.sessionContextForKey(sm.KeyForSession(session.ID))
	}
	msg := &Message{Platform: q.msgPlatform, SessionKey: q.msgSessionKey, ChannelKey: q.channelKey, UserID: q.userID, UserName: q.userName,
		Content: q.content, MessageID: q.messageID, Images: q.images, Files: q.files, FromVoice: q.fromVoice, UserMessageTimeMs: q.userMessageTimeMs, ReplyCtx: q.replyCtx}
	if msg.SessionKey == "" {
		msg.SessionKey = sm.KeyForSession(session.ID)
	}
	e.processInteractiveMessageWith(q.platform, msg, session, agent, sm, key, workspace, msg.SessionKey)
	return true
}

// releaseRotationReservation closes the queue/unlock race for idle API callers
// and cancelled internal deliveries. Pending swaps retain priority over FIFO.
func (e *Engine) releaseRotationReservation(session *Session, sm *SessionManager, key string) {
	e.rotationMu.Lock()
	route := sm.KeyForSession(session.ID)
	if e.ctx.Err() != nil || sm.LookupActive(route) != session {
		session.Unlock()
		e.rotationMu.Unlock()
		return
	}
	op, pending := sm.PendingRotation(route, session.ID)
	e.interactiveMu.Lock()
	state := e.interactiveStates[key]
	hasQueue := false
	if state != nil {
		state.mu.Lock()
		hasQueue = !state.stopped && len(state.pendingMessages) > 0
		state.mu.Unlock()
	}
	if pending {
		e.interactiveMu.Unlock()
		e.rotationMu.Unlock()
		go func() {
			target, err := e.resolveSendTarget(route, false)
			if err != nil || target.platform == nil {
				reason := "reply route unavailable"
				if err != nil {
					reason = err.Error()
				}
				if saveErr := sm.FinishRotation(op.RequestID, "FAILED", reason); saveErr != nil {
					slog.Error("rotation failure receipt", "error", saveErr)
				}
				e.releaseRotationReservation(session, sm, key)
				return
			}
			if _, err := e.dispatchReservedRotation(session, sm, key, route, op, target); err != nil {
				slog.Error("deferred rotation failed", "error", err)
			}
		}()
		return
	}
	if hasQueue {
		e.interactiveMu.Unlock()
		e.rotationMu.Unlock()
		go func() {
			if !e.processRotationFailureQueue(state, session, sm, key) {
				e.releaseRotationReservation(session, sm, key)
			}
		}()
		return
	}
	session.Unlock()
	e.interactiveMu.Unlock()
	e.rotationMu.Unlock()
}

func (e *Engine) beginRotationLifecycle(route string) func() {
	e.rotationMu.Lock()
	if e.rotationLifecycle == nil {
		e.rotationLifecycle = make(map[string]int)
	}
	e.rotationLifecycle[route]++
	e.rotationMu.Unlock()
	e.cancelScheduledRotation(e.interactiveKeyForSessionKey(route), "manual lifecycle command")
	return func() { e.rotationMu.Lock(); e.rotationLifecycle[route]--; e.rotationMu.Unlock() }
}

func (e *Engine) rotationMessageKey(key string) string {
	e.interactiveMu.Lock()
	defer e.interactiveMu.Unlock()
	if state := e.interactiveStates[key]; state != nil {
		state.mu.Lock()
		defer state.mu.Unlock()
		if state.rotationRouteKey != "" {
			return state.rotationRouteKey
		}
	}
	return rotationSessionKey(key)
}

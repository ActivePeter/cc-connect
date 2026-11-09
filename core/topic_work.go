package core

import (
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// NativeGoalIdleTimeout is shared with the native deferred-input watchdog.
const NativeGoalIdleTimeout = 60 * time.Second

// WorkStateNotifier reports native activity which can outlive the foreground lock.
// Callbacks only signal a wake: they must never wait for engine locks or delivery.
type WorkStateNotifier interface{ SetWorkStateCallback(func()) }
type NativeTurnProgress interface{ NativeTurnProgress() (uint64, bool) }
type NativeTurnActivity interface{ LastNativeTurnActivity() time.Time }

type topicWorkWatcher struct {
	baseCompleted   uint64
	resultsAcked    atomic.Uint64
	signalMu        sync.Mutex
	as              AgentSession
	wake            chan struct{}
	running         atomic.Bool
	pendingDelivery atomic.Int64
}
type topicWork struct {
	frontBusy, nativeTurn, goalActive, deliveryPending bool
	deferred                                           int
	lastNative                                         time.Time
}

func (e *Engine) isTopicRoute(p Platform, msg *Message) bool {
	policy, ok := p.(TopicSessionPolicy)
	if !ok || msg == nil {
		return false
	}
	chat, thread, _ := policy.TopicSession(msg.ReplyCtx)
	return chat != "" && thread != ""
}
func (e *Engine) topicKnown(key string) bool {
	e.topicMu.Lock()
	defer e.topicMu.Unlock()
	for _, g := range e.topicGroups {
		if _, ok := g.known[key]; ok {
			return true
		}
	}
	return false
}
func (e *Engine) watchTopicWork(key string, state *interactiveState) {
	isTopic := e.topicKnown(key)
	state.mu.Lock()
	as := state.agentSession
	notifier, ok := as.(WorkStateNotifier)
	if !ok || (state.topicWork != nil && state.topicWork.as == as) {
		state.mu.Unlock()
		return
	}
	w := &topicWorkWatcher{as: as, wake: make(chan struct{}, 1)}
	if progress, ok := as.(NativeTurnProgress); ok {
		w.baseCompleted, _ = progress.NativeTurnProgress()
	}
	state.topicWork = w
	state.mu.Unlock()
	signal := func() {
		w.signalMu.Lock()
		defer w.signalMu.Unlock()
		if _, ok := as.(NativeTurnProgress); !ok {
			running := as.Alive() && agentTurnRunning(as)
			if running && !w.running.Swap(running) {
				w.pendingDelivery.Add(1)
			} else if !running {
				w.running.Store(false)
			}
		}

		select {
		case w.wake <- struct{}{}:
		default:
		}
	}
	notifier.SetWorkStateCallback(signal)
	signal()
	if !isTopic {
		return
	} // delivery tracking only; legacy routing remains untouched
	go func() {
		for {
			select {
			case <-e.ctx.Done():
				return
			case <-w.wake:
			}
			e.reactivateTopicNative(key)
			e.topicIdle(key)
			if !as.Alive() {
				return
			}
		}
	}()
}
func (e *Engine) topicDeliveryFinished(state *interactiveState) {
	state.mu.Lock()
	w := state.topicWork
	state.mu.Unlock()
	if w == nil {
		return
	}
	if progress, ok := w.as.(NativeTurnProgress); ok {
		completed, _ := progress.NativeTurnProgress()
		for {
			n := w.resultsAcked.Load()
			if completed <= w.baseCompleted+n || w.resultsAcked.CompareAndSwap(n, n+1) {
				break
			}
		}
	} else {
		for {
			n := w.pendingDelivery.Load()
			if n == 0 || w.pendingDelivery.CompareAndSwap(n, n-1) {
				break
			}
		}
	}

	select {
	case w.wake <- struct{}{}:
	default:
	}
}
func (e *Engine) topicWorkSnapshot(key string) topicWork {
	var out topicWork
	_, sm := e.sessionContextForKey(key)
	if s := sm.LookupActive(key); s != nil {
		out.frontBusy = s.Busy()
	}
	iKey := e.interactiveKeyForSessionKey(key)
	e.rotationMu.Lock()
	out.frontBusy = out.frontBusy || e.rotationWorkers[iKey]
	e.rotationMu.Unlock()
	e.interactiveMu.Lock()
	state := e.interactiveStates[iKey]
	e.interactiveMu.Unlock()
	if state == nil {
		return out
	}
	state.mu.Lock()
	as, w := state.agentSession, state.topicWork
	state.mu.Unlock()
	if as == nil || !as.Alive() {
		return out
	}
	out.nativeTurn = agentTurnRunning(as)
	if gc, ok := as.(GoalController); ok {
		out.goalActive = gc.GetGoal().Active()
	}
	if q, ok := as.(DeferredMessageSender); ok {
		out.deferred = q.PendingMidTurnMessages()
	}
	if a, ok := as.(NativeTurnActivity); ok {
		out.lastNative = a.LastNativeTurnActivity()
	}
	out.deliveryPending = w != nil && w.as == as && w.deliveryOutstanding()
	return out
}

// Only commands which cannot start a model turn bypass data admission. Unknown
// slash commands/skills and /goal objectives retain the normal FIFO path.
func isTopicControlCommand(text string) bool {
	parts := splitCommandArgs(strings.TrimSpace(text))
	if len(parts) == 0 || !strings.HasPrefix(parts[0], "/") {
		return false
	}
	id := matchPrefix(strings.ToLower(strings.TrimPrefix(parts[0], "/")), builtinCommands)
	switch id {
	case "new", "list", "switch", "name", "current", "status", "usage", "history", "allow", "model", "reasoning", "mode", "lang", "quiet", "provider", "memory", "stop", "cancel", "help", "version", "commands", "skills", "config", "doctor", "upgrade", "restart", "alias", "delete", "bind", "search", "show", "dir", "tts", "workspace", "whoami", "web", "ps", "diff":
		return true
	case "goal":
		return len(parts) == 1 || (len(parts) == 2 && (strings.EqualFold(parts[1], "clear") || strings.EqualFold(parts[1], "status")))
	}
	return false
}
func isTopicGoalSet(text string) bool {
	parts := splitCommandArgs(strings.TrimSpace(text))
	return len(parts) > 1 && strings.HasPrefix(parts[0], "/") && matchPrefix(strings.ToLower(strings.TrimPrefix(parts[0], "/")), builtinCommands) == "goal" && !isTopicControlCommand(text)
}
func (e *Engine) clearTopicQueuedGoals(key string) int {
	dropped := 0
	e.topicMu.Lock()
	for _, g := range e.topicGroups {
		if _, ok := g.known[key]; !ok {
			continue
		}
		if g.goalGeneration == nil {
			g.goalGeneration = make(map[string]uint64)
		}
		g.goalGeneration[key]++
		filter := func(items []topicEnvelope) []topicEnvelope {
			out := items[:0]
			for _, env := range items {
				if len(env.msg.Images) > 0 || len(env.msg.Files) > 0 || env.msg.Audio != nil || !isTopicGoalSet(env.msg.Content) {
					out = append(out, env)
				} else {
					dropped++
				}
			}
			return out
		}
		if items, ok := g.waiting[key]; ok {
			items = filter(items)
			if len(items) > 0 {
				g.waiting[key] = items
			} else {
				delete(g.waiting, key)
				for i, k := range g.order {
					if k == key {
						g.order = append(g.order[:i], g.order[i+1:]...)
						break
					}
				}
			}
		}
		if r := g.running[key]; r != nil {
			r.inbox = filter(r.inbox)
		}
	}
	e.topicMu.Unlock()
	return dropped
}

func (e *Engine) topicGoalCurrent(msg *Message) bool {
	if !msg.topicGoalSet {
		return true
	}
	e.topicMu.Lock()
	defer e.topicMu.Unlock()
	for _, g := range e.topicGroups {
		if _, ok := g.known[msg.SessionKey]; ok {
			return g.goalGeneration[msg.SessionKey] == msg.topicGoalGeneration
		}
	}
	return false
}

// A goal-only lease can time out without changing the native goal. If that
// process later reports work, account for its real activity before admitting
// more data. Native autonomous work cannot be postponed by the engine FIFO.
func (e *Engine) reactivateTopicNative(key string) {
	work := e.topicWorkSnapshot(key)
	if !work.nativeTurn && work.deferred == 0 && !work.deliveryPending {
		return
	}
	e.topicMu.Lock()
	for id, g := range e.topicGroups {
		if _, ok := g.known[key]; !ok || g.running[key] != nil || g.stopping || e.ctx.Err() != nil {
			continue
		}
		r := &topicRun{inbox: g.waiting[key]}
		delete(g.waiting, key)
		for i, k := range g.order {
			if k == key {
				g.order = append(g.order[:i], g.order[i+1:]...)
				break
			}
		}
		for _, env := range r.inbox {
			env.msg.topicRun = r
		}
		g.running[key] = r
		if len(r.inbox) > 0 {
			r.dispatching = true
			go e.dispatchTopicInbox(id, key, r)
		}
	}
	e.topicMu.Unlock()
}

func (w *topicWorkWatcher) deliveryOutstanding() bool {
	if progress, ok := w.as.(NativeTurnProgress); ok {
		completed, running := progress.NativeTurnProgress()
		total := completed
		if running {
			total++
		}
		return total > w.baseCompleted+w.resultsAcked.Load()
	}
	return w.pendingDelivery.Load() > 0
}

// drainAgentEvents preserves the existing resync policy while accounting for
// each terminal result deliberately discarded at the ownership boundary. A
// completion counter may advance before emit, so never reset acknowledgements
// to a native snapshot: only events actually removed from the channel are acked.
func (e *Engine) drainAgentEvents(state *interactiveState, session *Session, sessions *SessionManager, key string) {
	state.mu.Lock()
	as := state.agentSession
	state.mu.Unlock()
	if as == nil {
		return
	}
	drained := 0
	for {
		select {
		case event, ok := <-as.Events():
			if !ok {
				return
			}
			drained++
			e.discardNativeResult(state, session, sessions, key, event)
		default:
			if drained > 0 {
				slog.Warn("drained stale events from previous turn", "count", drained)
			}
			return
		}
	}
}

// discardNativeResult is also used when the unsolicited reader has already
// consumed a terminal event when cancellation transfers channel ownership.
// Discarding releases its delivery reservation, but is never proof of platform
// delivery: an already scheduled rotation must fail before that release.
func (e *Engine) discardNativeResult(state *interactiveState, session *Session, sessions *SessionManager, key string, event Event) {
	if event.Type != EventResult || !event.Done {
		return
	}
	const reason = "terminal reply discarded during event resync; old conversation retained"
	var failed bool
	var p Platform
	var replyCtx any
	e.rotationMu.Lock()
	if op, ok := sessions.PendingRotation(rotationSessionKey(key), session.ID); ok {
		state.mu.Lock()
		state.rotationOutcome = rotationTurnOutcome{Complete: true, Delivered: false}
		p, replyCtx = state.platform, state.replyCtx
		state.mu.Unlock()
		if err := sessions.FinishRotation(op.RequestID, "FAILED", reason); err != nil {
			slog.Error("rotation discarded-result receipt failed", "request_id", op.RequestID, "error", err)
		}
		failed = true
	}
	e.rotationMu.Unlock()
	e.topicDeliveryFinished(state)
	if failed && p != nil {
		if err := e.sendWithError(p, replyCtx, e.i18n.Tf(MsgRotationFailed, reason)); err != nil {
			slog.Warn("rotation discarded-result notice failed", "error", err)
		}
	}
}

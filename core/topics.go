package core

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// TopicSessionPolicy opts a platform into per-group admission. Existing routes
// are untouched when this returns an empty thread.
type TopicSessionPolicy interface {
	TopicSession(replyCtx any) (chatID, threadID string, limit int)
}
type TopicInfo struct {
	ChatID   string
	RootID   string
	ThreadID string
}
type TopicReceipt struct {
	RootID    string `json:"root_message_id"`
	ThreadID  string `json:"thread_id"`
	MessageID string `json:"message_id,omitempty"`
	Status    string `json:"status"`
}
type TopicLatest struct {
	Sender string `json:"sender,omitempty"`
	Time   string `json:"time,omitempty"`
	Error  string `json:"error,omitempty"`
}

// TopicPlatform owns service-specific calls; the engine never imports adapters.
type TopicPlatform interface {
	CreateTopic(context.Context, any, string, string, string, string) (TopicReceipt, error)
	TopicInfo(any) TopicInfo
	TopicTitle(context.Context, string) (string, error)
	TopicLatest(context.Context, string) (TopicLatest, error)
}
type topicEnvelope struct {
	p   Platform
	msg *Message
}
type topicRun struct {
	workMu        sync.Mutex
	goalOnlySince time.Time
	goalTimer     *time.Timer
	goalTimerAt   time.Time
	startMu       sync.Mutex
	cancelled     atomic.Bool
	dispatching   bool
	inbox         []topicEnvelope
}
type topicGroup struct {
	stopMu         sync.Mutex
	platform       string
	chat           string
	limit          int
	stopping       bool
	running        map[string]*topicRun
	waiting        map[string][]topicEnvelope
	order          []string
	known          map[string]struct{}
	closing        map[string]int
	goalGeneration map[string]uint64
}
type TopicSessionStatus struct {
	SessionKey   string `json:"session_key"`
	State        string `json:"state"`
	ProcessAlive bool   `json:"process_alive"`
}
type TopicStopResult struct {
	Stopped int `json:"stopped"`
	Cleared int `json:"cleared_messages"`
}

func topicGroupID(platform, chat string) string { return platform + "\x00" + chat }

func (e *Engine) routeTopicMessage(p Platform, msg *Message) bool {
	policy, ok := p.(TopicSessionPolicy)
	if !ok || msg == nil || msg.topicAdmitted || msg.Recalled || msg.IsPermissionResponse || (len(msg.Images) == 0 && len(msg.Files) == 0 && msg.Audio == nil && isTopicControlCommand(msg.Content)) {
		return false
	}
	chat, thread, limit := policy.TopicSession(msg.ReplyCtx)
	if chat == "" || thread == "" {
		return false
	}
	if limit <= 0 {
		limit = 4
	}
	cp := *msg
	cp.topicAdmitted = true
	env := topicEnvelope{p, &cp}
	e.topicMu.Lock()
	if e.topicGroups == nil {
		e.topicGroups = make(map[string]*topicGroup)
	}
	id := topicGroupID(p.Name(), chat)
	g := e.topicGroups[id]
	if g == nil {
		g = &topicGroup{platform: p.Name(), chat: chat, limit: limit, running: map[string]*topicRun{}, waiting: map[string][]topicEnvelope{}, known: map[string]struct{}{}, closing: map[string]int{}, goalGeneration: map[string]uint64{}}
		e.topicGroups[id] = g
	}
	g.known[msg.SessionKey] = struct{}{}
	cp.topicGoalSet = isTopicGoalSet(msg.Content)
	cp.topicGoalGeneration = g.goalGeneration[msg.SessionKey]
	if g.stopping || e.ctx.Err() != nil {
		e.topicMu.Unlock()
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgTopicStopping))
		return true
	}
	if r := g.running[msg.SessionKey]; r != nil && r.cancelled.Load() {
		e.topicMu.Unlock()
		e.reply(p, msg.ReplyCtx, e.i18n.T(MsgTopicStopping))
		return true
	}
	if r := g.running[msg.SessionKey]; r != nil && !r.cancelled.Load() {
		cp.topicRun = r
		r.inbox = append(r.inbox, env)
		if !r.dispatching {
			r.dispatching = true
			go e.dispatchTopicInbox(id, msg.SessionKey, r)
		}
		e.topicMu.Unlock()
		return true
	}
	if _, exists := g.waiting[msg.SessionKey]; !exists {
		g.order = append(g.order, msg.SessionKey)
	}
	g.waiting[msg.SessionKey] = append(g.waiting[msg.SessionKey], env)
	ahead := 0
	for _, k := range g.order {
		if k == msg.SessionKey {
			break
		}
		ahead++
	}
	starts := e.pumpTopicsLocked(id, g)
	queued := g.waiting[msg.SessionKey] != nil
	slog.Info("topic message admitted", "session", msg.SessionKey, "message_id", msg.MessageID, "queued", queued, "topics_ahead", ahead)
	e.topicMu.Unlock()
	for _, start := range starts {
		start()
	}
	if queued {
		e.reply(p, msg.ReplyCtx, e.i18n.Tf(MsgTopicQueued, ahead))
	}
	return true
}

// Must hold topicMu. Reservations are published before launching dispatchers,
// so a finish/stop racing with a fresh message cannot release a new reservation.
func (e *Engine) pumpTopicsLocked(id string, g *topicGroup) []func() {
	var launches []func()
	for !g.stopping && e.ctx.Err() == nil && len(g.running) < g.limit && len(g.order) > 0 {
		key := g.order[0]
		g.order = g.order[1:]
		r := &topicRun{dispatching: true, inbox: g.waiting[key]}
		delete(g.waiting, key)
		g.running[key] = r
		for _, m := range r.inbox {
			m.msg.topicRun = r
		}
		launches = append(launches, func() { go e.dispatchTopicInbox(id, key, r) })
	}
	return launches
}
func (e *Engine) dispatchTopicInbox(id, key string, r *topicRun) {
	for {
		e.topicMu.Lock()
		g := e.topicGroups[id]
		if g == nil || g.running[key] != r || r.cancelled.Load() || len(r.inbox) == 0 {
			r.dispatching = false
			e.topicMu.Unlock()
			e.topicIdle(key)
			return
		}
		env := r.inbox[0]
		r.inbox = r.inbox[1:]
		e.topicMu.Unlock()
		if !r.cancelled.Load() {
			e.handleMessage(env.p, env.msg)
		}
	}
}
func (e *Engine) topicIdle(key string) {
	e.topicMu.Lock()
	var id string
	var r *topicRun
	for gid, g := range e.topicGroups {
		if g.running[key] != nil {
			id, r = gid, g.running[key]
			break
		}
	}
	e.topicMu.Unlock()
	if r == nil {
		return
	}
	// Read backend state without topicMu; callbacks may wake us during startup/close.
	r.workMu.Lock()
	defer r.workMu.Unlock()
	work := e.topicWorkSnapshot(key)
	now := time.Now()
	e.topicMu.Lock()
	g := e.topicGroups[id]
	if g == nil || g.running[key] != r {
		e.topicMu.Unlock()
		return
	}
	hardBusy := g.closing[key] > 0 || r.dispatching || work.frontBusy || work.nativeTurn || work.deferred > 0 || work.deliveryPending
	occupied := hardBusy
	if work.goalActive && !hardBusy {
		if r.goalOnlySince.IsZero() {
			r.goalOnlySince = now
		}
		if work.lastNative.After(r.goalOnlySince) {
			r.goalOnlySince = work.lastNative
		}
		timeout := e.topicGoalIdleTimeout
		if timeout <= 0 {
			timeout = NativeGoalIdleTimeout
		}
		deadline := r.goalOnlySince.Add(timeout)
		if now.Before(deadline) {
			occupied = true
			if r.goalTimer == nil || !r.goalTimerAt.Equal(deadline) {
				if r.goalTimer != nil {
					r.goalTimer.Stop()
				}
				r.goalTimerAt = deadline
				r.goalTimer = time.AfterFunc(time.Until(deadline), func() { e.topicIdle(key) })
			}
		}
	} else {
		r.goalOnlySince = time.Time{}
	}
	if !work.goalActive || hardBusy || !occupied {
		if r.goalTimer != nil {
			r.goalTimer.Stop()
			r.goalTimer = nil
		}
	}
	if occupied {
		e.topicMu.Unlock()
		return
	}
	delete(g.running, key)
	starts := e.pumpTopicsLocked(id, g)
	e.topicMu.Unlock()
	for _, start := range starts {
		start()
	}
}

func (e *Engine) cancelTopic(key string) int {
	e.topicMu.Lock()
	var run *topicRun
	var dropped []topicEnvelope
	for _, g := range e.topicGroups {
		if r := g.running[key]; r != nil {
			r.cancelled.Store(true)
			if r.goalTimer != nil {
				r.goalTimer.Stop()
				r.goalTimer = nil
			}
			run = r
			dropped = append(dropped, r.inbox...)
			r.inbox = nil
		}
		dropped = append(dropped, g.waiting[key]...)
		delete(g.waiting, key)
		for i := 0; i < len(g.order); i++ {
			if g.order[i] == key {
				g.order = append(g.order[:i], g.order[i+1:]...)
				break
			}
		}
	}
	e.topicMu.Unlock()
	// Serialize cancellation with this topic's startup, not other topics or turns.
	if run != nil {
		run.startMu.Lock()
		run.startMu.Unlock()
	}
	for _, env := range dropped {
		e.reply(env.p, env.msg.ReplyCtx, e.i18n.T(MsgTopicCancelled))
	}
	return len(dropped)
}
func (e *Engine) StopTopics(platform, chat string) TopicStopResult {
	id := topicGroupID(platform, chat)
	e.topicMu.Lock()
	g := e.topicGroups[id]
	if g == nil {
		e.topicMu.Unlock()
		return TopicStopResult{}
	}
	e.topicMu.Unlock()
	g.stopMu.Lock()
	defer g.stopMu.Unlock()
	e.topicMu.Lock()
	g.stopping = true
	var keys []string
	for key := range g.known {
		keys = append(keys, key)
	}
	e.topicMu.Unlock()
	out := TopicStopResult{}
	for _, key := range keys {
		out.Cleared += e.cancelTopic(key)
		if e.stopInteractiveSessionSilently(e.interactiveKeyForSessionKey(key)) {
			out.Stopped++
		}
		e.topicIdle(key)
	}
	e.topicMu.Lock()
	g.stopping = false
	e.topicMu.Unlock()
	slog.Info("topic sessions stopped", "platform", platform, "chat", chat, "stopped", out.Stopped, "cleared", out.Cleared)
	return out
}
func (e *Engine) TopicSessions(platform, chat string) []TopicSessionStatus {
	e.topicMu.Lock()
	var out []TopicSessionStatus
	if g := e.topicGroups[topicGroupID(platform, chat)]; g != nil {
		for key := range g.known {
			state := "idle"
			if g.closing[key] > 0 {
				state = "closing"
			} else if g.running[key] != nil {
				state = "running"
			} else if g.waiting[key] != nil {
				state = "queued"
			}
			out = append(out, TopicSessionStatus{SessionKey: key, State: state, ProcessAlive: g.closing[key] > 0})
		}
	}
	e.topicMu.Unlock()
	for i := range out {
		key := e.interactiveKeyForSessionKey(out[i].SessionKey)
		e.interactiveMu.Lock()
		s := e.interactiveStates[key]
		e.interactiveMu.Unlock()
		if s != nil {
			s.mu.Lock()
			out[i].ProcessAlive = out[i].ProcessAlive || (s.agentSession != nil && s.agentSession.Alive())
			s.mu.Unlock()
		}
	}
	return out
}
func (e *Engine) prefixTopicOrigin(p Platform, msg *Message) {
	tp, ok := p.(TopicPlatform)
	if !ok || strings.HasPrefix(strings.TrimSpace(msg.Content), "/") {
		return
	}
	info := tp.TopicInfo(msg.ReplyCtx)
	if info.ThreadID == "" {
		return
	}
	records, err := e.topicRecordsForSession("")
	if err != nil {
		slog.Warn("topic origins unavailable", "error", err)
		return
	}
	for _, r := range records {
		if r.Platform != p.Name() || r.ChatID != info.ChatID || r.ThreadID != info.ThreadID {
			continue
		}
		title := r.Title
		if current, err := tp.TopicTitle(e.ctx, r.RootID); err == nil && current != "" {
			title = current
		}
		msg.ExtraContent = fmt.Sprintf("[话题 %s thread=%s]\n%s", title, info.ThreadID, msg.ExtraContent)
		return
	}
}

// closeTopics makes memory-only queue loss explicit on orderly shutdown.
func (e *Engine) closeTopics() {
	e.topicMu.Lock()
	if len(e.topicGroups) == 0 {
		e.topicMu.Unlock()
		return
	}
	var dropped []topicEnvelope
	for _, g := range e.topicGroups {
		g.stopping = true
		for _, r := range g.running {
			r.cancelled.Store(true)
			if r.goalTimer != nil {
				r.goalTimer.Stop()
				r.goalTimer = nil
			}
			dropped = append(dropped, r.inbox...)
			r.inbox = nil
		}
		for _, q := range g.waiting {
			dropped = append(dropped, q...)
		}
		g.waiting = map[string][]topicEnvelope{}
		g.order = nil
	}
	e.topicMu.Unlock()
	// Engine ctx is already cancelled, but platforms are still open here.
	// Give all shutdown receipts one shared deadline, not a timeout per item.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for _, env := range dropped {
		if err := env.p.Reply(ctx, env.msg.ReplyCtx, e.i18n.T(MsgTopicCancelled)); err != nil {
			slog.Warn("topic shutdown cancellation receipt failed", "session", env.msg.SessionKey, "message_id", env.msg.MessageID, "error", err)
		}
	}
	slog.Info("topic queue discarded on shutdown", "messages", len(dropped))
}

func isTopicStopCommand(text string) bool {
	parts := strings.Fields(text)
	return len(parts) > 0 && parts[0] == "/stop"
}

// Keep close receipts after interactiveStates removes the session. A verifier
// must not mistake asynchronous removal for a subprocess that has exited.
func (e *Engine) trackTopicClose(interactiveKey string) func() {
	e.topicMu.Lock()
	var group *topicGroup
	var key string
	for _, g := range e.topicGroups {
		for candidate := range g.known {
			if interactiveKey == candidate || strings.HasSuffix(interactiveKey, ":"+candidate) {
				group = g
				key = candidate
				if g.closing == nil {
					g.closing = map[string]int{}
				}
				g.closing[key]++
				break
			}
		}
		if group != nil {
			break
		}
	}
	e.topicMu.Unlock()
	return func() {
		if group != nil {
			e.topicMu.Lock()
			group.closing[key]--
			e.topicMu.Unlock()
			go e.topicIdle(key)
		}
	}
}

package core

import (
	"fmt"
	"time"
)

// turnHook reports backend turn state separately from process/session lifetime.
type turnHook struct {
	engine            *Engine
	session           *Session
	key, platform, id string
	ended             bool
}

func (e *Engine) beginTurnHook(session *Session, key, platform, content string) *turnHook {
	t := &turnHook{engine: e, session: session, key: key, platform: platform, id: fmt.Sprintf("%s-%d", session.ID, time.Now().UnixNano())}
	e.hooks.Emit(HookEvent{Event: "turn.started", SessionKey: key, Platform: platform, Content: content, Extra: map[string]any{"turn_id": t.id, "session_id": session.ID, "agent_session_id": session.GetAgentSessionID(), "agent": e.agent.Name()}})
	return t
}

func (t *turnHook) end(reason, content string) {
	if t == nil || t.ended {
		return
	}
	t.ended = true
	t.engine.hooks.Emit(HookEvent{Event: "turn.ended", SessionKey: t.key, Platform: t.platform, Content: content, Extra: map[string]any{"turn_id": t.id, "session_id": t.session.ID, "agent_session_id": t.session.GetAgentSessionID(), "agent": t.engine.agent.Name(), "reason": reason}})
}

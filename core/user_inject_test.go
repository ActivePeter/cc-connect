package core

import (
	"errors"
	"strings"
	"testing"
	"time"
)

type userInjectSession struct {
	*queuingAgentSession
	injectErr error
	messageID string
	images    []ImageAttachment
	files     []FileAttachment
}

func (s *userInjectSession) SendMidTurn(prompt, id string, images []ImageAttachment, files []FileAttachment) error {
	if s.injectErr != nil {
		return s.injectErr
	}
	s.sendMu.Lock()
	s.messageID, s.images, s.files = id, images, files
	s.sendMu.Unlock()
	return s.Send(prompt, id, images, files)
}

func TestHandleMessage_BusyInterjectionPreservesUserMessage(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	as := &userInjectSession{queuingAgentSession: newQueuingSession("live")}
	e := NewEngine("test", &controllableAgent{nextSession: as}, []Platform{p}, t.TempDir()+"/sessions.json", LangEnglish)
	defer e.Stop()
	e.SetInjectSender(true)
	key := "test:room:user"
	session := e.sessions.GetOrCreateActive(key)
	gen, locked := session.TryLock()
	if !locked {
		t.Fatal("lock")
	}
	defer session.UnlockWithoutUpdate(gen)
	state := &interactiveState{agentSession: as, platform: p, replyCtx: "original", midTurnReady: true, currentTurnUserMessageTimeMs: 100}
	e.interactiveStates[key] = state
	accepted := 0
	msg := &Message{SessionKey: key, Platform: "test", UserID: "user", UserName: "Alice", MessageID: "followup", Content: "change the plan", ExtraContent: "quoted context", ReplyCtx: "followup", UserMessageTimeMs: 200,
		Images: []ImageAttachment{{Data: []byte("image"), MimeType: "image/png"}}, Files: []FileAttachment{{Data: []byte("file"), FileName: "notes.txt"}}, OnAccepted: func() { accepted++ }}
	e.ReceiveMessage(p, msg)
	if accepted != 1 {
		t.Fatalf("accepted %d times", accepted)
	}
	as.sendMu.Lock()
	if len(as.sendCalls) != 1 || !strings.Contains(as.sendCalls[0], "quoted context\nchange the plan") || !strings.Contains(as.sendCalls[0], "sender_id=user") {
		t.Fatalf("sent: %v", as.sendCalls)
	}
	if as.messageID != "followup" || len(as.images) != 1 || len(as.files) != 1 {
		t.Fatal("attachment metadata lost")
	}
	as.sendMu.Unlock()
	if history := session.GetHistory(10); len(history) != 1 || history[0].Content != msg.Content {
		t.Fatalf("history: %v", history)
	}
	if len(state.pendingMessages) != 0 || state.currentTurnUserMessageTimeMs != 200 || state.replyCtx != "original" {
		t.Fatal("interjection changed turn ownership or queued another turn")
	}
	if got := p.getSent(); len(got) != 1 || got[0] != e.i18n.T(MsgMessageInjected) {
		t.Fatalf("replies: %v", got)
	}
	// A late redelivery must not insert the older request again.
	e.ReceiveMessage(p, &Message{SessionKey: key, Content: "old request", UserMessageTimeMs: 150})
	if len(as.sendCalls) != 1 {
		t.Fatal("stale request injected")
	}
}

func TestHandleMessage_InterjectionFallbackKeepsQueue(t *testing.T) {
	for _, scenario := range []string{"unsupported", "starting", "not_sent", "stopped", "write_error", "backlog", "mode_override"} {
		t.Run(scenario, func(t *testing.T) {
			p := &stubPlatformEngine{n: "test"}
			as := &userInjectSession{queuingAgentSession: newQueuingSession("live")}
			e := NewEngine("test", &controllableAgent{nextSession: as}, []Platform{p}, "", LangEnglish)
			defer e.Stop()
			key := "test:user"
			session := e.sessions.GetOrCreateActive(key)
			gen, _ := session.TryLock()
			defer session.UnlockWithoutUpdate(gen)
			state := &interactiveState{agentSession: as, platform: p, midTurnReady: true}
			msg := &Message{SessionKey: key, MessageID: "new", Content: "followup", ReplyCtx: "new"}
			switch scenario {
			case "unsupported":
				state.agentSession = as.queuingAgentSession
			case "starting":
				state.agentSession = nil
			case "not_sent":
				state.midTurnReady = false
			case "stopped":
				state.stopped = true
			case "write_error":
				as.injectErr = errors.New("stdin write failed")
			case "backlog":
				state.pendingMessages = []queuedMessage{{content: "earlier", platform: p}}
			case "mode_override":
				msg.ModeOverride = "default"
			}
			e.interactiveStates[key] = state
			e.ReceiveMessage(p, msg)
			if len(as.sendCalls) != 0 {
				t.Fatal("unexpected immediate write")
			}
			if len(state.pendingMessages) == 0 || state.pendingMessages[len(state.pendingMessages)-1].messageID != "new" {
				t.Fatal("message lost from fallback queue")
			}
			if got := p.getSent(); len(got) != 1 || got[0] != e.i18n.T(MsgMessageQueued) {
				t.Fatalf("replies: %v", got)
			}
		})
	}
}

func waitForInterjectionReady(t *testing.T, e *Engine, key string) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		e.interactiveMu.Lock()
		state := e.interactiveStates[key]
		e.interactiveMu.Unlock()
		if state != nil {
			state.mu.Lock()
			ready := state.midTurnReady
			state.mu.Unlock()
			if ready {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatal("turn never became ready for interjection")
		case <-tick.C:
		}
	}
}

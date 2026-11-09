package core

import (
	"errors"
	"strings"
	"testing"
)

type goalInjectSession struct {
	*goalTestSession
	injectErr error
	injected  []string
}

func (s *goalInjectSession) SendMidTurn(prompt, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	if s.injectErr != nil {
		return s.injectErr
	}
	s.injected = append(s.injected, prompt)
	return nil
}
func goalInjectFixture(t *testing.T, always, foreground bool) (*Engine, *stubPlatformEngine, *goalInjectSession, string) {
	t.Helper()
	e, p, base, key := newGoalEngine(t)
	base.info = GoalInfo{Objective: "finish original goal", Status: "active"}
	base.running = true
	as := &goalInjectSession{goalTestSession: base}
	e.interactiveStates[key].agentSession = as
	e.interactiveStates[key].midTurnReady = foreground
	e.SetGoalModeAlways(always)
	if foreground {
		session := e.sessions.GetOrCreateActive(key)
		if !session.TryLock() {
			t.Fatal("lock")
		}
		t.Cleanup(session.UnlockWithoutUpdate)
	}
	return e, p, as, key
}

func TestGoalActive_InterjectsForegroundAndAutomaticTurns(t *testing.T) {
	for _, always := range []bool{false, true} {
		for _, foreground := range []bool{false, true} {
			name := "manual"
			if always {
				name = "always"
			}
			if foreground {
				name += "/foreground"
			} else {
				name += "/automatic"
			}
			t.Run(name, func(t *testing.T) {
				e, p, as, key := goalInjectFixture(t, always, foreground)
				accepted := 0
				e.ReceiveMessage(p, &Message{SessionKey: key, Content: "remember the marker", MessageID: "followup", OnAccepted: func() { accepted++ }})
				if len(as.injected) != 1 || !strings.Contains(as.injected[0], "remember the marker") || accepted != 1 {
					t.Fatalf("sent=%v accepted=%d", as.injected, accepted)
				}
				if got := p.getSent(); len(got) != 1 || got[0] != e.i18n.T(MsgMessageInjected) {
					t.Fatalf("reply: %v", got)
				}
				if info, clears, sets := as.state(); info.Objective != "finish original goal" || info.Status != "active" || clears != 0 || sets != 0 {
					t.Fatal("goal replaced or cleared")
				}
				if queuedSendCount(as.queuingAgentSession) != 0 || len(e.interactiveStates[key].pendingMessages) != 0 {
					t.Fatal("started or queued another turn")
				}
				history := e.sessions.GetOrCreateActive(key).GetHistory(10)
				if len(history) != 1 || history[0].Content != "remember the marker" {
					t.Fatalf("history: %v", history)
				}
			})
		}
	}
}

func TestGoalActive_UnavailableInterjectionKeepsExplicitFallback(t *testing.T) {
	for _, always := range []bool{false, true} {
		for _, scenario := range []string{"between_turns", "steer_error", "image", "file", "mode_override", "slash_command"} {
			name := "manual/"
			if always {
				name = "always/"
			}
			t.Run(name+scenario, func(t *testing.T) {
				e, p, as, key := goalInjectFixture(t, always, false)
				msg := &Message{SessionKey: key, Content: "followup"}
				accepted := 0
				msg.OnAccepted = func() { accepted++ }
				switch scenario {
				case "between_turns":
					as.running = false
				case "steer_error":
					as.injectErr = errors.New("expected active turn id changed")
				case "image":
					msg.Images = []ImageAttachment{{}}
				case "file":
					msg.Files = []FileAttachment{{}}
				case "mode_override":
					msg.ModeOverride = "default"
				case "slash_command":
					msg.Content = "/unknown-command"
				}
				e.ReceiveMessage(p, msg)
				wantReplies := 1
				if scenario == "slash_command" {
					wantReplies = 2 // existing unknown-command notice, then goal gating
				}
				if got := p.getSent(); len(got) != wantReplies || !strings.Contains(got[len(got)-1], "goal 进行中") {
					t.Fatalf("reply: %v", got)
				}
				if accepted != 0 || len(as.injected) != 0 || len(e.interactiveStates[key].pendingMessages) != 0 || queuedSendCount(as.queuingAgentSession) != 0 {
					t.Fatal("rejected message was acknowledged or queued")
				}
				if !as.GetGoal().Active() {
					t.Fatal("goal stopped")
				}
			})
		}
	}
}

func TestInjectAPI_AutomaticGoalTurnWithIdleForeground(t *testing.T) {
	e, _, as, key := goalInjectFixture(t, true, false)
	status, err := e.InjectToBusySession(key, "api interjection")
	if err != nil || status != InjectInjected || len(as.injected) != 1 {
		t.Fatalf("%s %v %v", status, err, as.injected)
	}
	as.running = false
	status, err = e.InjectToBusySession(key, "between turns")
	if err != nil || status != InjectIdle || len(as.injected) != 1 {
		t.Fatalf("%s %v %v", status, err, as.injected)
	}
}

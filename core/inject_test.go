package core

import (
	"errors"
	"testing"
)

type injectRecordingSession struct {
	stubAgentSession
	sent []string
	dead bool
}

func (s *injectRecordingSession) Send(p string, _ string, _ []ImageAttachment, _ []FileAttachment) error {
	s.sent = append(s.sent, p)
	return nil
}
func (s *injectRecordingSession) Alive() bool { return !s.dead }

type namedStubAgent struct {
	stubAgent
	name string
}

func (a *namedStubAgent) Name() string { return a.name }

func TestInjectIdleSessionWritesNothing(t *testing.T) {
	e := newTestEngine()
	rec := &injectRecordingSession{}
	e.interactiveStates["s1"] = &interactiveState{agentSession: rec, agent: &namedStubAgent{name: "claudecode"}}
	got, err := e.InjectToBusySession("s1", "hello")
	if err != nil || got != InjectIdle {
		t.Fatalf("idle session: got %q, %v; want idle", got, err)
	}
	if len(rec.sent) != 0 {
		t.Fatalf("idle session must not be written to, sent %v", rec.sent)
	}
	// The probe must not leave the session locked.
	if !e.sessions.GetOrCreateActive("s1").TryLock() {
		t.Fatal("inject left the session locked")
	}
}

func TestInjectBusySessionWritesToAgent(t *testing.T) {
	e := newTestEngine()
	rec := &injectRecordingSession{}
	e.interactiveStates["s1"] = &interactiveState{agentSession: rec, agent: &namedStubAgent{name: "claudecode"}}
	sess := e.sessions.GetOrCreateActive("s1")
	if !sess.TryLock() {
		t.Fatal("lock")
	}
	defer sess.UnlockWithoutUpdate()
	got, err := e.InjectToBusySession("s1", "stop now")
	if err != nil || got != InjectInjected {
		t.Fatalf("busy session: got %q, %v; want injected", got, err)
	}
	if len(rec.sent) != 1 || rec.sent[0] != "stop now" {
		t.Fatalf("sent %v", rec.sent)
	}
}

func TestInjectBusyButNoLiveAgentIsIdle(t *testing.T) {
	e := newTestEngine()
	rec := &injectRecordingSession{dead: true}
	e.interactiveStates["s1"] = &interactiveState{agentSession: rec, agent: &namedStubAgent{name: "claudecode"}}
	sess := e.sessions.GetOrCreateActive("s1")
	sess.TryLock()
	defer sess.UnlockWithoutUpdate()
	got, err := e.InjectToBusySession("s1", "x")
	if err != nil || got != InjectIdle || len(rec.sent) != 0 {
		t.Fatalf("dead agent: got %q, %v, sent %v", got, err, rec.sent)
	}
	delete(e.interactiveStates, "s1")
	got, err = e.InjectToBusySession("s1", "x")
	if err != nil || got != InjectIdle {
		t.Fatalf("no state: got %q, %v", got, err)
	}
}

func TestInjectRejectsOtherAgents(t *testing.T) {
	e := newTestEngine()
	rec := &injectRecordingSession{}
	e.interactiveStates["s1"] = &interactiveState{agentSession: rec, agent: &namedStubAgent{name: "codex"}}
	sess := e.sessions.GetOrCreateActive("s1")
	sess.TryLock()
	defer sess.UnlockWithoutUpdate()
	if _, err := e.InjectToBusySession("s1", "x"); !errors.Is(err, ErrInjectUnsupported) {
		t.Fatalf("codex: err %v, want ErrInjectUnsupported", err)
	}
	if len(rec.sent) != 0 {
		t.Fatalf("sent %v", rec.sent)
	}
}

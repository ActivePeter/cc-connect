package core

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRotationStoreLatestAndRetirementSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")
	sm := NewSessionManager(path)
	if sm.LookupActive("missing") != nil || len(sm.AllSessions()) != 0 {
		t.Fatal("lookup created session")
	}
	s := sm.GetOrCreateActive("key")
	s.SetAgentSessionID("backend-A", "stub")
	s.TouchUserActivity()
	s.SetActiveProvider("provider")
	sm.Save()
	first, err := sm.PrepareRotation("key", s.ID, "r1", "", "FIRST")
	if err != nil {
		t.Fatal(err)
	}
	same, err := sm.PrepareRotation("key", s.ID, "r1", "", "FIRST")
	if err != nil || same != first {
		t.Fatal("request not idempotent", same, err)
	}
	if _, err = sm.PrepareRotation("key", s.ID, "r1", "", "DIFFERENT"); !errors.Is(err, ErrRotationStale) {
		t.Fatal(err)
	}
	last, err := sm.PrepareRotation("key", s.ID, "r2", "new", "LAST")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = sm.CommitRotation(first); !errors.Is(err, ErrRotationStale) {
		t.Fatal("superseded commit", err)
	}
	fresh, err := sm.CommitRotation(last)
	if err != nil {
		t.Fatal(err)
	}
	if !sm.IsRetiredAgentID("backend-A") || fresh.ID == s.ID || fresh.FirstInput().Text != "LAST" {
		t.Fatal("incomplete commit")
	}
	s.SetAgentSessionID("late-old-init", "stub")
	fresh.SetAgentSessionID("backend-A", "stub")
	if s.GetAgentSessionID() != "" || fresh.GetAgentSessionID() != "" {
		t.Fatal("retired ID rebound")
	}
	if sm.SwitchToAgentSession("key", "backend-A", "stub", "old") != nil {
		t.Fatal("native resume not fenced")
	}
	if _, err = sm.SwitchSession("key", s.ID); err == nil {
		t.Fatal("logical resume not fenced")
	}
	restored := NewSessionManager(path)
	if restored.LookupActive("key").ID != fresh.ID || !restored.IsRetiredAgentID("backend-A") {
		t.Fatal("restart lost retirement")
	}
	old := restored.FindByID(s.ID)
	if old.GetLastUserActivity().IsZero() || old.GetActiveProvider() != "provider" {
		t.Fatal("snapshot lost activity/provider")
	}
	if _, err = restored.SwitchSession("key", s.ID); err == nil {
		t.Fatal("restart restored source")
	}
}

func TestRotationStoreCommitWriteFailureKeepsSource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")
	sm := NewSessionManager(path)
	s := sm.GetOrCreateActive("key")
	s.SetAgentSessionID("A", "stub")
	op, err := sm.PrepareRotation("key", s.ID, "r", "", "handoff")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err = sm.CommitRotation(op); err == nil {
		t.Fatal("write failure swallowed")
	}
	if sm.LookupActive("key") != s || s.GetAgentSessionID() != "A" || sm.IsRetiredAgentID("A") {
		t.Fatal("failed commit mutated active state")
	}
}

func TestRotationStoreRestartCancelsUnconfirmedAndPreservesOutbox(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.json")
	sm := NewSessionManager(path)
	s := sm.GetOrCreateActive("key")
	op, err := sm.PrepareRotation("key", s.ID, "r", "", "handoff")
	if err != nil {
		t.Fatal(err)
	}
	restored := NewSessionManager(path)
	got, _ := restored.Rotation(op.RequestID)
	if got.Status != "CANCELLED" || restored.LookupActive("key").ID != s.ID {
		t.Fatal("unconfirmed operation resumed", got)
	}
	op, err = sm.PrepareRotation("key", s.ID, "r2", "", "handoff")
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := sm.CommitRotation(op)
	if err != nil {
		t.Fatal(err)
	}
	if err = sm.SetFirstInputState(fresh, "in_flight"); err != nil {
		t.Fatal(err)
	}
	if got := NewSessionManager(path).LookupActive("key").FirstInput(); got == nil || got.State != "in_flight" {
		t.Fatal("ambiguous outbox lost")
	}
}

func TestRotationStoreConcurrentRegistrations(t *testing.T) {
	sm := NewSessionManager(filepath.Join(t.TempDir(), "sessions.json"))
	s := sm.GetOrCreateActive("key")
	done := make(chan error, 20)
	for i := 0; i < 20; i++ {
		go func(i int) {
			_, err := sm.PrepareRotation("key", s.ID, time.Unix(int64(i), 0).String(), "", "handoff")
			done <- err
		}(i)
	}
	for i := 0; i < 20; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	for _, op := range sm.rotations {
		if op.Status == "SCHEDULED" {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("pending=%d", n)
	}
}

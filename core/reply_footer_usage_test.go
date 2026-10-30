package core

import (
	"context"
	"strings"
	"testing"
	"time"
)

// blockingUsageAgent models the claude /usage probe blocked in its pty reader.
type blockingUsageAgent struct {
	*stubAgent
	release chan struct{}
}

func (a *blockingUsageAgent) GetUsage(ctx context.Context) (*UsageReport, error) {
	<-a.release
	return nil, ctx.Err()
}

// Finishing a turn must never wait for a usage probe.
func TestReplyFooterNeverBlocksOnUsageProbe(t *testing.T) {
	p := &stubPlatformEngine{n: "test"}
	a := &blockingUsageAgent{stubAgent: &stubAgent{}, release: make(chan struct{})}
	defer close(a.release)
	e := NewEngine("test", a, []Platform{p}, "", LangEnglish)
	defer e.Stop()
	e.SetReplyFooterEnabled(true)
	e.replyFooterUsage = replyFooterUsageCache{text: "STALE-USAGE", fetchedAt: time.Now().Add(-time.Minute)}

	key := "test:usage-nonblock"
	session := e.sessions.GetOrCreateActive(key)
	as := newControllableSession("usage-nonblock")
	state := &interactiveState{agentSession: as, platform: p, replyCtx: "ctx"}
	e.interactiveStates[key] = state

	as.events <- Event{Type: EventResult, Content: "hi", Done: true}
	start := time.Now()
	e.processInteractiveEvents(state, session, e.sessions, key, "m1", time.Now(), nil, nil, state.replyCtx)
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("turn completion blocked %v on the usage probe", elapsed)
	}
	if sent := p.getSent(); len(sent) != 1 || !strings.Contains(sent[0], "STALE-USAGE") {
		t.Fatalf("reply = %#v, want a footer from the stale cache", sent)
	}
}

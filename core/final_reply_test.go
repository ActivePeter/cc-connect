package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type completionTestPlatform struct {
	*stubPlatformEngine
	failSend   bool
	failNotify bool
}

func (p *completionTestPlatform) Send(ctx context.Context, replyCtx any, content string) error {
	if p.failSend {
		return errors.New("test delivery failure")
	}
	return p.stubPlatformEngine.Send(ctx, replyCtx, content)
}

func (p *completionTestPlatform) NotifyFinalReply(ctx context.Context, replyCtx any, message string) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("notification context must have a deadline")
	}
	if p.failNotify {
		return errors.New("test notification failure")
	}
	return p.stubPlatformEngine.Send(ctx, replyCtx, fmt.Sprintf("@%v %s", replyCtx, message))
}

func completionMentions(sent []string) []string {
	var mentions []string
	for _, s := range sent {
		if strings.HasPrefix(s, "@") {
			mentions = append(mentions, s)
		}
	}
	return mentions
}

func TestFinalReply_OnlyAfterVisibleTerminalDelivery(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		response             string
		failSend, failNotify bool
		wantMentions         int
	}{
		{name: "visible", response: "The result", wantMentions: 1},
		{name: "silent", response: "NO_REPLY"},
		{name: "delivery failure", response: "The result", failSend: true},
		{name: "notification failure does not resend answer", response: "The result", failNotify: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &completionTestPlatform{stubPlatformEngine: &stubPlatformEngine{n: "test"}, failSend: tc.failSend, failNotify: tc.failNotify}
			e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
			defer e.Stop()
			sess := newControllableSession("final-reply")
			session := e.sessions.GetOrCreateActive("test:requester")
			state := &interactiveState{agentSession: sess, platform: p, replyCtx: "requester"}
			sess.events <- Event{Type: EventResult, Content: "compaction", Done: false}
			sess.events <- Event{Type: EventResult, Content: tc.response, Done: true}
			e.processInteractiveEvents(state, session, e.sessions, "test:requester", "", time.Now(), nil, nil, "requester")
			sent := p.getSent()
			if got := len(completionMentions(sent)); got != tc.wantMentions {
				t.Fatalf("mentions=%d, want %d; sent=%v", got, tc.wantMentions, sent)
			}
			if tc.wantMentions == 1 && (len(sent) != 2 || sent[0] != tc.response || !strings.HasPrefix(sent[1], "@requester ")) {
				t.Fatalf("expected answer then one requester notification, got %v", sent)
			}
			if tc.failNotify && (len(sent) != 1 || sent[0] != tc.response) {
				t.Fatalf("failed notification changed answer delivery: %v", sent)
			}
		})
	}
}

func TestFinalReply_BackgroundSkipsProgressAndSilentResults(t *testing.T) {
	p := &completionTestPlatform{stubPlatformEngine: &stubPlatformEngine{n: "test"}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	defer e.Stop()
	sess := newControllableSession("background-final")
	session := e.sessions.GetOrCreateActive("test:requester")
	state := &interactiveState{agentSession: sess, platform: p, replyCtx: "requester"}
	e.startUnsolicitedReader(state, session, e.sessions, "test:requester", "")
	defer e.stopUnsolicitedReader(state)
	sess.events <- Event{Type: EventResult, Content: "compaction", Done: false}
	sess.events <- Event{Type: EventResult, Content: "NO_REPLY", Done: true}
	sess.events <- Event{Type: EventResult, Content: "background result", Done: true}
	sent := waitForPlatformSend(p.stubPlatformEngine, 4, 2*time.Second)
	if len(sent) != 4 || len(completionMentions(sent)) != 1 || sent[2] != "background result" || !strings.HasPrefix(sent[3], "@requester ") {
		t.Fatalf("expected only final visible background result to notify requester, got %v", sent)
	}
}

func TestFinalReply_StoppedTurnDoesNotNotify(t *testing.T) {
	p := &completionTestPlatform{stubPlatformEngine: &stubPlatformEngine{n: "test"}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	defer e.Stop()
	state := &interactiveState{agentSession: newControllableSession("stopped"), platform: p, replyCtx: "requester"}
	state.markStopped()
	session := e.sessions.GetOrCreateActive("test:requester")
	e.processInteractiveEvents(state, session, e.sessions, "test:requester", "", time.Now(), nil, nil, "requester")
	if got := completionMentions(p.getSent()); len(got) != 0 {
		t.Fatalf("stopped turn notified completion: %v", got)
	}
}

type completionStreamingPlatform struct{ *completionTestPlatform }

func (p *completionStreamingPlatform) CreateStreamingCard(_ context.Context, replyCtx any) (StreamingCard, error) {
	return &completionStreamingCard{platform: p, replyCtx: replyCtx}, nil
}

type completionStreamingCard struct {
	platform *completionStreamingPlatform
	replyCtx any
}

func (c *completionStreamingCard) Update(context.Context, string) error { return nil }
func (c *completionStreamingCard) Failed() bool                         { return false }
func (c *completionStreamingCard) Finalize(ctx context.Context, content string) error {
	return c.platform.Send(ctx, c.replyCtx, "final card: "+content)
}
func TestFinalReply_StreamingCardFinalizesBeforeNotification(t *testing.T) {
	p := &completionStreamingPlatform{&completionTestPlatform{stubPlatformEngine: &stubPlatformEngine{n: "test"}}}
	e := NewEngine("test", &stubAgent{}, []Platform{p}, "", LangEnglish)
	defer e.Stop()
	sess := newControllableSession("card")
	state := &interactiveState{agentSession: sess, platform: p, replyCtx: "requester"}
	session := e.sessions.GetOrCreateActive("test:requester")
	sess.events <- Event{Type: EventResult, Content: "Finished", Done: true}
	e.processInteractiveEvents(state, session, e.sessions, "test:requester", "", time.Now(), nil, nil, "requester")
	sent := p.getSent()
	if len(sent) != 2 || !strings.Contains(sent[0], "final card: ") || !strings.HasPrefix(sent[1], "@requester ") {
		t.Fatalf("expected finalized card before native notification, got %v", sent)
	}
}

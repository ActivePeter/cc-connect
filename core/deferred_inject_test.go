package core

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type deferredTestSession struct {
	*goalInjectSession
	queueMu       sync.Mutex
	queued        []DeferredMessage
	queueErr      error
	beforeEnqueue func(DeferredMessage)
}

func (s *deferredTestSession) EnqueueMidTurn(m DeferredMessage) error {
	if s.beforeEnqueue != nil {
		s.beforeEnqueue(m)
	}
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if s.queueErr != nil {
		return s.queueErr
	}
	s.queued = append(s.queued, m)
	return nil
}
func (s *deferredTestSession) PendingMidTurnMessages() int {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	return len(s.queued)
}
func (s *deferredTestSession) ClearMidTurnMessages() int {
	s.queueMu.Lock()
	q := s.queued
	s.queued = nil
	s.queueMu.Unlock()
	for _, m := range q {
		m.Notify(DeferredCanceled, nil)
	}
	return len(q)
}
func (s *deferredTestSession) deliver(status string) {
	s.queueMu.Lock()
	m := s.queued[0]
	s.queued = s.queued[1:]
	s.queueMu.Unlock()
	m.Notify(status, nil)
}
func deferredFixture(t *testing.T, always, foreground bool) (*Engine, *stubPlatformEngine, *deferredTestSession, string) {
	e, p, as, key := goalInjectFixture(t, always, foreground)
	d := &deferredTestSession{goalInjectSession: as}
	e.interactiveStates[key].agentSession = d
	return e, p, d, key
}
func TestDeferredMessage_AlwaysOrdinaryAndFinishingTurns(t *testing.T) {
	for _, status := range []string{"", "complete", "active"} {
		for _, foreground := range []bool{true, false} {
			t.Run(status+map[bool]string{true: "/foreground", false: "/background"}[foreground], func(t *testing.T) {
				e, p, as, key := deferredFixture(t, true, foreground)
				as.mu.Lock()
				as.info = GoalInfo{Status: status, Objective: map[bool]string{true: "original", false: ""}[status != ""]}
				as.mu.Unlock()
				sendText(e, p, key, "correct the plan")
				if len(as.injected) != 1 || as.PendingMidTurnMessages() != 0 {
					t.Fatal("ordinary/finishing turn did not receive steer")
				}
				if _, _, sets := as.state(); sets != 0 {
					t.Fatal("new goal set while ordinary turn ran")
				}
				if got := p.getSent(); len(got) != 1 || got[0] != e.i18n.T(MsgMessageInjected) {
					t.Fatal(got)
				}
			})
		}
	}
}
func TestDeferredMessage_FailedSteerQueuesAndNotifiesOnce(t *testing.T) {
	e, p, as, key := deferredFixture(t, true, false)
	as.injectErr = errors.New("expected turn id expired")
	accepted := 0
	e.ReceiveMessage(p, &Message{SessionKey: key, Content: "first", MessageID: "one", OnAccepted: func() { accepted++ }})
	e.ReceiveMessage(p, &Message{SessionKey: key, Content: "second", MessageID: "two", OnAccepted: func() { accepted++ }})
	if as.PendingMidTurnMessages() != 2 || accepted != 2 || len(as.injected) != 0 {
		t.Fatal("message lost or bypassed queue")
	}
	as.queueMu.Lock()
	ids := as.queued[0].MessageID + "," + as.queued[1].MessageID
	as.queueMu.Unlock()
	if ids != "one,two" {
		t.Fatal(ids)
	}
	as.deliver(DeferredInserted)
	as.deliver(DeferredSent)
	got := p.getSent()
	if len(got) != 4 || got[0] != e.i18n.T(MsgDeferredMessageQueued) || got[1] != got[0] || got[2] != e.i18n.T(MsgMessageInjected) || got[3] != e.i18n.T(MsgDeferredMessageSent) {
		t.Fatal(got)
	}
	if len(e.sessions.GetOrCreateActive(key).GetHistory(10)) != 2 {
		t.Fatal("history duplicated")
	}
}
func TestDeferredMessage_GapAttachmentAndQueueFailure(t *testing.T) {
	for _, scenario := range []string{"gap", "attachment", "queue_error"} {
		t.Run(scenario, func(t *testing.T) {
			e, p, as, key := deferredFixture(t, true, false)
			msg := &Message{SessionKey: key, Content: "new input", MessageID: "followup"}
			switch scenario {
			case "gap":
				as.running = false
			case "attachment":
				msg.Files = []FileAttachment{{FileName: "a.txt", Data: []byte("x")}}
			case "queue_error":
				as.injectErr = errors.New("steer failed")
				as.queueErr = errors.New("queue full")
			}
			e.ReceiveMessage(p, msg)
			got := strings.Join(p.getSent(), "|")
			if strings.Contains(got, "重发") || strings.Contains(got, "goal 进行中") {
				t.Fatal(got)
			}
			if scenario == "queue_error" {
				if !strings.Contains(got, "queue full") {
					t.Fatal(got)
				}
			} else if as.PendingMidTurnMessages() != 1 {
				t.Fatal("not queued")
			}
			if scenario == "attachment" && !strings.Contains(got, "Attachment queued") {
				t.Fatal(got)
			}
		})
	}
}
func TestDeferredMessage_ClearAndStopReportQueueCount(t *testing.T) {
	for _, cmd := range []string{"/goal clear", "/stop"} {
		t.Run(cmd, func(t *testing.T) {
			e, p, as, key := deferredFixture(t, true, false)
			as.running = false
			sendText(e, p, key, "one")
			sendText(e, p, key, "two")
			p.clearSent()
			sendText(e, p, key, cmd)
			if as.PendingMidTurnMessages() != 0 {
				t.Fatal("queue survived command")
			}
			got := strings.Join(p.getSent(), "|")
			if !strings.Contains(got, "Cleared 2 queued") {
				t.Fatal(got)
			}
		})
	}
}
func TestDeferredMessage_FirstSendStillWaits(t *testing.T) {
	e, p, as, key := deferredFixture(t, true, true)
	as.mu.Lock()
	as.info = GoalInfo{}
	as.running = false
	as.mu.Unlock()
	e.interactiveStates[key].midTurnReady = false
	sendText(e, p, key, "next input")
	if len(as.injected) != 0 || as.PendingMidTurnMessages() != 0 || len(e.interactiveStates[key].pendingMessages) != 1 {
		t.Fatal("overtook initial Send")
	}
}
func TestDeferredMessage_QueuedAutomaticGoalDoesNotReject(t *testing.T) {
	e, p, as, key := deferredFixture(t, true, false)
	q := queuedMessage{goalAuto: true, goalObjective: true, content: "complete batch", messageID: "queued", msgSessionKey: key, platform: p}
	if err := e.sendQueuedGoal(e.interactiveStates[key], as, 0, q.content, p, nil, q); err != nil {
		t.Fatal(err)
	}
	if as.PendingMidTurnMessages() != 1 || strings.Contains(strings.Join(p.getSent(), "|"), "重发") {
		t.Fatal("automatic goal rejected")
	}
	as.ClearMidTurnMessages()
}

func TestDeferredMessage_IdleAlwaysSetsGoal(t *testing.T) {
	e, p, as, key := deferredFixture(t, true, false)
	as.mu.Lock()
	as.info = GoalInfo{}
	as.running = false
	as.mu.Unlock()
	sendText(e, p, key, "a complete fresh objective")
	if info, _, sets := as.state(); sets != 1 || info.Objective != "a complete fresh objective" {
		t.Fatalf("goal=%v sets=%d", info, sets)
	}
	if len(as.injected) != 0 || as.PendingMidTurnMessages() != 0 {
		t.Fatal("idle input queued or steered")
	}
}
func TestDeferredMessage_ClearAlsoCountsForegroundBacklog(t *testing.T) {
	e, p, as, key := deferredFixture(t, true, false)
	as.running = false
	sendText(e, p, key, "native queued")
	e.interactiveStates[key].pendingMessages = []queuedMessage{{content: "startup queued", goalObjective: true}}
	p.clearSent()
	sendText(e, p, key, "/goal clear")
	if as.PendingMidTurnMessages() != 0 || len(e.interactiveStates[key].pendingMessages) != 0 {
		t.Fatal("queue survived clear")
	}
	if got := strings.Join(p.getSent(), "|"); !strings.Contains(got, "Cleared 2 queued") {
		t.Fatal(got)
	}
}

func TestDeferredMessage_ConcurrentArrivalCannotOvertakeFailedSteer(t *testing.T) {
	e, p, as, key := deferredFixture(t, true, false)
	as.injectErr = errors.New("expired turn")
	entered, release := make(chan struct{}), make(chan struct{})
	as.beforeEnqueue = func(m DeferredMessage) {
		if m.MessageID == "one" {
			close(entered)
			<-release
		}
	}
	firstDone := make(chan struct{})
	go func() {
		e.ReceiveMessage(p, &Message{SessionKey: key, Content: "first", MessageID: "one"})
		close(firstDone)
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first message did not reach enqueue")
	}
	as.mu.Lock()
	as.injectErr = nil
	as.mu.Unlock()
	secondDone := make(chan struct{})
	go func() {
		e.ReceiveMessage(p, &Message{SessionKey: key, Content: "second", MessageID: "two"})
		close(secondDone)
	}()
	overtook := false
	select {
	case <-secondDone:
		overtook = true
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case <-firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("first message stuck")
	}
	select {
	case <-secondDone:
	case <-time.After(2 * time.Second):
		t.Fatal("second message stuck")
	}
	if overtook {
		t.Fatal("second message overtook failed steer before it entered FIFO")
	}
	as.queueMu.Lock()
	defer as.queueMu.Unlock()
	if len(as.queued) != 2 || as.queued[0].MessageID != "one" || as.queued[1].MessageID != "two" {
		t.Fatalf("queue=%+v", as.queued)
	}
	got := p.getSent()
	if len(got) != 2 || got[0] != e.i18n.T(MsgDeferredMessageQueued) || got[1] != got[0] {
		t.Fatal(got)
	}
}

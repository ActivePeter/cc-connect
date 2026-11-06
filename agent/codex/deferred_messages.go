package codex

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/chenhg5/cc-connect/core"
)

const (
	maxDeferredMessages         = 64
	deferredIdleWatchdogTimeout = 60 * time.Second
)

type deferredMessage struct {
	core.DeferredMessage
	lastAttempt     uint64
	afterCompletion uint64
	epoch           uint64
	enqueuedAt      time.Time
}

var _ core.DeferredMessageSender = (*appServerSession)(nil)

func (s *appServerSession) PendingMidTurnMessages() int {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	return len(s.messageQueue) + s.queueInFlight
}

func (s *appServerSession) EnqueueMidTurn(m core.DeferredMessage) error {
	s.stateMu.Lock()
	seq, ended := s.turnSequence, s.turnCompletions
	s.stateMu.Unlock()
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	if !s.alive.Load() {
		return fmt.Errorf("codex: session is closed")
	}
	if len(s.messageQueue)+s.queueInFlight >= maxDeferredMessages {
		return fmt.Errorf("codex: deferred message queue is full (%d)", maxDeferredMessages)
	}
	q := &deferredMessage{DeferredMessage: m, afterCompletion: ended, epoch: s.queueEpoch, enqueuedAt: time.Now()}
	if m.Attempted {
		q.lastAttempt = seq
	}
	s.messageQueue = append(s.messageQueue, q)
	if s.queueWake == nil {
		s.queueWake = make(chan struct{}, 1)
		s.wg.Add(1)
		go s.runDeferredMessages()
	}
	slog.Info("codex message queued", "msg_id", m.MessageID, "depth", len(s.messageQueue), "attachments", len(m.Images)+len(m.Files))
	select {
	case s.queueWake <- struct{}{}:
	default:
	}
	return nil
}

func (s *appServerSession) signalDeferredMessages() {
	s.queueMu.Lock()
	wake := s.queueWake
	s.queueMu.Unlock()
	if wake != nil {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func (s *appServerSession) ClearMidTurnMessages() int { return s.clearDeferredMessages("command") }
func (s *appServerSession) clearDeferredMessages(reason string) int {
	// Let an in-flight RPC settle before clearing. Otherwise its fallback
	// SetGoal could land after /goal clear and resurrect the canceled work.
	s.queueDeliveryMu.Lock()
	s.queueMu.Lock()
	pending := s.messageQueue
	s.messageQueue = nil
	s.queueEpoch++
	s.stopDeferredWatchdogLocked()
	attempts, misses := s.queueAttempts, s.queueMisses
	s.queueMu.Unlock()
	s.queueDeliveryMu.Unlock()
	s.signalDeferredMessages()
	if len(pending) > 0 {
		slog.Info("codex deferred messages discarded", "count", len(pending), "reason", reason, "steer_attempts", attempts, "steer_misses", misses)
	}
	for _, m := range pending {
		if m.Notify != nil {
			m.Notify(core.DeferredCanceled, nil)
		}
	}
	return len(pending)
}

func (s *appServerSession) runDeferredMessages() {
	defer s.wg.Done()
	defer s.stopDeferredWatchdog()
	for {
		s.queueMu.Lock()
		var watchdog <-chan time.Time
		if s.queueWatchdog != nil {
			watchdog = s.queueWatchdog.C
		}
		s.queueMu.Unlock()
		select {
		case <-s.ctx.Done():
			return
		case <-s.queueWake:
		case <-watchdog:
		}
		for s.deliverDeferredHead() {
		}
		s.armDeferredWatchdog()
	}
}

// The queue worker owns a single reusable timer. Clearing/starting a turn can
// stop it synchronously; the buffered wake then makes the worker reevaluate.
func (s *appServerSession) stopDeferredWatchdogLocked() {
	if s.queueWatchdog != nil {
		s.queueWatchdog.Stop()
	}
}

func (s *appServerSession) stopDeferredWatchdog() {
	s.queueMu.Lock()
	s.stopDeferredWatchdogLocked()
	s.queueMu.Unlock()
}

func deferredIdleSince(head *deferredMessage, completed time.Time) time.Time {
	if completed.After(head.enqueuedAt) {
		return completed
	}
	return head.enqueuedAt
}

func (s *appServerSession) armDeferredWatchdog() {
	s.queueMu.Lock()
	defer s.queueMu.Unlock()
	s.stateMu.Lock()
	turn, completed := s.currentTurn, s.lastTurnCompletedAt
	s.stateMu.Unlock()
	if len(s.messageQueue) == 0 || turn != "" || !s.GetGoal().Active() || !s.alive.Load() {
		s.stopDeferredWatchdogLocked()
		return
	}
	wait := time.Until(deferredIdleSince(s.messageQueue[0], completed).Add(deferredIdleWatchdogTimeout))
	if s.queueWatchdog == nil {
		s.queueWatchdog = time.NewTimer(wait)
	} else {
		s.queueWatchdog.Reset(wait)
	}
}

// deliverDeferredHead never blocks the RPC reader. Each failed steer is tried
// at most once per turn/started generation; the head remains ahead of later
// messages. Terminal goals and attachment boundaries use ordinary Send.
func (s *appServerSession) deliverDeferredHead() bool {
	s.queueDeliveryMu.Lock()
	more, notify := s.dispatchDeferredHead()
	s.queueDeliveryMu.Unlock()
	// Platform callbacks may block or invoke commands. Never run them while
	// holding the delivery lock.
	if notify != nil {
		notify()
	}
	return more
}

func (s *appServerSession) dispatchDeferredHead() (bool, func()) {
	s.queueMu.Lock()
	if len(s.messageQueue) == 0 {
		s.queueMu.Unlock()
		return false, nil
	}
	head := s.messageQueue[0]
	s.queueMu.Unlock()
	s.stateMu.Lock()
	turn, seq, ended, blocked := s.currentTurn, s.turnSequence, s.turnCompletions, s.turnUnsteerable
	completed := s.lastTurnCompletedAt
	s.stateMu.Unlock()
	goal := s.GetGoal()
	attachment := len(head.Images)+len(head.Files) > 0
	idleSince := deferredIdleSince(head, completed)
	watchdog := goal.Active() && turn == "" && time.Since(idleSince) >= deferredIdleWatchdogTimeout
	fallback := watchdog || (!goal.Active() && turn == "") || (attachment && ended > head.afterCompletion)
	if !fallback && (turn == "" || blocked || attachment || head.lastAttempt == seq) {
		return false, nil
	}
	s.queueMu.Lock()
	if len(s.messageQueue) == 0 || s.messageQueue[0] != head || head.epoch != s.queueEpoch {
		s.queueMu.Unlock()
		return true, nil
	}
	batch := []*deferredMessage{head}
	if fallback {
		batch = append([]*deferredMessage(nil), s.messageQueue...)
		s.messageQueue = nil
	} else {
		s.messageQueue = s.messageQueue[1:]
		head.lastAttempt = seq
		s.queueAttempts++
	}
	s.queueInFlight += len(batch)
	if watchdog {
		s.queueWatchdogTriggers++
		slog.Warn("codex deferred idle watchdog triggered", "trigger_count", s.queueWatchdogTriggers, "wait_seconds", time.Since(idleSince).Seconds(), "messages", len(batch), "msg_id", head.MessageID)
	}
	s.queueMu.Unlock()
	var err error
	status := core.DeferredInserted
	if fallback {
		var prompts, objectives []string
		var images []core.ImageAttachment
		var files []core.FileAttachment
		asGoal := false
		for _, m := range batch {
			prompts = append(prompts, m.Prompt)
			objectives = append(objectives, m.GoalObjective)
			images = append(images, m.Images...)
			files = append(files, m.Files...)
			asGoal = asGoal || m.AsGoal
		}
		// Goal mode only accepts text objectives. Attachments retain ordinary Send
		// semantics even on always instances, without changing an existing goal.
		if asGoal && !watchdog && !goal.Active() && len(images)+len(files) == 0 {
			_, err = s.SetGoal(strings.Join(objectives, "\n\n"))
		} else {
			err = s.Send(strings.Join(prompts, "\n\n"), head.MessageID, images, files)
		}
		status = core.DeferredSent
	} else {
		err = s.SendMidTurn(head.Prompt, head.MessageID, nil, nil)
	}
	s.queueMu.Lock()
	s.queueInFlight -= len(batch)
	canceled := head.epoch != s.queueEpoch || !s.alive.Load()
	if err != nil && !fallback && !canceled {
		s.messageQueue = append([]*deferredMessage{head}, s.messageQueue...)
		s.queueMisses++
	}
	s.queueMu.Unlock()
	if err != nil && !fallback && !canceled {
		slog.Info("codex queued steer deferred to next turn", "msg_id", head.MessageID, "turn_id", turn, "error", err)
		return false, nil
	}
	return true, func() {
		for _, m := range batch {
			if canceled {
				if m.Notify != nil {
					m.Notify(core.DeferredCanceled, nil)
				}
				continue
			}
			slog.Info("codex deferred message delivered", "msg_id", m.MessageID, "disposition", status, "error", err)
			if m.Notify != nil {
				m.Notify(status, err)
			}
		}
	}
}

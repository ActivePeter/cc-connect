package core

import (
	"context"
	"log/slog"
	"strings"
	"time"
)

// FinalReplyNotifier is an optional platform capability for notifying the
// requester after a terminal, visible agent reply has been delivered. replyCtx
// belongs to that turn, including when messages are queued in a shared session.
// It is not called for intermediate progress, commands, or cancelled turns.
type FinalReplyNotifier interface {
	NotifyFinalReply(ctx context.Context, replyCtx any, message string) error
}

func (e *Engine) notifyFinalReply(p Platform, replyCtx any) {
	notifier, ok := p.(FinalReplyNotifier)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(e.ctx, 15*time.Second)
	defer cancel()
	if err := notifier.NotifyFinalReply(ctx, replyCtx, e.i18n.T(MsgFinalReplyComplete)); err != nil {
		// A failed notification must not cause the final answer to be resent.
		slog.Warn("final reply notification failed", "platform", p.Name(), "error", err)
	}
}

func hasVisibleFinalReply(content string) bool {
	if isSilentReply(content) {
		return false
	}
	if stripped, ok := stripTrailingSilent(content); ok {
		content = stripped
	}
	return strings.TrimSpace(content) != ""
}

package feishu

import (
	"context"
	"fmt"
	"html"
	"strings"

	"github.com/chenhg5/cc-connect/core"
)

var _ core.FinalReplyNotifier = (*Platform)(nil)
var _ core.FinalReplyNotifier = (*interactivePlatform)(nil)

// NotifyFinalReply sends a separate native text mention. Finalizing an existing
// progress card does not reliably trigger a new mention notification in Feishu.
func (p *Platform) NotifyFinalReply(ctx context.Context, replyCtx any, message string) error {
	if !p.mentionRequesterOnCompletion {
		return nil
	}
	rc, ok := replyCtx.(replyContext)
	if !ok || !rc.groupChat || rc.chatID == "" {
		return nil
	}
	target, name := rc.requesterID, rc.requesterName
	if target == "" {
		// Bot-triggered (or unknown-sender) turn: fall back to the configured
		// human so someone is still notified. Never mention the bot that
		// triggered the turn, or two bots would keep waking each other up.
		target = p.completionFallbackMention
		if target == "" || strings.EqualFold(target, rc.senderID) {
			return nil
		}
		name = p.resolveUserName(target)
	}
	if strings.EqualFold(target, "all") {
		return nil
	}
	if name == "" {
		name = target
	}
	content := fmt.Sprintf(`<at user_id="%s">%s</at> %s`, html.EscapeString(target), html.EscapeString(name), html.EscapeString(message))
	// Reuse normal routing so thread replies and reply_to_trigger retain their
	// configured behavior. The native <at> tag selects Feishu's text message API.
	if err := p.Reply(ctx, rc, content); err != nil {
		return fmt.Errorf("%s: notify final reply: %w", p.tag(), err)
	}
	return nil
}

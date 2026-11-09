package feishu

import (
	"context"
	"fmt"
	"time"

	"github.com/chenhg5/cc-connect/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

// A chat's group-versus-p2p identity does not change when a group switches
// between ordinary and topic mode. Cache only confirmed classifications; a
// failed lookup must remain retryable, never become a negative (p2p) cache hit.
func (p *Platform) cacheEventChatType(chatID, chatType string) {
	if chatID == "" {
		return
	}
	switch chatType {
	case "group":
		p.groupChatCache.Store(chatID, true)
	case "p2p":
		p.groupChatCache.Store(chatID, false)
	}
}

// resolveGroupChat recovers the metadata absent from persisted session keys.
// Both private chats and groups use oc_ IDs, so the key cannot establish type.
func (p *Platform) resolveGroupChat(ctx context.Context, chatID string) (bool, error) {
	if chatID == "" {
		return false, fmt.Errorf("%s: chat ID is required", p.tag())
	}
	if group, ok := p.groupChatCache.Load(chatID); ok {
		return group.(bool), nil
	}
	if p.client == nil {
		return false, fmt.Errorf("%s: cannot determine chat mode: API client unavailable", p.tag())
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := p.client.Im.Chat.Get(ctx, larkim.NewGetChatReqBuilder().ChatId(chatID).Build())
	if err != nil {
		return false, fmt.Errorf("%s: determine chat mode: %w", p.tag(), err)
	}
	if resp == nil || !resp.Success() || resp.Data == nil {
		if resp == nil {
			return false, fmt.Errorf("%s: determine chat mode: empty response", p.tag())
		}
		return false, fmt.Errorf("%s: determine chat mode: code=%d msg=%s", p.tag(), resp.Code, resp.Msg)
	}
	// ChatType is private/public (group visibility), not group/p2p. The official
	// SDK's ChatMode is group/topic/p2p and is the discriminator we need.
	var group bool
	switch stringValue(resp.Data.ChatMode) {
	case "group", "topic":
		group = true
	case "p2p":
		group = false
	default:
		return false, fmt.Errorf("%s: determine chat mode: unsupported or missing chat_mode", p.tag())
	}
	p.groupChatCache.Store(chatID, group)
	return group, nil
}

// ResolveTopicInfo may query missing chat type only for explicit topic creation.
// TopicInfo remains a pure projection for inbound routing and topic prefixes.
func (p *Platform) ResolveTopicInfo(ctx context.Context, reply any) (core.TopicInfo, error) {
	rc, ok := reply.(replyContext)
	if !ok || rc.chatID == "" {
		return core.TopicInfo{}, fmt.Errorf("%s: topic creation requires a chat context", p.tag())
	}
	if !rc.groupChat {
		group, err := p.resolveGroupChat(ctx, rc.chatID)
		if err != nil {
			return core.TopicInfo{}, err
		}
		rc.groupChat = group
	}
	return p.TopicInfo(rc), nil
}

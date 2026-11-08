package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/chenhg5/cc-connect/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func (p *Platform) threadRoutingEnabled() bool { return p.threadIsolation || p.topicOnly }
func (p *Platform) TopicSession(ctx any) (string, string, int) {
	rc, ok := ctx.(replyContext)
	if !ok || !p.topicOnly || rc.threadID == "" || !rc.groupChat {
		return "", "", 0
	}
	limit := p.topicConcurrency
	if limit < 1 {
		limit = 4
	}
	return rc.chatID, rc.threadID, limit
}
func (p *Platform) TopicInfo(ctx any) core.TopicInfo {
	rc, ok := ctx.(replyContext)
	if !ok || !rc.groupChat {
		return core.TopicInfo{}
	}
	return core.TopicInfo{ChatID: rc.chatID, ThreadID: rc.threadID, RootID: rc.rootID}
}
func (p *Platform) resolveTopicTarget(to string) (string, error) {
	if id := p.mentionMap[to]; id != "" {
		to = id
	}
	if !strings.HasPrefix(to, "ou_") {
		return "", fmt.Errorf("topic target must be an open_id or configured mention_map name")
	}
	for _, r := range to {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return "", fmt.Errorf("invalid topic target")
		}
	}
	if to == p.getBotOpenID() {
		return "", fmt.Errorf("topic target must be another agent")
	}
	return to, nil
}
func (p *Platform) CreateTopic(ctx context.Context, reply any, to, title, body, request string) (core.TopicReceipt, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	info := p.TopicInfo(reply)
	out := core.TopicReceipt{Status: "failed"}
	if info.ChatID == "" {
		return out, fmt.Errorf("topic creation requires a group")
	}
	target, err := p.resolveTopicTarget(to)
	if err != nil {
		return out, err
	}
	if strings.ContainsAny(title, "\r\n") {
		return out, fmt.Errorf("topic title must be one line")
	}
	rootText, _ := json.Marshal(map[string]string{"text": "【子任务 " + title + "】"})
	root, err := p.client.Im.Message.Create(ctx, larkim.NewCreateMessageReqBuilder().ReceiveIdType("chat_id").
		Body(larkim.NewCreateMessageReqBodyBuilder().ReceiveId(info.ChatID).MsgType("text").Content(string(rootText)).Uuid(request+"-root").Build()).Build())
	if err != nil {
		return out, fmt.Errorf("create topic root: %w", err)
	}
	if !root.Success() || root.Data == nil {
		return out, fmt.Errorf("create topic root: code=%d msg=%s", root.Code, root.Msg)
	}
	out.RootID = stringValue(root.Data.MessageId)
	if out.RootID == "" {
		return out, fmt.Errorf("create topic root returned no message_id")
	}
	text, _ := json.Marshal(map[string]string{"text": fmt.Sprintf("<at user_id=\"%s\"></at>\n%s", target, body)})
	child, err := p.client.Im.Message.Reply(ctx, larkim.NewReplyMessageReqBuilder().MessageId(out.RootID).
		Body(larkim.NewReplyMessageReqBodyBuilder().MsgType("text").Content(string(text)).ReplyInThread(true).Uuid(request+"-reply").Build()).Build())
	if err != nil {
		return out, fmt.Errorf("reply to topic root %s: %w", out.RootID, err)
	}
	if !child.Success() || child.Data == nil {
		return out, fmt.Errorf("reply to topic root %s: code=%d msg=%s", out.RootID, child.Code, child.Msg)
	}
	out.MessageID = stringValue(child.Data.MessageId)
	out.ThreadID = stringValue(child.Data.ThreadId)
	if out.ThreadID == "" {
		return out, fmt.Errorf("reply accepted but thread_id missing; root=%s reply=%s", out.RootID, out.MessageID)
	}
	out.Status = "sent"
	return out, nil
}
func (p *Platform) topicMessages(ctx context.Context, thread string) ([]*larkim.Message, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := p.client.Im.Message.List(ctx, larkim.NewListMessageReqBuilder().ContainerIdType("thread").ContainerId(thread).SortType("ByCreateTimeDesc").PageSize(1).Build())
	if err != nil {
		return nil, err
	}
	if !resp.Success() {
		return nil, fmt.Errorf("topic list: code=%d msg=%s", resp.Code, resp.Msg)
	}
	if resp.Data == nil {
		return nil, nil
	}
	return resp.Data.Items, nil
}
func (p *Platform) TopicLatest(ctx context.Context, thread string) (core.TopicLatest, error) {
	items, err := p.topicMessages(ctx, thread)
	if err != nil {
		return core.TopicLatest{}, err
	}
	if len(items) == 0 {
		return core.TopicLatest{}, nil
	}
	m := items[0]
	sender := ""
	if m.Sender != nil {
		sender = stringValue(m.Sender.Id)
	}
	return core.TopicLatest{Sender: sender, Time: stringValue(m.CreateTime)}, nil
}
func (p *Platform) TopicTitle(ctx context.Context, root string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := p.client.Im.Message.Get(ctx, larkim.NewGetMessageReqBuilder().MessageId(root).Build())
	if err != nil {
		return "", err
	}
	if !resp.Success() || resp.Data == nil || len(resp.Data.Items) == 0 {
		return "", fmt.Errorf("topic root unavailable")
	}
	m := resp.Data.Items[0]
	if m.Body == nil || m.Body.Content == nil {
		return "", fmt.Errorf("topic root body unavailable")
	}
	var b struct {
		Text string `json:"text"`
	}
	if err = json.Unmarshal([]byte(*m.Body.Content), &b); err != nil {
		return "", err
	}
	return strings.TrimSuffix(strings.TrimPrefix(b.Text, "【子任务 "), "】"), nil
}

package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chenhg5/cc-connect/core"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestTopicOnlySessionKeysAndLegacyRegression(t *testing.T) {
	cases := []struct {
		name string
		msg  *larkim.EventMessage
		want string
	}{
		{"top", &larkim.EventMessage{ChatType: strPtr("group"), MessageId: strPtr("om_top")}, "feishu:oc_chat:ou_user"},
		{"reply", &larkim.EventMessage{ChatType: strPtr("group"), RootId: strPtr("om_root"), ParentId: strPtr("om_parent")}, "feishu:oc_chat:ou_user"},
		{"topic", &larkim.EventMessage{ChatType: strPtr("group"), ThreadId: strPtr("omt_thread"), RootId: strPtr("om_root")}, "feishu:oc_chat:thread:omt_thread"},
		{"p2p", &larkim.EventMessage{ChatType: strPtr("p2p"), ThreadId: strPtr("omt_thread")}, "feishu:oc_chat:ou_user"},
	}
	p := &Platform{platformName: "feishu", topicOnly: true}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := p.makeSessionKey(c.msg, "oc_chat", "ou_user"); got != c.want {
				t.Fatalf("%s != %s", got, c.want)
			}
		})
	}
	p.shareSessionInChannel = true
	if got := p.makeSessionKey(cases[1].msg, "oc_chat", "ou_user"); got != "feishu:oc_chat" {
		t.Fatal(got)
	}
	p.topicOnly = false
	p.threadIsolation = true
	if got := p.makeSessionKey(cases[0].msg, "oc_chat", "ou_user"); got != "feishu:oc_chat:root:om_top" {
		t.Fatal(got)
	}
	if got := p.makeSessionKey(cases[2].msg, "oc_chat", "ou_user"); got != "feishu:oc_chat:root:om_root" {
		t.Fatal(got)
	}
	p.threadIsolation = false
	if got := p.makeSessionKey(cases[2].msg, "oc_chat", "ou_user"); got != "feishu:oc_chat" {
		t.Fatal(got)
	}
}
func TestTopicOnlyOptions(t *testing.T) {
	for _, o := range []map[string]any{
		{"thread_isolation_mode": "bad"},
		{"thread_isolation_mode": "topic_only", "thread_isolation": true},
		{"thread_isolation_mode": "topic_only", "topic_concurrency": 0},
		{"thread_isolation_mode": "topic_only", "topic_concurrency": 65},
	} {
		o["app_id"] = "id"
		o["app_secret"] = "secret"
		if _, err := New(o); err == nil {
			t.Fatalf("accepted %#v", o)
		}
	}
	pAny, err := New(map[string]any{"app_id": "id", "app_secret": "secret", "enable_feishu_card": false, "thread_isolation_mode": "topic_only"})
	if err != nil {
		t.Fatal(err)
	}
	p := pAny.(*Platform)
	if !p.topicOnly || p.topicConcurrency != 4 {
		t.Fatal("default policy")
	}
	rc := replyContext{groupChat: true, chatID: "oc_chat", threadID: "omt_topic", sessionKey: "feishu:oc_chat:thread:omt_topic", messageID: "om_reply"}
	p.noReplyToTrigger = true
	if !p.shouldUseThreadOrReplyAPI(rc) {
		t.Fatal("topic escaped to chat with reply suppression")
	}
	body := p.buildReplyMessageReqBody(rc, "text", "hello")
	if body.ReplyInThread == nil || !*body.ReplyInThread {
		t.Fatal("reply_in_thread missing")
	}
	if !p.markThreadSessionActive(rc.sessionKey) || !p.isActiveThreadSession(rc.sessionKey) {
		t.Fatal("attachment continuation missing")
	}
}
func TestTopicSDKCreationAndPartialReceipt(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			var rootCalls, replyCalls int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/open-apis/auth/v3/tenant_access_token/internal":
					writeJSON(t, w, map[string]any{"code": 0, "expire": 7200, "tenant_access_token": "test-topic-token"})
				case "/open-apis/im/v1/messages":
					rootCalls++
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					content, _ := body["content"].(string)
					if strings.Contains(content, "<at") || !strings.Contains(content, "子任务 Title") {
						t.Errorf("root mention/title: %s", content)
					}
					writeJSON(t, w, map[string]any{"code": 0, "data": map[string]any{"message_id": "om_root"}})
				case "/open-apis/im/v1/messages/om_root/reply":
					replyCalls++
					var body map[string]any
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					var text struct{ Text string }
					_ = json.Unmarshal([]byte(body["content"].(string)), &text)
					if body["reply_in_thread"] != true || !strings.HasPrefix(text.Text, "<at user_id=\"ou_executor\"></at>\nSPEC") {
						t.Errorf("reply=%#v text=%s", body, text.Text)
					}
					if fail {
						writeJSON(t, w, map[string]any{"code": 999, "msg": "rejected"})
						return
					}
					writeJSON(t, w, map[string]any{"code": 0, "data": map[string]any{"message_id": "om_reply", "thread_id": "omt_created"}})
				default:
					t.Errorf("unexpected %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer srv.Close()
			p := &Platform{platformName: "feishu", botOpenID: "ou_commander", mentionMap: map[string]string{"executor": "ou_executor"}, client: lark.NewClient("topic"+fmt.Sprint(fail), "secret", lark.WithOpenBaseUrl(srv.URL), lark.WithHttpClient(srv.Client()))}
			out, err := p.CreateTopic(context.Background(), replyContext{chatID: "oc_chat", groupChat: true}, "executor", "Title", "SPEC", strings.Repeat("a", 32))
			if rootCalls != 1 || replyCalls != 1 || out.RootID != "om_root" {
				t.Fatalf("partial receipt=%+v calls=%d/%d", out, rootCalls, replyCalls)
			}
			if fail {
				if err == nil || out.Status == "sent" {
					t.Fatal("failed reply accepted")
				}
			} else if err != nil || out.ThreadID != "omt_created" || out.Status != "sent" {
				t.Fatalf("%+v %v", out, err)
			}
			if _, err = p.resolveTopicTarget("ou_commander"); err == nil {
				t.Fatal("self target accepted")
			}
			if _, err = p.resolveTopicTarget("unknown"); err == nil {
				t.Fatal("unknown alias accepted")
			}
		})
	}
}
func TestTopicOnlyBotEventRoutesWithActualThreadID(t *testing.T) {
	got := make(chan *core.Message, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "tenant_access_token") {
			writeJSON(t, w, map[string]any{"code": 0, "expire": 7200, "tenant_access_token": "test-topic-event-token"})
			return
		}
		writeJSON(t, w, map[string]any{"code": 0, "data": map[string]any{"items": []any{map[string]any{"msg_type": "text", "body": map[string]string{"content": "{\"text\":\"ROOT\"}"}}}}})
	}))
	defer srv.Close()
	p := &Platform{platformName: "feishu", topicOnly: true, botOpenID: "ou_executor", dedup: &core.MessageDedup{}, client: lark.NewClient("topic-event", "secret", lark.WithOpenBaseUrl(srv.URL), lark.WithHttpClient(srv.Client())), handler: func(_ core.Platform, m *core.Message) { got <- m }}
	content := "{\"text\":\"@_user_1 SPEC\"}"
	err := p.onMessage(context.Background(), &larkim.P2MessageReceiveV1{Event: &larkim.P2MessageReceiveV1Data{
		Sender:  &larkim.EventSender{SenderId: &larkim.UserId{OpenId: strPtr("ou_commander")}, SenderType: strPtr("app")},
		Message: &larkim.EventMessage{MessageId: strPtr("om_child"), RootId: strPtr("om_root"), ThreadId: strPtr("omt_actual"), ChatId: strPtr("oc_chat"), ChatType: strPtr("group"), MessageType: strPtr("text"), Content: &content, CreateTime: strPtr(strconv.FormatInt(time.Now().Add(time.Second).UnixMilli(), 10)), Mentions: []*larkim.MentionEvent{{Key: strPtr("@_user_1"), Id: &larkim.UserId{OpenId: strPtr("ou_executor")}}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		if m.SessionKey != "feishu:oc_chat:thread:omt_actual" {
			t.Fatal(m.SessionKey)
		}
		rc := m.ReplyCtx.(replyContext)
		if rc.threadID != "omt_actual" || !p.shouldReplyInThread(rc) {
			t.Fatalf("%+v", rc)
		}
		main, err := p.ReconstructReplyCtx("feishu:oc_chat:ou_commander")
		if err != nil || p.TopicInfo(main).ChatID != "oc_chat" {
			t.Fatalf("inbound event did not retain group type: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("bot mention not delivered")
	}
}

func TestTopicOnlyOnMessageAttachmentWithoutMention(t *testing.T) {
	const appID = "cli_topic_admit"
	const appSecret = "secret-thread-admit"
	const botOpenID = "ou_bot"
	const userOpenID = "ou_user"
	const chatID = "oc_chat"
	const rootMsgID = "om_root"
	const imageKey = "img_in_thread"

	imageBytes := []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/open-apis/auth/v3/tenant_access_token/internal":
			w.Header().Set("Content-Type", "application/json")
			writeJSON(t, w, map[string]any{
				"code":                0,
				"msg":                 "success",
				"expire":              7200,
				"tenant_access_token": "tenant-token",
			})
		case strings.HasSuffix(r.URL.Path, "/resources/"+imageKey):
			w.Header().Set("Content-Type", "image/png")
			if _, err := w.Write(imageBytes); err != nil {
				t.Fatalf("write image: %v", err)
			}
		default:
			// Unknown calls (e.g. user/chat info lookups) return empty success
			// so dispatch can continue past optional metadata fetches.
			w.Header().Set("Content-Type", "application/json")
			writeJSON(t, w, map[string]any{"code": 0, "msg": "success", "data": map[string]any{}})
		}
	}))
	defer srv.Close()

	received := make(chan *core.Message, 8)
	p := &Platform{
		platformName: "feishu",
		domain:       srv.URL,
		appID:        appID,
		appSecret:    appSecret,
		botOpenID:    botOpenID,
		topicOnly:    true,
		dedup:        &core.MessageDedup{},
		client: lark.NewClient(appID, appSecret,
			lark.WithOpenBaseUrl(srv.URL),
			lark.WithHttpClient(srv.Client()),
		),
		handler: func(_ core.Platform, msg *core.Message) {
			received <- msg
		},
	}

	chatType := "group"
	senderType := "user"
	now := time.Now().UnixMilli()
	createTime := func() *string {
		s := strconv.FormatInt(now, 10)
		now++
		return &s
	}

	buildEvent := func(msgID, msgType, content string, mentions []*larkim.MentionEvent, rootID string) *larkim.P2MessageReceiveV1 {
		ev := &larkim.P2MessageReceiveV1{
			Event: &larkim.P2MessageReceiveV1Data{
				Sender: &larkim.EventSender{
					SenderId:   &larkim.UserId{OpenId: stringPtr(userOpenID)},
					SenderType: &senderType,
				},
				Message: &larkim.EventMessage{
					MessageId:   stringPtr(msgID),
					ChatId:      stringPtr(chatID),
					ChatType:    &chatType,
					MessageType: stringPtr(msgType),
					Content:     stringPtr(content),
					CreateTime:  createTime(),
					Mentions:    mentions,
				},
			},
		}
		if msgID == rootMsgID || rootID == rootMsgID {
			ev.Event.Message.ThreadId = stringPtr("omt_actual")
		} else if rootID != "" {
			ev.Event.Message.ThreadId = stringPtr("omt_other")
		}
		if rootID != "" {
			ev.Event.Message.RootId = stringPtr(rootID)
		}
		return ev
	}

	botMention := []*larkim.MentionEvent{
		{
			Key:  stringPtr("@_user_1"),
			Id:   &larkim.UserId{OpenId: stringPtr(botOpenID)},
			Name: stringPtr("claude code"),
		},
	}

	// Step 1: opening message @mentions the bot — establishes the thread.
	if err := p.onMessage(context.Background(), buildEvent(rootMsgID, "text", `{"text":"@_user_1 看看这个"}`, botMention, "")); err != nil {
		t.Fatalf("onMessage(root) error = %v", err)
	}
	threadKey := "feishu:" + chatID + ":thread:omt_actual"
	if !p.isActiveThreadSession(threadKey) {
		t.Fatalf("thread %q should be marked active after @bot text", threadKey)
	}
	select {
	case <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the opening @bot text to dispatch")
	}

	// Step 2: follow-up image in the same thread, no @mention — should pass through.
	imgContent := `{"image_key":"` + imageKey + `"}`
	if err := p.onMessage(context.Background(), buildEvent("om_thread_img", "image", imgContent, nil, rootMsgID)); err != nil {
		t.Fatalf("onMessage(image in thread) error = %v", err)
	}
	select {
	case msg := <-received:
		if msg.MessageID != "om_thread_img" {
			t.Fatalf("expected dispatched image om_thread_img, got %q", msg.MessageID)
		}
		if len(msg.Images) != 1 {
			t.Fatalf("expected dispatched image to carry 1 attachment, got %d", len(msg.Images))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected image in active thread to be dispatched without @mention")
	}

	// Step 3: image in a *different* thread without @mention — should still be dropped.
	if err := p.onMessage(context.Background(), buildEvent("om_other_img", "image", imgContent, nil, "om_other_root")); err != nil {
		t.Fatalf("onMessage(image in unrelated thread) error = %v", err)
	}
	select {
	case msg := <-received:
		t.Fatalf("image in unrelated thread should be dropped, but handler received %q", msg.MessageID)
	case <-time.After(300 * time.Millisecond):
		// expected: nothing dispatched
	}

	// Step 4: text without @mention in the active thread — should be dropped
	// (only attachments are admitted; otherwise unrelated thread chatter would
	// flood the agent).
	if err := p.onMessage(context.Background(), buildEvent("om_thread_text", "text", `{"text":"刚才那张图能看到吗"}`, nil, rootMsgID)); err != nil {
		t.Fatalf("onMessage(text in thread without mention) error = %v", err)
	}
	select {
	case msg := <-received:
		t.Fatalf("text in active thread without @mention should be dropped, got %q", msg.MessageID)
	case <-time.After(300 * time.Millisecond):
		// expected
	}
}

func TestTopicOnlyColdReplyContextUsesMessageAnchor(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "tenant_access_token") {
			writeJSON(t, w, map[string]any{"code": 0, "expire": 7200, "tenant_access_token": "cold-topic-token"})
			return
		}
		if r.URL.Path == "/open-apis/im/v1/chats/oc_chat" {
			writeJSON(t, w, map[string]any{"code": 0, "data": map[string]any{"chat_mode": "group"}})
			return
		}
		if r.URL.Query().Get("container_id_type") != "thread" || r.URL.Query().Get("container_id") != "omt_topic" {
			t.Errorf("query %s", r.URL.RawQuery)
		}
		writeJSON(t, w, map[string]any{"code": 0, "data": map[string]any{"items": []any{map[string]any{"message_id": "om_latest", "root_id": "om_root"}}}})
	}))
	defer srv.Close()
	p := &Platform{platformName: "feishu", topicOnly: true, client: lark.NewClient("topic-cold", "secret", lark.WithOpenBaseUrl(srv.URL), lark.WithHttpClient(srv.Client()))}
	ctx, err := p.ReconstructReplyCtx("feishu:oc_chat:thread:omt_topic")
	if err != nil {
		t.Fatal(err)
	}
	rc := ctx.(replyContext)
	if rc.messageID != "om_latest" || rc.threadID != "omt_topic" || !p.shouldReplyInThread(rc) {
		t.Fatalf("invalid reply anchor: %+v", rc)
	}
}

package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func TestCompletionMention_NativeTextAndRouting(t *testing.T) {
	for _, route := range []string{"reply", "thread", "new message"} {
		t.Run(route, func(t *testing.T) {
			type request struct {
				path string
				body map[string]any
			}
			requests := make(chan request, 2)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if strings.Contains(r.URL.Path, "/auth/") {
					writeJSON(t, w, map[string]any{"code": 0, "expire": 7200, "tenant_access_token": "test-token"})
					return
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				requests <- request{r.URL.Path, body}
				writeJSON(t, w, map[string]any{"code": 0, "data": map[string]any{"message_id": "om_result"}})
			}))
			defer srv.Close()
			p := &Platform{
				platformName: "feishu", mentionRequesterOnCompletion: true,
				threadIsolation: route == "thread", noReplyToTrigger: route == "new message",
				client: lark.NewClient("completion-"+route, "test-secret", lark.WithOpenBaseUrl(srv.URL), lark.WithHttpClient(srv.Client())),
			}
			rc := replyContext{messageID: "om_trigger", chatID: "oc_group", sessionKey: "feishu:oc_group:root:om_trigger", groupChat: true, requesterID: "ou_alice", requesterName: `Alice <&>"`}
			if err := p.NotifyFinalReply(context.Background(), rc, "Reply complete."); err != nil {
				t.Fatal(err)
			}
			select {
			case req := <-requests:
				wantPath := "/open-apis/im/v1/messages/om_trigger/reply"
				if route == "new message" {
					wantPath = "/open-apis/im/v1/messages"
				}
				if req.path != wantPath || req.body["msg_type"] != "text" {
					t.Fatalf("unexpected request: %+v", req)
				}
				if (req.body["reply_in_thread"] == true) != (route == "thread") {
					t.Fatalf("wrong thread routing: %+v", req.body)
				}
				var content map[string]string
				if err := json.Unmarshal([]byte(req.body["content"].(string)), &content); err != nil {
					t.Fatal(err)
				}
				want := `<at user_id="ou_alice">Alice &lt;&amp;&gt;&#34;</at> Reply complete.`
				if content["text"] != want {
					t.Fatalf("text=%q, want %q", content["text"], want)
				}
			default:
				t.Fatal("no native mention sent")
			}
			if len(requests) != 0 {
				t.Fatal("duplicate completion notification")
			}
		})
	}
}

func TestCompletionMention_OptInAndHumanGroupOnly(t *testing.T) {
	for _, tc := range []struct {
		name           string
		enabled, group bool
		id             string
	}{
		{"disabled", false, true, "ou_alice"},
		{"private", true, false, "ou_alice"},
		{"unknown or bot requester", true, true, ""},
		{"never everyone", true, true, "all"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A nil API client proves no send is attempted in these cases.
			p := &Platform{mentionRequesterOnCompletion: tc.enabled}
			if err := p.NotifyFinalReply(context.Background(), replyContext{chatID: "oc_group", groupChat: tc.group, requesterID: tc.id}, "Complete"); err != nil {
				t.Fatal(err)
			}
		})
	}
	for _, interactive := range []bool{false, true} {
		for _, enabled := range []bool{false, true} {
			p, err := newPlatform("feishu", lark.FeishuBaseUrl, map[string]any{"app_id": "test", "app_secret": "test", "enable_feishu_card": interactive, "mention_requester_on_completion": enabled})
			if err != nil {
				t.Fatal(err)
			}
			base, ok := p.(*Platform)
			if !ok {
				base = p.(*interactivePlatform).Platform
			}
			if base.mentionRequesterOnCompletion != enabled {
				t.Fatal("completion option not preserved")
			}
			if _, ok := p.(core.FinalReplyNotifier); !ok {
				t.Fatal("missing completion capability")
			}
		}
	}
}

func TestCompletionMention_CapturesEachInboundRequester(t *testing.T) {
	received := make(chan *core.Message, 4)
	p := &Platform{platformName: "feishu", dedup: &core.MessageDedup{}, groupReplyAll: true,
		handler: func(_ core.Platform, m *core.Message) { received <- m }}
	p.chatNameCache.Store("oc_group", "Test group")
	for i, senderType := range []string{"user", "user", "app"} {
		id := fmt.Sprintf("ou_sender%d", i)
		p.userNameCache.Store(id, "Sender "+id)
		err := p.onMessage(context.Background(), &larkim.P2MessageReceiveV1{Event: &larkim.P2MessageReceiveV1Data{
			Sender:  &larkim.EventSender{SenderId: &larkim.UserId{OpenId: strPtr(id)}, SenderType: strPtr(senderType)},
			Message: &larkim.EventMessage{MessageId: strPtr(fmt.Sprintf("om_%d", i)), ChatId: strPtr("oc_group"), ChatType: strPtr("group"), MessageType: strPtr("text"), Content: strPtr(`{"text":"Please finish this"}`)},
		}})
		if err != nil {
			t.Fatal(err)
		}
		select {
		case m := <-received:
			rc := m.ReplyCtx.(replyContext)
			want := id
			if senderType != "user" {
				want = ""
			}
			if rc.requesterID != want || !rc.groupChat || rc.requesterName != "Sender "+id {
				t.Fatalf("wrong requester context: %+v", rc)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("message was not dispatched")
		}
	}
}

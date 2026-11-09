package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chenhg5/cc-connect/core"
	lark "github.com/larksuite/oapi-sdk-go/v3"
)

type contextFixture struct {
	server  *httptest.Server
	lookups atomic.Int32
	roots   atomic.Int32
	replies atomic.Int32
	sends   atomic.Int32
	mode    string
	fail    atomic.Bool
	timeout atomic.Bool
}

func newContextFixture(t *testing.T, mode string) *contextFixture {
	t.Helper()
	f := &contextFixture{mode: mode}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "tenant_access_token"):
			writeJSON(t, w, map[string]any{"code": 0, "expire": 7200, "tenant_access_token": "f1-fixture-token"})
		case r.URL.Path == "/open-apis/im/v1/chats/oc_context":
			f.lookups.Add(1)
			if f.timeout.Load() {
				<-r.Context().Done()
				return
			}
			if f.fail.Load() {
				writeJSON(t, w, map[string]any{"code": 999, "msg": "fixture metadata denied"})
				return
			}
			// chat_type is deliberately private for every mode, including p2p;
			// it is visibility metadata, never evidence that a chat is a group.
			writeJSON(t, w, map[string]any{"code": 0, "data": map[string]any{"chat_mode": f.mode, "chat_type": "private"}})
		case r.URL.Path == "/open-apis/im/v1/messages":
			var body struct {
				Content   string `json:"content"`
				ReceiveID string `json:"receive_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			var content struct {
				Text string `json:"text"`
			}
			_ = json.Unmarshal([]byte(body.Content), &content)
			if body.ReceiveID != "oc_context" {
				t.Errorf("wrong destination: %s", body.ReceiveID)
			}
			if strings.HasPrefix(content.Text, "【子任务 ") {
				f.roots.Add(1)
				if strings.Contains(content.Text, "<at") {
					t.Error("root unexpectedly mentions executor")
				}
			} else {
				f.sends.Add(1)
			}
			writeJSON(t, w, map[string]any{"code": 0, "data": map[string]any{"message_id": "om_context_root"}})
		case r.URL.Path == "/open-apis/im/v1/messages/om_context_root/reply":
			f.replies.Add(1)
			var body struct {
				Content       string `json:"content"`
				ReplyInThread bool   `json:"reply_in_thread"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			var content struct {
				Text string `json:"text"`
			}
			_ = json.Unmarshal([]byte(body.Content), &content)
			if !body.ReplyInThread || !strings.HasPrefix(content.Text, "<at user_id=\"ou_executor\"></at>\nSPEC") {
				t.Errorf("invalid topic body: %+v", body)
			}
			writeJSON(t, w, map[string]any{"code": 0, "data": map[string]any{"message_id": "om_context_reply", "thread_id": "omt_context"}})
		default:
			t.Errorf("unexpected SDK call: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(f.server.Close)
	return f
}
func (f *contextFixture) platform() *Platform {
	return &Platform{platformName: "feishu", botOpenID: "ou_commander", mentionMap: map[string]string{"executor": "ou_executor"}, client: lark.NewClient("f1-context-fixture", "secret", lark.WithOpenBaseUrl(f.server.URL), lark.WithHttpClient(f.server.Client()))}
}

type contextAgent struct{ onSend func() }

func (*contextAgent) Name() string { return "f1-fixture" }
func (a *contextAgent) StartSession(context.Context, string) (core.AgentSession, error) {
	return &contextSession{onSend: a.onSend, events: make(chan core.Event, 1)}, nil
}
func (*contextAgent) ListSessions(context.Context) ([]core.AgentSessionInfo, error) { return nil, nil }
func (*contextAgent) Stop() error                                                   { return nil }

type contextSession struct {
	onSend func()
	events chan core.Event
	closed atomic.Bool
}

func (s *contextSession) Send(string, string, []core.ImageAttachment, []core.FileAttachment) error {
	if s.onSend != nil {
		s.onSend()
	}
	s.events <- core.Event{Type: core.EventResult, Content: "F1_TIMER_DONE", Done: true}
	return nil
}
func (*contextSession) RespondPermission(string, core.PermissionResult) error { return nil }
func (s *contextSession) Events() <-chan core.Event                           { return s.events }
func (*contextSession) CurrentSessionID() string                              { return "native-f1-context" }
func (s *contextSession) Alive() bool                                         { return !s.closed.Load() }
func (s *contextSession) Close() error                                        { s.closed.Store(true); return nil }

func contextEngine(t *testing.T, p *Platform, a *contextAgent, key string) *core.Engine {
	t.Helper()
	data := t.TempDir()
	store := filepath.Join(data, "sessions.json")
	// Persist then reload: no in-memory inbound replyContext survives this.
	sm := core.NewSessionManager(store)
	sm.GetOrCreateActive(key)
	if err := sm.SaveError(); err != nil {
		t.Fatal(err)
	}
	e := core.NewEngine("f1", a, []core.Platform{p}, store, core.LangEnglish)
	e.SetDataDir(data)
	t.Cleanup(func() {
		if err := e.Stop(); err != nil {
			t.Error(err)
		}
	})
	return e
}

// Exercise the same Unix HTTP endpoint used by cc-connect topic new. The
// Engine and Feishu adapter are real; only the agent and remote SDK are fakes.
func contextTopicAPI(t *testing.T, e *core.Engine) func(core.TopicRequest) (core.TopicRecord, error) {
	t.Helper()
	// Keep the Unix socket path below its OS length limit even for long tests.
	dir, err := os.MkdirTemp("", "f1-api-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	api, err := core.NewAPIServer(dir)
	if err != nil {
		t.Fatal(err)
	}
	api.RegisterEngine("f1", e)
	api.Start()
	t.Cleanup(api.Stop)
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", api.SocketPath())
	}}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	return func(req core.TopicRequest) (core.TopicRecord, error) {
		req.Project = "f1"
		body, err := json.Marshal(req)
		if err != nil {
			return core.TopicRecord{}, err
		}
		resp, err := client.Post("http://unix/topics/new", "application/json", bytes.NewReader(body))
		if err != nil {
			return core.TopicRecord{}, err
		}
		defer resp.Body.Close()
		var out struct {
			Topic core.TopicRecord `json:"topic"`
			Error string           `json:"error"`
		}
		if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return out.Topic, err
		}
		if resp.StatusCode != http.StatusOK {
			return out.Topic, fmt.Errorf("HTTP %d: %s", resp.StatusCode, out.Error)
		}
		return out.Topic, nil
	}
}

func TestTopicCreationFromReconstructedGroupContexts(t *testing.T) {
	for _, path := range []string{"timer-mainline", "restart-cold", "send-rebuilt"} {
		t.Run(path, func(t *testing.T) {
			f := newContextFixture(t, "group")
			p := f.platform()
			// A separate, discarded adapter knows the inbound event. The new
			// adapter must recover type from SDK metadata, not inherit a cache.
			prior := f.platform()
			prior.cacheEventChatType("oc_context", "group")
			if _, err := prior.ReconstructReplyCtx("feishu:oc_context:ou_commander"); err != nil {
				t.Fatal(err)
			}
			if f.lookups.Load() != 0 {
				t.Fatal("event type did not seed cache")
			}
			key := "feishu:oc_context:ou_commander"
			req := core.TopicRequest{SessionKey: key, RequestID: strings.Repeat("a", 32), To: "executor", Title: "F1", Body: "SPEC"}
			a := &contextAgent{}
			e := contextEngine(t, p, a, key)
			create := contextTopicAPI(t, e)
			var record core.TopicRecord
			var err error
			if path == "timer-mainline" {
				outcome := make(chan error, 1)
				a.onSend = func() { record, err = create(req); outcome <- err }
				if timerErr := e.ExecuteTimerJob(&core.TimerJob{ID: "f1", Project: "f1", SessionKey: key, Prompt: "CREATE_F1_TOPIC"}); timerErr != nil {
					t.Fatal(timerErr)
				}
				select {
				case err = <-outcome:
				case <-time.After(time.Second):
					t.Fatal("timer did not execute fixture agent")
				}
			} else {
				if path == "send-rebuilt" {
					if err = e.SendToSession(key, "F1_EXTERNAL_SEND"); err != nil {
						t.Fatal(err)
					}
				}
				record, err = create(req)
			}
			if err != nil || record.Status != "sent" || record.ThreadID != "omt_context" || record.ChatID != "oc_context" {
				t.Fatalf("topic=%+v err=%v", record, err)
			}
			if f.lookups.Load() != 1 || f.roots.Load() != 1 || f.replies.Load() != 1 {
				t.Fatalf("SDK calls: lookup=%d roots=%d replies=%d", f.lookups.Load(), f.roots.Load(), f.replies.Load())
			}
			// Cache is shared across timer/send/API reconstruction in one adapter.
			if _, err = p.ReconstructReplyCtx(key); err != nil {
				t.Fatal(err)
			}
			if f.lookups.Load() != 1 {
				t.Fatal("confirmed type was not cached")
			}
		})
	}
}
func TestReconstructedP2PRejectsTopicCreation(t *testing.T) {
	f := newContextFixture(t, "p2p")
	p := f.platform()
	key := "feishu:oc_context:ou_user"
	e := contextEngine(t, p, &contextAgent{}, key)
	create := contextTopicAPI(t, e)
	req := core.TopicRequest{SessionKey: key, RequestID: strings.Repeat("b", 32), To: "executor", Title: "F1", Body: "SPEC"}
	for i := 0; i < 2; i++ {
		if _, err := create(req); err == nil || !strings.Contains(err.Error(), "not a group context") {
			t.Fatalf("p2p accepted or wrong error: %v", err)
		}
	}
	if f.lookups.Load() != 1 || f.roots.Load() != 0 || f.replies.Load() != 0 {
		t.Fatal("p2p type not cached or message sent")
	}
	if err := e.SendToSession(key, "P2P_SEND_STILL_ALLOWED"); err != nil {
		t.Fatal(err)
	}
}
func TestReconstructedChatModeFailureIsNotCachedAsP2P(t *testing.T) {
	for _, mode := range []string{"group", "", "unexpected"} {
		t.Run(fmt.Sprintf("mode=%s", mode), func(t *testing.T) {
			f := newContextFixture(t, mode)
			p := f.platform()
			if mode == "group" {
				f.fail.Store(true)
			}
			key := "feishu:oc_context:ou_user"
			rc, err := p.ReconstructReplyCtx(key)
			if err != nil {
				t.Fatalf("ordinary reconstruction queried metadata: %v", err)
			}
			if f.lookups.Load() != 0 {
				t.Fatal("ordinary reconstruction queried metadata")
			}
			if _, err = p.ResolveTopicInfo(context.Background(), rc); err == nil || !strings.Contains(err.Error(), "determine chat mode") {
				t.Fatalf("metadata error hidden: %v", err)
			}
			if _, ok := p.groupChatCache.Load("oc_context"); ok {
				t.Fatal("unknown cached as p2p")
			}
			if f.roots.Load() != 0 {
				t.Fatal("unknown type sent messages")
			}
			// Retry is meaningful after metadata/permission recovery.
			if mode == "group" {
				f.fail.Store(false)
				if _, err := p.ResolveTopicInfo(context.Background(), rc); err != nil {
					t.Fatal(err)
				}
			} else {
				_, _ = p.ResolveTopicInfo(context.Background(), rc)
			}
			if f.lookups.Load() != 2 {
				t.Fatal("failed classification poisoned cache")
			}
		})
	}
}
func TestChatModeClassificationAndInboundCache(t *testing.T) {
	for _, mode := range []string{"group", "topic", "p2p"} {
		t.Run(mode, func(t *testing.T) {
			f := newContextFixture(t, mode)
			p := f.platform()
			rc, err := p.ReconstructReplyCtx("feishu:oc_context:ou_user")
			if err != nil {
				t.Fatal(err)
			}
			if f.lookups.Load() != 0 {
				t.Fatal("ordinary reconstruction queried metadata")
			}
			info, err := p.ResolveTopicInfo(context.Background(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if got := info.ChatID; (got != "") != (mode != "p2p") {
				t.Fatalf("chat_mode=%s group chat ID=%s", mode, got)
			}
		})
	}
	p := &Platform{platformName: "feishu"} // No API client: events suffice.
	p.cacheEventChatType("oc_group", "group")
	p.cacheEventChatType("oc_p2p", "p2p")
	p.cacheEventChatType("oc_unknown", "private")
	for _, chat := range []string{"oc_group", "oc_p2p"} {
		rc, err := p.ReconstructReplyCtx("feishu:" + chat + ":ou_user")
		if err != nil {
			t.Fatal(err)
		}
		if (p.TopicInfo(rc).ChatID != "") != (chat == "oc_group") {
			t.Fatalf("wrong inbound classification: %s", chat)
		}
	}
	rc, err := p.ReconstructReplyCtx("feishu:oc_unknown:ou_user")
	if err != nil {
		t.Fatalf("unknown metadata blocked ordinary reconstruction: %v", err)
	}
	if _, err = p.ResolveTopicInfo(context.Background(), rc); err == nil {
		t.Fatal("event private visibility guessed as chat type")
	}
}
func TestChatModeLookupHonorsContext(t *testing.T) {
	f := newContextFixture(t, "group")
	p := f.platform()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.resolveGroupChat(ctx, "oc_context"); err == nil {
		t.Fatal("canceled lookup succeeded")
	}
	if _, ok := p.groupChatCache.Load("oc_context"); ok {
		t.Fatal("canceled lookup poisoned cache")
	}
}

// A metadata outage must affect only explicit topic creation, not the normal
// timer/cron/send messages which previously needed no Chat.Get capability.
func TestChatMetadataOutageDoesNotBlockOrdinaryDelivery(t *testing.T) {
	for _, failure := range []string{"denied", "timeout"} {
		t.Run(failure, func(t *testing.T) {
			f := newContextFixture(t, "group")
			if failure == "denied" {
				f.fail.Store(true)
			} else {
				f.timeout.Store(true)
			}
			p := f.platform()
			// A short fixture HTTP timeout exercises the actual SDK's timeout
			// path without making the regression wait for the 5s production cap.
			sdkHTTP := &http.Client{Timeout: 100 * time.Millisecond}
			p.client = lark.NewClient("f1-outage-fixture", "secret", lark.WithOpenBaseUrl(f.server.URL), lark.WithHttpClient(sdkHTTP))
			key := "feishu:oc_context:ou_user"
			e := contextEngine(t, p, &contextAgent{}, key)
			create := contextTopicAPI(t, e)
			if _, err := p.ReconstructReplyCtx(key); err != nil {
				t.Fatal(err)
			}
			if err := e.SendToSession(key, "F1_SEND_DURING_METADATA_OUTAGE"); err != nil {
				t.Fatal(err)
			}
			if err := e.ExecuteTimerJob(&core.TimerJob{ID: "f1-outage-timer", Project: "f1", SessionKey: key, Prompt: "ORDINARY_TIMER"}); err != nil {
				t.Fatal(err)
			}
			if err := e.ExecuteCronJob(&core.CronJob{ID: "f1-outage-cron", Project: "f1", SessionKey: key, Prompt: "ORDINARY_CRON"}); err != nil {
				t.Fatal(err)
			}
			// Every ordinary run sends start + final result, in addition to
			// the explicit external send. No group-classification lookup occurs.
			if f.lookups.Load() != 0 || f.sends.Load() != 5 {
				t.Fatalf("ordinary delivery: metadata=%d sends=%d", f.lookups.Load(), f.sends.Load())
			}
			req := core.TopicRequest{SessionKey: key, RequestID: strings.Repeat("c", 32), To: "executor", Title: "F1", Body: "SPEC"}
			if _, err := create(req); err == nil || !strings.Contains(err.Error(), "determine chat mode") || strings.Contains(err.Error(), "not a group context") {
				t.Fatalf("topic failure lost metadata cause: %v", err)
			}
			if f.lookups.Load() != 1 || f.roots.Load() != 0 || f.replies.Load() != 0 {
				t.Fatalf("topic failure side effects: metadata=%d roots=%d replies=%d", f.lookups.Load(), f.roots.Load(), f.replies.Load())
			}
			// The outage did not mark this unknown chat as p2p or block later
			// sends after a failed topic request in the same live session.
			if _, ok := p.groupChatCache.Load("oc_context"); ok {
				t.Fatal("metadata failure cached as p2p")
			}
			if err := e.SendToSession(key, "F1_SEND_AFTER_FAILED_TOPIC"); err != nil {
				t.Fatal(err)
			}
			if f.lookups.Load() != 1 || f.sends.Load() != 6 {
				t.Fatal("failed topic poisoned ordinary send")
			}
		})
	}
}

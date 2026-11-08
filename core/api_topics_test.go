package core

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTopicAPIEndpoints(t *testing.T) {
	e, p, a := tfEnv(t, 1)
	key := "test:chat:commander"
	e.sessions.GetOrCreateActive(key)
	e.interactiveStates[key] = &interactiveState{platform: p, replyCtx: tfContext{chat: "chat"}}
	api := &APIServer{engines: map[string]*Engine{"test": e}}
	call := func(method, path string, value any) *httptest.ResponseRecorder {
		t.Helper()
		b, _ := json.Marshal(value)
		r := httptest.NewRequest(method, path, bytes.NewReader(b))
		w := httptest.NewRecorder()
		api.handleTopics(w, r)
		return w
	}
	req := TopicRequest{Project: "test", SessionKey: key, RequestID: strings.Repeat("c", 32), To: "executor", Title: "title", Body: "body"}
	w := call("POST", "/topics/new", req)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "thread-created") {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	w = call("GET", "/topics?project=test&session_key="+key, nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"sender":"executor"`) {
		t.Fatalf("list: %s", w.Body.String())
	}
	tfSend(e, p, "one", "ONE")
	tfNext(t, a, "ONE")
	tfSend(e, p, "two", "TWO")
	w = call("GET", "/topics/sessions?project=test&platform=test&chat_id=chat", nil)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "queued") {
		t.Fatalf("sessions: %s", w.Body.String())
	}
	w = call("POST", "/topics/stop", map[string]string{"project": "test", "platform": "test", "chat_id": "chat"})
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"cleared_messages":1`) {
		t.Fatalf("stop: %s", w.Body.String())
	}
	w = call("POST", "/topics/new", map[string]string{"project": "wrong"})
	if w.Code != http.StatusNotFound {
		t.Fatal("wrong project", w.Code)
	}
}
func TestTopicPartialFailureNotReplayed(t *testing.T) {
	e, p, _ := tfEnv(t, 1)
	p.fail = true
	key := "test:chat:commander"
	e.sessions.GetOrCreateActive(key)
	e.interactiveStates[key] = &interactiveState{platform: p, replyCtx: tfContext{chat: "chat"}}
	req := TopicRequest{SessionKey: key, RequestID: strings.Repeat("d", 32), To: "executor", Title: "title", Body: "body"}
	r, err := e.CreateTopic(req)
	if err == nil || r.RootID != "root" {
		t.Fatalf("%+v %v", r, err)
	}
	_, err = e.CreateTopic(req)
	if err == nil || p.creates.Load() != 1 {
		t.Fatal("ambiguous replay")
	}
	e.topicRecordsLoaded = false
	rlist, err := e.topicRecordsForSession(key)
	if err != nil || len(rlist) != 1 || rlist[0].RootID != "root" {
		t.Fatal("partial lost", err)
	}
}

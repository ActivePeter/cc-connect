package core

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestRotationAPIValidationLookupAndStatus(t *testing.T) {
	e, a, p, old := rotStart(t)
	_ = a
	_ = p
	api := &APIServer{engines: map[string]*Engine{"rotation": e}}
	get := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		api.handleSessionRotation(w, httptest.NewRequest("GET", path, nil))
		return w
	}
	if w := get("/session/current?project=rotation&session_key=missing"); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
	if e.sessions.LookupActive("missing") != nil {
		t.Fatal("lookup created row")
	}
	w := get("/session/current?project=rotation&session_key=test:chat:user")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	req := RotateRequest{Project: "rotation", SessionKey: "test:chat:user", ExpectedSessionID: old.ID, ExpectedScope: e.sessions.RotationScope(), RequestID: "r-api", Handoff: "SECRET"}
	b, _ := json.Marshal(req)
	w = httptest.NewRecorder()
	api.handleSessionRotation(w, httptest.NewRequest("POST", "/session/new", bytes.NewReader(b)))
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = get("/session/rotation?project=rotation&session_key=test:chat:user&request_id=r-api"); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	req.RequestID = "stale"
	req.ExpectedSessionID = "old"
	b, _ = json.Marshal(req)
	w = httptest.NewRecorder()
	api.handleSessionRotation(w, httptest.NewRequest("POST", "/session/new", bytes.NewReader(b)))
	if w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, body := range []string{"{", "{}", string(b) + " {}"} {
		w = httptest.NewRecorder()
		api.handleSessionRotation(w, httptest.NewRequest("POST", "/session/new", bytes.NewBufferString(body)))
		if w.Code != 400 {
			t.Fatal(w.Code, body)
		}
	}
}

func TestRotationIdleSwitchAndPendingHandoffAfterRestart(t *testing.T) {
	e, a, _, old := rotStart(t)
	a.at(0).events <- Event{Type: EventResult, Done: true, Content: "A_DONE"}
	rotWait(t, func() bool { return !old.Busy() })
	op, err := e.ScheduleRotation(RotateRequest{SessionKey: "test:chat:user", ExpectedSessionID: old.ID, RequestID: "idle", Handoff: "IDLE_HANDOFF"})
	if err != nil || op.Status != "SWITCHED" {
		t.Fatal(op, err)
	}
	rotWait(t, func() bool { return a.at(1) != nil && len(a.at(1).sent()) == 1 })
	if a.at(1).id == a.at(0).id {
		t.Fatal("backend reused")
	}
}

func TestRotationAmbiguousHandoffDoesNotReplay(t *testing.T) {
	a := &rotAgent{}
	p := &rotPlatform{stubPlatformEngine: stubPlatformEngine{n: "test"}}
	e := NewEngine("rotation", a, []Platform{p}, "", LangEnglish)
	defer e.cancel()
	old := e.sessions.GetOrCreateActive("test:chat:user")
	op, err := e.sessions.PrepareRotation("test:chat:user", old.ID, "r", "", "HANDOFF")
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := e.sessions.CommitRotation(op)
	if err != nil {
		t.Fatal(err)
	}
	fresh.Unlock()
	if err = e.sessions.SetFirstInputState(fresh, "in_flight"); err != nil {
		t.Fatal(err)
	}
	e.ReceiveMessage(p, &Message{Platform: "test", SessionKey: "test:chat:user", Content: "REAL_INPUT", ReplyCtx: "chat"})
	rotWait(t, func() bool { return len(p.getSent()) > 0 })
	if a.count() != 0 {
		t.Fatal("ambiguous outbox silently replayed")
	}
}

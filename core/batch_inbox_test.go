package core

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func batchTestEngine(t *testing.T) (*Engine, *cujAgent, *cujReplyCtxPlatform) {
	t.Helper()
	p := &cujReplyCtxPlatform{&stubPlatformEngine{n: "test"}}
	a := &cujAgent{}
	e := NewEngine("batch-test", a, []Platform{p}, filepath.Join(t.TempDir(), "sessions.json"), LangEnglish)
	e.dataDir = t.TempDir()
	if err := e.SetBatchInbox(BatchInboxConfig{ChannelPrefix: "test:group:", SessionKey: "test:group:owner", Debounce: time.Nanosecond, MaxWait: time.Nanosecond, Prompt: "Read batch"}); err != nil {
		t.Fatal(err)
	}
	e.markPlatformReady(p)
	t.Cleanup(func() { e.Stop() })
	return e, a, p
}

func batchSend(e *Engine, p Platform, id, content string) {
	e.ReceiveMessage(p, &Message{SessionKey: "test:group:" + id, Platform: "test", MessageID: id, UserID: id, UserName: id, Content: content, ReplyCtx: "ctx"})
}

func waitBatch(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for batch")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestCUJ_BatchInbox_DrainsAllAndDefersArrivalsDuringTurn(t *testing.T) {
	e, a, p := batchTestEngine(t)
	batchSend(e, p, "first", "begin task A")
	batchSend(e, p, "second", "end task A")
	batchSend(e, p, "third", "begin task B")
	a.nextSessionEvents = []Event{{Type: EventResult, Content: "batch one done", Done: true}}
	a.nextSessionDelayMs = 150
	done := make(chan struct{})
	go func() { e.runBatchInbox(); close(done) }()
	waitBatch(t, func() bool { return e.batchInbox.status()["running"].(bool) })
	batchSend(e, p, "fourth", "end task B")
	batchSend(e, p, "fifth", "begin task C")
	<-done
	if got := e.batchInbox.status()["pending"]; got != 2 {
		t.Fatalf("next batch pending=%v", got)
	}
	prompt := a.sessions[0].sentPrompts[0]
	if !strings.Contains(prompt, "3 messages") {
		t.Fatalf("first batch did not drain all: %s", prompt)
	}
	data, err := os.ReadFile(strings.TrimSuffix(e.batchInbox.path, ".json") + "-current.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []BatchMessage
	if json.Unmarshal(data, &rows) != nil || len(rows) != 3 {
		t.Fatalf("batch data=%s", data)
	}
	e.runBatchInbox()
	if got := len(a.sessions[0].sentPrompts); got != 2 {
		t.Fatalf("agent received %d prompts for 5 arrivals, want 2 batches", got)
	}
	if !strings.Contains(a.sessions[0].sentPrompts[1], "2 messages") {
		t.Fatal("second batch not merged")
	}
	if got := len(p.getSent()); got != 2 {
		t.Fatalf("user saw %d responses, want one per batch", got)
	}
	if e.batchInbox.status()["pending"] != 0 {
		t.Fatal("queue not drained")
	}
}

func TestBatchInbox_RetryDedupAndInterruptedRecovery(t *testing.T) {
	e, _, p := batchTestEngine(t)
	content := "notice\n[CC_SESSION_EVENT]\n{\"event_id\":\"same-event\"}"
	batchSend(e, p, "delivery-1", content)
	batchSend(e, p, "delivery-2", content)
	if e.batchInbox.status()["pending"] != 1 {
		t.Fatal("transport retry duplicated")
	}
	b := e.batchInbox
	b.mu.Lock()
	b.disk.InFlight = b.disk.Pending
	b.disk.Pending = nil
	err := b.saveLocked()
	b.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SetBatchInbox(b.cfg); err != nil {
		t.Fatal(err)
	}
	if e.batchInbox.status()["pending"] != 1 || e.batchInbox.status()["paused"] != true {
		t.Fatal("interrupted batch was lost or auto-resumed")
	}
	batchSend(e, p, "event-3", "[CC_SESSION_EVENT]\n{\"event_id\":\"new-event\"}")
	if e.batchInbox.status()["paused"] != true {
		t.Fatal("automated events resumed a failed batch")
	}
	batchSend(e, p, "human", "retry with latest data")
	if e.batchInbox.status()["paused"] != false {
		t.Fatal("human retry did not resume")
	}
	e.runBatchInbox()
	if e.batchInbox.status()["pending"] != 0 {
		t.Fatal("resumed batch did not complete")
	}
}

func TestBatchInbox_FailureRetainsAllWithoutSpin(t *testing.T) {
	e, a, p := batchTestEngine(t)
	a.failStartCount = 1
	batchSend(e, p, "one", "one")
	batchSend(e, p, "two", "two")
	e.runBatchInbox()
	if e.batchInbox.status()["pending"] != 2 || e.batchInbox.status()["paused"] != true {
		t.Fatal("failed batch was lost")
	}
	e.runBatchInbox()
	if len(a.sessions) != 0 {
		t.Fatal("failed batch retried without instruction")
	}
}

func TestBatchInbox_OnlyConfiguredChannelAndSlashCommandsBypass(t *testing.T) {
	e, _, p := batchTestEngine(t)
	if e.interceptBatchMessage(p, &Message{SessionKey: "test:other:user", Content: "ordinary"}) {
		t.Fatal("captured unrelated group")
	}
	m := &Message{SessionKey: "test:group:owner", Content: "/stop"}
	if e.interceptBatchMessage(p, m) {
		t.Fatal("stop command was queued")
	}
	if e.interceptBatchMessage(p, &Message{SessionKey: "test:group:x", Content: "photo", Images: []ImageAttachment{{MimeType: "image/png"}}}) {
		t.Fatal("attachment silently discarded")
	}
}

func TestTurnHooks_CompactionDoesNotEndAndQueuedTurnsEachReport(t *testing.T) {
	env := newCUJEnv(t)
	defer env.engine.Stop()
	var mu sync.Mutex
	var hooks []HookEvent
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var ev HookEvent
		json.NewDecoder(r.Body).Decode(&ev)
		mu.Lock()
		hooks = append(hooks, ev)
		mu.Unlock()
		w.WriteHeader(204)
	}))
	defer srv.Close()
	async := false
	env.engine.SetHooks(NewHookManager("test", []HookConfig{{Event: "*", Type: "http", URL: srv.URL, Async: &async}}, "sh", "-c", ""))
	env.agent.nextSessionEvents = []Event{{Type: EventResult, Done: false}, {Type: EventResult, Content: "one", Done: true}}
	env.agent.nextSessionDelayMs = 100
	env.userSends("owner", "first turn")
	waitBatch(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, ev := range hooks {
			if ev.Event == "turn.started" {
				return true
			}
		}
		return false
	})
	env.userSends("owner", "second turn")
	waitBatch(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, ev := range hooks {
			if ev.Event == "turn.ended" {
				n++
			}
		}
		return n == 2
	})
	mu.Lock()
	defer mu.Unlock()
	var life []string
	ids := map[string]int{}
	for _, ev := range hooks {
		if ev.Event == "turn.started" || ev.Event == "turn.ended" {
			life = append(life, string(ev.Event))
			ids[fmt.Sprint(ev.Extra["turn_id"])]++
		}
	}
	if strings.Join(life, ",") != "turn.started,turn.ended,turn.started,turn.ended" {
		t.Fatalf("lifecycle=%v", life)
	}
	if len(ids) != 2 {
		t.Fatalf("turn IDs=%v", ids)
	}
	for _, n := range ids {
		if n != 2 {
			t.Fatal("unpaired lifecycle")
		}
	}
}

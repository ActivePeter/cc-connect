package claudecode

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/chenhg5/cc-connect/core"
)

type permissionInitBuffer struct{ bytes.Buffer }

func (b *permissionInitBuffer) Close() error { return nil }

// Regression: cmd includes --dangerously-skip-permissions while the configured
// mode is auto (the root-compatible launch path). Claude reports bypass in its
// init event, but the adapter used to keep auto and forward background requests
// to core, which denies them when there is no foreground user turn.
func TestBackgroundPermission_InitBypassAfterForegroundResult(t *testing.T) {
	input := &permissionInitBuffer{}
	hooks := &ccPermissionHookRunner{}
	hooks.once.Do(func() {}) // isolate this test from user hook configuration
	cs := &claudeSession{stdin: input, events: make(chan core.Event, 8),
		ctx: context.Background(), ccHooks: hooks}
	cs.alive.Store(true)
	cs.setPermissionMode("auto")
	cs.handleReadLoopLine(`{"type":"system","subtype":"init","permissionMode":"bypassPermissions"}`)
	cs.handleReadLoopLine(`{"type":"result","subtype":"success","result":"foreground done"}`)
	cs.handleReadLoopLine(`{"type":"control_request","request_id":"background-bash","request":{"subtype":"can_use_tool","tool_name":"Bash","input":{"command":"printf test"}}}`)
	for len(cs.events) > 0 {
		if event := <-cs.events; event.Type == core.EventPermissionRequest {
			t.Fatal("background tool request escaped to the no-active-turn denial path")
		}
	}
	var response struct {
		Type     string `json:"type"`
		Response struct {
			RequestID string `json:"request_id"`
			Response  struct {
				Behavior     string         `json:"behavior"`
				UpdatedInput map[string]any `json:"updatedInput"`
			} `json:"response"`
		} `json:"response"`
	}
	if err := json.Unmarshal(input.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Type != "control_response" || response.Response.RequestID != "background-bash" ||
		response.Response.Response.Behavior != "allow" ||
		response.Response.Response.UpdatedInput["command"] != "printf test" {
		t.Fatalf("unexpected permission response: %s", input.Bytes())
	}
}

func TestSystemInitPermissionModePreservesRestrictions(t *testing.T) {
	for _, mode := range []string{"default", "auto", "plan", "acceptEdits", "dontAsk", "bypassPermissions"} {
		t.Run(mode, func(t *testing.T) {
			cs := &claudeSession{}
			cs.setPermissionMode("bypassPermissions")
			cs.handleSystem(map[string]any{"subtype": "init", "permissionMode": mode})
			if got := cs.permissionModeValue(); got != mode {
				t.Fatalf("mode = %q, want %q", got, mode)
			}
			if cs.autoApprove.Load() != (mode == "bypassPermissions") {
				t.Fatal("incorrect automatic approval")
			}
		})
	}
	for _, event := range []map[string]any{
		{"subtype": "init"},
		{"subtype": "init", "permissionMode": "unknown-future-mode"},
		{"subtype": "status", "permissionMode": "bypassPermissions"},
	} {
		cs := &claudeSession{}
		cs.setPermissionMode("default")
		cs.handleSystem(event)
		if cs.permissionModeValue() != "default" || cs.autoApprove.Load() {
			t.Fatal("unexpected mode escalation")
		}
	}
}

package codex

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/chenhg5/cc-connect/core"
)

type steerRPCWriter struct{ write func([]byte) }

func (w *steerRPCWriter) Write(b []byte) (int, error) { w.write(b); return len(b), nil }
func (w *steerRPCWriter) Close() error                { return nil }

func TestSendMidTurn_UsesSteerAndPreservesGoal(t *testing.T) {
	s := &appServerSession{currentTurn: "turn-1", goalObjective: "original", goalStatus: "active"}
	s.alive.Store(true)
	s.threadID.Store("thread-1")
	var request struct {
		ID     int64                      `json:"id"`
		Method string                     `json:"method"`
		Params map[string]json.RawMessage `json:"params"`
	}
	s.stdin = &steerRPCWriter{write: func(b []byte) {
		if err := json.Unmarshal(b, &request); err != nil {
			panic(err)
		}
		// Notifications can advance the turn while the RPC is in flight. The
		// request must not hold stateMu or overwrite this newer turn on return.
		s.handleNotification("turn/started", json.RawMessage(`{"threadId":"thread-1","turn":{"id":"turn-2"}}`))
		s.handleResponse(rpcResponseEnvelope{ID: request.ID, Result: json.RawMessage(`{"turnId":"turn-1"}`)})
	}}
	if err := s.SendMidTurn("change the plan", "msg-1", nil, nil); err != nil {
		t.Fatal(err)
	}
	if request.Method != "turn/steer" {
		t.Fatal(request.Method)
	}
	if string(request.Params["threadId"]) != `"thread-1"` || string(request.Params["expectedTurnId"]) != `"turn-1"` {
		t.Fatalf("params: %s", request.Params)
	}
	var input []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(request.Params["input"], &input); err != nil {
		t.Fatal(err)
	}
	if len(input) != 1 || input[0].Type != "text" || input[0].Text != "change the plan" {
		t.Fatalf("input: %+v", input)
	}
	if len(request.Params) != 3 || s.currentTurn != "turn-2" || s.GetGoal().Objective != "original" || s.GetGoal().Status != "active" {
		t.Fatal("steer changed turn/goal state")
	}
}

func TestSendMidTurn_RejectsUnavailableWithoutWriting(t *testing.T) {
	for _, scenario := range []string{"closed", "no_thread", "no_turn", "image", "file"} {
		t.Run(scenario, func(t *testing.T) {
			s := &appServerSession{currentTurn: "turn-1"}
			s.alive.Store(true)
			s.threadID.Store("thread-1")
			s.stdin = &steerRPCWriter{write: func([]byte) { panic("unexpected RPC") }}
			var images []core.ImageAttachment
			var files []core.FileAttachment
			switch scenario {
			case "closed":
				s.alive.Store(false)
			case "no_thread":
				s.threadID.Store("")
			case "no_turn":
				s.currentTurn = ""
			case "image":
				images = []core.ImageAttachment{{}}
			case "file":
				files = []core.FileAttachment{{}}
			}
			if err := s.SendMidTurn("hello", "", images, files); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestSendMidTurn_SurfacesServerRejectionsWithoutRetry(t *testing.T) {
	for _, reason := range []string{"no active turn to steer", "expected active turn id `old` but found `new`", "cannot steer a review turn", "cannot steer a compact turn"} {
		t.Run(reason, func(t *testing.T) {
			s := &appServerSession{currentTurn: "turn-1"}
			s.alive.Store(true)
			s.threadID.Store("thread-1")
			calls := 0
			s.stdin = &steerRPCWriter{write: func(b []byte) {
				calls++
				var req struct {
					ID int64 `json:"id"`
				}
				_ = json.Unmarshal(b, &req)
				s.handleResponse(rpcResponseEnvelope{ID: req.ID, Error: &rpcError{Code: -32600, Message: reason}})
			}}
			err := s.SendMidTurn("hello", "", nil, nil)
			if err == nil || !strings.Contains(err.Error(), reason) || !strings.Contains(err.Error(), "turn/steer") || errors.Is(err, core.ErrInjectUnsupported) || calls != 1 {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestSendMidTurn_OrdinaryTurnNeedsNoGoal(t *testing.T) {
	s := &appServerSession{currentTurn: "ordinary-turn"}
	s.alive.Store(true)
	s.threadID.Store("thread-1")
	calls := 0
	s.stdin = &steerRPCWriter{write: func(b []byte) {
		calls++
		var req struct {
			ID     int64  `json:"id"`
			Method string `json:"method"`
		}
		_ = json.Unmarshal(b, &req)
		if req.Method != "turn/steer" {
			panic(req.Method)
		}
		s.handleResponse(rpcResponseEnvelope{ID: req.ID, Result: json.RawMessage(`{"turnId":"ordinary-turn"}`)})
	}}
	if err := s.SendMidTurn("ordinary correction", "", nil, nil); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || s.GetGoal().Status != "" {
		t.Fatal("ordinary steering required or created a goal")
	}
}

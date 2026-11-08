package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/chenhg5/cc-connect/core"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Real child processes and the real Codex exec adapter; only the CLI/model
// boundary is replaced. All children must be waiting concurrently before release.
func TestTopicSessionsConcurrentCodexProcesses(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX shell fixture")
	}
	dir := t.TempDir()
	script := filepath.Join(dir, "codex-fixture")
	body := "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"$TF_DIR/$TF_LABEL.pid\"\nread release < \"$TF_DIR/$TF_LABEL.gate\"\nprintf '{\"type\":\"thread.started\",\"thread_id\":\"%s\"}\\n' \"$TF_LABEL\"\nprintf '{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"DONE-%s\"}}\\n' \"$TF_LABEL\"\nprintf '{\"type\":\"turn.completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}\\n'\n"
	if err := os.WriteFile(script, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	a := &Agent{cmd: script, backend: "exec", workDir: dir, codexHome: dir, mode: "yolo", activeIdx: -1}
	var sessions []core.AgentSession
	for i := 0; i < 3; i++ {
		label := fmt.Sprint(i)
		// A FIFO provides a barrier without an unbounded shell sleep/poll loop.
		if out, err := runTopicMkfifo(filepath.Join(dir, label+".gate")); err != nil {
			t.Fatal(string(out), err)
		}
		a.SetSessionEnv([]string{"TF_DIR=" + dir, "TF_LABEL=" + label})
		s, err := a.StartSession(context.Background(), "")
		if err != nil {
			t.Fatal(err)
		}
		sessions = append(sessions, s)
		defer s.Close()
		if err = s.Send("isolated topic "+label, "", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	pids := map[string]bool{}
	deadline := time.Now().Add(3 * time.Second)
	for len(pids) < 3 && time.Now().Before(deadline) {
		for i := 0; i < 3; i++ {
			if b, err := os.ReadFile(filepath.Join(dir, fmt.Sprint(i)+".pid")); err == nil {
				pids[strings.TrimSpace(string(b))] = true
			}
		}
		if len(pids) < 3 {
			time.Sleep(time.Millisecond)
		}
	}
	if len(pids) != 3 {
		t.Fatalf("independent child pids=%v", pids)
	}
	t.Logf("three Codex child processes overlapped before gate release: %v at %s", pids, time.Now().Format(time.RFC3339Nano))
	// Stop one subprocess; the other sessions must still deliver independently.
	if err := sessions[0].Close(); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 3; i++ {
		gate, err := os.OpenFile(filepath.Join(dir, fmt.Sprint(i)+".gate"), os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = gate.WriteString("go\n")
		gate.Close()
		got := false
		var output strings.Builder
		timer := time.NewTimer(3 * time.Second)
	loop:
		for {
			select {
			case ev, ok := <-sessions[i].Events():
				if !ok {
					break loop
				}
				output.WriteString(ev.Content)
				if ev.Type == core.EventResult {
					got = strings.Contains(output.String(), "DONE-"+fmt.Sprint(i))
					break loop
				}
			case <-timer.C:
				break loop
			}
		}
		timer.Stop()
		if !got {
			t.Fatalf("topic %d did not finish independently", i)
		}
	}
}

func runTopicMkfifo(path string) ([]byte, error) {
	return exec.Command("mkfifo", path).CombinedOutput()
}

func TestTopicAppServerHelper(t *testing.T) {
	if os.Getenv("CC_TF_HELPER") != "1" {
		return
	}
	label := os.Getenv("TF_LABEL")
	write := func(v any) { _ = json.NewEncoder(os.Stdout).Encode(v) }
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		var r struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if json.Unmarshal(scanner.Bytes(), &r) != nil || len(r.ID) == 0 {
			continue
		}
		result := map[string]any{}
		switch r.Method {
		case "thread/start":
			result = map[string]any{"thread": map[string]any{"id": "thread-" + label}}
		case "turn/start":
			result = map[string]any{"turn": map[string]any{"id": "turn-" + label, "status": "inProgress"}}
		case "test/release":
			result = map[string]any{"turnId": "turn-" + label}
		}
		write(map[string]any{"id": r.ID, "result": result})
		if r.Method == "test/release" {
			write(map[string]any{"method": "item/completed", "params": map[string]any{"threadId": "thread-" + label, "turnId": "turn-" + label, "item": map[string]any{"type": "agentMessage", "id": "item-" + label, "text": "DONE-" + label}}})
			write(map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "thread-" + label, "turn": map[string]any{"id": "turn-" + label, "status": "completed"}}})
		}
	}
	os.Exit(0)
}
func TestTopicSessionsConcurrentAppServerProcesses(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX launcher")
	}
	dir := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	launcher := filepath.Join(dir, "codex")
	if err = os.WriteFile(launcher, []byte("#!/bin/sh\nexec \"$TF_TEST_BINARY\" -test.run=TestTopicAppServerHelper\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	a := &Agent{cmd: launcher, backend: "app_server", appServerURL: "stdio", workDir: dir, codexHome: dir, mode: "yolo", activeIdx: -1}
	var sessions []*appServerSession
	pids := map[int]bool{}
	for i := 0; i < 3; i++ {
		a.SetSessionEnv([]string{"CC_TF_HELPER=1", "TF_LABEL=" + fmt.Sprint(i), "TF_TEST_BINARY=" + binary})
		s, err := a.StartSession(context.Background(), "")
		if err != nil {
			t.Fatal(err)
		}
		as := s.(*appServerSession)
		defer as.Close()
		sessions = append(sessions, as)
		pids[as.cmd.Process.Pid] = true
		if err = as.Send("TASK-"+fmt.Sprint(i), "", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if len(pids) != 3 {
		t.Fatalf("shared process: %v", pids)
	}
	t.Logf("three app-server stdio processes active together: %v", pids)
	if err = sessions[0].Close(); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < 3; i++ {
		if !sessions[i].Alive() {
			t.Fatal("sibling process closed")
		}
		var released any
		if err = sessions[i].request("test/release", map[string]any{}, &released); err != nil {
			t.Fatal(err)
		}
		var output strings.Builder
		deadline := time.NewTimer(3 * time.Second)
		done := false
	loop:
		for {
			select {
			case ev, ok := <-sessions[i].Events():
				if !ok {
					break loop
				}
				output.WriteString(ev.Content)
				if ev.Type == core.EventResult {
					done = true
					break loop
				}
			case <-deadline.C:
				break loop
			}
		}
		deadline.Stop()
		if !done || !strings.Contains(output.String(), "DONE-"+fmt.Sprint(i)) {
			t.Fatalf("sibling %d: done=%v output=%q", i, done, output.String())
		}
	}
}

package claudecode

import (
	"bytes"
	"os/exec"
	"strings"
	"sync"
	"runtime"
	"testing"
	"time"

	"github.com/creack/pty"
)

// Grandchildren inherit the pty slave, so closing the master does not unblock
// a pending Read; the cleanup must kill the group and wait bounded.
func TestFinishUsageProbeDoesNotBlockOnGrandchild(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("pty semantics differ on windows")
	}
	cmd := exec.Command("sh", "-c", "sleep 30 & wait")
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: 40, Cols: 120})
	if err != nil {
		t.Fatalf("pty.StartWithSize: %v", err)
	}

	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := ptmx.Read(buf); err != nil {
				readDone <- err
				return
			}
		}
	}()
	processDone := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(processDone)
	}()

	start := time.Now()
	finishClaudeUsageProbe(ptmx, cmd, readDone, processDone)
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("probe cleanup took %v; it must be bounded", elapsed)
	}
}

// A grandchild that escapes the process group (setsid) still holds the stderr
// pipe, so cmd.Wait would block forever without WaitDelay.
func TestFinishUsageProbeReturnsWhenEscapedGrandchildHoldsStderr(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("pty semantics differ on windows")
	}
	if _, err := exec.LookPath("setsid"); err != nil {
		t.Skip("setsid not available")
	}
	var stderr bytes.Buffer
	cmd := exec.Command("sh", "-c", "setsid sleep 30 & echo SPAWNED; wait")
	cmd.Stderr = &stderr
	cmd.WaitDelay = claudeUsageWaitDelay
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: 40, Cols: 120})
	if err != nil {
		t.Fatalf("pty.StartWithSize: %v", err)
	}
	readDone := make(chan error, 1)
	output := &syncBuffer{}
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := ptmx.Read(buf)
			if n > 0 {
				output.Write(buf[:n])
			}
			if err != nil {
				readDone <- err
				return
			}
		}
	}()
	processDone := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(processDone)
	}()
	// Let the shell spawn the setsid'd child before we tear the group down.
	for i := 0; i < 300 && !output.Contains("SPAWNED"); i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if !output.Contains("SPAWNED") {
		t.Fatal("grandchild was never spawned")
	}

	start := time.Now()
	finishClaudeUsageProbe(ptmx, cmd, readDone, processDone)
	if elapsed := time.Since(start); elapsed > 6*time.Second {
		t.Fatalf("probe cleanup took %v; WaitDelay must bound it", elapsed)
	}
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Contains(s string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.Contains(b.buf.String(), s)
}

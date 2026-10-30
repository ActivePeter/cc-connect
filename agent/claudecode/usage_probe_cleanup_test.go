package claudecode

import (
	"os/exec"
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

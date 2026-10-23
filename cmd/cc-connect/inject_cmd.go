package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/chenhg5/cc-connect/core"
)

// runInject writes a message into a session's running turn (POST /inject).
// Exit codes: 0 injected, 3 the session has no running turn (nothing written;
// deliver it with a timer instead), 1 error.
func runInject(args []string) {
	var req core.InjectRequest
	var dataDir string
	useStdin := false
	for i := 0; i < len(args); i++ {
		next := func() string {
			if i+1 >= len(args) {
				fmt.Fprintf(os.Stderr, "Error: %s requires a value\n", args[i])
				os.Exit(1)
			}
			i++
			return args[i]
		}
		switch args[i] {
		case "--project", "-p":
			req.Project = next()
		case "--session", "-s":
			req.SessionKey = next()
		case "--message", "-m":
			req.Message = next()
		case "--data-dir":
			dataDir = next()
		case "--stdin":
			useStdin = true
		case "-h", "--help":
			printInjectUsage()
			return
		default:
			fmt.Fprintf(os.Stderr, "Error: unknown argument %q\n", args[i])
			printInjectUsage()
			os.Exit(1)
		}
	}
	if useStdin {
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: read stdin: %v\n", err)
			os.Exit(1)
		}
		req.Message = string(b)
	}
	if req.Project == "" {
		req.Project = os.Getenv("CC_PROJECT")
	}
	if strings.TrimSpace(req.Message) == "" || req.SessionKey == "" {
		printInjectUsage()
		os.Exit(1)
	}
	sockPath := resolveSocketPath(dataDir)
	client := &http.Client{Transport: &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) { return net.Dial("unix", sockPath) },
	}}
	payload, _ := json.Marshal(req)
	resp, err := client.Post("http://unix/inject", "application/json", bytes.NewReader(payload))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "Error: %s\n", strings.TrimSpace(string(body)))
		os.Exit(1)
	}
	var out struct {
		Status string `json:"status"`
	}
	_ = json.Unmarshal(body, &out)
	fmt.Println(out.Status)
	if out.Status != core.InjectInjected {
		os.Exit(3)
	}
}

func printInjectUsage() {
	fmt.Println(`Usage: cc-connect inject -s <session_key> [-p <project>] (-m <text> | --stdin)

Write a message into the session's running turn. Claude Code reads it at its
next tool boundary, so a busy session sees it within one tool call instead of
after the whole turn. Prints "injected" (exit 0), or "idle" (exit 3) when the
session has no running turn and nothing was written: deliver it with
"cc-connect timer add" instead.`)
}

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/chenhg5/cc-connect/core"
)

type sessionNewOptions struct {
	Request core.RotateRequest
	DataDir string
	JSON    bool
}

func parseSessionNew(args []string) (sessionNewOptions, error) {
	var opts sessionNewOptions
	fs := flag.NewFlagSet("session new", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var file, prompt string
	fs.StringVar(&opts.Request.Project, "p", os.Getenv("CC_PROJECT"), "project")
	fs.StringVar(&opts.Request.Project, "project", os.Getenv("CC_PROJECT"), "project")
	fs.StringVar(&opts.Request.SessionKey, "s", os.Getenv("CC_SESSION_KEY"), "session key")
	fs.StringVar(&opts.Request.SessionKey, "session", os.Getenv("CC_SESSION_KEY"), "session key")
	fs.StringVar(&opts.DataDir, "data-dir", "", "data directory")
	fs.StringVar(&opts.Request.Name, "name", "", "new session name")
	fs.StringVar(&file, "handoff", "", "UTF-8 handoff file")
	fs.StringVar(&prompt, "prompt", "", "first input")
	fs.BoolVar(&opts.JSON, "json", false, "JSON response")
	if err := fs.Parse(args); err != nil {
		return opts, err
	}
	if fs.NArg() != 0 {
		return opts, fmt.Errorf("unexpected positional arguments")
	}
	provided := 0
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "handoff" || f.Name == "prompt" {
			provided++
		}
	})
	if provided != 1 {
		return opts, fmt.Errorf("exactly one of --handoff or --prompt is required")
	}
	if file != "" {
		f, err := os.Open(file)
		if err != nil {
			return opts, err
		}
		defer f.Close()
		st, err := f.Stat()
		if err != nil {
			return opts, err
		}
		if !st.Mode().IsRegular() {
			return opts, fmt.Errorf("handoff must be a regular file")
		}
		data, err := io.ReadAll(io.LimitReader(f, 65537))
		if err != nil {
			return opts, err
		}
		prompt = string(data)
	}
	if strings.TrimSpace(prompt) == "" || len(prompt) > 65536 || !utf8.ValidString(prompt) {
		return opts, fmt.Errorf("handoff must be nonempty UTF-8, at most 64 KiB")
	}
	if opts.Request.SessionKey == "" {
		return opts, fmt.Errorf("session key required (-s or CC_SESSION_KEY)")
	}
	opts.Request.Handoff = prompt
	if opts.Request.Project == os.Getenv("CC_PROJECT") && opts.Request.SessionKey == os.Getenv("CC_SESSION_KEY") && resolveSocketPath(opts.DataDir) == resolveSocketPath("") {
		opts.Request.ExpectedSessionID = os.Getenv("CC_SESSION_ID")
		opts.Request.ExpectedScope = os.Getenv("CC_SESSION_SCOPE")
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return opts, err
	}
	opts.Request.RequestID = hex.EncodeToString(id)
	return opts, nil
}

func sessionRotationClient(socket string) *http.Client {
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", socket)
	}}
	return &http.Client{Transport: transport, Timeout: 45 * time.Second}
}

func requestSessionNew(opts sessionNewOptions) (core.RotationOperation, error) {
	c := sessionRotationClient(resolveSocketPath(opts.DataDir))
	defer c.CloseIdleConnections()
	if opts.Request.ExpectedSessionID == "" || opts.Request.ExpectedScope == "" {
		query := url.Values{"project": {opts.Request.Project}, "session_key": {opts.Request.SessionKey}}
		resp, err := c.Get("http://localhost/session/current?" + query.Encode())
		if err != nil {
			return core.RotationOperation{}, err
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if err != nil {
			return core.RotationOperation{}, err
		}
		if resp.StatusCode != 200 {
			return core.RotationOperation{}, fmt.Errorf("lookup session: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
		}
		var current struct {
			ID    string `json:"session_id"`
			Scope string `json:"session_scope"`
		}
		if err = json.Unmarshal(data, &current); err != nil {
			return core.RotationOperation{}, err
		}
		if opts.Request.ExpectedSessionID != "" && opts.Request.ExpectedSessionID != current.ID {
			return core.RotationOperation{}, core.ErrRotationStale
		}
		opts.Request.ExpectedSessionID = current.ID
		opts.Request.ExpectedScope = current.Scope
	}
	body, err := json.Marshal(opts.Request)
	if err != nil {
		return core.RotationOperation{}, err
	}
	resp, err := c.Post("http://localhost/session/new", "application/json", bytes.NewReader(body))
	if err != nil {
		return core.RotationOperation{}, fmt.Errorf("request %s: %w (query /session/rotation before retrying)", opts.Request.RequestID, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return core.RotationOperation{}, err
	}
	if resp.StatusCode != 200 && resp.StatusCode != 202 {
		return core.RotationOperation{}, fmt.Errorf("rotation: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var op core.RotationOperation
	err = json.Unmarshal(data, &op)
	return op, err
}

func runSession(args []string) {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
		fmt.Println("Usage: cc-connect session new (--handoff FILE | --prompt TEXT) [-p PROJECT] [-s KEY] [--data-dir DIR] [--name NAME] [--json]")
		return
	}
	if args[0] != "new" {
		fmt.Fprintln(os.Stderr, "unknown session operation")
		os.Exit(1)
	}
	opts, err := parseSessionNew(args[1:])
	if err != nil {
		if err == flag.ErrHelp {
			runSession(nil)
			return
		}
		fmt.Fprintln(os.Stderr, "session new:", err)
		os.Exit(1)
	}
	op, err := requestSessionNew(opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "session new:", err)
		os.Exit(1)
	}
	if opts.JSON {
		_ = json.NewEncoder(os.Stdout).Encode(op)
	} else {
		fmt.Printf("%s request_id=%s session_id=%s revision=%d\n", op.Status, op.RequestID, op.TargetID, op.Revision)
	}
}

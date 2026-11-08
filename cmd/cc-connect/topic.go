package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/chenhg5/cc-connect/core"
)

type topicOptions struct {
	Request core.TopicRequest
	DataDir string
}

func parseTopicOptions(operation string, args []string, stdin io.Reader) (topicOptions, error) {
	var o topicOptions
	fs := flag.NewFlagSet("topic "+operation, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&o.Request.Project, "p", os.Getenv("CC_PROJECT"), "project")
	fs.StringVar(&o.Request.SessionKey, "s", os.Getenv("CC_SESSION_KEY"), "session key")
	fs.StringVar(&o.DataDir, "data-dir", "", "data directory")
	var file string
	var fromStdin bool
	if operation == "new" {
		fs.StringVar(&o.Request.To, "to", "", "executor open_id or mention_map name")
		fs.StringVar(&o.Request.Title, "title", "", "title")
		fs.StringVar(&o.Request.RequestID, "request-id", "", "idempotency key (32 hex digits)")
		fs.StringVar(&file, "file", "", "UTF-8 specification file")
		fs.BoolVar(&fromStdin, "stdin", false, "read specification from stdin")
	}
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	if fs.NArg() != 0 {
		return o, fmt.Errorf("unexpected positional arguments")
	}
	if o.Request.Project == "" || o.Request.SessionKey == "" {
		return o, fmt.Errorf("CC_PROJECT/CC_SESSION_KEY or -p/-s required")
	}
	if operation == "list" {
		return o, nil
	}
	if (file != "") == fromStdin {
		return o, fmt.Errorf("exactly one of --file or --stdin required")
	}
	input := stdin
	if file != "" {
		f, err := os.Open(file)
		if err != nil {
			return o, err
		}
		defer f.Close()
		st, err := f.Stat()
		if err != nil {
			return o, err
		}
		if !st.Mode().IsRegular() {
			return o, fmt.Errorf("specification must be a regular file")
		}
		input = f
	}
	b, err := io.ReadAll(io.LimitReader(input, 65537))
	if err != nil {
		return o, err
	}
	o.Request.Body = string(b)
	if strings.TrimSpace(o.Request.Body) == "" || len(b) > 65536 || !utf8.Valid(b) {
		return o, fmt.Errorf("body must be nonempty UTF-8, at most 64 KiB")
	}
	if o.Request.To == "" || strings.TrimSpace(o.Request.Title) == "" || len([]rune(o.Request.Title)) > 100 || strings.ContainsAny(o.Request.Title, "\r\n") || !utf8.ValidString(o.Request.Title) {
		return o, fmt.Errorf("--to and one-line --title (max 100 chars) required")
	}
	if o.Request.RequestID == "" {
		id := make([]byte, 16)
		if _, err = rand.Read(id); err != nil {
			return o, err
		}
		o.Request.RequestID = hex.EncodeToString(id)
	}
	id, err := hex.DecodeString(o.Request.RequestID)
	if err != nil || len(id) != 16 {
		return o, fmt.Errorf("--request-id must be 32 hex digits")
	}
	return o, nil
}
func requestTopic(operation string, o topicOptions) (json.RawMessage, error) {
	client := sessionRotationClient(resolveSocketPath(o.DataDir))
	defer client.CloseIdleConnections()
	var resp *http.Response
	var err error
	if operation == "list" {
		query := url.Values{"project": {o.Request.Project}, "session_key": {o.Request.SessionKey}}
		resp, err = client.Get("http://localhost/topics?" + query.Encode())
	} else {
		b, _ := json.Marshal(o.Request)
		resp, err = client.Post("http://localhost/topics/new", "application/json", bytes.NewReader(b))
	}
	if err != nil {
		return nil, fmt.Errorf("request_id=%s: %w; use topic list to check before retrying", o.Request.RequestID, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		if json.Valid(b) {
			return b, fmt.Errorf("topic %s: HTTP %d", operation, resp.StatusCode)
		}
		return nil, fmt.Errorf("topic %s: HTTP %d: %s", operation, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if !json.Valid(b) {
		return nil, fmt.Errorf("invalid JSON response")
	}
	return b, nil
}
func runTopic(args []string) {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
		fmt.Println("Usage: cc-connect topic new --to EXECUTOR --title TITLE (--stdin | --file FILE) [-p PROJECT] [-s KEY] [--data-dir DIR] [--request-id HEX]\n       cc-connect topic list [-p PROJECT] [-s KEY] [--data-dir DIR]")
		return
	}
	op := args[0]
	if op != "new" && op != "list" {
		fmt.Fprintln(os.Stderr, "unknown topic operation")
		os.Exit(1)
	}
	opts, err := parseTopicOptions(op, args[1:], os.Stdin)
	if err == flag.ErrHelp {
		runTopic(nil)
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "topic:", err)
		os.Exit(1)
	}
	result, err := requestTopic(op, opts)
	if len(result) > 0 {
		fmt.Println(string(result))
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

package main

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTopicCLIParseAndValidation(t *testing.T) {
	t.Setenv("CC_PROJECT", "commander")
	t.Setenv("CC_SESSION_KEY", "test:chat:commander")
	o, err := parseTopicOptions("new", []string{"--to", "executor", "--title", "title", "--stdin"}, strings.NewReader("SPEC"))
	if err != nil || o.Request.Project != "commander" || o.Request.Body != "SPEC" || len(o.Request.RequestID) != 32 {
		t.Fatalf("%+v %v", o, err)
	}
	file := filepath.Join(t.TempDir(), "spec")
	if err = os.WriteFile(file, []byte("FILE"), 0600); err != nil {
		t.Fatal(err)
	}
	o, err = parseTopicOptions("new", []string{"-p", "override", "-s", "key", "--to", "ou_executor", "--title", "title", "--file", file}, nil)
	if err != nil || o.Request.Project != "override" || o.Request.Body != "FILE" {
		t.Fatal(o, err)
	}
	for _, args := range [][]string{
		{"--to", "executor", "--title", "title"},
		{"--to", "executor", "--title", "title", "--stdin", "--file", file},
		{"--to", "executor", "--title", "a\nb", "--stdin"},
		{"--to", "executor", "--title", "title", "--stdin", "--request-id", "bad"},
	} {
		if _, err = parseTopicOptions("new", args, strings.NewReader("SPEC")); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
	if _, err = parseTopicOptions("new", []string{"--to", "executor", "--title", "title", "--stdin"}, strings.NewReader(strings.Repeat("x", 65537))); err == nil {
		t.Fatal("oversize accepted")
	}
	if _, err = parseTopicOptions("list", nil, nil); err != nil {
		t.Fatal(err)
	}
}
func TestTopicCLIUnixSocketJSONAndPartialReceipt(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "tf-cli-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	if err := os.MkdirAll(filepath.Join(dir, "run"), 0700); err != nil {
		t.Fatal(err)
	}
	socket := resolveSocketPath(dir)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "GET" {
			if r.URL.Query().Get("session_key") != "KEY" {
				t.Error(r.URL.String())
			}
			io.WriteString(w, "[]")
			return
		}
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req["body"] != "SPEC" {
			t.Error(req)
		}
		w.WriteHeader(409)
		io.WriteString(w, "{\"topic\":{\"root_message_id\":\"om_partial\",\"status\":\"failed\"},\"error\":\"reply failed\"}")
	})}
	go server.Serve(listener)
	defer server.Close()
	o, err := parseTopicOptions("new", []string{"-p", "test", "-s", "KEY", "--data-dir", dir, "--to", "ou_executor", "--title", "title", "--stdin"}, strings.NewReader("SPEC"))
	if err != nil {
		t.Fatal(err)
	}
	data, err := requestTopic("new", o)
	if err == nil || !strings.Contains(string(data), "om_partial") {
		t.Fatalf("%s %v", data, err)
	}
	data, err = requestTopic("list", o)
	if err != nil || string(data) != "[]" {
		t.Fatalf("%s %v", data, err)
	}
}

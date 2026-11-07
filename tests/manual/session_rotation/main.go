// Command session-rotation-live exercises a real backend and real child CLI;
// only platform delivery is faked. Run against an explicitly isolated root.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	_ "github.com/chenhg5/cc-connect/agent/claudecode"
	_ "github.com/chenhg5/cc-connect/agent/codex"
	"github.com/chenhg5/cc-connect/config"
	"github.com/chenhg5/cc-connect/core"
)

type platform struct {
	mu            sync.Mutex
	sent          []string
	notices       chan string
	engine        *core.Engine
	sourceAgentID string
}

func (p *platform) Name() string                                   { return "rotation-test" }
func (p *platform) Start(core.MessageHandler) error                { return nil }
func (p *platform) Stop() error                                    { return nil }
func (p *platform) Reply(c context.Context, r any, s string) error { return p.Send(c, r, s) }
func (p *platform) Send(_ context.Context, _ any, s string) error {
	p.mu.Lock()
	p.sent = append(p.sent, s)
	if strings.Contains(s, "SOURCE_DONE") && p.sourceAgentID == "" {
		if active := p.engine.GetSessions().LookupActive("rotation-test:isolated:user"); active != nil {
			p.sourceAgentID = active.GetAgentSessionID()
		}
	}
	p.mu.Unlock()
	select {
	case p.notices <- s:
	default:
	}
	return nil
}
func (p *platform) wait(marker string) error {
	deadline := time.NewTimer(180 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case s := <-p.notices:
			if strings.Contains(s, "SOURCE_ROTATION_FAILED") {
				return fmt.Errorf("source command or environment guard failed")
			}
			if strings.Contains(s, marker) {
				return nil
			}
		case <-deadline.C:
			return fmt.Errorf("timeout waiting for %s", marker)
		}
	}
}
func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0600)
}
func run() error {
	root := flag.String("root", "", "isolated root, required")
	agentName := flag.String("agent", "claudecode", "claudecode or codex")
	backend := flag.String("backend", "exec", "exec or app_server")
	codexHome := flag.String("codex-home", "", "isolated native Codex home")
	sourceHome := flag.String("source-codex-home", "", "reuse existing local login; never sent over the network")
	effort := flag.String("effort", "", "reasoning effort override for this isolated test")
	phase := flag.Int("phase", 1, "1 creates and rotates; 2 restarts and verifies")
	credentialConfig := flag.String("credential-config", "", "read-only existing config for claudecode API env")
	flag.Parse()
	home, _ := os.UserHomeDir()
	localAllowed := filepath.Join(home, "cc-connect-workspace", "outputs", "ccrot") + string(filepath.Separator)
	if *root == "" || !filepath.IsAbs(*root) || (!strings.HasPrefix(*root, "/mnt/pool/ccrot/") && !strings.HasPrefix(*root, localAllowed) && !strings.HasPrefix(*root, filepath.Join(home, "ccrot")+string(filepath.Separator))) {
		return fmt.Errorf("explicit isolated ccrot root required")
	}
	for _, d := range []string{*root, filepath.Join(*root, "data"), filepath.Join(*root, "work")} {
		if err := os.MkdirAll(d, 0700); err != nil {
			return err
		}
	}
	opts := map[string]any{"work_dir": filepath.Join(*root, "work"), "backend": *backend, "mode": "yolo", "cc_data_dir": filepath.Join(*root, "data"), "app_server_url": "stdio"}
	if *effort != "" {
		opts["reasoning_effort"] = *effort
	}
	if *agentName == "codex" && *codexHome != "" {
		if !strings.HasPrefix(*codexHome, *root+string(filepath.Separator)) {
			return fmt.Errorf("native home must be inside isolation root")
		}
		if err := prepareCodexHome(*sourceHome, *codexHome); err != nil {
			return err
		}
		opts["codex_home"] = *codexHome
		opts["env"] = map[string]any{"CODEX_HOME": *codexHome}
	}
	model := ""
	if *agentName == "claudecode" {
		opts["mode"] = "bypassPermissions"
		if *credentialConfig != "" {
			cfg, err := config.Load(*credentialConfig)
			if err != nil {
				return err
			}
			found := false
			for _, project := range cfg.Projects {
				if project.Name == "ser7-claude" {
					opts["env"] = project.Agent.Options["env"]
					found = true
				}
			}
			if !found {
				return fmt.Errorf("credential project not found")
			}
		}
		// The existing provider selects the actual model; report it in the logs/fixture.
		model = "configured claudecode provider (see init event)"
	}
	if err := writeJSON(filepath.Join(*root, "isolation-config.json"), map[string]any{"agent": *agentName, "backend": *backend, "phase": *phase, "data_dir": filepath.Join(*root, "data"), "work_dir": opts["work_dir"], "platform": "fake only", "credential_config": *credentialConfig, "model": model, "codex_home": *codexHome, "reasoning_effort": *effort, "app_server_transport": "stdio (no TCP listener)"}); err != nil {
		return err
	}
	ag, err := core.CreateAgent(*agentName, opts)
	if err != nil {
		return err
	}
	p := &platform{notices: make(chan string, 100)}
	e := core.NewEngine("rotation-live", ag, []core.Platform{p}, filepath.Join(*root, "data/sessions.json"), core.LangEnglish)
	p.engine = e
	e.SetDataDir(filepath.Join(*root, "data"))
	e.SetStreamPreviewCfg(core.StreamPreviewCfg{Enabled: false})
	e.SetDisplayConfig(core.DisplayCfg{Mode: "quiet", CardMode: "legacy", ThinkingMessages: false, ToolMessages: false})
	if err = e.Start(); err != nil {
		return err
	}
	defer e.Stop()
	api, err := core.NewAPIServer(filepath.Join(*root, "data"))
	if err != nil {
		return err
	}
	api.RegisterEngine("rotation-live", e)
	api.Start()
	defer api.Stop()
	key := "rotation-test:isolated:user"
	send := func(text string) {
		e.ReceiveMessage(p, &core.Message{Platform: p.Name(), SessionKey: key, Content: text, ReplyCtx: "isolated", UserID: "test-user"})
	}
	if *phase == 1 {
		quote := func(text string) string { return "'" + strings.ReplaceAll(text, "'", "'\"'\"'") + "'" }
		dataDir := quote(filepath.Join(*root, "data"))
		executable, err := os.Executable()
		if err != nil {
			return err
		}
		cli := quote(filepath.Join(filepath.Dir(executable), "cc-connect"))
		command := "test \"$CC_PROJECT\" = 'rotation-live' && test \"$CC_SESSION_KEY\" = 'rotation-test:isolated:user' && test \"$CC_DATA_DIR\" = " + dataDir + " && test -n \"$CC_SESSION_ID\" && test -n \"$CC_SESSION_SCOPE\" && " + cli + " session new --data-dir " + dataDir + " --prompt 'Reply exactly HANDOFF_SECRET_ROT1, do no other work.' --json"
		send("This is an isolated session-rotation test. Run exactly one shell command: " + command + ". It must return SCHEDULED. If the tool fails or does not return SCHEDULED, reply exactly SOURCE_ROTATION_FAILED. Otherwise reply exactly SOURCE_DONE. Do not use cc-connect send, /new, background tasks, or other tools.")
		if err = p.wait("HANDOFF_SECRET_ROT1"); err != nil {
			return err
		}
		active := e.GetSessions().LookupActive(key)
		if active == nil {
			return fmt.Errorf("new active missing")
		}
		p.mu.Lock()
		oldID := p.sourceAgentID
		sent := append([]string(nil), p.sent...)
		p.mu.Unlock()
		newID := active.GetAgentSessionID()
		if oldID == "" || newID == "" || oldID == newID {
			return fmt.Errorf("backend IDs not distinct: %q %q", oldID, newID)
		}
		firstDone := -1
		notice := -1
		handoff := -1
		for i, s := range sent {
			if firstDone < 0 && strings.Contains(s, "SOURCE_DONE") {
				firstDone = i
			}
			if notice < 0 && strings.Contains(s, "Session switched") {
				notice = i
			}
			if handoff < 0 && strings.Contains(s, "HANDOFF_SECRET_ROT1") {
				handoff = i
			}
		}
		if firstDone < 0 || notice <= firstDone || handoff <= notice {
			return fmt.Errorf("reply ordering invalid")
		}
		if !e.GetSessions().IsRetiredAgentID(oldID) {
			return fmt.Errorf("source backend not retired")
		}
		return writeJSON(filepath.Join(*root, "phase1.json"), map[string]any{"pass": true, "pid": os.Getpid(), "source_agent_id": oldID, "new_agent_id": newID, "active_session_id": active.ID, "platform_messages": sent})
	}
	raw, err := os.ReadFile(filepath.Join(*root, "phase1.json"))
	if err != nil {
		return err
	}
	var before struct {
		Source  string `json:"source_agent_id"`
		New     string `json:"new_agent_id"`
		Logical string `json:"active_session_id"`
	}
	if err = json.Unmarshal(raw, &before); err != nil {
		return err
	}
	current := e.GetSessions().LookupActive(key)
	if current == nil || current.ID != before.Logical || current.GetAgentSessionID() != before.New {
		return fmt.Errorf("restart changed active")
	}
	if e.GetSessions().SwitchToAgentSession(key, before.Source, *agentName, "retired") != nil {
		return fmt.Errorf("source restored on restart")
	}
	send("Reply with the exact handoff marker you received before the process restarted, followed by RESTART_SECRET_ROT1. Do no other work.")
	if err = p.wait("RESTART_SECRET_ROT1"); err != nil {
		return err
	}
	p.mu.Lock()
	replies := strings.Join(p.sent, "\n")
	p.mu.Unlock()
	if !strings.Contains(replies, "HANDOFF_SECRET_ROT1") {
		return fmt.Errorf("restart lost handoff context")
	}
	current = e.GetSessions().LookupActive(key)
	if *agentName == "codex" && current.GetAgentSessionID() != before.New {
		return fmt.Errorf("Codex did not resume the new thread")
	}
	if current.GetAgentSessionID() == before.Source {
		return fmt.Errorf("source resumed")
	}
	return writeJSON(filepath.Join(*root, "phase2.json"), map[string]any{"pass": true, "pid": os.Getpid(), "active_session_id": current.ID, "resumed_agent_id": current.GetAgentSessionID(), "retired_source": before.Source, "handoff_recalled": true})
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// prepareCodexHome uses the existing local login without touching its session
// store. Credentials stay on this machine and must not be included in artifacts.
func prepareCodexHome(source, target string) error {
	if source == "" {
		return fmt.Errorf("source-codex-home required for isolated native home")
	}
	sourceAbs, err := filepath.Abs(source)
	if err != nil {
		return err
	}
	targetAbs, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	if sourceAbs == targetAbs {
		return fmt.Errorf("source and isolated home must differ")
	}
	if err = os.MkdirAll(target, 0700); err != nil {
		return err
	}
	auth := filepath.Join(target, "auth.json")
	if _, err = os.Stat(auth); os.IsNotExist(err) {
		data, readErr := os.ReadFile(filepath.Join(source, "auth.json"))
		if readErr != nil {
			return fmt.Errorf("existing login required: %w", readErr)
		}
		if err = os.WriteFile(auth, data, 0600); err != nil {
			return err
		}
	}
	configPath := filepath.Join(target, "config.toml")
	if _, err = os.Stat(configPath); err == nil {
		return nil
	}
	var original map[string]any
	if _, err = toml.DecodeFile(filepath.Join(source, "config.toml"), &original); err != nil {
		return err
	}
	minimal := map[string]any{"cli_auth_credentials_store": "file"}
	for _, key := range []string{"model", "model_provider", "model_providers", "model_reasoning_effort", "model_reasoning_summary", "model_verbosity", "model_context_window", "model_auto_compact_token_limit", "model_supports_reasoning_summaries"} {
		if value, ok := original[key]; ok {
			minimal[key] = value
		}
	}
	f, err := os.OpenFile(configPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	return toml.NewEncoder(f).Encode(minimal)
}

package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// TopicInfoResolver optionally resolves missing group metadata for topic
// creation only. Reply-context reconstruction and ordinary sends stay offline.
type TopicInfoResolver interface {
	ResolveTopicInfo(context.Context, any) (TopicInfo, error)
}

type TopicRecord struct {
	BodyHash string `json:"body_sha256"`

	RequestID  string    `json:"request_id"`
	SessionKey string    `json:"session_key"`
	Platform   string    `json:"platform"`
	ChatID     string    `json:"chat_id"`
	To         string    `json:"to"`
	Title      string    `json:"title"`
	CreatedAt  time.Time `json:"created_at"`
	TopicReceipt
	Latest TopicLatest `json:"latest"`
	Error  string      `json:"error,omitempty"`
}
type TopicRequest struct {
	Project    string `json:"project"`
	SessionKey string `json:"session_key"`
	RequestID  string `json:"request_id"`
	To         string `json:"to"`
	Title      string `json:"title"`
	Body       string `json:"body"`
}

func (e *Engine) topicStorePath() string {
	return filepath.Join(e.dataDir, "topics", fmt.Sprintf("%x.json", []byte(e.name)))
}
func (e *Engine) loadTopicsLocked() error {
	if e.topicRecordsLoaded {
		return nil
	}
	path := e.topicStorePath()
	b, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	records := map[string]TopicRecord{}
	if len(b) > 0 {
		if err = json.Unmarshal(b, &records); err != nil {
			return err
		}
	}
	e.topicRecords = records
	e.topicRecordsLoaded = true
	return nil
}
func (e *Engine) putTopic(r TopicRecord) error {
	e.topicRecordsMu.Lock()
	defer e.topicRecordsMu.Unlock()
	if err := e.loadTopicsLocked(); err != nil {
		return err
	}
	next := make(map[string]TopicRecord, len(e.topicRecords)+1)
	for k, v := range e.topicRecords {
		next[k] = v
	}
	next[r.RequestID] = r
	b, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return err
	}
	path := e.topicStorePath()
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".topic-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(name, path)
	}
	if err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr = dir.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	e.topicRecords = next
	return nil
}
func (e *Engine) topicRecordsForSession(key string) ([]TopicRecord, error) {
	e.topicRecordsMu.Lock()
	defer e.topicRecordsMu.Unlock()
	if err := e.loadTopicsLocked(); err != nil {
		return nil, err
	}
	out := []TopicRecord{}
	for _, r := range e.topicRecords {
		if key == "" || r.SessionKey == key {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}
func (e *Engine) CreateTopic(req TopicRequest) (TopicRecord, error) {
	if len(req.RequestID) != 32 || !validTopicRequestID(req.RequestID) || !utf8.ValidString(req.Body) || !utf8.ValidString(req.Title) || strings.ContainsAny(req.Title, "\r\n") || req.SessionKey == "" || strings.TrimSpace(req.To) == "" || strings.TrimSpace(req.Title) == "" || strings.TrimSpace(req.Body) == "" || len(req.Body) > 65536 || len([]rune(req.Title)) > 100 {
		return TopicRecord{}, fmt.Errorf("request_id, session_key, to, title (max 100 chars), body (max 64 KiB) required")
	}
	_, sm := e.sessionContextForKey(req.SessionKey)
	if sm.LookupActive(req.SessionKey) == nil {
		return TopicRecord{}, fmt.Errorf("unknown session key")
	}
	target, err := e.resolveSendTarget(req.SessionKey, false)
	if err != nil {
		return TopicRecord{}, err
	}
	tp, ok := target.platform.(TopicPlatform)
	if !ok {
		return TopicRecord{}, fmt.Errorf("platform does not support topic creation")
	}
	info := tp.TopicInfo(target.replyCtx)
	if resolver, ok := tp.(TopicInfoResolver); ok {
		info, err = resolver.ResolveTopicInfo(e.ctx, target.replyCtx)
		if err != nil {
			return TopicRecord{}, fmt.Errorf("resolve topic group context: %w", err)
		}
	}
	if info.ChatID == "" {
		return TopicRecord{}, fmt.Errorf("not a group context")
	}
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(req.Body)))
	// Serialize only registration; remote calls never hold the persistent-store lock.
	e.topicCreateMu.Lock()
	records, err := e.topicRecordsForSession("")
	if err != nil {
		e.topicCreateMu.Unlock()
		return TopicRecord{}, err
	}
	for _, r := range records {
		if r.RequestID == req.RequestID {
			e.topicCreateMu.Unlock()
			if r.SessionKey != req.SessionKey || r.Title != req.Title || r.To != req.To || r.BodyHash != hash {
				return r, fmt.Errorf("request_id already used")
			}
			if r.Status != "sent" {
				return r, fmt.Errorf("prior topic request is %s: %s; do not blindly replay", r.Status, r.Error)
			}
			return r, nil
		}
	}
	record := TopicRecord{BodyHash: hash, RequestID: req.RequestID, SessionKey: req.SessionKey, Platform: target.platform.Name(), ChatID: info.ChatID, To: req.To, Title: req.Title, CreatedAt: time.Now(), TopicReceipt: TopicReceipt{Status: "creating"}}
	err = e.putTopic(record)
	e.topicCreateMu.Unlock()
	if err != nil {
		return record, err
	}
	receipt, sendErr := tp.CreateTopic(e.ctx, target.replyCtx, req.To, req.Title, req.Body, req.RequestID)
	record.TopicReceipt = receipt
	if sendErr != nil {
		record.Status = "failed"
		record.Error = sendErr.Error()
	}
	if err := e.putTopic(record); err != nil {
		return record, fmt.Errorf("topic outcome persistence failed (root=%s thread=%s): %w", record.RootID, record.ThreadID, err)
	}
	return record, sendErr
}
func (s *APIServer) handleTopics(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TopicRequest
		Platform string `json:"platform"`
		ChatID   string `json:"chat_id"`
	}
	if r.Method == http.MethodGet {
		req.Project = r.URL.Query().Get("project")
		req.SessionKey = r.URL.Query().Get("session_key")
		req.Platform = r.URL.Query().Get("platform")
		req.ChatID = r.URL.Query().Get("chat_id")
	} else if r.Method == http.MethodPost {
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 512<<10)).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", 400)
			return
		}
	} else {
		http.Error(w, "method not allowed", 405)
		return
	}
	s.mu.RLock()
	e := s.engines[req.Project]
	s.mu.RUnlock()
	if e == nil {
		http.Error(w, "unknown project", 404)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/topics/new" && r.Method == http.MethodPost:
		record, err := e.CreateTopic(req.TopicRequest)
		if err != nil {
			w.WriteHeader(409)
		}
		_ = json.NewEncoder(w).Encode(struct {
			Record TopicRecord `json:"topic"`
			Error  string      `json:"error,omitempty"`
		}{record, errorString(err)})
	case r.URL.Path == "/topics" && r.Method == http.MethodGet:
		records, err := e.topicRecordsForSession(req.SessionKey)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		for i := range records {
			for _, platform := range e.platforms {
				if platform.Name() != records[i].Platform {
					continue
				}
				if tp, ok := platform.(TopicPlatform); ok && records[i].ThreadID != "" {
					latest, err := tp.TopicLatest(r.Context(), records[i].ThreadID)
					if err != nil {
						latest.Error = err.Error()
					}
					records[i].Latest = latest
				}
			}
		}
		_ = json.NewEncoder(w).Encode(records)
	case r.URL.Path == "/topics/stop" && r.Method == http.MethodPost:
		if req.Platform == "" || req.ChatID == "" {
			http.Error(w, "platform and chat_id required", 400)
			return
		}
		_ = json.NewEncoder(w).Encode(e.StopTopics(req.Platform, req.ChatID))
	case r.URL.Path == "/topics/sessions" && r.Method == http.MethodGet:
		_ = json.NewEncoder(w).Encode(e.TopicSessions(req.Platform, req.ChatID))
	default:
		http.Error(w, "method not allowed", 405)
	}
}
func errorString(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

func validTopicRequestID(id string) bool {
	b, err := hex.DecodeString(id)
	return err == nil && len(b) == 16
}

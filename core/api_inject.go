package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

// InjectRequest is the body of POST /inject.
type InjectRequest struct {
	Project    string `json:"project"`
	SessionKey string `json:"session_key"`
	Message    string `json:"message"`
}

func (s *APIServer) handleInject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req InjectRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.SessionKey == "" || req.Message == "" {
		http.Error(w, "session_key and message are required", http.StatusBadRequest)
		return
	}
	s.mu.RLock()
	var engine *Engine
	var ok bool
	if req.Project != "" {
		engine, ok = s.engines[req.Project]
	} else if len(s.engines) == 1 {
		for _, e := range s.engines {
			engine, ok = e, true
		}
	}
	s.mu.RUnlock()
	if !ok {
		if req.Project == "" {
			http.Error(w, "project is required (multiple projects configured)", http.StatusBadRequest)
			return
		}
		http.Error(w, fmt.Sprintf("project %q not found", req.Project), http.StatusNotFound)
		return
	}
	status, err := engine.InjectToBusySession(req.SessionKey, req.Message)
	if err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, ErrInjectUnsupported) {
			code = http.StatusConflict
		}
		http.Error(w, err.Error(), code)
		return
	}
	apiJSON(w, http.StatusOK, map[string]string{"status": status})
}

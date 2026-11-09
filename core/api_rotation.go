package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"unicode/utf8"
)

func (s *APIServer) rotationEngine(project string) (*Engine, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if project != "" {
		if e := s.engines[project]; e != nil {
			return e, nil
		}
		return nil, ErrRotationNotFound
	}
	if len(s.engines) == 1 {
		for _, e := range s.engines {
			return e, nil
		}
	}
	return nil, fmt.Errorf("project is required")
}

func rotationAPIError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, ErrRotationNotFound) {
		status = http.StatusNotFound
	}
	if errors.Is(err, ErrRotationStale) || errors.Is(err, ErrRotationNativeBusy) {
		status = http.StatusConflict
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

func (s *APIServer) handleSessionRotation(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/session/new" {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", 405)
			return
		}
		var req RotateRequest
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 512<<10))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			http.Error(w, "invalid rotation request", 400)
			return
		}
		var extra any
		if err := dec.Decode(&extra); err != io.EOF {
			http.Error(w, "one JSON request required", 400)
			return
		}
		if req.SessionKey == "" || req.ExpectedSessionID == "" || req.ExpectedScope == "" || req.RequestID == "" || !utf8.ValidString(req.Handoff) || len(req.Handoff) > 65536 || strings.TrimSpace(req.Handoff) == "" {
			http.Error(w, "invalid session key, expected ID, request ID or handoff", 400)
			return
		}
		e, err := s.rotationEngine(req.Project)
		if err != nil {
			rotationAPIError(w, err)
			return
		}
		op, err := e.ScheduleRotation(req)
		if err != nil {
			rotationAPIError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		status := http.StatusOK
		if op.Status == "SCHEDULED" {
			status = http.StatusAccepted
		}
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(op)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", 405)
		return
	}
	e, err := s.rotationEngine(r.URL.Query().Get("project"))
	if err != nil {
		rotationAPIError(w, err)
		return
	}
	key := r.URL.Query().Get("session_key")
	if key == "" {
		http.Error(w, "session_key required", 400)
		return
	}
	_, sm := e.sessionContextForKey(key)
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == "/session/current" {
		active := sm.LookupActive(key)
		if active == nil {
			rotationAPIError(w, ErrRotationNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"project": e.name, "session_key": key, "session_id": active.ID, "session_scope": sm.RotationScope()})
		return
	}
	op, err := sm.Rotation(r.URL.Query().Get("request_id"))
	if err != nil {
		rotationAPIError(w, err)
		return
	}
	if op.SessionKey != key {
		rotationAPIError(w, ErrRotationNotFound)
		return
	}
	_ = json.NewEncoder(w).Encode(op)
}

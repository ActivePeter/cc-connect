package core

import (
	"encoding/json"
	"io"
	"net/http"
)

// Local Unix socket only. Reporting our own bot's event uses this endpoint,
// because messaging platforms do not reliably echo a bot's own messages.
func (s *APIServer) handleBatch(w http.ResponseWriter, r *http.Request) {
	project := r.URL.Query().Get("project")
	s.mu.RLock()
	e := s.engines[project]
	s.mu.RUnlock()
	if e == nil || e.batchInbox == nil {
		http.Error(w, "batch inbox not configured", http.StatusNotFound)
		return
	}
	b := e.batchInbox
	switch r.URL.Path {
	case "/batch/status":
		if r.Method != "GET" {
			http.Error(w, "GET only", 405)
			return
		}
		apiJSON(w, 200, b.status())
	case "/batch/enqueue":
		if r.Method != "POST" {
			http.Error(w, "POST only", 405)
			return
		}
		var m BatchMessage
		if err := json.NewDecoder(io.LimitReader(r.Body, 256<<10)).Decode(&m); err != nil {
			http.Error(w, "invalid JSON", 400)
			return
		}
		if id := batchEventID(m.Content); id != "" {
			m.ID = "event:" + id
		}
		if err := b.enqueue(m, false); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		apiJSON(w, 200, map[string]string{"status": "accepted"})
	case "/batch/retry":
		if r.Method != "POST" {
			http.Error(w, "POST only", 405)
			return
		}
		b.mu.Lock()
		b.disk.Paused = false
		err := b.saveLocked()
		b.mu.Unlock()
		if err != nil {
			http.Error(w, "cannot save retry", 500)
			return
		}
		apiJSON(w, 200, b.status())
	default:
		http.NotFound(w, r)
	}
}

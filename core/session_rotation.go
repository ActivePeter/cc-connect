package core

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	ErrRotationNotFound = errors.New("session or rotation not found")
	ErrRotationStale    = errors.New("source session or rotation changed")
)

// FirstInput is an outbox. An interrupted in-flight delivery is never silently
// replayed: backend acceptance may have occurred before the receipt was saved.
type FirstInput struct {
	RequestID string `json:"request_id"`
	Text      string `json:"text"`
	State     string `json:"state"`
}

type RotationOperation struct {
	RequestID  string    `json:"request_id"`
	SessionKey string    `json:"session_key"`
	SourceID   string    `json:"source_id"`
	TargetID   string    `json:"target_id"`
	Name       string    `json:"name,omitempty"`
	Handoff    string    `json:"handoff"`
	Revision   uint64    `json:"revision"`
	Status     string    `json:"status"`
	Error      string    `json:"error,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

func (s *Session) isRetiredID(id string) bool {
	if s.retirement == nil || id == "" {
		return false
	}
	_, ok := s.retirement.Load(id)
	return ok
}

func (sm *SessionManager) IsRetiredAgentID(id string) bool {
	_, ok := sm.retirement.Load(id)
	return id != "" && ok
}

func (sm *SessionManager) LookupActive(key string) *Session {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	s := sm.sessions[sm.activeSession[key]]
	if s != nil && !s.Retired {
		return s
	}
	return nil
}

func cloneSession(s *Session) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := &Session{ID: s.ID, Name: s.Name, AgentSessionID: s.AgentSessionID,
		AgentType: s.AgentType, PastAgentSessionIDs: append([]string(nil), s.PastAgentSessionIDs...),
		ActiveProvider: s.ActiveProvider, History: append([]HistoryEntry(nil), s.History...),
		CreatedAt: s.CreatedAt, UpdatedAt: s.UpdatedAt, LastUserActivity: s.LastUserActivity,
		Retired: s.Retired}
	if cp.AgentSessionID == ContinueSession {
		cp.AgentSessionID = ""
	}
	if s.PendingFirstInput != nil {
		v := *s.PendingFirstInput
		cp.PendingFirstInput = &v
	}
	return cp
}

func (sm *SessionManager) snapshotLocked() sessionSnapshot {
	snap := sessionSnapshot{Sessions: make(map[string]*Session), ActiveSession: make(map[string]string),
		UserSessions: make(map[string][]string), Counter: sm.counter, SessionNames: make(map[string]string),
		UserMeta: make(map[string]*UserMeta), PastIDTracking: true, LegacyData: sm.legacyData, Version: snapshotVersion,
		Rotations: make(map[string]*RotationOperation), RetiredAgentIDs: make(map[string]bool)}
	for id, s := range sm.sessions {
		snap.Sessions[id] = cloneSession(s)
	}
	for k, v := range sm.activeSession {
		snap.ActiveSession[k] = v
	}
	for k, v := range sm.userSessions {
		snap.UserSessions[k] = append([]string(nil), v...)
	}
	for k, v := range sm.sessionNames {
		snap.SessionNames[k] = v
	}
	for k, v := range sm.userMeta {
		if v != nil {
			cp := *v
			snap.UserMeta[k] = &cp
		}
	}
	for k, v := range sm.rotations {
		cp := *v
		snap.Rotations[k] = &cp
	}
	for k, v := range sm.retiredAgentIDs {
		snap.RetiredAgentIDs[k] = v
	}
	if snap.LegacyData {
		all := true
		for _, s := range snap.Sessions {
			if s.AgentSessionID == "" && len(s.PastAgentSessionIDs) == 0 {
				all = false
				break
			}
		}
		if all {
			snap.LegacyData = false
		}
	}
	return snap
}

func (sm *SessionManager) writeSnapshotLocked(snap sessionSnapshot) error {
	if sm.storePath == "" {
		return nil
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal sessions: %w", err)
	}
	if err = os.MkdirAll(filepath.Dir(sm.storePath), 0755); err != nil {
		return fmt.Errorf("create sessions directory: %w", err)
	}
	if err = AtomicWriteFile(sm.storePath, data, 0600); err != nil {
		return fmt.Errorf("commit sessions: %w", err)
	}
	return nil
}

// PrepareRotation persists the latest uncommitted request without mutating the
// active mapping. All methods take manager before session locks.
func (sm *SessionManager) PrepareRotation(key, expected, request, name, handoff string) (RotationOperation, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if op := sm.rotations[request]; op != nil {
		if op.SessionKey != key || op.SourceID != expected || op.Name != name || op.Handoff != handoff {
			return RotationOperation{}, ErrRotationStale
		}
		return *op, nil
	}
	source := sm.sessions[sm.activeSession[key]]
	if source == nil {
		return RotationOperation{}, ErrRotationNotFound
	}
	if source.ID != expected || source.Retired {
		return RotationOperation{}, ErrRotationStale
	}
	snap := sm.snapshotLocked()
	rev := uint64(1)
	for _, op := range snap.Rotations {
		if op.SessionKey != key {
			continue
		}
		if op.Revision >= rev {
			rev = op.Revision + 1
		}
		if op.Status == "SCHEDULED" {
			op.Status = "SUPERSEDED"
		}
	}
	snap.Counter++
	op := RotationOperation{RequestID: request, SessionKey: key, SourceID: expected, TargetID: fmt.Sprintf("s%d", snap.Counter),
		Name: name, Handoff: handoff, Revision: rev, Status: "SCHEDULED", CreatedAt: time.Now()}
	snap.Rotations[request] = &op
	if err := sm.writeSnapshotLocked(snap); err != nil {
		return RotationOperation{}, err
	}
	sm.counter = snap.Counter
	sm.rotations = snap.Rotations
	return op, nil
}

func (sm *SessionManager) Rotation(request string) (RotationOperation, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if op := sm.rotations[request]; op != nil {
		return *op, nil
	}
	return RotationOperation{}, ErrRotationNotFound
}

func (sm *SessionManager) PendingRotation(key, source string) (RotationOperation, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for _, op := range sm.rotations {
		if op.SessionKey == key && op.SourceID == source && op.Status == "SCHEDULED" {
			return *op, true
		}
	}
	return RotationOperation{}, false
}

func (sm *SessionManager) FinishRotation(request, status, reason string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := sm.rotations[request]
	if op == nil {
		return ErrRotationNotFound
	}
	if op.Status != "SCHEDULED" {
		return nil
	}
	snap := sm.snapshotLocked()
	snap.Rotations[request].Status = status
	snap.Rotations[request].Error = reason
	if err := sm.writeSnapshotLocked(snap); err != nil {
		// Terminal failure is visible in memory even when its receipt cannot
		// be persisted. Restart cancels any unconfirmed on-disk request.
		volatile := *snap.Rotations[request]
		volatile.Error += "; receipt not persisted: " + err.Error()
		sm.rotations[request] = &volatile
		return err
	}
	sm.rotations = snap.Rotations
	return nil
}

func (sm *SessionManager) CancelRotations(key, reason string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	snap := sm.snapshotLocked()
	changed := false
	for _, op := range snap.Rotations {
		if op.SessionKey == key && op.Status == "SCHEDULED" {
			op.Status = "CANCELLED"
			op.Error = reason
			changed = true
		}
	}
	if !changed {
		return nil
	}
	if err := sm.writeSnapshotLocked(snap); err != nil {
		sm.rotations = snap.Rotations
		return err
	}
	sm.rotations = snap.Rotations
	return nil
}

// CommitRotation publishes only after the complete candidate snapshot is durable.
func (sm *SessionManager) CommitRotation(op RotationOperation) (*Session, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	current := sm.rotations[op.RequestID]
	if current == nil || current.Status != "SCHEDULED" || current.Revision != op.Revision || sm.activeSession[op.SessionKey] != op.SourceID {
		return nil, ErrRotationStale
	}
	source := sm.sessions[op.SourceID]
	snap := sm.snapshotLocked()
	old := snap.Sessions[op.SourceID]
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.Retired || source.AgentSessionID != old.AgentSessionID {
		return nil, ErrRotationStale
	}
	old.Retired = true
	ids := append(append([]string(nil), old.PastAgentSessionIDs...), old.AgentSessionID)
	for _, id := range ids {
		if id != "" && id != ContinueSession {
			snap.RetiredAgentIDs[id] = true
		}
	}
	now := time.Now()
	fresh := &Session{busy: true, ID: op.TargetID, Name: op.Name, CreatedAt: now, UpdatedAt: now, ActiveProvider: source.ActiveProvider,
		PendingFirstInput: &FirstInput{RequestID: op.RequestID, Text: op.Handoff, State: "pending"}}
	snap.Sessions[fresh.ID] = fresh
	snap.ActiveSession[op.SessionKey] = fresh.ID
	snap.UserSessions[op.SessionKey] = append(snap.UserSessions[op.SessionKey], fresh.ID)
	snap.Rotations[op.RequestID].Status = "SWITCHED"
	if err := sm.writeSnapshotLocked(snap); err != nil {
		return nil, err
	}
	source.Retired = true
	sm.retiredAgentIDs = snap.RetiredAgentIDs
	for id := range sm.retiredAgentIDs {
		sm.retirement.Store(id, true)
	}
	fresh.retirement = &sm.retirement
	sm.sessions[fresh.ID] = fresh
	sm.activeSession = snap.ActiveSession
	sm.userSessions = snap.UserSessions
	sm.rotations = snap.Rotations
	return fresh, nil
}

// SetFirstInputState writes the outbox receipt before publishing it.
func (sm *SessionManager) SetFirstInputState(s *Session, state string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	snap := sm.snapshotLocked()
	cp := snap.Sessions[s.ID]
	if cp == nil || cp.PendingFirstInput == nil {
		return ErrRotationNotFound
	}
	cp.PendingFirstInput.State = state
	if err := sm.writeSnapshotLocked(snap); err != nil {
		return err
	}
	s.mu.Lock()
	s.PendingFirstInput = cp.PendingFirstInput
	s.mu.Unlock()
	return nil
}

func (s *Session) FirstInput() *FirstInput {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.PendingFirstInput == nil {
		return nil
	}
	cp := *s.PendingFirstInput
	return &cp
}

func (sm *SessionManager) recoverRotationsLocked() {
	if sm.rotations == nil {
		sm.rotations = make(map[string]*RotationOperation)
	}
	if sm.retiredAgentIDs == nil {
		sm.retiredAgentIDs = make(map[string]bool)
	}
	for id := range sm.retiredAgentIDs {
		sm.retirement.Store(id, true)
	}
	for _, s := range sm.sessions {
		s.retirement = &sm.retirement
	}
	for _, op := range sm.rotations {
		if op.Status == "SCHEDULED" {
			op.Status = "CANCELLED"
			op.Error = "process restarted before reply delivery was confirmed"
			slog.Warn("session rotation cancelled on restart", "request_id", op.RequestID)
		}
	}
}

// Ensure the fence pointer remains an ordinary shared synchronization object.
var _ *sync.Map

// KeyForSession uses persisted ownership, independent of workspace path syntax.
func (sm *SessionManager) KeyForSession(id string) string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	for key, ids := range sm.userSessions {
		for _, sid := range ids {
			if sid == id {
				return key
			}
		}
	}
	return ""
}

// RotationScope prevents equal local IDs in two workspace stores from matching.
func (sm *SessionManager) RotationScope() string {
	material := sm.storePath
	if material == "" {
		material = fmt.Sprintf("memory:%p", sm)
	} else if absolute, err := filepath.Abs(material); err == nil {
		material = absolute
	}
	digest := sha256.Sum256([]byte(material))
	return fmt.Sprintf("%x", digest[:16])
}

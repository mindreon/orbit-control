// Package memstore is the in-process Repository used by tests and local dev
// without ORBIT_CONTROL_DB_URL. It mirrors the Postgres visibility rules:
// soft-deleted rooms and their children are invisible, and an idempotency key
// that points at an invisible room yields ErrNotFound.
package memstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"sort"
	"sync"
	"time"

	"github.com/mindreon/orbit-control/internal/store"
)

type roomRow struct {
	rec       store.RoomRecord
	deletedAt *time.Time
	deletedBy string
}

type approvalRow struct {
	tenantID string
	rec      store.ApprovalRecord
}

type messageRow struct {
	tenantID string
	rec      store.MessageRecord
}

type userRow struct {
	tenantID string
	rec      store.UserRecord
}

type idemRow struct {
	requestHash []byte
	taskID      string
	expiresAt   time.Time
}

type Store struct {
	mu        sync.Mutex
	users     map[string]userRow
	rooms     map[string]*roomRow
	messages  map[string][]messageRow
	approvals map[string]*approvalRow
	idem      map[string]idemRow
}

var _ store.Repository = (*Store)(nil)

func New() *Store {
	return &Store{
		users:     map[string]userRow{},
		rooms:     map[string]*roomRow{},
		messages:  map[string][]messageRow{},
		approvals: map[string]*approvalRow{},
		idem:      map[string]idemRow{},
	}
}

func (s *Store) Close() {}

// CheckTenant accepts any non-empty tenant: the in-memory store has no
// tenant registry (dev/test only).
func (s *Store) CheckTenant(_ context.Context, tenantID string) error {
	if tenantID == "" {
		return store.ErrNotFound
	}
	return nil
}

func (s *Store) UpsertUser(_ context.Context, tenantID string, u store.UserRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[u.ID]; !ok {
		s.users[u.ID] = userRow{tenantID: tenantID, rec: u}
	}
	return nil
}

// liveRoom must be called with s.mu held.
func (s *Store) liveRoom(tenantID, roomID string) (*roomRow, bool) {
	row, ok := s.rooms[roomID]
	if !ok || row.rec.TenantID != tenantID || row.deletedAt != nil {
		return nil, false
	}
	return row, true
}

func idemKey(tenantID string, rec *store.IdempotencyRecord) string {
	return tenantID + "\x00" + rec.CreatedBy + "\x00" + hex.EncodeToString(rec.KeyHash)
}

func (s *Store) CreateRoom(_ context.Context, tenantID string, room store.RoomRecord, idem *store.IdempotencyRecord) (store.RoomRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if idem != nil {
		if existing, ok := s.idem[idemKey(tenantID, idem)]; ok {
			row, visible := s.liveRoom(tenantID, existing.taskID)
			if !visible {
				return store.RoomRecord{}, false, store.ErrNotFound
			}
			if existing.expiresAt.After(now) {
				if !bytes.Equal(existing.requestHash, idem.RequestHash) {
					return store.RoomRecord{}, false, store.ErrIdempotencyKeyReused
				}
				return row.rec, true, nil
			}
		}
	}
	room.TenantID = tenantID
	if room.CreatedAt.IsZero() {
		room.CreatedAt = now
	}
	room.UpdatedAt = room.CreatedAt
	s.rooms[room.ID] = &roomRow{rec: room}
	if idem != nil {
		s.idem[idemKey(tenantID, idem)] = idemRow{
			requestHash: append([]byte(nil), idem.RequestHash...),
			taskID:      room.ID,
			expiresAt:   idem.ExpiresAt,
		}
	}
	return room, false, nil
}

func (s *Store) GetRoom(_ context.Context, tenantID, userID, roomID string) (store.RoomRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.liveRoom(tenantID, roomID)
	if !ok || row.rec.CreatedBy != userID {
		return store.RoomRecord{}, store.ErrNotFound
	}
	return row.rec, nil
}

func (s *Store) GetRoomForWorker(_ context.Context, tenantID, roomID string) (store.RoomRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.liveRoom(tenantID, roomID)
	if !ok {
		return store.RoomRecord{}, store.ErrNotFound
	}
	return row.rec, nil
}

func (s *Store) ListRooms(_ context.Context, tenantID, userID string) ([]store.RoomRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []store.RoomRecord{}
	for _, row := range s.rooms {
		if row.rec.TenantID == tenantID && row.rec.CreatedBy == userID && row.deletedAt == nil {
			out = append(out, row.rec)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (s *Store) UpdateRoomState(_ context.Context, tenantID, roomID, state, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.liveRoom(tenantID, roomID)
	if !ok {
		return store.ErrNotFound
	}
	row.rec.State = state
	if sessionID != "" {
		row.rec.SessionID = sessionID
	}
	row.rec.UpdatedAt = time.Now().UTC()
	return nil
}

func (s *Store) SoftDeleteRoom(_ context.Context, tenantID, userID, roomID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.liveRoom(tenantID, roomID)
	if !ok || row.rec.CreatedBy != userID {
		return 0, nil
	}
	now := time.Now().UTC()
	row.deletedAt = &now
	row.deletedBy = userID
	return 1, nil
}

func (s *Store) AppendMessage(_ context.Context, tenantID string, m store.MessageRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.liveRoom(tenantID, m.TaskID); !ok {
		return store.ErrNotFound
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now().UTC()
	}
	s.messages[m.TaskID] = append(s.messages[m.TaskID], messageRow{tenantID: tenantID, rec: m})
	return nil
}

func (s *Store) ListMessages(_ context.Context, tenantID, userID, roomID string) ([]store.MessageRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.liveRoom(tenantID, roomID)
	if !ok || row.rec.CreatedBy != userID {
		return nil, store.ErrNotFound
	}
	out := []store.MessageRecord{}
	for _, m := range s.messages[roomID] {
		if m.tenantID == tenantID {
			out = append(out, m.rec)
		}
	}
	return out, nil
}

func (s *Store) CreateApproval(_ context.Context, tenantID string, a store.ApprovalRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.liveRoom(tenantID, a.TaskID); !ok {
		return store.ErrNotFound
	}
	if a.CreatedAt.IsZero() {
		a.CreatedAt = time.Now().UTC()
	}
	a.SessionID = ""
	s.approvals[a.ID] = &approvalRow{tenantID: tenantID, rec: a}
	return nil
}

// approvalView must be called with s.mu held.
func (s *Store) approvalView(tenantID, userID string, row *approvalRow) (store.ApprovalRecord, bool) {
	if row.tenantID != tenantID {
		return store.ApprovalRecord{}, false
	}
	room, ok := s.liveRoom(tenantID, row.rec.TaskID)
	if !ok || room.rec.CreatedBy != userID {
		return store.ApprovalRecord{}, false
	}
	rec := row.rec
	rec.SessionID = room.rec.SessionID
	return rec, true
}

func (s *Store) GetApproval(_ context.Context, tenantID, userID, approvalID string) (store.ApprovalRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.approvals[approvalID]
	if !ok {
		return store.ApprovalRecord{}, store.ErrNotFound
	}
	rec, ok := s.approvalView(tenantID, userID, row)
	if !ok {
		return store.ApprovalRecord{}, store.ErrNotFound
	}
	return rec, nil
}

func (s *Store) ListApprovals(_ context.Context, tenantID, userID string) ([]store.ApprovalRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []store.ApprovalRecord{}
	for _, row := range s.approvals {
		if rec, ok := s.approvalView(tenantID, userID, row); ok {
			out = append(out, rec)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (s *Store) ReopenApproval(_ context.Context, tenantID, approvalID, decision string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.approvals[approvalID]
	if !ok || row.tenantID != tenantID || row.rec.Status != "decided" || row.rec.Decision != decision {
		return nil
	}
	row.rec.Status = "pending"
	row.rec.Decision = ""
	row.rec.DecidedAt = nil
	return nil
}

func (s *Store) DecideApproval(_ context.Context, tenantID, approvalID, status, decision string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.approvals[approvalID]
	if !ok || row.tenantID != tenantID {
		return store.ErrNotFound
	}
	if _, ok := s.liveRoom(tenantID, row.rec.TaskID); !ok {
		return store.ErrNotFound
	}
	if row.rec.Status != "pending" {
		return store.ErrApprovalNotPending
	}
	now := time.Now().UTC()
	row.rec.Status = status
	row.rec.Decision = decision
	row.rec.DecidedAt = &now
	return nil
}

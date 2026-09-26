package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// ErrNotFound covers "does not exist", "belongs to another tenant or user",
// and "soft-deleted". Callers must not distinguish these (contract §16/§17).
var ErrNotFound = errors.New("not found")

// ErrIdempotencyKeyReused means the key was already used with a different
// request body (§9, 409 IDEMPOTENCY_KEY_REUSED).
var ErrIdempotencyKeyReused = errors.New("idempotency key reused with a different request")

// ErrApprovalNotPending means the approval was already decided (409). The
// decision UPDATE is conditional on status = 'pending', so concurrent
// decisions produce exactly one winner.
var ErrApprovalNotPending = errors.New("approval is not pending")

// ErrStorage wraps unexpected database failures. Its text is for server logs
// only and must not be echoed to clients.
var ErrStorage = errors.New("storage error")

// Repository is the tenant-scoped persistence boundary for control.
//
// Every method that reads or writes a tenant table takes tenantID explicitly;
// implementations must scope every statement by it (§18.5 query layer). The
// Postgres implementation additionally relies on RLS as a backstop.
type Repository interface {
	// CheckTenant returns ErrNotFound when the tenant does not exist. Control
	// never creates tenants; ops does (deploy/postgres/ensure-tenant.sql).
	CheckTenant(ctx context.Context, tenantID string) error
	// UpsertUser inserts the user if it does not exist yet.
	UpsertUser(ctx context.Context, tenantID string, u UserRecord) error

	// CreateRoom inserts a room. When idem is non-nil the idempotency key is
	// reserved in the same transaction. If the key already maps to a visible
	// room with the same request hash, that room is returned with
	// replayed=true and nothing is inserted. If the key maps to a room that is
	// no longer visible (soft-deleted), ErrNotFound is returned and nothing is
	// inserted (§18.7a M1-1).
	CreateRoom(ctx context.Context, tenantID string, room RoomRecord, idem *IdempotencyRecord) (out RoomRecord, replayed bool, err error)
	// GetRoom returns a live room created by userID.
	GetRoom(ctx context.Context, tenantID, userID, roomID string) (RoomRecord, error)
	// GetRoomForWorker returns a live room regardless of creator; used only
	// by the internal worker ingest path.
	GetRoomForWorker(ctx context.Context, tenantID, roomID string) (RoomRecord, error)
	ListRooms(ctx context.Context, tenantID, userID string) ([]RoomRecord, error)
	UpdateRoomState(ctx context.Context, tenantID, roomID, state, sessionID string) error
	// SoftDeleteRoom marks a live room created by userID as deleted and
	// returns the number of affected rows (0 → 404).
	SoftDeleteRoom(ctx context.Context, tenantID, userID, roomID string) (int64, error)

	AppendMessage(ctx context.Context, tenantID string, m MessageRecord) error
	ListMessages(ctx context.Context, tenantID, userID, roomID string) ([]MessageRecord, error)

	CreateApproval(ctx context.Context, tenantID string, a ApprovalRecord) error
	GetApproval(ctx context.Context, tenantID, userID, approvalID string) (ApprovalRecord, error)
	ListApprovals(ctx context.Context, tenantID, userID string) ([]ApprovalRecord, error)
	// DecideApproval moves a pending approval to status/decision; if it is no
	// longer pending it returns ErrApprovalNotPending. A decided approval is
	// final (DB trigger); there is no reopen in phase 1.
	DecideApproval(ctx context.Context, tenantID, approvalID, status, decision string) error

	Close()
}

type UserRecord struct {
	ID          string
	Issuer      string
	Subject     string
	DisplayName string
	Email       string
}

type RoomRecord struct {
	ID               string
	TenantID         string
	CreatedBy        string
	Kind             string
	Title            string
	State            string
	PermissionPreset string
	Runtime          json.RawMessage
	SessionID        string
	PersonaID        string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type MessageRecord struct {
	ID        string
	TaskID    string
	Role      string
	Type      string
	Text      string
	CreatedAt time.Time
}

type ApprovalRecord struct {
	ID                string
	TaskID            string
	SessionID         string // read-only: joined from rooms.session_id
	ApprovalRequestID string
	CallID            string
	ToolName          string
	Reason            string
	Status            string
	Decision          string
	CreatedAt         time.Time
	DecidedAt         *time.Time
}

// IdempotencyRecord carries only hashes; the raw Idempotency-Key is never
// stored.
type IdempotencyRecord struct {
	CreatedBy   string
	KeyHash     []byte
	RequestHash []byte
	ExpiresAt   time.Time
}

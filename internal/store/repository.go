package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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
	// longer pending it returns ErrApprovalNotPending. Reopen is T9 only
	// (not_delivered back to pending), and only through DeliveryStore.
	DecideApproval(ctx context.Context, tenantID, approvalID, status, decision string) error

	// ListPersonas returns this tenant's assistants, newest first.
	ListPersonas(ctx context.Context, tenantID string) ([]PersonaRecord, error)
	// CreatePersona inserts one assistant. Rows are not updated or deleted.
	CreatePersona(ctx context.Context, tenantID string, p PersonaRecord) error
	// ListMcpConnectors returns this tenant's connectors, newest first.
	ListMcpConnectors(ctx context.Context, tenantID string) ([]McpConnectorRecord, error)
	// CreateMcpConnector inserts one connector. EnvRefs are names, never values.
	CreateMcpConnector(ctx context.Context, tenantID string, c McpConnectorRecord) error

	// ListSkillCatalog reads the shared SkillHub copy. tenantID is required so
	// only an authenticated caller can ask; rows are not scoped by tenant.
	ListSkillCatalog(ctx context.Context, tenantID string, q SkillCatalogQuery) (SkillCatalogPage, error)
	// UpsertSkillCatalog inserts or refreshes catalog rows. An existing
	// trending rank is left as it is.
	UpsertSkillCatalog(ctx context.Context, tenantID string, rows []SkillRecord) error
	// ReplaceSkillTrending marks the current trending order. Ranks not in rows
	// are cleared. This does not call SkillHub.
	ReplaceSkillTrending(ctx context.Context, tenantID string, rows []SkillRecord) error
	// ListSkillCategories returns category labels for the shared catalog.
	ListSkillCategories(ctx context.Context, tenantID string) ([]SkillCategoryRecord, error)
	// UpsertSkillCategories stores category labels. It does not delete keys
	// that disappeared upstream.
	UpsertSkillCategories(ctx context.Context, tenantID string, rows []SkillCategoryRecord) error

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
	Failure          json.RawMessage
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
	DeliveryState     string
	DeliveryAttempt   int
	ResultAttempt     *int
	CreatedAt         time.Time
	DecidedAt         *time.Time
}

// EventRecord is one durable per-task event. Seq is the SSE cursor (§18.4).
type EventRecord struct {
	Seq       int64
	EventUID  string
	TaskID    string
	Type      string
	Source    string
	Payload   json.RawMessage
	CreatedAt time.Time
}

// DecideResultWrite is the idempotent persisted outcome of one delivery_attempt
// (FM-61). UpdateRoom is false when the workflow accepted the decision but
// returned no turn, so the room state is left as it is.
type DecideResultWrite struct {
	ApprovalID string          `json:"approvalId"`
	Attempt    int             `json:"attempt"`
	RoomID     string          `json:"roomId"`
	RoomState  string          `json:"roomState"`
	UpdateRoom bool            `json:"updateRoom"`
	Texts      []string        `json:"texts"`
	Next       *ApprovalRecord `json:"next,omitempty"`
}

// IdempotencyRecord carries only hashes; the raw Idempotency-Key is never
// stored.
type IdempotencyRecord struct {
	CreatedBy   string
	KeyHash     []byte
	RequestHash []byte
	ExpiresAt   time.Time
}

// PersonaRecord is one assistant persona. Env secrets are never stored here.
type PersonaRecord struct {
	ID              string
	Name            string
	Instructions    string
	McpConnectorIDs []string
	CreatedAt       time.Time
}

// McpConnectorRecord is one MCP launcher. EnvRefs and HeaderRefs are names
// only. HeaderRefs are stored as "Header-Name:ENV_NAME".
type McpConnectorRecord struct {
	ID          string
	Name        string
	Transport   string
	Command     string
	Args        []string
	EnvRefs     []string
	URL         string
	HeaderRefs  []string
	DefaultOpen bool
	CreatedAt   time.Time
}

// SkillRecord is one row of the shared SkillHub catalog. It is display
// metadata only: no package bytes and no secret values.
type SkillRecord struct {
	ID             string
	Slug           string
	Handle         string
	Name           string
	Description    string
	Category       string
	CategoryName   string
	IconURL        string
	Downloads      int64
	Stars          int64
	Source         string
	Version        string
	RequiresAPIKey bool
	Paid           bool
	Score          float64
	UpdatedAt      time.Time
	SyncedAt       time.Time
	TrendingRank   int
}

// SkillCategoryRecord is a display label for SkillRecord.Category.
type SkillCategoryRecord struct {
	Key       string
	Name      string
	NameEn    string
	SortOrder int
}

// SkillCatalogQuery selects a page of the local catalog.
// Sort is score, downloads, updated_at, stars, or trending.
// RequiresAPIKey and Paid are "", "true", or "false".
type SkillCatalogQuery struct {
	Sort           string
	Category       string
	Source         string
	Keyword        string
	RequiresAPIKey string
	Paid           string
	Page           int
	PageSize       int
}

// SkillCatalogPage is one page plus the newest sync time (zero when empty).
type SkillCatalogPage struct {
	Items    []SkillRecord
	Total    int
	Page     int
	PageSize int
	SyncedAt time.Time
}

// NormalizeSkillQuery clamps a catalog query to the values the stores implement.
func NormalizeSkillQuery(q SkillCatalogQuery) SkillCatalogQuery {
	switch q.Sort {
	case "downloads", "updated_at", "stars", "trending":
	default:
		q.Sort = "score"
	}
	q.Category = clipToken(q.Category, 64)
	switch q.Source {
	case "clawhub", "community", "enterprise":
	default:
		q.Source = ""
	}
	q.Keyword = clipText(q.Keyword, 80)
	q.RequiresAPIKey = boolWord(q.RequiresAPIKey)
	q.Paid = boolWord(q.Paid)
	if q.Page < 1 {
		q.Page = 1
	}
	if q.Page > 10000 {
		q.Page = 10000
	}
	if q.PageSize < 1 {
		q.PageSize = 24
	}
	if q.PageSize > 48 {
		q.PageSize = 48
	}
	return q
}

func boolWord(v string) string {
	switch strings.TrimSpace(strings.ToLower(v)) {
	case "true", "false":
		return strings.TrimSpace(strings.ToLower(v))
	default:
		return ""
	}
}

func clipText(s string, n int) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, "\x00", ""))
	r := []rune(s)
	if len(r) > n {
		return string(r[:n])
	}
	return s
}

func clipToken(s string, n int) string {
	s = clipText(s, n)
	if s == "" {
		return ""
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.' || r == '_' || r == '-':
		default:
			return ""
		}
	}
	return s
}

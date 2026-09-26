package store

import (
	"context"
	"encoding/json"
	"time"
)

// DeliveryStore is the C34 approval delivery machine. Only the Postgres
// repository implements it. The in-memory repository stays on DecideApproval.
type DeliveryStore interface {
	// ClaimDecision is T1: pending/NULL → decided + decision + in_flight,
	// delivery_attempt + 1. Zero rows is ErrApprovalNotPending.
	ClaimDecision(ctx context.Context, tenantID, approvalID, decision string) (attempt int, err error)
	// SetDeliveryState moves delivery_state from → to when delivery_attempt
	// equals attempt (T2, T3, T5, T11). false means the row no longer matches.
	SetDeliveryState(ctx context.Context, tenantID, approvalID, from, to string, attempt int) (bool, error)
	// ReopenUndelivered is T4 then T9 in one transaction. false means the
	// in_flight row was no longer this attempt.
	ReopenUndelivered(ctx context.Context, tenantID, approvalID string, attempt int) (bool, error)
	// ApplyDecideResult sets result_attempt = attempt only when it is still
	// NULL and delivery_state is delivered, and in that same transaction
	// inserts the resumed messages and updates the room (FM-61). false means
	// a previous write already recorded this attempt.
	ApplyDecideResult(ctx context.Context, tenantID string, w DecideResultWrite) (bool, error)
	// ListUnwrittenDeliveries returns delivered rows in tenantID whose
	// result was never written.
	ListUnwrittenDeliveries(ctx context.Context, tenantID string) ([]ApprovalRecord, error)
	// FailRoom marks the room failed with failure {code, message}.
	FailRoom(ctx context.Context, tenantID, roomID, code, message string) error
	// FailUnresolved applies T5 or T11 and marks the room failed in one
	// transaction (FM-65). false means the approval update matched 0 rows:
	// the transaction rolled back and the room was not failed.
	FailUnresolved(ctx context.Context, tenantID, approvalID, from string, attempt int, roomID string, failure json.RawMessage) (bool, error)
	// CancelPending is T10 for every pending approval of the room (abort).
	CancelPending(ctx context.Context, tenantID, roomID string) error
	// ApplyRoomFailed stores failure verbatim and, on the first write, appends
	// the event in the same transaction (FM-67). false means the room was
	// already failed and nothing was written.
	ApplyRoomFailed(ctx context.Context, tenantID, roomID string, failure json.RawMessage, ev EventRecord) (bool, error)
	// ListTenantIDs returns every tenant. The reconciler compensates all of
	// them (FM-71). tenants is not a tenant-scoped table.
	ListTenantIDs(ctx context.Context) ([]string, error)
	// ListReconcileRows returns the rows one reconcile pass may move.
	ListReconcileRows(ctx context.Context, tenantID string) ([]ReconcileRow, error)
	// SaveResultBody stashes the write-back while the row is delivered and
	// result_attempt is still NULL (FM-71).
	SaveResultBody(ctx context.Context, tenantID, approvalID string, attempt int, body json.RawMessage) (bool, error)
	// LoadResultBody reads that stash. false means there is no body.
	LoadResultBody(ctx context.Context, tenantID, approvalID string, attempt int) (DecideResultWrite, bool, error)
	// TransitionAt is T3 for the reconciler: the WHERE matches the attempt
	// and the delivery_updated_at that was read.
	TransitionAt(ctx context.Context, tenantID, approvalID string, attempt int, updatedAt time.Time) (bool, error)
	// ClaimRetry is T6. false means another pass already claimed it.
	ClaimRetry(ctx context.Context, tenantID, approvalID string, attempt int) (int, bool, error)
	// ReopenNotDelivered is T9 for a leftover not_delivered row.
	ReopenNotDelivered(ctx context.Context, tenantID, approvalID string, attempt int) (bool, error)
}

// ReconcileRow is one approval the database-driven reconciler may move.
type ReconcileRow struct {
	Approval          ApprovalRecord
	RoomState         string
	DeliveryUpdatedAt time.Time
	ResultBody        json.RawMessage
}

// EventStore is the durable per-task event log (contract §18.4).
type EventStore interface {
	// AppendEvent increments rooms.last_event_seq and inserts the event in
	// one transaction. allowFinished is true for control's own closing
	// event; a closed, failed, or deleted room otherwise yields ErrNotFound
	// and does not move the seq.
	AppendEvent(ctx context.Context, tenantID string, ev EventRecord, allowFinished bool) (int64, error)
	// EventsAfter returns events with seq > after, the room head, and whether
	// after is a known cursor (0 or a stored seq of this room). There is no
	// eviction window: an unknown cursor is not "expired".
	EventsAfter(ctx context.Context, tenantID, roomID string, after int64) (rows []EventRecord, head int64, cursorOK bool, err error)
	// RoomEventHead is the room's last_event_seq, or 0 when the room has no
	// events. A missing room is ErrNotFound.
	RoomEventHead(ctx context.Context, tenantID, roomID string) (int64, error)
	// FindRoomTenant resolves the tenant that owns roomID. It has no tenant
	// argument because the caller does not know the tenant yet (artifact
	// ingest). The lookup still filters rooms by tenant_id.
	FindRoomTenant(ctx context.Context, roomID string) (string, error)
}

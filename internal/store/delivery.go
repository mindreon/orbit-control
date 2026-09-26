package store

import "context"

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

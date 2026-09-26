package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/store"
)

func (s *Store) FailUnresolved(ctx context.Context, tenantID, approvalID, from string, attempt int, roomID string, failure json.RawMessage) (bool, error) {
	name := "t11"
	if from == "in_flight" {
		name = "t5"
	}
	if err := noteDeliveryFault(name); err != nil {
		return false, err
	}
	if len(failure) == 0 {
		failure = []byte(`{}`)
	}
	ok := false
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE approvals
			   SET delivery_state = 'unresolved'
			 WHERE tenant_id = $1 AND id = $2
			   AND delivery_state = $3
			   AND delivery_attempt = $4`,
			tenantID, approvalID, from, attempt)
		if err != nil {
			return raiseOr("fail unresolved", err)
		}
		if tag.RowsAffected() != 1 {
			return errLostRace
		}
		tag, err = tx.Exec(ctx, `
			UPDATE rooms
			   SET state = 'failed', failure = $3, updated_at = now()
			 WHERE tenant_id = $1 AND id = $2
			   AND deleted_at IS NULL
			   AND state <> 'failed'`,
			tenantID, roomID, failure)
		if err != nil {
			return storageErr("fail room with approval", err)
		}
		if tag.RowsAffected() != 1 {
			return errLostRace
		}
		ok = true
		return nil
	})
	if errors.Is(err, errLostRace) {
		return false, nil
	}
	return ok, err
}

func (s *Store) CancelPending(ctx context.Context, tenantID, roomID string) error {
	return s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			UPDATE approvals
			   SET status = 'cancelled', decided_at = now()
			 WHERE tenant_id = $1 AND task_id = $2
			   AND status = 'pending' AND delivery_state IS NULL`,
			tenantID, roomID)
		if err != nil {
			return raiseOr("cancel pending approvals", err)
		}
		return nil
	})
}

func (s *Store) ApplyRoomFailed(ctx context.Context, tenantID, roomID string, failure json.RawMessage, ev store.EventRecord) (bool, error) {
	if len(failure) == 0 {
		failure = []byte(`{}`)
	}
	wrote := false
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		var state string
		err := tx.QueryRow(ctx, `
			SELECT state FROM rooms
			 WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL
			 FOR UPDATE`,
			tenantID, roomID).Scan(&state)
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrNotFound
		}
		if err != nil {
			return storageErr("lock room", err)
		}
		if state == "failed" {
			return nil
		}
		tag, err := tx.Exec(ctx, `
			UPDATE rooms
			   SET state = 'failed', failure = $3, updated_at = now()
			 WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL`,
			tenantID, roomID, failure)
		if err != nil {
			return storageErr("apply room failed", err)
		}
		if tag.RowsAffected() != 1 {
			return store.ErrNotFound
		}
		var seq int64
		err = tx.QueryRow(ctx, `
			UPDATE rooms
			   SET last_event_seq = last_event_seq + 1
			 WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL
			RETURNING last_event_seq`,
			tenantID, roomID).Scan(&seq)
		if err != nil {
			return storageErr("advance event seq", err)
		}
		payload := ev.Payload
		if len(payload) == 0 {
			payload = []byte(`{}`)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO events (tenant_id, task_id, seq, event_uid, type, source, payload)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			tenantID, roomID, seq, ev.EventUID, ev.Type, ev.Source, payload); err != nil {
			return childWriteErr("insert room.failed", err)
		}
		wrote = true
		return nil
	})
	return wrote, err
}

func (s *Store) ListTenantIDs(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT id FROM tenants ORDER BY id`)
	if err != nil {
		return nil, storageErr("list tenants", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, storageErr("scan tenant", err)
		}
		ids = append(ids, id)
	}
	if ids == nil {
		ids = []string{}
	}
	return ids, rows.Err()
}

func (s *Store) ListReconcileRows(ctx context.Context, tenantID string) ([]store.ReconcileRow, error) {
	out := []store.ReconcileRow{}
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT a.id, a.task_id, COALESCE(r.session_id, ''), COALESCE(a.approval_request_id, ''),
			       COALESCE(a.call_id, ''), a.tool_name, a.reason, a.status, a.decision,
			       COALESCE(a.delivery_state, ''), a.delivery_attempt, a.result_attempt,
			       a.created_at, a.decided_at, COALESCE(a.delivery_updated_at, a.created_at),
			       a.result_body, r.state
			  FROM approvals a
			  JOIN rooms r ON r.id = a.task_id AND r.tenant_id = a.tenant_id
			 WHERE a.tenant_id = $1
			   AND r.deleted_at IS NULL
			   AND (
			        (a.delivery_state = 'delivered' AND a.result_attempt IS NULL)
			     OR a.delivery_state IN ('in_flight', 'unknown', 'not_delivered')
			     OR (a.delivery_state = 'unresolved' AND a.decided_at > now() - interval '24 hours')
			   )`,
			tenantID)
		if err != nil {
			return storageErr("list reconcile rows", err)
		}
		defer rows.Close()
		for rows.Next() {
			var row store.ReconcileRow
			a := &row.Approval
			if err := rows.Scan(&a.ID, &a.TaskID, &a.SessionID, &a.ApprovalRequestID,
				&a.CallID, &a.ToolName, &a.Reason, &a.Status, &a.Decision,
				&a.DeliveryState, &a.DeliveryAttempt, &a.ResultAttempt,
				&a.CreatedAt, &a.DecidedAt, &row.DeliveryUpdatedAt,
				&row.ResultBody, &row.RoomState); err != nil {
				return storageErr("scan reconcile row", err)
			}
			out = append(out, row)
		}
		return rows.Err()
	})
	return out, err
}

func (s *Store) SaveResultBody(ctx context.Context, tenantID, approvalID string, attempt int, body json.RawMessage) (bool, error) {
	ok := false
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE approvals
			   SET result_body = $4
			 WHERE tenant_id = $1 AND id = $2
			   AND delivery_state = 'delivered'
			   AND delivery_attempt = $3
			   AND result_attempt IS NULL
			   AND result_body IS NULL`,
			tenantID, approvalID, attempt, body)
		if err != nil {
			return raiseOr("save result body", err)
		}
		ok = tag.RowsAffected() == 1
		return nil
	})
	return ok, err
}

func (s *Store) LoadResultBody(ctx context.Context, tenantID, approvalID string, attempt int) (store.DecideResultWrite, bool, error) {
	var raw []byte
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			SELECT result_body
			  FROM approvals
			 WHERE tenant_id = $1 AND id = $2
			   AND delivery_state = 'delivered'
			   AND delivery_attempt = $3
			   AND result_body IS NOT NULL`,
			tenantID, approvalID, attempt).Scan(&raw)
		if errors.Is(err, pgx.ErrNoRows) {
			raw = nil
			return nil
		}
		return err
	})
	if err != nil {
		return store.DecideResultWrite{}, false, storageErr("load result body", err)
	}
	if raw == nil {
		return store.DecideResultWrite{}, false, nil
	}
	var w store.DecideResultWrite
	if err := json.Unmarshal(raw, &w); err != nil {
		return store.DecideResultWrite{}, false, storageErr("decode result body", err)
	}
	return w, true, nil
}

func (s *Store) TransitionAt(ctx context.Context, tenantID, approvalID string, attempt int, updatedAt time.Time) (bool, error) {
	if err := noteDeliveryFault("t3"); err != nil {
		return false, err
	}
	ok := false
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE approvals
			   SET delivery_state = 'unknown'
			 WHERE tenant_id = $1 AND id = $2
			   AND delivery_state = 'in_flight'
			   AND delivery_attempt = $3
			   AND delivery_updated_at = $4`,
			tenantID, approvalID, attempt, updatedAt)
		if err != nil {
			return raiseOr("stall in flight", err)
		}
		ok = tag.RowsAffected() == 1
		return nil
	})
	return ok, err
}

func (s *Store) ClaimRetry(ctx context.Context, tenantID, approvalID string, attempt int) (int, bool, error) {
	next := 0
	ok := false
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			UPDATE approvals
			   SET delivery_state = 'in_flight',
			       delivery_attempt = delivery_attempt + 1
			 WHERE tenant_id = $1 AND id = $2
			   AND delivery_state = 'unknown'
			   AND delivery_attempt = $3
			RETURNING delivery_attempt`,
			tenantID, approvalID, attempt).Scan(&next)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return raiseOr("claim retry", err)
		}
		ok = true
		return nil
	})
	return next, ok, err
}

func (s *Store) ReopenNotDelivered(ctx context.Context, tenantID, approvalID string, attempt int) (bool, error) {
	ok := false
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE approvals
			   SET status = 'pending',
			       decision = '',
			       decided_at = NULL,
			       delivery_state = NULL
			 WHERE tenant_id = $1 AND id = $2
			   AND status = 'decided'
			   AND delivery_state = 'not_delivered'
			   AND delivery_attempt = $3`,
			tenantID, approvalID, attempt)
		if err != nil {
			return raiseOr("reopen not delivered", err)
		}
		ok = tag.RowsAffected() == 1
		return nil
	})
	return ok, err
}

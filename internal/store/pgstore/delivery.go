package pgstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/store"
)

const sqlstateRaiseException = "P0001"

func raiseOr(op string, err error) error {
	if code, _ := pgCode(err); code == sqlstateRaiseException {
		return store.ErrApprovalNotPending
	}
	return childWriteErr(op, err)
}

func (s *Store) ClaimDecision(ctx context.Context, tenantID, approvalID, decision string) (int, error) {
	var attempt int
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			UPDATE approvals
			   SET status = 'decided',
			       decision = $3,
			       decided_at = now(),
			       delivery_state = 'in_flight',
			       delivery_attempt = delivery_attempt + 1,
			       delivery_updated_at = now()
			 WHERE tenant_id = $1 AND id = $2
			   AND status = 'pending' AND delivery_state IS NULL
			RETURNING delivery_attempt`,
			tenantID, approvalID, decision).Scan(&attempt)
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrApprovalNotPending
		}
		if err != nil {
			return raiseOr("claim decision", err)
		}
		return nil
	})
	return attempt, err
}

func (s *Store) SetDeliveryState(ctx context.Context, tenantID, approvalID, from, to string, attempt int) (bool, error) {
	ok := false
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE approvals
			   SET delivery_state = $4,
			       delivery_updated_at = now()
			 WHERE tenant_id = $1 AND id = $2
			   AND delivery_state = $3
			   AND delivery_attempt = $5`,
			tenantID, approvalID, from, to, attempt)
		if err != nil {
			return raiseOr("set delivery state", err)
		}
		ok = tag.RowsAffected() == 1
		return nil
	})
	return ok, err
}

func (s *Store) ReopenUndelivered(ctx context.Context, tenantID, approvalID string, attempt int) (bool, error) {
	ok := false
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE approvals
			   SET delivery_state = 'not_delivered',
			       delivery_updated_at = now()
			 WHERE tenant_id = $1 AND id = $2
			   AND delivery_state = 'in_flight'
			   AND delivery_attempt = $3`,
			tenantID, approvalID, attempt)
		if err != nil {
			return raiseOr("mark not delivered", err)
		}
		if tag.RowsAffected() != 1 {
			return nil
		}
		tag, err = tx.Exec(ctx, `
			UPDATE approvals
			   SET status = 'pending',
			       decision = '',
			       decided_at = NULL,
			       delivery_state = NULL,
			       delivery_updated_at = now()
			 WHERE tenant_id = $1 AND id = $2
			   AND status = 'decided'
			   AND delivery_state = 'not_delivered'
			   AND delivery_attempt = $3`,
			tenantID, approvalID, attempt)
		if err != nil {
			return raiseOr("reopen approval", err)
		}
		ok = tag.RowsAffected() == 1
		return nil
	})
	return ok, err
}

func (s *Store) ApplyDecideResult(ctx context.Context, tenantID string, w store.DecideResultWrite) (bool, error) {
	wrote := false
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE approvals
			   SET result_attempt = delivery_attempt
			 WHERE tenant_id = $1 AND id = $2
			   AND delivery_state = 'delivered'
			   AND delivery_attempt = $3
			   AND result_attempt IS NULL`,
			tenantID, w.ApprovalID, w.Attempt)
		if err != nil {
			return raiseOr("record decide result", err)
		}
		if tag.RowsAffected() != 1 {
			return nil
		}
		wrote = true
		for i, text := range w.Texts {
			if text == "" {
				continue
			}
			id := fmt.Sprintf("msg_%s_%d_%d", w.ApprovalID, w.Attempt, i)
			if _, err := tx.Exec(ctx, `
				INSERT INTO messages (id, tenant_id, task_id, role, type, text)
				VALUES ($1, $2, $3, 'assistant', '', $4)`,
				id, tenantID, w.RoomID, text); err != nil {
				return childWriteErr("insert resumed message", err)
			}
		}
		if w.Next != nil {
			next := w.Next
			if _, err := tx.Exec(ctx, `
				INSERT INTO approvals (id, tenant_id, task_id, approval_request_id, call_id,
				                       tool_name, reason, status, decision, created_at)
				VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), $6, $7, $8, $9, COALESCE($10, now()))`,
				next.ID, tenantID, next.TaskID, next.ApprovalRequestID, next.CallID,
				next.ToolName, next.Reason, next.Status, next.Decision, nullTime(next.CreatedAt)); err != nil {
				return childWriteErr("insert next approval", err)
			}
		}
		if w.UpdateRoom {
			tag, err := tx.Exec(ctx, `
				UPDATE rooms
				   SET state = $3, updated_at = now()
				 WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL`,
				tenantID, w.RoomID, w.RoomState)
			if err != nil {
				return storageErr("update room after decide", err)
			}
			if tag.RowsAffected() != 1 {
				return store.ErrNotFound
			}
		}
		return nil
	})
	return wrote, err
}

func (s *Store) ListUnwrittenDeliveries(ctx context.Context, tenantID string) ([]store.ApprovalRecord, error) {
	out := []store.ApprovalRecord{}
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT a.id, a.task_id, COALESCE(r.session_id, ''), COALESCE(a.approval_request_id, ''),
			       COALESCE(a.call_id, ''), a.tool_name, a.reason, a.status, a.decision,
			       COALESCE(a.delivery_state, ''), a.delivery_attempt, a.result_attempt,
			       a.created_at, a.decided_at
			  FROM approvals a
			  JOIN rooms r ON r.id = a.task_id AND r.tenant_id = a.tenant_id
			 WHERE a.tenant_id = $1
			   AND a.delivery_state = 'delivered'
			   AND a.result_attempt IS NULL
			   AND r.deleted_at IS NULL`,
			tenantID)
		if err != nil {
			return storageErr("list unwritten deliveries", err)
		}
		defer rows.Close()
		for rows.Next() {
			a, err := scanApproval(rows)
			if err != nil {
				return storageErr("scan unwritten delivery", err)
			}
			out = append(out, a)
		}
		return rows.Err()
	})
	return out, err
}

func (s *Store) FailRoom(ctx context.Context, tenantID, roomID, code, message string) error {
	return s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE rooms
			   SET state = 'failed',
			       failure = jsonb_build_object('code', $3::text, 'message', $4::text),
			       updated_at = now()
			 WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL`,
			tenantID, roomID, code, message)
		if err != nil {
			return storageErr("fail room", err)
		}
		if tag.RowsAffected() != 1 {
			return store.ErrNotFound
		}
		return nil
	})
}

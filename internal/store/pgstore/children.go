package pgstore

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/store"
)

func ownsLiveRoom(ctx context.Context, tx pgx.Tx, tenantID, userID, roomID string) error {
	var ok bool
	err := tx.QueryRow(ctx, `
		SELECT EXISTS (
		  SELECT 1 FROM rooms
		   WHERE tenant_id = $1 AND id = $2 AND created_by = $3 AND deleted_at IS NULL)`,
		tenantID, roomID, userID).Scan(&ok)
	if err != nil {
		return storageErr("check room", err)
	}
	if !ok {
		return store.ErrNotFound
	}
	return nil
}

func (s *Store) AppendMessage(ctx context.Context, tenantID string, m store.MessageRecord) error {
	return s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO messages (id, tenant_id, task_id, role, type, text, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, now()))`,
			m.ID, tenantID, m.TaskID, m.Role, m.Type, m.Text, nullTime(m.CreatedAt))
		if err != nil {
			return childWriteErr("append message", err)
		}
		return nil
	})
}

func (s *Store) ListMessages(ctx context.Context, tenantID, userID, roomID string) ([]store.MessageRecord, error) {
	out := []store.MessageRecord{}
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		if err := ownsLiveRoom(ctx, tx, tenantID, userID, roomID); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT id, task_id, role, type, text, created_at
			  FROM messages
			 WHERE tenant_id = $1 AND task_id = $2
			 ORDER BY created_at, id`,
			tenantID, roomID)
		if err != nil {
			return storageErr("list messages", err)
		}
		defer rows.Close()
		for rows.Next() {
			var m store.MessageRecord
			if err := rows.Scan(&m.ID, &m.TaskID, &m.Role, &m.Type, &m.Text, &m.CreatedAt); err != nil {
				return storageErr("scan message", err)
			}
			out = append(out, m)
		}
		if err := rows.Err(); err != nil {
			return storageErr("list messages", err)
		}
		return nil
	})
	return out, err
}

func (s *Store) CreateApproval(ctx context.Context, tenantID string, a store.ApprovalRecord) error {
	return s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO approvals (id, tenant_id, task_id, approval_request_id, call_id,
			                       tool_name, reason, status, decision, created_at)
			VALUES ($1, $2, $3, NULLIF($4, ''), NULLIF($5, ''), $6, $7, $8, $9, COALESCE($10, now()))`,
			a.ID, tenantID, a.TaskID, a.ApprovalRequestID, a.CallID,
			a.ToolName, a.Reason, a.Status, a.Decision, nullTime(a.CreatedAt))
		if err != nil {
			return childWriteErr("create approval", err)
		}
		return nil
	})
}

const approvalSelect = `
	SELECT a.id, a.task_id, r.session_id, COALESCE(a.approval_request_id, ''), COALESCE(a.call_id, ''),
	       a.tool_name, a.reason, a.status, a.decision, a.created_at, a.decided_at
	  FROM approvals a
	  JOIN rooms r ON r.id = a.task_id
	 WHERE a.tenant_id = $1 AND r.tenant_id = $1 AND r.created_by = $2 AND r.deleted_at IS NULL`

func scanApproval(row pgx.Row) (store.ApprovalRecord, error) {
	var a store.ApprovalRecord
	err := row.Scan(&a.ID, &a.TaskID, &a.SessionID, &a.ApprovalRequestID, &a.CallID,
		&a.ToolName, &a.Reason, &a.Status, &a.Decision, &a.CreatedAt, &a.DecidedAt)
	return a, err
}

func (s *Store) GetApproval(ctx context.Context, tenantID, userID, approvalID string) (store.ApprovalRecord, error) {
	var out store.ApprovalRecord
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		a, err := scanApproval(tx.QueryRow(ctx, approvalSelect+` AND a.id = $3`, tenantID, userID, approvalID))
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrNotFound
		}
		if err != nil {
			return storageErr("get approval", err)
		}
		out = a
		return nil
	})
	return out, err
}

func (s *Store) ListApprovals(ctx context.Context, tenantID, userID string) ([]store.ApprovalRecord, error) {
	out := []store.ApprovalRecord{}
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, approvalSelect+` ORDER BY a.created_at`, tenantID, userID)
		if err != nil {
			return storageErr("list approvals", err)
		}
		defer rows.Close()
		for rows.Next() {
			a, err := scanApproval(rows)
			if err != nil {
				return storageErr("scan approval", err)
			}
			out = append(out, a)
		}
		if err := rows.Err(); err != nil {
			return storageErr("list approvals", err)
		}
		return nil
	})
	return out, err
}

func (s *Store) DecideApproval(ctx context.Context, tenantID, approvalID, status, decision string) error {
	return s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE approvals
			   SET status = $3, decision = $4, decided_at = now()
			 WHERE tenant_id = $1 AND id = $2`,
			tenantID, approvalID, status, decision)
		if err != nil {
			return childWriteErr("decide approval", err)
		}
		if tag.RowsAffected() == 0 {
			return store.ErrNotFound
		}
		return nil
	})
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

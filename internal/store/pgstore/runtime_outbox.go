package pgstore

import (
	"context"

	"github.com/mindreon/orbit-control/internal/task"
)

// ListenRuntimeOutbox wakes a projector as soon as a worker commits an
// outbox row. The caller still polls because PostgreSQL notifications are
// intentionally lossy and are only a latency optimization.
func (s *Store) ListenRuntimeOutbox(ctx context.Context, tenantID string) (<-chan struct{}, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, storageErr("acquire outbox listener", err)
	}
	if _, err := conn.Exec(ctx, "LISTEN task_events"); err != nil {
		conn.Release()
		return nil, storageErr("listen task events", err)
	}
	if _, err := conn.Exec(ctx, "SELECT set_config('app.tenant_id', $1, true)", tenantID); err != nil {
		conn.Release()
		return nil, storageErr("scope task event listener", err)
	}
	wake := make(chan struct{}, 1)
	go func() {
		defer conn.Release()
		defer close(wake)
		for {
			if _, err := conn.Conn().WaitForNotification(ctx); err != nil {
				return
			}
			select {
			case wake <- struct{}{}:
			default:
			}
		}
	}()
	return wake, nil
}

// projectorLeaderKey identifies the projector lock; it is held on one dedicated connection for as long as the replica
// leads, so it disappears with the connection.
const projectorLeaderKey = "orbit.runtime.projector.leader"

func (s *Store) AcquireProjectorLeadership(ctx context.Context) (func(), bool, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, false, storageErr("acquire projector leadership", err)
	}
	var held bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtext($1))", projectorLeaderKey).Scan(&held); err != nil || !held {
		conn.Release()
		if err != nil {
			return nil, false, storageErr("projector leadership", err)
		}
		return nil, false, nil
	}
	return func() {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock(hashtext($1))", projectorLeaderKey)
		conn.Release()
	}, true, nil
}

func (s *Store) ClaimRuntimeOutbox(ctx context.Context, limit int) ([]task.OutboxRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, storageErr("begin projector batch", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext('orbit.runtime.projector'))"); err != nil {
		return nil, storageErr("projector lock", err)
	}
	rows, err := tx.Query(ctx, `
		SELECT id, tenant_id, task_id, event_id, body
		  FROM runtime_outbox
		 WHERE projected_at IS NULL
		 ORDER BY id
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, storageErr("claim runtime outbox", err)
	}
	defer rows.Close()
	items := []task.OutboxRecord{}
	for rows.Next() {
		var item task.OutboxRecord
		if err := rows.Scan(&item.ID, &item.TenantID, &item.TaskID, &item.EventID, &item.Body); err != nil {
			return nil, storageErr("scan runtime outbox", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, storageErr("read runtime outbox", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, storageErr("commit projector claim", err)
	}
	return items, nil
}

func (s *Store) MarkRuntimeOutboxProjected(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `UPDATE runtime_outbox SET projected_at = now() WHERE id = ANY($1)`, ids)
	if err != nil {
		return storageErr("mark runtime outbox", err)
	}
	return nil
}

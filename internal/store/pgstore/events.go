package pgstore

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/store"
)

func (s *Store) AppendEvent(ctx context.Context, tenantID string, ev store.EventRecord, allowFinished bool) (int64, error) {
	var seq int64
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			UPDATE rooms
			   SET last_event_seq = last_event_seq + 1
			 WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL
			   AND ($3::bool OR state NOT IN ('closed', 'failed'))
			RETURNING last_event_seq`,
			tenantID, ev.TaskID, allowFinished).Scan(&seq)
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrNotFound
		}
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
			tenantID, ev.TaskID, seq, ev.EventUID, ev.Type, ev.Source, payload); err != nil {
			return childWriteErr("insert event", err)
		}
		return nil
	})
	return seq, err
}

func (s *Store) RoomEventHead(ctx context.Context, tenantID, roomID string) (int64, error) {
	var head int64
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			SELECT last_event_seq
			  FROM rooms
			 WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL`,
			tenantID, roomID).Scan(&head)
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrNotFound
		}
		if err != nil {
			return storageErr("event head", err)
		}
		return nil
	})
	return head, err
}

func (s *Store) EventsAfter(ctx context.Context, tenantID, roomID string, after int64) ([]store.EventRecord, int64, bool, error) {
	var (
		rows     []store.EventRecord
		head     int64
		cursorOK bool
	)
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		err := tx.QueryRow(ctx, `
			SELECT last_event_seq
			  FROM rooms
			 WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL`,
			tenantID, roomID).Scan(&head)
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrNotFound
		}
		if err != nil {
			return storageErr("event head", err)
		}
		switch {
		case after == 0:
			cursorOK = true
		case after > head:
			cursorOK = false
		default:
			if err := tx.QueryRow(ctx, `
				SELECT EXISTS (
				  SELECT 1 FROM events
				   WHERE tenant_id = $1 AND task_id = $2 AND seq = $3)`,
				tenantID, roomID, after).Scan(&cursorOK); err != nil {
				return storageErr("event cursor", err)
			}
		}
		if !cursorOK {
			return nil
		}
		q, err := tx.Query(ctx, `
			SELECT seq, event_uid, task_id, type, source, payload, created_at
			  FROM events
			 WHERE tenant_id = $1 AND task_id = $2 AND seq > $3
			 ORDER BY seq`,
			tenantID, roomID, after)
		if err != nil {
			return storageErr("list events", err)
		}
		defer q.Close()
		for q.Next() {
			var ev store.EventRecord
			if err := q.Scan(&ev.Seq, &ev.EventUID, &ev.TaskID, &ev.Type, &ev.Source, &ev.Payload, &ev.CreatedAt); err != nil {
				return storageErr("scan event", err)
			}
			rows = append(rows, ev)
		}
		return q.Err()
	})
	if err != nil {
		return nil, head, false, err
	}
	if rows == nil {
		rows = []store.EventRecord{}
	}
	return rows, head, cursorOK, nil
}

// FindRoomTenant scans tenant ids (tenants has no RLS) and then reads the
// room inside that tenant's transaction. A forged tenant on the request is
// never consulted.
func (s *Store) FindRoomTenant(ctx context.Context, roomID string) (string, error) {
	q, err := s.pool.Query(ctx, `SELECT id FROM tenants`)
	if err != nil {
		return "", storageErr("list tenants", err)
	}
	var ids []string
	for q.Next() {
		var id string
		if err := q.Scan(&id); err != nil {
			q.Close()
			return "", storageErr("scan tenant", err)
		}
		ids = append(ids, id)
	}
	if err := q.Err(); err != nil {
		q.Close()
		return "", storageErr("list tenants", err)
	}
	q.Close()
	for _, tenantID := range ids {
		var found string
		err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
			err := tx.QueryRow(ctx, `
				SELECT tenant_id FROM rooms
				 WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL`,
				tenantID, roomID).Scan(&found)
			if errors.Is(err, pgx.ErrNoRows) {
				return store.ErrNotFound
			}
			if err != nil {
				return storageErr("find room tenant", err)
			}
			return nil
		})
		if err == nil && found != "" {
			return found, nil
		}
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return "", err
		}
	}
	return "", store.ErrNotFound
}

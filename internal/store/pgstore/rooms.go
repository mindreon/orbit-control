package pgstore

import (
	"bytes"
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/store"
)

const roomColumns = `id, tenant_id, created_by, kind, title, state, permission_preset,
	runtime, session_id, persona_id, created_at, updated_at`

func scanRoom(row pgx.Row) (store.RoomRecord, error) {
	var r store.RoomRecord
	err := row.Scan(&r.ID, &r.TenantID, &r.CreatedBy, &r.Kind, &r.Title, &r.State,
		&r.PermissionPreset, &r.Runtime, &r.SessionID, &r.PersonaID, &r.CreatedAt, &r.UpdatedAt)
	return r, err
}

func getOwnedRoom(ctx context.Context, tx pgx.Tx, tenantID, userID, roomID string) (store.RoomRecord, error) {
	r, err := scanRoom(tx.QueryRow(ctx, `
		SELECT `+roomColumns+`
		  FROM rooms
		 WHERE tenant_id = $1 AND id = $2 AND created_by = $3 AND deleted_at IS NULL`,
		tenantID, roomID, userID))
	if errors.Is(err, pgx.ErrNoRows) {
		return store.RoomRecord{}, store.ErrNotFound
	}
	if err != nil {
		return store.RoomRecord{}, storageErr("get room", err)
	}
	return r, nil
}

var errIdempotencyConflict = errors.New("idempotency key primary key conflict")

type idemLookup struct {
	requestHash []byte
	taskID      string
	live        bool
}

func lookupIdempotency(ctx context.Context, tx pgx.Tx, tenantID string, idem *store.IdempotencyRecord) (idemLookup, bool, error) {
	var l idemLookup
	err := tx.QueryRow(ctx, `
		SELECT request_hash, task_id, expires_at > now()
		  FROM idempotency_keys
		 WHERE tenant_id = $1 AND created_by = $2 AND key_hash = $3`,
		tenantID, idem.CreatedBy, idem.KeyHash).Scan(&l.requestHash, &l.taskID, &l.live)
	if errors.Is(err, pgx.ErrNoRows) {
		return idemLookup{}, false, nil
	}
	if err != nil {
		return idemLookup{}, false, storageErr("lookup idempotency key", err)
	}
	return l, true, nil
}

func replayFrom(ctx context.Context, tx pgx.Tx, tenantID string, idem *store.IdempotencyRecord, l idemLookup) (store.RoomRecord, bool, error) {
	if !bytes.Equal(l.requestHash, idem.RequestHash) {
		return store.RoomRecord{}, false, store.ErrIdempotencyKeyReused
	}
	r, err := getOwnedRoom(ctx, tx, tenantID, idem.CreatedBy, l.taskID)
	if err != nil {
		return store.RoomRecord{}, false, err
	}
	return r, true, nil
}

func (s *Store) CreateRoom(ctx context.Context, tenantID string, room store.RoomRecord, idem *store.IdempotencyRecord) (store.RoomRecord, bool, error) {
	out, replayed, err := s.createRoomOnce(ctx, tenantID, room, idem)
	if !errors.Is(err, errIdempotencyConflict) {
		return out, replayed, err
	}
	// The INSERT hit the primary key. Either a concurrent request with the
	// same key committed first (visible now → replay), or the key belongs to
	// a soft-deleted room that RLS hides (→ 404, never 500, never a new
	// task; §18.7a M1-1). The failed transaction already rolled back the room.
	var res store.RoomRecord
	err = s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		l, found, err := lookupIdempotency(ctx, tx, tenantID, idem)
		if err != nil {
			return err
		}
		if !found {
			return store.ErrNotFound
		}
		res, replayed, err = replayFrom(ctx, tx, tenantID, idem, l)
		return err
	})
	if err != nil {
		return store.RoomRecord{}, false, err
	}
	return res, replayed, nil
}

func (s *Store) createRoomOnce(ctx context.Context, tenantID string, room store.RoomRecord, idem *store.IdempotencyRecord) (store.RoomRecord, bool, error) {
	var out store.RoomRecord
	replayed := false
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		if idem != nil {
			l, found, err := lookupIdempotency(ctx, tx, tenantID, idem)
			if err != nil {
				return err
			}
			if found && l.live {
				out, replayed, err = replayFrom(ctx, tx, tenantID, idem, l)
				return err
			}
			if found {
				if _, err := tx.Exec(ctx, `
					DELETE FROM idempotency_keys
					 WHERE tenant_id = $1 AND created_by = $2 AND key_hash = $3`,
					tenantID, idem.CreatedBy, idem.KeyHash); err != nil {
					return storageErr("expire idempotency key", err)
				}
			}
		}
		runtime := room.Runtime
		if len(runtime) == 0 {
			runtime = []byte(`{}`)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO rooms (id, tenant_id, created_by, kind, title, state, permission_preset,
			                   runtime, session_id, persona_id, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $11)`,
			room.ID, tenantID, room.CreatedBy, room.Kind, room.Title, room.State,
			room.PermissionPreset, runtime, room.SessionID, room.PersonaID, room.CreatedAt); err != nil {
			return storageErr("insert room", err)
		}
		if idem != nil {
			_, err := tx.Exec(ctx, `
				INSERT INTO idempotency_keys (key_hash, tenant_id, created_by, request_hash, task_id, expires_at)
				VALUES ($1, $2, $3, $4, $5, $6)`,
				idem.KeyHash, tenantID, idem.CreatedBy, idem.RequestHash, room.ID, idem.ExpiresAt)
			if code, constraint := pgCode(err); code == sqlstateUniqueViolation && constraint == "idempotency_keys_pkey" {
				return errIdempotencyConflict
			}
			if err != nil {
				return storageErr("insert idempotency key", err)
			}
		}
		var err error
		out, err = getOwnedRoom(ctx, tx, tenantID, room.CreatedBy, room.ID)
		return err
	})
	if err != nil {
		return store.RoomRecord{}, false, err
	}
	return out, replayed, nil
}

func (s *Store) GetRoom(ctx context.Context, tenantID, userID, roomID string) (store.RoomRecord, error) {
	var out store.RoomRecord
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		var err error
		out, err = getOwnedRoom(ctx, tx, tenantID, userID, roomID)
		return err
	})
	return out, err
}

func (s *Store) GetRoomForWorker(ctx context.Context, tenantID, roomID string) (store.RoomRecord, error) {
	var out store.RoomRecord
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		r, err := scanRoom(tx.QueryRow(ctx, `
			SELECT `+roomColumns+`
			  FROM rooms
			 WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL`,
			tenantID, roomID))
		if errors.Is(err, pgx.ErrNoRows) {
			return store.ErrNotFound
		}
		if err != nil {
			return storageErr("get room for worker", err)
		}
		out = r
		return nil
	})
	return out, err
}

func (s *Store) ListRooms(ctx context.Context, tenantID, userID string) ([]store.RoomRecord, error) {
	out := []store.RoomRecord{}
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT `+roomColumns+`
			  FROM rooms
			 WHERE tenant_id = $1 AND created_by = $2 AND deleted_at IS NULL
			 ORDER BY created_at DESC`,
			tenantID, userID)
		if err != nil {
			return storageErr("list rooms", err)
		}
		defer rows.Close()
		for rows.Next() {
			r, err := scanRoom(rows)
			if err != nil {
				return storageErr("scan room", err)
			}
			out = append(out, r)
		}
		if err := rows.Err(); err != nil {
			return storageErr("list rooms", err)
		}
		return nil
	})
	return out, err
}

func (s *Store) UpdateRoomState(ctx context.Context, tenantID, roomID, state, sessionID string) error {
	return s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE rooms
			   SET state = $3, session_id = COALESCE(NULLIF($4, ''), session_id), updated_at = now()
			 WHERE tenant_id = $1 AND id = $2 AND deleted_at IS NULL`,
			tenantID, roomID, state, sessionID)
		if err != nil {
			return storageErr("update room state", err)
		}
		if tag.RowsAffected() == 0 {
			return store.ErrNotFound
		}
		return nil
	})
}

// SoftDeleteRoom goes only through the SECURITY DEFINER function; a plain
// UPDATE ... SET deleted_at from orbit_app fails the rooms SELECT policy on
// the new row (§18.5).
func (s *Store) SoftDeleteRoom(ctx context.Context, tenantID, userID, roomID string) (int64, error) {
	var n int32
	err := s.inTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `SELECT orbit_soft_delete_room($1, $2)`, roomID, userID).Scan(&n); err != nil {
			return storageErr("soft delete room", err)
		}
		return nil
	})
	if errors.Is(err, store.ErrNotFound) {
		return 0, nil
	}
	return int64(n), err
}

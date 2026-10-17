package db

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mindreon/orbit-control/internal/platform/ids"
)

// Session pins a connection while retaining transaction-local tenant isolation.
// It is used only by schedule synchronization; no business transaction spans RPCs.
type Session struct{ conn *pgxpool.Conn }

func (d *DB) WithSessionLock(ctx context.Context, key string, seed int, fn func(*Session) error) (bool, error) {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Release()
	var acquired bool
	if err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1,$2))`, key, seed).Scan(&acquired); err != nil {
		return false, err
	}
	if !acquired {
		return false, nil
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		var unlocked bool
		if err := conn.QueryRow(cleanup, `SELECT pg_advisory_unlock(hashtextextended($1,$2))`, key,
			seed).Scan(&unlocked); err != nil || !unlocked {
			// A connection with an uncertain lock must never return to the pool.
			_ = conn.Conn().Close(cleanup)
		}
	}()
	return true, fn(&Session{conn: conn})
}

func (s *Session) WithTenant(ctx context.Context, tenant string, fn func(pgx.Tx) error) error {
	if !ids.Valid(tenant) {
		return errors.New("invalid tenant id")
	}
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if _, err = tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true)`, tenant); err != nil {
		return err
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

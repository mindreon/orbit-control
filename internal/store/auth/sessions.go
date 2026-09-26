// Package auth is the only access path to the pre-login tables (sessions,
// oidc_login_state), which have no RLS because the tenant is not known yet
// (§18.5). Raw session ids never reach SQL: every method hashes them here and
// only sha256(id) is stored (§18.6). These methods are the S-DB-1 allowlisted
// exceptions to the tenantID rule.
package auth

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mindreon/orbit-control/internal/store"
)

// MinSessionIDLen rejects ids too short to be the ≥128-bit random value
// §17.3 requires.
const MinSessionIDLen = 16

type Session struct {
	UserID    string
	TenantID  string
	ExpiresAt time.Time
}

type Sessions struct {
	pool *pgxpool.Pool
}

func NewSessions(pool *pgxpool.Pool) *Sessions { return &Sessions{pool: pool} }

func hashID(rawID string) ([]byte, error) {
	if len(rawID) < MinSessionIDLen {
		return nil, fmt.Errorf("session id shorter than %d characters", MinSessionIDLen)
	}
	sum := sha256.Sum256([]byte(rawID))
	return sum[:], nil
}

func storageErr(op string, err error) error {
	return fmt.Errorf("%w (%s): %v", store.ErrStorage, op, err)
}

// Create stores a session under sha256(rawID).
func (s *Sessions) Create(ctx context.Context, rawID, userID, tenantID string, expiresAt time.Time) error {
	h, err := hashID(rawID)
	if err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO sessions (id_hash, user_id, tenant_id, expires_at)
		VALUES ($1, $2, $3, $4)`, h, userID, tenantID, expiresAt); err != nil {
		return storageErr("create session", err)
	}
	return nil
}

// Lookup resolves a live session and refreshes last_seen_at.
func (s *Sessions) Lookup(ctx context.Context, rawID string) (Session, error) {
	h, err := hashID(rawID)
	if err != nil {
		return Session{}, store.ErrNotFound
	}
	var out Session
	err = s.pool.QueryRow(ctx, `
		UPDATE sessions SET last_seen_at = now()
		 WHERE id_hash = $1 AND expires_at > now()
		RETURNING user_id, tenant_id, expires_at`, h).Scan(&out.UserID, &out.TenantID, &out.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Session{}, store.ErrNotFound
	}
	if err != nil {
		return Session{}, storageErr("lookup session", err)
	}
	return out, nil
}

// Delete destroys the server session (logout, §17.3).
func (s *Sessions) Delete(ctx context.Context, rawID string) error {
	h, err := hashID(rawID)
	if err != nil {
		return store.ErrNotFound
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM sessions WHERE id_hash = $1`, h); err != nil {
		return storageErr("delete session", err)
	}
	return nil
}

package audit

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/jcs"
)

// Record is the payload canonicalized with RFC 8785 before hashing.
type Record struct {
	ID         string          `json:"id"`
	TenantID   string          `json:"tenant_id"`
	ActorID    string          `json:"actor_id"`
	ActorKind  string          `json:"actor_kind"`
	Action     string          `json:"action"`
	TargetType string          `json:"target_type"`
	TargetID   string          `json:"target_id"`
	Detail     json.RawMessage `json:"detail"`
	IP         string          `json:"ip"`
	UserAgent  string          `json:"user_agent"`
	OccurredAt time.Time       `json:"occurred_at"`
}

func Hash(prev []byte, r Record) ([]byte, error) {
	b, e := jcs.Marshal(r)
	if e != nil {
		return nil, e
	}
	h := sha256.New()
	h.Write(prev)
	h.Write(b)
	return h.Sum(nil), nil
}

func Append(
	ctx context.Context,
	tx pgx.Tx,
	p auth.Principal,
	tenant, action, targetType, target string,
	detail any,
) error {
	if _, e := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", tenant); e != nil {
		return e
	}
	prev := make([]byte, 32)
	var last time.Time
	e := tx.QueryRow(ctx, "SELECT hash,occurred_at FROM audit_log WHERE tenant_id=$1 ORDER BY occurred_at DESC,id DESC LIMIT 1", tenant).
		Scan(&prev, &last)
	if e != nil && !errors.Is(e, pgx.ErrNoRows) {
		return e
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if !now.After(last) {
		now = last.Add(time.Microsecond)
	}
	b, e := json.Marshal(detail)
	if e != nil {
		return e
	}
	// Normalize persisted JSON before computing the JCS hash.
	if e = tx.QueryRow(ctx, "SELECT $1::jsonb", b).Scan(&b); e != nil {
		return e
	}
	meta, _ := ctx.Value(metadataKey{}).(metadata)
	r := Record{
		IP:         meta.IP,
		UserAgent:  meta.UserAgent,
		ID:         ids.New(),
		TenantID:   tenant,
		ActorID:    p.ID,
		ActorKind:  p.Kind,
		Action:     action,
		TargetType: targetType,
		TargetID:   target,
		Detail:     b,
		OccurredAt: now,
	}
	h, e := Hash(prev, r)
	if e != nil {
		return e
	}
	_, e = tx.Exec(
		ctx,
		"INSERT INTO audit_log(id,tenant_id,actor_id,actor_kind,action,target_type,target_id,detail"+
			",ip,user_agent,prev_hash,hash,occurred_at) "+
			"VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
		r.ID,
		tenant,
		p.ID,
		p.Kind,
		action,
		targetType,
		target,
		b,
		r.IP,
		r.UserAgent,
		prev,
		h,
		now,
	)
	return e
}

type Head struct {
	Count int    `json:"count"`
	Hash  string `json:"hash"`
}

func Verify(ctx context.Context, tx pgx.Tx) (Head, error) {
	head := Head{Hash: hex.EncodeToString(make([]byte, 32))}
	rows, e := tx.Query(
		ctx,
		"SELECT id::text,tenant_id::text,actor_id::text,actor_kind,action,target_type,target_id,det"+
			"ail,ip,user_agent,occurred_at,prev_hash,hash FROM audit_log WHERE "+
			"tenant_id=NULLIF(current_setting('app.tenant_id',true),'')::uuid ORDER BY occurred_at,id",
	)
	if e != nil {
		return head, e
	}
	defer rows.Close()
	prev := make([]byte, 32)
	for rows.Next() {
		var r Record
		var ph, h []byte
		if e = rows.Scan(&r.ID,
			&r.TenantID,
			&r.ActorID,
			&r.ActorKind,
			&r.Action,
			&r.TargetType,
			&r.TargetID,
			&r.Detail,
			&r.IP,
			&r.UserAgent,
			&r.OccurredAt,
			&ph,
			&h); e != nil {
			return head, e
		}
		r.OccurredAt = r.OccurredAt.UTC()
		expected, e := Hash(prev, r)
		if e != nil {
			return head, e
		}
		if !bytes.Equal(prev, ph) || !bytes.Equal(expected, h) {
			return head, fmt.Errorf("audit chain broken at %s (hash %s)", r.ID, hex.EncodeToString(h))
		}
		prev = h
		head.Count++
		head.Hash = hex.EncodeToString(h)
	}
	return head, rows.Err()
}

type metadataKey struct{}
type metadata struct{ IP, UserAgent string }

func Context(ctx context.Context, ip, userAgent string) context.Context {
	return context.WithValue(ctx, metadataKey{}, metadata{IP: ip, UserAgent: userAgent})
}

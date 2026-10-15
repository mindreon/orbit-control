package httpx

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"net/http"
)

// IdempotencyStore supports the two transactions around a durable Temporal Update.
// Lookup claims an unfinished key; a retry must match even before a response exists.
type IdempotencyStore struct{ Keys ReplayKeys }

func (s IdempotencyStore) Lookup(ctx context.Context, tx pgx.Tx, tenant, principal string, r *http.Request, body []byte) (*Recorder, error) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		return nil, nil
	}
	if len(key) > 256 {
		return nil, E(422, "INVALID_IDEMPOTENCY_KEY", "Idempotency-Key is too long")
	}
	hash := sha256.Sum256(body)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,2))`, tenant+principal+key); err != nil {
		return nil, err
	}
	var method, path string
	var oldHash, headers, response []byte
	var status, version int
	err := tx.QueryRow(ctx, `SELECT method,path,request_hash,status,headers,response,key_version FROM idempotency_keys WHERE tenant_id=$1 AND principal_id=$2 AND key=$3 AND expires_at>now()`, tenant, principal, key).Scan(&method, &path, &oldHash, &status, &headers, &response, &version)
	if err == nil {
		if !SameRequest(method, path, oldHash, r.Method, replayPath(r), hash[:]) {
			return nil, E(422, "IDEMPOTENCY_MISMATCH", "Idempotency-Key was used with a different request")
		}
		if status == 0 {
			return nil, nil
		}
		g, e := s.Keys.cipher(version)
		if e != nil {
			return nil, e
		}
		plain, e := g.Open(nil, nil, response, []byte(tenant+principal+key))
		if e != nil {
			return nil, e
		}
		rec := NewRecorder()
		rec.Status = status
		rec.Body = plain
		if e = json.Unmarshal(headers, &rec.H); e != nil {
			return nil, e
		}
		return rec, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO idempotency_keys(tenant_id,principal_id,key,method,path,request_hash,status,headers,response,key_version,expires_at) VALUES($1,$2,$3,$4,$5,$6,0,'{}','',$7,now()+interval '24 hours') ON CONFLICT(tenant_id,principal_id,key) DO UPDATE SET method=EXCLUDED.method,path=EXCLUDED.path,request_hash=EXCLUDED.request_hash,status=0,headers='{}',response='',key_version=EXCLUDED.key_version,created_at=now(),expires_at=EXCLUDED.expires_at`, tenant, principal, key, r.Method, replayPath(r), hash[:], s.Keys.Current)
	return nil, err
}
func (s IdempotencyStore) Save(ctx context.Context, tx pgx.Tx, tenant, principal string, r *http.Request, rec *Recorder) error {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		return nil
	}
	g, err := s.Keys.cipher(s.Keys.Current)
	if err != nil {
		return err
	}
	encrypted := g.Seal(nil, nil, rec.Body, []byte(tenant+principal+key))
	headers, err := json.Marshal(rec.H)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE idempotency_keys SET status=$4,headers=$5,response=$6,key_version=$7 WHERE tenant_id=$1 AND principal_id=$2 AND key=$3 AND status=0`, tenant, principal, key, rec.Status, headers, encrypted, s.Keys.Current)
	return err
}

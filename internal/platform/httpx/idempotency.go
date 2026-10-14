package httpx

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"

	"github.com/jackc/pgx/v5"
)

// Idempotent must execute inside the same tenant transaction as the handler.
// Advisory locking prevents concurrent requests from performing duplicate writes.
func Idempotent(ctx context.Context, tx pgx.Tx, tenant, principal string, replayKey []byte, w http.ResponseWriter, r *http.Request, next func(http.ResponseWriter, *http.Request) error) error {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || r.Method == "GET" {
		return next(w, r)
	}
	if len(key) > 256 {
		return E(422, "INVALID_IDEMPOTENCY_KEY", "Idempotency-Key is too long")
	}
	b, e := io.ReadAll(r.Body)
	if e != nil {
		return E(413, "BODY_TOO_LARGE", "Request body exceeds limit")
	}
	r.Body = io.NopCloser(bytes.NewReader(b))
	hash := sha256.Sum256(b)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,2))", tenant+principal+key); e != nil {
		return e
	}
	block, e := aes.NewCipher(replayKey)
	if e != nil {
		return e
	}
	gcm, e := cipher.NewGCMWithRandomNonce(block)
	if e != nil {
		return e
	}
	var method, path string
	var savedHash, body []byte
	var status int
	var headers []byte
	e = tx.QueryRow(ctx, "SELECT method,path,request_hash,status,headers,response FROM idempotency_keys WHERE principal_id=$1 AND key=$2 AND namespace='http' AND expires_at>now()", principal, key).Scan(&method, &path, &savedHash, &status, &headers, &body)
	if e == nil {
		if !SameRequest(method, path, savedHash, r.Method, r.URL.RequestURI(), hash[:]) {
			return E(422, "IDEMPOTENCY_MISMATCH", "Idempotency-Key was used with a different request")
		}
		var h http.Header
		if e = json.Unmarshal(headers, &h); e != nil {
			return e
		}
		for k, v := range h {
			w.Header()[k] = v
		}
		plain, e := gcm.Open(nil, nil, body, []byte(tenant+principal+key))
		if e != nil {
			return e
		}
		w.WriteHeader(status)
		_, e = w.Write(plain)
		return e
	}
	if e != pgx.ErrNoRows {
		return e
	}
	rec := NewRecorder()
	if e = next(rec, r); e != nil {
		return e
	}
	if rec.Status == 0 {
		rec.Status = 200
	}
	body = gcm.Seal(nil, nil, rec.Body, []byte(tenant+principal+key))
	h, e := json.Marshal(rec.H)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `INSERT INTO idempotency_keys(tenant_id,principal_id,key,method,path,request_hash,status,headers,response,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,now()+interval '24 hours') ON CONFLICT(tenant_id,principal_id,namespace,key) DO UPDATE SET method=EXCLUDED.method,path=EXCLUDED.path,request_hash=EXCLUDED.request_hash,status=EXCLUDED.status,headers=EXCLUDED.headers,response=EXCLUDED.response,created_at=now(),expires_at=EXCLUDED.expires_at`, tenant, principal, key, r.Method, r.URL.RequestURI(), hash[:], rec.Status, h, body)
	if e != nil {
		return e
	}
	rec.FlushTo(w)
	return nil
}
func SameRequest(method, path string, hash []byte, newMethod, newPath string, newHash []byte) bool {
	return method == newMethod && path == newPath && bytes.Equal(hash, newHash)
}

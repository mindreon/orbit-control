package httpx

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ReplayKeys are purpose-derived from versioned secrets KEKs, independently of JWT keys.
type ReplayKeys struct {
	Current int
	Keys    map[int][]byte
}

func NewReplayKeys(current int, keks map[int][]byte) (ReplayKeys, error) {
	keys := ReplayKeys{Current: current, Keys: map[int][]byte{}}
	for version, kek := range keks {
		if len(kek) != 32 {
			return keys, fmt.Errorf("invalid secrets KEK")
		}
		key, err := hkdf.Key(sha256.New, kek, nil, "orbit:idempotency:v1", 32)
		if err != nil {
			return keys, err
		}
		keys.Keys[version] = key
	}
	if keys.Keys[current] == nil {
		return keys, fmt.Errorf("missing current secrets KEK")
	}
	return keys, nil
}
func (k ReplayKeys) cipher(version int) (cipher.AEAD, error) {
	key, ok := k.Keys[version]
	if !ok {
		return nil, fmt.Errorf("unknown replay key version %d", version)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCMWithRandomNonce(block)
}

type requestBodyKey struct{}

// WithRequestBody reuses a bounded upload body already read before the transaction.
func WithRequestBody(ctx context.Context, b []byte) context.Context {
	return context.WithValue(ctx, requestBodyKey{}, b)
}

// Idempotent must execute inside the same tenant transaction as the handler.
// Advisory locking prevents concurrent requests from performing duplicate writes.
func Idempotent(ctx context.Context, tx pgx.Tx, tenant, principal string, replayKeys ReplayKeys, w http.ResponseWriter, r *http.Request, next func(http.ResponseWriter, *http.Request) error) error {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || r.Method == "GET" {
		return next(w, r)
	}
	if len(key) > 256 {
		return E(422, "INVALID_IDEMPOTENCY_KEY", "Idempotency-Key is too long")
	}
	b, ok := ctx.Value(requestBodyKey{}).([]byte)
	var e error
	if !ok {
		b, e = io.ReadAll(r.Body)
		if e != nil {
			return E(413, "BODY_TOO_LARGE", "Request body exceeds limit")
		}
	}
	r.Body = io.NopCloser(bytes.NewReader(b))
	hash := sha256.Sum256(b)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,2))", tenant+principal+key); e != nil {
		return e
	}
	var method, path string
	var savedHash, body []byte
	var status, version int
	var headers []byte
	e = tx.QueryRow(ctx, "SELECT method,path,request_hash,status,headers,response,key_version FROM idempotency_keys WHERE principal_id=$1 AND key=$2 AND expires_at>now()", principal, key).Scan(&method, &path, &savedHash, &status, &headers, &body, &version)
	if e == nil {
		if !SameRequest(method, path, savedHash, r.Method, replayPath(r), hash[:]) {
			return E(422, "IDEMPOTENCY_MISMATCH", "Idempotency-Key was used with a different request")
		}
		var h http.Header
		if e = json.Unmarshal(headers, &h); e != nil {
			return e
		}
		for k, v := range h {
			w.Header()[k] = v
		}
		gcm, e := replayKeys.cipher(version)
		if e != nil {
			return e
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
	gcm, e := replayKeys.cipher(replayKeys.Current)
	if e != nil {
		return e
	}
	body = gcm.Seal(nil, nil, rec.Body, []byte(tenant+principal+key))
	h, e := json.Marshal(rec.H)
	if e != nil {
		return e
	}
	_, e = tx.Exec(ctx, `INSERT INTO idempotency_keys(tenant_id,principal_id,key,method,path,request_hash,status,headers,response,key_version,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now()+interval '24 hours') ON CONFLICT(tenant_id,principal_id,key) DO UPDATE SET method=EXCLUDED.method,path=EXCLUDED.path,request_hash=EXCLUDED.request_hash,status=EXCLUDED.status,headers=EXCLUDED.headers,response=EXCLUDED.response,key_version=EXCLUDED.key_version,created_at=now(),expires_at=EXCLUDED.expires_at`, tenant, principal, key, r.Method, replayPath(r), hash[:], rec.Status, h, body, replayKeys.Current)
	if e != nil {
		return e
	}
	rec.FlushTo(w)
	return nil
}
func SameRequest(method, path string, hash []byte, newMethod, newPath string, newHash []byte) bool {
	return method == newMethod && path == newPath && bytes.Equal(hash, newHash)
}

// Invitation tokens are bearer secrets. Persist only a digest of their URI.
func replayPath(r *http.Request) string {
	path := r.URL.RequestURI()
	if strings.HasPrefix(r.URL.Path, "/v1/invitations/") {
		hash := sha256.Sum256([]byte(path))
		return "/v1/invitations/sha256:" + hex.EncodeToString(hash[:])
	}
	return path
}

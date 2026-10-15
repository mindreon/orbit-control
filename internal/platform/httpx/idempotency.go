package httpx

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/hex"
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
func Idempotent(
	ctx context.Context,
	tx pgx.Tx,
	tenant, principal string,
	replayKeys ReplayKeys,
	w http.ResponseWriter,
	r *http.Request,
	next func(http.ResponseWriter, *http.Request) error,
) error {
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
	store := IdempotencyStore{Keys: replayKeys}
	cached, e := store.Lookup(ctx, tx, tenant, principal, r, b)
	if e != nil {
		return e
	}
	if cached != nil {
		cached.FlushTo(w)
		return nil
	}
	rec := NewRecorder()
	if e = next(rec, r); e != nil {
		return e
	}
	if rec.Status == 0 {
		rec.Status = http.StatusOK
	}
	if e = store.Save(ctx, tx, tenant, principal, r, rec); e != nil {
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

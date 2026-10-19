package httpx

import (
	"encoding/base64"
	"net/http"
	"strings"
	"time"

	"github.com/mindreon/orbit-control/internal/platform/ids"
)

// TimePagination uses a self-contained timestamp/UUID keyset rather than looking
// up a mutable row by ID. Callers select the ordering column and direction.
func TimePagination(r *http.Request) (Page, error) {
	clone := r.Clone(r.Context())
	u := *r.URL
	clone.URL = &u
	q := u.Query()
	q.Del("cursor")
	clone.URL.RawQuery = q.Encode()
	p, err := Pagination(clone)
	if err != nil {
		return p, err
	}
	if encoded := r.URL.Query().Get("cursor"); encoded != "" {
		raw, e := base64.RawURLEncoding.DecodeString(encoded)
		if e != nil {
			return p, E(400, "INVALID_CURSOR", "Invalid cursor")
		}
		if _, _, e = TimeKey(string(raw)); e != nil {
			return p, e
		}
		p.Cursor = string(raw)
	}
	return p, nil
}

func TimeKey(cursor string) (time.Time, string, error) {
	if cursor == "" {
		return time.Time{}, "", nil
	}
	stamp, id, ok := strings.Cut(cursor, "|")
	at, err := time.Parse(time.RFC3339Nano, stamp)
	if !ok || err != nil || !ids.Valid(id) {
		return time.Time{}, "", E(400, "INVALID_CURSOR", "Invalid cursor")
	}
	return at, id, nil
}

func TimeCursor(at time.Time, id string) string {
	return Cursor(at.UTC().Format(time.RFC3339Nano) + "|" + id)
}

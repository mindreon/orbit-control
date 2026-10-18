package httpx

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mindreon/orbit-control/internal/platform/ids"
	orbitlog "github.com/mindreon/orbit-control/internal/platform/log"
)

type Error struct {
	Status    int    `json:"-"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
	Details   any    `json:"details"`
}

func (e *Error) Error() string { return e.Message }
func E(status int, code, message string) *Error {
	return &Error{Status: status, Code: code, Message: message, Details: map[string]any{}}
}
func WriteError(w http.ResponseWriter, err error) {
	var e *Error
	if !errors.As(err, &e) {
		if errors.Is(err, pgx.ErrNoRows) {
			e = E(404, "NOT_FOUND", "Resource not found")
		} else {
			var p *pgconn.PgError
			if errors.As(err, &p) && p.Code == "23505" {
				e = E(409, "CONFLICT", "Resource already exists")
			} else if errors.As(err, &p) && (p.Code == "23503" || p.Code == "23514" || p.Code == "22P02") {
				e = E(422, "INVALID_INPUT", "Invalid resource reference or value")
			} else {
				e = E(500, "INTERNAL", "Request failed")
				e.Retryable = true
			}
		}
	}
	JSON(w, e.Status, map[string]any{"error": e})
}
func JSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func Decode(r *http.Request, v any) error {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if e := d.Decode(v); e != nil {
		return E(400, "INVALID_JSON", "Invalid JSON body")
	}
	if d.Decode(&struct{}{}) != io.EOF {
		return E(400, "INVALID_JSON", "Expected one JSON value")
	}
	return nil
}

type Page struct {
	Cursor string
	Limit  int
}

func Pagination(r *http.Request) (Page, error) {
	p := Page{Limit: 50}
	if s := r.URL.Query().Get("limit"); s != "" {
		n, e := strconv.Atoi(s)
		if e != nil || n < 1 || n > 100 {
			return p, E(400, "INVALID_LIMIT", "limit must be between 1 and 100")
		}
		p.Limit = n
	}
	if s := r.URL.Query().Get("cursor"); s != "" {
		b, e := base64.RawURLEncoding.DecodeString(s)
		if e != nil || !ids.Valid(string(b)) {
			return p, E(400, "INVALID_CURSOR", "Invalid cursor")
		}
		p.Cursor = string(b)
	}
	return p, nil
}
func Cursor(id string) string { return base64.RawURLEncoding.EncodeToString([]byte(id)) }

// Recorder buffers responses so a failed database transaction never returns success.
type Recorder struct {
	H      http.Header
	Status int
	Body   []byte
}

func NewRecorder() *Recorder            { return &Recorder{H: make(http.Header)} }
func (r *Recorder) Header() http.Header { return r.H }
func (r *Recorder) WriteHeader(n int) {
	if r.Status == 0 {
		r.Status = n
	}
}
func (r *Recorder) Write(b []byte) (int, error) {
	if r.Status == 0 {
		r.Status = 200
	}
	r.Body = append(r.Body, b...)
	return len(b), nil
}
func (r *Recorder) FlushTo(w http.ResponseWriter) {
	for k, v := range r.H {
		w.Header()[k] = v
	}
	w.WriteHeader(r.Status)
	_, _ = w.Write(r.Body)
}
func Middleware(logger *slog.Logger, publicOrigin string, origins []string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if !ids.Valid(id) {
			id = ids.New()
		}
		w.Header().Set("X-Request-ID", id)
		w.Header().Add("Vary", "Origin")
		started := time.Now()
		rec := NewRecorder()
		defer func() {
			if value := recover(); value != nil {
				headers := rec.H.Clone()
				rec = NewRecorder()
				rec.H = headers
				WriteError(rec, E(500, "INTERNAL", "Request failed"))
				logger.ErrorContext(r.Context(),
					"request panic",
					"request_id",
					id,
					"panic_type",
					fmt.Sprintf("%T", value),
					"panic",
					orbitlog.PanicValue(value),
					"stack",
					string(debug.Stack()),
				)
			}
			if rec.Status == 0 {
				rec.Status = 200
			}
			rec.FlushTo(w)
			logger.InfoContext(r.Context(),
				"http access",
				"request_id",
				id,
				"method",
				r.Method,
				"path",
				logPath(r.URL.Path),
				"status",
				rec.Status,
				"duration_ms",
				time.Since(started).Milliseconds(),
			)
		}()
		if o := r.Header.Get("Origin"); o != "" {
			allowed := o == publicOrigin || slices.Contains(origins, o)
			if !allowed {
				if r.Method != "GET" && r.Method != "HEAD" && r.Method != "OPTIONS" {
					WriteError(rec, E(403, "CORS_DENIED", "Origin is not allowed"))
					return
				}
			} else {
				rec.Header().Set("Access-Control-Allow-Origin", o)
				rec.Header().Set("Access-Control-Expose-Headers", "X-Request-ID")
				if r.Method == "OPTIONS" {
					rec.Header().Set("Access-Control-Max-Age", "600")
					rec.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, POST, PATCH, DELETE, OPTIONS")
					rec.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Orbit-Tenant, X-Request-ID, Idempotency-Key")
					rec.WriteHeader(204)
					return
				}
			}
		}
		bodyLimit := int64(1 << 20)
		if r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/v1/assets/") &&
			strings.HasSuffix(r.URL.Path, "/versions") {
			bodyLimit = 50 << 20
		}
		r.Body = http.MaxBytesReader(rec, r.Body, bodyLimit)
		next.ServeHTTP(rec, r)
	})
}
func Required(s, name string) error {
	if strings.TrimSpace(s) == "" || len(s) > 256 {
		return E(422, "INVALID_INPUT", fmt.Sprintf("%s is required and must be at most 256 bytes", name))
	}
	return nil
}

func logPath(path string) string {
	if strings.HasPrefix(path, "/v1/invitations/") {
		return "/v1/invitations/[REDACTED]:accept"
	}
	return path
}

package streamgw

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json/v2"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
)

func testConfig() Config {
	return Config{MaxConnections: 100, PerTenant: 10, PerSubject: 3, BufferFrames: 4, BufferBytes: 64 << 10,
		Heartbeat: time.Second, WriteTimeout: time.Second, ShutdownGrace: time.Second, MaxAge: time.Hour, InfoCache: time.Second}
}

func testServer(t *testing.T) (*Server, auth.StreamClaims, ed25519.PrivateKey, *bytes.Buffer) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	v, err := auth.NewStreamVerifier(map[string][]byte{"test": pub})
	require.NoError(t, err)
	var logs bytes.Buffer
	s := New(&natsx.Bus{Conn: &nats.Conn{}}, v, testConfig(), slog.New(slog.NewJSONHandler(&logs, nil)))
	s.connected = func() bool { return true }
	c := auth.StreamClaims{TenantID: ids.New(), SessionID: ids.New(), RunID: ids.New(), Scope: "run",
		Issuer: auth.StreamIssuer, Audience: jwt.ClaimStrings{"stream"}, IssuedAt: jwt.NewNumericDate(time.Now()),
		NotBefore: jwt.NewNumericDate(time.Now()), ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute))}
	c.Subject = "run:" + c.RunID
	return s, c, key, &logs
}

func signed(t *testing.T, c auth.StreamClaims, key ed25519.PrivateKey, kid string) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, c)
	token.Header["kid"] = kid
	raw, err := token.SignedString(key)
	require.NoError(t, err)
	return raw
}

func TestAuthorization(t *testing.T) {
	for _, mode := range []string{"valid", "kid", "expired", "nbf", "path", "scope", "header", "bad-header", "signature"} {
		t.Run(mode, func(t *testing.T) {
			s, claims, key, logs := testServer(t)
			path, kid := claims.RunID, "test"
			want := 401
			switch mode {
			case "valid", "header":
				want = 200
			case "kid":
				kid = "wrong"
			case "expired":
				claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Second))
			case "nbf":
				claims.NotBefore = jwt.NewNumericDate(time.Now().Add(time.Second))
			case "path":
				path, want = ids.New(), 403
			case "scope":
				claims.Scope, claims.RunID, claims.Subject = "session", "", "session:"+claims.SessionID
				want = 403
			case "signature":
				_, key, _ = ed25519.GenerateKey(nil)
			}
			token := signed(t, claims, key, kid)
			r := httptest.NewRequest("GET", "/v1/runs/"+path+"/events?token="+token, nil)
			r.SetPathValue("r", path)
			if mode == "header" {
				r.URL.RawQuery = "token=invalid"
				r.Header.Set("Authorization", "Bearer "+token)
			}
			if mode == "bad-header" {
				r.Header.Set("Authorization", token)
			}
			out := httptest.NewRecorder()
			_, ok := s.authorize(out, r)
			require.Equal(t, want == 200, ok)
			require.Equal(t, want, out.Code)
			require.Empty(t, r.URL.Query().Get("token"))
			require.NotContains(t, logs.String(), token)
			require.NotContains(t, out.Body.String(), token)
		})
	}
}

func TestLimits(t *testing.T) {
	for _, mode := range []string{"total", "tenant", "subject"} {
		t.Run(mode, func(t *testing.T) {
			s, _, _, _ := testServer(t)
			s.cfg = testConfig()
			switch mode {
			case "total":
				s.cfg.MaxConnections = 1
			case "tenant":
				s.cfg.PerTenant = 1
			case "subject":
				s.cfg.PerSubject = 1
			}
			release, ok := s.acquire("a", "s", "run")
			require.True(t, ok)
			tenant, subject := "a", "s"
			if mode == "total" {
				tenant, subject = "b", "t"
			} else if mode == "tenant" {
				subject = "t"
			}
			_, ok = s.acquire(tenant, subject, "run")
			require.False(t, ok)
			release()
			release, ok = s.acquire(tenant, subject, "run")
			require.True(t, ok)
			release()
			require.Empty(t, s.limits.tenants)
			require.Empty(t, s.limits.subjects)
		})
	}
	s, c, key, _ := testServer(t)
	s.cfg.PerSubject = 1
	release, ok := s.acquire(c.TenantID, subject(c), c.Scope)
	require.True(t, ok)
	defer release()
	r := httptest.NewRequest("GET", "/v1/runs/"+c.RunID+"/events", nil)
	r.Header.Set("Authorization", "Bearer "+signed(t, c, key, "test"))
	out := httptest.NewRecorder()
	s.Router().ServeHTTP(out, r)
	require.Equal(t, 429, out.Code)
	require.Equal(t, "5", out.Header().Get("Retry-After"))
}

func TestStartPolicy(t *testing.T) {
	for _, value := range []string{"", "0", "1", "42", "-1", "+1", "bad", "18446744073709551615", "18446744073709551616"} {
		t.Run(value, func(t *testing.T) {
			cursor, err := contract.ParseEventCursor(value)
			cfg, cfgErr := consumerConfig("run.t.s.r", cursor)
			if value == "-1" || value == "+1" || value == "bad" || len(value) == 20 {
				require.True(t, err != nil || cfgErr != nil)
				return
			}
			require.NoError(t, err)
			require.NoError(t, cfgErr)
			require.Equal(t, []string{"run.t.s.r"}, cfg.FilterSubjects)
			if cursor == 0 {
				require.Equal(t, jetstream.DeliverAllPolicy, cfg.DeliverPolicy)
			} else {
				require.Equal(t, jetstream.DeliverByStartSequencePolicy, cfg.DeliverPolicy)
				require.Equal(t, cursor+1, cfg.OptStartSeq)
			}
		})
	}
}

type deadlineWriter struct {
	mu       sync.Mutex
	data     bytes.Buffer
	headers  http.Header
	deadline time.Time
	blocked  bool
}

func (w *deadlineWriter) Header() http.Header { return w.headers }
func (w *deadlineWriter) WriteHeader(int)     {}
func (w *deadlineWriter) Flush()              {}
func (w *deadlineWriter) SetWriteDeadline(t time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.deadline = t
	return nil
}
func (w *deadlineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	deadline, blocked := w.deadline, w.blocked
	w.mu.Unlock()
	if blocked {
		<-time.After(time.Until(deadline))
		return 0, &timeoutError{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.data.Write(p)
}

type timeoutError struct{}

func (*timeoutError) Error() string { return "write timeout" }
func (*timeoutError) Timeout() bool { return true }

func TestWriterAndBackpressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _, _, _ := testServer(t)
		q := newFrameQueue(2, 6)
		require.True(t, q.push(t.Context(), frame{seq: 1, kind: "message.final", data: []byte(`"a\nb"`)}))
		ctx, cancel := context.WithCancel(t.Context())
		pushed := false
		go func() { pushed = q.push(ctx, frame{data: []byte("x")}) }()
		synctest.Wait()
		require.False(t, pushed, "byte budget blocks producer")
		cancel()
		synctest.Wait()
		require.False(t, pushed)
		q.release(<-q.frames)
		require.True(t, q.push(t.Context(), frame{seq: 7, kind: "run.completed", data: []byte(`{}`), terminal: true}))
		w := &deadlineWriter{headers: http.Header{}}
		reason, n := s.writeEvents(t.Context(), w, q)
		require.Equal(t, "run_finished", reason)
		require.Equal(t, 1, n)
		expected := `retry: 1000

id: 7
event: run.completed
data: {}

event: end
data: {"reason":"run_finished"}

`
		require.Equal(t, expected, w.data.String())
		w.blocked = true
		reason, _ = s.writeEvents(t.Context(), w, newFrameQueue(1, 4))
		require.Equal(t, "write_timeout", reason)
		require.EqualValues(t, 1, s.metrics.values["streamgw_slow_client_closed_total"])
		encoded := encodeFrame(frame{seq: 1, kind: "message.final", data: []byte(`{"text":"a\nb"}`)})
		require.Equal(t, 4, bytes.Count(encoded, []byte("\n")))
	})
}

func TestHeartbeatAndLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, _, _, _ := testServer(t)
		ctx, cancel := context.WithTimeout(t.Context(), 1500*time.Millisecond)
		defer cancel()
		w := &deadlineWriter{headers: http.Header{}}
		reason, n := s.writeEvents(ctx, w, newFrameQueue(1, 4))
		require.Equal(t, "closed", reason)
		require.Zero(t, n)
		require.Equal(t, "retry: 1000\n\n: ping\n\n", w.data.String())
	})
}

// Verify that future contract data never enters transport diagnostics.
func TestInvalidRequestDoesNotLogQuery(t *testing.T) {
	s, c, key, logs := testServer(t)
	token := signed(t, c, key, "test")
	for _, suffix := range []string{"&types=secret-event-content", "&exclude=bad", "&types=run.started&exclude=run.failed"} {
		r := httptest.NewRequest("GET", "/v1/runs/"+c.RunID+"/events?token="+token+suffix, nil)
		out := httptest.NewRecorder()
		s.Router().ServeHTTP(out, r)
		require.Equal(t, 422, out.Code)
		require.NotContains(t, out.Body.String(), "secret-event-content")
	}
	require.NotContains(t, logs.String(), token)
	require.NotContains(t, logs.String(), "secret-event-content")
}

// Test helpers shared with the real NATS suite.
func eventBytes(t *testing.T, c auth.StreamClaims, n int, kind string) []byte {
	t.Helper()
	data := map[string]any{"status": "running"}
	if kind == "message.delta" || kind == "reasoning.delta" {
		data = map[string]any{"message_id": "msg", "delta": "a\nb"}
	} else if kind == "run.completed" {
		data = map[string]any{"status": "succeeded"}
	} else if kind == "message.final" {
		data = map[string]any{"message_id": "msg", "text": "a\nb"}
	}
	raw, err := json.Marshal(contract.RunEvent{V: 1, Id: fmt.Sprintf("%s:main:0:0:%d", c.RunID, n),
		TenantId: c.TenantID, SessionId: c.SessionID, RunId: c.RunID, Member: "main", Ts: time.Now().UTC(), Type: contract.EventType(kind),
		Data: data})
	require.NoError(t, err)
	return raw
}

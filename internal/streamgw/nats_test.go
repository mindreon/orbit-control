//go:build nats_integration

package streamgw

import (
	"bufio"
	"cmp"
	"context"
	"crypto/ed25519"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
)

type published struct {
	seq  uint64
	id   string
	kind string
}

func realGateway(t *testing.T, maxMsgs int64) (*Server, *natsx.Bus, auth.StreamClaims, func(string, uint64, string) *http.Response) {
	t.Helper()
	url := os.Getenv("ORBIT_TEST_NATS_URL")
	if url == "" {
		t.Skip("ORBIT_TEST_NATS_URL required")
	}
	bus, err := natsx.Connect(url, "orbit-v2-test-gateway")
	require.NoError(t, err)
	t.Cleanup(bus.Close)
	s, c, key, _ := testServer(t)
	s.js, s.connected = bus.JS, bus.Conn.IsConnected
	s.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	s.stream = "TEST_" + ids.New()
	_, err = bus.JS.CreateStream(t.Context(), jetstream.StreamConfig{Name: s.stream, Subjects: []string{"run." + c.TenantID + ".>"},
		Storage: jetstream.MemoryStorage, MaxMsgs: maxMsgs, MaxMsgSize: 64 << 10, Duplicates: 10 * time.Minute})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, bus.JS.DeleteStream(ctx, s.stream))
	})
	s.cfg.InfoCache = time.Nanosecond
	s.cfg.BufferFrames = 4
	s.cfg.PerSubject = 50
	s.cfg.PerTenant = 1000
	s.cfg.MaxConnections = 10000
	h := httptest.NewServer(s.Router())
	t.Cleanup(func() {
		s.cancel()
		h.CloseClientConnections()
		h.Close()
	})
	client := &http.Client{Timeout: 10 * time.Second}
	connect := func(path string, cursor uint64, query string) *http.Response {
		claims := c
		claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(time.Minute))
		if strings.HasPrefix(path, "/v1/sessions/") {
			claims.Scope, claims.RunID, claims.Subject = "session", "", "session:"+c.SessionID
		} else {
			claims.RunID = strings.Split(path, "/")[3]
			claims.Subject = "run:" + claims.RunID
		}
		r, err := http.NewRequestWithContext(t.Context(), "GET", h.URL+path+query, nil)
		require.NoError(t, err)
		r.Header.Set("Authorization", "Bearer "+signed(t, claims, key, "test"))
		r.Header.Set("Last-Event-ID", strconv.FormatUint(cursor, 10))
		response, err := client.Do(r)
		require.NoError(t, err)
		return response
	}
	return s, bus, c, connect
}

func readSSE(t *testing.T, scan *bufio.Scanner) (published, bool) {
	t.Helper()
	var result published
	end := false
	for scan.Scan() {
		line := scan.Text()
		switch {
		case strings.HasPrefix(line, "id: "):
			var err error
			result.seq, err = strconv.ParseUint(strings.TrimPrefix(line, "id: "), 10, 64)
			require.NoError(t, err)
		case strings.HasPrefix(line, "event: "):
			result.kind = strings.TrimPrefix(line, "event: ")
			end = result.kind == "end"
		case strings.HasPrefix(line, "data: ") && !end:
			var event struct {
				ID string `json:"id"`
			}
			require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event))
			result.id = event.ID
		case line == "" && (result.seq > 0 || end):
			return result, end
		}
	}
	require.NoError(t, scan.Err())
	t.Fatal("stream ended before expected frame")
	return result, end
}

func TestReconnectExactlyOnce(t *testing.T) {
	s, bus, c, connect := realGateway(t, -1)
	const count = 10000
	var mu sync.Mutex
	var events []published
	jobs := make(chan int, count)
	errors := make(chan error, count)
	for n := range count {
		jobs <- n
	}
	close(jobs)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for n := range jobs {
				kind := []string{"run.status", "message.delta", "reasoning.delta", "message.final"}[n%4]
				raw := eventBytes(t, c, n+1, kind)
				id := fmt.Sprintf("%s:main:0:0:%d", c.RunID, n+1)
				ack, err := bus.JS.Publish(t.Context(), subject(c), raw, jetstream.WithMsgID(id))
				if err != nil {
					errors <- err
					continue
				}
				if n%10 == 0 {
					dup, err := bus.JS.Publish(t.Context(), subject(c), raw, jetstream.WithMsgID(id))
					if err != nil || dup.Sequence != ack.Sequence || !dup.Duplicate {
						errors <- fmt.Errorf("publish deduplication failed: %v", err)
					}
				}
				mu.Lock()
				events = append(events, published{seq: ack.Sequence, id: id, kind: kind})
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	require.Len(t, events, count)
	slices.SortFunc(events, func(a, b published) int { return cmp.Compare(a.seq, b.seq) })
	for seed := range 100 {
		rng := rand.New(rand.NewPCG(uint64(seed), 17))
		filter := rng.IntN(3)
		query := []string{"", "?types=run.status,message.final", "?exclude=reasoning.delta"}[filter]
		want := slices.DeleteFunc(slices.Clone(events), func(e published) bool {
			return filter == 1 && (e.kind == "reasoning.delta" || e.kind == "message.delta") || filter == 2 && e.kind == "reasoning.delta"
		})
		var cursor uint64
		index := 0
		for index < len(want) {
			response := connect("/v1/runs/"+c.RunID+"/events", cursor, query)
			require.Equal(t, 200, response.StatusCode)
			scan := bufio.NewScanner(response.Body)
			scan.Buffer(make([]byte, 4096), 128<<10)
			batch := min(1+rng.IntN(200), len(want)-index)
			for range batch {
				got, end := readSSE(t, scan)
				require.False(t, end)
				require.Equal(t, want[index], got, "seed=%d index=%d", seed, index)
				require.Greater(t, got.seq, cursor)
				cursor = got.seq
				index++
			}
			require.NoError(t, response.Body.Close())
		}
	}
	s.metrics.mu.Lock()
	require.Zero(t, s.metrics.values["streamgw_order_violation_total"])
	s.metrics.mu.Unlock()
	t.Log("100 seeds × 10,000 source events; concurrent publish, 10% duplicate retries; exact ordered filtered sets verified")
}

func TestSessionOrderAndTerminalResume(t *testing.T) {
	_, bus, c, connect := realGateway(t, -1)
	other := c
	other.RunID = ids.New()
	for n := range 10 {
		claims := c
		if n%2 == 1 {
			claims = other
		}
		require.NoError(t, bus.Publish(t.Context(), subject(claims), ids.New(), eventBytes(t, claims, n+1, "run.status")))
	}
	session := connect("/v1/sessions/"+c.SessionID+"/events", 0, "")
	sessionScan := bufio.NewScanner(session.Body)
	for n := range 10 {
		got, end := readSSE(t, sessionScan)
		require.False(t, end)
		require.EqualValues(t, n+1, got.seq)
	}
	require.NoError(t, session.Body.Close())
	var last uint64
	for n := range 5 {
		response := connect("/v1/runs/"+c.RunID+"/events", last, "")
		require.Equal(t, 200, response.StatusCode, "iteration %d", n)
		frame, end := readSSE(t, bufio.NewScanner(response.Body))
		require.False(t, end)
		require.EqualValues(t, n*2+1, frame.seq)
		last = frame.seq
		require.NoError(t, response.Body.Close())
	}
	require.NoError(t, bus.Publish(t.Context(), subject(c), ids.New(), eventBytes(t, c, 11, "run.completed")))
	response := connect("/v1/runs/"+c.RunID+"/events", last, "")
	scan := bufio.NewScanner(response.Body)
	terminal, end := readSSE(t, scan)
	require.False(t, end)
	require.Equal(t, "run.completed", terminal.kind)
	_, end = readSSE(t, scan)
	require.True(t, end)
	require.NoError(t, response.Body.Close())
	response = connect("/v1/runs/"+c.RunID+"/events", terminal.seq, "")
	defer response.Body.Close()
	_, end = readSSE(t, bufio.NewScanner(response.Body))
	require.True(t, end, "resume after terminal produces no duplicate terminal event, just end")
}

func TestExpiredCursorAndZero(t *testing.T) {
	_, bus, c, connect := realGateway(t, 100)
	for n := range 300 {
		require.NoError(t, bus.Publish(t.Context(), subject(c), ids.New(), eventBytes(t, c, n+1, "run.status")))
	}
	response := connect("/v1/runs/"+c.RunID+"/events", 5, "")
	require.Equal(t, 409, response.StatusCode)
	raw, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Contains(t, string(raw), "EVENT_CURSOR_EXPIRED")
	require.NoError(t, response.Body.Close())
	response = connect("/v1/runs/"+c.RunID+"/events", 0, "")
	defer response.Body.Close()
	require.Equal(t, 200, response.StatusCode)
	first, _ := readSSE(t, bufio.NewScanner(response.Body))
	require.EqualValues(t, 201, first.seq)
}

func TestSlowClientIsolationAndResume(t *testing.T) {
	s, bus, c, connect := realGateway(t, -1)
	s.cfg.WriteTimeout = 50 * time.Millisecond
	slow := connect("/v1/runs/"+c.RunID+"/events", 0, "")
	defer slow.Body.Close()
	const count = 700
	for n := range count {
		raw := eventBytes(t, c, n+1, "message.final")
		raw = []byte(strings.Replace(string(raw), `"a\nb"`, strconv.Quote(strings.Repeat("x", 16000)), 1))
		require.NoError(t, bus.Publish(t.Context(), subject(c), ids.New(), raw))
	}
	other := c
	other.RunID = ids.New()
	require.NoError(t, bus.Publish(t.Context(), subject(other), ids.New(), eventBytes(t, other, 1, "run.status")))
	fast := connect("/v1/runs/"+other.RunID+"/events", 0, "")
	got, _ := readSSE(t, bufio.NewScanner(fast.Body))
	require.EqualValues(t, count+1, got.seq)
	require.NoError(t, fast.Body.Close())
	require.Eventually(t, func() bool {
		s.metrics.mu.Lock()
		defer s.metrics.mu.Unlock()
		return s.metrics.values["streamgw_slow_client_closed_total"] > 0
	}, 5*time.Second, 10*time.Millisecond)
	resumed := connect("/v1/runs/"+c.RunID+"/events", 0, "")
	scan := bufio.NewScanner(resumed.Body)
	scan.Buffer(make([]byte, 4096), 128<<10)
	for n := range count {
		got, end := readSSE(t, scan)
		require.False(t, end)
		require.EqualValues(t, n+1, got.seq)
	}
	require.NoError(t, resumed.Body.Close())
}

func TestGracefulShutdownAndResume(t *testing.T) {
	s, bus, c, connect := realGateway(t, -1)
	s.cfg.ShutdownGrace = 100 * time.Millisecond
	response := connect("/v1/runs/"+c.RunID+"/events", 0, "")
	scan := bufio.NewScanner(response.Body)
	done := make(chan error, 1)
	s.BeginShutdown()
	go func() { done <- s.Shutdown(t.Context()) }()
	out := httptest.NewRecorder()
	s.Router().ServeHTTP(out, httptest.NewRequest("GET", "/healthz", nil))
	require.Equal(t, 503, out.Code)
	rejected := connect("/v1/runs/"+c.RunID+"/events", 0, "")
	require.Equal(t, 503, rejected.StatusCode)
	require.NoError(t, rejected.Body.Close())
	require.NoError(t, bus.Publish(t.Context(), subject(c), ids.New(), eventBytes(t, c, 1, "run.status")))
	first, end := readSSE(t, scan)
	require.False(t, end)
	require.EqualValues(t, 1, first.seq)
	require.NoError(t, <-done)
	require.NoError(t, response.Body.Close())
	require.NoError(t, bus.Publish(t.Context(), subject(c), ids.New(), eventBytes(t, c, 2, "run.status")))
	next := New(bus, s.verifier, testConfig(), s.logger)
	next.stream = s.stream
	h := httptest.NewServer(next.Router())
	defer h.Close()
	defer next.cancel()
	// Mint a fresh token by obtaining the token header used by the authorized test request.
	_, freshClaims, key, _ := testServer(t)
	freshClaims.TenantID, freshClaims.SessionID, freshClaims.RunID = c.TenantID, c.SessionID, c.RunID
	freshClaims.Subject = "run:" + c.RunID
	v, err := auth.NewStreamVerifier(map[string][]byte{"test": key.Public().(ed25519.PublicKey)})
	require.NoError(t, err)
	next.verifier = v
	req, err := http.NewRequestWithContext(t.Context(), "GET", h.URL+"/v1/runs/"+c.RunID+"/events", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+signed(t, freshClaims, key, "test"))
	req.Header.Set("Last-Event-ID", strconv.FormatUint(first.seq, 10))
	resumed, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	require.NoError(t, err)
	defer resumed.Body.Close()
	got, end := readSSE(t, bufio.NewScanner(resumed.Body))
	require.False(t, end)
	require.EqualValues(t, 2, got.seq)
}

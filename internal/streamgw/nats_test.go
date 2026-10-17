//go:build nats_integration

package streamgw

import (
	"bufio"
	"cmp"
	"context"
	"crypto/ed25519"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
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
		Storage: jetstream.MemoryStorage, MaxMsgs: maxMsgs, MaxMsgSize: natsx.MaxMessageSize, Duplicates: 10 * time.Minute})
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
	seedSlots := make(chan struct{}, 8)
	for seed := range 100 {
		t.Run(fmt.Sprintf("seed-%03d", seed), func(t *testing.T) {
			t.Parallel()
			seedSlots <- struct{}{}
			defer func() { <-seedSlots }()
			started := time.Now()
			connections := 0
			var cursor uint64
			index := 0
			defer func() {
				t.Logf("seed=%d published=%d read=%d cursor=%d reconnects=%d elapsed=%s",
					seed, len(events), index, cursor, connections, time.Since(started))
				if t.Failed() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					stream, err := bus.JS.Stream(ctx, s.stream)
					if err == nil {
						info, infoErr := stream.Info(ctx)
						if infoErr == nil {
							t.Logf("stream messages=%d consumers=%d", info.State.Msgs, info.State.Consumers)
						}
						list := stream.ListConsumers(ctx)
						for info := range list.Info() {
							t.Logf("consumer=%s pending=%d waiting=%d delivered_stream=%d delivered_consumer=%d",
								info.Name, info.NumPending, info.NumWaiting, info.Delivered.Stream, info.Delivered.Consumer)
						}
					}
				}
			}()
			rng := rand.New(rand.NewPCG(uint64(seed), 17))
			filter := rng.IntN(3)
			query := []string{"", "?types=run.status,message.final", "?exclude=reasoning.delta"}[filter]
			want := slices.DeleteFunc(slices.Clone(events), func(e published) bool {
				return filter == 1 && (e.kind == "reasoning.delta" || e.kind == "message.delta") || filter == 2 && e.kind == "reasoning.delta"
			})
			for index < len(want) {
				connections++
				response := connect("/v1/runs/"+c.RunID+"/events", cursor, query)
				t.Cleanup(func() { response.Body.Close() })
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
		})
	}
	require.Zero(t, metricCount(t, s, "streamgw_order_violation_total"))
	t.Log("selected reconnection seeds over 10,000 source events; concurrent publish and 10% duplicate retries verified")
}

// A pull request's byte budget also counts the subject and ack reply, so the
// largest message the stream accepts must still arrive at the minimum buffer.
func TestLargestAcceptedMessageReachesMinimumBuffer(t *testing.T) {
	s, bus, c, connect := realGateway(t, -1)
	s.cfg.BufferBytes = 2 * natsx.MaxMessageSize
	padded := func(size int) []byte {
		var event map[string]any
		require.NoError(t, json.Unmarshal(eventBytes(t, c, 1, "run.status"), &event))
		event["future_field"] = ""
		base, err := json.Marshal(event)
		require.NoError(t, err)
		event["future_field"] = strings.Repeat("x", size-len(base))
		raw, err := json.Marshal(event)
		require.NoError(t, err)
		require.Len(t, raw, size)
		return raw
	}
	var largest int
	for size := natsx.MaxMessageSize; size > natsx.MaxMessageSize-1024; size-- {
		if bus.Publish(t.Context(), subject(c), ids.New(), padded(size)) == nil {
			largest = size
			break
		}
	}
	require.NotZero(t, largest, "the stream accepts a message just under its limit")
	require.Less(t, largest, natsx.MaxMessageSize, "subject and headers count toward the stream limit")
	response := connect("/v1/runs/"+c.RunID+"/events", 0, "")
	defer response.Body.Close()
	require.Equal(t, 200, response.StatusCode)
	frame, end := readSSE(t, bufio.NewScanner(response.Body))
	require.False(t, end)
	require.EqualValues(t, 1, frame.seq)
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
	marker := []byte(fmt.Sprintf(`{"run_id":%q,"event_id":%q}`, c.RunID, c.RunID+":main:0:0:11"))
	require.NoError(t, bus.Publish(t.Context(), natsx.EndSubject(c.TenantID, c.SessionID, c.RunID), "end:"+c.RunID, marker))
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
	s.cfg.WriteTimeout = 100 * time.Millisecond
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
		return metricCount(t, s, "streamgw_slow_client_closed_total") > 0
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
	next, err := New(bus, s.verifier, testConfig(), s.logger)
	require.NoError(t, err)
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

func TestInvalidFramesFiltersAndConnectionAge(t *testing.T) {
	s, bus, c, connect := realGateway(t, -1)
	// Malformed and cross-tenant envelopes never reach the client. Pretty JSON
	// remains semantically identical but is compacted to one SSE data line.
	require.NoError(t, bus.Publish(t.Context(), subject(c), ids.New(), []byte(`{"secret":"do not forward"}`)))
	wrong := c
	wrong.TenantID = ids.New()
	require.NoError(t, bus.Publish(t.Context(), subject(c), ids.New(), eventBytes(t, wrong, 2, "run.status")))
	raw := eventBytes(t, c, 3, "run.status")
	var pretty any
	require.NoError(t, json.Unmarshal(raw, &pretty))
	raw, err := json.Marshal(pretty, jsontext.Multiline(true))
	require.NoError(t, err)
	require.NoError(t, bus.Publish(t.Context(), subject(c), ids.New(), raw))
	require.NoError(t, bus.Publish(t.Context(), subject(c), ids.New(), eventBytes(t, c, 4, "run.completed")))
	response := connect("/v1/runs/"+c.RunID+"/events", 0, "?types=run.status")
	scan := bufio.NewScanner(response.Body)
	got, end := readSSE(t, scan)
	require.False(t, end)
	require.EqualValues(t, 3, got.seq)
	_, end = readSSE(t, scan)
	require.True(t, end, "filtering the terminal event must still end the run stream")
	require.NoError(t, response.Body.Close())
	require.EqualValues(t, 2, metricCount(t, s, "streamgw_invalid_frames_total"))
	// Session scope never ends on a run terminal, and hard connection age closes
	// silently. The separate writer/token unit tests use a virtual clock.
	s.cfg.MaxAge = 50 * time.Millisecond
	response = connect("/v1/sessions/"+c.SessionID+"/events", 0, "")
	raw, err = io.ReadAll(response.Body)
	require.True(t, err == nil || errors.Is(err, io.ErrUnexpectedEOF))
	require.NotContains(t, string(raw), "event: end")
	require.Contains(t, string(raw), "event: run.completed")
	require.NoError(t, response.Body.Close())
}

func TestE35LateTerminalMatrix(t *testing.T) {
	s, bus, c, connect := realGateway(t, -1)
	terminal, err := bus.JS.Publish(t.Context(), subject(c), eventBytes(t, c, 1, "run.completed"))
	require.NoError(t, err)
	endSubject := natsx.EndSubject(c.TenantID, c.SessionID, c.RunID)
	require.NoError(t, bus.Publish(t.Context(), endSubject, "end:"+c.RunID, []byte(`{"run_id":"r","event_id":"terminal"}`)))
	late, err := bus.JS.Publish(t.Context(), subject(c), eventBytes(t, c, 2, "run.status"))
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		cursor uint64
		query  string
		seqs   []uint64
	}{
		{name: "zero", cursor: 0, query: "", seqs: []uint64{terminal.Sequence, late.Sequence}},
		{name: "terminal", cursor: terminal.Sequence, query: "", seqs: []uint64{late.Sequence}},
		{name: "late", cursor: late.Sequence, query: "", seqs: nil},
		{name: "filter-terminal", cursor: 0, query: "?types=run.status", seqs: []uint64{late.Sequence}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := connect("/v1/runs/"+c.RunID+"/events", tc.cursor, tc.query)
			defer response.Body.Close()
			require.Equal(t, 200, response.StatusCode)
			scan := bufio.NewScanner(response.Body)
			for _, seq := range tc.seqs {
				frame, end := readSSE(t, scan)
				require.False(t, end)
				require.Equal(t, seq, frame.seq)
			}
			_, end := readSSE(t, scan)
			require.True(t, end)
		})
	}
	s.cfg.MaxAge = 30 * time.Millisecond
	response := connect("/v1/sessions/"+c.SessionID+"/events", 0, "")
	raw, err := io.ReadAll(response.Body)
	response.Body.Close()
	require.True(t, err == nil || errors.Is(err, io.ErrUnexpectedEOF))
	require.NotContains(t, string(raw), "event: end")
	require.Equal(t, 2, strings.Count(string(raw), "id: "))
	require.Zero(t, metricCount(t, s, "streamgw_invalid_frames_total"))
}

func TestE35NoMarkerIdleThenLiveTerminal(t *testing.T) {
	s, bus, c, connect := realGateway(t, -1)
	s.cfg.Heartbeat = 5 * time.Millisecond
	ack, err := bus.JS.Publish(t.Context(), subject(c), eventBytes(t, c, 1, "run.status"))
	require.NoError(t, err)
	response := connect("/v1/runs/"+c.RunID+"/events", ack.Sequence, "")
	defer response.Body.Close()
	scan := bufio.NewScanner(response.Body)
	found := false
	for scan.Scan() {
		require.NotEqual(t, "event: end", scan.Text())
		if scan.Text() == ": ping" {
			found = true
			break
		}
	}
	require.True(t, found, "tail without marker stays live and heartbeats")
	require.NoError(t, bus.Publish(t.Context(), subject(c), "terminal", eventBytes(t, c, 2, "run.completed")))
	require.NoError(t, bus.Publish(t.Context(), natsx.EndSubject(c.TenantID, c.SessionID, c.RunID), "end:"+c.RunID, []byte(`{}`)))
	frame, end := readSSE(t, scan)
	require.False(t, end)
	require.Equal(t, "run.completed", frame.kind)
	_, end = readSSE(t, scan)
	require.True(t, end)
}

// The main event consumer succeeds, but the marker lookup goes to a missing
// stream and receives an actual JetStream API error before any SSE headers.
type missingMarkerStream struct{ eventSource }

func (s missingMarkerStream) OrderedConsumer(
	ctx context.Context, stream string, cfg jetstream.OrderedConsumerConfig,
) (jetstream.Consumer, error) {
	if strings.HasSuffix(cfg.FilterSubjects[0], ".end") {
		stream = "MISSING_" + stream
	}
	return s.eventSource.OrderedConsumer(ctx, stream, cfg)
}
func TestE35MarkerLookupUnavailable(t *testing.T) {
	s, bus, c, connect := realGateway(t, -1)
	ack, err := bus.JS.Publish(t.Context(), subject(c), eventBytes(t, c, 1, "run.status"))
	require.NoError(t, err)
	s.js = missingMarkerStream{eventSource: s.js}
	response := connect("/v1/runs/"+c.RunID+"/events", ack.Sequence, "")
	defer response.Body.Close()
	require.Equal(t, 503, response.StatusCode)
	raw, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Contains(t, string(raw), "STREAM_UNAVAILABLE")
}

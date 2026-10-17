package sink

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
)

type fakeStore struct {
	mu                  sync.Mutex
	writeErr, rejectErr error
	panicWrite          bool
	writes              int
	rejects             []rejection
	gate                <-chan struct{}
}

func (s *fakeStore) write(ctx context.Context, _ contract.RunEvent, _ uint64, _ []byte) (bool, error) {
	if s.panicWrite {
		panic("sensitive panic")
	}
	if s.gate != nil {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-s.gate:
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writes++
	return s.writes == 1, s.writeErr
}
func (s *fakeStore) reject(_ context.Context, r rejection) error {
	s.rejects = append(s.rejects, r)
	return s.rejectErr
}

type fakeMsg struct {
	jetstream.Msg
	mu                          sync.Mutex
	subject                     string
	raw                         []byte
	meta                        jetstream.MsgMetadata
	acks, naks, progress, terms int
	delay                       time.Duration
}

func (m *fakeMsg) Data() []byte                              { return m.raw }
func (m *fakeMsg) Subject() string                           { return m.subject }
func (m *fakeMsg) Metadata() (*jetstream.MsgMetadata, error) { return &m.meta, nil }
func (m *fakeMsg) Ack() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acks++
	return nil
}
func (m *fakeMsg) NakWithDelay(delay time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.naks++
	m.delay = delay
	return nil
}
func (m *fakeMsg) InProgress() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.progress++
	return nil
}
func (m *fakeMsg) Term() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.terms++
	return nil
}

func validMessage(t *testing.T, kind string) *fakeMsg {
	t.Helper()
	tenant, session, run := ids.New(), ids.New(), ids.New()
	data := map[string]any{"status": "running"}
	switch kind {
	case "message.delta", "reasoning.delta":
		data = map[string]any{"message_id": "msg", "delta": "private content"}
	case "sandbox.output":
		data = map[string]any{"stream": "stdout", "chunk": "private content"}
	case "usage.update":
		data = map[string]any{"input_tokens": 1, "output_tokens": 2, "model": "test"}
	}
	raw, err := json.Marshal(contract.RunEvent{V: 1, Id: run + ":main:0:0:1", TenantId: tenant, SessionId: session,
		RunId: run, Member: "main", Ts: time.Now().UTC(), Type: contract.EventType(kind), Data: data})
	require.NoError(t, err)
	return &fakeMsg{subject: "run." + tenant + "." + session + "." + run, raw: raw,
		meta: jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: 1}, NumDelivered: 1, Timestamp: time.Now()}}
}

func testSink(store eventStore) *Sink {
	metrics, err := NewMetrics(nil)
	if err != nil {
		panic(err)
	}
	return &Sink{store: store, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), metrics: metrics}
}

func TestClassification(t *testing.T) {
	for _, kind := range []string{"run.status", "message.delta", "reasoning.delta"} {
		m := validMessage(t, kind)
		event, reason := classify(m.subject, 1, m.raw)
		require.Empty(t, reason)
		require.Equal(t, kind == "run.status", persistent(event.Type))
		_, reason = classify(m.subject+".extra", 1, m.raw)
		require.Equal(t, "subject_mismatch", reason)
	}
	for _, kind := range []contract.EventType{"usage.update", "sandbox.output", "message.delta", "reasoning.delta"} {
		require.False(t, persistent(kind))
	}
	require.True(t, persistent("sandbox.restored"))
	_, reason := classify("run.invalid", 1, []byte(`{}`))
	require.Equal(t, "invalid_contract", reason)
	require.Empty(t, subjectTenant("run.invalid"))
}

func TestHandleOutcomes(t *testing.T) {
	for _, mode := range []string{
		"written", "duplicate", "skip", "invalid", "subject", "fk", "check", "retry", "max", "panic", "reject-failure"} {
		t.Run(mode, func(t *testing.T) {
			store := &fakeStore{}
			s := testSink(store)
			m := validMessage(t, "run.status")
			want := "written"
			switch mode {
			case "duplicate":
				store.writes, want = 1, "duplicate"
			case "skip":
				m, want = validMessage(t, "message.delta"), "skipped"
			case "invalid":
				m.raw, want = []byte(`{"sensitive":"content"}`), "rejected"
			case "subject":
				m.subject += ".extra"
				want = "rejected"
			case "fk":
				store.writeErr, want = &pgconn.PgError{Code: "23503"}, "rejected"
			case "check":
				store.writeErr, want = &pgconn.PgError{Code: "23514"}, "rejected"
			case "retry":
				store.writeErr = &pgconn.PgError{Code: "XX000", Message: "private error"}
			case "max":
				m.meta.NumDelivered, want = 10, "rejected"
				store.writeErr = &pgconn.PgError{Code: "XX000"}
			case "panic":
				store.panicWrite, want = true, "rejected"
			case "reject-failure":
				m.raw = []byte(`{}`)
				store.rejectErr = &pgconn.PgError{Code: "XX000"}
			}
			s.process(t.Context(), m)
			require.Zero(t, m.terms, "no outcome may terminate an unpersisted delivery")
			if mode == "retry" || mode == "reject-failure" {
				require.Zero(t, m.acks)
				require.Equal(t, 1, m.naks)
				return
			}
			require.Equal(t, 1, m.acks)
			require.Zero(t, m.naks)
			if want == "rejected" {
				require.Len(t, store.rejects, 1)
				require.Equal(t, m.raw, store.rejects[0].payload)
				require.Equal(t, subjectTenant(m.subject), store.rejects[0].tenant)
			}
		})
	}
}

func TestOutboxValidationReasons(t *testing.T) {
	m := validMessage(t, "run.status")
	var event contract.RunEvent
	require.NoError(t, json.Unmarshal(m.raw, &event))
	row := outboxEvent{tenant: event.TenantId, run: event.RunId, id: event.Id, payload: m.raw}
	subject, reason, _ := validateOutbox(row)
	require.Equal(t, m.subject, subject)
	require.Empty(t, reason)
	row.id = "mismatch"
	_, reason, _ = validateOutbox(row)
	require.Equal(t, "identity_mismatch", reason)
	row.id = event.Id
	event.SessionId = "not-a-uuid"
	row.payload, _ = json.Marshal(event)
	_, reason, _ = validateOutbox(row)
	require.Equal(t, "subject_unresolvable", reason)
	event.SessionId = ids.New()
	event.Type = "future-invalid-type"
	row.payload, _ = json.Marshal(event)
	_, reason, _ = validateOutbox(row)
	require.Equal(t, "invalid_envelope", reason)
	row.payload = []byte(`{`)
	_, reason, _ = validateOutbox(row)
	require.Equal(t, "invalid_envelope", reason)
}

type safeRetryError struct{}

func (safeRetryError) Error() string     { return "not sent" }
func (safeRetryError) SafeToRetry() bool { return true }

func TestRetryClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "08", err: fmt.Errorf("wrapped: %w", &pgconn.PgError{Code: "08006"}), want: true},
		{name: "40", err: &pgconn.PgError{Code: "40001"}, want: true},
		{name: "deadlock", err: &pgconn.PgError{Code: "40P01"}, want: true},
		{name: "too many", err: &pgconn.PgError{Code: "53300"}, want: true},
		{name: "57", err: &pgconn.PgError{Code: "57P01"}, want: true},
		{name: "cancelled", err: &pgconn.PgError{Code: "57014"}, want: true},
		{name: "eof", err: io.EOF, want: true},
		{name: "unexpected eof", err: io.ErrUnexpectedEOF, want: true},
		{name: "closed", err: pgconn.ErrConnClosed, want: true},
		{name: "deadline", err: context.DeadlineExceeded, want: true},
		{name: "safe to retry", err: fmt.Errorf("wrapped: %w", safeRetryError{}), want: true},
		{name: "network", err: &net.OpError{Op: "read", Net: "tcp", Err: errors.New("reset")}, want: true},
		{name: "unclassified", err: &pgconn.PgError{Code: "XX000"}, want: false},
		{name: "permission", err: &pgconn.PgError{Code: "42501"}, want: false},
		{name: "auth", err: &pgconn.PgError{Code: "28P01"}, want: false},
		{name: "unique", err: &pgconn.PgError{Code: "23505"}, want: false},
		{name: "plain", err: errors.New("plain"), want: false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, infrastructureError(tc.err)) })
	}
	require.Equal(t, 5*time.Second, retryDelay(1))
	require.Equal(t, 15*time.Second, retryDelay(2))
	require.Equal(t, 60*time.Second, retryDelay(10))
}

func TestInfrastructureRetryBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{writeErr: &pgconn.PgError{Code: "08006"}}
		s := testSink(store)
		m := validMessage(t, "run.status")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan struct{})
		go func() {
			s.process(ctx, m)
			close(done)
		}()
		synctest.Wait()
		writes := func() int {
			store.mu.Lock()
			defer store.mu.Unlock()
			return store.writes
		}
		require.Equal(t, 1, writes())
		time.Sleep(5*time.Second - time.Millisecond)
		synctest.Wait()
		require.Equal(t, 1, writes(), "no second attempt before the 5s first backoff")
		store.mu.Lock()
		store.writeErr = nil
		store.mu.Unlock()
		time.Sleep(time.Millisecond)
		synctest.Wait()
		require.Equal(t, 2, writes(), "second attempt exactly 5s after the first")
		<-done
		require.Equal(t, 1, m.acks)
		require.Zero(t, m.naks)
	})
}

func TestInfrastructureOutageBeyondDeliveryBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeStore{writeErr: &pgconn.PgError{Code: "08006"}}
		s := testSink(store)
		m := validMessage(t, "run.status")
		m.meta.NumDelivered = 10 // Still attempt persistence, never reject healthy data.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan struct{})
		go func() {
			s.process(ctx, m)
			close(done)
		}()
		time.Sleep(12 * time.Minute)
		synctest.Wait()
		store.mu.Lock()
		require.Greater(t, store.writes, 10)
		require.Empty(t, store.rejects)
		store.mu.Unlock()
		m.mu.Lock()
		require.Zero(t, m.acks)
		require.Zero(t, m.naks)
		require.Greater(t, m.progress, 50)
		m.mu.Unlock()
		store.mu.Lock()
		store.writeErr = nil // The database recovers; the held delivery must now write and Ack.
		store.mu.Unlock()
		time.Sleep(time.Minute)
		synctest.Wait()
		<-done
		m.mu.Lock()
		defer m.mu.Unlock()
		require.Equal(t, 1, m.acks)
		require.Zero(t, m.naks)
	})
}

func TestInfrastructureOutageCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := testSink(&fakeStore{writeErr: context.DeadlineExceeded})
		m := validMessage(t, "run.status")
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			s.process(ctx, m)
			close(done)
		}()
		time.Sleep(time.Minute)
		cancel()
		synctest.Wait()
		<-done
		require.Zero(t, m.acks)
		require.Zero(t, m.naks)
	})
}

func TestProgressDuringDatabaseFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		store := &fakeStore{gate: gate}
		s := testSink(store)
		m := validMessage(t, "run.status")
		go s.process(t.Context(), m)
		// JetStream redelivers a first delivery after the first backoff step.
		time.Sleep(natsx.SinkBackOff()[0])
		synctest.Wait()
		m.mu.Lock()
		require.GreaterOrEqual(t, m.progress, 2, "heartbeats arrive inside the first redelivery window")
		m.mu.Unlock()
		time.Sleep(31 * time.Second)
		synctest.Wait()
		m.mu.Lock()
		require.GreaterOrEqual(t, m.progress, 1)
		require.Zero(t, m.acks)
		m.mu.Unlock()
		close(gate)
		synctest.Wait()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		require.Equal(t, 1, m.acks)
	})
}

func TestMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	metrics, err := NewMetrics(provider.Meter("sink-test"))
	require.NoError(t, err)
	s := testSink(&fakeStore{})
	s.metrics = metrics
	s.process(t.Context(), validMessage(t, "run.status"))
	s.process(t.Context(), validMessage(t, "message.delta"))
	metrics.batch.Record(t.Context(), 0.02)
	metrics.pending.Record(t.Context(), 3)
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	seen := map[string]bool{}
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			seen[item.Name] = true
			switch values := item.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range values.DataPoints {
					require.EqualValues(t, 1, point.Value)
					for _, attr := range point.Attributes.ToSlice() {
						require.Contains(t, []string{"result", "type", "reason"}, string(attr.Key))
					}
				}
			case metricdata.Histogram[float64]:
				require.Equal(t, "s", item.Unit)
				require.Empty(t, values.DataPoints[0].Attributes.ToSlice())
			case metricdata.Gauge[int64]:
				require.EqualValues(t, 3, values.DataPoints[0].Value)
			}
		}
	}
	for _, name := range []string{"sink_messages_total", "sink_skipped_total", "sink_lag_seconds",
		"sink_batch_duration_seconds", "sink_pending"} {
		require.True(t, seen[name], name)
	}
}

func testMetrics(t *testing.T) *Metrics {
	t.Helper()
	m, err := NewMetrics(nil)
	require.NoError(t, err)
	return m
}

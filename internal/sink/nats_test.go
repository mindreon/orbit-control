//go:build nats_integration

package sink

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/mindreon/orbit-control/internal/activities/ctl"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
	"github.com/mindreon/orbit-control/internal/sessions"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/migrations"
)

// TestMain isolates migrations and failure-injection grants from every other
// package using ORBIT_TEST_DATABASE_URL. Only this uniquely owned DB is dropped.
func TestMain(m *testing.M) {
	raw := os.Getenv("ORBIT_TEST_DATABASE_URL")
	if raw == "" || os.Getenv("ORBIT_TEST_NATS_URL") == "" {
		os.Exit(m.Run())
	}
	u, err := url.Parse(raw)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid sink test database URL")
		os.Exit(1)
	}
	name := "wp7b_sink_" + strings.ReplaceAll(ids.New(), "-", "")
	u.Path = "/postgres"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	admin, err := pgx.Connect(ctx, u.String())
	if err == nil {
		_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()+" OWNER orbit_owner")
	}
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot create disposable sink database")
		os.Exit(1)
	}
	u.Path = "/" + name
	os.Setenv("ORBIT_TEST_DATABASE_URL", u.String())
	code := m.Run()
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	_, err = admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot remove disposable sink database")
		code = 1
	}
	admin.Close(ctx)
	cancel()
	os.Exit(code)
}

type fixture struct {
	db                                               *db.DB
	admin                                            *pgx.Conn
	bus                                              *natsx.Bus
	consumer                                         jetstream.Consumer
	tenant, session, run, workspace, stream, subject string
	logger                                           *slog.Logger
}

func roleURL(t *testing.T, raw, role string) string {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	q := u.Query()
	q.Set("options", "-c role="+role)
	u.RawQuery = q.Encode()
	return u.String()
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	raw, natsURL := os.Getenv("ORBIT_TEST_DATABASE_URL"), os.Getenv("ORBIT_TEST_NATS_URL")
	if raw == "" || natsURL == "" {
		t.Skip("ORBIT_TEST_DATABASE_URL and ORBIT_TEST_NATS_URL required")
	}
	require.NoError(t, migrations.Run(t.Context(), roleURL(t, raw, "orbit_owner"), "up"))
	admin, err := pgx.Connect(t.Context(), raw)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close(context.Background())) })
	d, err := db.OpenApp(t.Context(), roleURL(t, raw, "orbit_app"))
	require.NoError(t, err)
	t.Cleanup(d.Close)
	bus, err := natsx.Connect(natsURL, "orbit-v2-test-sink")
	require.NoError(t, err)
	t.Cleanup(bus.Close)
	f := &fixture{db: d, admin: admin, bus: bus,
		tenant: ids.New(), session: ids.New(), run: ids.New(), workspace: ids.New(), stream: "TEST_" + ids.New(),
		logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	f.subject = "run." + f.tenant + "." + f.session + "." + f.run
	_, err = admin.Exec(t.Context(), `INSERT INTO tenants(id,slug,name) VALUES($1,$1::uuid::text,'Sink')`, f.tenant)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), `INSERT INTO workspaces(id,tenant_id,name) VALUES($1,$2,'Sink')`, f.workspace, f.tenant)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), `INSERT INTO principals(id,kind,email,display_name,idp_subject)
 VALUES($1,'user',$1::uuid::text||'@sink.test','Sink',$1::uuid::text)`, f.workspace)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), `INSERT INTO memberships(tenant_id,principal_id,status,source) VALUES($1,$2,'active','direct')`,
		f.tenant, f.workspace)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), `INSERT INTO sessions(id,tenant_id,workspace_id,owner_id,title,entry_kind,entry_ref,message_policy,status)
 VALUES($1,$2,$3,$3,'Sink','agent','agent:test','queue','idle')`, f.session, f.tenant, f.workspace)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), `INSERT INTO runs(id,tenant_id,session_id,trigger,status) VALUES($1,$2,$3,'api','queued')`, f.run,
		f.tenant, f.session)
	require.NoError(t, err)
	_, err = bus.JS.CreateStream(t.Context(), jetstream.StreamConfig{Name: f.stream, Subjects: []string{"run." + f.tenant + ".>"},
		Storage: jetstream.MemoryStorage, Duplicates: 10 * time.Minute, MaxMsgSize: 64 << 10})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, bus.JS.DeleteStream(ctx, f.stream))
		for _, table := range []string{"audit_log", "run_event_rejects", "run_events", "control_events", "runs", "sessions", "workspaces",
			"memberships", "tenants"} {
			column := "tenant_id"
			if table == "tenants" {
				column = "id"
			}
			_, err := admin.Exec(ctx, "DELETE FROM "+pgx.Identifier{table}.Sanitize()+" WHERE "+column+"=$1", f.tenant)
			require.NoError(t, err)
		}
		_, err := admin.Exec(ctx, `DELETE FROM principals WHERE id=$1`, f.workspace)
		require.NoError(t, err)
	})
	f.consumer, err = bus.JS.CreateOrUpdateConsumer(t.Context(),
		f.stream, jetstream.ConsumerConfig{Durable: "sink", FilterSubject: "run." + f.tenant + ".>",
			AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: -1, MaxAckPending: 2000,
			BackOff: natsx.SinkBackOff()})
	require.NoError(t, err)
	return f
}

func (f *fixture) event(t *testing.T, n int, kind string) []byte {
	t.Helper()
	m := validMessage(t, kind)
	var event contract.RunEvent
	require.NoError(t, json.Unmarshal(m.raw, &event))
	event.Id, event.TenantId, event.SessionId, event.RunId = fmt.Sprintf("%s:main:0:0:%d", f.run, n), f.tenant, f.session, f.run
	raw, err := json.Marshal(event)
	require.NoError(t, err)
	require.NoError(t, contract.Validate("RunEvent", raw))
	return raw
}

func (f *fixture) count(t *testing.T, table string) int {
	t.Helper()
	var count int
	require.NoError(t, f.admin.QueryRow(t.Context(), "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()+" WHERE tenant_id=$1",
		f.tenant).Scan(&count))
	return count
}

func (f *fixture) next(t *testing.T) jetstream.Msg {
	t.Helper()
	msg, err := f.consumer.Next(jetstream.FetchMaxWait(2 * time.Second))
	require.NoError(t, err)
	return msg
}

func TestSinkRedeliveryAndPoison(t *testing.T) {
	f := newFixture(t)
	s := New(f.db, f.consumer, f.logger, testMetrics(t))
	stream, err := f.bus.JS.Stream(t.Context(), f.stream)
	require.NoError(t, err)
	cfg := stream.CachedInfo().Config
	cfg.Duplicates = 100 * time.Millisecond
	_, err = f.bus.JS.UpdateStream(t.Context(), cfg)
	require.NoError(t, err)
	// Publish n=5 before n=1; preserve their actual stream sequence in history.
	original := f.event(t, 5, "run.status")
	require.NoError(t, f.bus.Publish(t.Context(), f.subject, "id5", original))
	msg := f.next(t)
	meta, err := msg.Metadata()
	require.NoError(t, err)
	result, _, err := s.handle(t.Context(), msg, meta)
	require.NoError(t, err)
	require.Equal(t, "written", result)
	require.NoError(t, msg.Nak())
	redelivered := f.next(t)
	redeliveryMeta, err := redelivered.Metadata()
	require.NoError(t, err)
	require.Equal(t, meta.Sequence.Stream, redeliveryMeta.Sequence.Stream)
	s.process(t.Context(), redelivered)
	require.Equal(t, 1, f.count(t, "run_events"))
	// A 100ms test dedup window models a production retry outside the 10-minute window.
	time.Sleep(110 * time.Millisecond)
	require.NoError(t, f.bus.Publish(t.Context(), f.subject, "id5", original))
	s.process(t.Context(), f.next(t))
	require.Equal(t, 1, f.count(t, "run_events"))
	require.NoError(t, f.bus.Publish(t.Context(), f.subject, ids.New(), f.event(t, 1, "run.status")))
	s.process(t.Context(), f.next(t))
	require.Equal(t, 2, f.count(t, "run_events"))
	rows, err := f.admin.Query(t.Context(), `SELECT event_id,stream_seq FROM run_events WHERE tenant_id=$1 ORDER BY stream_seq`, f.tenant)
	require.NoError(t, err)
	var got []string
	for rows.Next() {
		var id string
		var seq int
		require.NoError(t, rows.Scan(&id, &seq))
		got = append(got, id)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Equal(t, []string{f.run + ":main:0:0:5", f.run + ":main:0:0:1"}, got)
	for _, kind := range []string{"message.delta", "reasoning.delta"} {
		require.NoError(t, f.bus.Publish(t.Context(), f.subject, ids.New(), f.event(t, 10, kind)))
		s.process(t.Context(), f.next(t))
	}
	for _, mode := range []string{"invalid", "subject", "missing-run"} {
		subject, raw := f.subject, f.event(t, 11, "run.status")
		switch mode {
		case "invalid":
			raw = []byte(`{"content":"must not be logged"}`)
		case "subject":
			subject = "run." + f.tenant + "." + ids.New() + "." + f.run
		case "missing-run":
			var event contract.RunEvent
			require.NoError(t, json.Unmarshal(raw, &event))
			event.RunId = ids.New()
			event.Id = event.RunId + ":main:0:0:11"
			raw, err = json.Marshal(event)
			require.NoError(t, err)
			subject = "run." + f.tenant + "." + f.session + "." + event.RunId
		}
		require.NoError(t, f.bus.Publish(t.Context(), subject, ids.New(), raw))
		s.process(t.Context(), f.next(t))
	}
	require.Equal(t, 3, f.count(t, "run_event_rejects"))
	require.NoError(t, f.bus.Publish(t.Context(), f.subject, ids.New(), f.event(t, 12, "run.status")))
	s.process(t.Context(), f.next(t))
	require.Equal(t, 3, f.count(t, "run_events"), "poison messages do not stall the consumer")
	require.Eventually(t, func() bool {
		info, err := f.consumer.Info(t.Context())
		return err == nil && info.NumAckPending == 0 && info.NumPending == 0
	}, time.Second, time.Millisecond)
}

func TestTwoSinkReplicas(t *testing.T) {
	f := newFixture(t)
	const count = 600
	for n := range count {
		require.NoError(t, f.bus.Publish(t.Context(), f.subject, ids.New(), f.event(t, n+1, "run.status")))
	}
	ctx, cancel := context.WithCancel(t.Context())
	var wg sync.WaitGroup
	for range 2 {
		consumer, err := f.bus.JS.Consumer(t.Context(), f.stream, "sink")
		require.NoError(t, err)
		s := New(f.db, consumer, f.logger, testMetrics(t))
		wg.Go(func() { require.NoError(t, s.Run(ctx)) })
	}
	defer wg.Wait()
	defer cancel()
	require.Eventually(t, func() bool { return f.count(t, "run_events") == count }, 15*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		info, err := f.consumer.Info(t.Context())
		return err == nil && info.NumAckPending == 0 && info.NumPending == 0
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, count, f.count(t, "run_events"))
	require.Zero(t, f.count(t, "run_event_rejects"))
	var delivered uint64
	info, err := f.consumer.Info(t.Context())
	require.NoError(t, err)
	delivered = info.Delivered.Consumer
	require.EqualValues(t, count, delivered, "queued batch heartbeats prevent premature redelivery")
}

func TestOutboxFailureRecoveryAndConcurrentOrder(t *testing.T) {
	f := newFixture(t)
	// Interrupt only this Activity's NATS TCP path; the test-owned broker stays isolated.
	natsURL, err := url.Parse(os.Getenv("ORBIT_TEST_NATS_URL"))
	require.NoError(t, err)
	proxy := startPGProxy(t, natsURL.Host)
	natsURL.Host = proxy.listener.Addr().String()
	outageBus, err := natsx.Connect(natsURL.String(), "orbit-v2-test-outbox-outage")
	require.NoError(t, err)
	defer outageBus.Close()
	proxy.setOffline(true)
	require.Eventually(t, func() bool { return !outageBus.Conn.IsConnected() }, time.Second, time.Millisecond)
	activities := &ctl.Activities{DB: f.db, NATS: outageBus, Logger: f.logger}
	err = activities.SetRunStatus(t.Context(), w.StatusInput{RunOp: w.RunOp{TenantID: f.tenant, SessionID: f.session, RunID: f.run,
		Member: "main", Seq: 0, EventN: 1, EventAt: time.Now().UTC()}, Status: "running"})
	require.NoError(t, err)
	var pending bool
	require.NoError(t, f.admin.QueryRow(t.Context(), `SELECT published_at IS NULL FROM control_events
 WHERE tenant_id=$1`, f.tenant).Scan(&pending))
	require.True(t, pending)
	proxy.setOffline(false)
	require.Eventually(t, outageBus.Conn.IsConnected, 5*time.Second, 10*time.Millisecond)
	outbox := &Outbox{DB: f.db, Publisher: outageBus, Logger: f.logger, Interval: time.Millisecond}
	require.NoError(t, outbox.Republish(t.Context()))
	info, err := f.consumer.Info(t.Context())
	require.NoError(t, err)
	require.NoError(t, f.admin.QueryRow(t.Context(), `SELECT published_at IS NULL FROM control_events
 WHERE tenant_id=$1`, f.tenant).Scan(&pending))
	require.True(t, pending, "30-second age gate is retained")
	// NATS may have buffered the timed-out Activity publish and delivered it on
	// reconnect. Treat that ambiguity exactly as production does: sink deduplicates.
	before := 0
	if info.NumPending > 0 {
		projection := New(f.db, f.consumer, f.logger, testMetrics(t))
		projection.process(t.Context(), f.next(t))
		before = 1
	}
	// Immutable created_at cannot be updated. Replace only this fixture's outbox
	// with old fixture rows so the retention delay does not require a real timer.
	_, err = f.admin.Exec(t.Context(), `DELETE FROM control_events WHERE tenant_id=$1`, f.tenant)
	require.NoError(t, err)
	createdAt := time.Now().Add(-31 * time.Second)
	for _, n := range []int{5, 1, 3} {
		raw := f.event(t, n, "run.status")
		_, err = f.admin.Exec(t.Context(), `INSERT INTO control_events(tenant_id,run_id,event_id,operation,payload,created_at)
 VALUES($1,$2,$3,$4,$5,$6)`, f.tenant, f.run, fmt.Sprintf("%s:main:0:0:%d", f.run, n), n, raw, createdAt)
		require.NoError(t, err)
	}
	// Equal creation timestamps isolate operation/event-id tie breaking.
	// Insert a same-operation pair with deterministic IDs and shared creation time.
	raw := f.event(t, 6, "run.status")
	_, err = f.admin.Exec(t.Context(), `INSERT INTO control_events(tenant_id,run_id,event_id,operation,payload,created_at)
 SELECT tenant_id,run_id,$2,operation,$3,created_at FROM control_events WHERE tenant_id=$1 AND operation=5`,
		f.tenant, f.run+":main:0:0:6", raw)
	require.NoError(t, err)
	want := []string{f.run + ":main:0:0:1", f.run + ":main:0:0:3", f.run + ":main:0:0:5", f.run + ":main:0:0:6"}
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { require.NoError(t, outbox.Republish(t.Context())) })
	}
	wg.Wait()
	require.NoError(t, outbox.Republish(t.Context()))
	var got []string
	s := New(f.db, f.consumer, f.logger, testMetrics(t))
	for range len(want) {
		msg := f.next(t)
		var event contract.RunEvent
		require.NoError(t, json.Unmarshal(msg.Data(), &event))
		got = append(got, event.Id)
		require.Equal(t, f.subject, msg.Subject())
		s.process(t.Context(), msg)
	}
	require.Equal(t, want, got)
	require.Equal(t, before+len(want), f.count(t, "run_events"))
	info, err = f.consumer.Info(t.Context())
	require.NoError(t, err)
	require.Zero(t, info.NumPending, "replica scans do not duplicate publication")
	var unmarked int
	require.NoError(t, f.admin.QueryRow(t.Context(), `SELECT count(*) FROM control_events
 WHERE tenant_id=$1 AND published_at IS NULL`, f.tenant).Scan(&unmarked))
	require.Zero(t, unmarked)
}

type publishFunc func(context.Context, string, string, []byte) error

func (fn publishFunc) Publish(ctx context.Context, subject, id string, raw []byte) error {
	return fn(ctx, subject, id, raw)
}

func insertOutbox(t *testing.T, f *fixture, n int) {
	t.Helper()
	_, err := f.admin.Exec(t.Context(), `INSERT INTO control_events(tenant_id,run_id,event_id,operation,payload,created_at)
 VALUES($1,$2,$3,$4,$5,now()-interval '31 seconds')`, f.tenant, f.run,
		fmt.Sprintf("%s:main:0:0:%d", f.run, n), n, f.event(t, n, "run.status"))
	require.NoError(t, err)
}

func TestOutboxSingleConnectionAndTenantRLS(t *testing.T) {
	a, b := newFixture(t), newFixture(t)
	for _, f := range []*fixture{a, b} {
		insertOutbox(t, f, 1)
		_, err := f.admin.Exec(t.Context(), `INSERT INTO control_events(tenant_id,run_id,event_id,operation,payload,created_at)
 VALUES($1,$2,'poison',0,'{}',now()-interval '32 seconds')`, f.tenant, f.run)
		require.NoError(t, err)
	}
	u, err := url.Parse(roleURL(t, os.Getenv("ORBIT_TEST_DATABASE_URL"), "orbit_app"))
	require.NoError(t, err)
	q := u.Query()
	q.Set("pool_max_conns", "1")
	u.RawQuery = q.Encode()
	d, err := db.OpenApp(t.Context(), u.String())
	require.NoError(t, err)
	t.Cleanup(d.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	outbox := &Outbox{DB: d, Publisher: a.bus, Logger: a.logger}
	require.NoError(t, outbox.Republish(ctx), "relay must not acquire a second pool connection")
	for _, f := range []*fixture{a, b} {
		var pending int
		require.NoError(t, f.admin.QueryRow(ctx, `SELECT count(*) FROM control_events
 WHERE tenant_id=$1 AND published_at IS NULL AND quarantined_at IS NULL`, f.tenant).Scan(&pending))
		require.Zero(t, pending)
	}
	require.NoError(t, d.Global(ctx, func(tx pgx.Tx) error {
		var count int
		err := tx.QueryRow(ctx, `SELECT count(*) FROM control_events`).Scan(&count)
		require.Zero(t, count, "transaction-local tenant context must not leak to the pooled connection")
		return err
	}))
}

func TestOutboxCancellationAndCompetingReplica(t *testing.T) {
	f := newFixture(t)
	insertOutbox(t, f, 1)
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	outbox := &Outbox{DB: f.db, Logger: f.logger, Publisher: publishFunc(func(ctx context.Context, _, _ string, _ []byte) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})}
	done := make(chan error, 1)
	go func() { done <- outbox.Republish(ctx) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("first relay did not publish")
	}
	// A competing replica skips a lock held by an in-flight batch, without publishing.
	other := &Outbox{DB: f.db, Publisher: f.bus, Logger: f.logger}
	probe, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	require.NoError(t, other.Republish(probe))
	info, err := f.consumer.Info(t.Context())
	require.NoError(t, err)
	require.Zero(t, info.NumPending)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled relay did not release its transaction")
	}
	require.NoError(t, other.Republish(t.Context()), "cancellation must release the lock for recovery")
	info, err = f.consumer.Info(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, info.NumPending)
}

func TestOutboxPublishFailureCommitsPrefix(t *testing.T) {
	f := newFixture(t)
	insertOutbox(t, f, 1)
	insertOutbox(t, f, 2)
	calls := 0
	outbox := &Outbox{DB: f.db, Logger: f.logger, Publisher: publishFunc(func(ctx context.Context, subject, id string, raw []byte) error {
		calls++
		if calls == 2 {
			return fmt.Errorf("synthetic publish failure")
		}
		return f.bus.Publish(ctx, subject, id, raw)
	})}
	require.Error(t, outbox.Republish(t.Context()))
	var pending int
	require.NoError(t, f.admin.QueryRow(t.Context(), `SELECT count(*) FROM control_events
 WHERE tenant_id=$1 AND published_at IS NULL`, f.tenant).Scan(&pending))
	require.Equal(t, 1, pending, "publication failure commits the successful prefix")
	var quarantined int
	require.NoError(t, f.admin.QueryRow(t.Context(), `SELECT count(*) FROM control_events
 WHERE tenant_id=$1 AND quarantined_at IS NOT NULL`, f.tenant).Scan(&quarantined))
	require.Zero(t, quarantined, "publish failures never quarantine")
	outbox.Publisher = f.bus
	require.NoError(t, outbox.Republish(t.Context()))
	info, err := f.consumer.Info(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, info.NumPending, "published prefix uses the same msg ID on recovery")
}

func TestOutboxQuarantineIsolationRecoveryAndPermissions(t *testing.T) {
	a, b := newFixture(t), newFixture(t)
	poison := a.event(t, 50, "run.status")
	_, err := a.admin.Exec(t.Context(), `INSERT INTO control_events(tenant_id,run_id,event_id,operation,payload,created_at)
 VALUES($1,$2,'poison',0,$3,now()-interval '32 seconds')`, a.tenant, a.run, poison)
	require.NoError(t, err)
	insertOutbox(t, a, 1)
	insertOutbox(t, b, 1)
	var logs bytes.Buffer
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
	outbox := &Outbox{DB: a.db, Publisher: a.bus, Logger: slog.New(slog.NewJSONHandler(&logs, nil)), Meter: provider.Meter("outbox-test")}
	require.NoError(t, outbox.Republish(t.Context()))
	require.NoError(t, outbox.Republish(t.Context()))
	var reason string
	var stored []byte
	require.NoError(t, a.admin.QueryRow(t.Context(), `SELECT quarantine_reason,payload FROM control_events
 WHERE tenant_id=$1 AND event_id='poison'`, a.tenant).Scan(&reason, &stored))
	require.Equal(t, "identity_mismatch", reason)
	original := bytes.Clone(stored)
	for _, f := range []*fixture{a, b} {
		info, err := f.consumer.Info(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 1, info.NumPending)
	}
	for _, query := range []string{
		`UPDATE control_events SET published_at=NULL WHERE event_id<>'poison'`,
		`UPDATE control_events SET quarantine_reason='unknown',quarantined_at=now() WHERE event_id<>'poison'`,
	} {
		require.Error(t, a.db.WithTenant(t.Context(), a.tenant, func(tx pgx.Tx) error {
			_, err := tx.Exec(t.Context(), query)
			return err
		}))
	}
	require.Contains(t, logs.String(), "outbox.quarantine")
	require.NotContains(t, logs.String(), string(poison))
	require.NotContains(t, logs.String(), "running")
	var measurements metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &measurements))
	counter := measurements.ScopeMetrics[0].Metrics[0]
	require.Equal(t, "outbox_quarantined_total", counter.Name)
	point := counter.Data.(metricdata.Sum[int64]).DataPoints[0]
	require.EqualValues(t, 1, point.Value)
	require.Len(t, point.Attributes.ToSlice(), 1)
	require.Equal(t, "reason", string(point.Attributes.ToSlice()[0].Key))
	for _, query := range []string{
		`UPDATE control_events SET payload='{}' WHERE event_id='poison'`,
		`UPDATE control_events SET event_id='changed' WHERE event_id='poison'`,
		`UPDATE control_events SET quarantined_at=NULL,quarantine_reason=NULL WHERE event_id='poison'`,
		`UPDATE control_events SET published_at=now() WHERE event_id='poison'`,
		`UPDATE control_events SET quarantine_reason='invalid_envelope' WHERE event_id='poison'`,
		`UPDATE control_events SET quarantined_at=NULL WHERE event_id='poison'`,
	} {
		require.Error(t, a.db.WithTenant(t.Context(), a.tenant, func(tx pgx.Tx) error {
			_, err := tx.Exec(t.Context(), query)
			return err
		}))
	}
	owner, err := db.Open(t.Context(), roleURL(t, os.Getenv("ORBIT_TEST_DATABASE_URL"), "orbit_owner"))
	require.NoError(t, err)
	t.Cleanup(owner.Close)
	cutoff := time.Now().Add(time.Hour)
	require.Error(t, sessions.PruneEvents(t.Context(), a.db, nil, nil, &cutoff))
	require.NoError(t, sessions.PruneEvents(t.Context(), owner, nil, nil, &cutoff))
	q := sessions.RequeueControlEvents{Tenant: a.tenant, Run: a.run, EventID: "poison", Reason: "identity_mismatch", DryRun: true}
	n, err := sessions.RequeueEvents(t.Context(), owner, q)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	var stillIsolated bool
	require.NoError(t, a.admin.QueryRow(t.Context(), `SELECT quarantined_at IS NOT NULL FROM control_events
 WHERE tenant_id=$1 AND event_id='poison'`, a.tenant).Scan(&stillIsolated))
	require.True(t, stillIsolated, "dry-run cannot clear quarantine")
	q.DryRun = false
	n, err = sessions.RequeueEvents(t.Context(), owner, q)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	require.NoError(t, outbox.Republish(t.Context()), "unchanged invalid data is quarantined again")
	n, err = sessions.RequeueEvents(t.Context(), owner, q)
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	// A corrected validator is simulated without modifying immutable payload bytes.
	outbox.validate = func(outboxEvent) (string, string, contract.RunEvent) { return a.subject, "", contract.RunEvent{} }
	require.NoError(t, outbox.Republish(t.Context()))
	require.NoError(t, a.admin.QueryRow(t.Context(), `SELECT payload FROM control_events
 WHERE tenant_id=$1 AND event_id='poison' AND published_at IS NOT NULL AND quarantined_at IS NULL`, a.tenant).Scan(&stored))
	require.Equal(t, original, stored)
}

func TestOutboxQuarantineNoStarvationAndRollback(t *testing.T) {
	f := newFixture(t)
	for n := range 501 {
		_, err := f.admin.Exec(t.Context(), `INSERT INTO control_events(tenant_id,run_id,event_id,operation,payload,created_at)
 VALUES($1,$2,$3,$4,'{}',now()-interval '32 seconds')`, f.tenant, f.run, fmt.Sprintf("poison%d", n), n)
		require.NoError(t, err)
	}
	insertOutbox(t, f, 1000)
	_, err := f.admin.Exec(t.Context(), `REVOKE UPDATE(quarantined_at) ON control_events FROM orbit_app`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := f.admin.Exec(context.Background(), `GRANT UPDATE(quarantined_at) ON control_events TO orbit_app`)
		require.NoError(t, err)
	})
	outbox := &Outbox{DB: f.db, Publisher: f.bus, Logger: f.logger}
	require.Error(t, outbox.Republish(t.Context()))
	var isolated int
	require.NoError(t, f.admin.QueryRow(t.Context(), `SELECT count(*) FROM control_events
 WHERE tenant_id=$1 AND quarantined_at IS NOT NULL`, f.tenant).Scan(&isolated))
	require.Zero(t, isolated)
	_, err = f.admin.Exec(t.Context(), `GRANT UPDATE(quarantined_at) ON control_events TO orbit_app`)
	require.NoError(t, err)
	for range 2 {
		require.NoError(t, outbox.Republish(t.Context()))
	}
	info, err := f.consumer.Info(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, info.NumPending)
	require.NoError(t, f.admin.QueryRow(t.Context(), `SELECT count(*) FROM control_events
 WHERE tenant_id=$1 AND quarantined_at IS NOT NULL`, f.tenant).Scan(&isolated))
	require.Equal(t, 501, isolated)
}

func TestOutboxNATSOutageDoesNotQuarantine(t *testing.T) {
	f := newFixture(t)
	insertOutbox(t, f, 1)
	u, err := url.Parse(os.Getenv("ORBIT_TEST_NATS_URL"))
	require.NoError(t, err)
	proxy := startPGProxy(t, u.Host)
	u.Host = proxy.listener.Addr().String()
	bus, err := natsx.Connect(u.String(), "orbit-v2-test-relay-outage")
	require.NoError(t, err)
	t.Cleanup(bus.Close)
	proxy.setOffline(true)
	require.Eventually(t, func() bool { return !bus.Conn.IsConnected() }, time.Second, time.Millisecond)
	outbox := &Outbox{DB: f.db, Publisher: bus, Logger: f.logger}
	require.Error(t, outbox.Republish(t.Context()))
	var pending bool
	require.NoError(t, f.admin.QueryRow(t.Context(), `SELECT published_at IS NULL AND quarantined_at IS NULL
 FROM control_events WHERE tenant_id=$1`, f.tenant).Scan(&pending))
	require.True(t, pending, "NATS outage cannot turn valid events into poison")
	proxy.setOffline(false)
	require.Eventually(t, bus.Conn.IsConnected, 5*time.Second, time.Millisecond)
	require.NoError(t, outbox.Republish(t.Context()))
}

func TestOutboxPublishBudgetCommitsProgress(t *testing.T) {
	f := newFixture(t)
	for n := range 12 {
		insertOutbox(t, f, n+1)
	}
	outbox := &Outbox{DB: f.db, Logger: f.logger, publishBudget: 60 * time.Millisecond,
		Publisher: publishFunc(func(ctx context.Context, subject, id string, raw []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(15 * time.Millisecond):
			}
			return f.bus.Publish(ctx, subject, id, raw)
		})}
	previous := 0
	for range 12 {
		err := outbox.Republish(t.Context())
		require.True(t, err == nil || errors.Is(err, context.DeadlineExceeded))
		var published int
		require.NoError(t, f.admin.QueryRow(t.Context(), `SELECT count(*) FROM control_events
 WHERE tenant_id=$1 AND published_at IS NOT NULL`, f.tenant).Scan(&published))
		require.Greater(t, published, previous, "short publication budgets must still advance durable prefix")
		previous = published
		if published == 12 {
			break
		}
	}
	require.Equal(t, 12, previous)
}

func TestOutboxOversizedJSONBQuarantined(t *testing.T) {
	a, b := newFixture(t), newFixture(t)
	var event map[string]any
	require.NoError(t, json.Unmarshal(a.event(t, 1, "run.status"), &event))
	event["future_field"] = strings.Repeat("x", 70<<10)
	raw, err := json.Marshal(event)
	require.NoError(t, err)
	require.NoError(t, contract.Validate("RunEvent", raw))
	_, err = a.admin.Exec(t.Context(), `INSERT INTO control_events(tenant_id,run_id,event_id,operation,payload,created_at)
 VALUES($1,$2,$3,1,$4,now()-interval '32 seconds')`, a.tenant, a.run, event["id"], raw)
	require.NoError(t, err)
	insertOutbox(t, b, 1)
	require.NoError(t, (&Outbox{DB: a.db, Publisher: a.bus, Logger: a.logger}).Republish(t.Context()))
	var reason string
	require.NoError(t, a.admin.QueryRow(t.Context(), `SELECT quarantine_reason FROM control_events
 WHERE tenant_id=$1`, a.tenant).Scan(&reason))
	require.Equal(t, "invalid_envelope", reason)
	info, err := b.consumer.Info(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 1, info.NumPending)
}

// insertTerminal stores a run.completed event old enough for the relay to pick up.
func insertTerminal(t *testing.T, f *fixture) {
	t.Helper()
	var terminal contract.RunEvent
	require.NoError(t, json.Unmarshal(f.event(t, 1, "run.status"), &terminal))
	terminal.Type, terminal.Data = contract.EventType("run.completed"), map[string]any{"status": "succeeded"}
	raw, err := json.Marshal(terminal)
	require.NoError(t, err)
	require.NoError(t, contract.Validate("RunEvent", raw))
	_, err = f.admin.Exec(t.Context(), `INSERT INTO control_events(tenant_id,run_id,event_id,operation,payload,created_at)
 VALUES($1,$2,$3,1,$4,now()-interval '31 seconds')`, f.tenant, f.run, terminal.Id, raw)
	require.NoError(t, err)
}

func TestOutboxMarkPublishedFailureRollsBackAndDeduplicates(t *testing.T) {
	f := newFixture(t)
	insertTerminal(t, f)
	_, err := f.admin.Exec(t.Context(), `REVOKE UPDATE(published_at) ON control_events FROM orbit_app`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := f.admin.Exec(context.Background(), `GRANT UPDATE(published_at) ON control_events TO orbit_app`)
		require.NoError(t, err)
	})
	outbox := &Outbox{DB: f.db, Publisher: f.bus, Logger: f.logger}
	require.Error(t, outbox.Republish(t.Context()), "publish and marker succeed, then the published_at UPDATE is refused")
	var pending bool
	require.NoError(t, f.admin.QueryRow(t.Context(),
		`SELECT published_at IS NULL FROM control_events WHERE tenant_id=$1`, f.tenant).Scan(&pending))
	require.True(t, pending, "the failed transaction rolled back")
	_, err = f.admin.Exec(t.Context(), `GRANT UPDATE(published_at) ON control_events TO orbit_app`)
	require.NoError(t, err)
	require.NoError(t, outbox.Republish(t.Context()))
	require.NoError(t, f.admin.QueryRow(t.Context(),
		`SELECT published_at IS NULL FROM control_events WHERE tenant_id=$1`, f.tenant).Scan(&pending))
	require.False(t, pending)
	stream, err := f.bus.JS.Stream(t.Context(), f.stream)
	require.NoError(t, err)
	info, err := stream.Info(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 2, info.State.Msgs, "the retry is deduplicated: terminal and marker, once each")
	for range 2 {
		New(f.db, f.consumer, f.logger, testMetrics(t)).process(t.Context(), f.next(t))
	}
	require.Equal(t, 1, f.count(t, "run_events"))
	require.Zero(t, f.count(t, "run_event_rejects"))
}

func TestOutboxMarkerPublishFailure(t *testing.T) {
	f := newFixture(t)
	insertTerminal(t, f)
	fail := true
	publisher := publishFunc(func(ctx context.Context, subject, id string, raw []byte) error {
		if strings.HasSuffix(subject, ".end") && fail {
			return errors.New("marker publish failed")
		}
		return f.bus.Publish(ctx, subject, id, raw)
	})
	outbox := &Outbox{DB: f.db, Publisher: publisher, Logger: f.logger}
	require.Error(t, outbox.Republish(t.Context()))
	var pending bool
	require.NoError(t, f.admin.QueryRow(t.Context(),
		`SELECT published_at IS NULL FROM control_events WHERE tenant_id=$1`, f.tenant).Scan(&pending))
	require.True(t, pending)
	fail = false
	require.NoError(t, outbox.Republish(t.Context()))
	require.NoError(t, f.admin.QueryRow(t.Context(),
		`SELECT published_at IS NULL FROM control_events WHERE tenant_id=$1`, f.tenant).Scan(&pending))
	require.False(t, pending)
	terminal, marker := f.next(t), f.next(t)
	tm, err := terminal.Metadata()
	require.NoError(t, err)
	mm, err := marker.Metadata()
	require.NoError(t, err)
	require.EqualValues(t, 1, tm.Sequence.Stream)
	require.EqualValues(t, 2, mm.Sequence.Stream)
	New(f.db, f.consumer, f.logger, testMetrics(t)).process(t.Context(), terminal)
	New(f.db, f.consumer, f.logger, testMetrics(t)).process(t.Context(), marker)
	require.Equal(t, 1, f.count(t, "run_events"))
	require.Zero(t, f.count(t, "run_event_rejects"))
}

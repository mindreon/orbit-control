//go:build nats_integration

package sink

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/activities/ctl"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
	"github.com/mindreon/orbit-control/internal/sessions"
	"github.com/mindreon/orbit-control/internal/streamgw"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

// Remap the production stream name to this test's isolated stream. All requests
// still go to real JetStream; no floor, sequence, or pending information is faked.
type fixtureJS struct {
	jetstream.JetStream
	stream string
}

func (j fixtureJS) Stream(ctx context.Context, _ string) (jetstream.Stream, error) {
	return j.JetStream.Stream(ctx, j.stream)
}
func (j fixtureJS) Consumer(ctx context.Context, _, _ string) (jetstream.Consumer, error) {
	return j.JetStream.Consumer(ctx, j.stream, "sink")
}
func (j fixtureJS) OrderedConsumer(ctx context.Context, _ string, c jetstream.OrderedConsumerConfig) (jetstream.Consumer, error) {
	return j.JetStream.OrderedConsumer(ctx, j.stream, c)
}

type blockedStore struct {
	eventStore
	entered chan struct{}
	release chan struct{}
}

func (s blockedStore) write(ctx context.Context, e contract.RunEvent, seq uint64, raw []byte) (bool, error) {
	if seq == 1 {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return s.eventStore.write(ctx, e, seq, raw)
}
func fixturePage(t *testing.T, f *fixture, after, floor uint64) sessions.EventPage {
	t.Helper()
	var page sessions.EventPage
	err := f.db.WithTenant(t.Context(), f.tenant, func(tx pgx.Tx) error {
		ctx := auth.Context(db.Context(t.Context(), tx), auth.Principal{}, f.tenant)
		var err error
		page, err = sessions.Events(ctx, f.session, f.run, sessions.EventQuery{After: after, Floor: floor, Limit: 20,
			Filter: contract.EventFilter{Types: []string{}, Exclude: []string{}}})
		return err
	})
	require.NoError(t, err)
	return page
}

func TestE35HistoryGapAndFloorSelectRace(t *testing.T) {
	f := newFixture(t)
	bus := &natsx.Bus{Conn: f.bus.Conn, JS: fixtureJS{JetStream: f.bus.JS, stream: f.stream}}
	floor, err := natsx.NewEventFloor(bus)
	require.NoError(t, err)
	for n := range 3 {
		require.NoError(t, f.bus.Publish(t.Context(), f.subject, ids.New(), f.event(t, n+1, "run.status")))
	}
	first, second, third := f.next(t), f.next(t), f.next(t)
	store := blockedStore{eventStore: postgresStore{db: f.db}, entered: make(chan struct{}), release: make(chan struct{})}
	s := New(f.db, f.consumer, f.logger, testMetrics(t))
	s.store = store
	done := make(chan struct{})
	go func() {
		s.process(t.Context(), first)
		close(done)
	}()
	<-store.entered
	s.process(t.Context(), third)
	s.process(t.Context(), second)
	require.Equal(t, 2, f.count(t, "run_events"))
	b, err := floor.Floor(t.Context())
	require.NoError(t, err)
	require.Zero(t, b)
	page := fixturePage(t, f, 0, b)
	require.Empty(t, page.Items, "later committed rows must remain hidden behind the gap")
	require.Equal(t, "0", page.ResumeFrom)

	pub, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	signer, err := auth.NewStreamSigner(key, "test", map[string][]byte{"test": pub})
	require.NoError(t, err)
	token, err := signer.Issue(auth.StreamClaims{Scope: "run", TenantID: f.tenant, SessionID: f.session, RunID: f.run})
	require.NoError(t, err)
	gateway, err := streamgw.New(bus, signer.StreamVerifier, streamgw.Config{
		MaxConnections: 10, PerTenant: 10, PerSubject: 10, BufferFrames: 16, BufferBytes: 1 << 20,
		Heartbeat: time.Second, WriteTimeout: time.Second, MaxAge: time.Minute, InfoCache: time.Second,
	}, f.logger)
	require.NoError(t, err)
	httpServer := httptest.NewServer(gateway.Router())
	defer httpServer.Close()
	r, err := http.NewRequestWithContext(t.Context(), "GET", httpServer.URL+"/v1/runs/"+f.run+"/events", nil)
	require.NoError(t, err)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Last-Event-ID", page.ResumeFrom)
	response, err := (&http.Client{Timeout: 3 * time.Second}).Do(r)
	require.NoError(t, err)
	require.Equal(t, 200, response.StatusCode)
	scan := bufio.NewScanner(response.Body)
	var seqs []int
	for len(seqs) < 3 && scan.Scan() {
		if strings.HasPrefix(scan.Text(), "id: ") {
			n, err := strconv.Atoi(strings.TrimPrefix(scan.Text(), "id: "))
			require.NoError(t, err)
			seqs = append(seqs, n)
		}
	}
	require.Equal(t, []int{1, 2, 3}, seqs, "SSE resumes before the blocked row and fills the entire gap")
	response.Body.Close()
	close(store.release)
	<-done
	require.Eventually(t, func() bool {
		info, err := f.consumer.Info(t.Context())
		return err == nil && info.AckFloor.Stream == 3
	}, time.Second, time.Millisecond)
	fresh, err := natsx.NewEventFloor(bus)
	require.NoError(t, err)
	b, err = fresh.Floor(t.Context())
	require.NoError(t, err)
	require.EqualValues(t, 3, b)
	require.Len(t, fixturePage(t, f, 0, b).Items, 3)
	// Simulate a commit+Ack precisely between the floor read above and SELECT.
	require.NoError(t, f.bus.Publish(t.Context(), f.subject, "race", f.event(t, 4, "run.status")))
	s.store = postgresStore{db: f.db}
	s.process(t.Context(), f.next(t))
	page = fixturePage(t, f, 0, b)
	require.Len(t, page.Items, 3, "a newer committed row must not leak into the old floor's page")
	require.Equal(t, "3", page.ResumeFrom)
	require.Eventually(t, func() bool {
		info, err := f.consumer.Info(t.Context())
		return err == nil && info.AckFloor.Stream == 4
	}, time.Second, time.Millisecond)
	newFloor, err := natsx.NewEventFloor(bus)
	require.NoError(t, err)
	b, err = newFloor.Floor(t.Context())
	require.NoError(t, err)
	require.Len(t, fixturePage(t, f, 3, b).Items, 1, "the concurrent row remains available on the next read")
}

func TestE35TerminalMarkerFailureRelayRecovery(t *testing.T) {
	for _, status := range []string{"succeeded", "failed"} {
		t.Run(status, func(t *testing.T) {
			f := newFixture(t)
			_, err := f.admin.Exec(t.Context(), `UPDATE runs SET created_by=$2 WHERE id=$1`, f.run, f.workspace)
			require.NoError(t, err)
			var subjects, idsUsed []string
			failMarker := true
			publisher := publishFunc(func(ctx context.Context, subject, id string, raw []byte) error {
				subjects = append(subjects, subject)
				idsUsed = append(idsUsed, id)
				if strings.HasSuffix(subject, ".end") && failMarker {
					return errors.New("marker unavailable")
				}
				return f.bus.Publish(ctx, subject, id, raw)
			})
			activities := &ctl.Activities{DB: f.db, NATS: publisher, Logger: f.logger}
			in := w.FinalizeInput{RunOp: w.RunOp{TenantID: f.tenant, SessionID: f.session, RunID: f.run,
				Member: "main", EventN: 1, EventAt: time.Now().UTC()}, Status: status, ErrorCode: "TEST"}
			require.NoError(t, activities.FinalizeRun(t.Context(), in))
			require.Equal(t, []string{f.subject, f.subject + ".end"}, subjects)
			require.Equal(t, "end:"+f.run, idsUsed[1])
			var pending bool
			err = f.admin.QueryRow(t.Context(), `SELECT published_at IS NULL FROM control_events WHERE tenant_id=$1`, f.tenant).Scan(&pending)
			require.NoError(t, err)
			require.True(t, pending)
			// Keep the exact terminal payload and id, replacing only fixture creation age.
			_, err = f.admin.Exec(t.Context(), `WITH old AS (DELETE FROM control_events WHERE tenant_id=$1 RETURNING *)
 INSERT INTO control_events(tenant_id,run_id,event_id,operation,payload,created_at)
 SELECT tenant_id,run_id,event_id,operation,payload,now()-interval '31 seconds' FROM old`, f.tenant)
			require.NoError(t, err)
			failMarker = false
			require.NoError(t, (&Outbox{DB: f.db, Publisher: publisher, Logger: f.logger}).Republish(t.Context()))
			require.Equal(t, []string{f.subject, f.subject + ".end", f.subject, f.subject + ".end"}, subjects)
			require.Equal(t, idsUsed[0], idsUsed[2], "relay retains the terminal dedup id")
			err = f.admin.QueryRow(t.Context(), `SELECT published_at IS NULL FROM control_events WHERE tenant_id=$1`, f.tenant).Scan(&pending)
			require.NoError(t, err)
			require.False(t, pending)
			terminal, marker := f.next(t), f.next(t)
			tmeta, err := terminal.Metadata()
			require.NoError(t, err)
			mmeta, err := marker.Metadata()
			require.NoError(t, err)
			require.EqualValues(t, 1, tmeta.Sequence.Stream, "republication deduplicates to the original seq")
			require.EqualValues(t, 2, mmeta.Sequence.Stream)
			var body map[string]string
			require.NoError(t, json.Unmarshal(marker.Data(), &body))
			require.Equal(t, f.run, body["run_id"])
			require.Equal(t, idsUsed[0], body["event_id"])
			s := New(f.db, f.consumer, f.logger, testMetrics(t))
			s.process(t.Context(), terminal)
			s.process(t.Context(), marker)
			require.Equal(t, 1, f.count(t, "run_events"))
			require.Zero(t, f.count(t, "run_event_rejects"))
			require.Eventually(t, func() bool {
				info, err := f.consumer.Info(t.Context())
				return err == nil && info.AckFloor.Stream == 2
			}, time.Second, time.Millisecond)
		})
	}
}

func TestE35HealthyPostgresAfterFifteenDeliveries(t *testing.T) {
	f := newFixture(t)
	require.NoError(t, f.bus.Publish(t.Context(), f.subject, "many-deliveries", f.event(t, 1, "run.status")))
	for range 14 {
		msg := f.next(t)
		require.NoError(t, msg.Nak())
	}
	msg := f.next(t)
	meta, err := msg.Metadata()
	require.NoError(t, err)
	require.EqualValues(t, 15, meta.NumDelivered)
	New(f.db, f.consumer, f.logger, testMetrics(t)).process(t.Context(), msg)
	require.Equal(t, 1, f.count(t, "run_events"))
	require.Zero(t, f.count(t, "run_event_rejects"))
	require.Eventually(t, func() bool {
		info, err := f.consumer.Info(t.Context())
		return err == nil && info.AckFloor.Stream == 1
	}, time.Second, time.Millisecond)
}

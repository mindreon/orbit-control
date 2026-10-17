package integration

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/tenancy"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type outboxPublisher struct {
	fail     bool
	subjects []string
}

func (p *outboxPublisher) Publish(_ context.Context, subject, _ string, _ []byte) error {
	p.subjects = append(p.subjects, subject)
	if p.fail {
		return errors.New("unavailable")
	}
	return nil
}
func testStreamOutbox(t *testing.T, d *db.DB, admin *pgx.Conn) {
	t.Run("OrderedPublishFailure", func(t *testing.T) { testOrderedPublishFailure(t, d, admin) })
	f := newRunFixture(t, d, admin)
	bus := &outboxPublisher{fail: true}
	f.activities.NATS = bus
	f.activities.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	f.start(t)
	require.Len(t, bus.subjects, 2)
	for _, subject := range bus.subjects {
		require.Equal(t, w.EventSubject(f.tenant, f.session, f.run), subject)
	}
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var total, pending int
		require.NoError(t, tx.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE published_at IS NULL)
 FROM control_events WHERE run_id=$1`, f.run).Scan(&total, &pending))
		require.Equal(t, 1, total)
		require.Equal(t, total, pending)
		return nil
	})
	// Exercise the SECURITY DEFINER scan without a tenant context; recent rows are excluded.
	var count int
	require.NoError(t, d.Global(t.Context(), func(tx pgx.Tx) error {
		return tx.QueryRow(t.Context(), `SELECT count(*) FROM control_events_unpublished(500) WHERE run_id=$1`, f.run).Scan(&count)
	}))
	require.Zero(t, count)
	// The trigger also protects against an accidental broad UPDATE grant.
	_, err := admin.Exec(t.Context(), `UPDATE control_events SET created_at=now()-interval '1 minute' WHERE run_id=$1`, f.run)
	require.Error(t, err)
	// A fixture row can be inserted old; stored event contents remain immutable.
	_, err = admin.Exec(t.Context(), `INSERT INTO control_events(tenant_id,run_id,event_id,operation,payload,created_at)
 SELECT tenant_id,run_id,event_id||'0',operation,jsonb_set(payload,'{id}',to_jsonb(event_id||'0')),
 now()-interval '1 minute' FROM control_events WHERE run_id=$1`, f.run)
	require.NoError(t, err)
	require.NoError(t, d.Global(t.Context(), func(tx pgx.Tx) error {
		return tx.QueryRow(t.Context(), `SELECT count(*) FROM control_events_unpublished(500) WHERE run_id=$1`, f.run).Scan(&count)
	}))
	require.Equal(t, 1, count)
	err = d.WithTenant(t.Context(), f.tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(t.Context(), `UPDATE control_events SET payload='{}' WHERE run_id=$1`, f.run)
		return err
	})
	require.Error(t, err)
	bus.fail = false
	require.NoError(t, f.activities.SetRunStatus(t.Context(), w.StatusInput{RunOp: f.op(), Status: "paused"}))
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var marked bool
		require.NoError(t, tx.QueryRow(ctx, `SELECT published_at IS NOT NULL FROM control_events WHERE run_id=$1 AND operation=$2`,
			f.run, f.operation).Scan(&marked))
		require.True(t, marked)
		return nil
	})
	// A failed publication marker also leaves the successful business mutation committed.
	_, err = admin.Exec(t.Context(), `REVOKE UPDATE (published_at) ON control_events FROM orbit_app`)
	require.NoError(t, err)
	defer func() {
		_, err := admin.Exec(t.Context(), `GRANT UPDATE (published_at) ON control_events TO orbit_app`)
		require.NoError(t, err)
	}()
	require.NoError(t, f.activities.SetRunStatus(t.Context(), w.StatusInput{RunOp: f.op(), Status: "running"}))
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var pending bool
		require.NoError(t, tx.QueryRow(ctx, `SELECT published_at IS NULL FROM control_events WHERE run_id=$1 AND operation=$2`,
			f.run, f.operation).Scan(&pending))
		require.True(t, pending)
		return nil
	})
	_, err = admin.Exec(t.Context(), `GRANT UPDATE (published_at) ON control_events TO orbit_app`)
	require.NoError(t, err)
	// A tenant cannot mark another tenant's outbox, nor change its own event identity.
	other := newRunFixture(t, d, admin)
	require.NoError(t, d.WithTenant(t.Context(), other.tenant, func(tx pgx.Tx) error {
		result, err := tx.Exec(t.Context(), `UPDATE control_events SET published_at=now() WHERE run_id=$1`, f.run)
		require.Zero(t, result.RowsAffected())
		return err
	}))
}
func testOrderedPublishFailure(t *testing.T, d *db.DB, admin *pgx.Conn) {
	f := newRunFixture(t, d, admin)
	f.start(t)
	raw, err := os.ReadFile("../contract/v2/examples/SegmentResult.completed.json")
	require.NoError(t, err)
	var result contract.SegmentResult
	require.NoError(t, json.Unmarshal(raw, &result))
	result.Final.MessageId = ids.New()
	result.Final.ContentRef = nil
	artifact := result.Artifacts[0]
	result.Artifacts = nil
	for _, path := range []string{"first.md", "second.md"} {
		artifact.Path = path
		artifact.ObjectRef = "t/" + f.tenant + "/artifacts/" + f.run + "/" + path
		result.Artifacts = append(result.Artifacts, artifact)
	}
	bus := &outboxPublisher{fail: true}
	f.activities.NATS = bus
	var logs bytes.Buffer
	f.activities.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	op := f.op()
	_, err = f.activities.RecordSegment(t.Context(), w.RecordSegmentInput{RunOp: op, Result: result})
	require.NoError(t, err, "publication failure must not fail the Activity")
	require.Equal(t, []string{w.EventSubject(f.tenant, f.session, f.run)}, bus.subjects)
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var total, pending int
		var firstID, status string
		require.NoError(t, tx.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE published_at IS NULL), min(event_id)
 FROM control_events WHERE run_id=$1 AND operation=$2`, f.run, op.EventN).Scan(&total, &pending, &firstID))
		require.Equal(t, 2, total)
		require.Equal(t, total, pending)
		require.NoError(t, tx.QueryRow(ctx, `SELECT status FROM run_segments WHERE run_id=$1`, f.run).Scan(&status))
		require.Equal(t, "completed", status)
		for _, field := range []string{"run_id=" + f.run, "event_id=" + firstID, "operation=" + strconv.FormatInt(op.EventN, 10),
			"error_type=*errors.errorString"} {
			require.Contains(t, logs.String(), field)
		}
		require.NotContains(t, logs.String(), "unavailable")
		return nil
	})
}

func testStreamTokens(t *testing.T, d *db.DB, admin *pgx.Conn) {
	f := newRunFixture(t, d, admin)
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		return tenancy.Bind(ctx, tx, auth.From(ctx), f.tenant, "user", f.actor, "session.owner", "session", f.session)
	})
	c := config.Config{
		AuthMode: "dev", KeyFile: t.TempDir() + "/auth.key", CapabilityKeyFile: t.TempDir() + "/cap.key", CapabilityKeyID: "test",
		StreamKeyFile: t.TempDir() + "/stream.key", StreamKeyID: "test", SecretsKeyVersion: 1, SecretsKEKs: map[int][]byte{1: make([]byte, 32)}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	a, err := auth.New(t.Context(), d, c, logger)
	require.NoError(t, err)
	defer a.Close()
	token, err := a.Signer.DevToken(auth.Principal{ID: f.actor, Kind: "user"})
	require.NoError(t, err)
	router := (&httpapi.Handler{DB: d, Auth: a, Config: c, Logger: logger}).Router()
	request := func(path, tenant string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("X-Orbit-Tenant", tenant)
		out := httptest.NewRecorder()
		router.ServeHTTP(out, req)
		return out
	}
	for _, scope := range []string{"run", "session"} {
		path := "/v1/runs/" + f.run + "/stream-token"
		if scope == "session" {
			path = "/v1/sessions/" + f.session + "/stream-token"
		}
		out := request(path, f.tenant)
		require.Equal(t, 200, out.Code, out.Body.String())
		var response struct {
			Token   string `json:"stream_token"`
			Expires int    `json:"expires_in"`
		}
		require.NoError(t, json.Unmarshal(out.Body.Bytes(), &response))
		claims, err := a.Stream.Verify(response.Token)
		require.NoError(t, err)
		require.Equal(t, scope, claims.Scope)
		require.Equal(t, f.tenant, claims.TenantID)
		if scope == "run" {
			require.Equal(t, f.run, claims.RunID)
			require.Equal(t, "run:"+f.run, claims.Subject)
		} else {
			require.Empty(t, claims.RunID)
			require.Equal(t, "session:"+f.session, claims.Subject)
		}
		require.Equal(t, f.session, claims.SessionID)
		require.Equal(t, 900, response.Expires)
		require.WithinDuration(t, time.Now().Add(15*time.Minute), claims.ExpiresAt.Time, 2*time.Second)
	}
	// Membership in the tenant grants no access to this particular session.
	outsider := ids.New()
	_, err = admin.Exec(t.Context(), `INSERT INTO principals(id,kind,email,display_name,idp_subject)
 VALUES($1,'user','outsider@example.test','Outsider',$1::uuid::text)`, outsider)
	require.NoError(t, err)
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO memberships(tenant_id,principal_id,status,source) VALUES($1,$2,'active','direct')`,
			f.tenant, outsider)
		return err
	})
	ownerToken := token
	token, err = a.Signer.DevToken(auth.Principal{ID: outsider, Kind: "user"})
	require.NoError(t, err)
	for _, path := range []string{"/v1/sessions/" + f.session + "/stream-token", "/v1/runs/" + f.run + "/stream-token"} {
		require.Equal(t, 404, request(path, f.tenant).Code)
	}
	token = ownerToken
	other := newRunFixture(t, d, admin)
	require.Equal(t, 404, request("/v1/sessions/"+other.session+"/stream-token", f.tenant).Code)
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE sessions SET status='archived' WHERE id=$1`, f.session)
		return err
	})
	require.Equal(t, 409, request("/v1/sessions/"+f.session+"/stream-token", f.tenant).Code)
	require.Equal(t, 409, request("/v1/runs/"+f.run+"/stream-token", f.tenant).Code)
}

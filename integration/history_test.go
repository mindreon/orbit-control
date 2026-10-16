package integration

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/sessions"
	"github.com/mindreon/orbit-control/internal/tenancy"
)

func testHistory(t *testing.T, d *db.DB, admin *pgx.Conn) {
	f := newRunFixture(t, d, admin)
	otherRun := ids.New()
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		if err := tenancy.Bind(ctx, tx, auth.From(ctx), f.tenant, "user", f.actor, "session.owner", "session", f.session); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO runs(id,tenant_id,session_id,trigger,status)
 VALUES($1,$2,$3,'api','queued')`, otherRun, f.tenant, f.session)
		require.NoError(t, err)
		// Deliberate arrival disorder; the read order is always JetStream sequence.
		for _, seq := range []int{5, 1, 3} {
			run, kind := f.run, "run.started"
			if seq == 3 {
				run, kind = otherRun, "run.status"
			}
			payload := fmt.Appendf(nil, `{"v":1,"id":"%s:main:0:0:%d","tenant_id":"%s","session_id":"%s",
 "run_id":"%s","member":"main","segment":0,"attempt":0,"ts":"2026-10-16T00:00:00Z","type":"%s","data":{}}`,
				run, seq, f.tenant, f.session, run, kind)
			_, err := tx.Exec(ctx, `INSERT INTO run_events
 (tenant_id,run_id,session_id,member,segment,attempt,event_id,stream_seq,type,payload,ts)
 VALUES($1,$2,$3,'main',0,0,$4,$5,$6,$7,$8)`, f.tenant, run, f.session, fmt.Sprint(seq), seq, kind, payload, time.Now())
			require.NoError(t, err)
		}
		return nil
	})
	c := config.Config{AuthMode: "dev", KeyFile: t.TempDir() + "/auth.key", CapabilityKeyFile: t.TempDir() + "/cap.key",
		CapabilityKeyID: "test", StreamKeyFile: t.TempDir() + "/stream.key", StreamKeyID: "test",
		SecretsKeyVersion: 1, SecretsKEKs: map[int][]byte{1: make([]byte, 32)}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	a, err := auth.New(t.Context(), d, c, logger)
	require.NoError(t, err)
	defer a.Close()
	token, err := a.Signer.DevToken(auth.Principal{ID: f.actor, Kind: "user"})
	require.NoError(t, err)
	router := (&httpapi.Handler{DB: d, Auth: a, Config: c, Logger: logger}).Router()
	request := func(path string, code int) sessions.EventPage {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Orbit-Tenant", f.tenant)
		out := httptest.NewRecorder()
		router.ServeHTTP(out, r)
		require.Equal(t, code, out.Code, out.Body.String())
		var page sessions.EventPage
		if code == 200 {
			require.NoError(t, json.Unmarshal(out.Body.Bytes(), &page))
		}
		return page
	}
	base := "/v1/sessions/" + f.session + "/history"
	first := request(base+"?limit=2", 200)
	require.Len(t, first.Items, 2)
	require.Equal(t, "3", first.ResumeFrom)
	require.Equal(t, "3", first.NextCursor)
	last := request(base+"?after="+first.NextCursor, 200)
	require.Len(t, last.Items, 1)
	require.Equal(t, "5", last.ResumeFrom)
	require.Empty(t, last.NextCursor)
	empty := request(base+"?after=999", 200)
	require.Empty(t, empty.Items)
	require.Equal(t, "999", empty.ResumeFrom)
	filtered := request(base+"?types=run.status", 200)
	require.Len(t, filtered.Items, 1)
	require.Equal(t, "3", filtered.ResumeFrom)
	require.Len(t, request(base+"?exclude=run.status", 200).Items, 2)
	require.Len(t, request("/v1/runs/"+f.run+"/history", 200).Items, 2)
	request(base+"?types=bad", 422)
	other := newRunFixture(t, d, admin)
	request("/v1/sessions/"+other.session+"/history", 404)
	request("/v1/runs/"+other.run+"/history", 404)
	outsider, err := a.Signer.DevToken(auth.Principal{ID: other.actor, Kind: "user"})
	require.NoError(t, err)
	token = outsider
	request(base, 403)
}

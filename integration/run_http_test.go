package integration

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/tenancy"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type runObjects struct{ testBlobs }

func (s runObjects) PutObject(ctx context.Context, tenant, key string, data []byte, mime string) error {
	return nil
}
func (s runObjects) GetObject(context.Context, string, string, int64) ([]byte, string, error) {
	return []byte(`{"text":"input"}`), "application/json", nil
}
func (s runObjects) PresignGet(context.Context, string, string, time.Duration) (string, error) {
	return "http://objects.invalid/file", nil
}
func (s runObjects) Head(context.Context, string, string) (int64, string, error) {
	return 0, "application/json", nil
}

type runResolver struct{ lock contract.RunLock }

func (s runResolver) Resolve(_ context.Context, in locks.Request) (contract.RunLock, error) {
	lock := s.lock
	lock.LockId = in.LockID
	raw, err := json.Marshal(lock)
	if err == nil {
		lock.Digest, err = contract.RunLockDigest(raw)
	}
	return lock, err
}

type runTemporal struct {
	active           string
	updates, submits int
	submitError      error
	signalError      error
}

func (c *runTemporal) QuerySession(context.Context, string) (w.SessionState, error) {
	return w.SessionState{ActiveRunID: c.active}, nil
}
func (c *runTemporal) Update(context.Context, string, string, string, any, any) error {
	c.updates++
	return temporal.NewApplicationError("already completed", "RUN_NOT_ACTIVE")
}
func (c *runTemporal) Submit(_ context.Context, _ w.SessionInput, msg w.SubmitMessageInput) (w.SubmitMessageOutput, error) {
	c.submits++
	return w.SubmitMessageOutput{RunID: msg.RunID, Disposition: "started"}, c.submitError
}
func (c *runTemporal) Signal(context.Context, string, string, any) error { return c.signalError }

func testRunHTTP(t *testing.T, d *db.DB, admin *pgx.Conn) {
	for _, mode := range []string{"fallback", "rejected", "duplicate", "lease-unavailable", "lease-missing"} {
		t.Run(mode, func(t *testing.T) {
			f := newRunFixture(t, d, admin)
			f.start(t)
			var lock contract.RunLock
			f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
				var raw []byte
				require.NoError(t, tx.QueryRow(ctx, `SELECT lock FROM run_locks WHERE id=$1`, f.lock).Scan(&raw))
				require.NoError(t, json.Unmarshal(raw, &lock))
				_, err := tx.Exec(ctx, `UPDATE sessions SET message_policy='steer' WHERE id=$1`, f.session)
				if err != nil {
					return err
				}
				return tenancy.Bind(ctx, tx, auth.From(ctx), f.tenant, "user", f.actor, "session.owner", "session", f.session)
			})
			// The fixture resolver intentionally omits bundle files: only admission and
			// Temporal delivery are under test here; bundle verification has its own suite.
			entry := lock.Entries[lock.Members[0].AgentEntry]
			entry.Files = nil
			lock.Entries[lock.Members[0].AgentEntry] = entry
			c := config.Config{
				AuthMode: "dev", KeyFile: t.TempDir() + "/auth.key", CapabilityKeyFile: t.TempDir() + "/cap.key",
				StreamKeyFile: t.TempDir() + "/stream.key", StreamKeyID: "test",
				CapabilityKeyID: "test", SecretsKeyVersion: 1, SecretsKEKs: map[int][]byte{1: make([]byte, 32)}}
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			a, err := auth.New(t.Context(), d, c, logger)
			require.NoError(t, err)
			defer a.Close()
			token, err := a.Signer.DevToken(auth.Principal{ID: f.actor, Kind: "user"})
			require.NoError(t, err)
			temporalClient := &runTemporal{active: f.run}
			if mode == "rejected" {
				temporalClient.submitError = temporal.NewApplicationError("queue full", "QUEUE_FULL")
			}
			if mode == "duplicate" {
				temporalClient.submitError = temporal.NewApplicationError("duplicate", "DUPLICATE_RUN")
			}
			if mode == "lease-unavailable" {
				temporalClient.signalError = serviceerror.NewUnavailable("offline")
			}
			if mode == "lease-missing" {
				temporalClient.signalError = serviceerror.NewNotFound("gone")
			}
			h := (&httpapi.Handler{DB: d, Auth: a, Config: c, Logger: logger, Temporal: temporalClient,
				Objects: runObjects{testBlobs: testBlobs{root: t.TempDir()}}, Locks: runResolver{lock: lock}}).Router()
			path := "/v1/sessions/" + f.session + "/messages"
			if strings.HasPrefix(mode, "lease-") {
				path = "/v1/sessions/" + f.session + "/archive"
			}
			request := httptest.NewRequest("POST", path, bytes.NewBufferString(`{"text":"new input"}`))
			request.Header.Set("Authorization", "Bearer "+token)
			request.Header.Set("X-Orbit-Tenant", f.tenant)
			request.Header.Set("Idempotency-Key", "run-http:"+mode)
			response := httptest.NewRecorder()
			h.ServeHTTP(response, request)
			want := http.StatusAccepted
			if mode == "rejected" {
				want = 409
			}
			if mode == "lease-unavailable" {
				want = 503
			}
			require.Equal(t, want, response.Code, response.Body.String())
			if mode == "duplicate" {
				var accepted w.SubmitMessageOutput
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &accepted))
				require.NotEmpty(t, accepted.RunID)
				require.Equal(t, "queued", accepted.Disposition)
				replay := httptest.NewRecorder()
				retry := httptest.NewRequest("POST", path, bytes.NewBufferString(`{"text":"new input"}`))
				retry.Header = request.Header.Clone()
				h.ServeHTTP(replay, retry)
				require.Equal(t, 202, replay.Code)
				require.JSONEq(t, response.Body.String(), replay.Body.String())
			}
			if strings.HasPrefix(mode, "lease-") {
				return
			}
			require.Equal(t, 1, temporalClient.updates)
			require.Equal(t, 1, temporalClient.submits)
			f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
				var messages, orphaned, queued int
				require.NoError(t, tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE run_id=$2)
 FROM messages
 WHERE tenant_id=$1
 AND role='user'`, f.tenant, f.run).Scan(&messages, &orphaned))
				require.Equal(t, 1, messages)
				require.Zero(t, orphaned)
				require.NoError(
					t,
					tx.QueryRow(ctx, `SELECT count(*) FROM runs WHERE tenant_id=$1 AND status='queued'`, f.tenant).Scan(&queued),
				)
				if mode == "rejected" {
					require.Zero(t, queued)
				} else {
					require.Equal(t, 1, queued)
				}
				return nil
			})
		})
	}
	// An approver can read only while the session permission remains effective.
	f := newRunFixture(t, d, admin)
	approver := ids.New()
	_, err := admin.Exec(t.Context(), `INSERT INTO principals(id,kind,email,display_name,idp_subject)
 VALUES($1,'user','approver@example.test','Approver',$1::uuid::text)`, approver)
	require.NoError(t, err)
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(
			ctx,
			`INSERT INTO memberships(tenant_id,principal_id,status,source) VALUES($1,$2,'active','direct')`,
			f.tenant,
			approver,
		)
		return err
	})
	f.start(t)
	req := contract.InteractionRequest{Kind: "ask_user", InteractionId: ids.New(), Question: &contract.Question{Text: "Ready?"}}
	op := f.op()
	_, err = f.activities.CreateInteraction(
		t.Context(),
		w.InteractionInput{RunOp: op, Request: req, ExpiresAt: op.EventAt.Add(time.Hour)},
	)
	require.NoError(t, err)
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(
			ctx,
			`UPDATE interactions SET approvers=jsonb_build_array($2::text) WHERE id=$1`,
			req.InteractionId,
			approver,
		)
		return err
	})
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		return tenancy.Bind(ctx, tx, auth.From(ctx), f.tenant, "user", approver, "session.viewer", "session", f.session)
	})
	c := config.Config{
		AuthMode: "dev", KeyFile: t.TempDir() + "/auth.key", CapabilityKeyFile: t.TempDir() + "/cap.key",
		StreamKeyFile: t.TempDir() + "/stream.key", StreamKeyID: "test",
		CapabilityKeyID: "test", SecretsKeyVersion: 1, SecretsKEKs: map[int][]byte{1: make([]byte, 32)}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	a, err := auth.New(t.Context(), d, c, logger)
	require.NoError(t, err)
	defer a.Close()
	token, err := a.Signer.DevToken(auth.Principal{ID: approver, Kind: "user"})
	require.NoError(t, err)
	h := (&httpapi.Handler{DB: d, Auth: a, Config: c, Logger: logger}).Router()
	for _, revoked := range []bool{false, true} {
		if revoked {
			f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `DELETE FROM role_bindings WHERE tenant_id=$1 AND principal_id=$2`, f.tenant, approver)
				return err
			})
		}
		for _, path := range []string{"/v1/interactions/" + req.InteractionId, "/v1/interactions?assignee=me"} {
			r := httptest.NewRequest("GET", path, nil)
			r.Header.Set("Authorization", "Bearer "+token)
			r.Header.Set("X-Orbit-Tenant", f.tenant)
			response := httptest.NewRecorder()
			h.ServeHTTP(response, r)
			want := 200
			if revoked && !strings.Contains(path, "?") {
				want = 404
			}
			require.Equal(t, want, response.Code, response.Body.String())
			if revoked {
				require.NotContains(t, response.Body.String(), req.InteractionId)
			} else {
				require.Contains(t, response.Body.String(), req.InteractionId)
			}
		}
	}
}

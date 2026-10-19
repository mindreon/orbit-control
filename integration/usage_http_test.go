package integration

import (
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
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/telemetry"
	"github.com/mindreon/orbit-control/internal/tenancy"
	"github.com/mindreon/orbit-control/internal/usage"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func publicUsageHandler(t *testing.T, f *runFixture, operators map[string]bool) (http.Handler, string) {
	t.Helper()
	c := config.Config{AuthMode: "dev", KeyFile: t.TempDir() + "/auth.key", CapabilityKeyFile: t.TempDir() + "/cap.key",
		StreamKeyFile: t.TempDir() + "/stream.key", StreamKeyID: "test", CapabilityKeyID: "test", PlatformOperators: operators,
		SecretsKeyVersion: 1, SecretsKEKs: map[int][]byte{1: make([]byte, 32)}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	a, err := auth.New(t.Context(), f.d, c, logger)
	require.NoError(t, err)
	t.Cleanup(a.Close)
	token, err := a.Signer.DevToken(auth.Principal{ID: f.actor, Kind: "user"})
	require.NoError(t, err)
	return (&httpapi.Handler{DB: f.d, Auth: a, Config: c, Logger: logger, Temporal: &runTemporal{},
		Objects: runObjects{testBlobs: testBlobs{root: t.TempDir()}}, Locks: f.activities.Sessions.Locks}).Router(), token
}

func publicRequest(t *testing.T, h http.Handler, token, tenant, method, path, body string) (int, map[string]any, string) {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-Orbit-Tenant", tenant)
	if method != "GET" {
		r.Header.Set("Idempotency-Key", ids.New())
		r.Header.Set("Content-Type", "application/json")
	}
	out := httptest.NewRecorder()
	h.ServeHTTP(out, r)
	var value map[string]any
	require.NoError(t, json.Unmarshal(out.Body.Bytes(), &value), out.Body.String())
	return out.Code, value, out.Body.String()
}

func TestReplayAuthorization(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	f := newRunFixture(t, d, admin)
	other := newRunFixture(t, d, admin)
	f.start(t)
	traceID := "0123456789abcdef0123456789abcdef"
	_, err := admin.Exec(t.Context(), "UPDATE runs SET trace_id=$2,temporal_workflow_id='private-workflow' WHERE id=$1", f.run, traceID)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), `INSERT INTO tool_effects(id,tenant_id,run_id,member,effect_key,tool,args_digest,effect_class,
 checkpoint_version,status,result_preview,result_ref)VALUES($1,$2,$3,'main','private-key','write','private-digest','external_write',1,
 'unknown','{"canary":"forbidden-preview-canary"}','private-ref')`, ids.New(), f.tenant, f.run)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), `INSERT INTO interactions(id,tenant_id,run_id,session_id,member,workflow_id,kind,request,
 approvers,status,expires_at)VALUES($1,$2,$3,$4,'main','private-workflow','ask_user',$5,'[]','pending',now()+interval '1 hour')`,
		ids.New(), f.tenant, f.run, f.session, []byte(`{"interaction_id":"fixture","kind":"ask_user","question":{"text":"fixture"}}`))
	require.NoError(t, err)
	for _, role := range []string{"none", "session.viewer"} {
		if role != "none" {
			f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
				return tenancy.Bind(ctx, tx, auth.From(ctx), f.tenant, "user", f.actor, role, "session", f.session)
			})
		}
		for _, operator := range []bool{false, true} {
			h, token := publicUsageHandler(t, f, map[string]bool{f.actor: operator})
			for _, run := range []string{f.run, other.run} {
				for _, suffix := range []string{"", "/segments", "/effects", "/interactions", "/cost"} {
					status, value, raw := publicRequest(t, h, token, f.tenant, "GET", "/v1/runs/"+run+suffix, "")
					want := 404
					if role != "none" && run == f.run {
						want = 200
					}
					require.Equal(t, want, status, raw)
					if status != 200 {
						continue
					}
					if suffix == "" {
						if operator {
							require.Equal(t, traceID, value["ops"].(map[string]any)["trace_id"])
						} else {
							for _, key := range []string{"ops", "trace_id", "temporal_workflow_id"} {
								require.NotContains(t, value, key)
							}
						}
					}
					if suffix == "/effects" {
						for _, bad := range []string{"result_preview", "result_ref", "args_digest", "effect_key", "forbidden-preview-canary"} {
							require.NotContains(t, raw, bad)
						}
					}
					if suffix == "/interactions" {
						require.NotContains(t, raw, "remind_at")
						require.Len(t, value["items"].([]any), 1)
					}
				}
			}
			status, value, raw := publicRequest(t, h, token, f.tenant, "GET", "/v1/me", "")
			require.Equal(t, 200, status, raw)
			require.Equal(t, operator, value["platform_operator"])
		}
	}
}

func TestUsageTokensAdmission(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	old := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	telemetry.InitializeMetrics()
	t.Cleanup(func() {
		otel.SetMeterProvider(old)
		_ = provider.Shutdown(context.Background())
	})
	d, admin, _ := rollupDatabase(t)
	for _, scope := range []string{"tenant", "workspace", "user"} {
		for _, action := range []string{"block", "warn"} {
			t.Run(scope+"-"+action, func(t *testing.T) {
				r := newScheduleRig(t, d, admin)
				scopeID := map[string]string{"tenant": r.tenant, "workspace": r.workspace, "user": r.actor}[scope]
				var policyID = ids.New()
				r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
					_, err := tx.Exec(ctx, `INSERT INTO quota_policies(id,tenant_id,scope_type,scope_id,metric,"limit",period,action)
 VALUES($1,$2,$3,$4,'tokens',20,'total',$5)`, policyID, r.tenant, scope, scopeID, action)
					if err != nil {
						return err
					}
					for _, meter := range usage.Meters() {
						_, err = usage.RecordUsage(ctx, tx, usage.Entry{TenantID: r.tenant,
							WorkspaceID: &r.workspace, PrincipalID: &r.actor, Meter: meter, Source: "model", Quantity: "5", UnitPrice: "1",
							IdempotencyKey: string(meter), OccurredAt: time.Now()})
						if err != nil {
							return err
						}
					}
					return nil
				})
				h, token := publicUsageHandler(t, r.runFixture, nil)
				before := metricTotal(t, reader, "orbit.quota.rejections")
				status, _, raw := publicRequest(t, h, token, r.tenant, "POST", "/v1/sessions/"+r.session+"/messages", `{"text":"new"}`)
				want := 202
				if action == "block" {
					want = 429
				}
				require.Equal(t, want, status, raw)
				if action == "block" {
					require.Equal(t, before+1, metricTotal(t, reader, "orbit.quota.rejections"))
					require.Contains(t, raw, "QUOTA_EXCEEDED")
				}
				row := r.create(t)
				fire := time.Now().UTC().Truncate(time.Second)
				out, err := r.activities.CreateScheduledSession(t.Context(),
					w.ScheduledSessionInput{TenantID: r.tenant, ScheduleID: row.ID, FireAt: fire})
				require.NoError(t, err)
				if action == "block" {
					require.Equal(t, "QUOTA_EXCEEDED", out.Skip)
					var status, code, schedule string
					require.NoError(t, admin.QueryRow(t.Context(), `SELECT r.status,r.error_code,s.status FROM schedule_runs r
 JOIN schedules s ON s.id=r.schedule_id WHERE r.schedule_id=$1`, row.ID).Scan(&status, &code, &schedule))
					require.Equal(t, "failed", status)
					require.Equal(t, "QUOTA_EXCEEDED", code)
					require.Equal(t, "active", schedule)
					require.Equal(t, before+2, metricTotal(t, reader, "orbit.quota.rejections"))
					var runs int
					require.NoError(t, admin.QueryRow(t.Context(), "SELECT count(*) FROM runs WHERE tenant_id=$1", r.tenant).Scan(&runs))
					require.Equal(t, 1, runs)
				} else {
					require.Equal(t, before, metricTotal(t, reader, "orbit.quota.rejections"))
					require.Empty(t, out.Skip)
					require.NotNil(t, out.Run)
				}
				var ratio float64
				require.NoError(t, admin.QueryRow(
					t.Context(), "SELECT ratio::float8 FROM ops_quota_utilization() WHERE policy_id=$1",
					policyID).Scan(&ratio))
				require.Equal(t, float64(1), ratio)
				_, err = admin.Exec(t.Context(), `UPDATE quota_policies SET "limit"=21 WHERE id=$1`, policyID)
				require.NoError(t, err)
				status, _, raw = publicRequest(t, h, token, r.tenant, "POST", "/v1/sessions/"+r.session+"/messages", `{"text":"allowed"}`)
				require.Equal(t, 202, status, raw)
			})
		}
	}
}

func TestUsageReports(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	for _, role := range []string{"tenant.member", "tenant.auditor", "ws.admin", "ws.viewer"} {
		t.Run(role, func(t *testing.T) {
			f := newRunFixture(t, d, admin)
			f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
				scope, id := "tenant", f.tenant
				if strings.HasPrefix(role, "ws.") {
					scope, id = "workspace", f.workspace
				}
				return tenancy.Bind(ctx, tx, auth.From(ctx), f.tenant, "user", f.actor, role, scope, id)
			})
			h, token := publicUsageHandler(t, f, nil)
			hour := time.Now().UTC().Truncate(time.Hour).Add(-2 * time.Hour)
			profile := ids.New()
			_, err := admin.Exec(t.Context(), `INSERT INTO model_profiles(id,tenant_id,name,provider,base_url,model)
 VALUES($1,$2,'profile-fixture','fixture','https://fixture.invalid','fixture-model')`, profile, f.tenant)
			require.NoError(t, err)
			for _, meter := range usage.Meters() {
				f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
					_, err := (usage.Service{Now: func() time.Time { return hour }}).RecordUsage(ctx, tx, usage.Entry{TenantID: f.tenant,
						WorkspaceID: &f.workspace, PrincipalID: &f.actor, RunID: &f.run, Member: new("main"), ModelProfileID: &profile,
						Source: "model", Meter: meter, Quantity: "5", UnitPrice: "1", Millicredits: 2, IdempotencyKey: string(meter), OccurredAt: hour})
					return err
				})
			}
			require.NoError(t, usage.RollupHour(t.Context(), d, usage.DirtyHour{TenantID: f.tenant, Hour: hour}, nil))
			_, err = recordHour(t.Context(), d, f.tenant, "dirty", hour.Add(time.Hour), hour.Add(time.Hour))
			require.NoError(t, err)
			times := "&from=" + hour.Format(time.RFC3339) + "&to=" + hour.Add(3*time.Hour).Format(time.RFC3339)
			for _, scope := range []string{"tenant", "workspace", "user", "other-user"} {
				query := "/v1/usage?scope=" + scope + times
				allowed := role == "tenant.auditor" || scope == "user" || role == "ws.admin" && scope == "workspace"
				if scope == "workspace" {
					query += "&id=" + f.workspace
				}
				if scope == "other-user" {
					query = "/v1/usage?scope=user&id=" + ids.New() + times
				}
				status, _, raw := publicRequest(t, h, token, f.tenant, "GET", query, "")
				want := 403
				if allowed {
					want = 200
				}
				require.Equal(t, want, status, raw)
			}
			for _, groups := range []string{"", "meter", "workspace,principal,asset,model_profile,member,source,meter"} {
				status, value, raw := publicRequest(t, h, token, f.tenant, "GET", "/v1/usage?scope=user"+times+"&group_by="+groups, "")
				require.Equal(t, 200, status, raw)
				require.Equal(t, float64(1), value["dirty_hours"])
				computed, err := time.Parse(time.RFC3339, value["computed_through"].(string))
				require.NoError(t, err)
				require.True(t, hour.Add(time.Hour).Equal(computed))
				items := value["items"].([]any)
				if groups == "" {
					require.Len(t, items, 1)
					require.Nil(t, items[0].(map[string]any)["quantity"])
				} else {
					require.Len(t, items, 4)
					for _, rawItem := range items {
						item := rawItem.(map[string]any)
						require.Equal(t, "5", item["quantity"])
						if strings.Contains(groups, "model_profile") {
							require.Equal(t, "profile-fixture", item["model_profile_name"])
							require.Nil(t, item["asset_ref"])
						}
					}
				}
			}
			for _, query := range []string{
				"scope=workspace",
				"scope=tenant&id=" + f.tenant,
				"scope=user&from=2026-01-01T00:01:00Z",
				"scope=user&from=2025-01-01T00:00:00Z&to=2026-01-01T00:00:00Z",
				"scope=user&group_by=invalid",
			} {
				status, _, raw := publicRequest(t, h, token, f.tenant, "GET", "/v1/usage?"+query, "")
				require.Equal(t, 422, status, raw)
			}
		})
	}
}

package integration

import (
	"context"
	"encoding/json/v2"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/schedules"
	"github.com/mindreon/orbit-control/internal/sessions"
	"github.com/mindreon/orbit-control/internal/tenancy"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

// scheduleClient records the actual Temporal writes, not just the DB projection.
// Hooks execute outside its mutex so a blocked write can race a committed PATCH.
type scheduleClient struct {
	client.ScheduleClient
	mu                       sync.Mutex
	exists                   bool
	fail                     bool
	writes                   []string
	pauses, deletes, creates int
	onCall                   func()
	onUpdate                 func()
}
type scheduleHandle struct {
	client.ScheduleHandle
	c *scheduleClient
}

func (c *scheduleClient) GetHandle(context.Context, string) client.ScheduleHandle {
	return &scheduleHandle{c: c}
}
func (c *scheduleClient) call() {
	if c.onCall != nil {
		c.onCall()
	}
}
func (c *scheduleClient) Create(_ context.Context, in client.ScheduleOptions) (client.ScheduleHandle, error) {
	c.call()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.creates++
	if c.fail {
		return nil, errors.New("schedule service unavailable")
	}
	c.exists = true
	c.writes = append(c.writes, in.Spec.CronExpressions[0])
	return &scheduleHandle{c: c}, nil
}
func (h *scheduleHandle) Describe(context.Context) (*client.ScheduleDescription, error) {
	h.c.call()
	h.c.mu.Lock()
	defer h.c.mu.Unlock()
	if !h.c.exists {
		return nil, serviceerror.NewNotFound("missing")
	}
	return &client.ScheduleDescription{}, nil
}
func (h *scheduleHandle) Update(_ context.Context, in client.ScheduleUpdateOptions) error {
	h.c.call()
	if h.c.onUpdate != nil {
		h.c.onUpdate()
	}
	update, err := in.DoUpdate(client.ScheduleUpdateInput{})
	if err != nil {
		return err
	}
	h.c.mu.Lock()
	defer h.c.mu.Unlock()
	h.c.writes = append(h.c.writes, update.Schedule.Spec.CronExpressions[0])
	return nil
}
func (h *scheduleHandle) Pause(context.Context, client.SchedulePauseOptions) error {
	h.c.call()
	h.c.mu.Lock()
	defer h.c.mu.Unlock()
	h.c.pauses++
	return nil
}
func (h *scheduleHandle) Unpause(context.Context, client.ScheduleUnpauseOptions) error {
	h.c.call()
	return nil
}
func (h *scheduleHandle) Delete(context.Context) error {
	h.c.call()
	h.c.mu.Lock()
	defer h.c.mu.Unlock()
	h.c.deletes++
	h.c.exists = false
	return nil
}

type scheduleRig struct {
	*runFixture
	admin  *pgx.Conn
	svc    schedules.Service
	client *scheduleClient
	input  schedules.CreateInput
}

func newScheduleRig(t *testing.T, d *db.DB, admin *pgx.Conn) *scheduleRig {
	t.Helper()
	f := newRunFixture(t, d, admin)
	var lock contract.RunLock
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var raw []byte
		if err := tx.QueryRow(ctx, `SELECT lock FROM run_locks WHERE id=$1`, f.lock).Scan(&raw); err != nil {
			return err
		}
		if err := json.Unmarshal(raw, &lock); err != nil {
			return err
		}
		return tenancy.Bind(ctx, tx, auth.From(ctx), f.tenant, "user", f.actor, "ws.editor", "workspace", f.workspace)
	})
	// Submission tests use a verified fixture lock; bundle resolution is covered separately.
	for i := range lock.Entries {
		lock.Entries[i].Files = nil
	}
	c := &scheduleClient{}
	r := &scheduleRig{runFixture: f, admin: admin, client: c, svc: schedules.Service{DB: d, Locks: runResolver{lock: lock}, Temporal: c},
		input: schedules.CreateInput{WorkspaceID: f.workspace, Name: "Scheduled work", Cron: "0 * * * *", Timezone: "UTC",
			EntryKind: "agent", EntryRef: lock.Entry.Ref, Prompt: "Do the scheduled work", BudgetMillicredits: 200000}}
	f.activities.Sessions = sessions.Service{Locks: r.svc.Locks, Objects: runObjects{testBlobs: testBlobs{root: t.TempDir()}}}
	f.activities.Schedules = &r.svc
	return r
}
func (r *scheduleRig) create(t *testing.T) schedules.Schedule {
	t.Helper()
	var row schedules.Schedule
	r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		row, err = r.svc.Create(ctx, ids.New(), r.input)
		return err
	})
	return row
}
func (r *scheduleRig) row(t *testing.T, id string) schedules.Schedule {
	t.Helper()
	var row schedules.Schedule
	r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var raw []byte
		if err := tx.QueryRow(ctx, `SELECT to_jsonb(s) FROM schedules s WHERE tenant_id=$1 AND id=$2`, r.tenant, id).Scan(&raw); err != nil {
			return err
		}
		return json.Unmarshal(raw, &row)
	})
	return row
}
func (r *scheduleRig) patch(t *testing.T, id string, in schedules.PatchInput) schedules.Schedule {
	t.Helper()
	var row schedules.Schedule
	r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var err error
		row, err = r.svc.Patch(ctx, id, in)
		return err
	})
	return row
}
func scheduleCode(t *testing.T, err error, code string) {
	t.Helper()
	var he *httpx.Error
	require.ErrorAs(t, err, &he)
	require.Equal(t, code, he.Code)
}

func testSchedules(t *testing.T, d *db.DB, admin *pgx.Conn) {
	t.Run("T-P6/Firing", func(t *testing.T) {
		r := newScheduleRig(t, d, admin)
		row := r.create(t)
		now := time.Now().UTC().Truncate(time.Second)
		in := w.ScheduledSessionInput{TenantID: r.tenant, ScheduleID: row.ID, FireAt: now, Now: now}
		first, err := r.activities.CreateScheduledSession(t.Context(), in)
		require.NoError(t, err)
		require.NotNil(t, first.Run)
		require.True(t, first.Run.Unattended)
		again, err := r.activities.CreateScheduledSession(t.Context(), in)
		require.NoError(t, err)
		require.Equal(t, first, again)
		// BuildStart on the same admitted session reads the saved immutable lock.
		r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
			s := sessions.Session{ID: first.Run.SessionID, WorkspaceID: r.workspace, OwnerID: r.actor, EntryKind: "agent",
				EntryRef: r.input.EntryRef}
			_, _, message := sessions.ScheduledIDs(row.ID, now.Format(time.RFC3339))
			msg, err := r.activities.Sessions.BuildStart(ctx, tx, s, auth.From(ctx), first.Run.RunID, message, r.input.Prompt,
				&r.input.BudgetMillicredits)
			if err != nil {
				return err
			}
			want := w.NewRunInput(w.SessionInput{V: 1, TenantID: r.tenant, WorkspaceID: r.workspace, SessionID: s.ID, OwnerID: r.actor,
				Policy: "queue"}, msg, first.Run.EventSubject)
			want.Unattended = true
			require.Equal(t, want, *first.Run)
			return nil
		})
		for _, tc := range []struct {
			age  int
			skip string
		}{{age: 3719, skip: ""}, {age: 3721, skip: "STALE_FIRE"}} {
			in.FireAt = now.Add(-time.Duration(tc.age) * time.Second)
			out, err := r.activities.CreateScheduledSession(t.Context(), in)
			require.NoError(t, err)
			require.Equal(t, tc.skip, out.Skip)
		}
		// Revocation pauses and versions the schedule, then synchronizes Pause.
		r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `DELETE FROM role_bindings WHERE tenant_id=$1 AND scope_id=$2`, r.tenant, r.workspace)
			return err
		})
		in.FireAt = now.Add(time.Hour)
		in.Now = in.FireAt
		out, err := r.activities.CreateScheduledSession(t.Context(), in)
		require.NoError(t, err)
		require.Equal(t, "OWNER_UNAUTHORIZED", out.Skip)
		paused := r.row(t, row.ID)
		require.Equal(t, "paused", paused.Status)
		require.Equal(t, row.Version+1, paused.Version)
		require.Equal(t, paused.Version, paused.SyncedVersion)
		require.Positive(t, r.client.pauses)
	})
	t.Run("T-P6/QuotaDoesNotPause", func(t *testing.T) {
		r := newScheduleRig(t, d, admin)
		row := r.create(t)
		r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx,
				`INSERT INTO quota_policies(id,tenant_id,scope_type,scope_id,metric,"limit",period,action)
 VALUES($1,$2,'workspace',$3,'runs_concurrent',0,'total','block')`, ids.New(), r.tenant, r.workspace)
			return err
		})
		now := time.Now().UTC().Truncate(time.Second)
		out, err := r.activities.CreateScheduledSession(t.Context(),
			w.ScheduledSessionInput{TenantID: r.tenant, ScheduleID: row.ID, FireAt: now, Now: now})
		require.NoError(t, err)
		require.Equal(t, "QUOTA_EXCEEDED", out.Skip)
		require.Equal(t, "active", r.row(t, row.ID).Status)
		require.Zero(t, r.client.pauses)
	})
	t.Run("T-P7/Quota", func(t *testing.T) {
		r := newScheduleRig(t, d, admin)
		r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `INSERT INTO quota_policies(id,tenant_id,scope_type,scope_id,metric,"limit",period,action)
 VALUES($1,$2,'workspace',$3,'schedules',1,'total','block')`, ids.New(), r.tenant, r.workspace)
			return err
		})
		row := r.create(t)
		r.patch(t, row.ID, schedules.PatchInput{Status: new("paused")})
		err := d.WithTenant(t.Context(), r.tenant, func(tx pgx.Tx) error {
			ctx := auth.Context(db.Context(t.Context(), tx), auth.Principal{ID: r.actor, Kind: "user"}, r.tenant)
			_, err := r.svc.Create(ctx, ids.New(), r.input)
			return err
		})
		scheduleCode(t, err, "SCHEDULE_QUOTA_EXCEEDED")
	})
	t.Run("T-P7/DefaultAndConcurrent", func(t *testing.T) {
		r := newScheduleRig(t, d, admin)
		for range 20 {
			r.create(t)
		}
		err := r.createError(t)
		scheduleCode(t, err, "SCHEDULE_QUOTA_EXCEEDED")
		r = newScheduleRig(t, d, admin)
		r.svc.DefaultLimit = 1
		start := make(chan struct{})
		results := make(chan error, 2)
		for range 2 {
			go func() {
				<-start
				results <- r.createError(t)
			}()
		}
		close(start)
		a, b := <-results, <-results
		if a == nil {
			a, b = b, a
		}
		require.NoError(t, b)
		scheduleCode(t, a, "SCHEDULE_QUOTA_EXCEEDED")
	})
	t.Run("T-P7b/Sweep", func(t *testing.T) {
		r := newScheduleRig(t, d, admin)
		row := r.create(t)
		r.client.fail = true
		pending, err := r.svc.Sync(t.Context(), r.tenant, row.ID)
		require.Error(t, err)
		require.True(t, pending)
		require.Zero(t, r.row(t, row.ID).SyncedVersion)
		r.client.fail = false
		// F75 permits a session-level seed-10 lock, but no transaction may span Temporal calls.
		r.client.onCall = func() {
			// Tests log in as the DB owner then SET ROLE orbit_app: pg_stat_activity.usename still names
			// the login, not current_user. Find the sync connection by its seed-10 advisory lock instead.
			var pid, locks int
			var inTransaction bool
			err := admin.QueryRow(t.Context(), `SELECT l.pid,a.xact_start IS NOT NULL,
 (SELECT count(*) FROM pg_locks held WHERE held.pid=l.pid AND held.locktype='advisory' AND held.granted)
 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid
 WHERE a.datname=current_database() AND l.locktype='advisory' AND l.granted AND l.objsubid=1
`).Scan(&pid, &inTransaction, &locks)
			require.NoError(t, err)
			require.Positive(t, pid)
			require.False(t, inTransaction, "Temporal called inside a tenant transaction")
			require.Equal(t, 1, locks, "only the F75 per-schedule session lock may span Temporal calls")
		}
		require.NoError(t, r.svc.SweepOnce(t.Context()))
		synced := r.row(t, row.ID)
		require.Equal(t, synced.Version, synced.SyncedVersion)
		r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
			_, err := r.svc.Delete(ctx, row.ID)
			return err
		})
		require.NoError(t, r.svc.SweepOnce(t.Context()))
		require.Equal(t, 1, r.client.deletes)
	})
	t.Run("T-P7/LockSeeds", func(t *testing.T) { testScheduleLockSeeds(t, d, admin) })
	t.Run("T-P7b/CreateThenSyncOverHTTP", func(t *testing.T) { testScheduleHTTPSync(t, d, admin) })
	t.Run("T-P7c/OwnerTransfer", func(t *testing.T) { testScheduleOwnerTransfer(t, d, admin) })
	t.Run("T-P7e/DerivedIdentity", func(t *testing.T) { testScheduleIdentity(t, d, admin) })
	t.Run("T-P11/ScheduledRunActive", func(t *testing.T) { testScheduledRunActive(t, d, admin) })
	t.Run("T-P7d/SerializeAndReread", func(t *testing.T) {
		r := newScheduleRig(t, d, admin)
		row := r.create(t)
		r.client.exists = true
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		r.client.onUpdate = func() {
			once.Do(func() {
				close(entered)
				<-release
			})
		}
		done := make(chan error, 1)
		go func() {
			pending, err := r.svc.Sync(t.Context(), r.tenant, row.ID)
			if err == nil && pending {
				err = errors.New("first sync still pending")
			}
			done <- err
		}()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("sync did not reach Update")
		}
		r.patch(t, row.ID, schedules.PatchInput{Cron: new("15 * * * *")})
		pending, err := r.svc.Sync(t.Context(), r.tenant, row.ID)
		require.NoError(t, err)
		require.True(t, pending)
		close(release)
		require.NoError(t, <-done)
		require.Equal(t, []string{"0 * * * *", "15 * * * *"}, r.client.writes)
		synced := r.row(t, row.ID)
		require.Equal(t, int64(2), synced.Version)
		require.Equal(t, synced.Version, synced.SyncedVersion)
	})
}
func (r *scheduleRig) createError(t *testing.T) error {
	t.Helper()
	return r.d.WithTenant(t.Context(), r.tenant, func(tx pgx.Tx) error {
		ctx := auth.Context(db.Context(t.Context(), tx), auth.Principal{ID: r.actor, Kind: "user"}, r.tenant)
		_, err := r.svc.Create(ctx, ids.New(), r.input)
		return err
	})
}

// holdAdvisory keeps a transaction-level advisory lock open on its own connection until the returned release runs.
func holdAdvisory(t *testing.T, admin *pgx.Conn, tenant string, seed int) func() {
	t.Helper()
	tx, err := admin.Begin(t.Context())
	require.NoError(t, err)
	_, err = tx.Exec(t.Context(), `SELECT pg_advisory_xact_lock(hashtextextended($1,$2))`, tenant, seed)
	require.NoError(t, err)
	return func() { require.NoError(t, tx.Rollback(context.Background())) }
}
func testScheduleLockSeeds(t *testing.T, d *db.DB, admin *pgx.Conn) {
	r := newScheduleRig(t, d, admin)
	// The usage admission lock (seed 7) belongs to runs: schedule quota must not wait on it.
	release := holdAdvisory(t, admin, r.tenant, 7)
	done := make(chan error, 1)
	go func() { done <- r.createError(t) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Create waited on the usage admission lock")
	}
	release()
	// The schedule quota lock (seed 9) serializes counting.
	release = holdAdvisory(t, admin, r.tenant, 9)
	go func() { done <- r.createError(t) }()
	select {
	case <-done:
		t.Fatal("Create did not wait on the schedule quota lock")
	case <-time.After(300 * time.Millisecond):
	}
	release()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Create stayed blocked after the quota lock was released")
	}
}
func (r *scheduleRig) request(t *testing.T, method, path, body, key string) (int, []byte) {
	t.Helper()
	h, token := r.api(t, r.agentLock(t), &teamTemporal{}, func(h *httpapi.Handler) { h.Schedules = &r.svc })
	return r.call(t, h, token, method, path, body, key)
}
func (r *scheduleRig) agentLock(t *testing.T) contract.RunLock {
	return readLock(t, "RunLock.agent.json")
}
func (r *scheduleRig) body() string {
	raw, _ := json.Marshal(r.input)
	return string(raw)
}
func testScheduleHTTPSync(t *testing.T, d *db.DB, admin *pgx.Conn) {
	r := newScheduleRig(t, d, admin)
	r.client.fail = true
	status, raw := r.request(t, "POST", "/v1/schedules", r.body(), "sync-1")
	require.Equal(t, 201, status, string(raw))
	var row schedules.Schedule
	require.NoError(t, json.Unmarshal(raw, &row))
	require.True(t, row.SyncPending)
	require.Zero(t, r.row(t, row.ID).SyncedVersion)
}
func (r *scheduleRig) addOwner(t *testing.T, role string) string {
	t.Helper()
	id := ids.New()
	_, err := r.admin.Exec(t.Context(), `INSERT INTO principals(id,kind,email,display_name,idp_subject)
 VALUES($1::uuid,'user',$1::text||'@example.test','Other',$1::text)`, id)
	require.NoError(t, err)
	_, err = r.admin.Exec(t.Context(), `INSERT INTO memberships(tenant_id,principal_id,status,source) VALUES($1,$2,'active','direct')`,
		r.tenant, id)
	require.NoError(t, err)
	_, err = r.admin.Exec(t.Context(), `INSERT INTO role_bindings(id,tenant_id,principal_type,principal_id,role,scope_type,scope_id,created_by)
 VALUES($1,$2,'user',$3,$4,'workspace',$5,$3)`, ids.New(), r.tenant, id, role, r.workspace)
	require.NoError(t, err)
	return id
}
func (r *scheduleRig) as(t *testing.T, principal string, fn func(context.Context) error) error {
	t.Helper()
	return r.d.WithTenant(t.Context(), r.tenant, func(tx pgx.Tx) error {
		return fn(auth.Context(db.Context(t.Context(), tx), auth.Principal{ID: principal, Kind: "user"}, r.tenant))
	})
}
func testScheduleOwnerTransfer(t *testing.T, d *db.DB, admin *pgx.Conn) {
	r := newScheduleRig(t, d, admin)
	row := r.create(t)
	other := r.addOwner(t, "ws.admin")
	patch := func(id string, in schedules.PatchInput) schedules.Schedule {
		var out schedules.Schedule
		require.NoError(t, r.as(t, other, func(ctx context.Context) error {
			var err error
			out, err = r.svc.Patch(ctx, id, in)
			return err
		}))
		return out
	}
	require.Equal(t, r.actor, patch(row.ID, schedules.PatchInput{Name: new("Renamed")}).OwnerID)
	require.Equal(t, r.actor, patch(row.ID, schedules.PatchInput{Status: new("paused")}).OwnerID)
	require.Equal(t, r.actor, patch(row.ID, schedules.PatchInput{Cron: new(row.Cron)}).OwnerID, "the same cron is not a content change")
	var deleted schedules.Schedule
	require.NoError(t, r.as(t, other, func(ctx context.Context) error {
		var err error
		deleted, err = r.svc.Delete(ctx, r.create(t).ID)
		return err
	}))
	require.Equal(t, r.actor, deleted.OwnerID)
	transferred := patch(row.ID, schedules.PatchInput{Prompt: new("A different prompt"), Status: new("active")})
	require.Equal(t, other, transferred.OwnerID)
	var audits int
	r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT count(*) FROM audit_log WHERE tenant_id=$1 AND action='schedule.owner_transferred' AND target_id=$2`,
			r.tenant, row.ID).Scan(&audits)
	})
	require.Equal(t, 1, audits)
	// The next firing is authorized, admitted and attributed to the new owner.
	now := time.Now().UTC().Truncate(time.Second)
	out, err := r.activities.CreateScheduledSession(t.Context(),
		w.ScheduledSessionInput{TenantID: r.tenant, ScheduleID: row.ID, FireAt: now, Now: now})
	require.NoError(t, err)
	require.NotNil(t, out.Run)
	var createdBy string
	r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT created_by::text FROM runs WHERE tenant_id=$1 AND id=$2`, r.tenant, out.Run.RunID).Scan(&createdBy)
	})
	require.Equal(t, other, createdBy)
}
func testScheduleIdentity(t *testing.T, d *db.DB, admin *pgx.Conn) {
	r := newScheduleRig(t, d, admin)
	status, raw := r.request(t, "POST", "/v1/schedules", r.body(), "shared-key")
	require.Equal(t, 201, status, string(raw))
	var first schedules.Schedule
	require.NoError(t, json.Unmarshal(raw, &first))
	// The same principal in another tenant using the same key creates a different row.
	tenant, workspace := ids.New(), ids.New()
	_, err := admin.Exec(t.Context(), `INSERT INTO tenants(id,slug,name) VALUES($1::uuid,$1::text,'Second')`, tenant)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), `INSERT INTO memberships(tenant_id,principal_id,status,source) VALUES($1,$2,'active','direct')`, tenant,
		r.actor)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), `INSERT INTO workspaces(id,tenant_id,name) VALUES($1,$2,'Second')`, workspace, tenant)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), `INSERT INTO role_bindings(id,tenant_id,principal_type,principal_id,role,scope_type,scope_id,created_by)
 VALUES($1,$2,'user',$3,'ws.editor','workspace',$4,$3)`, ids.New(), tenant, r.actor, workspace)
	require.NoError(t, err)
	second := *r
	second.runFixture = &runFixture{}
	*second.runFixture = *r.runFixture
	second.tenant, second.workspace = tenant, workspace
	second.input.WorkspaceID = workspace
	status, raw = second.request(t, "POST", "/v1/schedules", second.body(), "shared-key")
	require.Equal(t, 201, status, string(raw))
	var other schedules.Schedule
	require.NoError(t, json.Unmarshal(raw, &other))
	require.NotEqual(t, first.ID, other.ID)
	// Idempotency records expire after 24 hours; the key stays bound to its schedule.
	_, err = admin.Exec(t.Context(), `DELETE FROM idempotency_keys WHERE tenant_id=$1`, r.tenant)
	require.NoError(t, err)
	status, raw = r.request(t, "POST", "/v1/schedules", r.body(), "shared-key")
	require.Equal(t, 201, status, string(raw))
	var again schedules.Schedule
	require.NoError(t, json.Unmarshal(raw, &again))
	require.Equal(t, first.ID, again.ID)
	var count int
	require.NoError(t, admin.QueryRow(t.Context(), `SELECT count(*) FROM schedules WHERE tenant_id=$1`, r.tenant).Scan(&count))
	require.Equal(t, 1, count)
}
func testScheduledRunActive(t *testing.T, d *db.DB, admin *pgx.Conn) {
	r := newScheduleRig(t, d, admin)
	row := r.create(t)
	now := time.Now().UTC().Truncate(time.Second)
	fire := w.ScheduledSessionInput{TenantID: r.tenant, ScheduleID: row.ID, FireAt: now, Now: now}
	out, err := r.activities.CreateScheduledSession(t.Context(), fire)
	require.NoError(t, err)
	require.NotNil(t, out.Run)
	h, token := r.api(t, r.agentLock(t), &teamTemporal{}, func(h *httpapi.Handler) { h.Schedules = &r.svc })
	send := func(key string) (int, []byte) {
		return r.call(t, h, token, "POST", "/v1/sessions/"+out.Run.SessionID+"/messages", `{"text":"More"}`, key)
	}
	status, raw := send("running")
	require.Equal(t, 409, status, string(raw))
	require.Equal(t, "SCHEDULED_RUN_ACTIVE", errorCode(t, raw))
	// The child run already ended but RecordScheduleRun has not run yet: the row still says running.
	_, err = admin.Exec(t.Context(), `UPDATE runs SET status='succeeded',ended_at=now() WHERE id=$1`, out.Run.RunID)
	require.NoError(t, err)
	status, raw = send("ended-not-recorded")
	require.Equal(t, 409, status, string(raw))
	require.Equal(t, "SCHEDULED_RUN_ACTIVE", errorCode(t, raw))
	require.NoError(t, r.activities.RecordScheduleRun(t.Context(), w.RecordScheduleInput{TenantID: r.tenant, ScheduleID: row.ID, FireAt: now}))
	status, raw = send("recorded")
	require.Equal(t, 202, status, string(raw))
}

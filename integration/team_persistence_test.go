package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/activities/ctl"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/kernelapi"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/temporalx"
	"github.com/mindreon/orbit-control/internal/sessions"
	"github.com/mindreon/orbit-control/internal/tenancy"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

// recordingPublisher fails its first publication when asked, leaving that event unpublished.
type recordingPublisher struct {
	failFirst bool
	ids       []string
	calls     int
}

func (p *recordingPublisher) Publish(_ context.Context, _, id string, _ []byte) error {
	p.calls++
	if p.failFirst && p.calls == 1 {
		return context.DeadlineExceeded
	}
	p.ids = append(p.ids, id)
	return nil
}

type temporalCall struct{ Kind, Workflow, Name, Key string }

// teamTemporal records every call; Update results are scripted by updateFn.
type teamTemporal struct {
	runTemporal
	calls    []temporalCall
	updateFn func(call int, in any) error
	updates  []any
	onUpdate func()
	submits  []w.SubmitMessageInput
}

var _ temporalx.Client = (*teamTemporal)(nil)

func (c *teamTemporal) Update(_ context.Context, workflow, name, key string, in, out any) error {
	c.calls = append(c.calls, temporalCall{Kind: "update", Workflow: workflow, Name: name, Key: key})
	c.updates = append(c.updates, in)
	if c.onUpdate != nil {
		c.onUpdate()
	}
	if c.updateFn != nil {
		if err := c.updateFn(len(c.calls), in); err != nil {
			return err
		}
	}
	switch result := out.(type) {
	case *w.CancelRunOutput:
		result.Disposition = "cancelling"
	case *w.ResolveInteractionOutput:
		result.Accepted = true
	}
	return nil
}
func (c *teamTemporal) Signal(_ context.Context, workflow, name string, _ any) error {
	c.calls = append(c.calls, temporalCall{Kind: "signal", Workflow: workflow, Name: name, Key: ""})
	return nil
}
func (c *teamTemporal) Submit(_ context.Context, _ w.SessionInput, msg w.SubmitMessageInput) (w.SubmitMessageOutput, error) {
	c.submits = append(c.submits, msg)
	return w.SubmitMessageOutput{RunID: msg.RunID, Disposition: "started"}, nil
}

type teamRig struct {
	*runFixture
	admin     *pgx.Conn
	team      contract.RunLock
	agentLock contract.RunLock
	roster    []contract.RosterEntry
}

func readLock(t *testing.T, name string) contract.RunLock {
	t.Helper()
	raw, err := os.ReadFile("../contract/v2/examples/" + name)
	require.NoError(t, err)
	var lock contract.RunLock
	require.NoError(t, json.Unmarshal(raw, &lock))
	for key, entry := range lock.Entries {
		entry.Files = nil
		lock.Entries[key] = entry
	}
	return lock
}

// newTeamRig turns the run fixture into a team run: the saved lock is the team example and
// PrepareTeamRun has projected its roster.
func newTeamRig(t *testing.T, d *db.DB, admin *pgx.Conn) *teamRig {
	t.Helper()
	f := newRunFixture(t, d, admin)
	r := &teamRig{runFixture: f, admin: admin, agentLock: readLock(t, "RunLock.agent.json"), team: readLock(t, "RunLock.team.json")}
	r.team.LockId = f.lock
	raw, err := json.Marshal(r.team)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), `UPDATE run_locks SET lock=$2,digest=$3 WHERE id=$1`, f.lock, raw, r.team.Digest)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), `UPDATE sessions SET entry_kind='team',entry_ref=$2 WHERE id=$1`, f.session, r.team.Entry.Ref)
	require.NoError(t, err)
	for _, member := range r.team.Members {
		r.roster = append(r.roster, contract.RosterEntry{Name: member.Name, Role: contract.TeamRole(member.Role), Status: "idle",
			Description: member.Description})
	}
	board := contract.TeamBoard{Roster: r.roster}
	require.NoError(t, f.activities.PrepareTeamRun(t.Context(), w.PrepareTeamInput{RunOp: r.op("team", 0), Budget: 200000,
		Team: w.TeamSpec{Roster: r.roster, Leader: "leader", MaxParallel: 2, ReviewMaxRounds: 3}, Board: board}))
	return r
}
func (r *teamRig) op(member string, seq int) w.RunOp {
	op := r.runFixture.op()
	op.Member, op.Seq = member, seq
	return op
}
func (r *teamRig) status(t *testing.T) string {
	t.Helper()
	var status string
	r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT status FROM runs WHERE tenant_id=$1 AND id=$2`, r.tenant, r.run).Scan(&status)
	})
	return status
}

// statusEvents lists the run.status events in publication order.
func (r *teamRig) statusEvents(t *testing.T) []string {
	t.Helper()
	var out []string
	r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT payload->'data'->>'status' FROM control_events
 WHERE tenant_id=$1 AND run_id=$2 AND payload->>'type'='run.status' ORDER BY operation,event_id`, r.tenant, r.run)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var status string
			if err = rows.Scan(&status); err != nil {
				return err
			}
			out = append(out, status)
		}
		return rows.Err()
	})
	return out
}
func (r *teamRig) interaction(t *testing.T, member string, kind string) string {
	t.Helper()
	req := contract.InteractionRequest{Kind: contract.InteractionKind(kind), InteractionId: ids.New()}
	if kind == "ask_user" {
		req.Question = &contract.Question{Text: "Ready?"}
	} else {
		raw, err := os.ReadFile("../contract/v2/examples/InteractionRequest.tool_approval.json")
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, &req))
		req.InteractionId = ids.New()
	}
	op := r.op(member, 1)
	_, err := r.createInteraction(t, w.InteractionInput{RunOp: op, Request: req, RemindAt: op.EventAt.Add(time.Hour),
		EscalateAt: op.EventAt.Add(2 * time.Hour), ExpiresAt: op.EventAt.Add(3 * time.Hour)})
	require.NoError(t, err)
	return req.InteractionId
}
func (r *teamRig) decide(t *testing.T, member, interaction, body string, expired bool) {
	t.Helper()
	var decision contract.Decision
	require.NoError(t, json.Unmarshal([]byte(body), &decision))
	require.NoError(t, r.activities.ApplyDecision(t.Context(), w.DecisionInput{RunOp: r.op(member, 1), InteractionID: interaction,
		Kind: decision.Kind, Decision: decision, Actor: r.actor, Expired: expired}))
}

// api builds the public handler for the fixture principal, who holds ws.editor.
func (f *runFixture) api(t *testing.T, lock contract.RunLock, tc temporalx.Client, mutate ...func(*httpapi.Handler)) (http.Handler,
	string) {
	t.Helper()
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		return tenancy.Bind(ctx, tx, auth.From(ctx), f.tenant, "user", f.actor, "ws.editor", "workspace", f.workspace)
	})
	c := config.Config{
		AuthMode: "dev", KeyFile: t.TempDir() + "/auth.key", CapabilityKeyFile: t.TempDir() + "/cap.key",
		StreamKeyFile: t.TempDir() + "/stream.key", StreamKeyID: "test",
		CapabilityKeyID: "test", SecretsKeyVersion: 1, SecretsKEKs: map[int][]byte{1: make([]byte, 32)}, DefaultRunBudget: 200000}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	a, err := auth.New(t.Context(), f.d, c, logger)
	require.NoError(t, err)
	t.Cleanup(a.Close)
	token, err := a.Signer.DevToken(auth.Principal{ID: f.actor, Kind: "user"})
	require.NoError(t, err)
	h := &httpapi.Handler{DB: f.d, Auth: a, Config: c, Logger: logger, Temporal: tc,
		Objects: runObjects{testBlobs: testBlobs{root: t.TempDir()}}, Locks: runResolver{lock: lock}}
	for _, fn := range mutate {
		fn(h)
	}
	return h.Router(), token
}
func (f *runFixture) call(t *testing.T, h http.Handler, token, method, path, body, key string) (int, []byte) {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Orbit-Tenant", f.tenant)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	out := httptest.NewRecorder()
	h.ServeHTTP(out, req)
	return out.Code, out.Body.Bytes()
}
func errorCode(t *testing.T, raw []byte) string {
	t.Helper()
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(raw, &body))
	return body.Error.Code
}

func testTeamPersistence(t *testing.T, d *db.DB, admin *pgx.Conn) {
	t.Run("T-P2/RecordBoardRetry", func(t *testing.T) {
		r := newTeamRig(t, d, admin)
		board := contract.TeamBoard{Roster: r.roster, Tasks: []contract.TeamTask{{TaskId: "t1", Title: "Task", Description: "Do it",
			Status: "pending", Failures: new(0), Review: &contract.TaskReview{}}}}
		raw, err := os.ReadFile("../contract/v2/examples/RunEvent.team.task.created.json")
		require.NoError(t, err)
		var example contract.RunEvent
		require.NoError(t, json.Unmarshal(raw, &example))
		data, err := json.Marshal(example.Data)
		require.NoError(t, err)
		in := w.RecordBoardInput{RunOp: r.op("team", 0), Version: 5, Board: board,
			Events: []w.TeamEvent{{Type: string(example.Type), Data: data}}}
		for range 2 {
			require.NoError(t, r.activities.RecordBoard(t.Context(), in))
		}
		stale := in
		stale.RunOp = r.op("team", 0)
		stale.Version = 3
		stale.Board.Tasks = nil
		require.NoError(t, r.activities.RecordBoard(t.Context(), stale))
		r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
			var events, tasks, version int
			require.NoError(t, tx.QueryRow(ctx, `SELECT count(*) FROM control_events WHERE tenant_id=$1 AND run_id=$2 AND payload->>'type'=$3`,
				r.tenant, r.run, string(example.Type)).Scan(&events))
			require.NoError(t, tx.QueryRow(ctx, `SELECT count(*) FROM team_tasks WHERE tenant_id=$1 AND run_id=$2`, r.tenant, r.run).Scan(&tasks))
			require.NoError(t, tx.QueryRow(ctx, `SELECT board_version FROM team_runs WHERE tenant_id=$1 AND run_id=$2`, r.tenant,
				r.run).Scan(&version))
			require.Equal(t, 1, events)
			require.Equal(t, 1, tasks)
			require.Equal(t, 5, version)
			return nil
		})
	})
	t.Run("T-P3/PublishOnlyOwnMember", func(t *testing.T) {
		r := newTeamRig(t, d, admin)
		publisher := &recordingPublisher{failFirst: true}
		r.activities.NATS = publisher
		const operation = 900
		team := r.op("team", 0)
		team.EventN = operation
		require.NoError(t, r.activities.SetRunStatus(t.Context(), w.StatusInput{RunOp: team, Status: "paused"}))
		require.Empty(t, publisher.ids, "the team event stays unpublished after the failed publication")
		member := r.op("researcher", 1)
		member.EventN = operation
		req := contract.InteractionRequest{Kind: "ask_user", InteractionId: ids.New(), Question: &contract.Question{Text: "Ready?"}}
		_, err := r.createInteraction(t, w.InteractionInput{RunOp: member, Request: req, ExpiresAt: member.EventAt.Add(time.Hour)})
		require.NoError(t, err)
		require.NotEmpty(t, publisher.ids)
		for _, id := range publisher.ids {
			require.Contains(t, id, ":researcher:", "the member operation republished another member's event")
		}
	})
	t.Run("T-P4/MemberDoesNotMoveRun", func(t *testing.T) {
		r := newTeamRig(t, d, admin)
		_, err := admin.Exec(t.Context(), `UPDATE runs SET status='waiting_input' WHERE id=$1`, r.run)
		require.NoError(t, err)
		member := r.op("researcher", 1)
		require.NoError(t, r.activities.StartSegment(t.Context(), w.StartSegmentInput{RunOp: member}))
		require.NoError(t, r.activities.SetRunStatus(t.Context(), w.StatusInput{RunOp: r.op("researcher", 1), Status: "running"}))
		require.Equal(t, "waiting_input", r.status(t))
		raw, err := os.ReadFile("../contract/v2/examples/SegmentResult.completed.json")
		require.NoError(t, err)
		var result contract.SegmentResult
		require.NoError(t, json.Unmarshal(raw, &result))
		result.Artifacts = nil
		result.Final.MessageId = ids.New()
		result.Final.ContentRef = nil
		_, err = r.activities.RecordSegment(t.Context(), w.RecordSegmentInput{RunOp: r.op("researcher", 1), Result: result})
		require.NoError(t, err)
		r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
			var messages int
			require.NoError(t, tx.QueryRow(ctx, `SELECT count(*) FROM messages WHERE tenant_id=$1 AND run_id=$2`, r.tenant, r.run).Scan(&messages))
			require.Zero(t, messages)
			return nil
		})
	})
	t.Run("T-P4b/WaitingStatusTable", func(t *testing.T) {
		r := newTeamRig(t, d, admin)
		a := r.interaction(t, "researcher", "ask_user")
		require.Equal(t, "waiting_input", r.status(t))
		b := r.interaction(t, "reviewer", "tool_approval")
		require.Equal(t, "waiting_approval", r.status(t))
		r.decide(t, "researcher", a, `{"kind":"ask_user","answer":"Ready"}`, false)
		require.Equal(t, "waiting_approval", r.status(t))
		require.NoError(t, r.activities.SetRunStatus(t.Context(), w.StatusInput{RunOp: r.op("team", 0), Status: "paused"}))
		require.Equal(t, "paused", r.status(t))
		r.decide(t, "reviewer", b, `{"kind":"tool_approval","choice":"deny"}`, true)
		require.Equal(t, "paused", r.status(t))
		require.NoError(t, r.activities.SetRunStatus(t.Context(), w.StatusInput{RunOp: r.op("team", 0), Status: "running"}))
		require.Equal(t, "running", r.status(t))
		// Exactly one run.status event per change: nothing is emitted when the derived status stays the same.
		require.Equal(t, []string{"waiting_input", "waiting_approval", "paused", "running"}, r.statusEvents(t))
		pending := r.interaction(t, "leader", "ask_user")
		require.NoError(t, r.activities.FinalizeTeamRun(t.Context(), w.FinalizeTeamInput{RunOp: r.op("team", 0), Status: "succeeded",
			Board: contract.TeamBoard{Roster: r.roster}}))
		before := r.statusEvents(t)
		require.NoError(t, r.activities.CancelInteraction(t.Context(), w.NotifyInput{RunOp: r.op("leader", 1), InteractionID: pending}))
		require.Equal(t, "succeeded", r.status(t))
		require.Equal(t, before, r.statusEvents(t))
	})
	t.Run("T-P4c/ResolveRoutesToMemberWorkflow", func(t *testing.T) {
		r := newTeamRig(t, d, admin)
		interaction := r.interaction(t, "researcher", "ask_user")
		fake := &teamTemporal{}
		h, token := r.api(t, r.agentLock, fake)
		status, raw := r.call(t, h, token, "POST", "/v1/interactions/"+interaction+"/resolve", `{"kind":"ask_user","answer":"Ready"}`, "")
		require.Equal(t, 200, status, string(raw))
		require.Equal(t, []temporalCall{{Kind: "update", Workflow: "member:" + r.tenant + ":" + r.run + ":alice:1", Name: w.ResolveInteraction,
			Key: "resolve:" + interaction}}, fake.calls)
	})
	t.Run("T-P4d/WaitForTeamWorkflow", func(t *testing.T) {
		r := newTeamRig(t, d, admin)
		id := w.TeamRunWorkflowID(r.tenant, r.run)
		_, err := admin.Exec(t.Context(), `UPDATE runs SET temporal_workflow_id=$2 WHERE id=$1`, r.run, id)
		require.NoError(t, err)
		fake := mocks.NewClient(t)
		run := mocks.NewWorkflowRun(t)
		fake.On("GetWorkflow", mock.Anything, id, "").Return(run).Once()
		run.On("Get", mock.Anything, nil).Return(nil).Once()
		a := &ctl.Activities{DB: d, Temporal: fake}
		require.NoError(t, a.WaitForRun(t.Context(), r.op("team", 0)))
	})
	t.Run("T-P4e/CancelScheduledTeamSignals", func(t *testing.T) {
		for _, trigger := range []string{"schedule", "user_message"} {
			r := newTeamRig(t, d, admin)
			id := w.TeamRunWorkflowID(r.tenant, r.run)
			_, err := admin.Exec(t.Context(), `UPDATE runs SET temporal_workflow_id=$2,trigger=$3 WHERE id=$1`, r.run, id, trigger)
			require.NoError(t, err)
			fake := &teamTemporal{}
			h, token := r.api(t, r.agentLock, fake)
			status, raw := r.call(t, h, token, "POST", "/v1/runs/"+r.run+"/cancel", `{}`, "")
			require.Equal(t, 202, status, string(raw))
			require.Len(t, fake.calls, 1)
			if trigger == "schedule" {
				require.Equal(t, temporalCall{Kind: "signal", Workflow: id, Name: w.Cancel, Key: ""}, fake.calls[0])
			} else {
				require.Equal(t, w.CancelRun, fake.calls[0].Name)
				require.Equal(t, w.SessionWorkflowID(r.tenant, r.session), fake.calls[0].Workflow)
			}
		}
	})
	t.Run("T-P5/SandboxMountsIndependentOfCaller", func(t *testing.T) {
		r := newTeamRig(t, d, admin)
		var inputs []w.AcquireInput
		fake := mocks.NewClient(t)
		fake.On("NewWithStartWorkflowOperation", mock.Anything, mock.Anything, mock.Anything).Return(struct {
			client.WithStartWorkflowOperation
		}{})
		handle := mocks.NewWorkflowUpdateHandle(t)
		handle.On("Get", mock.Anything, mock.Anything).Return(nil)
		fake.On("UpdateWithStartWorkflow", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			options := args.Get(1).(client.UpdateWithStartWorkflowOptions)
			inputs = append(inputs, options.UpdateOptions.Args[0].(w.AcquireInput))
		}).Return(handle, nil)
		a := &ctl.Activities{DB: d, Temporal: fake}
		for _, member := range []string{"leader", "researcher", "reviewer"} {
			op := r.op(member, 1)
			_, err := a.AcquireSandbox(t.Context(),
				w.AcquireSandboxInput{RunOp: op, Spec: w.SandboxSpec{IsolationKey: w.IsolationKey(r.tenant, r.session)},
					LockID: r.lock, Holder: r.run, Turn: 1})
			require.NoError(t, err)
		}
		require.Len(t, inputs, 3)
		require.Len(t, inputs[0].Mounts, len(r.team.Members)+countSkills(r.team))
		for _, in := range inputs[1:] {
			require.Equal(t, inputs[0].Mounts, in.Mounts)
			require.Equal(t, inputs[0].SpecDigest, in.SpecDigest)
		}
	})
	t.Run("T-P8/KernelTeamEndpoints", func(t *testing.T) { testKernelTeam(t, d, admin) })
	t.Run("T-P12/TeamRunIsOneConcurrentRun", func(t *testing.T) {
		r := newTeamRig(t, d, admin)
		for _, member := range r.roster {
			require.NoError(t, r.activities.StartSegment(t.Context(), w.StartSegmentInput{RunOp: r.op(string(member.Name), 1)}))
		}
		r.tx(t, func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `INSERT INTO quota_policies(id,tenant_id,scope_type,scope_id,metric,"limit",period,action)
 VALUES($1,$2,'tenant',$2,'runs_concurrent',2,'total','block')`, ids.New(), r.tenant)
			return err
		})
		svc := sessions.Service{Locks: runResolver{lock: r.agentLock}, Objects: runObjects{testBlobs: testBlobs{root: t.TempDir()}}}
		submit := func() error {
			session := ids.New()
			return d.WithTenant(t.Context(), r.tenant, func(tx pgx.Tx) error {
				ctx := auth.Context(db.Context(t.Context(), tx), auth.Principal{ID: r.actor, Kind: "user"}, r.tenant)
				if _, err := tx.Exec(ctx, `INSERT INTO sessions(id,tenant_id,workspace_id,owner_id,title,entry_kind,entry_ref,message_policy,status)
 VALUES($1,$2,$3,$4,'Next','agent','agent:test','queue','idle')`, session, r.tenant, r.workspace, r.actor); err != nil {
					return err
				}
				s := sessions.Session{ID: session, WorkspaceID: r.workspace, OwnerID: r.actor, EntryKind: "agent", EntryRef: "agent:test"}
				_, err := svc.BuildStart(ctx, tx, s, auth.From(ctx), ids.New(), ids.New(), "hello", nil)
				return err
			})
		}
		// Three member turns plus the team run itself would already exceed a limit of two if they were counted.
		require.NoError(t, submit())
		scheduleCode(t, submit(), "QUOTA_EXCEEDED")
	})
}

func countSkills(lock contract.RunLock) int {
	n := 0
	for _, member := range lock.Members {
		n += len(member.Skills)
	}
	return n
}

func (r *teamRig) memberToken(t *testing.T, router http.Handler, member string) string {
	t.Helper()
	require.NoError(t, r.activities.StartSegment(t.Context(), w.StartSegmentInput{RunOp: r.op(member, 1)}))
	raw, err := json.Marshal(contract.TokenRequest{TenantId: r.tenant, RunId: r.run, Purpose: "segment",
		Member: new(contract.MemberName(member)), Seq: new(1), Attempt: new(1)})
	require.NoError(t, err)
	req := httptest.NewRequest("POST", "/internal/v1/tokens", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer fixture-service-credential")
	out := httptest.NewRecorder()
	router.ServeHTTP(out, req)
	require.Equal(t, 200, out.Code, out.Body.String())
	var token contract.TokenResponse
	require.NoError(t, json.Unmarshal(out.Body.Bytes(), &token))
	return string(token.Token)
}

func testKernelTeam(t *testing.T, d *db.DB, admin *pgx.Conn) {
	r := newTeamRig(t, d, admin)
	fake := &teamTemporal{}
	open := -1
	fake.onUpdate = func() {
		// The Update must happen after the capability transaction committed.
		require.NoError(t, admin.QueryRow(t.Context(), `SELECT count(*) FROM pg_stat_activity
 WHERE datname=current_database() AND usename='orbit_app' AND xact_start IS NOT NULL`).Scan(&open))
		require.Zero(t, open, "Temporal Update ran inside a database transaction")
	}
	router := (&kernelapi.Handler{DB: d, Signer: r.signer, Secrets: r.keys, Temporal: fake,
		ServiceTokenSHA256: r.serviceDigest()}).Router()
	token := r.memberToken(t, r.router, "researcher")
	update := func(body string) (int, []byte) {
		req := httptest.NewRequest("POST", "/internal/v1/runs/"+r.run+"/team/updates/CreateTasks", bytes.NewBufferString(body))
		req.Header.Set("Authorization", "Bearer "+token)
		out := httptest.NewRecorder()
		router.ServeHTTP(out, req)
		return out.Code, out.Body.Bytes()
	}
	body := `{"caller":"leader","request_id":"req-1","tasks":[{"title":"Task","description":"Do it"}]}`
	status, raw := update(body)
	require.Equal(t, 200, status, string(raw))
	require.Equal(t, contract.MemberName("researcher"), *fake.updates[0].(*contract.CreateTasksInput).Caller,
		"the token, not the body, names the caller")
	require.Equal(t, "req-1", fake.calls[0].Key)

	fake.updateFn = func(_ int, _ any) error { return temporal.NewApplicationError("not leader", "TEAM_NOT_LEADER") }
	status, raw = update(body)
	require.Equal(t, 409, status, string(raw))
	require.Equal(t, "TEAM_NOT_LEADER", errorCode(t, raw))

	// A NotFound while the run has not ended is the Continue-As-New gap: retry with the same Update ID.
	fake.calls = nil
	fake.updateFn = func(call int, _ any) error {
		if call == 1 {
			return serviceerror.NewNotFound("between executions")
		}
		return nil
	}
	status, raw = update(body)
	require.Equal(t, 200, status, string(raw))
	require.Len(t, fake.calls, 2)
	require.Equal(t, fake.calls[0].Key, fake.calls[1].Key)

	fake.updateFn = func(int, any) error { return serviceerror.NewNotFound("closed") }
	// The ended run no longer authorizes the member token, so end it after the request is already authorized.
	fake.onUpdate = func() {
		_, err := admin.Exec(t.Context(), `UPDATE runs SET ended_at=now() WHERE id=$1`, r.run)
		require.NoError(t, err)
	}
	status, raw = update(body)
	require.Equal(t, 409, status, string(raw))
	require.Equal(t, "TEAM_FINISHED", errorCode(t, raw))
}

func (f *runFixture) serviceDigest() string {
	digest := sha256.Sum256([]byte("fixture-service-credential"))
	return hex.EncodeToString(digest[:])
}

//go:build temporal_integration

package integration

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/temporalx"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/agentrun"
	"github.com/mindreon/orbit-control/internal/workflows/membertask"
	"github.com/mindreon/orbit-control/internal/workflows/sandboxlease"
	"github.com/mindreon/orbit-control/internal/workflows/scheduledrun"
	"github.com/mindreon/orbit-control/internal/workflows/session"
	"github.com/mindreon/orbit-control/internal/workflows/teamrun"
	"github.com/mindreon/orbit-control/internal/workflows/usagerollup"
)

var update = flag.Bool("update", false, "write real dev-stack histories")

type fixtures struct {
	sync.Mutex
	calls map[string]int
}

func (f *fixtures) segment(ctx context.Context, in contract.SegmentInput) (contract.SegmentResult, error) {
	f.Lock()
	n := f.calls[in.RunId] + 1
	f.calls[in.RunId] = n
	f.Unlock()
	res := contract.SegmentResult{V: 1, Status: "completed", CheckpointVersion: in.ExpectedCheckpointVersion + 1,
		Progress: contract.SegmentProgress{NewOutput: true}}
	name := in.RunId
	if strings.HasSuffix(name, "pause") {
		name = "pause"
	}
	switch name {
	case "yield", "can":
		threshold := 3
		if name == "can" {
			threshold = w.RunContinueAfter + 1
		}
		if in.Seq < threshold {
			res.Status = "yielded"
			res.YieldReason = new(contract.YieldReason("iteration_cap"))
		}
	case "approval", "expiry":
		if in.Seq == 1 {
			res.Status = "interrupted"
			res.Interaction = &contract.InteractionRequest{InteractionId: "approval", Kind: "tool_approval"}
		}
	case "cancel", "pause", "steer":
		if n == 1 {
			duration := time.Second
			if name == "cancel" {
				duration = time.Minute
			}
			timer := time.NewTimer(duration)
			defer timer.Stop()
			heartbeat := time.NewTicker(50 * time.Millisecond)
			defer heartbeat.Stop()
		waiting:
			for {
				select {
				case <-heartbeat.C:
					activity.RecordHeartbeat(ctx)
				case <-ctx.Done():
					return res, ctx.Err()
				case <-timer.C:
					break waiting
				}
			}
			if in.RunId == "pause" {
				res.Status = "yielded"
				res.YieldReason = new(contract.YieldReason("iteration_cap"))
			}
		}
	case "sandbox":
		if n == 1 {
			return res, temporal.NewNonRetryableApplicationError("lost", contract.ErrorSandboxUnavailable, nil)
		}
	case "compact":
		if n == 1 {
			return res, temporal.NewNonRetryableApplicationError("context", contract.ErrorContextLengthExceeded, nil)
		}
		if in.Input.Kind == "compact" {
			res.Status = "yielded"
			res.YieldReason = new(contract.YieldReason("compacted"))
		}
	}
	return res, nil
}
func registerFixtures(reg worker.Worker, f *fixtures) {
	reg.RegisterWorkflowWithOptions(agentrun.Run, workflow.RegisterOptions{Name: w.AgentRunWorkflow})
	reg.RegisterWorkflowWithOptions(session.Run, workflow.RegisterOptions{Name: w.SessionWorkflow})
	reg.RegisterWorkflowWithOptions(sandboxlease.Run, workflow.RegisterOptions{Name: w.SandboxLeaseWorkflow})
	registerUsageFixtures(reg)
	reg.RegisterActivityWithOptions(f.segment, activity.RegisterOptions{Name: contract.ActivityRunAgentSegment})
	reg.RegisterActivityWithOptions(func(context.Context, w.PrepareInput) (w.PrepareOutput, error) {
		return w.PrepareOutput{}, nil
	}, activity.RegisterOptions{Name: "PrepareRun"})
	reg.RegisterActivityWithOptions(func(context.Context, w.AcquireSandboxInput) (contract.SandboxLease, error) {
		return contract.SandboxLease{Provider: "local", SandboxId: "fixture"}, nil
	}, activity.RegisterOptions{Name: "AcquireSandbox"})
	reg.RegisterActivityWithOptions(func(context.Context, w.RecordSegmentInput) (w.RecordSegmentOutput, error) {
		return w.RecordSegmentOutput{}, nil
	}, activity.RegisterOptions{Name: "RecordSegment"})
	reg.RegisterActivityWithOptions(func(_ context.Context, in w.InteractionInput) (w.InteractionOutput, error) {
		return w.InteractionOutput{CreatedAt: in.EventAt}, nil
	}, activity.RegisterOptions{Name: "CreateInteraction"})
	reg.RegisterActivityWithOptions(
		func(context.Context, w.StartSegmentInput) error {
			return nil
		},
		activity.RegisterOptions{Name: "StartSegment"},
	)
	reg.RegisterActivityWithOptions(
		func(context.Context, w.FinalizeInput) error {
			return nil
		},
		activity.RegisterOptions{Name: "FinalizeRun"},
	)
	reg.RegisterActivityWithOptions(
		func(context.Context, w.StatusInput) error {
			return nil
		},
		activity.RegisterOptions{Name: "SetRunStatus"},
	)
	reg.RegisterActivityWithOptions(
		func(context.Context, w.NotifyInput) error {
			return nil
		},
		activity.RegisterOptions{Name: "NotifyInteraction"},
	)
	reg.RegisterActivityWithOptions(
		func(context.Context, w.DecisionInput) error {
			return nil
		},
		activity.RegisterOptions{Name: "ApplyDecision"},
	)
	reg.RegisterActivityWithOptions(
		func(context.Context, w.SandboxStateInput) error {
			return nil
		},
		activity.RegisterOptions{Name: "RecordSandboxState"},
	)
	lease := func() contract.SandboxLease {
		return contract.SandboxLease{
			Provider: "opensandbox", SandboxId: "fixture", ExpiresAt: time.Now().Add(time.Hour)}
	}
	reg.RegisterActivityWithOptions(
		func(context.Context, contract.SandboxAcquireInput) (contract.SandboxLease, error) {
			return lease(), nil
		},
		activity.RegisterOptions{Name: contract.ActivitySandboxCreate},
	)
	reg.RegisterActivityWithOptions(
		func(context.Context, contract.SandboxConfigureInput) (contract.SandboxLease, error) {
			return lease(), nil
		},
		activity.RegisterOptions{Name: contract.ActivitySandboxConfigure},
	)
	reg.RegisterActivityWithOptions(
		func(context.Context, contract.SandboxOpInput) (contract.SandboxInspectResult, error) {
			return contract.SandboxInspectResult{Exists: true}, nil
		},
		activity.RegisterOptions{Name: contract.ActivitySandboxInspect},
	)
	reg.RegisterActivityWithOptions(
		func(context.Context, contract.SandboxOpInput) (contract.SandboxSnapshotResult, error) {
			return contract.SandboxSnapshotResult{SnapshotId: "snapshot"}, nil
		},
		activity.RegisterOptions{Name: contract.ActivitySandboxSnapshot},
	)
	for _, name := range []string{contract.ActivitySandboxPause, contract.ActivitySandboxDestroy} {
		reg.RegisterActivityWithOptions(
			func(context.Context, contract.SandboxOpInput) error {
				return nil
			},
			activity.RegisterOptions{Name: name},
		)
	}
	reg.RegisterActivityWithOptions(func(context.Context, contract.SandboxSnapshotDeleteInput) error {
		return nil
	},
		activity.RegisterOptions{Name: contract.ActivitySandboxSnapshotDelete})
}
func devClient(t *testing.T) client.Client {
	t.Helper()
	address := os.Getenv("ORBIT_TEST_TEMPORAL_ADDRESS")
	if address == "" {
		address = "127.0.0.1:57233"
	}
	ns := os.Getenv("ORBIT_TEST_TEMPORAL_NAMESPACE")
	if ns == "" {
		ns = "orbit-v2-wp6b-histories"
	}
	c, err := client.Dial(client.Options{HostPort: address, Namespace: ns,
		Interceptors: []interceptor.ClientInterceptor{temporalx.TemporalTracing()}})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	_, err = c.WorkflowService().RegisterNamespace(t.Context(), &workflowservice.RegisterNamespaceRequest{
		Namespace: ns, WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	if _, exists := err.(*serviceerror.NamespaceAlreadyExists); !exists {
		require.NoError(t, err)
	}
	for _, name := range []string{"TenantId", "WorkspaceId", "RunKind"} {
		_, err = c.OperatorService().AddSearchAttributes(t.Context(), &operatorservice.AddSearchAttributesRequest{
			Namespace: ns, SearchAttributes: map[string]enums.IndexedValueType{name: enums.INDEXED_VALUE_TYPE_KEYWORD},
		})
		if _, exists := err.(*serviceerror.AlreadyExists); !exists {
			require.NoError(t, err)
		}
	}
	return c
}
func runInput(name string) w.RunInput {
	limits := w.DefaultLimits(200000)
	limits.RemindAfter = 2 * time.Second
	limits.EscalateAfter = 4 * time.Second
	limits.ExpireAfter = 6 * time.Second
	return w.RunInput{
		V:                  1,
		TenantID:           "tenant",
		WorkspaceID:        "workspace",
		SessionID:          "session",
		RunID:              name,
		Member:             "main",
		Lock:               contract.SegmentLockRef{Id: "lock", Digest: "sha256:" + strings.Repeat("0", 64)},
		AgentSessionKey:    "sess:session:000000000000",
		InputRef:           "t/tenant/runs/" + name + "/input/message.json",
		EventSubject:       "run.tenant." + name,
		BudgetMillicredits: 200000,
		Limits:             limits,
		Sandbox:            w.SandboxSpec{IsolationKey: "isolation", Profile: "default", TTLSeconds: 1800},
		Tier:               "standard",
	}
}
func TestRecordHistories(t *testing.T) {
	recordingProvider(t)
	c := devClient(t)
	queue := "orbit-v2-wp6b-" + time.Now().Format("150405.000000")
	f := &fixtures{calls: map[string]int{}}
	ctl := worker.New(c, queue, worker.Options{})
	registerFixtures(ctl, f)
	// Child workflows and Activities retain their public, cross-language task queues in the isolated namespace.
	runWorker := worker.New(c, contract.TaskQueueCtl, worker.Options{})
	registerFixtures(runWorker, f)
	agent := worker.New(c, contract.TaskQueueAgentStandard, worker.Options{})
	agent.RegisterActivityWithOptions(f.segment, activity.RegisterOptions{Name: contract.ActivityRunAgentSegment})
	sandbox := worker.New(c, contract.TaskQueueSandbox, worker.Options{})
	registerFixtures(sandbox, f)
	for _, reg := range []worker.Worker{ctl, runWorker, agent, sandbox} {
		require.NoError(t, reg.Start())
		t.Cleanup(reg.Stop)
	}
	for _, name := range []string{
		"complete",
		"yield",
		"approval",
		"expiry",
		"cancel",
		"pause",
		"steer",
		"can",
		"sandbox",
		"compact",
	} {
		t.Run(name, func(t *testing.T) { recordRun(t, c, queue, name, f) })
	}
	t.Run("usage-rollup", func(t *testing.T) { recordUsageRollup(t, c, queue) })
	t.Run("trace-session", func(t *testing.T) { recordTracedSession(t, c, queue) })
	t.Run("session", func(t *testing.T) { recordSession(t, c, queue, "session") })
	t.Run("session-subject", func(t *testing.T) { recordSession(t, c, queue, "session-subject") })
	t.Run("lease", func(t *testing.T) { recordLease(t, c, queue) })
	for _, archived := range []bool{false, true} {
		name := "lease-maintenance-can"
		if archived {
			name = "lease-archive-can"
		}
		t.Run(name, func(t *testing.T) { recordLeaseContinuation(t, c, queue, name, archived) })
	}
}
func recordRun(t *testing.T, c client.Client, queue, name string, fixture *fixtures) {
	in := runInput(name)
	run, err := c.ExecuteWorkflow(
		t.Context(),
		client.StartWorkflowOptions{ID: queue + ":" + name, TaskQueue: queue},
		w.AgentRunWorkflow,
		in,
	)
	require.NoError(t, err)
	firstRunID := run.GetRunID()
	switch name {
	case "approval":
		waitProgress(t, c, run.GetID(), func(p w.RunProgress) bool { return p.PendingInteractionID != "" })
		var out w.ResolveInteractionOutput
		updateWorkflow(
			t,
			c,
			run.GetID(),
			w.ResolveInteraction,
			"approval",
			w.ResolveInteractionInput{
				InteractionID: "approval",
				ActorID:       "owner",
				Decision: contract.Decision{
					Kind:   "tool_approval",
					Choice: new(contract.DecisionChoice("allow_once")),
				},
			},
			&out,
		)
	case "cancel", "pause", "steer":
		require.Eventually(t, func() bool {
			fixture.Lock()
			defer fixture.Unlock()
			return fixture.calls[in.RunID] > 0
		}, 10*time.Second, 10*time.Millisecond)
		if name == "cancel" {
			require.NoError(t, c.SignalWorkflow(t.Context(), run.GetID(), "", w.Cancel, w.CancelInput{Reason: "user"}))
		}
		if name == "pause" {
			require.NoError(t, c.SignalWorkflow(t.Context(), run.GetID(), "", w.Pause, w.ActorInput{}))
			waitProgress(t, c, run.GetID(), func(p w.RunProgress) bool { return p.Status == "paused" })
			require.NoError(t, c.SignalWorkflow(t.Context(), run.GetID(), "", w.Resume, w.ActorInput{}))
		}
		if name == "steer" {
			updateWorkflow(t, c, run.GetID(), w.Steer, "steer", w.SteerInput{ContentRef: "t/tenant/runs/steer/steer/message"}, nil)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	require.NoError(t, run.Get(ctx, nil))
	save(t, c, run.GetID(), firstRunID, name)
}
func waitProgress(t *testing.T, c client.Client, id string, ready func(w.RunProgress) bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		value, err := c.QueryWorkflow(t.Context(), id, "", w.Progress)
		if err != nil {
			return false
		}
		var p w.RunProgress
		return value.Get(&p) == nil && ready(p)
	}, 10*time.Second, 20*time.Millisecond)
}
func updateWorkflow(t *testing.T, c client.Client, id, name, key string, in, out any) {
	t.Helper()
	handle, err := c.UpdateWorkflow(
		t.Context(),
		client.UpdateWorkflowOptions{WorkflowID: id, UpdateName: name, UpdateID: key,
			WaitForStage: client.WorkflowUpdateStageCompleted, Args: []any{in}},
	)
	require.NoError(t, err)
	require.NoError(t, handle.Get(t.Context(), out))
}
func recordSession(t *testing.T, c client.Client, queue, name string) {
	sessionID := queue + ":" + name
	run, err := c.ExecuteWorkflow(
		t.Context(),
		client.StartWorkflowOptions{ID: sessionID, TaskQueue: queue},
		w.SessionWorkflow,
		w.SessionInput{
			V:           1,
			TenantID:    "tenant",
			WorkspaceID: "workspace",
			SessionID:   sessionID,
			Policy:      "queue",
		},
	)
	require.NoError(t, err)
	for _, name := range []string{"pause", "complete"} {
		in := runInput(name)
		msg := w.SubmitMessageInput{V: 1, RunID: sessionID + name, Lock: in.Lock, BudgetMillicredits: in.BudgetMillicredits,
			Limits: in.Limits, Sandbox: in.Sandbox}
		updateWorkflow(t, c, run.GetID(), w.SubmitMessage, name, msg, nil)
	}
	require.Eventually(t, func() bool {
		value, err := c.QueryWorkflow(t.Context(), run.GetID(), "", w.State)
		if err != nil {
			return false
		}
		var state w.SessionState
		return value.Get(&state) == nil && state.ActiveRunID == "" && state.QueueLen == 0
	}, 15*time.Second, 20*time.Millisecond)
	for _, name := range []string{"pause", "complete"} {
		child, err := c.DescribeWorkflowExecution(t.Context(), w.AgentRunWorkflowID("tenant", sessionID+name), "")
		require.NoError(t, err)
		require.Equal(t, enums.WORKFLOW_EXECUTION_STATUS_COMPLETED, child.WorkflowExecutionInfo.Status)
	}
	require.NoError(t, c.SignalWorkflow(t.Context(), run.GetID(), "", w.Archive, w.ArchiveInput{}))
	require.NoError(t, run.Get(t.Context(), nil))
	save(t, c, run.GetID(), run.GetRunID(), name)
}
func recordLease(t *testing.T, c client.Client, queue string) {
	run, err := c.ExecuteWorkflow(
		t.Context(),
		client.StartWorkflowOptions{ID: queue + ":lease", TaskQueue: queue},
		w.SandboxLeaseWorkflow,
		w.LeaseInput{TenantID: "tenant", IsolationKey: "isolation"},
	)
	require.NoError(t, err)
	ac := w.AcquireInput{
		Provider:   "opensandbox",
		RunID:      "lease-run",
		SessionID:  "session",
		Profile:    "default",
		TTLSeconds: 3600,
		SpecDigest: "digest",
	}
	updateWorkflow(t, c, run.GetID(), w.Acquire, "one", ac, nil)
	require.NoError(t, c.SignalWorkflow(t.Context(), run.GetID(), "", w.Touch, w.TouchInput{RunID: ac.RunID, InUse: false}))
	require.Eventually(t, func() bool {
		value, err := c.QueryWorkflow(t.Context(), run.GetID(), "", w.Lease)
		if err != nil {
			return false
		}
		var state w.LeaseCarry
		return value.Get(&state) == nil && state.State == "snapshotted"
	}, 16*time.Minute, time.Second)
	updateWorkflow(t, c, run.GetID(), w.Acquire, "restore", ac, nil)
	require.NoError(t, c.SignalWorkflow(t.Context(), run.GetID(), "", w.Archive, w.ArchiveInput{}))
	require.NoError(t, run.Get(t.Context(), nil))
	save(t, c, run.GetID(), run.GetRunID(), "lease")
}
func save(t *testing.T, c client.Client, id, runID, name string) {
	t.Helper()
	iterator := c.GetWorkflowHistory(t.Context(), id, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	history := &historypb.History{}
	for iterator.HasNext() {
		ev, err := iterator.Next()
		require.NoError(t, err)
		history.Events = append(history.Events, ev)
	}
	require.NotEmpty(t, history.Events)
	replayer, err := worker.NewWorkflowReplayerWithOptions(
		worker.WorkflowReplayerOptions{Interceptors: append(teamrun.WorkerOptions().Interceptors, temporalx.TemporalTracing())})
	require.NoError(t, err)
	replayer.RegisterWorkflowWithOptions(agentrun.Run, workflow.RegisterOptions{Name: w.AgentRunWorkflow})
	replayer.RegisterWorkflowWithOptions(session.Run, workflow.RegisterOptions{Name: w.SessionWorkflow})
	replayer.RegisterWorkflowWithOptions(sandboxlease.Run, workflow.RegisterOptions{Name: w.SandboxLeaseWorkflow})
	replayer.RegisterWorkflowWithOptions(teamrun.Run, workflow.RegisterOptions{Name: w.TeamRunWorkflow})
	replayer.RegisterWorkflowWithOptions(membertask.Run, workflow.RegisterOptions{Name: w.MemberTaskWorkflow})
	replayer.RegisterWorkflowWithOptions(scheduledrun.Run, workflow.RegisterOptions{Name: w.ScheduledRunWorkflow})
	replayer.RegisterWorkflowWithOptions(usagerollup.Run, workflow.RegisterOptions{Name: w.UsageRollupWorkflow})
	require.NoError(t, replayer.ReplayWorkflowHistoryWithOptions(nil, history,
		worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: id, RunID: runID}}))
	if *update {
		raw, err := protojson.MarshalOptions{Indent: "  "}.Marshal(history)
		require.NoError(t, err)
		path := filepath.Join("..", "testdata", "histories", name+".json")
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, raw, 0644))
	}
}

func recordLeaseContinuation(t *testing.T, c client.Client, queue, name string, archived bool) {
	carry := &w.LeaseCarry{
		State: "paused", Gen: 2, Touches: 1000, Archived: archived,
		Lease:   &contract.SandboxLease{Provider: "opensandbox", SandboxId: "fixture"},
		Spec:    w.AcquireInput{Provider: "opensandbox", RunID: "lease-run", SessionID: "session", Profile: "default", TTLSeconds: 3600},
		RetryAt: time.Now().Add(2 * time.Second), Backoff: time.Minute, Dirty: true, PendingRestored: new(true),
		SnapshotPending: true, SnapshotID: "snapshot", ObsoleteSnapshot: "old-snapshot",
	}
	run, err := c.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{ID: queue + ":" + name, TaskQueue: queue},
		w.SandboxLeaseWorkflow, w.LeaseInput{TenantID: "tenant", IsolationKey: "isolation", Carry: carry})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		d, err := c.DescribeWorkflowExecution(t.Context(), run.GetID(), run.GetRunID())
		return err == nil && d.WorkflowExecutionInfo.Status == enums.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
	}, 10*time.Second, 100*time.Millisecond)
	save(t, c, run.GetID(), run.GetRunID(), name)
	if !archived {
		require.NoError(t, c.SignalWorkflow(t.Context(), run.GetID(), "", w.Archive, w.ArchiveInput{}))
	}
	require.NoError(t, c.GetWorkflow(t.Context(), run.GetID(), "").Get(t.Context(), nil))
}

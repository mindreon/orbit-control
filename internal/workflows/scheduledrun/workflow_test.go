package scheduledrun

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

const fire = "2026-03-01T09:00:00Z"

type rig struct {
	t        *testing.T
	env      *testsuite.TestWorkflowEnvironment
	mu       sync.Mutex
	order    []string
	sessions []w.ScheduledSessionInput
	agents   []w.RunInput
	teams    []w.TeamRunInput
	children []string
	canceled bool
	start    w.ScheduledStart
	childErr error
}

func newRig(t *testing.T, start w.ScheduledStart) *rig {
	t.Helper()
	r := &rig{t: t, start: start}
	var suite testsuite.WorkflowTestSuite
	r.env = suite.NewTestWorkflowEnvironment()
	r.env.SetStartTime(time.Date(2026, 3, 1, 9, 0, 30, 0, time.UTC))
	r.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: w.ScheduledRunWorkflowIDBase("tenant", "schedule") + "-" + fire})
	r.env.RegisterWorkflowWithOptions(func(ctx workflow.Context, in w.RunInput) error {
		return r.child(ctx, "agent", func() { r.agents = append(r.agents, in) })
	}, workflow.RegisterOptions{Name: w.AgentRunWorkflow})
	r.env.RegisterWorkflowWithOptions(func(ctx workflow.Context, in w.TeamRunInput) error {
		return r.child(ctx, "team", func() { r.teams = append(r.teams, in) })
	}, workflow.RegisterOptions{Name: w.TeamRunWorkflow})
	r.env.RegisterActivityWithOptions(func(_ context.Context, in w.ScheduledSessionInput) (w.ScheduledStart, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.sessions = append(r.sessions, in)
		return r.start, nil
	}, activity.RegisterOptions{Name: "CreateScheduledSession"})
	r.env.RegisterActivityWithOptions(func(context.Context, w.RecordScheduleInput) error {
		r.mu.Lock()
		r.order = append(r.order, "record")
		r.mu.Unlock()
		return nil
	}, activity.RegisterOptions{Name: "RecordScheduleRun"})
	r.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", w.Archive, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		r.mu.Lock()
		r.order = append(r.order, "archive:"+args.String(1))
		r.mu.Unlock()
	})
	return r
}
func (r *rig) child(ctx workflow.Context, kind string, record func()) error {
	r.mu.Lock()
	record()
	r.children = append(r.children, workflow.GetInfo(ctx).WorkflowExecution.ID)
	r.mu.Unlock()
	if r.childErr != nil {
		return r.childErr
	}
	if err := workflow.Sleep(ctx, time.Hour); err != nil {
		r.mu.Lock()
		r.canceled = true
		r.mu.Unlock()
		return err
	}
	return nil
}
func (r *rig) run() error {
	r.env.ExecuteWorkflow(Run, w.ScheduledRunInput{TenantID: "tenant", ScheduleID: "schedule"})
	require.True(r.t, r.env.IsWorkflowCompleted())
	return r.env.GetWorkflowError()
}

func agentStart() w.ScheduledStart {
	return w.ScheduledStart{Kind: "agent", Run: &w.RunInput{V: 1, TenantID: "tenant", WorkspaceID: "workspace", SessionID: "session",
		RunID: "run", Lock: contract.SegmentLockRef{Id: "lock", Digest: "sha256:" + strings.Repeat("0", 64)}, Unattended: true}}
}
func teamStart() w.ScheduledStart {
	return w.ScheduledStart{Kind: "team", Team: &w.TeamRunInput{V: 1, TenantID: "tenant", WorkspaceID: "workspace", SessionID: "session",
		RunID: "run", Unattended: true, Lock: contract.SegmentLockRef{Id: "lock", Digest: "sha256:" + strings.Repeat("0", 64)},
		Team: w.TeamSpec{Leader: "leader", MaxParallel: 1}}}
}

func TestChildReceivesTheActivityInputAndIsArchivedBeforeRecording(t *testing.T) {
	// T-S1 (§6.3): the child gets exactly the ScheduledStart input and the session is archived before the fire is recorded.
	for name, start := range map[string]w.ScheduledStart{"agent": agentStart(), "team": teamStart()} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t, start)
			require.NoError(t, r.run())
			if name == "agent" {
				require.Equal(t, []w.RunInput{*start.Run}, r.agents)
				require.Equal(t, []string{w.AgentRunWorkflowID("tenant", "run")}, r.children)
			} else {
				require.Equal(t, []w.TeamRunInput{*start.Team}, r.teams)
				require.Equal(t, []string{w.TeamRunWorkflowID("tenant", "run")}, r.children)
			}
			require.Equal(t, []string{"archive:" + w.LeaseWorkflowID("tenant", w.IsolationKey("tenant", "session")), "record"}, r.order)
		})
	}
}

func TestSkippedFireStartsNothing(t *testing.T) {
	// T-S2: an inactive or deleted schedule skips the fire without a child, archive or record.
	for _, reason := range []string{"inactive", "deleted"} {
		t.Run(reason, func(t *testing.T) {
			r := newRig(t, w.ScheduledStart{Skip: reason})
			require.NoError(t, r.run())
			require.Empty(t, r.children)
			require.Empty(t, r.order)
		})
	}
}

func TestActivityReceivesWorkflowTime(t *testing.T) {
	// T-S3: the Activity decides staleness from workflow time and the fire time parsed from the workflow ID.
	r := newRig(t, w.ScheduledStart{Skip: "inactive"})
	require.NoError(t, r.run())
	require.Len(t, r.sessions, 1)
	require.Equal(t, time.Date(2026, 3, 1, 9, 0, 30, 0, time.UTC), r.sessions[0].Now)
	require.Equal(t, time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC), r.sessions[0].FireAt)
}

func TestSameFireStartsTheSameChild(t *testing.T) {
	// T-S5: a retried fire derives the same child workflow ID, so a second start is a duplicate rather than a second run.
	first, second := newRig(t, agentStart()), newRig(t, agentStart())
	require.NoError(t, first.run())
	require.NoError(t, second.run())
	require.Equal(t, first.children, second.children)
}

func TestChildFailureDoesNotFailTheSchedule(t *testing.T) {
	// T-S6: business failure is projected by the child; the scheduled workflow still records the fire.
	r := newRig(t, agentStart())
	r.childErr = temporal.NewNonRetryableApplicationError("boom", "BOOM", nil)
	require.NoError(t, r.run())
	require.Equal(t, "record", r.order[len(r.order)-1])
}

func TestInvalidFireTimeSuffix(t *testing.T) {
	// T-S7: a workflow ID without a valid fire timestamp is rejected before any Activity runs.
	for _, id := range []string{"schedrun:tenant:schedule-garbage", "schedrun:tenant:schedule-2026-13-01T09:00:00Z", "other"} {
		r := newRig(t, agentStart())
		r.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: id})
		err := r.run()
		var app *temporal.ApplicationError
		require.ErrorAs(t, err, &app, id)
		require.Equal(t, "SCHEDULE_FIRE_TIME_INVALID", app.Type())
		require.Empty(t, r.sessions)
	}
}

func TestParentCancellationCancelsTheChild(t *testing.T) {
	// T-S8 (F52): cancelling the schedule action requests cancellation of the child and still records the fire.
	r := newRig(t, agentStart())
	r.env.RegisterDelayedCallback(func() { r.env.CancelWorkflow() }, 10*time.Second)
	_ = r.run()
	require.True(t, r.canceled)
	require.Equal(t, "record", r.order[len(r.order)-1])
}

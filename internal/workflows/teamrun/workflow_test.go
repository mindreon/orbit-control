package teamrun

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func TestLeaderWithoutTasks(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(func(workflow.Context, w.MemberTaskInput) (w.TurnResult, error) { return w.TurnResult{}, nil }, workflow.RegisterOptions{Name: w.MemberTaskWorkflow})
	env.RegisterActivityWithOptions(func(context.Context, w.PrepareTeamInput) error { return nil }, activity.RegisterOptions{Name: "PrepareTeamRun"})
	env.RegisterActivityWithOptions(func(context.Context, w.AcquireSandboxInput) (contract.SandboxLease, error) {
		return contract.SandboxLease{Provider: "local", SandboxId: "sandbox"}, nil
	}, activity.RegisterOptions{Name: "AcquireSandbox"})
	env.RegisterActivityWithOptions(func(context.Context, w.RecordBoardInput) error { return nil }, activity.RegisterOptions{Name: "RecordBoard"})
	env.RegisterActivityWithOptions(func(_ context.Context, in w.TurnOp) (w.TurnResult, error) {
		return w.TurnResult{Member: in.Member, Turn: in.Turn, Status: "completed", Cursor: w.MemberCursor{Turn: in.Turn}, Final: &contract.FinalMessage{MessageId: "summary", TextPreview: "done"}}, nil
	}, activity.RegisterOptions{Name: "WaitForTurn"})
	var final w.FinalizeTeamInput
	env.RegisterActivityWithOptions(func(_ context.Context, in w.FinalizeTeamInput) error { final = in; return nil }, activity.RegisterOptions{Name: "FinalizeTeamRun"})
	env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", mock.Anything, mock.Anything).Return(nil)
	in := w.TeamRunInput{V: 1, TenantID: "tenant", WorkspaceID: "workspace", SessionID: "session", RunID: "run",
		Lock:               contract.SegmentLockRef{Id: "lock", Digest: "sha256:" + strings.Repeat("0", 64)},
		BudgetMillicredits: 100000, Limits: w.DefaultLimits(100000), Tier: "standard",
		Team: w.TeamSpec{Roster: []contract.RosterEntry{{Name: "leader", Role: "leader", Status: "idle", Description: "lead"}}, Leader: "leader", MaxParallel: 1, ReviewMaxRounds: 3}}
	env.ExecuteWorkflow(Run, in)
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, "succeeded", final.Status)
	require.Equal(t, "done", final.Summary)
}

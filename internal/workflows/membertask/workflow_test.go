package membertask

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func TestMemberCursorAndOwnership(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var started w.StartSegmentInput
	var segment contract.SegmentInput
	env.RegisterActivityWithOptions(func(_ context.Context, in w.StartSegmentInput) error {
		started = in
		return nil
	}, activity.RegisterOptions{Name: "StartSegment"})
	env.RegisterActivityWithOptions(func(_ context.Context, in contract.SegmentInput) (contract.SegmentResult, error) {
		segment = in
		return contract.SegmentResult{V: 1, Status: "completed", CheckpointVersion: 8,
			Progress: contract.SegmentProgress{NewOutput: true}}, nil
	}, activity.RegisterOptions{Name: contract.ActivityRunAgentSegment})
	env.RegisterActivityWithOptions(func(context.Context, w.RecordSegmentInput) (w.RecordSegmentOutput, error) {
		return w.RecordSegmentOutput{RunUsedMillicredits: 200}, nil
	}, activity.RegisterOptions{Name: "RecordSegment"})
	env.OnSignalExternalWorkflow(mock.Anything, w.LeaseWorkflowID("tenant", "isolation"), "", w.Touch,
		mock.MatchedBy(func(in w.TouchInput) bool { return in.Holder == "run/alice" })).Return(nil)
	in := w.MemberTaskInput{V: 1, TenantID: "tenant", RunID: "run", SessionID: "session", Member: "alice",
		Role: "teammate", Turn: 2, Kind: "task", Cursor: w.MemberCursor{Seq: 4, CheckpointVersion: 7, EventN: 30},
		Lock:         contract.SegmentLockRef{Id: "lock", Digest: "sha256:" + strings.Repeat("0", 64)},
		EventSubject: "run.tenant.session.run", WorkspaceID: "workspace", PrincipalID: "owner",
		BudgetMillicredits: 10000, Limits: w.DefaultLimits(10000), Tier: "standard",
		Input: contract.SegmentTurn{Kind: "follow_up"}, Sandbox: w.SandboxSpec{IsolationKey: "isolation"},
		Lease: contract.SandboxLease{Provider: "local", SandboxId: "sandbox"}}
	env.ExecuteWorkflow(Run, in)
	require.NoError(t, env.GetWorkflowError())
	var result w.TurnResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, int64(31), started.EventN)
	require.Equal(t, 5, segment.Seq)
	require.Equal(t, 7, segment.ExpectedCheckpointVersion)
	require.Equal(t, "team:run:alice", segment.AgentSessionKey)
	require.Equal(t, contract.TeamRole("teammate"), segment.Team.Role)
	require.Equal(t, "completed", result.Status)
	require.Equal(t, 5, result.Cursor.Seq)
	require.Equal(t, int64(200), result.RunUsedMillicredits)
	// Unregistered PrepareRun/AcquireSandbox/FinalizeRun would fail execution.
}

func TestDeliveryDeduplication(t *testing.T) {
	st := &w.RunCarry{DeliveredMail: []string{"old"}, DeliveredSteer: []string{"old-ref"}}
	in := w.DeliverInput{Mailbox: []contract.MailboxMessage{
		{MessageId: "old"}, {MessageId: "new", From: "leader", SentAt: time.Unix(1, 0)},
	}, Steer: []contract.SteerItem{{ContentRef: new("old-ref")}, {ContentRef: new("new-ref")}}}
	deliver(st, in)
	deliver(st, in)
	require.Len(t, st.Mailbox, 1)
	require.Equal(t, "new", st.Mailbox[0].MessageId)
	require.Len(t, st.Steer, 1)
}

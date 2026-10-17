package membertask

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
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

func TestEndTurnDuringSegmentStopsAfterCommit(t *testing.T) {
	// T-M2 (§1.5): EndTurn that arrives while a segment runs lets the segment commit and then ends the turn.
	r := newRig(t, step{after: 5 * sec, result: yielded(8)})
	r.at(sec, func() { endTurn(r) })
	result, err := r.run()
	require.NoError(t, err)
	require.Len(t, r.segments, 1)
	require.Equal(t, "ended", result.Status)
}

func TestDeliverDuringSegmentSchedulesFollowUp(t *testing.T) {
	// T-M3 (§1.5): mail delivered while a segment runs becomes the next segment's follow_up and is reported delivered.
	r := newRig(t, step{after: 5 * sec, result: completed(8)}, step{result: completed(9)})
	r.at(sec, func() { deliverMail(r, "m1") })
	result, err := r.run()
	require.NoError(t, err)
	require.Len(t, r.segments, 2)
	require.Empty(t, r.segments[0].Mailbox)
	require.Equal(t, contract.SegmentTurnKind("follow_up"), r.segments[1].Input.Kind)
	require.Equal(t, "m1", r.segments[1].Mailbox[0].MessageId)
	require.Equal(t, []string{"m1"}, result.DeliveredMail)
	require.Equal(t, "completed", result.Status)
}

func TestMailArrivingBeforeCommitRunsAnotherSegment(t *testing.T) {
	// T-M4 (§1.5): a completed segment with undelivered mail does not end the turn.
	r := newRig(t, step{result: completed(8)}, step{result: completed(9)})
	r.env.OnActivity("RecordSegment", mock.Anything, mock.Anything).After(3*sec).Return(w.RecordSegmentOutput{RunUsedMillicredits: 200}, nil)
	r.at(2*sec, func() { deliverMail(r, "late") })
	result, err := r.run()
	require.NoError(t, err)
	require.Len(t, r.segments, 2)
	require.Equal(t, contract.SegmentTurnKind("follow_up"), r.segments[1].Input.Kind)
	require.Equal(t, []string{"late"}, result.DeliveredMail)
}

func hitl(t *testing.T) (*rig, w.TurnResult) {
	r := newRig(t,
		step{result: contract.SegmentResult{V: 1, Status: "interrupted", CheckpointVersion: 8,
			Interaction: &contract.InteractionRequest{InteractionId: "i1", Kind: "tool_approval"}}},
		step{result: completed(9)})
	r.at(5*sec, func() {
		r.env.UpdateWorkflow(w.ResolveInteraction, "resolve", &testsuite.TestUpdateCallback{
			OnAccept: func() {}, OnReject: func(err error) { t.Errorf("resolve rejected: %v", err) }, OnComplete: func(any, error) {},
		}, w.ResolveInteractionInput{InteractionID: "i1", ActorID: "owner", Decision: contract.Decision{Kind: "tool_approval", Choice: new(contract.DecisionChoice("allow_once"))}})
	})
	result, err := r.run()
	require.NoError(t, err)
	return r, result
}

func TestInteractionWaitsForResolve(t *testing.T) {
	// T-M5 (§1.5): the member records the interaction, waits for its decision, then continues.
	r, result := hitl(t)
	require.Len(t, r.interactions, 1)
	require.Equal(t, "i1", r.interactions[0].Request.InteractionId)
	require.Len(t, r.segments, 2)
	require.Equal(t, contract.SegmentTurnKind("interactive_input"), r.segments[1].Input.Kind)
	require.Equal(t, "completed", result.Status)
}

func TestInteractionIsOwnedByTheMemberWorkflow(t *testing.T) {
	// T-M11 (F21): CreateInteraction runs under the member workflow's ID, which control stores as
	// interactions.workflow_id so a decision is routed there.
	r, _ := hitl(t)
	var found bool
	for _, info := range r.infos {
		if info.ActivityType.Name == "CreateInteraction" {
			found = true
			require.Equal(t, w.MemberTaskWorkflowID("tenant", "run", "alice", 2), info.WorkflowExecution.ID)
		}
	}
	require.True(t, found)
}

func TestUnattendedInteractionFailsWithoutWaiting(t *testing.T) {
	// T-M6 (F26): an unattended member never waits for a human; the turn fails with UNATTENDED_INTERACTION.
	r := newRig(t, step{result: contract.SegmentResult{V: 1, Status: "interrupted", CheckpointVersion: 8,
		Interaction: &contract.InteractionRequest{InteractionId: "i1", Kind: "effect_uncertain"}}})
	r.in.Unattended = true
	result, err := r.run()
	require.NoError(t, err)
	require.Equal(t, "failed", result.Status)
	require.Equal(t, "UNATTENDED_INTERACTION", result.Code)
	require.Len(t, r.interactions, 1)
	require.Len(t, r.cancelled, 1)
	require.Equal(t, "i1", r.cancelled[0].InteractionID)
}

func TestReacquireUsesMemberHolderAndAttempt(t *testing.T) {
	// T-M8 (F43): replacing a lost sandbox acquires under the member's holder with Attempt 1 (Attempt 0 would
	// mean "no reacquire").
	r := newRig(t, step{err: sandboxUnavailable()}, step{result: completed(8)})
	result, err := r.run()
	require.NoError(t, err)
	require.Equal(t, "completed", result.Status)
	require.Len(t, r.acquires, 1)
	require.Equal(t, "run/alice", r.acquires[0].Holder)
	require.Equal(t, 2, r.acquires[0].Turn)
	require.Equal(t, 1, r.acquires[0].Attempt)
}

func TestMemberNeverOwnsRunLifecycle(t *testing.T) {
	// T-M9 (F50): whatever the outcome, a member turn never prepares the run, acquires the first sandbox or finalizes the run.
	for name, step := range map[string]step{
		"completed": {result: completed(8)},
		"failed":    {result: contract.SegmentResult{V: 1, Status: "failed", CheckpointVersion: 8, Error: &contract.SegmentError{Code: "model_error"}}},
		"policy":    {err: temporal.NewNonRetryableApplicationError("denied", contract.ErrorPolicyDenied, nil)},
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t, step)
			_, err := r.run()
			require.NoError(t, err)
			for _, forbidden := range []string{"PrepareRun", "FinalizeRun", "AcquireSandbox"} {
				require.Zero(t, r.called(forbidden), forbidden)
			}
		})
	}
}

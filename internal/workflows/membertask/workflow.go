package membertask

import (
	"slices"

	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/segloop"
)

func Run(ctx workflow.Context, in w.MemberTaskInput) (w.TurnResult, error) {
	carry := in.Carry
	if carry == nil {
		carry = &w.RunCarry{Status: "running", CommittedSeq: in.Cursor.Seq, ContinuedAt: in.Cursor.Seq,
			CheckpointVersion: in.Cursor.CheckpointVersion, EventN: in.Cursor.EventN, ActiveSeconds: in.Cursor.ActiveSeconds,
			UsedMillicredits: in.RunUsedMillicredits, PendingTurn: in.Input, Mailbox: in.Mailbox, Steer: in.Steer,
			Lease: in.Lease, Acquisitions: 1}
	}
	run := w.RunInput{V: in.V, TenantID: in.TenantID, WorkspaceID: in.WorkspaceID, SessionID: in.SessionID,
		RunID: in.RunID, PrincipalID: in.PrincipalID, Member: in.Member, Lock: in.Lock,
		AgentSessionKey: w.TeamSessionKey(in.RunID, in.Member), EventSubject: in.EventSubject,
		BudgetMillicredits: in.BudgetMillicredits, Limits: in.Limits, Sandbox: in.Sandbox, Tier: in.Tier,
		Unattended: in.Unattended, Carry: carry}
	result := w.TurnResult{Member: in.Member, Turn: in.Turn}
	hooks := segloop.Hooks{
		TrackDeliveries: true,
		Prepare:         func(workflow.Context, w.RunInput, *segloop.State) error { return nil },
		ContinueAsNew: func(ctx workflow.Context, st *segloop.State) error {
			in.Carry = st
			return workflow.NewContinueAsNewError(ctx, w.MemberTaskWorkflow, in)
		},
		Input:     func(out *contract.SegmentInput) { out.Team = &contract.TeamContext{Role: in.Role, Task: in.Task} },
		ShouldEnd: func(st *segloop.State) bool { return st.EndTurn },
		Finish: func(_ workflow.Context, st *segloop.State, status, code string) error {
			if status == "succeeded" {
				status = "completed"
				if st.EndTurn {
					status = "ended"
				}
			}
			result.Status, result.Code, result.Final = status, code, st.LastFinal
			result.Cursor = w.MemberCursor{Seq: st.CommittedSeq, CheckpointVersion: st.CheckpointVersion,
				EventN: st.EventN, Turn: in.Turn, ActiveSeconds: st.ActiveSeconds}
			result.RunUsedMillicredits = st.UsedMillicredits
			result.DeliveredMail, result.DeliveredSteer = st.DeliveredMail, st.DeliveredSteer
			return nil
		},
		Holder: w.LeaseHolder(in.RunID, in.Member), Turn: in.Turn, Drain: drain,
	}
	err := segloop.Run(ctx, run, hooks)
	return result, err
}

func drain(ctx workflow.Context, st *segloop.State) {
	ch := workflow.GetSignalChannel(ctx, w.Deliver)
	var in w.DeliverInput
	for ch.ReceiveAsync(&in) {
		deliver(st, in)
	}
	end := workflow.GetSignalChannel(ctx, w.EndTurn)
	for end.ReceiveAsync(nil) {
		st.EndTurn = true
	}
}
func deliver(st *segloop.State, in w.DeliverInput) {
	for _, item := range in.Mailbox {
		if slices.Contains(st.DeliveredMail, item.MessageId) {
			continue
		}
		if !slices.ContainsFunc(st.Mailbox, func(old contract.MailboxMessage) bool { return old.MessageId == item.MessageId }) {
			st.Mailbox = append(st.Mailbox, item)
		}
	}
	for _, item := range in.Steer {
		if item.ContentRef == nil || slices.Contains(st.DeliveredSteer, *item.ContentRef) {
			continue
		}
		if !slices.ContainsFunc(st.Steer, func(old contract.SteerItem) bool {
			return old.ContentRef != nil && *old.ContentRef == *item.ContentRef
		}) {
			st.Steer = append(st.Steer, item)
		}
	}
}

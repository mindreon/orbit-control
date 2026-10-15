package session

import (
	"fmt"
	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"
	"time"
)

func Run(ctx workflow.Context, in w.SessionInput) error {
	queue := []w.SubmitMessageInput{}
	policy := in.Policy
	if in.Carry != nil {
		queue = in.Carry.Queue
		policy = in.Carry.Policy
	}
	var active string
	var child workflow.ChildWorkflowFuture
	archived := false
	runs := 0
	finalize := func(msg w.SubmitMessageInput, reason string) error {
		dc, _ := workflow.NewDisconnectedContext(ctx)
		return workflow.ExecuteActivity(w.Ctl(dc, "FinalizeRun"), "FinalizeRun", w.FinalizeInput{RunOp: w.RunOp{TenantID: in.TenantID, SessionID: in.SessionID, RunID: msg.RunID, Member: "main", EventAt: workflow.Now(ctx), EventN: 1}, Status: "cancelled", Reason: reason, NeverStarted: true}).Get(dc, nil)
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, w.SubmitMessage, func(ctx workflow.Context, msg w.SubmitMessageInput) (w.SubmitMessageOutput, error) {
		if policy == "interrupt" && active != "" {
			if err := child.SignalChildWorkflow(ctx, w.Cancel, w.CancelInput{Reason: "interrupted_by_new_message"}).Get(ctx, nil); err != nil {
				return w.SubmitMessageOutput{}, err
			}
			queue = append([]w.SubmitMessageInput{msg}, queue...)
			return w.SubmitMessageOutput{RunID: msg.RunID, Disposition: "queued"}, nil
		}
		pos := len(queue)
		disposition := "started"
		if active != "" {
			pos++
			disposition = "queued"
		}
		queue = append(queue, msg)
		return w.SubmitMessageOutput{RunID: msg.RunID, Position: pos, Disposition: disposition}, nil
	}, workflow.UpdateHandlerOptions{Validator: func(msg w.SubmitMessageInput) error {
		if archived || len(queue) >= 20 || msg.V != 1 || msg.RunID == "" || msg.RunID == active {
			return fmt.Errorf("session unavailable or invalid message")
		}
		for _, q := range queue {
			if q.RunID == msg.RunID {
				return fmt.Errorf("duplicate run")
			}
		}
		return nil
	}}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, w.CancelRun, func(ctx workflow.Context, c w.CancelRunInput) (w.CancelRunOutput, error) {
		if active == c.RunID {
			err := child.SignalChildWorkflow(ctx, w.Cancel, w.CancelInput{ActorID: c.ActorID, Reason: c.Reason}).Get(ctx, nil)
			return w.CancelRunOutput{Disposition: "cancelling"}, err
		}
		for i, q := range queue {
			if q.RunID == c.RunID {
				queue = append(queue[:i], queue[i+1:]...)
				return w.CancelRunOutput{Disposition: "dequeued"}, finalize(q, c.Reason)
			}
		}
		return w.CancelRunOutput{}, fmt.Errorf("run missing")
	}, workflow.UpdateHandlerOptions{Validator: func(c w.CancelRunInput) error {
		if active == c.RunID && active != "" {
			return nil
		}
		for _, q := range queue {
			if q.RunID == c.RunID {
				return nil
			}
		}
		return fmt.Errorf("RUN_FINISHED")
	}}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, w.State, func() (w.SessionState, error) {
		status := "idle"
		if active != "" {
			status = "active"
		}
		if archived {
			status = "archived"
		}
		return w.SessionState{ActiveRunID: active, QueueLen: len(queue), Status: status}, nil
	}); err != nil {
		return err
	}
	workflow.Go(ctx, func(ctx workflow.Context) {
		ch := workflow.GetSignalChannel(ctx, w.SetPolicy)
		for {
			var s w.SetPolicyInput
			ch.Receive(ctx, &s)
			policy = s.Policy
		}
	})
	workflow.Go(ctx, func(ctx workflow.Context) {
		var a w.ArchiveInput
		workflow.GetSignalChannel(ctx, w.Archive).Receive(ctx, &a)
		archived = true
	})
	for {
		ok, err := workflow.AwaitWithTimeout(ctx, 7*24*time.Hour, func() bool { return archived || (active == "" && len(queue) > 0) })
		if err != nil {
			return err
		}
		if archived {
			if active != "" {
				if err = child.SignalChildWorkflow(ctx, w.Cancel, w.CancelInput{Reason: "session_archived"}).Get(ctx, nil); err != nil {
					return err
				}
				if err = workflow.Await(ctx, func() bool { return active == "" }); err != nil {
					return err
				}
			}
			for _, q := range queue {
				if err = finalize(q, "session_archived"); err != nil {
					return err
				}
			}
			_ = workflow.SignalExternalWorkflow(ctx, w.LeaseWorkflowID(in.TenantID, w.IsolationKey(in.TenantID, in.SessionID)), "", w.Archive, w.ArchiveInput{}).Get(ctx, nil)
			return nil
		}
		if !ok && active == "" && len(queue) == 0 {
			return nil
		}
		if active == "" && workflow.AllHandlersFinished(ctx) && (runs >= 100 || workflow.GetInfo(ctx).GetContinueAsNewSuggested()) {
			in.Carry = &w.SessionCarry{Queue: queue, Policy: policy}
			return workflow.NewContinueAsNewError(ctx, w.SessionWorkflow, in)
		}
		if active == "" && len(queue) > 0 {
			msg := queue[0]
			queue = queue[1:]
			active = msg.RunID
			ri := w.RunInput{V: 1, TenantID: in.TenantID, WorkspaceID: in.WorkspaceID, SessionID: in.SessionID, RunID: msg.RunID, PrincipalID: msg.PrincipalID, MessageID: msg.MessageID, Member: "main", Lock: msg.Lock, AgentSessionKey: w.AgentSessionKey(in.SessionID, msg.Lock.Digest), EventSubject: w.EventSubject(in.TenantID, msg.RunID), InputRef: msg.InputRef, BudgetMillicredits: msg.BudgetMillicredits, Limits: msg.Limits, Sandbox: msg.Sandbox, Tier: "standard"}
			cc := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: w.AgentRunWorkflowID(in.TenantID, msg.RunID), TaskQueue: contract.TaskQueueCtl, ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL, WaitForCancellation: true, WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE})
			child = workflow.ExecuteChildWorkflow(cc, w.AgentRunWorkflow, ri)
			future := child
			workflow.Go(ctx, func(ctx workflow.Context) {
				if e := future.Get(ctx, nil); e != nil {
					workflow.GetLogger(ctx).Error("agent child failed", "error", e)
				}
				active = ""
				runs++
			})
		}
	}
}

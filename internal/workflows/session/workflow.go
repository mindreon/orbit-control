package session

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type sessionWorkflow struct {
	in         w.SessionInput
	queue      []w.SubmitMessageInput
	policy     string
	active     string
	activeKind string
	child      workflow.ChildWorkflowFuture
	external   bool
	archived   bool
	runs       int
	limit      int
}

func Run(ctx workflow.Context, in w.SessionInput) error {
	limit, err := w.ContinueThreshold(ctx, w.SessionContinueAfter)
	if err != nil {
		return err
	}
	s := &sessionWorkflow{in: in, policy: in.Policy, limit: limit}
	if in.Carry != nil {
		s.queue = in.Carry.Queue
		s.policy = in.Carry.Policy
	}
	if err := s.register(ctx); err != nil {
		return err
	}
	for {
		ok, err := workflow.AwaitWithTimeout(ctx, 7*24*time.Hour, func() bool {
			return s.archived || (s.active == "" && (len(s.queue) > 0 || s.shouldContinue(ctx)))
		})
		if err != nil {
			return err
		}
		if s.archived {
			return s.drain(ctx)
		}
		if s.shouldContinue(ctx) {
			in.Carry = &w.SessionCarry{Queue: s.queue, Policy: s.policy}
			return workflow.NewContinueAsNewError(ctx, w.SessionWorkflow, in)
		}
		if !ok && s.active == "" && len(s.queue) == 0 {
			if err := archiveLease(ctx, in); err != nil {
				return err
			}
			// Submit updates may arrive while the external signal is in flight.
			if len(s.queue) == 0 && workflow.AllHandlersFinished(ctx) {
				return nil
			}
			continue
		}
		if s.active == "" && len(s.queue) > 0 {
			s.startNext(ctx)
		}
	}
}
func (s *sessionWorkflow) shouldContinue(ctx workflow.Context) bool {
	return s.active == "" && workflow.AllHandlersFinished(ctx) &&
		(s.runs >= s.limit || workflow.GetInfo(ctx).GetContinueAsNewSuggested())
}
func (s *sessionWorkflow) register(ctx workflow.Context) error {
	if err := workflow.SetUpdateHandlerWithOptions(ctx, w.SubmitMessage, s.submit,
		workflow.UpdateHandlerOptions{Validator: s.validateSubmit}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(
		ctx, w.CancelRun, s.cancel, workflow.UpdateHandlerOptions{Validator: s.validateCancel},
	); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, w.State, func() (w.SessionState, error) {
		status := "idle"
		if s.active != "" {
			status = "active"
		}
		if s.archived {
			status = "archived"
		}
		return w.SessionState{ActiveRunID: s.active, QueueLen: len(s.queue), Status: status}, nil
	}); err != nil {
		return err
	}
	workflow.Go(ctx, func(ctx workflow.Context) {
		ch := workflow.GetSignalChannel(ctx, w.SetPolicy)
		for {
			var in w.SetPolicyInput
			ch.Receive(ctx, &in)
			s.policy = in.Policy
		}
	})
	workflow.Go(ctx, func(ctx workflow.Context) {
		workflow.GetSignalChannel(ctx, w.Archive).Receive(ctx, nil)
		s.archived = true
	})
	return nil
}
func (s *sessionWorkflow) validateSubmit(in w.SubmitMessageInput) error {
	if s.archived {
		return temporal.NewNonRetryableApplicationError("Session archived", "SESSION_ARCHIVED", nil)
	}
	if in.RunID == s.active && s.active != "" {
		return temporal.NewNonRetryableApplicationError("Duplicate run", "DUPLICATE_RUN", nil)
	}
	for _, q := range s.queue {
		if q.RunID == in.RunID {
			return temporal.NewNonRetryableApplicationError("Duplicate run", "DUPLICATE_RUN", nil)
		}
	}
	if len(s.queue) >= 20 {
		return temporal.NewNonRetryableApplicationError("Session queue full", "QUEUE_FULL", nil)
	}
	if in.V != 1 || in.RunID == "" {
		return temporal.NewNonRetryableApplicationError("Invalid message", "INVALID_INPUT", nil)
	}
	return nil
}
func (s *sessionWorkflow) submit(ctx workflow.Context, msg w.SubmitMessageInput) (w.SubmitMessageOutput, error) {
	if s.policy == "interrupt" && s.active != "" {
		if err := s.signalActive(ctx, w.CancelInput{Reason: "interrupted_by_new_message"}); err != nil {
			return w.SubmitMessageOutput{}, err
		}
		s.queue = append([]w.SubmitMessageInput{msg}, s.queue...)
		return w.SubmitMessageOutput{RunID: msg.RunID, Disposition: "queued"}, nil
	}
	pos, disposition := len(s.queue), "started"
	if s.active != "" {
		pos++
		disposition = "queued"
	}
	s.queue = append(s.queue, msg)
	return w.SubmitMessageOutput{RunID: msg.RunID, Position: pos, Disposition: disposition}, nil
}
func (s *sessionWorkflow) validateCancel(in w.CancelRunInput) error {
	if s.active == in.RunID && s.active != "" {
		return nil
	}
	for _, q := range s.queue {
		if q.RunID == in.RunID {
			return nil
		}
	}
	return fmt.Errorf("RUN_FINISHED")
}
func (s *sessionWorkflow) cancel(ctx workflow.Context, in w.CancelRunInput) (w.CancelRunOutput, error) {
	if s.active == in.RunID {
		err := s.signalActive(ctx, w.CancelInput{ActorID: in.ActorID, Reason: in.Reason})
		return w.CancelRunOutput{Disposition: "cancelling"}, err
	}
	for i, q := range s.queue {
		if q.RunID == in.RunID {
			// Remove admission before blocking so the main loop cannot start a run
			// whose queued cancellation is already being finalized.
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			if err := s.finalize(ctx, q, in.Reason); err != nil {
				s.queue = append([]w.SubmitMessageInput{q}, s.queue...)
				return w.CancelRunOutput{}, err
			}
			return w.CancelRunOutput{Disposition: "dequeued"}, nil
		}
	}
	return w.CancelRunOutput{}, fmt.Errorf("run missing")
}
func (s *sessionWorkflow) signalActive(ctx workflow.Context, in w.CancelInput) error {
	if s.external {
		return workflow.SignalExternalWorkflow(ctx, w.RunWorkflowID(s.activeKind, s.in.TenantID, s.active), "", w.Cancel, in).Get(ctx, nil)
	}
	return s.child.SignalChildWorkflow(ctx, w.Cancel, in).Get(ctx, nil)
}
func (s *sessionWorkflow) finalize(ctx workflow.Context, msg w.SubmitMessageInput, reason string) error {
	dc, _ := workflow.NewDisconnectedContext(ctx)
	input := w.FinalizeInput{RunOp: w.RunOp{
		TenantID: s.in.TenantID, SessionID: s.in.SessionID, RunID: msg.RunID, Member: "main", EventAt: workflow.Now(ctx), EventN: 1,
	}, Status: "cancelled", ErrorCode: "CANCELLED", Reason: reason, NeverStarted: true}
	return workflow.ExecuteActivity(w.Ctl(dc, "FinalizeRun"), "FinalizeRun", input).Get(dc, nil)
}
func (s *sessionWorkflow) drain(ctx workflow.Context) error {
	if s.active != "" {
		if err := s.signalActive(ctx, w.CancelInput{Reason: "session_archived"}); err != nil {
			return err
		}
		if err := workflow.Await(ctx, func() bool { return s.active == "" }); err != nil {
			return err
		}
	}
	for _, q := range s.queue {
		if err := s.finalize(ctx, q, "session_archived"); err != nil {
			return err
		}
	}
	return archiveLease(ctx, s.in)
}
func archiveLease(ctx workflow.Context, in w.SessionInput) error {
	err := workflow.SignalExternalWorkflow(
		ctx,
		w.LeaseWorkflowID(in.TenantID, w.IsolationKey(in.TenantID, in.SessionID)),
		"",
		w.Archive,
		w.ArchiveInput{},
	).Get(ctx, nil)
	if _, ok := errors.AsType[*temporal.UnknownExternalWorkflowExecutionError](err); ok {
		return nil
	}
	return err
}
func (s *sessionWorkflow) runInput(ctx workflow.Context, msg w.SubmitMessageInput) w.RunInput {
	subject := w.EventSubject(s.in.TenantID, s.in.SessionID, msg.RunID)
	if workflow.GetVersion(ctx, "event-subject-session", workflow.DefaultVersion, 1) == workflow.DefaultVersion {
		subject = "run." + s.in.TenantID + "." + msg.RunID
	}
	return w.NewRunInput(s.in, msg, subject)
}
func (s *sessionWorkflow) startNext(ctx workflow.Context) {
	msg := s.queue[0]
	s.queue = s.queue[1:]
	s.active = msg.RunID
	s.activeKind = "agent"
	teamChild := msg.EntryKind == "team" && workflow.GetVersion(ctx, "session-team-child", workflow.DefaultVersion,
		1) != workflow.DefaultVersion
	if teamChild {
		s.activeKind = "team"
	}
	s.external = false
	cc := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: w.RunWorkflowID(s.activeKind, s.in.TenantID, msg.RunID), TaskQueue: contract.TaskQueueCtl,
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL, WaitForCancellation: true,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		TypedSearchAttributes: temporal.NewSearchAttributes(
			temporal.NewSearchAttributeKeyKeyword("TenantId").ValueSet(s.in.TenantID),
			temporal.NewSearchAttributeKeyKeyword("WorkspaceId").ValueSet(s.in.WorkspaceID),
			temporal.NewSearchAttributeKeyKeyword("RunKind").ValueSet(runKind(s.activeKind))),
	})
	if teamChild {
		s.child = workflow.ExecuteChildWorkflow(cc, w.TeamRunWorkflow,
			w.NewTeamRunInput(s.in, msg, w.EventSubject(s.in.TenantID, s.in.SessionID, msg.RunID)))
	} else {
		s.child = workflow.ExecuteChildWorkflow(cc, w.AgentRunWorkflow, s.runInput(ctx, msg))
	}
	future := s.child
	workflow.Go(ctx, func(ctx workflow.Context) { s.waitChild(ctx, future, msg.RunID) })
}
func (s *sessionWorkflow) waitChild(ctx workflow.Context, future workflow.ChildWorkflowFuture, runID string) {
	var execution workflow.Execution
	err := future.GetChildWorkflowExecution().Get(ctx, &execution)
	if err == nil {
		err = future.Get(ctx, nil)
	}
	_, sdkStarted := errors.AsType[*temporal.ChildWorkflowExecutionAlreadyStartedError](err)
	_, serviceStarted := errors.AsType[*serviceerror.WorkflowExecutionAlreadyStarted](err)
	if sdkStarted || serviceStarted {
		s.external = true
		// Temporal does not expose an external ChildWorkflowFuture. Wait on the
		// existing stable workflow ID with a heartbeat Activity; signal it directly.
		err = workflow.ExecuteActivity(w.Ctl(ctx, "WaitForRun"), "WaitForRun", w.RunOp{TenantID: s.in.TenantID, RunID: runID}).Get(ctx, nil)
	}

	if err != nil {
		workflow.GetLogger(ctx).Error("agent child failed", "error", err)
	}
	s.active = ""
	s.runs++
}

func runKind(kind string) string {
	if kind == "team" {
		return "team"
	}
	return "agent"
}

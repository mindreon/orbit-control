// Package segloop implements the deterministic, checkpoint-driven segment loop.
package segloop

import (
	"fmt"
	"strings"
	"time"

	"errors"
	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type State = w.RunCarry
type loop struct {
	ctx           workflow.Context
	in            w.RunInput
	st            *State
	cancelSegment workflow.CancelFunc
}

func Restore(in w.RunInput) *State {
	if in.Carry != nil {
		return in.Carry
	}
	return &State{Status: "queued", PendingTurn: contract.SegmentTurn{Kind: "user_message", ContentRef: new(in.InputRef)}}
}
func Run(ctx workflow.Context, in w.RunInput) error {
	l := &loop{ctx: ctx, in: in, st: Restore(in)}
	if err := l.register(); err != nil {
		return err
	}
	if in.Carry == nil {
		var prep w.PrepareOutput
		if err := l.exec("PrepareRun", w.PrepareInput{RunOp: l.op(), SessionKey: in.AgentSessionKey, Budget: in.BudgetMillicredits}, &prep); err != nil {
			return l.finish("failed", "PREPARE_FAILED")
		}
		l.st.CheckpointVersion = prep.CheckpointVersion
		l.st.Status = "running"
		if err := l.acquire(); err != nil {
			return l.finish("failed", "SANDBOX_UNAVAILABLE")
		}
	}
	for {
		st := l.st
		if st.Cancel != nil {
			return l.finish("cancelled", st.Cancel.Reason)
		}
		if st.Paused {
			st.Status = "paused"
			if err := l.status("paused"); err != nil {
				return err
			}
			if err := workflow.Await(ctx, func() bool { return !st.Paused || st.Cancel != nil }); err != nil {
				return l.finish("cancelled", "CANCELLED")
			}
			if st.Cancel != nil {
				continue
			}
			st.Status = "running"
			if err := l.status("running"); err != nil {
				return err
			}
			continue
		}
		if st.UsedMillicredits >= in.BudgetMillicredits {
			return l.finish("budget_exhausted", "")
		}
		if st.ActiveSeconds >= int64(in.Limits.MaxActiveSeconds) {
			return l.finish("timed_out", "")
		}
		if st.CommittedSeq >= in.Limits.MaxSegments {
			return l.finish("failed", "SEGMENT_LIMIT")
		}
		if st.Interaction == nil && workflow.AllHandlersFinished(ctx) && ((st.CommittedSeq > st.ContinuedAt && st.CommittedSeq%200 == 0) || workflow.GetInfo(ctx).GetContinueAsNewSuggested()) {
			st.ContinuedAt = st.CommittedSeq
			in.Carry = st
			return workflow.NewContinueAsNewError(ctx, w.AgentRunWorkflow, in)
		}
		seq := st.CommittedSeq + 1
		turn := st.PendingTurn
		if turn.Kind == "continue" && len(st.Steer) > 0 {
			turn.Kind = "follow_up"
		}
		remaining := in.BudgetMillicredits - st.UsedMillicredits
		notice := contract.BudgetNotice("none")
		if st.UsedMillicredits >= in.BudgetMillicredits-in.BudgetMillicredits/5 {
			notice = "soft_warning"
		}
		seg := contract.SegmentInput{V: 1, TenantId: in.TenantID, WorkspaceId: in.WorkspaceID, PrincipalId: in.PrincipalID, SessionId: in.SessionID, RunId: in.RunID, Member: in.Member, Seq: seq, AgentSessionKey: in.AgentSessionKey, ExpectedCheckpointVersion: st.CheckpointVersion, Lock: in.Lock, Input: turn, Steer: append([]contract.SteerItem(nil), st.Steer...), Limits: contract.SegmentLimits{MaxIterations: in.Limits.MaxIterations, MaxWallSeconds: in.Limits.MaxWallSeconds, SegmentBudgetMillicredits: min(in.Limits.SegmentBudgetMillicredits, remaining), RemainingRunMillicredits: remaining}, BudgetNotice: notice, Sandbox: contract.SegmentSandbox{Provider: st.Lease.Provider, SandboxId: st.Lease.SandboxId, IsolationKey: contract.IsolationKey(in.Sandbox.IsolationKey), IsolatedSessionId: st.Lease.IsolatedSessionId}, EventSubject: in.EventSubject}
		if err := l.exec("StartSegment", w.StartSegmentInput{RunOp: l.op(), ExpectedVersion: st.CheckpointVersion}, nil); err != nil {
			return l.finish("failed", "START_SEGMENT_FAILED")
		}
		l.touch(true)
		actCtx, cancel := workflow.WithCancel(w.Agent(ctx, in.Tier))
		l.cancelSegment = cancel
		t0 := workflow.Now(ctx)
		var res contract.SegmentResult
		err := workflow.ExecuteActivity(actCtx, contract.ActivityRunAgentSegment, seg).Get(ctx, &res)
		st.ActiveSeconds += int64(workflow.Now(ctx).Sub(t0) / time.Second)
		l.cancelSegment = nil
		cancel()
		l.touch(false)
		if err != nil {
			if temporal.IsCanceledError(err) {
				reason := "CANCELLED"
				if st.Cancel != nil {
					reason = st.Cancel.Reason
				}
				return l.finish("cancelled", reason)
			}
			app, ok := errors.AsType[*temporal.ApplicationError](err)
			if !ok {
				return l.finish("failed", "SEGMENT_FAILED")
			}
			switch app.Type() {
			case contract.ErrorCheckpointConflict:
				var rec w.ReconcileOutput
				if e := l.exec("ReconcileCheckpoint", w.ReconcileInput{RunOp: l.op(), SessionKey: in.AgentSessionKey}, &rec); e != nil {
					return e
				}
				if rec.Result == nil {
					return l.finish("failed", "CHECKPOINT_CONFLICT")
				}
				res = *rec.Result
			case contract.ErrorContextLengthExceeded:
				if st.CompactedFor == seq || st.ResumeTurn != nil {
					return l.finish("failed", "CONTEXT_TOO_LARGE")
				}
				st.CompactedFor = seq
				st.ResumeTurn = new(st.PendingTurn)
				st.PendingTurn = contract.SegmentTurn{Kind: "compact"}
				continue
			case contract.ErrorSandboxUnavailable:
				st.SandboxRetries++
				if st.SandboxRetries > 2 {
					return l.finish("failed", "SANDBOX_UNAVAILABLE")
				}
				if e := l.acquire(); e != nil {
					return l.finish("failed", "SANDBOX_UNAVAILABLE")
				}
				continue
			case contract.ErrorBudgetExhausted:
				return l.finish("budget_exhausted", "")
			default:
				code := map[string]string{contract.ErrorPolicyDenied: "POLICY_DENIED", contract.ErrorInvalidSpec: "INVALID_SPEC", contract.ErrorInvalidSegmentInput: "INVALID_SEGMENT_INPUT", contract.ErrorLockVerificationFailed: "LOCK_VERIFICATION_FAILED", contract.ErrorCheckpointUnreadable: "CHECKPOINT_UNREADABLE", contract.ErrorSegmentSuperseded: "SEGMENT_SUPERSEDED"}[app.Type()]
				if code == "" {
					code = "SEGMENT_FAILED"
				}
				return l.finish("failed", code)
			}
		}
		var rec w.RecordSegmentOutput
		if e := l.exec("RecordSegment", w.RecordSegmentInput{RunOp: l.op(), Result: res}, &rec); e != nil {
			return l.finish("failed", "RECORD_SEGMENT_FAILED")
		}
		st.CommittedSeq = seq
		st.CheckpointVersion = res.CheckpointVersion
		st.UsedMillicredits = rec.RunUsedMillicredits
		st.Steer = st.Steer[len(seg.Steer):]
		st.SandboxRetries = 0
		if turn.Kind == "compact" {
			if res.Status == "failed" {
				return l.finish("failed", "CONTEXT_TOO_LARGE")
			}
			st.PendingTurn = *st.ResumeTurn
			st.ResumeTurn = nil
			continue
		}
		switch res.Status {
		case "completed":
			if len(st.Steer) > 0 {
				st.PendingTurn = contract.SegmentTurn{Kind: "follow_up"}
				continue
			}
			return l.finish("succeeded", "")
		case "interrupted":
			d, e := l.await(res.Interaction)
			if e != nil {
				return e
			}
			if d == nil {
				return l.finish("cancelled", "CANCELLED")
			}
			st.PendingTurn = *d
		case "yielded":
			if res.Progress.ToolCalls > 0 || res.Progress.NewOutput {
				st.NoProgress = 0
			} else {
				st.NoProgress++
			}
			if st.NoProgress >= in.Limits.NoProgressLimit {
				return l.finish("failed", "STUCK")
			}
			st.PendingTurn = contract.SegmentTurn{Kind: "continue"}
		case "failed":
			return l.finish("failed", string(res.Error.Code))
		default:
			return l.finish("failed", "INVALID_SEGMENT_RESULT")
		}
	}
}
func (l *loop) op() w.RunOp {
	l.st.EventN++
	return w.RunOp{TenantID: l.in.TenantID, SessionID: l.in.SessionID, RunID: l.in.RunID, Member: l.in.Member, Seq: l.st.CommittedSeq + 1, EventN: l.st.EventN, EventAt: workflow.Now(l.ctx)}
}
func (l *loop) exec(name string, in, out any) error {
	return workflow.ExecuteActivity(w.Ctl(l.ctx, name), name, in).Get(l.ctx, out)
}
func (l *loop) status(status string) error {
	return l.exec("SetRunStatus", w.StatusInput{RunOp: l.op(), Status: status}, nil)
}
func (l *loop) acquire() error {
	return l.exec("AcquireSandbox", w.AcquireSandboxInput{RunOp: l.op(), Spec: l.in.Sandbox, LockID: l.in.Lock.Id, Attempt: l.st.SandboxRetries}, &l.st.Lease)
}
func (l *loop) touch(use bool) {
	err := workflow.SignalExternalWorkflow(l.ctx, w.LeaseWorkflowID(l.in.TenantID, l.in.Sandbox.IsolationKey), "", w.Touch, w.TouchInput{RunID: l.in.RunID, InUse: use}).Get(l.ctx, nil)
	if err != nil {
		workflow.GetLogger(l.ctx).Warn("lease touch failed", "error", err)
	}
}
func (l *loop) finish(status, code string) error {
	l.st.Status = status
	if strings.HasPrefix(code, "asset_suspended:") {
		code = "ASSET_SUSPENDED"
	}
	ctx, _ := workflow.NewDisconnectedContext(l.ctx)
	return workflow.ExecuteActivity(w.Ctl(ctx, "FinalizeRun"), "FinalizeRun", w.FinalizeInput{RunOp: l.op(), Status: status, ErrorCode: code}).Get(ctx, nil)
}
func (l *loop) register() error {
	st := l.st
	ctx := l.ctx
	workflow.Go(ctx, func(ctx workflow.Context) {
		ch := workflow.GetSignalChannel(ctx, w.Cancel)
		for {
			var in w.CancelInput
			ch.Receive(ctx, &in)
			if st.Cancel == nil {
				st.Cancel = &in
				if l.cancelSegment != nil {
					l.cancelSegment()
				}
			}
		}
	})
	workflow.Go(ctx, func(ctx workflow.Context) {
		ch := workflow.GetSignalChannel(ctx, w.Pause)
		for {
			var in w.ActorInput
			ch.Receive(ctx, &in)
			st.Paused = true
		}
	})
	workflow.Go(ctx, func(ctx workflow.Context) {
		ch := workflow.GetSignalChannel(ctx, w.Resume)
		for {
			var in w.ActorInput
			ch.Receive(ctx, &in)
			st.Paused = false
		}
	})
	workflow.Go(ctx, func(ctx workflow.Context) {
		ch := workflow.GetSignalChannel(ctx, w.AssetSuspended)
		for {
			var in w.AssetSuspendedInput
			ch.Receive(ctx, &in)
			if st.Cancel == nil {
				st.Cancel = &w.CancelInput{Reason: "asset_suspended:" + in.AssetRef}
				if l.cancelSegment != nil {
					l.cancelSegment()
				}
			}
		}
	})
	if err := workflow.SetUpdateHandlerWithOptions(ctx, w.Steer, func(ctx workflow.Context, in w.SteerInput) (w.SteerOutput, error) {
		st.Steer = append(st.Steer, contract.SteerItem{ContentRef: new(in.ContentRef)})
		return w.SteerOutput{Queued: len(st.Steer)}, nil
	}, workflow.UpdateHandlerOptions{Validator: func(in w.SteerInput) error {
		if w.Terminal(st.Status) || len(st.Steer) >= 20 || !strings.HasPrefix(in.ContentRef, "t/"+l.in.TenantID+"/runs/"+l.in.RunID+"/steer/") {
			return fmt.Errorf("RUN_NOT_ACTIVE or invalid steer")
		}
		return nil
	}}); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(ctx, w.ResolveInteraction, func(ctx workflow.Context, in w.ResolveInteractionInput) (w.ResolveInteractionOutput, error) {
		st.Interaction.Decision = &in.Decision
		st.Interaction.Actor = in.ActorID
		return w.ResolveInteractionOutput{Accepted: true}, nil
	}, workflow.UpdateHandlerOptions{Validator: func(in w.ResolveInteractionInput) error {
		p := st.Interaction
		if p == nil || p.Decision != nil || in.InteractionID != p.ID || in.Decision.Kind != p.Kind || !workflow.Now(ctx).Before(p.ExpiresAt) || w.Terminal(st.Status) {
			return fmt.Errorf("INTERACTION_NOT_PENDING")
		}
		return nil
	}}); err != nil {
		return err
	}
	return workflow.SetQueryHandler(ctx, w.Progress, func() (w.RunProgress, error) {
		p := w.RunProgress{Status: st.Status, Seq: st.CommittedSeq, CheckpointVersion: st.CheckpointVersion, UsedMillicredits: st.UsedMillicredits, BudgetMillicredits: l.in.BudgetMillicredits, Paused: st.Paused, ActiveSeconds: st.ActiveSeconds}
		if st.Interaction != nil {
			p.PendingInteractionID = st.Interaction.ID
		}
		return p, nil
	})
}
func (l *loop) await(req *contract.InteractionRequest) (*contract.SegmentTurn, error) {
	if req == nil {
		return nil, fmt.Errorf("missing interaction")
	}
	now := workflow.Now(l.ctx)
	limits := l.in.Limits
	var rec w.InteractionOutput
	if err := l.exec("CreateInteraction", w.InteractionInput{RunOp: l.committedOp(), Request: *req, RemindAt: now.Add(limits.RemindAfter), EscalateAt: now.Add(limits.EscalateAfter), ExpiresAt: now.Add(limits.ExpireAfter)}, &rec); err != nil {
		return nil, err
	}
	st := l.st
	st.Interaction = &w.Pending{ID: req.InteractionId, Kind: req.Kind, Seq: st.CommittedSeq, ExpiresAt: now.Add(limits.ExpireAfter)}
	st.Status = "waiting_approval"
	if req.Kind == "ask_user" {
		st.Status = "waiting_input"
	}
	for _, step := range []struct {
		after time.Duration
		kind  string
	}{{limits.RemindAfter, "remind"}, {limits.EscalateAfter, "escalate"}, {limits.ExpireAfter, "expire"}} {
		ok, err := workflow.AwaitWithTimeout(l.ctx, max(time.Duration(0), now.Add(step.after).Sub(workflow.Now(l.ctx))), func() bool { return st.Interaction.Decision != nil || st.Cancel != nil })
		if err != nil {
			return nil, err
		}
		if ok {
			break
		}
		if step.kind == "expire" {
			d := contract.Decision{Kind: req.Kind}
			switch req.Kind {
			case "tool_approval":
				d.Choice = new(contract.DecisionChoice("deny"))
				d.Comment = new("Approval expired")
			case "effect_uncertain":
				d.Choice = new(contract.DecisionChoice("abandon"))
				d.Comment = new("Decision expired")
			case "ask_user":
				d.Answer = new("[no answer: the question expired]")
			}
			st.Interaction.Decision = &d
			st.Interaction.Actor = "system"
			st.Interaction.Expired = true
			break
		}
		if err := l.exec("NotifyInteraction", w.NotifyInput{RunOp: l.op(), InteractionID: req.InteractionId, Kind: step.kind}, nil); err != nil {
			workflow.GetLogger(l.ctx).Warn("interaction notification failed", "error", err)
		}
	}
	if st.Cancel != nil && st.Interaction.Decision == nil {
		err := l.exec("CancelInteraction", w.NotifyInput{RunOp: l.op(), InteractionID: req.InteractionId}, nil)
		return nil, err
	}
	p := st.Interaction
	if err := l.exec("ApplyDecision", w.DecisionInput{RunOp: l.op(), InteractionID: p.ID, Kind: p.Kind, Decision: *p.Decision, Actor: p.Actor, Expired: p.Expired}, nil); err != nil {
		return nil, err
	}
	st.Interaction = nil
	st.Status = "running"
	return &contract.SegmentTurn{Kind: "interactive_input", InteractionId: new(p.ID), Decision: p.Decision}, nil
}

func (l *loop) committedOp() w.RunOp { op := l.op(); op.Seq = l.st.CommittedSeq; return op }

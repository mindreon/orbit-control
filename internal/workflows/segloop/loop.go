// Package segloop implements the deterministic, checkpoint-driven segment loop.
package segloop

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type State = w.RunCarry

// Hooks isolates run ownership from the shared member segment protocol.
type Hooks struct {
	Prepare         func(workflow.Context, w.RunInput, *State) error
	ContinueAsNew   func(workflow.Context, *State) error
	Input           func(*contract.SegmentInput)
	ShouldEnd       func(*State) bool
	Finish          func(workflow.Context, *State, string, string) error
	Holder          string
	Turn            int
	Drain           func(workflow.Context, *State)
	TrackDeliveries bool
}

type loop struct {
	hooks         Hooks
	holderID      string
	turnID        int
	in            w.RunInput
	st            *State
	cancelSegment workflow.CancelFunc
	continueAfter int
}

func Restore(in w.RunInput) *State {
	if in.Carry != nil {
		return in.Carry
	}
	return &State{
		Status:      "queued",
		PendingTurn: contract.SegmentTurn{Kind: "user_message", ContentRef: new(in.InputRef)},
	}
}
func Run(ctx workflow.Context, in w.RunInput, hooks Hooks) error {
	limit, err := w.ContinueThreshold(ctx, w.RunContinueAfter)
	if err != nil {
		return err
	}
	l := &loop{in: in, st: Restore(in), continueAfter: limit, holderID: hooks.Holder, turnID: hooks.Turn}
	if l.holderID == "" {
		l.holderID = in.RunID
	}
	// Resolve ownership defaults once; the shared protocol never branches on a nil lifecycle.
	if hooks.Prepare == nil {
		hooks.Prepare = func(ctx workflow.Context, _ w.RunInput, _ *State) error { return l.prepareAgent(ctx) }
	}
	if hooks.ContinueAsNew == nil {
		hooks.ContinueAsNew = func(ctx workflow.Context, st *State) error {
			in.Carry = st
			return workflow.NewContinueAsNewError(ctx, w.AgentRunWorkflow, in)
		}
	}
	if hooks.Input == nil {
		hooks.Input = func(*contract.SegmentInput) {}
	}
	if hooks.ShouldEnd == nil {
		hooks.ShouldEnd = func(*State) bool { return false }
	}
	if hooks.Finish == nil {
		hooks.Finish = func(ctx workflow.Context, _ *State, status, code string) error {
			return l.finishAgent(ctx, status, code)
		}
	}
	if hooks.Drain == nil {
		hooks.Drain = func(workflow.Context, *State) {}
	}
	l.hooks = hooks
	if err := l.register(ctx); err != nil {
		return err
	}
	if err := l.hooks.Prepare(ctx, in, l.st); err != nil {
		return err
	}
	if w.Terminal(l.st.Status) {
		return nil
	}

	for {
		st := l.st
		hooks.Drain(ctx, st)
		if st.Cancel != nil {
			return l.cancelled(ctx)
		}
		if st.Paused {
			if err := l.waitResume(ctx); err != nil {
				return err
			}
			if w.Terminal(l.st.Status) {
				return nil
			}
			continue
		}
		if st.UsedMillicredits >= in.BudgetMillicredits {
			return l.finish(ctx, "budget_exhausted", "")
		}
		if st.ActiveSeconds >= int64(in.Limits.MaxActiveSeconds) {
			return l.finish(ctx, "timed_out", "")
		}
		if st.CommittedSeq >= in.Limits.MaxSegments {
			return l.finish(ctx, "failed", "SEGMENT_LIMIT")
		}
		if st.Interaction == nil && workflow.AllHandlersFinished(ctx) &&
			((st.CommittedSeq > st.ContinuedAt && st.CommittedSeq%l.continueAfter == 0) ||
				workflow.GetInfo(ctx).GetContinueAsNewSuggested()) {
			st.ContinuedAt = st.CommittedSeq
			return hooks.ContinueAsNew(ctx, st)
		}
		done, err := l.step(ctx)
		if done || err != nil {
			return err
		}
	}
}
func (l *loop) prepareAgent(ctx workflow.Context) error {
	in := l.in
	if in.Carry == nil {
		var prep w.PrepareOutput
		if err := l.exec(ctx,
			"PrepareRun",
			w.PrepareInput{RunOp: l.op(ctx), SessionKey: in.AgentSessionKey, Budget: in.BudgetMillicredits},
			&prep,
		); err != nil {
			return l.failure(ctx, err, "PREPARE_FAILED")
		}
		l.st.CheckpointVersion = prep.CheckpointVersion
		l.st.Status = "running"
		if err := l.acquire(ctx); err != nil {
			return l.failure(ctx, err, "SANDBOX_UNAVAILABLE")
		}
	}
	return nil
}
func (l *loop) waitResume(ctx workflow.Context) error {
	st := l.st

	st.Status = "paused"
	if err := l.status(ctx, "paused"); err != nil {
		return l.finish(ctx, "failed", "STATUS_FAILED")
	}
	if err := workflow.Await(ctx, func() bool { return !st.Paused || st.Cancel != nil }); err != nil {
		return l.finish(ctx, "cancelled", "CANCELLED")
	}
	if st.Cancel != nil {
		return nil
	}
	st.Status = "running"
	if err := l.status(ctx, "running"); err != nil {
		return l.finish(ctx, "failed", "STATUS_FAILED")
	}
	return nil
}
func (l *loop) step(ctx workflow.Context) (bool, error) {
	st := l.st
	in := l.in
	if l.shouldEnd() {
		return true, l.finish(ctx, "ended", "")
	}
	seq := st.CommittedSeq + 1
	turn := st.PendingTurn
	if turn.Kind == "continue" && (len(st.Steer) > 0 || len(st.Mailbox) > 0) {
		turn.Kind = "follow_up"
	}
	seg := l.segmentInput(ctx, turn, seq)
	if err := l.exec(ctx, "StartSegment", w.StartSegmentInput{RunOp: l.op(ctx), ExpectedVersion: st.CheckpointVersion}, nil); err != nil {
		return true, l.failure(ctx, err, "START_SEGMENT_FAILED")
	}
	if st.Cancel != nil {
		return true, l.cancelled(ctx)
	}
	l.touch(ctx, true)
	if st.Cancel != nil {
		return true, l.cancelled(ctx)
	}
	actCtx, cancel := workflow.WithCancel(w.Agent(ctx, in.Tier))
	l.cancelSegment = cancel
	t0 := workflow.Now(ctx)
	var res contract.SegmentResult
	err := workflow.ExecuteActivity(actCtx, contract.ActivityRunAgentSegment, seg).Get(ctx, &res)
	st.ActiveSeconds += int64(workflow.Now(ctx).Sub(t0) / time.Second)
	l.cancelSegment = nil
	cancel()
	l.touch(ctx, false)
	if err != nil {
		var retry, done bool
		res, retry, done, err = l.segmentError(ctx, err, seg)
		if done || err != nil {
			return true, err
		}
		if retry {
			return false, nil
		}
	}
	return l.commit(ctx, seg, turn, seq, res)
}
func (l *loop) segmentError(ctx workflow.Context, err error, seg contract.SegmentInput) (contract.SegmentResult, bool, bool, error) {
	st, in := l.st, l.in
	var res contract.SegmentResult
	if err != nil {
		if temporal.IsCanceledError(err) {
			return res, false, true, l.cancelled(ctx)
		}
		app, ok := errors.AsType[*temporal.ApplicationError](err)
		if !ok {
			return res, false, true, l.finish(ctx, "failed", "SEGMENT_FAILED")
		}
		switch app.Type() {
		case contract.ErrorCheckpointConflict:
			var rec w.ReconcileOutput
			if e := l.exec(ctx, "ReconcileCheckpoint", w.ReconcileInput{RunOp: l.op(ctx), SessionKey: in.AgentSessionKey}, &rec); e != nil {
				return res, false, true, l.failure(ctx, e, "RECONCILE_FAILED")
			}
			if rec.Result == nil {
				return res, false, true, l.finish(ctx, "failed", "CHECKPOINT_CONFLICT")
			}
			res = *rec.Result
		case contract.ErrorContextLengthExceeded:
			if st.CompactedFor != 0 || st.ResumeTurn != nil {
				return res, false, true, l.finish(ctx, "failed", "CONTEXT_TOO_LARGE")
			}
			st.CompactedFor = seg.Seq
			st.ResumeTurn = new(st.PendingTurn)
			st.PendingTurn = contract.SegmentTurn{Kind: "compact"}
			return res, true, false, nil
		case contract.ErrorSandboxUnavailable:
			st.SandboxRetries++
			if st.SandboxRetries > 2 {
				return res, false, true, l.failure(ctx, err, "SANDBOX_UNAVAILABLE")
			}
			if e := l.acquire(ctx); e != nil {
				return res, false, true, l.failure(ctx, e, "SANDBOX_UNAVAILABLE")
			}
			return res, true, false, nil
		case contract.ErrorBudgetExhausted:
			return res, false, true, l.finish(ctx, "budget_exhausted", "")
		default:
			code := map[string]string{
				contract.ErrorPolicyDenied:           "POLICY_DENIED",
				contract.ErrorInvalidSpec:            "INVALID_SPEC",
				contract.ErrorInvalidSegmentInput:    "INVALID_SEGMENT_INPUT",
				contract.ErrorLockVerificationFailed: "LOCK_VERIFICATION_FAILED",
				contract.ErrorCheckpointUnreadable:   "CHECKPOINT_UNREADABLE",
				contract.ErrorSegmentSuperseded:      "SEGMENT_SUPERSEDED",
			}[app.Type()]
			if code == "" {
				code = "SEGMENT_FAILED"
			}
			return res, false, true, l.finish(ctx, "failed", code)
		}
	}

	return res, false, false, nil
}
func (l *loop) commit(
	ctx workflow.Context, seg contract.SegmentInput, turn contract.SegmentTurn, seq int, res contract.SegmentResult,
) (bool, error) {
	st, in := l.st, l.in
	var rec w.RecordSegmentOutput
	if e := l.exec(ctx, "RecordSegment", w.RecordSegmentInput{RunOp: l.op(ctx), Result: res}, &rec); e != nil {
		return true, l.failure(ctx, e, "RECORD_SEGMENT_FAILED")
	}
	st.CommittedSeq = seq
	st.CheckpointVersion = res.CheckpointVersion
	st.UsedMillicredits = rec.RunUsedMillicredits
	for _, item := range seg.Steer {
		if l.hooks.TrackDeliveries && item.ContentRef != nil {
			st.DeliveredSteer = append(st.DeliveredSteer, *item.ContentRef)
		}
	}
	st.Steer = st.Steer[len(seg.Steer):]
	if l.hooks.TrackDeliveries {
		for _, item := range seg.Mailbox {
			st.DeliveredMail = append(st.DeliveredMail, item.MessageId)
		}
	}
	st.Mailbox = st.Mailbox[len(seg.Mailbox):]
	if l.hooks.TrackDeliveries {
		st.LastFinal = res.Final
	}
	st.SandboxRetries = 0
	// A delivery may arrive while RecordSegment yields. Consume it before deciding to finish.
	l.hooks.Drain(ctx, st)
	if st.Cancel != nil {
		return true, l.cancelled(ctx)
	}
	if turn.Kind == "compact" {
		if res.Status == "failed" {
			return true, l.finish(ctx, "failed", "CONTEXT_TOO_LARGE")
		}
		st.PendingTurn = *st.ResumeTurn
		st.ResumeTurn = nil
		return false, nil
	}
	// A committed retransmission ends this compaction attempt. Later growth may compact again.
	st.CompactedFor = 0
	switch res.Status {
	case "completed":
		if l.shouldEnd() {
			return true, l.finish(ctx, "ended", "")
		}
		if len(st.Steer) > 0 || len(st.Mailbox) > 0 {
			st.PendingTurn = contract.SegmentTurn{Kind: "follow_up"}
			return false, nil
		}
		return true, l.finish(ctx, "succeeded", "")
	case "interrupted":
		if in.Unattended && workflow.GetVersion(ctx, "unattended-interaction", workflow.DefaultVersion, 1) != workflow.DefaultVersion {
			if err := l.unattended(ctx, res.Interaction); err != nil {
				return true, l.failure(ctx, err, "INTERACTION_FAILED")
			}
			return true, l.finish(ctx, "failed", "UNATTENDED_INTERACTION")
		}
		d, e := l.await(ctx, res.Interaction)
		if e != nil {
			if temporal.IsCanceledError(e) {
				return true, l.cancelled(ctx)
			}
			return true, l.finish(ctx, "failed", "INTERACTION_FAILED")
		}
		if d == nil {
			return true, l.cancelled(ctx)
		}
		st.PendingTurn = *d
	case "yielded":
		if res.Progress.ToolCalls > 0 || res.Progress.NewOutput {
			st.NoProgress = 0
		} else {
			st.NoProgress++
		}
		if st.NoProgress >= in.Limits.NoProgressLimit {
			return true, l.finish(ctx, "failed", "STUCK")
		}
		st.PendingTurn = contract.SegmentTurn{Kind: "continue"}
	case "failed":
		return true, l.finish(ctx, "failed", strings.ToUpper(string(res.Error.Code)))
	default:
		return true, l.finish(ctx, "failed", "INVALID_SEGMENT_RESULT")
	}
	return false, nil
}

func (l *loop) op(ctx workflow.Context) w.RunOp {
	l.st.EventN++
	return w.RunOp{
		TenantID:  l.in.TenantID,
		SessionID: l.in.SessionID,
		RunID:     l.in.RunID,
		Member:    l.in.Member,
		Seq:       l.st.CommittedSeq + 1,
		EventN:    l.st.EventN,
		EventAt:   workflow.Now(ctx),
	}
}
func (l *loop) exec(ctx workflow.Context, name string, in, out any) error {
	return workflow.ExecuteActivity(w.Ctl(ctx, name), name, in).Get(ctx, out)
}
func (l *loop) status(ctx workflow.Context, status string) error {
	return l.exec(ctx, "SetRunStatus", w.StatusInput{RunOp: l.op(ctx), Status: status}, nil)
}
func (l *loop) acquire(ctx workflow.Context) error {
	attempt := l.st.Acquisitions
	l.st.Acquisitions++
	return l.exec(ctx, "AcquireSandbox", w.AcquireSandboxInput{
		RunOp: l.op(ctx), Spec: l.in.Sandbox, LockID: l.in.Lock.Id, Attempt: attempt, Holder: l.holder(), Turn: l.turn(),
	}, &l.st.Lease)
}
func (l *loop) touch(ctx workflow.Context, use bool) {
	err := workflow.SignalExternalWorkflow(
		ctx,
		w.LeaseWorkflowID(l.in.TenantID, l.in.Sandbox.IsolationKey),
		"",
		w.Touch,
		w.TouchInput{RunID: l.in.RunID, InUse: use, Holder: l.holder(), Member: l.in.Member},
	).Get(ctx, nil)
	if err != nil {
		workflow.GetLogger(ctx).Warn("lease touch failed", "error", err)
	}
}
func (l *loop) finish(ctx workflow.Context, status, code string) error {
	l.st.Status = status
	ctx, _ = workflow.NewDisconnectedContext(ctx)
	return l.hooks.Finish(ctx, l.st, status, code)
}
func (l *loop) finishAgent(ctx workflow.Context, status, code string) error {
	reason := ""
	if l.st.Cancel != nil {
		reason = l.st.Cancel.Reason
	}
	return workflow.ExecuteActivity(
		w.Ctl(ctx, "FinalizeRun"),
		"FinalizeRun",
		w.FinalizeInput{RunOp: l.op(ctx), Status: status, ErrorCode: code, Reason: reason},
	).Get(ctx, nil)
}
func (l *loop) register(ctx workflow.Context) error {
	st := l.st
	l.registerSignals(ctx)
	if err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		w.Steer,
		func(ctx workflow.Context, in w.SteerInput) (w.SteerOutput, error) {
			st.Steer = append(st.Steer, contract.SteerItem{ContentRef: new(in.ContentRef)})
			return w.SteerOutput{Queued: len(st.Steer)}, nil
		},
		workflow.UpdateHandlerOptions{Validator: func(in w.SteerInput) error {
			if len(st.Steer) >= 20 {
				return temporal.NewNonRetryableApplicationError("Steer queue is full", "QUEUE_FULL", nil)
			}
			if w.Terminal(st.Status) ||
				!strings.HasPrefix(in.ContentRef, "t/"+l.in.TenantID+"/runs/"+l.in.RunID+"/steer/") {
				return temporal.NewNonRetryableApplicationError("Run is not active", "RUN_NOT_ACTIVE", nil)
			}
			return nil
		}},
	); err != nil {
		return err
	}
	if err := workflow.SetUpdateHandlerWithOptions(
		ctx,
		w.ResolveInteraction,
		func(ctx workflow.Context, in w.ResolveInteractionInput) (w.ResolveInteractionOutput, error) {
			st.Interaction.Decision = &in.Decision
			st.Interaction.Actor = in.ActorID
			return w.ResolveInteractionOutput{Accepted: true}, nil
		},
		workflow.UpdateHandlerOptions{Validator: func(in w.ResolveInteractionInput) error {
			p := st.Interaction
			if p == nil || p.Decision != nil || in.InteractionID != p.ID || in.Decision.Kind != p.Kind ||
				!workflow.Now(ctx).Before(p.ExpiresAt) ||
				w.Terminal(st.Status) {
				return fmt.Errorf("INTERACTION_NOT_PENDING")
			}
			return nil
		}},
	); err != nil {
		return err
	}
	return workflow.SetQueryHandler(ctx, w.Progress, func() (w.RunProgress, error) {
		p := w.RunProgress{
			Status:             st.Status,
			Seq:                st.CommittedSeq,
			CheckpointVersion:  st.CheckpointVersion,
			UsedMillicredits:   st.UsedMillicredits,
			BudgetMillicredits: l.in.BudgetMillicredits,
			Paused:             st.Paused,
			ActiveSeconds:      st.ActiveSeconds,
		}
		if st.Interaction != nil {
			p.PendingInteractionID = st.Interaction.ID
		}
		return p, nil
	})
}
func (l *loop) await(ctx workflow.Context, req *contract.InteractionRequest) (*contract.SegmentTurn, error) {
	if req == nil {
		return nil, fmt.Errorf("missing interaction")
	}
	now := workflow.Now(ctx)
	limits := l.in.Limits
	var rec w.InteractionOutput
	if err := l.exec(ctx,
		"CreateInteraction",
		w.InteractionInput{
			RunOp:      l.committedOp(ctx),
			Request:    *req,
			RemindAt:   now.Add(limits.RemindAfter),
			EscalateAt: now.Add(limits.EscalateAfter),
			ExpiresAt:  now.Add(limits.ExpireAfter),
		},
		&rec,
	); err != nil {
		return nil, err
	}
	st := l.st
	st.Interaction = &w.Pending{
		ID:        req.InteractionId,
		Kind:      req.Kind,
		Seq:       st.CommittedSeq,
		ExpiresAt: now.Add(limits.ExpireAfter),
	}
	st.Status = "waiting_approval"
	if req.Kind == "ask_user" {
		st.Status = "waiting_input"
	}
	if err := l.waitDecision(ctx, now, req); err != nil {
		return nil, err
	}
	if st.Cancel != nil && st.Interaction.Decision == nil {
		err := l.exec(ctx, "CancelInteraction", w.NotifyInput{RunOp: l.op(ctx), InteractionID: req.InteractionId}, nil)
		return nil, err
	}
	p := st.Interaction
	if err := l.exec(ctx,
		"ApplyDecision",
		w.DecisionInput{
			RunOp:         l.op(ctx),
			InteractionID: p.ID,
			Kind:          p.Kind,
			Decision:      *p.Decision,
			Actor:         p.Actor,
			Expired:       p.Expired,
		},
		nil,
	); err != nil {
		return nil, err
	}
	st.Interaction = nil
	st.Status = "running"
	return &contract.SegmentTurn{Kind: "interactive_input", InteractionId: new(p.ID), Decision: p.Decision}, nil
}

func (l *loop) committedOp(ctx workflow.Context) w.RunOp {
	op := l.op(ctx)
	op.Seq = l.st.CommittedSeq
	return op
}

func (l *loop) failure(ctx workflow.Context, err error, code string) error {
	if temporal.IsCanceledError(err) || ctx.Err() != nil {
		return l.cancelled(ctx)
	}
	return l.finish(ctx, "failed", code)
}

func (l *loop) cancelled(ctx workflow.Context) error {
	code := "CANCELLED"
	if l.st.CancelCode == "ASSET_SUSPENDED" {
		code = "ASSET_SUSPENDED"
	}
	return l.finish(ctx, "cancelled", code)
}

func (l *loop) segmentInput(ctx workflow.Context, turn contract.SegmentTurn, seq int) contract.SegmentInput {
	in, st := l.in, l.st
	remaining := in.BudgetMillicredits - st.UsedMillicredits
	notice := contract.BudgetNotice("none")
	if st.UsedMillicredits >= in.BudgetMillicredits-in.BudgetMillicredits/5 {
		notice = "soft_warning"
	}
	out := contract.SegmentInput{
		V:                         1,
		TenantId:                  in.TenantID,
		WorkspaceId:               in.WorkspaceID,
		PrincipalId:               in.PrincipalID,
		SessionId:                 in.SessionID,
		RunId:                     in.RunID,
		Member:                    in.Member,
		Seq:                       seq,
		AgentSessionKey:           in.AgentSessionKey,
		ExpectedCheckpointVersion: st.CheckpointVersion,
		Lock:                      in.Lock,
		Input:                     turn,
		Steer:                     append([]contract.SteerItem(nil), st.Steer...),
		Mailbox:                   append([]contract.MailboxMessage(nil), st.Mailbox...),
		Unattended:                in.Unattended,
		Limits: contract.SegmentLimits{
			MaxIterations:             in.Limits.MaxIterations,
			MaxWallSeconds:            in.Limits.MaxWallSeconds,
			SegmentBudgetMillicredits: min(in.Limits.SegmentBudgetMillicredits, remaining),
			RemainingRunMillicredits:  remaining,
		},
		BudgetNotice: notice,
		Sandbox: contract.SegmentSandbox{
			Provider:          st.Lease.Provider,
			SandboxId:         st.Lease.SandboxId,
			IsolationKey:      contract.IsolationKey(in.Sandbox.IsolationKey),
			IsolatedSessionId: st.Lease.IsolatedSessionId,
		},
		EventSubject: in.EventSubject,
	}
	l.hooks.Input(&out)
	return out
}

func (l *loop) registerSignals(ctx workflow.Context) {
	st := l.st
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
				st.CancelCode = "ASSET_SUSPENDED"
				st.Cancel = &w.CancelInput{Reason: "asset_suspended:" + in.AssetRef}
				if l.cancelSegment != nil {
					l.cancelSegment()
				}
			}
		}
	})
}

func (l *loop) waitDecision(ctx workflow.Context, now time.Time, req *contract.InteractionRequest) error {
	st, limits := l.st, l.in.Limits
	for _, step := range []struct {
		after time.Duration
		kind  string
	}{{after: limits.RemindAfter,
		kind: "remind"},
		{after: limits.EscalateAfter,
			kind: "escalate"},
		{after: limits.ExpireAfter,
			kind: "expire"}} {
		ok, err := workflow.AwaitWithTimeout(
			ctx,
			max(time.Duration(0), now.Add(step.after).Sub(workflow.Now(ctx))),
			func() bool {
				return st.Interaction.Decision != nil || st.Cancel != nil
			},
		)
		if err != nil {
			return err
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
		if err := l.exec(
			ctx, "NotifyInteraction", w.NotifyInput{RunOp: l.op(ctx), InteractionID: req.InteractionId, Kind: step.kind}, nil,
		); err != nil {
			workflow.GetLogger(ctx).Warn("interaction notification failed", "error", err)
		}
	}
	return nil
}

func (l *loop) shouldEnd() bool { return l.hooks.ShouldEnd(l.st) }
func (l *loop) holder() string  { return l.holderID }
func (l *loop) turn() int       { return l.turnID }
func (l *loop) unattended(ctx workflow.Context, req *contract.InteractionRequest) error {
	if req == nil {
		return fmt.Errorf("missing interaction")
	}
	now := workflow.Now(ctx)
	var out w.InteractionOutput
	if err := l.exec(ctx, "CreateInteraction", w.InteractionInput{RunOp: l.committedOp(ctx), Request: *req,
		RemindAt: now.Add(l.in.Limits.RemindAfter), EscalateAt: now.Add(l.in.Limits.EscalateAfter),
		ExpiresAt: now.Add(l.in.Limits.ExpireAfter)}, &out); err != nil {
		return err
	}
	return l.exec(ctx, "CancelInteraction", w.NotifyInput{RunOp: l.op(ctx), InteractionID: req.InteractionId}, nil)
}

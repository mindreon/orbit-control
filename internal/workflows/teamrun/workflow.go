package teamrun

import (
	"slices"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/team"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type boardView struct {
	Board    contract.TeamBoard
	Active   map[string]w.ActiveTurn
	Finished bool
}
type run struct {
	in           w.TeamRunInput
	st           *w.TeamCarry
	waitCtx      workflow.Context
	results      []w.TurnResult
	pending      []workflow.Future
	turns        []w.TurnRow
	limit        int
	failure      string
	pauseChanged bool
}

func Run(ctx workflow.Context, in w.TeamRunInput) error {
	limit, err := w.ContinueThreshold(ctx, w.TeamContinueAfter)
	if err != nil {
		return err
	}
	waitCtx, cancelWait := workflow.NewDisconnectedContext(ctx)
	defer cancelWait()
	r := &run{in: in, st: in.Carry, waitCtx: waitCtx, limit: limit}
	if r.st == nil {
		r.st = &w.TeamCarry{Status: "running", Board: contract.TeamBoard{Roster: slices.Clone(in.Team.Roster)},
			Mail: map[string][]contract.MailboxMessage{}, Cursors: map[string]w.MemberCursor{}, Active: map[string]w.ActiveTurn{},
			Applied: map[string]w.AppliedUpdate{}, StartedAt: workflow.Now(ctx)}
	}
	if err := r.register(ctx); err != nil {
		return err
	}
	r.registerPauseSignals(ctx)
	deadlineCtx, cancelDeadline := workflow.WithCancel(ctx)
	defer cancelDeadline()
	deadline := workflow.NewTimer(deadlineCtx,
		max(r.st.StartedAt.Add(time.Duration(in.Limits.MaxActiveSeconds)*time.Second).Sub(workflow.Now(ctx)), 0))
	if in.Carry == nil {
		if err := r.exec(ctx, "PrepareTeamRun", w.PrepareTeamInput{RunOp: r.op(ctx), Budget: in.BudgetMillicredits,
			Team: in.Team, Board: r.st.Board}, nil); err != nil {
			return r.shutdown(ctx, "failed", "PREPARE_FAILED")
		}
		if err := r.exec(ctx, "AcquireSandbox", w.AcquireSandboxInput{RunOp: r.op(ctx), Spec: in.Sandbox,
			LockID: in.Lock.Id, Holder: in.RunID}, &r.st.Lease); err != nil {
			return r.shutdown(ctx, "failed", "SANDBOX_UNAVAILABLE")
		}
		r.pending = append(r.pending, workflow.SignalExternalWorkflow(ctx, w.LeaseWorkflowID(in.TenantID, in.Sandbox.IsolationKey), "",
			w.Touch, w.TouchInput{RunID: in.RunID, Holder: in.RunID, Member: w.MemberTeam, InUse: false}))
		r.start(ctx, team.Action{Member: in.Team.Leader, Kind: "lead"}, contract.SegmentTurn{Kind: "user_message", ContentRef: new(in.InputRef)})
	} else {
		for _, member := range workflow.DeterministicKeys(r.st.Active) {
			r.wait(r.st.Active[member])
		}
	}
	for {
		r.drain(ctx)
		for len(r.results) > 0 {
			result := r.results[0]
			r.results = r.results[1:]
			r.result(ctx, result)
		}
		if r.pauseChanged {
			status := "running"
			if r.st.Paused {
				status = "paused"
			}
			// Clear before yielding: a signal received during the Activity must request another write.
			r.pauseChanged = false
			if err := r.exec(ctx, "SetRunStatus", w.StatusInput{RunOp: r.op(ctx), Status: status}, nil); err != nil {
				return r.shutdown(ctx, "failed", "STATUS_FAILED")
			}
		}
		if err := r.flush(ctx); err != nil {
			return r.shutdown(ctx, "failed", "RECORD_BOARD_FAILED")
		}
		if status, code := r.terminal(ctx); status != "" {
			return r.shutdown(ctx, status, code)
		}
		if !r.st.Paused && r.st.Cancel == nil {
			board, actions := team.StartScan(r.st.Board, r.active(), in.Team.MaxParallel, r.st.Reserve)
			for i := range board.Tasks {
				if board.Tasks[i].Status != r.st.Board.Tasks[i].Status {
					r.st.Dirty = true
				}
			}
			r.st.Board = board
			for _, action := range actions {
				task := r.task(action.TaskID)
				r.start(ctx, action, contract.SegmentTurn{Kind: "follow_up", Text: new(team.TaskNotice(*task, r.st.Board))})
			}
			for _, action := range team.ReviewScan(r.st.Board, r.active(), in.Team.MaxParallel, r.st.Reserve) {
				r.start(ctx, action, contract.SegmentTurn{Kind: "follow_up", Text: new(team.ReviewNotice(*r.task(action.TaskID)))})
			}
			r.wake(ctx)
		}
		if r.failure != "" {
			continue
		}
		r.collectPending(ctx)
		r.drain(ctx)
		if len(r.results) == 0 && !r.st.Dirty && len(r.st.Outbox) == 0 && len(r.pending) == 0 &&
			!r.pauseChanged && !r.pausePending(ctx) && workflow.AllHandlersFinished(ctx) &&
			(r.st.Steps >= r.limit || workflow.GetInfo(ctx).GetContinueAsNewSuggested()) {
			r.st.Steps = 0
			in.Carry = r.st
			return workflow.NewContinueAsNewError(ctx, w.TeamRunWorkflow, in)
		}
		if r.st.Dirty || len(r.results) > 0 {
			continue
		}
		err := workflow.Await(ctx, func() bool {
			return deadline.IsReady() || r.pauseChanged || len(r.results) > 0 || r.st.Dirty || r.signals(ctx) || ctx.Err() != nil || r.readyPending()
		})
		if err != nil && ctx.Err() == nil {
			return err
		}
	}
}
func (r *run) op(ctx workflow.Context) w.RunOp {
	r.st.EventN++
	return w.RunOp{TenantID: r.in.TenantID, SessionID: r.in.SessionID, RunID: r.in.RunID, Member: w.MemberTeam,
		EventN: r.st.EventN, EventAt: workflow.Now(ctx)}
}
func (r *run) exec(ctx workflow.Context, name string, in, out any) error {
	if ctx.Err() != nil {
		ctx, _ = workflow.NewDisconnectedContext(ctx)
	}
	return workflow.ExecuteActivity(w.Ctl(ctx, name), name, in).Get(ctx, out)
}
func (r *run) active() []team.Active {
	out := []team.Active{}
	for _, member := range workflow.DeterministicKeys(r.st.Active) {
		turn := r.st.Active[member]
		out = append(out, team.Active{Member: member, Kind: turn.Kind, TaskID: turn.TaskID})
	}
	return out
}
func (r *run) signal(ctx workflow.Context, turn w.ActiveTurn, name string, in any) {
	if ctx.Err() != nil {
		ctx, _ = workflow.NewDisconnectedContext(ctx)
	}
	r.pending = append(r.pending, workflow.SignalExternalWorkflow(ctx,
		w.MemberTaskWorkflowID(r.in.TenantID, r.in.RunID, turn.Member, turn.Turn), "", name, in))
}
func (r *run) readyPending() bool {
	for _, future := range r.pending {
		if future.IsReady() {
			return true
		}
	}
	return false
}
func (r *run) collectPending(ctx workflow.Context) {
	pending := r.pending[:0]
	for _, future := range r.pending {
		if !future.IsReady() {
			pending = append(pending, future)
			continue
		}
		if err := future.Get(ctx, nil); err != nil {
			workflow.GetLogger(ctx).Warn("member signal failed", "error", err)
		}
	}
	r.pending = pending
}
func (r *run) wait(turn w.ActiveTurn) {
	future := workflow.ExecuteActivity(w.Ctl(r.waitCtx, "WaitForTurn"), "WaitForTurn",
		w.TurnOp{TenantID: r.in.TenantID, RunID: r.in.RunID, Member: turn.Member, Turn: turn.Turn})
	workflow.Go(r.waitCtx, func(ctx workflow.Context) {
		var result w.TurnResult
		if err := future.Get(ctx, &result); err != nil {
			result = w.TurnResult{Member: turn.Member, Turn: turn.Turn, Status: "failed", Code: "TURN_LOST", Cursor: r.st.Cursors[turn.Member]}
		}
		r.results = append(r.results, result)
	})
}
func (r *run) start(ctx workflow.Context, action team.Action, input contract.SegmentTurn) {
	if r.st.Turns >= team.MaxTurnsTotal || r.failure != "" {
		return
	}
	if _, exists := r.st.Active[action.Member]; exists {
		return
	}
	cursor := r.st.Cursors[action.Member]
	cursor.Turn++
	r.st.Cursors[action.Member] = cursor
	r.st.Turns++
	r.st.Steps++
	limits := r.in.Limits
	limits.MaxSegments = cursor.Seq + team.TurnMaxSegments
	wall := team.TurnWallSeconds
	if action.Kind == "review" {
		wall = team.ReviewTurnWallSeconds
	}
	limits.MaxActiveSeconds = int(cursor.ActiveSeconds) + wall
	budget := r.in.BudgetMillicredits
	var brief *contract.TeamTaskBrief
	if action.Kind != "lead" {
		reserve := budget * team.ReserveFraction / 100
		budget -= reserve
		limits.SegmentBudgetMillicredits = min(limits.SegmentBudgetMillicredits, reserve/int64(r.in.Team.MaxParallel))
		task := r.task(action.TaskID)
		brief = &contract.TeamTaskBrief{TaskId: task.TaskId, Title: task.Title, Description: task.Description, NarrowedTools: task.NarrowedTools}
		if action.Kind == "task" {
			if !team.CanTransition(task.Status, "in_progress") {
				r.failure = "INVALID_SPEC"
				return
			}
			task.Status = "in_progress"
			task.Attempts++
			r.updated(task, false)
		}
	}
	steer := []contract.SteerItem{}
	if action.Kind == "lead" {
		steer = slices.Clone(r.st.UserSteer)
	}
	mail := slices.Clone(r.st.Mail[action.Member])
	in := w.MemberTaskInput{V: 1, TenantID: r.in.TenantID, WorkspaceID: r.in.WorkspaceID, SessionID: r.in.SessionID,
		RunID: r.in.RunID, PrincipalID: r.in.PrincipalID, Member: action.Member, Role: r.role(action.Member), Turn: cursor.Turn,
		Kind: action.Kind, Task: brief, Lock: r.in.Lock, EventSubject: r.in.EventSubject, Input: input, Mailbox: mail, Steer: steer,
		Cursor: cursor, RunUsedMillicredits: r.st.UsedMillicredits, BudgetMillicredits: budget, Limits: limits,
		Sandbox: r.in.Sandbox, Lease: r.st.Lease, Tier: r.in.Tier, Unattended: r.in.Unattended}
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		WorkflowID: w.MemberTaskWorkflowID(r.in.TenantID, r.in.RunID, action.Member, cursor.Turn), TaskQueue: contract.TaskQueueCtl,
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_ABANDON, WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		TypedSearchAttributes: temporal.NewSearchAttributes(
			temporal.NewSearchAttributeKeyKeyword("TenantId").ValueSet(r.in.TenantID),
			temporal.NewSearchAttributeKeyKeyword("WorkspaceId").ValueSet(r.in.WorkspaceID),
			temporal.NewSearchAttributeKeyKeyword("RunKind").ValueSet("member")),
	})
	future := workflow.ExecuteChildWorkflow(childCtx, w.MemberTaskWorkflow, in)
	if err := future.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
		r.failure = "TURN_START_CONFLICT"
		return
	}
	active := w.ActiveTurn{Member: action.Member, Turn: cursor.Turn, Kind: action.Kind, TaskID: action.TaskID}
	r.st.Active[action.Member] = active
	r.wait(active)
	r.emit("team.member.status",
		contract.TeamMemberStatus{Name: contract.MemberName(action.Member), Status: "working", TaskId: optional(action.TaskID)})
	r.turns = append(r.turns, w.TurnRow{Member: action.Member, Turn: cursor.Turn, Kind: action.Kind, TaskID: action.TaskID,
		Status: "running", StartedAt: workflow.Now(ctx)})
}
func optional(s string) *string {
	if s == "" {
		return nil
	}
	return new(s)
}
func (r *run) result(ctx workflow.Context, result w.TurnResult) {
	active, ok := r.st.Active[result.Member]
	if !ok || active.Turn != result.Turn {
		return
	}
	r.st.Cursors[result.Member] = result.Cursor
	r.st.UsedMillicredits = max(r.st.UsedMillicredits, result.RunUsedMillicredits)
	r.st.Reserve = r.st.Reserve || r.st.UsedMillicredits >= r.in.BudgetMillicredits-r.in.BudgetMillicredits*team.ReserveFraction/100
	r.st.Mail[result.Member] = slices.DeleteFunc(r.st.Mail[result.Member], func(item contract.MailboxMessage) bool {
		return slices.Contains(result.DeliveredMail, item.MessageId)
	})
	if active.Kind == "lead" {
		r.st.UserSteer = slices.DeleteFunc(r.st.UserSteer, func(item contract.SteerItem) bool {
			return item.ContentRef != nil && slices.Contains(result.DeliveredSteer, *item.ContentRef)
		})
	}
	delete(r.st.Active, result.Member)
	for _, key := range workflow.DeterministicKeys(r.st.Applied) {
		entry := r.st.Applied[key]
		if entry.Member == result.Member && entry.Turn == result.Turn {
			delete(r.st.Applied, key)
		}
	}
	r.st.Steps++
	invariant := result.Status != "ended" && team.Classify(result.Code) == team.Invariant
	if invariant {
		r.failure = result.Code
	}
	if active.Kind == "lead" && !invariant {
		if result.Final != nil {
			final := *result.Final
			final.TextPreview = team.TruncateEncoded(final.TextPreview, team.SummaryBytes)
			r.st.LastLeaderFinal = &final
		}
		switch result.Status {
		case "completed":
			if !r.st.EverCreated {
				r.st.Finished = true
				r.st.Summary = r.summary()
			}
		case "ended":
		case "cancelled":
			if r.st.Cancel == nil {
				r.failure = "LEADER_FAILED"
			}
		case "budget_exhausted":
			r.st.Status = "budget_exhausted"
		default:
			r.failure = "LEADER_FAILED"
		}
	} else if active.Kind != "lead" && !invariant {
		task := r.task(active.TaskID)
		if active.Kind == "review" {
			if result.Status != "ended" && result.Status != "cancelled" &&
				result.Status != "budget_exhausted" && task.Status == "in_review" &&
				task.Review != nil &&
				!slices.ContainsFunc(task.Review.Votes, func(v contract.ReviewVote) bool { return string(v.Reviewer) == result.Member }) {
				task.Review.Votes = append(task.Review.Votes, contract.ReviewVote{Reviewer: contract.MemberName(result.Member),
					Verdict: "reject", Comment: "reviewer did not submit a verdict"})
				r.settle(ctx, task)
			}
		} else if task.Status == "in_progress" {
			switch result.Status {
			case "ended", "cancelled":
			case "completed":
				if task.Status == "in_progress" {
					preview := ""
					if result.Final != nil {
						preview = result.Final.TextPreview
					}
					task.ResultText = new(team.TruncateEncoded(preview, team.ResultBytes))
					r.completeTask(task)
					r.updated(task, true)
				}
			case "budget_exhausted":
				task.Status = "pending"
				r.updated(task, false)
			default:
				failures := 0
				if task.Failures != nil {
					failures = *task.Failures
				}
				if (team.Classify(result.Code) == team.Retryable || result.Status == "timed_out") && failures < team.MaxTaskFailures-1 {
					task.Failures = new(failures + 1)
					task.Status = "pending"
				} else {
					task.Failures = new(failures + 1)
					task.Status = "failed"
					r.st.Escalations = append(r.st.Escalations, task.TaskId)
				}
				r.updated(task, false)
			}
		}
	}
	r.emit("team.member.status", contract.TeamMemberStatus{Name: contract.MemberName(result.Member), Status: "idle"})
	now := workflow.Now(ctx)
	r.turns = append(r.turns, w.TurnRow{Member: result.Member, Turn: result.Turn, Kind: active.Kind, TaskID: active.TaskID,
		Status: result.Status, Code: result.Code, EndedAt: &now})
}
func (r *run) summary() string {
	if r.st.LastLeaderFinal == nil {
		return ""
	}
	return team.TruncateEncoded(r.st.LastLeaderFinal.TextPreview, team.SummaryBytes)
}
func (r *run) wake(ctx workflow.Context) {
	if _, active := r.st.Active[r.in.Team.Leader]; active || r.st.Finished {
		return
	}
	text := ""
	switch {
	case len(r.st.Mail[r.in.Team.Leader]) > 0 || len(r.st.UserSteer) > 0:
		text = team.MailNotice
	case len(r.st.Escalations) > 0:
		text = team.EscalationNotice(r.st.Escalations, r.st.Board)
		r.st.Escalations = nil
	case r.st.Reserve && !r.st.ReserveNotified:
		text = team.BudgetNotice
		r.st.ReserveNotified = true
	default:
		for _, active := range r.active() {
			if active.Kind != "lead" {
				return
			}
		}
		_, starts := team.StartScan(r.st.Board, r.active(), r.in.Team.MaxParallel, r.st.Reserve)
		if len(starts) > 0 || len(team.ReviewScan(r.st.Board, r.active(), r.in.Team.MaxParallel, r.st.Reserve)) > 0 {
			return
		}
		for _, task := range r.st.Board.Tasks {
			if task.Status == "in_review" && !team.Busy(task.TaskId, r.active()) {
				return
			}
		}
		if r.st.IdleWakes > 0 {
			r.st.Finished = true
			r.st.Summary = r.summary()
			r.st.Dirty = true
			return
		}
		r.st.IdleWakes = 1
		text = team.SettledNotice
	}
	r.start(ctx, team.Action{Member: r.in.Team.Leader, Kind: "lead"}, contract.SegmentTurn{Kind: "follow_up", Text: new(text)})
}
func (r *run) terminal(ctx workflow.Context) (string, string) {
	if r.st.Cancel != nil {
		code := "CANCELLED"
		if r.st.CancelCode != "" {
			code = r.st.CancelCode
		}
		return "cancelled", code
	}
	if r.failure != "" {
		return "failed", r.failure
	}
	if r.st.UsedMillicredits >= r.in.BudgetMillicredits || r.st.Status == "budget_exhausted" {
		return "budget_exhausted", ""
	}
	if !workflow.Now(ctx).Before(r.st.StartedAt.Add(time.Duration(r.in.Limits.MaxActiveSeconds) * time.Second)) {
		return "timed_out", ""
	}
	if r.st.Finished && len(r.st.Active) == 0 {
		if r.st.Reserve {
			return "budget_exhausted", ""
		}
		return "succeeded", ""
	}
	if r.st.Turns >= team.MaxTurnsTotal && len(r.st.Active) == 0 {
		return "failed", "TEAM_TURN_LIMIT"
	}
	return "", ""
}
func (r *run) flush(ctx workflow.Context) error {
	if !r.st.Dirty && len(r.st.Outbox) == 0 {
		return nil
	}
	r.st.BoardVersion++
	// Handlers may append events while this Activity yields. Detach the batch
	// before scheduling so their new state remains dirty for the next flush.
	events, turns := r.st.Outbox, r.turns
	r.st.Dirty, r.st.Outbox, r.turns = false, nil, nil
	if err := r.exec(ctx, "RecordBoard", w.RecordBoardInput{RunOp: r.op(ctx), Version: r.st.BoardVersion,
		Board: r.st.Board, Turns: turns, Events: events}, nil); err != nil {
		return err
	}
	return nil
}
func (r *run) shutdown(ctx workflow.Context, status, code string) error {
	ctx, _ = workflow.NewDisconnectedContext(ctx)
	r.st.Status = status
	for _, member := range workflow.DeterministicKeys(r.st.Active) {
		r.signal(ctx, r.st.Active[member], w.Cancel, w.CancelInput{Reason: "team_finished"})
	}
	_, err := workflow.AwaitWithTimeout(ctx, time.Duration(r.in.Limits.MaxWallSeconds+60)*time.Second, func() bool {
		for len(r.results) > 0 {
			result := r.results[0]
			r.results = r.results[1:]
			r.result(ctx, result)
		}
		return len(r.st.Active) == 0
	})
	if err != nil {
		return err
	}
	for i := range r.st.Board.Tasks {
		task := &r.st.Board.Tasks[i]
		if !team.Terminal(task.Status) && team.CanTransition(task.Status, "cancelled") {
			task.Status = "cancelled"
			r.updated(task, false)
		}
	}
	if err := r.flush(ctx); err != nil {
		workflow.GetLogger(ctx).Error("final board flush failed", "error", err)
	}
	return r.exec(ctx, "FinalizeTeamRun",
		w.FinalizeTeamInput{RunOp: r.op(ctx), Status: status, Code: code, Summary: r.st.Summary, Board: r.st.Board}, nil)
}
func (r *run) signals(ctx workflow.Context) bool {
	for _, name := range []string{w.Cancel, pauseSignal, w.AssetSuspended} {
		if workflow.GetSignalChannel(ctx, name).Len() > 0 {
			return true
		}
	}
	return false
}
func (r *run) drain(ctx workflow.Context) {
	if ctx.Err() != nil && r.st.Cancel == nil {
		r.st.Cancel = &w.CancelInput{Reason: "parent_cancelled"}
	}
	for _, name := range []string{w.Cancel, w.AssetSuspended} {
		ch := workflow.GetSignalChannel(ctx, name)
		for ch.Len() > 0 {
			var input any
			switch name {
			case w.Cancel:
				var in w.CancelInput
				ch.ReceiveAsync(&in)
				input = in
				if r.st.Cancel == nil {
					r.st.Cancel = &in
				}
			case w.AssetSuspended:
				var in w.AssetSuspendedInput
				ch.ReceiveAsync(&in)
				input = in
				r.st.CancelCode = "ASSET_SUSPENDED"
				r.st.Cancel = &w.CancelInput{Reason: "asset_suspended:" + in.AssetRef}
			}
			for _, member := range workflow.DeterministicKeys(r.st.Active) {
				r.signal(ctx, r.st.Active[member], name, input)
			}
		}
	}
}

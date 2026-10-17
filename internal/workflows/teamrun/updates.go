package teamrun

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/team"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func rejected(code, message string) error {
	return temporal.NewNonRetryableApplicationError(message, code, nil)
}
func textError() error { return rejected("TEAM_TEXT_TOO_LONG", "Team text exceeds its byte limit") }
func (r *run) common(caller *contract.MemberName, request string) (bool, error) {
	if _, ok := r.st.Applied[request]; ok {
		return true, nil
	}
	if caller == nil || r.role(string(*caller)) == "" {
		return false, rejected("TEAM_UNKNOWN_MEMBER", "Unknown caller")
	}
	active, ok := r.st.Active[string(*caller)]
	if !ok {
		return false, rejected("TEAM_NOT_IN_TURN", "Caller has no active turn")
	}
	if w.Terminal(r.st.Status) || r.st.Finished {
		return false, rejected("TEAM_FINISHED", "Team has finished")
	}
	count := 0
	for _, entry := range r.st.Applied {
		if entry.Member == string(*caller) && entry.Turn == active.Turn {
			count++
		}
	}
	if count >= team.AppliedPerTurnMax || len(request) > 256 {
		return false, rejected("TEAM_TEXT_TOO_LONG", "Too many team writes in this turn; finish the turn before writing more")
	}
	return false, nil
}
func (r *run) leader(caller *contract.MemberName) error {
	if caller == nil || string(*caller) != r.in.Team.Leader {
		return rejected("TEAM_NOT_LEADER", "Only the leader may perform this operation")
	}
	return nil
}
func (r *run) role(member string) contract.TeamRole {
	for _, entry := range r.st.Board.Roster {
		if string(entry.Name) == member {
			return entry.Role
		}
	}
	return ""
}
func (r *run) task(id string) *contract.TeamTask {
	for i := range r.st.Board.Tasks {
		if r.st.Board.Tasks[i].TaskId == id {
			return &r.st.Board.Tasks[i]
		}
	}
	return nil
}
func (r *run) capacity(value any) error {
	if team.EncodedLen(r.st.Board)+team.EncodedLen(value) > team.BoardBytesMax {
		return rejected("TEAM_TEXT_TOO_LONG", "The team board is full; shorten task text")
	}
	return nil
}
func (r *run) applied(id string) bool {
	_, ok := r.st.Applied[id]
	return ok
}
func (r *run) remember(id string, caller *contract.MemberName, result any) {
	raw, _ := team.Marshal(result)
	member := string(*caller)
	r.st.Applied[id] = w.AppliedUpdate{Member: member, Turn: r.st.Active[member].Turn, Result: raw}
	r.st.Dirty = true
	r.st.Steps++
}
func (r *run) emit(typ string, value any) {
	raw, _ := team.Marshal(value)
	r.st.Outbox = append(r.st.Outbox, w.TeamEvent{Type: typ, Data: raw})
	r.st.Dirty = true
}
func (r *run) updated(task *contract.TeamTask, auto bool) {
	r.emit("team.task.updated", contract.TeamTaskUpdated{Task: *task, AutoCompleted: auto})
}
func (r *run) end(ctx workflow.Context, member string) {
	if active, ok := r.st.Active[member]; ok {
		r.signal(ctx, active, w.EndTurn, struct{}{})
	}
}
func (r *run) validateCreate(in contract.CreateTasksInput) error {
	if applied, err := r.common(in.Caller, in.RequestId); applied || err != nil {
		return err
	}
	if err := r.leader(in.Caller); err != nil {
		return err
	}
	if len(in.Tasks)+len(r.st.Board.Tasks) > team.MaxTasks {
		return rejected("TEAM_TOO_MANY_TASKS", "Too many tasks")
	}
	if len(in.Tasks) == 0 {
		return rejected("TEAM_BAD_TRANSITION", "At least one task is required")
	}
	for _, task := range in.Tasks {
		if task.Title == "" || task.Description == "" || len(task.Title) > team.TitleBytes ||
			len(task.Description) > team.DescriptionBytes || !team.ValidTools(task.NarrowedTools) {
			return textError()
		}
		if task.Assignee != nil && r.role(string(*task.Assignee)) != "teammate" {
			return rejected("TEAM_UNKNOWN_MEMBER", "Assignee must be a teammate")
		}
		for _, reviewer := range task.Reviewers {
			if r.role(string(reviewer)) != "reviewer" {
				return rejected("TEAM_NOT_REVIEWER", "Invalid reviewer")
			}
		}
		for _, dep := range task.Deps {
			if r.task(dep) == nil {
				return rejected("TEAM_UNKNOWN_TASK", "Unknown dependency")
			}
		}
	}
	tasks := make([]contract.TeamTask, len(in.Tasks))
	for i, input := range in.Tasks {
		tasks[i] = r.newTask(input, r.st.NextTask+i+1)
	}
	return r.capacity(tasks)
}
func (r *run) create(_ workflow.Context, in contract.CreateTasksInput) (contract.CreateTasksOutput, error) {
	if applied, ok := r.st.Applied[in.RequestId]; ok {
		var out contract.CreateTasksOutput
		err := team.Unmarshal(applied.Result, &out)
		return out, err
	}
	out := contract.CreateTasksOutput{TaskIds: []string{}}
	for _, input := range in.Tasks {
		r.st.NextTask++
		task := r.newTask(input, r.st.NextTask)
		r.st.Board.Tasks = append(r.st.Board.Tasks, task)
		out.TaskIds = append(out.TaskIds, task.TaskId)
		r.emit("team.task.created", contract.TeamTaskCreated{Task: task})
	}
	r.st.EverCreated, r.st.IdleWakes = true, 0
	r.remember(in.RequestId, in.Caller, out)
	return out, nil
}
func (r *run) validateAssign(in contract.AssignTaskInput) error {
	if applied, err := r.common(in.Caller, in.RequestId); applied || err != nil {
		return err
	}
	if err := r.leader(in.Caller); err != nil {
		return err
	}
	task := r.task(in.TaskId)
	if task == nil {
		return rejected("TEAM_UNKNOWN_TASK", "Unknown task")
	}
	if task.Status != "pending" && task.Status != "blocked" && (task.Status != "failed" || !team.CanTransition(task.Status, "pending")) {
		return rejected("TEAM_BAD_TRANSITION", "Task cannot be assigned")
	}
	if r.role(string(in.Member)) != "teammate" {
		return rejected("TEAM_UNKNOWN_MEMBER", "Assignee must be a teammate")
	}
	if !team.ValidTools(in.NarrowedTools) {
		return textError()
	}
	stored := *task
	stored.Assignee, stored.NarrowedTools = new(in.Member), slices.Clone(in.NarrowedTools)
	return r.taskCapacity(*task, stored)
}
func (r *run) assign(_ workflow.Context, in contract.AssignTaskInput) error {
	if r.applied(in.RequestId) {
		return nil
	}
	task := r.task(in.TaskId)
	task.Assignee, task.NarrowedTools = new(in.Member), slices.Clone(in.NarrowedTools)
	if task.Status == "failed" {
		task.Status, task.Failures = "pending", new(0)
		if task.Review != nil {
			task.Review.Round, task.Review.Votes = new(0), nil
		}
		if !team.DependenciesReady(*task, r.st.Board, r.active()) {
			task.Status = "blocked"
		}
	}
	r.st.IdleWakes = 0
	r.updated(task, false)
	r.remember(in.RequestId, in.Caller, struct{}{})
	return nil
}
func (r *run) validateComplete(in contract.CompleteTaskInput) error {
	if applied, err := r.common(in.Caller, in.RequestId); applied || err != nil {
		return err
	}
	task := r.task(in.TaskId)
	if task == nil {
		return rejected("TEAM_UNKNOWN_TASK", "Unknown task")
	}
	if task.Assignee == nil || *task.Assignee != *in.Caller {
		return rejected("TEAM_NOT_ASSIGNEE", "Caller is not the assignee")
	}
	active := r.st.Active[string(*in.Caller)]
	if !team.CanTransition(task.Status, "completed") || task.Status != "in_progress" || active.Kind != "task" || active.TaskID != in.TaskId {
		return rejected("TEAM_BAD_TRANSITION", "Task is not in progress in this turn")
	}
	if len(in.ResultText) > team.ResultBytes || len(in.Files) > team.FilesMax {
		return textError()
	}
	for _, file := range in.Files {
		if len(file) > team.FileBytes {
			return textError()
		}
	}
	stored := *task
	stored.ResultText, stored.Files = new(in.ResultText), slices.Clone(in.Files)
	return r.taskCapacity(*task, stored)
}
func (r *run) complete(ctx workflow.Context, in contract.CompleteTaskInput) error {
	if r.applied(in.RequestId) {
		return nil
	}
	task := r.task(in.TaskId)
	task.ResultText, task.Files = new(in.ResultText), slices.Clone(in.Files)
	r.completeTask(task)
	r.end(ctx, string(*in.Caller))
	r.updated(task, false)
	r.remember(in.RequestId, in.Caller, struct{}{})
	return nil
}
func (r *run) completeTask(task *contract.TeamTask) {
	task.Status = "completed"
	if task.Review != nil && len(task.Review.Reviewers) > 0 {
		task.Status, task.Review.Votes = "in_review", nil
	}
}
func (r *run) validateVerdict(in contract.SubmitVerdictInput) error {
	if applied, err := r.common(in.Caller, in.RequestId); applied || err != nil {
		return err
	}
	task := r.task(in.TaskId)
	if task == nil {
		return rejected("TEAM_UNKNOWN_TASK", "Unknown task")
	}
	if task.Review == nil || !slices.Contains(task.Review.Reviewers, *in.Caller) {
		return rejected("TEAM_NOT_REVIEWER", "Caller is not a reviewer")
	}
	active := r.st.Active[string(*in.Caller)]
	if !team.CanTransition(task.Status, "completed") || task.Status != "in_review" || active.Kind != "review" || active.TaskID != in.TaskId {
		return rejected("TEAM_BAD_TRANSITION", "Task is not in review in this turn")
	}
	for _, vote := range task.Review.Votes {
		if vote.Reviewer == *in.Caller {
			return rejected("TEAM_BAD_TRANSITION", "Already voted")
		}
	}
	if len(in.Comment) > team.CommentBytes {
		return textError()
	}
	vote := contract.ReviewVote{Reviewer: *in.Caller, Verdict: in.Verdict, Comment: in.Comment}
	return r.capacity([]contract.ReviewVote{vote})
}
func (r *run) verdict(ctx workflow.Context, in contract.SubmitVerdictInput) error {
	if r.applied(in.RequestId) {
		return nil
	}
	task := r.task(in.TaskId)
	task.Review.Votes = append(task.Review.Votes, contract.ReviewVote{Reviewer: *in.Caller, Verdict: in.Verdict, Comment: in.Comment})
	r.end(ctx, string(*in.Caller))
	r.settle(ctx, task)
	r.remember(in.RequestId, in.Caller, struct{}{})
	return nil
}
func (r *run) settle(ctx workflow.Context, task *contract.TeamTask) {
	status, review := team.Settle(*task, r.in.Team.ReviewMaxRounds)
	if status != task.Status && !team.CanTransition(task.Status, status) {
		r.failure = "INVALID_SPEC"
		return
	}
	task.Status, task.Review = status, &review
	if status == "failed" {
		r.st.Escalations = append(r.st.Escalations, task.TaskId)
	}
	if status == "pending" || status == "failed" {
		for _, member := range workflow.DeterministicKeys(r.st.Active) {
			active := r.st.Active[member]
			if active.TaskID == task.TaskId && active.Kind == "review" {
				r.end(ctx, member)
			}
		}
	}
	r.updated(task, false)
}
func (r *run) validateSend(in contract.SendMessageInput) error {
	if applied, err := r.common(in.Caller, in.RequestId); applied || err != nil {
		return err
	}
	if r.role(string(in.To)) == "" || in.To == *in.Caller {
		return rejected("TEAM_UNKNOWN_MEMBER", "Invalid recipient")
	}
	if len(in.Text) > team.MessageBytes {
		return textError()
	}
	msg := r.mail(in)
	if len(r.st.Mail[string(in.To)]) >= team.MailboxCap || team.EncodedLen(r.st.Mail)+team.EncodedLen(msg) > team.MailBytesMax {
		return rejected("TEAM_MAILBOX_FULL", "Team mailbox is full")
	}
	return nil
}
func (r *run) mail(in contract.SendMessageInput) contract.MailboxMessage {
	return contract.MailboxMessage{MessageId: fmt.Sprintf("m%d", r.st.NextMail+1), From: string(*in.Caller),
		Text: new(in.Text), SentAt: time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)}
}
func (r *run) send(ctx workflow.Context, in contract.SendMessageInput) error {
	if r.applied(in.RequestId) {
		return nil
	}
	msg := r.mail(in)
	msg.SentAt = workflow.Now(ctx)
	r.st.NextMail++
	to := string(in.To)
	r.st.Mail[to] = append(r.st.Mail[to], msg)
	if active, ok := r.st.Active[to]; ok {
		r.signal(ctx, active, w.Deliver, w.DeliverInput{Mailbox: []contract.MailboxMessage{msg}})
	}
	r.emit("team.message", contract.TeamMessage{From: *in.Caller, To: in.To, Text: new(in.Text), MessageId: msg.MessageId})
	r.remember(in.RequestId, in.Caller, struct{}{})
	return nil
}
func (r *run) validateFinish(in contract.FinishTeamInput) error {
	if applied, err := r.common(in.Caller, in.RequestId); applied || err != nil {
		return err
	}
	if err := r.leader(in.Caller); err != nil {
		return err
	}
	if len(in.Summary) > team.SummaryBytes {
		return textError()
	}
	for _, task := range r.st.Board.Tasks {
		if task.Status == "in_progress" || task.Status == "in_review" {
			return rejected("TEAM_BAD_TRANSITION", "Tasks are still active")
		}
	}
	return nil
}
func (r *run) finish(ctx workflow.Context, in contract.FinishTeamInput) error {
	if r.applied(in.RequestId) {
		return nil
	}
	r.st.Finished, r.st.Summary = true, team.TruncateEncoded(in.Summary, team.SummaryBytes)
	for i := range r.st.Board.Tasks {
		task := &r.st.Board.Tasks[i]
		if (task.Status == "pending" || task.Status == "blocked") && team.CanTransition(task.Status, "cancelled") {
			task.Status = "cancelled"
			r.updated(task, false)
		}
	}
	r.end(ctx, r.in.Team.Leader)
	r.remember(in.RequestId, in.Caller, struct{}{})
	return nil
}
func (r *run) register(ctx workflow.Context) error {
	for _, err := range []error{
		workflow.SetUpdateHandlerWithOptions(ctx, "CreateTasks", r.create, workflow.UpdateHandlerOptions{Validator: r.validateCreate}),
		workflow.SetUpdateHandlerWithOptions(ctx, "AssignTask", r.assign, workflow.UpdateHandlerOptions{Validator: r.validateAssign}),
		workflow.SetUpdateHandlerWithOptions(ctx, "SendMessage", r.send, workflow.UpdateHandlerOptions{Validator: r.validateSend}),
		workflow.SetUpdateHandlerWithOptions(ctx, "CompleteTask", r.complete, workflow.UpdateHandlerOptions{Validator: r.validateComplete}),
		workflow.SetUpdateHandlerWithOptions(ctx, "SubmitVerdict", r.verdict, workflow.UpdateHandlerOptions{Validator: r.validateVerdict}),
		workflow.SetUpdateHandlerWithOptions(ctx, "FinishTeam", r.finish, workflow.UpdateHandlerOptions{Validator: r.validateFinish}),
	} {
		if err != nil {
			return err
		}
	}
	if err := workflow.SetQueryHandler(ctx, w.Board, func() (boardView, error) {
		return boardView{Board: r.st.Board, Active: r.st.Active, Finished: r.st.Finished}, nil
	}); err != nil {
		return err
	}
	return workflow.SetUpdateHandlerWithOptions(ctx, w.Steer, func(ctx workflow.Context, in w.SteerInput) (w.SteerOutput, error) {
		r.st.UserSteer = append(r.st.UserSteer, contract.SteerItem{ContentRef: new(in.ContentRef)})
		if active, ok := r.st.Active[r.in.Team.Leader]; ok {
			r.signal(ctx, active, w.Deliver, w.DeliverInput{Steer: []contract.SteerItem{{ContentRef: new(in.ContentRef)}}})
		}
		r.st.Dirty, r.st.Steps = true, r.st.Steps+1
		return w.SteerOutput{Queued: len(r.st.UserSteer)}, nil
	}, workflow.UpdateHandlerOptions{Validator: func(in w.SteerInput) error {
		if w.Terminal(r.st.Status) || r.st.Finished {
			return rejected("RUN_NOT_ACTIVE", "Run is not active")
		}
		if len(r.st.UserSteer) >= team.SteerCap {
			return rejected("QUEUE_FULL", "Steer queue is full")
		}
		if !strings.HasPrefix(in.ContentRef, "t/"+r.in.TenantID+"/runs/"+r.in.RunID+"/steer/") {
			return rejected("RUN_NOT_ACTIVE", "Run is not active")
		}
		return nil
	}})
}

func (r *run) newTask(input contract.NewTask, id int) contract.TeamTask {
	reviewers := slices.Clone(input.Reviewers)
	if len(reviewers) == 0 {
		for _, entry := range r.st.Board.Roster {
			if entry.Role == "reviewer" {
				reviewers = append(reviewers, entry.Name)
			}
		}
	}
	task := contract.TeamTask{TaskId: fmt.Sprintf("t%d", id), Title: input.Title, Description: input.Description,
		Deps: slices.Clone(input.Deps), Assignee: input.Assignee, NarrowedTools: slices.Clone(input.NarrowedTools),
		Status: "pending", Failures: new(0), Review: &contract.TaskReview{Reviewers: reviewers, Round: new(0)}}
	if !team.DependenciesReady(task, r.st.Board, r.active()) {
		task.Status = "blocked"
	}
	return task
}
func (r *run) taskCapacity(old, next contract.TeamTask) error {
	if team.EncodedLen(r.st.Board)-team.EncodedLen(old)+team.EncodedLen(next) > team.BoardBytesMax {
		return rejected("TEAM_TEXT_TOO_LONG", "The team board is full; shorten task text")
	}
	return nil
}

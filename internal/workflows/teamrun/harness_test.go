package teamrun

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/team"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type memberStart struct {
	In w.MemberTaskInput
	At time.Duration
}
type signalRecord struct {
	Workflow, Name string
	At             time.Duration
	Arg            any
}
type outcome struct {
	CallsAtAccept, CallsAtDone int
	Done                       bool
	Err                        error
	Output                     any
	Accepted                   bool
	DoneAt                     time.Duration
}
type turnScript struct {
	member string
	turn   int
	after  time.Duration
	result w.TurnResult
	fail   bool
}
type callback struct {
	at    time.Duration
	fn    func()
	fired bool
}

// harness drives TeamRunWorkflow in the test environment. Member turns are
// scripted in virtual time: a turn's WaitForTurn result arrives a fixed delay
// after the turn starts, and member Updates are sent by callbacks scheduled at
// absolute virtual offsets from the start of the run. When the workflow
// continues as new, the harness starts the next execution from the carry and
// re-schedules whatever has not happened yet, so a test reads the same with or
// without the workflow_test_limits build tag.
type harness struct {
	t             *testing.T
	env           *testsuite.TestWorkflowEnvironment
	in            w.TeamRunInput
	mu            sync.Mutex
	starts        []memberStart
	signals       []signalRecord
	boards        []w.RecordBoardInput
	statuses      []w.StatusInput
	waits         []w.TurnOp
	calls         []string
	final         *w.FinalizeTeamInput
	scripts       []turnScript
	callbacks     []*callback
	carries       []w.TeamRunInput
	epoch         time.Duration
	base          time.Time
	ended         time.Time
	runs          int
	epochs        []time.Duration
	signalDelay   time.Duration
	seq           int
	onMemberStart func(w.MemberTaskInput)
}

func roster() []contract.RosterEntry {
	return []contract.RosterEntry{
		{Name: "leader", Role: "leader", Status: "idle", Description: "lead"},
		{Name: "alice", Role: "teammate", Status: "idle", Description: "work"},
		{Name: "bob", Role: "teammate", Status: "idle", Description: "work"},
		{Name: "reviewer", Role: "reviewer", Status: "idle", Description: "review"},
	}
}
func baseInput() w.TeamRunInput {
	return w.TeamRunInput{V: 1, TenantID: "tenant", WorkspaceID: "workspace", SessionID: "session", RunID: "run",
		Lock:     contract.SegmentLockRef{Id: "lock", Digest: "sha256:" + strings.Repeat("0", 64)},
		InputRef: "input", EventSubject: "run.tenant.session.run",
		BudgetMillicredits: 100000, Limits: w.DefaultLimits(100000), Tier: "standard",
		Sandbox: w.SandboxSpec{IsolationKey: "isolation"},
		Team:    w.TeamSpec{Roster: roster(), Leader: "leader", MaxParallel: 2, ReviewMaxRounds: 3}}
}
func newHarness(t *testing.T, mutate ...func(*w.TeamRunInput)) *harness {
	t.Helper()
	h := &harness{t: t, in: baseInput()}
	for _, fn := range mutate {
		fn(&h.in)
	}
	return h
}
func (h *harness) vnow(at time.Time) time.Duration { return h.epoch + at.Sub(h.base) }
func (h *harness) startOf(member string, turn int) (memberStart, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, item := range h.starts {
		if item.In.Member == member && item.In.Turn == turn {
			return item, true
		}
	}
	return memberStart{}, false
}
func (h *harness) scripted(member string, turn int) bool {
	for _, script := range h.scripts {
		if script.member == member && script.turn == turn {
			return true
		}
	}
	return false
}
func matchTurn(member string, turn int) any {
	return mock.MatchedBy(func(in w.TurnOp) bool { return in.Member == member && in.Turn == turn })
}

// remaining converts "delay after the turn started" into a delay from the start of this execution for turns
// that started in an earlier execution.
func (h *harness) remaining(member string, turn int, after time.Duration) time.Duration {
	if started, ok := h.startOf(member, turn); ok {
		return max(started.At+after-h.epoch, 0)
	}
	return after
}
func (h *harness) newEnv() {
	var suite testsuite.WorkflowTestSuite
	h.env = suite.NewTestWorkflowEnvironment()
	// Every execution continues the same virtual clock, so StartedAt arithmetic survives Continue-As-New.
	h.env.SetStartTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(h.epoch))
	h.base = h.env.Now()
	h.ended = time.Time{}
	h.env.SetTestTimeout(30 * time.Second)
	h.env.RegisterWorkflowWithOptions(func(ctx workflow.Context, in w.TeamRunInput) error {
		err := Run(ctx, in)
		h.ended = workflow.Now(ctx)
		return err
	}, workflow.RegisterOptions{Name: w.TeamRunWorkflow})
	// The member stub stays open and records the signals the team sends it; the scripted WaitForTurn result,
	// not the stub, ends each turn.
	h.env.RegisterWorkflowWithOptions(func(ctx workflow.Context, in w.MemberTaskInput) (w.TurnResult, error) {
		h.mu.Lock()
		h.starts = append(h.starts, memberStart{In: in, At: h.vnow(workflow.Now(ctx))})
		h.mu.Unlock()
		if h.onMemberStart != nil {
			h.onMemberStart(in)
		}
		id := workflow.GetInfo(ctx).WorkflowExecution.ID
		for _, name := range []string{w.Deliver, w.EndTurn, w.Cancel, w.Pause, w.Resume} {
			workflow.Go(ctx, func(ctx workflow.Context) {
				ch := workflow.GetSignalChannel(ctx, name)
				for {
					var arg any
					if !ch.Receive(ctx, &arg) {
						return
					}
					h.mu.Lock()
					h.signals = append(h.signals, signalRecord{Workflow: id, Name: name, Arg: arg, At: h.vnow(workflow.Now(ctx))})
					h.mu.Unlock()
				}
			})
		}
		return w.TurnResult{}, workflow.Sleep(ctx, 24*time.Hour)
	}, workflow.RegisterOptions{Name: w.MemberTaskWorkflow})
	h.record("PrepareTeamRun", func(context.Context, w.PrepareTeamInput) error { return nil })
	h.record("SetRunStatus", func(_ context.Context, in w.StatusInput) error {
		h.mu.Lock()
		h.statuses = append(h.statuses, in)
		h.mu.Unlock()
		return nil
	})
	h.record("AcquireSandbox", func(context.Context, w.AcquireSandboxInput) (contract.SandboxLease, error) {
		return contract.SandboxLease{Provider: "local", SandboxId: "sandbox"}, nil
	})
	h.record("RecordBoard", func(_ context.Context, in w.RecordBoardInput) error {
		h.mu.Lock()
		h.boards = append(h.boards, in)
		h.mu.Unlock()
		return nil
	})
	h.record("FinalizeTeamRun", func(_ context.Context, in w.FinalizeTeamInput) error {
		h.mu.Lock()
		h.final = &in
		h.mu.Unlock()
		return nil
	})
	h.env.RegisterActivityWithOptions(func(context.Context, w.TurnOp) (w.TurnResult, error) { return w.TurnResult{}, nil },
		activity.RegisterOptions{Name: "WaitForTurn"})
	h.env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, args converter.EncodedValues) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.calls = append(h.calls, info.ActivityType.Name)
		if info.ActivityType.Name == "WaitForTurn" {
			var op w.TurnOp
			if args.Get(&op) == nil {
				h.waits = append(h.waits, op)
			}
		}
	})
	// Signals to members that live in an earlier execution (or never existed here) reach this mock.
	external := h.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", mock.Anything,
		mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.signals = append(h.signals, signalRecord{Workflow: args.String(1), Name: args.String(3), Arg: args.Get(4), At: h.vnow(h.env.Now())})
	})
	if h.signalDelay > 0 && h.runs > 1 {
		external.After(h.signalDelay)
	}
	for _, script := range h.scripts {
		call := h.env.OnActivity("WaitForTurn", mock.Anything, matchTurn(script.member, script.turn))
		if script.fail {
			call.Return(w.TurnResult{}, temporal.NewNonRetryableApplicationError("wait failed", "WAIT_FAILED", nil))
			continue
		}
		call.After(h.remaining(script.member, script.turn, script.after)).Return(script.result, nil)
	}
	if h.in.Carry != nil {
		for _, member := range slices.Sorted(maps.Keys(h.in.Carry.Active)) {
			active := h.in.Carry.Active[member]
			if !h.scripted(member, active.Turn) {
				h.env.OnActivity("WaitForTurn", mock.Anything, matchTurn(member, active.Turn)).After(h.remaining(member, active.Turn, time.Minute)).
					Return(defaultTurn(w.TurnOp{Member: member, Turn: active.Turn}), nil)
			}
		}
	}
	h.env.OnActivity("WaitForTurn", mock.Anything, mock.Anything).After(time.Minute).Return(
		func(_ context.Context, in w.TurnOp) (w.TurnResult, error) { return defaultTurn(in), nil })
	for _, item := range h.callbacks {
		if !item.fired {
			h.env.RegisterDelayedCallback(func() {
				// The environment keeps its clock running for open children after the root execution ends.
				if h.ended.IsZero() {
					item.fired = true
					item.fn()
				}
			}, max(item.at-h.epoch, 10*time.Millisecond)) // after the workflow registered its handlers
		}
	}
}
func defaultTurn(in w.TurnOp) w.TurnResult {
	return w.TurnResult{Member: in.Member, Turn: in.Turn, Status: "completed", Cursor: w.MemberCursor{Turn: in.Turn},
		Final: &contract.FinalMessage{MessageId: "final", TextPreview: "done"}}
}

// record registers a recording Activity.
func (h *harness) record(name string, fn any) {
	h.env.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
}

// turn scripts the result of one member turn, delivered after the given delay from the turn's start.
func (h *harness) turn(member string, turn int, after time.Duration, result w.TurnResult) {
	result.Member, result.Turn = member, turn
	if result.Status == "" {
		result.Status = "completed"
	}
	if result.Cursor == (w.MemberCursor{}) {
		result.Cursor.Turn = turn
	}
	h.scripts = append(h.scripts, turnScript{member: member, turn: turn, after: after, result: result})
}

// turnError scripts WaitForTurn to fail without retries.
func (h *harness) turnError(member string, turn int) {
	h.scripts = append(h.scripts, turnScript{member: member, turn: turn, fail: true})
}

// run executes the workflow, following Continue-As-New, and returns the final error. Turns without a
// script complete one minute after they start.
func (h *harness) run() error {
	for {
		h.runs++
		h.newEnv()
		h.env.ExecuteWorkflow(w.TeamRunWorkflow, h.in)
		require.True(h.t, h.env.IsWorkflowCompleted())
		err := h.env.GetWorkflowError()
		var next *workflow.ContinueAsNewError
		if !errors.As(err, &next) {
			return err
		}
		var in w.TeamRunInput
		require.NoError(h.t, converter.GetDefaultDataConverter().FromPayloads(next.Input, &in))
		h.carries = append(h.carries, in)
		h.epoch = h.vnow(h.ended)
		h.epochs = append(h.epochs, h.epoch)
		h.in = in
		require.Less(h.t, h.runs, 60, "the workflow keeps continuing as new")
	}
}
func (h *harness) at(after time.Duration, fn func()) {
	h.callbacks = append(h.callbacks, &callback{at: after, fn: fn})
}

// update sends an Update from inside a callback.
func (h *harness) update(name, request string, input any) *outcome {
	out := &outcome{}
	h.seq++
	h.env.UpdateWorkflow(name, fmt.Sprintf("%s-%s-%d", name, request, h.seq), &testsuite.TestUpdateCallback{
		OnAccept: func() { out.Accepted, out.CallsAtAccept = true, h.callCount() },
		OnReject: func(err error) { out.Done, out.Err = true, err },
		OnComplete: func(value any, err error) {
			out.Done, out.Output, out.Err, out.CallsAtDone = true, value, err, h.callCount()
		},
	}, input)
	return out
}

func (h *harness) member(name string) *contract.MemberName { return new(contract.MemberName(name)) }
func (h *harness) startsOf(member string) []memberStart {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []memberStart
	for _, item := range h.starts {
		if item.In.Member == member {
			out = append(out, item)
		}
	}
	return out
}
func (h *harness) signalsNamed(name string) []signalRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []signalRecord
	for _, item := range h.signals {
		if item.Name == name {
			out = append(out, item)
		}
	}
	return out
}
func (h *harness) lastBoard() contract.TeamBoard {
	h.mu.Lock()
	defer h.mu.Unlock()
	require.NotEmpty(h.t, h.boards)
	return h.boards[len(h.boards)-1].Board
}
func (h *harness) task(id string) contract.TeamTask {
	for _, task := range h.lastBoard().Tasks {
		if task.TaskId == id {
			return task
		}
	}
	h.t.Fatalf("task %s is not on the board", id)
	return contract.TeamTask{}
}
func newTask(title string, assignee string, deps ...string) contract.NewTask {
	task := contract.NewTask{Title: title, Description: "do " + title, Deps: deps}
	if assignee != "" {
		task.Assignee = new(contract.MemberName(assignee))
	}
	return task
}
func (h *harness) create(caller, request string, tasks ...contract.NewTask) *outcome {
	return h.update("CreateTasks", request, contract.CreateTasksInput{Caller: h.member(caller), RequestId: request, Tasks: tasks})
}
func (h *harness) completeTask(caller, request, task string) *outcome {
	return h.update("CompleteTask", request,
		contract.CompleteTaskInput{Caller: h.member(caller), RequestId: request, TaskId: task, ResultText: "ready"})
}
func (h *harness) verdict(caller, request, task string, verdict contract.Verdict) *outcome {
	return h.update("SubmitVerdict", request, contract.SubmitVerdictInput{Caller: h.member(caller), RequestId: request, TaskId: task,
		Verdict: verdict, Comment: "review"})
}
func (h *harness) finish(caller, request, summary string) *outcome {
	return h.update("FinishTeam", request, contract.FinishTeamInput{Caller: h.member(caller), RequestId: request, Summary: summary})
}
func (h *harness) send(caller, request, to, text string) *outcome {
	return h.update("SendMessage", request,
		contract.SendMessageInput{Caller: h.member(caller), RequestId: request, To: contract.MemberName(to), Text: text})
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var app *temporal.ApplicationError
	require.ErrorAs(t, err, &app)
	require.Equal(t, code, app.Type())
}

// allTurns returns every turn row recorded through RecordBoard.
func (h *harness) allTurns() []w.TurnRow {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []w.TurnRow
	for _, board := range h.boards {
		out = append(out, board.Turns...)
	}
	return out
}
func (h *harness) cancelAt(after time.Duration) {
	h.at(after, func() { h.env.SignalWorkflow(w.Cancel, w.CancelInput{Reason: "test"}) })
}
func (h *harness) withoutReviewers() {
	h.in.Team.Roster = h.in.Team.Roster[:3]
}
func (h *harness) sawStatus(id string, status contract.TaskStatus) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, board := range h.boards {
		for _, task := range board.Board.Tasks {
			if task.TaskId == id && task.Status == status {
				return true
			}
		}
	}
	return false
}

func (h *harness) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.calls)
}

// needTurns skips scenarios that start more turns than the workflow_test_limits build allows.
func needTurns(t *testing.T, n int) {
	t.Helper()
	if team.MaxTurnsTotal < n {
		t.Skipf("needs %d turns; this build allows %d", n, team.MaxTurnsTotal)
	}
}

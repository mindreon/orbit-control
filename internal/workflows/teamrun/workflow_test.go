package teamrun

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/team"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

const sec = time.Second

func TestLeaderWithoutTasks(t *testing.T) {
	// T-R12 (F16): a leader turn that created no tasks finishes the run with its Final as the Summary
	// and is never woken with all_settled.
	h := newHarness(t, func(in *w.TeamRunInput) { in.Team.Roster = in.Team.Roster[:1] })
	h.turn("leader", 1, sec, w.TurnResult{Final: &contract.FinalMessage{MessageId: "m", TextPreview: "done"}})
	require.NoError(t, h.run())
	require.Equal(t, "succeeded", h.final.Status)
	require.Equal(t, "done", h.final.Summary)
	require.Len(t, h.startsOf("leader"), 1)
}

func TestDependencyAndReviewOrdering(t *testing.T) {
	// T-R1 (F45, I1): neither review nor the dependent task starts before the producing turn has ended.
	h := newHarness(t)
	var created, finished *outcome
	h.at(5*sec, func() { created = h.create("leader", "c1", newTask("t1", "alice")) })
	h.at(6*sec, func() { h.create("leader", "c2", newTask("t2", "alice", "t1")) })
	h.at(15*sec, func() { h.completeTask("alice", "a1", "t1") })
	h.at(35*sec, func() { h.verdict("reviewer", "v1", "t1", "approve") })
	h.at(50*sec, func() { h.completeTask("alice", "a2", "t2") })
	h.at(60*sec, func() { h.verdict("reviewer", "v2", "t2", "approve") })
	h.at(100*sec, func() { finished = h.finish("leader", "f1", "all done") })
	h.turn("leader", 1, 110*sec, w.TurnResult{Status: "ended"})
	h.turn("alice", 1, 25*sec, w.TurnResult{Status: "ended"})
	h.turn("reviewer", 1, 10*sec, w.TurnResult{Status: "ended"})
	h.turn("alice", 2, 15*sec, w.TurnResult{Status: "ended"})
	h.turn("reviewer", 2, 10*sec, w.TurnResult{Status: "ended"})
	require.NoError(t, h.run())
	require.NoError(t, created.Err)
	require.NoError(t, finished.Err)
	require.Equal(t, 30*sec, h.startsOf("reviewer")[0].At, "review waits for the assignee turn to end")
	require.Equal(t, 40*sec, h.startsOf("alice")[1].At, "t2 waits for t1's review turn to end")
	require.Equal(t, "succeeded", h.final.Status)
	require.Equal(t, contract.TaskStatus("completed"), h.task("t1").Status)
	require.Equal(t, contract.TaskStatus("completed"), h.task("t2").Status)
}

func TestDependentWaitsForProducerTurnWithoutReviewers(t *testing.T) {
	// T-R2 (F45): with no reviewer the task is completed at once, but its turn is still running.
	h := newHarness(t, func(in *w.TeamRunInput) { in.Team.Roster = in.Team.Roster[:3] })
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	h.at(6*sec, func() { h.create("leader", "c2", newTask("t2", "bob", "t1")) })
	h.at(15*sec, func() { h.completeTask("alice", "a1", "t1") })
	h.at(100*sec, func() { h.finish("leader", "f1", "done") })
	h.turn("leader", 1, 110*sec, w.TurnResult{Status: "ended"})
	h.turn("alice", 1, 25*sec, w.TurnResult{Status: "ended"})
	h.turn("bob", 1, 10*sec, w.TurnResult{Status: "ended"})
	require.NoError(t, h.run())
	require.Equal(t, 30*sec, h.startsOf("bob")[0].At)
	require.Equal(t, contract.TaskStatus("completed"), h.task("t1").Status)
}

func TestRejectRoundsEscalateAndReassign(t *testing.T) {
	// T-R3 (§1.4.5): the third reject fails the task and wakes the leader with an escalation;
	// reassigning resets failures and the review round.
	needTurns(t, 10)
	h := newHarness(t)
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	for round := range 3 {
		request := string(rune('1' + round))
		h.at(time.Duration(7+12*round)*sec, func() { h.completeTask("alice", "a"+request, "t1") })
		h.at(time.Duration(13+12*round)*sec, func() { h.verdict("reviewer", "v"+request, "t1", "reject") })
		h.turn("alice", round+1, 6*sec, w.TurnResult{Status: "ended"})
		h.turn("reviewer", round+1, 6*sec, w.TurnResult{Status: "ended"})
	}
	h.at(40*sec, func() {
		h.update("AssignTask", "as1", contract.AssignTaskInput{Caller: h.member("leader"), RequestId: "as1", TaskId: "t1", Member: "bob"})
	})
	h.turn("leader", 1, 20*sec, w.TurnResult{Status: "completed"})
	h.cancelAt(60 * sec)
	require.NoError(t, h.run())
	leader := h.startsOf("leader")
	require.Len(t, leader, 2)
	require.Equal(t, 37*sec, leader[1].At, "the escalation wakes the idle leader at the third reject")
	require.Contains(t, *leader[1].In.Input.Text, "[Tasks Escalated]")
	require.Len(t, h.startsOf("bob"), 1)
	require.True(t, h.sawStatus("t1", "failed"))
	task := h.task("t1")
	require.Equal(t, "bob", string(*task.Assignee))
	require.Equal(t, 0, *task.Failures)
	require.Equal(t, 0, *task.Review.Round)
	require.Empty(t, task.Review.Votes)
}

func TestMaxParallelAndLeaderQuota(t *testing.T) {
	// T-R4 (F6): MaxParallel counts reviewer turns but not the leader turn.
	h := newHarness(t, func(in *w.TeamRunInput) { in.Team.MaxParallel = 1 })
	h.at(5*sec, func() {
		h.create("leader", "c1", newTask("t1", "alice"), newTask("t2", "bob"), newTask("t3", "alice"))
	})
	h.turn("leader", 1, 600*sec, w.TurnResult{Status: "ended"})
	h.cancelAt(200 * sec)
	require.NoError(t, h.run())
	require.Equal(t, time.Duration(0), h.startsOf("leader")[0].At, "the leader turn runs beside a full quota")
	var previous time.Duration = -time.Hour
	for _, start := range h.starts {
		if start.In.Member == "leader" {
			continue
		}
		require.GreaterOrEqual(t, start.At-previous, 60*sec, "only one non-leader turn at a time")
		previous = start.At
	}
	require.GreaterOrEqual(t, len(h.starts), 3)
}

func TestCompleteTaskByNonAssigneeIsRejected(t *testing.T) {
	// T-R5: only the assignee completes a task; a rejected request leaves no Applied record.
	h := newHarness(t)
	var wrong, right *outcome
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	h.at(10*sec, func() { wrong = h.completeTask("leader", "x", "t1") })
	h.at(15*sec, func() { right = h.completeTask("alice", "x", "t1") })
	h.turn("leader", 1, 100*sec, w.TurnResult{Status: "ended"})
	h.turn("alice", 1, 20*sec, w.TurnResult{Status: "ended"})
	h.cancelAt(40 * sec)
	require.NoError(t, h.run())
	requireCode(t, wrong.Err, "TEAM_NOT_ASSIGNEE")
	require.NoError(t, right.Err)
	require.True(t, h.sawStatus("t1", "in_review"))
}

func TestSendMessageDeliveryPaths(t *testing.T) {
	// T-R8 (§1.4.3): in-flight members get a Deliver signal; idle members receive mail with their next turn;
	// mail the turn did not report as delivered is offered again.
	h := newHarness(t, func(in *w.TeamRunInput) { in.Team.Roster = in.Team.Roster[:3] })
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	var m1, m2 *outcome
	h.at(10*sec, func() { m1 = h.send("leader", "m1", "alice", "to alice") })
	h.at(11*sec, func() { m2 = h.send("leader", "m2", "bob", "to bob") })
	h.at(12*sec, func() { h.completeTask("alice", "a1", "t1") })
	h.at(30*sec, func() { h.create("leader", "c2", newTask("t2", "alice")) })
	h.at(40*sec, func() { h.create("leader", "c3", newTask("t3", "bob")) })
	h.turn("leader", 1, 300*sec, w.TurnResult{Status: "ended"})
	h.turn("alice", 1, 20*sec, w.TurnResult{Status: "ended"})
	h.cancelAt(100 * sec)
	require.NoError(t, h.run())
	require.NoError(t, m1.Err)
	require.NoError(t, m2.Err)
	deliveries := h.signalsNamed(w.Deliver)
	require.Len(t, deliveries, 1)
	require.Equal(t, w.MemberTaskWorkflowID("tenant", "run", "alice", 1), deliveries[0].Workflow)
	second := h.startsOf("alice")[1].In
	require.Len(t, second.Mailbox, 1)
	require.Equal(t, "m1", second.Mailbox[0].MessageId)
	first := h.startsOf("bob")[0].In
	require.Len(t, first.Mailbox, 1)
	require.Equal(t, "to bob", *first.Mailbox[0].Text)
}

func TestIdleLeaderWokenByMail(t *testing.T) {
	// T-R8 (§1.4.5): mail for an idle leader starts a leader turn that carries it.
	h := newHarness(t)
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	h.at(22*sec, func() { h.send("alice", "m1", "leader", "question") })
	h.turn("leader", 1, 20*sec, w.TurnResult{Status: "completed"})
	h.turn("alice", 1, 60*sec, w.TurnResult{Status: "ended"})
	h.turn("leader", 2, 5*sec, w.TurnResult{Status: "ended", DeliveredMail: []string{"m1"}})
	h.cancelAt(40 * sec)
	require.NoError(t, h.run())
	leader := h.startsOf("leader")
	require.Equal(t, 22*sec, leader[1].At)
	require.Equal(t, "m1", leader[1].In.Mailbox[0].MessageId)
	require.Equal(t, team.MailNotice, *leader[1].In.Input.Text)
}

func TestMailboxCapacity(t *testing.T) {
	// T-R9 (F44): the per-recipient cap rejects with TEAM_MAILBOX_FULL.
	h := newHarness(t)
	var results []*outcome
	for i := range team.MailboxCap + 1 {
		request := "m" + string(rune('a'+i))
		h.at(time.Duration(5+i)*sec, func() { results = append(results, h.send("leader", request, "alice", "hello")) })
	}
	h.turn("leader", 1, 100*sec, w.TurnResult{Status: "ended"})
	h.cancelAt(60 * sec)
	require.NoError(t, h.run())
	for _, result := range results[:team.MailboxCap] {
		require.NoError(t, result.Err)
	}
	requireCode(t, results[team.MailboxCap].Err, "TEAM_MAILBOX_FULL")
}

func autoComplete(t *testing.T) *harness {
	h := newHarness(t)
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	h.turn("leader", 1, 100*sec, w.TurnResult{Status: "ended"})
	h.turn("alice", 1, 10*sec, w.TurnResult{Status: "completed", Final: &contract.FinalMessage{MessageId: "f", TextPreview: "auto result"}})
	h.turn("reviewer", 1, 5*sec, w.TurnResult{Status: "completed"})
	h.turn("alice", 2, 100*sec, w.TurnResult{Status: "ended"})
	h.cancelAt(30 * sec)
	return h
}

func TestTaskTurnWithoutCompleteTaskIsAutoCompleted(t *testing.T) {
	// T-R10 (F13): a completed task turn that never called CompleteTask moves to review with the Final preview.
	h := autoComplete(t)
	require.NoError(t, h.run())
	var auto bool
	for _, board := range h.boards {
		for _, event := range board.Events {
			if event.Type == "team.task.updated" && strings.Contains(string(event.Data), `"auto_completed":true`) {
				auto = true
			}
		}
	}
	require.True(t, auto)
	require.Len(t, h.startsOf("reviewer"), 1, "the auto-completed task reaches review")
}

func TestReviewTurnWithoutVerdictRejects(t *testing.T) {
	// T-R11 (F14): a review turn that ended without a vote records a reject and settles.
	h := autoComplete(t)
	require.NoError(t, h.run())
	task := h.task("t1")
	require.NotNil(t, task.Review.ReworkNote)
	require.Contains(t, *task.Review.ReworkNote, "did not submit a verdict")
	require.Equal(t, 1, *task.Review.Round)
	require.Len(t, h.startsOf("alice"), 2, "the rejected task is reworked")
}

func TestAllSettledWakesLeaderOnce(t *testing.T) {
	// T-R13 (F63): without FinishTeam, exactly one all_settled wake runs, then the run succeeds with its Final.
	h := newHarness(t, func(in *w.TeamRunInput) { in.Team.Roster = in.Team.Roster[:3] })
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	h.at(12*sec, func() { h.completeTask("alice", "a1", "t1") })
	h.turn("leader", 1, 5*sec, w.TurnResult{Status: "completed"})
	h.turn("alice", 1, 8*sec, w.TurnResult{Status: "ended"})
	h.turn("leader", 2, 10*sec, w.TurnResult{Status: "completed", Final: &contract.FinalMessage{MessageId: "f", TextPreview: "wrap up"}})
	require.NoError(t, h.run())
	leader := h.startsOf("leader")
	require.Len(t, leader, 2)
	require.Equal(t, team.SettledNotice, *leader[1].In.Input.Text)
	require.Equal(t, "succeeded", h.final.Status)
	require.Equal(t, "wrap up", h.final.Summary)
}

func TestFinishTeamRules(t *testing.T) {
	// T-R14: FinishTeam is refused while a task runs; afterwards it succeeds and cancels pending tasks.
	h := newHarness(t, func(in *w.TeamRunInput) { in.Team.Roster = in.Team.Roster[:3] })
	var early, late *outcome
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	h.at(6*sec, func() { h.create("leader", "c2", newTask("t2", "")) })
	h.at(10*sec, func() { early = h.finish("leader", "f1", "too soon") })
	h.at(12*sec, func() { h.completeTask("alice", "a1", "t1") })
	h.at(20*sec, func() { late = h.finish("leader", "f2", "all done") })
	h.turn("leader", 1, 30*sec, w.TurnResult{Status: "ended"})
	h.turn("alice", 1, 10*sec, w.TurnResult{Status: "ended"})
	require.NoError(t, h.run())
	requireCode(t, early.Err, "TEAM_BAD_TRANSITION")
	require.NoError(t, late.Err)
	require.Equal(t, "succeeded", h.final.Status)
	require.Equal(t, contract.TaskStatus("cancelled"), h.task("t2").Status)
	require.Equal(t, contract.TaskStatus("completed"), h.task("t1").Status)
}

func TestUsedMillicreditsIsMonotonicMax(t *testing.T) {
	// T-R15 (I3, F35): Used is the maximum reported ledger sum, not a sum and not the last value.
	h := newHarness(t, func(in *w.TeamRunInput) { in.Team.Roster = in.Team.Roster[:3] })
	h.at(2*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	h.at(3*sec, func() { h.create("leader", "c2", newTask("t2", "bob")) })
	h.turn("alice", 1, 8*sec, w.TurnResult{RunUsedMillicredits: 100})
	h.turn("bob", 1, 17*sec, w.TurnResult{RunUsedMillicredits: 300})
	h.turn("leader", 1, 30*sec, w.TurnResult{RunUsedMillicredits: 80})
	h.turn("leader", 2, 5*sec, w.TurnResult{Final: &contract.FinalMessage{MessageId: "f", TextPreview: "done"}})
	require.NoError(t, h.run())
	require.Equal(t, int64(300), h.startsOf("leader")[1].In.RunUsedMillicredits)
}

func TestReserveStopsMemberTurnsAndNotifiesLeaderOnce(t *testing.T) {
	// T-R16 (F35): beyond B-reserve no member turn starts, the leader gets exactly one budget_low turn,
	// and the run ends budget_exhausted with its summary.
	h := newHarness(t, func(in *w.TeamRunInput) { in.Team.Roster = in.Team.Roster[:3] })
	h.at(2*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	h.at(13*sec, func() { h.create("leader", "c2", newTask("t2", "bob")) })
	h.at(15*sec, func() { h.finish("leader", "f1", "wrap") })
	h.turn("alice", 1, 8*sec, w.TurnResult{RunUsedMillicredits: 95000})
	h.turn("leader", 1, 11*sec, w.TurnResult{Status: "completed"})
	h.turn("leader", 2, 5*sec, w.TurnResult{Status: "ended"})
	require.NoError(t, h.run())
	leader := h.startsOf("leader")
	require.Len(t, leader, 2)
	require.Equal(t, team.BudgetNotice, *leader[1].In.Input.Text)
	require.Empty(t, h.startsOf("bob"))
	require.Equal(t, "budget_exhausted", h.final.Status)
	require.Equal(t, "wrap", h.final.Summary)
}

func TestSegmentBudgetIsCappedByReserveShare(t *testing.T) {
	// T-R16b (F35): with two parallel members the per-segment budget is reserve/MaxParallel, so both
	// segments together cannot consume the leader's reserve.
	h := newHarness(t, func(in *w.TeamRunInput) { in.Team.Roster = in.Team.Roster[:3] })
	h.at(2*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	h.at(15*sec, func() { h.create("leader", "c2", newTask("t2", "alice")) })
	h.at(16*sec, func() { h.create("leader", "c3", newTask("t3", "bob")) })
	h.at(30*sec, func() { h.finish("leader", "f1", "wrap") })
	h.turn("alice", 1, 8*sec, w.TurnResult{RunUsedMillicredits: 80000})
	h.turn("leader", 1, 18*sec, w.TurnResult{Status: "completed"})
	h.turn("alice", 2, 10*sec, w.TurnResult{RunUsedMillicredits: 85000})
	h.turn("bob", 1, 10*sec, w.TurnResult{RunUsedMillicredits: 90000})
	h.turn("leader", 2, 20*sec, w.TurnResult{Status: "ended"})
	require.NoError(t, h.run())
	for _, start := range []memberStart{h.startsOf("alice")[1], h.startsOf("bob")[0]} {
		require.Equal(t, int64(5000), start.In.Limits.SegmentBudgetMillicredits)
		require.Equal(t, int64(90000), start.In.BudgetMillicredits)
	}
	leader := h.startsOf("leader")[1].In
	require.Equal(t, int64(90000), leader.RunUsedMillicredits)
	require.Equal(t, int64(10000), leader.BudgetMillicredits-leader.RunUsedMillicredits, "the leader keeps the full reserve")
}

func twoTurnsOfAlice(t *testing.T) *harness {
	h := newHarness(t)
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	cursor := w.MemberCursor{Seq: 7, CheckpointVersion: 3, EventN: 40, ActiveSeconds: 100, Turn: 1}
	h.turn("alice", 1, 5*sec, w.TurnResult{Status: "completed", Cursor: cursor})
	h.turn("reviewer", 1, 5*sec, w.TurnResult{Status: "completed"})
	h.turn("leader", 1, 100*sec, w.TurnResult{Status: "ended"})
	h.cancelAt(20 * sec)
	require.NoError(t, h.run())
	return h
}

func TestTurnLimitsAreRelativeToCursor(t *testing.T) {
	// T-R17 (F47): per-turn limits add to the member's accumulated cursor and differ for review turns.
	h := twoTurnsOfAlice(t)
	review := h.startsOf("reviewer")[0].In
	require.Equal(t, team.TurnMaxSegments, review.Limits.MaxSegments)
	require.Equal(t, team.ReviewTurnWallSeconds, review.Limits.MaxActiveSeconds)
	rework := h.startsOf("alice")[1].In
	require.Equal(t, 7+team.TurnMaxSegments, rework.Limits.MaxSegments)
	require.Equal(t, 100+team.TurnWallSeconds, rework.Limits.MaxActiveSeconds)
}

func TestMemberCursorCarriesForwardOnlyTurn(t *testing.T) {
	// T-R18b (I2): the next turn starts from the previous result's cursor with only Turn advanced.
	h := twoTurnsOfAlice(t)
	require.Equal(t, w.MemberCursor{Seq: 7, CheckpointVersion: 3, EventN: 40, ActiveSeconds: 100, Turn: 2}, h.startsOf("alice")[1].In.Cursor)
}

func TestAppliedIsClearedWhenTurnEnds(t *testing.T) {
	// T-R18c (I4): a request id from an ended turn is not remembered; resending it is a stale turn.
	h := newHarness(t)
	var stale *outcome
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	h.at(15*sec, func() { stale = h.create("leader", "c1", newTask("t1", "alice")) })
	h.turn("leader", 1, 10*sec, w.TurnResult{Status: "completed"})
	h.turn("alice", 1, 60*sec, w.TurnResult{Status: "ended"})
	h.cancelAt(20 * sec)
	require.NoError(t, h.run())
	requireCode(t, stale.Err, "TEAM_NOT_IN_TURN")
}

func TestPauseResumeCancelCascade(t *testing.T) {
	// T-R18: Pause/Resume update runs.status once each and hold back new turns; Cancel reaches in-flight turns
	// and the run ends cancelled after their results.
	h := newHarness(t, func(in *w.TeamRunInput) { in.Team.Roster = in.Team.Roster[:3] })
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	h.at(6*sec, func() { h.create("leader", "c2", newTask("t2", "alice")) })
	h.at(10*sec, func() { h.env.SignalWorkflow(w.Pause, w.ActorInput{}) })
	h.at(20*sec, func() { h.env.SignalWorkflow(w.Resume, w.ActorInput{}) })
	h.cancelAt(30 * sec)
	h.turn("leader", 1, 60*sec, w.TurnResult{Status: "cancelled"})
	h.turn("alice", 1, 10*sec, w.TurnResult{Status: "completed"})
	h.turn("alice", 2, 50*sec, w.TurnResult{Status: "cancelled"})
	require.NoError(t, h.run())
	require.Equal(t, []string{"paused", "running"}, []string{h.statuses[0].Status, h.statuses[1].Status})
	require.Len(t, h.statuses, 2)
	for _, status := range h.statuses {
		require.Equal(t, w.MemberTeam, status.Member)
	}
	require.Equal(t, 20*sec, h.startsOf("alice")[1].At, "no turn starts while paused")
	require.Len(t, h.signalsNamed(w.Pause), 2)
	require.Len(t, h.signalsNamed(w.Resume), 1)
	cancelled := map[string]bool{}
	for _, signal := range h.signalsNamed(w.Cancel) {
		cancelled[signal.Workflow] = true
	}
	require.Equal(t, map[string]bool{w.MemberTaskWorkflowID("tenant", "run", "leader", 1): true,
		w.MemberTaskWorkflowID("tenant", "run", "alice", 2): true}, cancelled)
	require.Equal(t, "cancelled", h.final.Status)
}

func TestParentCancellationKeepsWaitingForResults(t *testing.T) {
	// T-R18 (F67): cancelling the workflow context must not cancel WaitForTurn; the in-flight results are
	// still processed and recorded as the members reported them.
	h := newHarness(t)
	h.at(30*sec, func() { h.env.CancelWorkflow() })
	h.turn("leader", 1, 60*sec, w.TurnResult{Status: "cancelled"})
	require.NoError(t, h.run())
	require.Equal(t, "cancelled", h.final.Status)
	for _, turn := range h.allTurns() {
		require.NotEqual(t, "TURN_LOST", turn.Code)
	}
	var recorded bool
	for _, turn := range h.allTurns() {
		recorded = recorded || (turn.Member == "leader" && turn.Status == "cancelled")
	}
	require.True(t, recorded, "the leader's real result is recorded")
	require.NotEmpty(t, h.signalsNamed(w.Cancel))
}

func TestWaitForTurnErrorBecomesTurnLost(t *testing.T) {
	// T-R18 (F67), T-R20 (F36): an error from WaitForTurn never bubbles up; the turn is lost and the run fails.
	h := newHarness(t)
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	h.turnError("alice", 1)
	h.turn("leader", 1, 20*sec, w.TurnResult{Status: "ended"})
	require.NoError(t, h.run())
	require.Equal(t, "failed", h.final.Status)
	require.Equal(t, "TURN_LOST", h.final.Code)
}

func TestFailureClassesDriveRetryAndRunFailure(t *testing.T) {
	// T-R19 (F46, §1.4.5): invariant codes fail the run; retryable codes requeue until MaxTaskFailures;
	// task failures fail the task at once and escalate.
	t.Run("invariant", func(t *testing.T) {
		h := newHarness(t)
		h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
		h.turn("alice", 1, 5*sec, w.TurnResult{Status: "failed", Code: "CHECKPOINT_CONFLICT"})
		h.turn("leader", 1, 20*sec, w.TurnResult{Status: "ended"})
		require.NoError(t, h.run())
		require.Equal(t, "failed", h.final.Status)
		require.Equal(t, "CHECKPOINT_CONFLICT", h.final.Code)
	})
	t.Run("retryable", func(t *testing.T) {
		h := newHarness(t)
		h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
		for turn := 1; turn <= team.MaxTaskFailures; turn++ {
			h.turn("alice", turn, 5*sec, w.TurnResult{Status: "failed", Code: "SEGMENT_FAILED"})
		}
		h.turn("leader", 1, 8*sec, w.TurnResult{Status: "completed"})
		h.turn("leader", 2, 60*sec, w.TurnResult{Status: "ended"})
		h.cancelAt(40 * sec)
		require.NoError(t, h.run())
		require.Len(t, h.startsOf("alice"), team.MaxTaskFailures)
		task := h.task("t1")
		require.Equal(t, contract.TaskStatus("failed"), task.Status)
		require.Equal(t, team.MaxTaskFailures, *task.Failures)
		require.Contains(t, *h.startsOf("leader")[1].In.Input.Text, "[Tasks Escalated]")
	})
	for _, code := range []string{"POLICY_DENIED", "UNATTENDED_INTERACTION"} {
		t.Run(code, func(t *testing.T) {
			h := newHarness(t)
			h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
			h.turn("alice", 1, 5*sec, w.TurnResult{Status: "failed", Code: code})
			h.turn("leader", 1, 8*sec, w.TurnResult{Status: "completed"})
			h.turn("leader", 2, 60*sec, w.TurnResult{Status: "ended"})
			h.cancelAt(40 * sec)
			require.NoError(t, h.run())
			require.Len(t, h.startsOf("alice"), 1)
			require.True(t, h.sawStatus("t1", "failed"))
			require.Contains(t, *h.startsOf("leader")[1].In.Input.Text, "[Tasks Escalated]")
		})
	}
}

func TestTurnStartConflictFailsTheRun(t *testing.T) {
	// T-R25 (§1.4.3): a child that cannot be started because its ID exists fails the run with TURN_START_CONFLICT.
	h := newHarness(t)
	// A result whose cursor was reset makes the all_settled wake reuse the first turn's workflow ID.
	h.turn("leader", 1, 10*sec, w.TurnResult{Status: "ended", Cursor: w.MemberCursor{Seq: 1}})
	require.NoError(t, h.run())
	require.Equal(t, "failed", h.final.Status)
	require.Equal(t, "TURN_START_CONFLICT", h.final.Code)
}

func TestUpdateHandlersScheduleNoActivities(t *testing.T) {
	// T-R26 (F49): an Update completes without any Activity starting between its acceptance and completion.
	h := newHarness(t)
	var out *outcome
	h.at(5*sec, func() { out = h.create("leader", "c1", newTask("t1", "alice")) })
	h.turn("leader", 1, 20*sec, w.TurnResult{Status: "ended"})
	h.cancelAt(15 * sec)
	require.NoError(t, h.run())
	require.True(t, out.Done)
	require.Equal(t, out.CallsAtAccept, out.CallsAtDone)
}

func TestRunDeadlineTimesOutAndCancelsTurns(t *testing.T) {
	// T-R17b (F72): the run-level wall clock includes time a turn spends waiting; at the deadline the
	// run times out and in-flight turns are cancelled.
	h := newHarness(t, func(in *w.TeamRunInput) { in.Limits.MaxActiveSeconds = 600 })
	h.turn("leader", 1, 650*sec, w.TurnResult{Status: "cancelled"})
	require.NoError(t, h.run())
	require.Equal(t, "timed_out", h.final.Status)
	signals := h.signalsNamed(w.Cancel)
	require.NotEmpty(t, signals)
	require.Equal(t, 600*sec, signals[0].At)
}

func TestDeliveredTaskIgnoresLateTurnFailures(t *testing.T) {
	for _, result := range []w.TurnResult{{Status: "budget_exhausted"}, {Status: "failed", Code: "SEGMENT_LIMIT"}, {Status: "timed_out"}} {
		t.Run(result.Status, func(t *testing.T) {
			h := newHarness(t)
			h.at(5*sec, func() { h.create("leader", "create", newTask("t1", "alice")) })
			h.at(10*sec, func() { h.completeTask("alice", "complete", "t1") })
			h.turn("leader", 1, 100*sec, w.TurnResult{Status: "ended"})
			h.turn("alice", 1, 20*sec, result)
			h.cancelAt(40 * sec)
			require.NoError(t, h.run())
			require.Len(t, h.startsOf("alice"), 1, "an already delivered task must not be requeued")
			require.NotEmpty(t, h.startsOf("reviewer"), "the delivery still proceeds to review")
		})
	}
}

func TestReviewCancellationAndBudgetDoNotVote(t *testing.T) {
	for _, status := range []string{"cancelled", "budget_exhausted"} {
		t.Run(status, func(t *testing.T) {
			h := newHarness(t)
			h.at(5*sec, func() { h.create("leader", "create", newTask("t1", "alice")) })
			h.turn("leader", 1, 100*sec, w.TurnResult{Status: "ended"})
			h.turn("alice", 1, 10*sec, w.TurnResult{Status: "completed"})
			h.turn("reviewer", 1, 5*sec, w.TurnResult{Status: status})
			h.turn("reviewer", 2, 100*sec, w.TurnResult{Status: "cancelled"})
			h.cancelAt(30 * sec)
			require.NoError(t, h.run())
			for _, board := range h.boards {
				for _, task := range board.Board.Tasks {
					require.Empty(t, task.Review.Votes)
					require.Zero(t, *task.Review.Round)
				}
			}
		})
	}
}

func TestRejectWaitsForOtherReviewTurnBeforeRework(t *testing.T) {
	h := newHarness(t)
	h.in.Team.Roster = append(h.in.Team.Roster, contract.RosterEntry{Name: "reviewer2", Role: "reviewer", Status: "idle"})
	h.at(5*sec, func() { h.create("leader", "create", newTask("t1", "alice")) })
	h.turn("leader", 1, 100*sec, w.TurnResult{Status: "ended"})
	h.turn("alice", 1, 10*sec, w.TurnResult{Status: "completed"})
	h.at(20*sec, func() { h.verdict("reviewer", "reject", "t1", "reject") })
	h.turn("reviewer", 1, 10*sec, w.TurnResult{Status: "ended"})
	h.turn("reviewer2", 1, 30*sec, w.TurnResult{Status: "ended"})
	h.cancelAt(55 * sec)
	require.NoError(t, h.run())
	starts := h.startsOf("alice")
	require.Len(t, starts, 2)
	require.GreaterOrEqual(t, starts[1].At, 45*sec)
}

func TestLeaderPlatformFailureKeepsItsCode(t *testing.T) {
	for _, code := range []string{"TURN_LOST", "CHECKPOINT_CONFLICT"} {
		t.Run(code, func(t *testing.T) {
			h := newHarness(t)
			h.turn("leader", 1, sec, w.TurnResult{Status: "failed", Code: code})
			require.NoError(t, h.run())
			require.Equal(t, "failed", h.final.Status)
			require.Equal(t, code, h.final.Code)
		})
	}
}

func TestProjectionFailuresFinalizeTheRun(t *testing.T) {
	for activity, code := range map[string]string{"PrepareTeamRun": "PREPARE_FAILED", "SetRunStatus": "STATUS_FAILED",
		"RecordBoard": "RECORD_BOARD_FAILED"} {
		t.Run(activity, func(t *testing.T) {
			h := newHarness(t)
			h.failActivity = activity
			h.at(5*sec, func() {
				if activity == "SetRunStatus" {
					h.env.SignalWorkflow(w.Pause, w.ActorInput{})
				} else {
					h.create("leader", "create", newTask("t1", "alice"))
				}
			})
			require.NoError(t, h.run())
			require.NotNil(t, h.final)
			require.Equal(t, "failed", h.final.Status)
			require.Equal(t, code, h.final.Code)
			count := 0
			for _, name := range h.calls {
				if name == activity {
					count++
				}
			}
			require.GreaterOrEqual(t, count, 5, "persistent failures must exhaust the real Activity retry policy")
		})
	}
}

func TestFinalizationFailureReturnsTheActivityError(t *testing.T) {
	h := newHarness(t)
	h.failActivity = "FinalizeTeamRun"
	h.cancelAt(sec)
	require.Error(t, h.run())
	require.NotNil(t, h.final)
}

func TestPauseResumeArrivalOrder(t *testing.T) {
	for _, names := range [][]string{{w.Resume, w.Pause}, {w.Pause, w.Resume}} {
		t.Run(names[0], func(t *testing.T) {
			h := newHarness(t)
			h.at(sec, func() {
				h.env.SignalWorkflowSkippingWorkflowTask(names[0], w.ActorInput{})
				h.env.SignalWorkflow(names[1], w.ActorInput{})
			})
			h.cancelAt(5 * sec)
			require.NoError(t, h.run())
			require.NotEmpty(t, h.statuses)
			want := "running"
			if names[1] == w.Pause {
				want = "paused"
			}
			require.Equal(t, want, h.statuses[len(h.statuses)-1].Status)
		})
	}
}

func TestPauseSignalsBatchIdleAndMidScan(t *testing.T) {
	t.Run("idle-batch", func(t *testing.T) {
		h := newHarness(t)
		h.at(sec, func() {
			h.env.SignalWorkflowSkippingWorkflowTask(w.Resume, w.ActorInput{})
			h.env.SignalWorkflow(w.Pause, w.ActorInput{})
		})
		h.cancelAt(5 * sec)
		require.NoError(t, h.run())
		require.NotEmpty(t, h.statuses)
		require.Equal(t, "paused", h.statuses[len(h.statuses)-1].Status)
	})
	t.Run("mid-scan", func(t *testing.T) {
		h := newHarness(t)
		h.at(sec, func() {
			h.create("leader", "create", newTask("t1", "alice"))
			h.create("leader", "create2", newTask("t2", "bob"))
		})
		h.onMemberStart = func(in w.MemberTaskInput) {
			if in.Member == "alice" {
				h.env.SignalWorkflowSkippingWorkflowTask(w.Resume, w.ActorInput{})
				h.env.SignalWorkflow(w.Pause, w.ActorInput{})
			}
		}
		h.turn("leader", 1, 20*sec, w.TurnResult{Status: "ended"})
		h.turn("alice", 1, 20*sec, w.TurnResult{Status: "ended"})
		h.turn("bob", 1, 20*sec, w.TurnResult{Status: "ended"})
		h.cancelAt(10 * sec)
		require.NoError(t, h.run())
		require.NotEmpty(t, h.statuses)
		require.Equal(t, "paused", h.statuses[len(h.statuses)-1].Status)
		found := map[string]bool{}
		for _, sig := range h.signalsNamed(w.Pause) {
			found[sig.Workflow] = true
		}
		require.True(t, found[w.MemberTaskWorkflowID("tenant", "run", "alice", 1)])
		require.True(t, found[w.MemberTaskWorkflowID("tenant", "run", "bob", 1)])
	})
}

func TestPauseMidScanDoesNotWakeLeaderUntilResume(t *testing.T) {
	h := newHarness(t)
	h.at(sec, func() {
		h.create("leader", "create", newTask("t1", "alice"), newTask("t2", "bob"))
	})
	h.onMemberStart = func(in w.MemberTaskInput) {
		if in.Member == "alice" && in.Turn == 1 {
			h.update(w.Steer, "leader-steer", w.SteerInput{ContentRef: "t/tenant/runs/run/steer/leader"})
			h.env.SignalWorkflowSkippingWorkflowTask(w.Resume, w.ActorInput{})
			h.env.SignalWorkflow(w.Pause, w.ActorInput{})
		}
	}
	h.at(15*sec, func() { h.env.SignalWorkflow(w.Resume, w.ActorInput{}) })
	h.turn("leader", 1, 20*sec, w.TurnResult{Status: "ended"})
	h.turn("alice", 1, 20*sec, w.TurnResult{Status: "ended"})
	h.turn("bob", 1, 20*sec, w.TurnResult{Status: "ended"})
	h.turn("leader", 2, 10*sec, w.TurnResult{Status: "ended"})
	h.cancelAt(35 * sec)
	require.NoError(t, h.run())
	leaderStarts := h.startsOf("leader")
	require.GreaterOrEqual(t, len(leaderStarts), 2, "leader wakes after Resume for queued steering")
	require.NotEmpty(t, leaderStarts[1].In.Steer)
	pauseSignals := map[string]bool{}
	for _, sig := range h.signalsNamed(w.Pause) {
		pauseSignals[sig.Workflow] = true
	}
	require.True(t, pauseSignals[w.MemberTaskWorkflowID("tenant", "run", "leader", 1)])
	require.True(t, pauseSignals[w.MemberTaskWorkflowID("tenant", "run", "alice", 1)])
	require.True(t, pauseSignals[w.MemberTaskWorkflowID("tenant", "run", "bob", 1)])
	require.False(t, pauseSignals[w.MemberTaskWorkflowID("tenant", "run", "leader", 2)])
}

func TestPublicSteerUsesAgentErrorCodes(t *testing.T) {
	h := newHarness(t)
	var invalid, overflow *outcome
	h.at(sec, func() {
		invalid = h.update(w.Steer, "invalid", w.SteerInput{ContentRef: "t/other/runs/run/steer/a"})
		for i := range team.SteerCap {
			h.update(w.Steer, fmt.Sprint(i), w.SteerInput{ContentRef: fmt.Sprintf("t/tenant/runs/run/steer/%d", i)})
		}
		overflow = h.update(w.Steer, "overflow", w.SteerInput{ContentRef: "t/tenant/runs/run/steer/overflow"})
	})
	h.cancelAt(5 * sec)
	require.NoError(t, h.run())
	requireCode(t, invalid.Err, "RUN_NOT_ACTIVE")
	requireCode(t, overflow.Err, "QUEUE_FULL")
}

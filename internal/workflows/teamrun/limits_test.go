//go:build workflow_test_limits

package teamrun

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/team"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

// These scenarios depend on the workflow_test_limits thresholds (Continue-As-New after 3 steps,
// 5 turns in total).

func TestCreateTasksIsIdempotentAcrossContinueAsNew(t *testing.T) {
	// T-R6 (F17): the same request id yields the same task ids before and after Continue-As-New.
	h := newHarness(t)
	var first, second, third *outcome
	h.at(5*sec, func() {
		first = h.create("leader", "c1", newTask("t1", "alice"))
		second = h.create("leader", "c1", newTask("t1", "alice"))
	})
	h.at(10*sec, func() { third = h.create("leader", "c1", newTask("t1", "alice")) })
	h.turn("leader", 1, 100*sec, w.TurnResult{Status: "ended"})
	h.turn("alice", 1, 100*sec, w.TurnResult{Status: "ended"})
	h.cancelAt(40 * sec)
	require.NoError(t, h.run())
	require.Greater(t, h.runs, 1, "the run continued as new")
	for _, out := range []*outcome{first, second, third} {
		require.NoError(t, out.Err)
		require.True(t, out.Done)
		require.Equal(t, contract.CreateTasksOutput{TaskIds: []string{"t1"}}, out.Output)
	}
	require.Len(t, h.lastBoard().Tasks, 1)
}

func TestCompleteTaskResendAfterContinueAsNew(t *testing.T) {
	// T-R7 (F37): a CompleteTask that was applied before Continue-As-New is answered from Applied even though
	// the task is already in review; a different request id is still rejected.
	h := newHarness(t)
	var resent, other *outcome
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	h.at(10*sec, func() { h.completeTask("alice", "a1", "t1") })
	h.at(11*sec, func() { h.send("leader", "m1", "alice", "one") })
	h.at(12*sec, func() { h.send("leader", "m2", "alice", "two") })
	h.at(20*sec, func() {
		resent = h.completeTask("alice", "a1", "t1")
		other = h.completeTask("alice", "a2", "t1")
	})
	h.turn("leader", 1, 300*sec, w.TurnResult{Status: "ended"})
	h.turn("alice", 1, 60*sec, w.TurnResult{Status: "ended"})
	h.cancelAt(50 * sec)
	require.NoError(t, h.run())
	require.Greater(t, h.runs, 2)
	require.True(t, resent.Done)
	require.NoError(t, resent.Err)
	requireCode(t, other.Err, "TEAM_BAD_TRANSITION")
}

func TestContinueAsNewFlushesAndRestartsWaits(t *testing.T) {
	// T-R21 (F36, F49): the board is recorded before Continue-As-New, each active turn gets a new WaitForTurn
	// in the next execution, and a result arriving afterwards is processed.
	h := newHarness(t)
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	h.at(15*sec, func() { h.completeTask("alice", "a1", "t1") })
	h.turn("leader", 1, 300*sec, w.TurnResult{Status: "ended"})
	h.turn("alice", 1, 40*sec, w.TurnResult{Status: "ended"})
	h.cancelAt(100 * sec)
	require.NoError(t, h.run())
	require.Greater(t, h.runs, 1)
	carry := h.carries[0].Carry
	require.Empty(t, carry.Outbox, "the Outbox is flushed before Continue-As-New")
	require.False(t, carry.Dirty)
	require.Contains(t, carry.Active, "alice")
	var recorded bool
	for _, board := range h.boards {
		recorded = recorded || len(board.Board.Tasks) == 1
	}
	require.True(t, recorded)
	waits := map[string]int{}
	for _, wait := range h.waits {
		waits[wait.Member]++
	}
	require.GreaterOrEqual(t, waits["alice"], 2)
	require.GreaterOrEqual(t, waits["leader"], 2)
	count := func(name string) (n int) {
		for _, call := range h.calls {
			if call == name {
				n++
			}
		}
		return n
	}
	require.Equal(t, 1, count("PrepareTeamRun"), "only the first execution prepares the run")
	var ended bool
	for _, turn := range h.allTurns() {
		ended = ended || (turn.Member == "alice" && turn.Status == "ended")
	}
	require.True(t, ended, "the result that arrived after Continue-As-New is processed")
}

func TestCancelSurvivesContinueAsNew(t *testing.T) {
	// T-R22 (F51): a Cancel delivered in the workflow task that reaches the Continue-As-New threshold is
	// not dropped; the run ends cancelled and in-flight turns are cancelled.
	h := newHarness(t)
	h.at(5*sec, func() {
		h.create("leader", "c1", newTask("t1", "alice"))
	})
	h.onMemberStart = func(in w.MemberTaskInput) {
		if in.Member == "alice" {
			h.env.SignalWorkflow(w.Cancel, w.CancelInput{Reason: "test"})
		}
	}
	h.turn("leader", 1, 30*sec, w.TurnResult{Status: "cancelled"})
	h.turn("alice", 1, 30*sec, w.TurnResult{Status: "cancelled"})
	require.NoError(t, h.run())
	require.Equal(t, "cancelled", h.final.Status)
	cancelled := map[string]bool{}
	for _, signal := range h.signalsNamed(w.Cancel) {
		cancelled[signal.Workflow] = true
	}
	require.True(t, cancelled[w.MemberTaskWorkflowID("tenant", "run", "alice", 1)])
}

func TestContinueAsNewWaitsForDeliveryFutures(t *testing.T) {
	// T-R22b (§1.4.2): SendMessage returns at once while its Deliver signal is slow; Continue-As-New waits
	// for the signal to complete first.
	h := newHarness(t)
	h.signalDelay = 30 * sec
	var sends []*outcome
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	for i, request := range []string{"m1", "m2", "m3"} {
		h.at(8*sec+sec*time.Duration(i)/10, func() { sends = append(sends, h.send("leader", request, "alice", "text")) })
	}
	h.turn("leader", 1, 400*sec, w.TurnResult{Status: "ended"})
	h.turn("alice", 1, 400*sec, w.TurnResult{Status: "ended"})
	h.cancelAt(200 * sec)
	require.NoError(t, h.run())
	require.Len(t, sends, 3)
	for _, send := range sends {
		require.True(t, send.Done)
		require.Less(t, send.DoneAt, 9*sec, "the Update does not wait for the signal")
	}
	require.GreaterOrEqual(t, h.epochs[1], 38*sec, "Continue-As-New waited for the delayed signals")
}

func TestRunClockSurvivesContinueAsNew(t *testing.T) {
	// T-R17b (F72): the run-level deadline is measured from StartedAt, which Continue-As-New carries.
	h := newHarness(t, func(in *w.TeamRunInput) { in.Limits.MaxActiveSeconds = 600 })
	h.at(5*sec, func() { h.create("leader", "c1", newTask("t1", "alice")) })
	h.turn("leader", 1, 650*sec, w.TurnResult{Status: "cancelled"})
	h.turn("alice", 1, 650*sec, w.TurnResult{Status: "cancelled"})
	require.NoError(t, h.run())
	require.Greater(t, h.runs, 1)
	require.Equal(t, "timed_out", h.final.Status)
	signals := h.signalsNamed(w.Cancel)
	require.NotEmpty(t, signals)
	require.Equal(t, 600*sec, signals[0].At)
}

func TestTurnLimitFailsTheRun(t *testing.T) {
	// T-R23 (§1.4.4): when the total turn budget is used up and nothing is active, the run fails.
	h := newHarness(t, func(in *w.TeamRunInput) { in.Team.Roster = in.Team.Roster[:3] })
	h.at(5*sec, func() {
		tasks := make([]contract.NewTask, team.MaxTurnsTotal)
		for i := range tasks {
			tasks[i] = newTask("task", "alice")
		}
		h.create("leader", "c1", tasks...)
	})
	h.turn("leader", 1, 10*sec, w.TurnResult{Status: "completed"})
	require.NoError(t, h.run())
	require.Equal(t, "failed", h.final.Status)
	require.Equal(t, "TEAM_TURN_LIMIT", h.final.Code)
	require.Len(t, h.starts, team.MaxTurnsTotal)
}

func TestPauseChangeIsWrittenBeforeContinueAsNew(t *testing.T) {
	h := newHarness(t)
	h.at(5*sec, func() { h.create("leader", "create", newTask("t1", "alice")) })
	h.onMemberStart = func(in w.MemberTaskInput) {
		if in.Member == "alice" {
			h.env.SignalWorkflow(w.Pause, w.ActorInput{})
		}
	}
	h.turn("leader", 1, 100*sec, w.TurnResult{Status: "ended"})
	h.turn("alice", 1, 100*sec, w.TurnResult{Status: "ended"})
	h.cancelAt(20 * sec)
	require.NoError(t, h.run())
	require.NotEmpty(t, h.carries)
	require.NotEmpty(t, h.statuses, "the pending pause change must not be lost on Continue-As-New")
	require.Equal(t, "paused", h.statuses[0].Status)
}

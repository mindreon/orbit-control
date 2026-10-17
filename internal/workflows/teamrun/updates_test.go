package teamrun

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/team"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func updateRun() *run {
	return &run{in: w.TeamRunInput{Team: w.TeamSpec{Leader: "leader", MaxParallel: 2, ReviewMaxRounds: 3}}, st: &w.TeamCarry{
		Status: "running", Board: contract.TeamBoard{Roster: []contract.RosterEntry{
			{Name: "leader", Role: "leader", Status: "idle", Description: "lead"},
			{Name: "alice", Role: "teammate", Status: "idle", Description: "work"},
			{Name: "reviewer", Role: "reviewer", Status: "idle", Description: "review"},
		}}, Active: map[string]w.ActiveTurn{"leader": {Member: "leader", Turn: 1, Kind: "lead"}},
		Applied: map[string]w.AppliedUpdate{}, Mail: map[string][]contract.MailboxMessage{}, Cursors: map[string]w.MemberCursor{},
	}}
}
func TestCreateIdempotencyAndValidation(t *testing.T) {
	r := updateRun()
	in := contract.CreateTasksInput{Caller: new(contract.MemberName("leader")), RequestId: "first", Tasks: []contract.NewTask{
		{Title: "one", Description: "first", Assignee: new(contract.MemberName("alice"))},
	}}
	require.NoError(t, r.validateCreate(in))
	out, err := r.create(nil, in)
	require.NoError(t, err)
	require.Equal(t, []string{"t1"}, out.TaskIds)
	require.NoError(t, r.validateCreate(in))
	replay, err := r.create(nil, in)
	require.NoError(t, err)
	require.Equal(t, out, replay)
	require.Len(t, r.st.Board.Tasks, 1)
	// The validator consults Applied before the caller/turn/status checks.
	r.st.Finished = true
	delete(r.st.Active, "leader")
	require.NoError(t, r.validateCreate(in))
	in.RequestId = "second"
	require.ErrorContains(t, r.validateCreate(in), "TEAM_NOT_IN_TURN")
}
func TestBoardCapacityRejectsWithoutMutation(t *testing.T) {
	for _, which := range []string{"create", "assign", "complete", "verdict"} {
		t.Run(which, func(t *testing.T) {
			r := updateRun()
			r.st.Board.Tasks = []contract.TeamTask{{TaskId: "t1", Title: "t", Description: strings.Repeat("<", team.BoardBytesMax/6),
				Assignee: new(contract.MemberName("alice")), Status: "pending", Review: &contract.TaskReview{Reviewers: []contract.MemberName{"reviewer"}}}}
			r.st.Active["alice"] = w.ActiveTurn{Member: "alice", Turn: 1, Kind: "task", TaskID: "t1"}
			r.st.Active["reviewer"] = w.ActiveTurn{Member: "reviewer", Turn: 1, Kind: "review", TaskID: "t1"}
			if which == "complete" {
				r.st.Board.Tasks[0].Status = "in_progress"
			}
			if which == "verdict" {
				r.st.Board.Tasks[0].Status = "in_review"
			}
			before, _ := team.Marshal(r.st)
			var err error
			switch which {
			case "create":
				err = r.validateCreate(contract.CreateTasksInput{Caller: new(contract.MemberName("leader")), RequestId: "r", Tasks: []contract.NewTask{{Title: "t", Description: "d"}}})
			case "assign":
				err = r.validateAssign(contract.AssignTaskInput{Caller: new(contract.MemberName("leader")), RequestId: "r", TaskId: "t1", Member: "alice"})
			case "complete":
				err = r.validateComplete(contract.CompleteTaskInput{Caller: new(contract.MemberName("alice")), RequestId: "r", TaskId: "t1", ResultText: "result"})
			case "verdict":
				err = r.validateVerdict(contract.SubmitVerdictInput{Caller: new(contract.MemberName("reviewer")), RequestId: "r", TaskId: "t1", Verdict: "approve", Comment: "yes"})
			}
			require.ErrorContains(t, err, "TEAM_TEXT_TOO_LONG")
			after, _ := team.Marshal(r.st)
			require.Equal(t, string(before), string(after))
		})
	}
}
func TestMailboxAndToolLimits(t *testing.T) {
	r := updateRun()
	r.st.Mail["reviewer"] = []contract.MailboxMessage{{MessageId: "m1", From: "leader", Text: new(strings.Repeat("<", team.MailBytesMax/6))}}
	in := contract.SendMessageInput{Caller: new(contract.MemberName("leader")), RequestId: "mail", To: "alice", Text: "hello"}
	require.ErrorContains(t, r.validateSend(in), "TEAM_MAILBOX_FULL")
	for _, tools := range [][]string{make([]string, 33), {strings.Repeat("x", 129)}} {
		create := contract.CreateTasksInput{Caller: new(contract.MemberName("leader")), RequestId: "tasks",
			Tasks: []contract.NewTask{{Title: "t", Description: "d", NarrowedTools: tools}}}
		require.ErrorContains(t, r.validateCreate(create), "TEAM_TEXT_TOO_LONG")
	}
}

package teamrun

import (
	"fmt"
	"slices"
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
				Assignee: new(contract.MemberName("alice")), Status: "pending",
				Review: &contract.TaskReview{Reviewers: []contract.MemberName{"reviewer"}}}}
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
				err = r.validateCreate(contract.CreateTasksInput{Caller: new(contract.MemberName("leader")), RequestId: "r",
					Tasks: []contract.NewTask{{Title: "t", Description: "d"}}})
			case "assign":
				err = r.validateAssign(contract.AssignTaskInput{Caller: new(contract.MemberName("leader")), RequestId: "r", TaskId: "t1",
					Member: "alice"})
			case "complete":
				err = r.validateComplete(contract.CompleteTaskInput{Caller: new(contract.MemberName("alice")), RequestId: "r", TaskId: "t1",
					ResultText: "result"})
			case "verdict":
				err = r.validateVerdict(contract.SubmitVerdictInput{Caller: new(contract.MemberName("reviewer")), RequestId: "r", TaskId: "t1",
					Verdict: "approve", Comment: "yes"})
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

func TestMaximalCarryStaysUnderOneMiB(t *testing.T) {
	// T-R27 (I5, F44, F77): fill every bounded field of the carry to its limit with the worst escaping characters
	// and the Temporal payload (encoding/json) must stay below 1 MiB.
	roster := []contract.RosterEntry{{Name: "leader", Role: "leader", Status: "idle", Description: "lead"},
		{Name: "alice", Role: "teammate", Status: "idle", Description: "work"}}
	for i := range team.MaxRoster - 2 {
		roster = append(roster, contract.RosterEntry{Name: contract.MemberName(fmt.Sprintf("rv%02d", i)), Role: "reviewer",
			Status: "idle", Description: strings.Repeat("<", 200)})
	}
	r := updateRun()
	r.in.Team.Roster = roster
	r.st.Board.Roster = roster
	r.st.Active = map[string]w.ActiveTurn{}
	for i, entry := range roster {
		r.st.Active[string(entry.Name)] = w.ActiveTurn{Member: string(entry.Name), Turn: i + 1, Kind: "lead"}
		r.st.Cursors[string(entry.Name)] = w.MemberCursor{Seq: 1 << 20, CheckpointVersion: 1 << 20, EventN: 1 << 40, Turn: 500,
			ActiveSeconds: 1 << 30}
	}
	leader := new(contract.MemberName("leader"))
	for i := range team.MaxTasks {
		in := contract.CreateTasksInput{Caller: leader, RequestId: fmt.Sprintf("create-%d", i), Tasks: []contract.NewTask{{
			Title: strings.Repeat("<", team.TitleBytes), Description: strings.Repeat("<", team.DescriptionBytes),
			Assignee: new(contract.MemberName("alice")), NarrowedTools: slices.Repeat([]string{strings.Repeat("<", team.ToolNameBytes)},
				team.NarrowedToolsMax)}}}
		if err := r.validateCreate(in); err != nil {
			requireCode(t, err, "TEAM_TEXT_TOO_LONG")
			break
		}
		_, err := r.create(nil, in)
		require.NoError(t, err)
	}
	require.LessOrEqual(t, team.EncodedLen(r.st.Board), team.BoardBytesMax, "validators cap the board")
	for i := range r.st.Board.Tasks {
		task := &r.st.Board.Tasks[i]
		task.Status, task.Failures, task.Attempts = "in_review", new(team.MaxTaskFailures), 3
		task.ResultText = new(team.TruncateEncoded(strings.Repeat("<", 10*team.ResultBytes), team.ResultBytes))
		for _, reviewer := range task.Review.Reviewers {
			task.Review.Votes = append(task.Review.Votes,
				contract.ReviewVote{Reviewer: reviewer, Verdict: "reject", Comment: "reviewer did not submit a verdict"})
		}
		task.Review.ReworkNote = new(team.TruncateEncoded(strings.Repeat("<", 10*team.CommentBytes), team.CommentBytes))
	}
	for i := 0; ; i++ {
		to := roster[1+i%(len(roster)-1)].Name
		in := contract.SendMessageInput{Caller: leader, RequestId: fmt.Sprintf("send-%d", i), To: to,
			Text: strings.Repeat("<", team.MessageBytes)}
		if err := r.validateSend(in); err != nil {
			requireCode(t, err, "TEAM_MAILBOX_FULL")
			break
		}
		r.st.NextMail++
		r.st.Mail[string(to)] = append(r.st.Mail[string(to)], r.mail(in))
	}
	require.LessOrEqual(t, team.EncodedLen(r.st.Mail), team.MailBytesMax)
	summary := team.TruncateEncoded(strings.Repeat("<", 10*team.SummaryBytes), team.SummaryBytes)
	r.st.Summary = summary
	r.st.LastLeaderFinal = &contract.FinalMessage{MessageId: strings.Repeat("m", 64), TextPreview: summary}
	for i := range team.SteerCap {
		r.st.UserSteer = append(r.st.UserSteer, contract.SteerItem{ContentRef: new("t/" + strings.Repeat("x", 120) + fmt.Sprint(i))})
	}
	for _, entry := range roster {
		for i := range 20 {
			result, _ := team.Marshal(contract.CreateTasksOutput{TaskIds: slices.Repeat([]string{"t32"}, team.MaxTasks)})
			r.st.Applied[fmt.Sprintf("%s-%s-%d", strings.Repeat("r", 40), entry.Name, i)] = w.AppliedUpdate{Member: string(entry.Name), Turn: 500,
				Result: result}
		}
	}
	for i := range team.MaxTasks {
		r.st.Escalations = append(r.st.Escalations, fmt.Sprintf("t%d", i))
	}
	r.st.Outbox, r.st.Dirty = nil, false // Continue-As-New only happens after the Outbox is flushed
	require.Less(t, team.EncodedLen(r.st), 1<<20)
}

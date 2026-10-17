package team

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/contract"
)

func TestTransitions(t *testing.T) {
	want := map[contract.TaskStatus][]contract.TaskStatus{
		"pending": {"in_progress", "blocked", "cancelled"}, "blocked": {"pending", "cancelled"},
		"in_progress": {"in_review", "completed", "pending", "cancelled", "failed"},
		"in_review":   {"completed", "pending", "cancelled", "failed"}, "failed": {"pending"}, "completed": {}, "cancelled": {},
	}
	for from, targets := range want {
		for to := range want {
			allowed := false
			for _, target := range targets {
				allowed = allowed || target == to
			}
			require.Equal(t, allowed, CanTransition(from, to), "%s -> %s", from, to)
		}
	}
}
func TestSettle(t *testing.T) {
	for _, tc := range []struct {
		name  string
		votes []contract.ReviewVote
		round int
		want  contract.TaskStatus
		next  int
	}{
		{name: "incomplete", votes: []contract.ReviewVote{{Reviewer: "a", Verdict: "approve"}}, want: "in_review"},
		{name: "approve", votes: []contract.ReviewVote{{Reviewer: "a", Verdict: "approve"}, {Reviewer: "b", Verdict: "approve"}},
			want: "completed"},
		{name: "veto immediately", votes: []contract.ReviewVote{{Reviewer: "a", Verdict: "reject", Comment: "fix"}}, want: "pending", next: 1},
		{name: "exhausted", round: 2, votes: []contract.ReviewVote{{Reviewer: "a", Verdict: "reject", Comment: "fix"}}, want: "failed", next: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task := contract.TeamTask{Status: "in_review",
				Review: &contract.TaskReview{Reviewers: []contract.MemberName{"a", "b"}, Votes: tc.votes, Round: new(tc.round)}}
			status, review := Settle(task, 3)
			require.Equal(t, tc.want, status)
			require.Equal(t, tc.next, *review.Round)
			if tc.next > 0 {
				require.Equal(t, "fix", *review.ReworkNote)
			}
		})
	}
}
func TestStartAndReviewScan(t *testing.T) {
	board := contract.TeamBoard{Tasks: []contract.TeamTask{
		{TaskId: "t1", Status: "completed"},
		{TaskId: "t2", Status: "blocked", Deps: []string{"t1"}, Assignee: new(contract.MemberName("alice"))},
		{TaskId: "t3", Status: "pending", Assignee: new(contract.MemberName("bob"))},
	}}
	busy := []Active{{Member: "alice", Kind: "task", TaskID: "t1"}}
	board.Tasks = append(board.Tasks, contract.TeamTask{TaskId: "t4", Status: "pending", Assignee: new(contract.MemberName("alice"))})
	busy = append(busy, Active{Member: "alice", Kind: "task", TaskID: "t4"})
	updated, actions := StartScan(board, busy, 2, false)
	require.NotContains(t, actions, Action{Member: "alice", Kind: "task", TaskID: "t4"}, "a pending task is busy by task id")
	busy = busy[:1]
	board.Tasks = board.Tasks[:3]
	updated, actions = StartScan(board, busy, 2, false)
	require.Equal(t, contract.TaskStatus("blocked"), updated.Tasks[1].Status)
	require.Equal(t, []Action{{Member: "bob", Kind: "task", TaskID: "t3"}}, actions)
	updated, actions = StartScan(board, nil, 1, false)
	require.Equal(t, contract.TaskStatus("pending"), updated.Tasks[1].Status)
	require.Equal(t, []Action{{Member: "alice", Kind: "task", TaskID: "t2"}}, actions)
	require.Equal(t, contract.TaskStatus("blocked"), board.Tasks[1].Status, "pure scan must not mutate input")
	_, actions = StartScan(board, nil, 2, true)
	require.Empty(t, actions)
	board.Tasks = []contract.TeamTask{{TaskId: "t1", Status: "in_review", Review: &contract.TaskReview{
		Reviewers: []contract.MemberName{"reviewer-a", "reviewer-b"}, Votes: []contract.ReviewVote{{Reviewer: "reviewer-a", Verdict: "approve"}},
	}}}
	require.Empty(t, ReviewScan(board, busy, 2, false), "assignee must finish writing before review")
	require.Equal(t, []Action{{Member: "reviewer-b", Kind: "review", TaskID: "t1"}}, ReviewScan(board, nil, 1, false))
	require.Empty(t, ReviewScan(board, nil, 2, true))
	require.Empty(t, ReviewScan(board, []Active{{Member: "reviewer-b", Kind: "review", TaskID: "other"}}, 1, false))
}
func TestNoticeGolden(t *testing.T) {
	task := contract.TeamTask{TaskId: "t1", Title: "Write", Description: "Deliver it", ResultText: new("done"), Files: []string{"a.txt"}}
	require.Equal(t,
		"[Task Started] Task [t1] \"Write\" is assigned to you — begin now.\n\n## Goal and "+
			"acceptance criteria\n\nDeliver it\n\nCall team_complete_task when done. "+
			"Use team_send_message to reach the leader for clarification.\n", TaskNotice(task, contract.TeamBoard{}))
	require.Equal(t,
		"[Review Assigned] Verify task [t1] \"Write\" against its acceptance "+
			"criteria.\n\nDeliver it\n\n## Deliverable\ndone\n- /workspace/a.txt\n\nInspect the files in /workspace. "+
			"Call team_submit_verdict with approve or reject and concrete feedback.\n", ReviewNotice(task))
	require.Equal(t, "[Tasks Escalated] Automatic rework has stopped. Reassign tasks or call team_finish.\n- [t1] Write; blocks t2\n",
		EscalationNotice([]string{"t1"}, contract.TeamBoard{Tasks: []contract.TeamTask{task, {TaskId: "t2", Deps: []string{"t1"}}}}))
	task.Review = &contract.TaskReview{ReworkNote: new("fix")}
	require.Contains(t, TaskNotice(task, contract.TeamBoard{}), "## Reviewer feedback\n\nfix\n")
	require.LessOrEqual(t, EncodedLen(TruncateEncoded(strings.Repeat("<", 5000), CommentBytes)), CommentBytes)
}

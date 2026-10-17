package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func testTeamProjection(t *testing.T, d *db.DB, admin *pgx.Conn) {
	f := newRunFixture(t, d, admin)
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE sessions SET entry_kind='team' WHERE tenant_id=$1 AND id=$2`, f.tenant, f.session)
		return err
	})
	op := f.op()
	op.Member = "team"
	roster := []contract.RosterEntry{{Name: "leader", Role: "leader", Status: "idle", Description: "lead"},
		{Name: "alice", Role: "teammate", Status: "idle", Description: "work"}}
	board := contract.TeamBoard{Roster: roster,
		Tasks: []contract.TeamTask{{TaskId: "t1", Title: "Task", Description: "Do it", Status: "pending", Failures: new(0),
			Review: &contract.TaskReview{}}}}
	require.NoError(t, f.activities.PrepareTeamRun(t.Context(), w.PrepareTeamInput{RunOp: op, Budget: 200000, Team: w.TeamSpec{Roster: roster,
		Leader: "leader", MaxParallel: 1, ReviewMaxRounds: 3}, Board: board}))
	op = f.op()
	op.Member = "team"
	input := w.RecordBoardInput{RunOp: op, Version: 2, Board: board,
		Turns: []w.TurnRow{{Member: "alice", Turn: 1, Kind: "task", TaskID: "t1", Status: "running", StartedAt: time.Now()}}}
	require.NoError(t, f.activities.RecordBoard(t.Context(), input))
	require.NoError(t, f.activities.RecordBoard(t.Context(), input))
	stale := input
	stale.Version = 1
	stale.Board.Tasks = nil
	require.NoError(t, f.activities.RecordBoard(t.Context(), stale))
	member := f.op()
	member.Member = "alice"
	require.NoError(t, f.activities.StartSegment(t.Context(), w.StartSegmentInput{RunOp: member}))
	require.NoError(t, f.activities.SetRunStatus(t.Context(), w.StatusInput{RunOp: member, Status: "paused"}))
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var status string
		var version, count int
		require.NoError(t, tx.QueryRow(ctx, `SELECT status FROM runs WHERE tenant_id=$1 AND id=$2`, f.tenant, f.run).Scan(&status))
		require.Equal(t, "running", status)
		require.NoError(t, tx.QueryRow(ctx, `SELECT board_version FROM team_runs WHERE tenant_id=$1 AND run_id=$2`, f.tenant,
			f.run).Scan(&version))
		require.Equal(t, 2, version)
		require.NoError(t, tx.QueryRow(ctx, `SELECT count(*) FROM team_tasks WHERE tenant_id=$1 AND run_id=$2`, f.tenant, f.run).Scan(&count))
		require.Equal(t, 1, count)
		return nil
	})
	op = f.op()
	op.Member = "team"
	require.NoError(t, f.activities.FinalizeTeamRun(t.Context(), w.FinalizeTeamInput{RunOp: op, Status: "succeeded", Summary: "summary",
		Board: board}))
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var count int
		require.NoError(t, tx.QueryRow(ctx, `SELECT count(*) FROM messages WHERE tenant_id=$1 AND run_id=$2 AND role='assistant' AND id=$3`,
			f.tenant, f.run, ids.Derive(f.run, "team-summary")).Scan(&count))
		require.Equal(t, 1, count)
		return nil
	})
}

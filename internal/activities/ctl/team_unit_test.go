package ctl

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type recordingTx struct {
	pgx.Tx
	args [][]any
}

func (tx *recordingTx) Exec(_ context.Context, _ string, args ...any) (pgconn.CommandTag, error) {
	tx.args = append(tx.args, args)
	return pgconn.CommandTag{}, nil
}

func TestEventIDsAreScopedByMember(t *testing.T) {
	// F4: the team workflow and a member share an EventN; their event ids differ by member so neither overwrites the other.
	const (
		tenant  = "11111111-1111-4111-8111-111111111111"
		session = "22222222-2222-4222-8222-222222222222"
		run     = "33333333-3333-4333-8333-333333333333"
	)
	at := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	ids := map[string]string{}
	for _, op := range []w.RunOp{
		{TenantID: tenant, SessionID: session, RunID: run, Member: w.MemberTeam, Seq: 0, EventN: 5, EventAt: at},
		{TenantID: tenant, SessionID: session, RunID: run, Member: "alice", Seq: 3, EventN: 5, EventAt: at},
	} {
		tx := &recordingTx{}
		require.NoError(t, persistEvents(t.Context(), tx, op, []event{{typ: "run.status", data: contract.RunStatus{Status: "running"}}}))
		require.Len(t, tx.args, 1)
		ids[op.Member] = tx.args[0][2].(string)
	}
	require.Equal(t, run+":team:0:0:5001", ids[w.MemberTeam])
	require.Equal(t, run+":alice:3:0:5001", ids["alice"])
}

func TestTeamRunStatusPrecedence(t *testing.T) {
	// F73: paused beats approval beats input; with none of them the run is running.
	for _, tc := range []struct {
		paused, approval, input bool
		want                    string
	}{
		{paused: false, approval: false, input: false, want: "running"},
		{paused: false, approval: false, input: true, want: "waiting_input"},
		{paused: false, approval: true, input: false, want: "waiting_approval"},
		{paused: false, approval: true, input: true, want: "waiting_approval"},
		{paused: true, approval: false, input: false, want: "paused"},
		{paused: true, approval: false, input: true, want: "paused"},
		{paused: true, approval: true, input: false, want: "paused"},
		{paused: true, approval: true, input: true, want: "paused"},
	} {
		require.Equal(t, tc.want, teamRunStatus(tc.paused, tc.approval, tc.input), "%+v", tc)
	}
}

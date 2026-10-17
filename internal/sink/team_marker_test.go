//go:build nats_integration

package sink

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/activities/ctl"
	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

// A team run has one event subject shared by TeamRun and member Activities; the end marker follows run.completed.
func TestTeamEventsShareSubjectAndMarkerFollowsCompletion(t *testing.T) {
	f := newFixture(t)
	_, err := f.admin.Exec(t.Context(), `UPDATE runs SET created_by=$2 WHERE id=$1`, f.run, f.workspace)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := f.admin.Exec(context.Background(), `DELETE FROM sandboxes WHERE tenant_id=$1`, f.tenant)
		require.NoError(t, err)
	})
	var subjects, idsUsed []string
	publisher := publishFunc(func(ctx context.Context, subject, id string, raw []byte) error {
		subjects = append(subjects, subject)
		idsUsed = append(idsUsed, id)
		return f.bus.Publish(ctx, subject, id, raw)
	})
	activities := &ctl.Activities{DB: f.db, NATS: publisher, Logger: f.logger}
	op := func(member string, n int64) w.RunOp {
		return w.RunOp{TenantID: f.tenant, SessionID: f.session, RunID: f.run, Member: member, EventN: n, EventAt: time.Now().UTC()}
	}
	member := op("alice", 1)
	require.NoError(t, activities.RecordSandboxState(t.Context(), w.SandboxStateInput{TenantID: f.tenant, SessionID: f.session,
		RunID: f.run, Member: "alice", IsolationKey: w.IsolationKey(f.tenant, f.session), Gen: 1, State: "ready", Profile: "default",
		Lease:  &contract.SandboxLease{Provider: "opensandbox", SandboxId: "box", ExpiresAt: time.Now().Add(time.Hour)},
		EventN: member.EventN, EventAt: member.EventAt, Restored: new(true)}))
	require.NoError(t, activities.FinalizeTeamRun(t.Context(), w.FinalizeTeamInput{RunOp: op("team", 2), Status: "succeeded"}))
	require.Equal(t, []string{f.subject, f.subject, f.subject + ".end"}, subjects)
	require.Equal(t, "end:"+f.run, idsUsed[2])
	first, second, marker := f.next(t), f.next(t), f.next(t)
	require.Equal(t, f.subject, first.Subject())
	require.Equal(t, f.subject, second.Subject())
	require.Equal(t, f.subject+".end", marker.Subject())
	require.Contains(t, idsUsed[0], ":alice:")
	require.Contains(t, idsUsed[1], ":team:")
}

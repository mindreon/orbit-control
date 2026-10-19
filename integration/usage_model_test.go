package integration

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/models"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/sessions"
	"github.com/mindreon/orbit-control/internal/usage"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func TestUsageMemberAssetsAndOrder(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	f := newRunFixture(t, d, admin)
	lock := contract.RunLock{}
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var raw []byte
		if err := tx.QueryRow(ctx, "SELECT lock FROM run_locks WHERE id=$1", f.lock).Scan(&raw); err != nil {
			return err
		}
		return json.Unmarshal(raw, &lock)
	})
	lock.Entry = contract.LockEntryRef{Kind: "team", Ref: "test/team", Version: "1.0.0"}
	lock.Members = []contract.LockMember{{Name: "alice", AgentEntry: 0, Role: "teammate", EffectivePermissions: lock.EffectivePermissions}}
	lock.Entries[0].Kind = "agent"
	lock.Entries[0].Ref = "test/alice"
	lock.Entries[0].Version = "2.0.0"
	raw, err := json.Marshal(lock)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), "UPDATE run_locks SET lock=$2 WHERE id=$1", f.lock, raw)
	require.NoError(t, err)
	profile := ids.New()
	binding := map[string]any{models.BindingKey(1, "fixture"): map[string]any{
		"id": profile, "pricing": contract.ModelPricing{InputPerMtokMillicredits: 1000000}}}
	bound, err := json.Marshal(binding)
	require.NoError(t, err)
	for _, member := range []string{"alice", "main", "team"} {
		_, err = admin.Exec(t.Context(), `INSERT INTO run_segments(tenant_id,run_id,member,seq,status,model_profiles)
 VALUES($1,$2,$3,1,'running',$4)`, f.tenant, f.run, member, bound)
		require.NoError(t, err)
		prefix := fmt.Sprintf("model:%s:%s:1:1:", f.run, member)
		now := time.Now().UTC()
		req := contract.UsageReport{Items: []contract.UsageItem{
			{Model: "fixture", InputTokens: 2, OccurredAt: now.Add(-time.Minute), IdempotencyKey: prefix + "3"},
			{Model: "fixture", InputTokens: 2, OccurredAt: now.Add(-time.Minute), IdempotencyKey: prefix + "2"},
			{Model: "fixture", InputTokens: 2, OccurredAt: now, IdempotencyKey: prefix + "1"},
		}}
		for repeat := range 2 {
			f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
				_, points, err := usage.ReportModelUsage(ctx,
					auth.CapabilityClaims{Tenant: f.tenant, Run: f.run, Member: member, Seq: 1, Attempt: 1}, req)
				require.NoError(t, err)
				if repeat == 0 {
					require.Len(t, points, 12)
				} else {
					require.Empty(t, points)
				}
				return err
			})
		}
		var keys []string
		rows, err := admin.Query(t.Context(), `SELECT idempotency_key,asset_ref,model_profile_id::text FROM usage_ledger
 WHERE tenant_id=$1 AND member=$2 AND meter='tokens.input' ORDER BY id`, f.tenant, member)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var key, asset, id string
			require.NoError(t, rows.Scan(&key, &asset, &id))
			keys = append(keys, key)
			want := "team:test/team@1.0.0"
			if member == "alice" {
				want = "agent:test/alice@2.0.0"
			}
			require.Equal(t, want, asset)
			require.Equal(t, profile, id)
		}
		require.NoError(t, rows.Err())
		rows.Close()
		require.Equal(t, []string{prefix + "2:in", prefix + "3:in", prefix + "1:in"}, keys)
	}
}

func TestSegmentEndReasonPersistence(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	for _, status := range []string{"completed", "yielded", "failed"} {
		f := newRunFixture(t, d, admin)
		f.start(t)
		result := contract.SegmentResult{V: 1, Status: contract.SegmentStatus(status), CheckpointVersion: 1,
			Progress: contract.SegmentProgress{Iterations: 1, NewOutput: true}}
		want := status
		if status == "yielded" {
			result.YieldReason = new(contract.YieldReasonWallCap)
			want = "wall_cap"
		}
		if status == "failed" {
			result.Error = &contract.SegmentError{Code: contract.SegmentErrorCodeModelError, Message: "fixture"}
			want = "model_error"
		}
		_, err := f.activities.RecordSegment(t.Context(), w.RecordSegmentInput{RunOp: f.op(), Result: result})
		require.NoError(t, err)
		f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
			page, err := sessions.Segments(ctx, f.run)
			if err != nil {
				return err
			}
			require.Len(t, page.Items, 1)
			item := page.Items[0]
			require.Equal(t, want, *item.EndReason)
			if status == "failed" {
				require.Equal(t, want, *item.ErrorCode)
			} else {
				require.Nil(t, item.ErrorCode)
			}
			require.NotNil(t, item.DurationMS)
			return nil
		})
	}
	f := newRunFixture(t, d, admin)
	f.start(t)
	require.NoError(t, f.activities.FinalizeRun(t.Context(), w.FinalizeInput{RunOp: f.op(), Status: "failed", ErrorCode: "fixture"}))
	var reason string
	require.NoError(t, admin.QueryRow(t.Context(),
		"SELECT end_reason FROM run_segments WHERE tenant_id=$1 AND run_id=$2", f.tenant, f.run).Scan(&reason))
	require.Equal(t, "run_terminated", reason)
}

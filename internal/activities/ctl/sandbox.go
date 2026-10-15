package ctl

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/jcs"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"slices"
	"time"
)

func (a *Activities) AcquireSandbox(ctx context.Context, in w.AcquireSandboxInput) (contract.SandboxLease, error) {
	var lease contract.SandboxLease
	var workspace string
	var lock contract.RunLock
	if err := a.DB.WithTenant(ctx, in.TenantID, func(tx pgx.Tx) error {
		var raw []byte
		err := tx.QueryRow(ctx, `SELECT s.workspace_id::text,l.lock FROM runs r JOIN sessions s ON s.id=r.session_id JOIN run_locks l ON l.id=r.lock_id WHERE r.tenant_id=$1 AND r.id=$2`, in.TenantID, in.RunID).Scan(&workspace, &raw)
		if err != nil {
			return err
		}
		return json.Unmarshal(raw, &lock)
	}); err != nil {
		return lease, err
	}
	mounts := []contract.SandboxMount{}
	for _, m := range lock.Members {
		if string(m.Name) == in.Member {
			mounts = append(mounts, contract.SandboxMount{Kind: "agent", LockId: lock.LockId, Entry: m.AgentEntry, Member: m.Name})
			for _, index := range m.Skills {
				mounts = append(mounts, contract.SandboxMount{Kind: "skill", LockId: lock.LockId, Entry: index, Member: m.Name})
			}
		}
	}
	egress := slices.Clone(in.Spec.Egress)
	slices.Sort(egress)
	rawSpec, err := jcs.Marshal(struct {
		Profile string                  `json:"profile"`
		Egress  []string                `json:"egress"`
		Mounts  []contract.SandboxMount `json:"mounts"`
	}{in.Spec.Profile, egress, mounts})
	if err != nil {
		return lease, err
	}
	specDigest, err := contract.ObjectDigest(rawSpec)
	if err != nil {
		return lease, err
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				activity.RecordHeartbeat(ctx)
			}
		}
	}()
	start := a.Temporal.NewWithStartWorkflowOperation(client.StartWorkflowOptions{ID: w.LeaseWorkflowID(in.TenantID, in.Spec.IsolationKey), TaskQueue: contract.TaskQueueCtl, WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING, WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE}, w.SandboxLeaseWorkflow, w.LeaseInput{TenantID: in.TenantID, IsolationKey: in.Spec.IsolationKey})
	handle, err := a.Temporal.UpdateWithStartWorkflow(ctx, client.UpdateWithStartWorkflowOptions{StartWorkflowOperation: start, UpdateOptions: client.UpdateWorkflowOptions{UpdateName: w.Acquire, UpdateID: fmt.Sprintf("run:%s:acq:%d", in.RunID, in.Attempt), WaitForStage: client.WorkflowUpdateStageCompleted, Args: []any{w.AcquireInput{SpecDigest: specDigest, RunID: in.RunID, SessionID: in.SessionID, WorkspaceID: workspace, Profile: in.Spec.Profile, Egress: in.Spec.Egress, Mounts: mounts, TTLSeconds: in.Spec.TTLSeconds, Reacquire: in.Attempt > 0}}}})
	if err != nil {
		return lease, err
	}
	err = handle.Get(ctx, &lease)
	if err == nil && a.Environment == "prod" && lease.Provider == "local" {
		return lease, temporalInvalid("local sandbox forbidden in prod")
	}
	return lease, err
}
func (a *Activities) RecordSandboxState(ctx context.Context, in w.SandboxStateInput) error {
	return a.DB.WithTenant(ctx, in.TenantID, func(tx pgx.Tx) error {
		provider := "local"
		var sandboxID *string
		var expires *time.Time
		if in.Lease != nil {
			provider = string(in.Lease.Provider)
			sandboxID = &in.Lease.SandboxId
			expires = &in.Lease.ExpiresAt
		}
		if a.Environment == "prod" && provider == "local" && in.Lease != nil {
			return temporalInvalid("local sandbox forbidden in prod")
		}
		profile, err := json.Marshal(map[string]string{"name": in.Profile})
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO sandboxes(tenant_id,isolation_key,provider,sandbox_id,state,profile,session_id,snapshot_id,expires_at,lease_gen) VALUES($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''),$9,$10) ON CONFLICT(tenant_id,isolation_key) DO UPDATE SET provider=EXCLUDED.provider,sandbox_id=EXCLUDED.sandbox_id,state=EXCLUDED.state,profile=EXCLUDED.profile,session_id=EXCLUDED.session_id,snapshot_id=EXCLUDED.snapshot_id,expires_at=EXCLUDED.expires_at,lease_gen=EXCLUDED.lease_gen,updated_at=now() WHERE sandboxes.lease_gen<=EXCLUDED.lease_gen`, in.TenantID, in.IsolationKey, provider, sandboxID, in.State, profile, in.SessionID, in.SnapshotID, expires, in.Gen)
		return err
	})
}

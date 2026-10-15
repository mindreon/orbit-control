package ctl

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/jcs"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func (a *Activities) AcquireSandbox(ctx context.Context, in w.AcquireSandboxInput) (contract.SandboxLease, error) {
	var lease contract.SandboxLease
	ac, err := a.sandboxAcquireInput(ctx, in)
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
	start := a.Temporal.NewWithStartWorkflowOperation(
		client.StartWorkflowOptions{
			ID:                       w.LeaseWorkflowID(in.TenantID, in.Spec.IsolationKey),
			TaskQueue:                contract.TaskQueueCtl,
			WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		},
		w.SandboxLeaseWorkflow,
		w.LeaseInput{TenantID: in.TenantID, IsolationKey: in.Spec.IsolationKey},
	)
	handle, err := a.Temporal.UpdateWithStartWorkflow(
		ctx,
		client.UpdateWithStartWorkflowOptions{
			StartWorkflowOperation: start,
			UpdateOptions: client.UpdateWorkflowOptions{
				UpdateName:   w.Acquire,
				UpdateID:     fmt.Sprintf("run:%s:acq:%d", in.RunID, in.Attempt),
				WaitForStage: client.WorkflowUpdateStageCompleted,
				Args:         []any{ac},
			},
		},
	)
	if err != nil {
		return lease, err
	}
	err = handle.Get(ctx, &lease)
	return lease, err
}
func (a *Activities) RecordSandboxState(ctx context.Context, in w.SandboxStateInput) error {
	if in.Restored != nil {
		return a.change(ctx, w.RunOp{TenantID: in.TenantID, SessionID: in.SessionID, RunID: in.RunID,
			EventN: in.EventN, EventAt: in.EventAt}, func(ctx context.Context, tx pgx.Tx) ([]event, error) {
			if err := a.recordSandbox(ctx, tx, in); err != nil {
				return nil, err
			}
			return []event{{typ: "sandbox.restored", data: contract.SandboxRestored{FromSnapshot: *in.Restored}}}, nil
		})
	}
	return a.DB.WithTenant(ctx, in.TenantID, func(tx pgx.Tx) error { return a.recordSandbox(ctx, tx, in) })
}
func (a *Activities) recordSandbox(ctx context.Context, tx pgx.Tx, in w.SandboxStateInput) error {
	if in.SessionID == "" && in.Gen == 0 && in.Lease == nil {
		return nil
	}
	state := in.State
	if state == "none" {
		state = "destroyed"
	}
	provider := string(in.Provider)
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
	_, err = tx.Exec(ctx, `INSERT INTO sandboxes(tenant_id,isolation_key,provider,sandbox_id,state,profile,
 session_id,snapshot_id,expires_at,lease_gen)
 VALUES($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''),$9,$10)
 ON CONFLICT(tenant_id,isolation_key)
 DO UPDATE SET provider=EXCLUDED.provider,sandbox_id=EXCLUDED.sandbox_id,
 state=EXCLUDED.state,profile=EXCLUDED.profile,session_id=EXCLUDED.session_id,snapshot_id=EXCLUDED.snapshot_id,
 expires_at=EXCLUDED.expires_at,lease_gen=EXCLUDED.lease_gen,updated_at=now()
 WHERE sandboxes.lease_gen<=EXCLUDED.lease_gen`, in.TenantID, in.IsolationKey, provider, sandboxID, state,
		profile, in.SessionID, in.SnapshotID, expires, in.Gen)
	return err
}

func (a *Activities) sandboxAcquireInput(ctx context.Context, in w.AcquireSandboxInput) (w.AcquireInput, error) {
	provider := a.SandboxProvider
	if provider == "" {
		provider = "local"
		if a.Environment == "prod" {
			provider = "opensandbox"
		}
	}
	if provider != "local" && provider != "opensandbox" || a.Environment == "prod" && provider == "local" {
		return w.AcquireInput{}, temporalInvalid("invalid sandbox provider")
	}
	var workspace string
	var gen int
	var lock contract.RunLock
	if err := a.DB.WithTenant(ctx, in.TenantID, func(tx pgx.Tx) error {
		var raw []byte
		err := tx.QueryRow(ctx, `SELECT s.workspace_id::text, l.lock, COALESCE(b.lease_gen, 0)
 FROM runs r
 JOIN sessions s ON s.tenant_id=r.tenant_id
 AND s.id=r.session_id
 JOIN run_locks l ON l.tenant_id=r.tenant_id
 AND l.id=r.lock_id
 LEFT JOIN sandboxes b ON b.tenant_id=r.tenant_id
 AND b.isolation_key=$3
 WHERE r.tenant_id=$1
 AND r.id=$2`, in.TenantID, in.RunID, in.Spec.IsolationKey).Scan(&workspace, &raw, &gen)
		if err != nil {
			return err
		}
		return json.Unmarshal(raw, &lock)
	}); err != nil {
		return w.AcquireInput{}, err
	}
	mounts := []contract.SandboxMount{}
	for _, m := range lock.Members {
		if string(m.Name) == in.Member {
			mounts = append(mounts, contract.SandboxMount{Kind: "agent", LockId: lock.LockId, Entry: m.AgentEntry, Member: m.Name})
			for _, index := range m.Skills {
				mounts = append(mounts, contract.SandboxMount{Kind: "skills", LockId: lock.LockId, Entry: index, Member: m.Name})
			}
		}
	}
	egress := slices.Clone(in.Spec.Egress)
	slices.Sort(egress)
	rawSpec, err := jcs.Marshal(struct {
		Provider contract.SandboxProvider `json:"provider"`
		Profile  string                   `json:"profile"`
		Egress   []string                 `json:"egress"`
		Mounts   []contract.SandboxMount  `json:"mounts"`
	}{Provider: provider, Profile: in.Spec.Profile, Egress: egress, Mounts: mounts})
	if err != nil {
		return w.AcquireInput{}, err
	}
	specDigest, err := contract.ObjectDigest(rawSpec)
	if err != nil {
		return w.AcquireInput{}, err
	}

	return w.AcquireInput{Provider: provider, Gen: gen, EventN: in.EventN, EventAt: in.EventAt, SpecDigest: specDigest, RunID: in.RunID,
		SessionID: in.SessionID, WorkspaceID: workspace, Profile: in.Spec.Profile, Egress: in.Spec.Egress, Mounts: mounts,
		TTLSeconds: in.Spec.TTLSeconds, Reacquire: in.Attempt > 0}, nil
}

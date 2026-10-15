package sandboxlease

import (
	"fmt"

	"time"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"go.temporal.io/sdk/workflow"
)

func Run(ctx workflow.Context, in w.LeaseInput) error {
	st := in.Carry
	if st == nil {
		st = &w.LeaseCarry{State: "none", InUse: map[string]time.Time{}, LastTouch: workflow.Now(ctx)}
	}
	if st.InUse == nil {
		st.InUse = map[string]time.Time{}
	}
	exec := func(name string, arg, out any) error {
		return workflow.ExecuteActivity(w.Sandbox(ctx), name, arg).Get(ctx, out)
	}
	record := func() error {
		return workflow.ExecuteActivity(w.Ctl(ctx, "RecordSandboxState"), "RecordSandboxState", w.SandboxStateInput{TenantID: in.TenantID, IsolationKey: in.IsolationKey, SessionID: st.Spec.SessionID, Gen: st.Gen, State: st.State, Lease: st.Lease, SnapshotID: st.SnapshotID, Profile: st.Spec.Profile}).Get(ctx, nil)
	}
	op := func() contract.SandboxOpInput {
		return contract.SandboxOpInput{TenantId: in.TenantID, IsolationKey: contract.IsolationKey(in.IsolationKey), Provider: st.Lease.Provider, SandboxId: st.Lease.SandboxId}
	}
	removeSnapshot := func(id string) error {
		return exec(contract.ActivitySandboxSnapshotDelete, contract.SandboxSnapshotDeleteInput{TenantId: in.TenantID, IsolationKey: contract.IsolationKey(in.IsolationKey), Provider: st.Lease.Provider, SnapshotId: id}, nil)
	}
	err := workflow.SetUpdateHandlerWithOptions(ctx, w.Acquire, func(ctx workflow.Context, ac w.AcquireInput) (contract.SandboxLease, error) {

		if st.Archived {
			return contract.SandboxLease{}, fmt.Errorf("lease archived")
		}
		if ac.TTLSeconds <= 0 {
			ac.TTLSeconds = 1800
		}
		st.Spec = ac

		digest := ac.SpecDigest
		var err error
		if st.Lease != nil && (ac.Reacquire || st.State == "ready") {
			var inspected contract.SandboxInspectResult
			if err = exec(contract.ActivitySandboxInspect, op(), &inspected); err != nil {
				return contract.SandboxLease{}, err
			}
			if !inspected.Exists {
				st.State = "none"
				if st.SnapshotID != "" {
					st.State = "snapshotted"
				}
			}
		}
		configure := digest != st.SpecDigest
		switch st.State {
		case "none", "snapshotted", "creating":
			if st.State != "creating" {
				st.Gen++
			}
			st.State = "creating"
			if err = record(); err != nil {
				return contract.SandboxLease{}, err
			}
			acq := contract.SandboxAcquireInput{TenantId: in.TenantID, IsolationKey: contract.IsolationKey(in.IsolationKey), Profile: ac.Profile, Egress: ac.Egress, Mounts: ac.Mounts, TtlSeconds: ac.TTLSeconds, Metadata: contract.SandboxMetadata{SessionId: ac.SessionID, WorkspaceId: ac.WorkspaceID, RunId: ac.RunID, LeaseGen: st.Gen}}
			if st.SnapshotID != "" {
				acq.SnapshotId = new(st.SnapshotID)
			}
			var lease contract.SandboxLease
			if err = exec(contract.ActivitySandboxCreate, acq, &lease); err != nil {
				return lease, err
			}
			st.Lease = &lease
			configure = true
		case "paused":
			var lease contract.SandboxLease
			if err = exec(contract.ActivitySandboxResume, op(), &lease); err != nil {
				return lease, err
			}
			st.Lease = &lease
		}
		// Persist the identity before Configure requests its sandbox capability.
		st.State = "ready"
		if err = record(); err != nil {
			return contract.SandboxLease{}, err
		}
		if configure {
			var lease contract.SandboxLease
			if err = exec(contract.ActivitySandboxConfigure, contract.SandboxConfigureInput{TenantId: in.TenantID, IsolationKey: contract.IsolationKey(in.IsolationKey), RunId: ac.RunID, Lease: *st.Lease, Egress: ac.Egress, Mounts: ac.Mounts}, &lease); err != nil {
				return lease, err
			}
			st.Lease = &lease
			st.SpecDigest = digest
		}
		st.LastTouch = workflow.Now(ctx)
		st.InUse[ac.RunID] = st.LastTouch
		return *st.Lease, record()
	}, workflow.UpdateHandlerOptions{Validator: func(ac w.AcquireInput) error {
		if st.Archived || ac.RunID == "" || ac.SessionID == "" {
			return fmt.Errorf("lease unavailable")
		}
		return nil
	}})
	if err != nil {
		return err
	}
	if err = workflow.SetQueryHandler(ctx, w.Lease, func() (w.LeaseCarry, error) { return *st, nil }); err != nil {
		return err
	}
	workflow.Go(ctx, func(ctx workflow.Context) {
		ch := workflow.GetSignalChannel(ctx, w.Touch)
		for {
			var t w.TouchInput
			ch.Receive(ctx, &t)
			st.LastTouch = workflow.Now(ctx)
			st.Touches++
			if t.InUse {
				st.InUse[t.RunID] = st.LastTouch
			} else {
				delete(st.InUse, t.RunID)
			}
		}
	})
	workflow.Go(ctx, func(ctx workflow.Context) {
		var a w.ArchiveInput
		workflow.GetSignalChannel(ctx, w.Archive).Receive(ctx, &a)
		st.Archived = true
	})
	for !st.Archived {
		if _, err = workflow.AwaitWithTimeout(ctx, 10*time.Second, func() bool { return st.Archived }); err != nil {
			return err
		}
		if st.Archived {
			break
		}

		now := workflow.Now(ctx)
		keys := workflow.DeterministicKeys(st.InUse)
		for _, key := range keys {
			if now.Sub(st.InUse[key]) >= 30*time.Minute {
				delete(st.InUse, key)
			}
		}
		if st.Lease != nil && st.State == "ready" && now.Add(time.Duration(st.Spec.TTLSeconds)*time.Second/2).After(st.Lease.ExpiresAt) {
			var lease contract.SandboxLease
			e := exec(contract.ActivitySandboxRenew, contract.SandboxRenewInput{TenantId: in.TenantID, IsolationKey: contract.IsolationKey(in.IsolationKey), Provider: st.Lease.Provider, SandboxId: st.Lease.SandboxId, TtlSeconds: st.Spec.TTLSeconds}, &lease)
			if e != nil {
				st.State = "none"
			} else {
				st.Lease = &lease
			}
			if err = record(); err != nil {
				return err
			}
		}
		if st.Lease != nil && st.Lease.Provider == "opensandbox" && len(st.InUse) == 0 {
			idle := now.Sub(st.LastTouch)
			if idle >= 15*time.Minute && (st.State == "ready" || st.State == "paused") {
				var snapshot contract.SandboxSnapshotResult
				if err = exec(contract.ActivitySandboxSnapshot, op(), &snapshot); err != nil {
					return err
				}
				old := st.SnapshotID
				st.SnapshotID = snapshot.SnapshotId
				if old != "" && old != st.SnapshotID {
					if err = removeSnapshot(old); err != nil {
						return err
					}
				}
				if err = exec(contract.ActivitySandboxDestroy, op(), nil); err != nil {
					return err
				}
				st.State = "snapshotted"
				if err = record(); err != nil {
					return err
				}
			} else if idle >= 2*time.Minute && st.State == "ready" {
				if err = exec(contract.ActivitySandboxPause, op(), nil); err != nil {
					return err
				}
				st.State = "paused"
				if err = record(); err != nil {
					return err
				}
			}
		}
		if st.Touches >= 1000 && workflow.AllHandlersFinished(ctx) {
			in.Carry = st
			st.Touches = 0
			return workflow.NewContinueAsNewError(ctx, w.SandboxLeaseWorkflow, in)
		}
	}
	if err = workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return err
	}
	if st.Lease != nil {
		if st.State != "snapshotted" {
			if err = exec(contract.ActivitySandboxDestroy, op(), nil); err != nil {
				return err
			}
		}
		if st.SnapshotID != "" {
			if err = removeSnapshot(st.SnapshotID); err != nil {
				return err
			}
		}
	}
	st.State = "destroyed"
	return record()
}

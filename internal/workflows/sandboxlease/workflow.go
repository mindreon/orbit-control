package sandboxlease

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type leaseWorkflow struct {
	in    w.LeaseInput
	st    *w.LeaseCarry
	mutex workflow.Mutex
	wake  workflow.Channel
}

func Run(ctx workflow.Context, in w.LeaseInput) error {
	st := in.Carry
	if st == nil {
		st = &w.LeaseCarry{State: "none", LastTouch: workflow.Now(ctx)}
	}
	if st.InUse == nil {
		st.InUse = map[string]time.Time{}
	}
	l := &leaseWorkflow{
		in:    in,
		st:    st,
		mutex: workflow.NewMutex(ctx),
		wake:  workflow.NewBufferedChannel(ctx, 1),
	}
	if err := l.register(ctx); err != nil {
		return err
	}
	for {
		l.drainSignals(ctx)
		if st.Lease != nil && workflow.AllHandlersFinished(ctx) &&
			(!st.Archived || !st.RetryAt.IsZero()) &&
			(st.Touches >= 1000 || workflow.GetInfo(ctx).GetContinueAsNewSuggested()) {
			st.Touches = 0
			in.Carry = st
			return workflow.NewContinueAsNewError(ctx, w.SandboxLeaseWorkflow, in)
		}
		if st.Archived {
			if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
				return err
			}
			if workflow.Now(ctx).Before(st.RetryAt) {
				l.wait(ctx)
				continue
			}
			if err := l.archive(ctx); err != nil {
				l.retry(ctx, err)
				continue
			}
			return nil
		}
		l.wait(ctx)
		if st.Archived || workflow.Now(ctx).Before(l.st.RetryAt) {
			continue
		}
		if err := l.mutex.Lock(ctx); err != nil {
			return err
		}
		err := l.maintain(ctx)
		l.mutex.Unlock()
		if err != nil {
			l.retry(ctx, err)
		} else {
			l.st.RetryAt, l.st.Backoff = time.Time{}, 0
		}
	}
}

// Provider retries are bounded per Activity; maintenance keeps trying for the lease lifetime.
func (l *leaseWorkflow) retry(ctx workflow.Context, err error) {
	l.st.Backoff = min(15*time.Minute, max(time.Minute, 2*l.st.Backoff))
	l.st.RetryAt = workflow.Now(ctx).Add(l.st.Backoff)
	workflow.GetLogger(ctx).Warn("lease maintenance failed", "error", err, "retry_after", l.st.Backoff)
}
func (l *leaseWorkflow) touch(ctx workflow.Context, in w.TouchInput) {
	l.st.LastTouch = workflow.Now(ctx)
	l.st.Touches++
	if in.InUse {
		holder := in.Holder
		if holder == "" {
			holder = in.RunID
		}
		l.st.InUse[holder] = l.st.LastTouch
	} else {
		holder := in.Holder
		if holder == "" {
			holder = in.RunID
		}
		delete(l.st.InUse, holder)
	}
}
func (l *leaseWorkflow) drainSignals(ctx workflow.Context) {
	ch := workflow.GetSignalChannel(ctx, w.Touch)
	var touch w.TouchInput
	for ch.ReceiveAsync(&touch) {
		l.touch(ctx, touch)
	}
	ch = workflow.GetSignalChannel(ctx, w.Archive)
	for ch.ReceiveAsync(nil) {
		l.st.Archived = true
	}
}

func (l *leaseWorkflow) exec(ctx workflow.Context, name string, arg, out any) error {
	return workflow.ExecuteActivity(w.Sandbox(ctx), name, arg).Get(ctx, out)
}
func (l *leaseWorkflow) record(ctx workflow.Context, restored *bool) error {
	l.st.Dirty = true
	if restored != nil {
		l.st.PendingRestored = restored
	}
	in := w.SandboxStateInput{
		TenantID: l.in.TenantID, IsolationKey: l.in.IsolationKey, SessionID: l.st.Spec.SessionID,
		Gen: l.st.Gen, State: l.st.State, Lease: l.st.Lease, SnapshotID: l.st.SnapshotID,
		Profile: l.st.Spec.Profile, RunID: l.st.Spec.RunID, Restored: l.st.PendingRestored, Provider: l.st.Spec.Provider,
		EventN: l.st.Spec.EventN, EventAt: l.st.Spec.EventAt, Member: l.st.Spec.Member, Holder: l.st.Spec.Holder,
	}
	err := workflow.ExecuteActivity(w.Ctl(ctx, "RecordSandboxState"), "RecordSandboxState", in).Get(ctx, nil)
	if err == nil {
		l.st.Dirty = false
		l.st.PendingRestored = nil
	}
	return err
}
func (l *leaseWorkflow) op() contract.SandboxOpInput {
	return contract.SandboxOpInput{
		TenantId: l.in.TenantID, IsolationKey: contract.IsolationKey(l.in.IsolationKey),
		Provider: l.st.Lease.Provider, SandboxId: l.st.Lease.SandboxId,
	}
}
func (l *leaseWorkflow) removeSnapshot(ctx workflow.Context, id string) error {
	return l.exec(ctx, contract.ActivitySandboxSnapshotDelete, contract.SandboxSnapshotDeleteInput{
		TenantId: l.in.TenantID, IsolationKey: contract.IsolationKey(l.in.IsolationKey),
		Provider: l.st.Lease.Provider, SnapshotId: id,
	}, nil)
}
func (l *leaseWorkflow) register(ctx workflow.Context) error {
	if err := workflow.SetQueryHandler(ctx, w.Lease, func() (w.LeaseCarry, error) { return *l.st, nil }); err != nil {
		return err
	}
	return workflow.SetUpdateHandlerWithOptions(ctx, w.Acquire, l.acquire,
		workflow.UpdateHandlerOptions{Validator: func(ac w.AcquireInput) error {
			if l.st.Archived {
				return temporal.NewNonRetryableApplicationError("lease closing", "LEASE_CLOSING", nil)
			}
			if ac.RunID == "" || ac.SessionID == "" || ac.Profile == "" ||
				(ac.Provider != "local" && ac.Provider != "opensandbox") {
				return fmt.Errorf("lease unavailable")
			}
			return nil
		}})
}
func (l *leaseWorkflow) acquire(ctx workflow.Context, ac w.AcquireInput) (contract.SandboxLease, error) {
	if err := l.mutex.Lock(ctx); err != nil {
		return contract.SandboxLease{}, err
	}
	defer l.mutex.Unlock()
	if l.st.Archived {
		return contract.SandboxLease{}, temporal.NewNonRetryableApplicationError("lease closing", "LEASE_CLOSING", nil)
	}
	if ac.TTLSeconds <= 0 {
		ac.TTLSeconds = 1800
	}
	if l.st.SnapshotPending {
		if err := l.snapshot(ctx); err != nil {
			return contract.SandboxLease{}, err
		}
	}
	st := l.st
	st.Spec = ac
	restored := st.Gen > 0 && (st.State == "none" || st.State == "snapshotted")
	if st.Lease != nil && (ac.Reacquire || st.State == "ready") {
		var inspected contract.SandboxInspectResult
		if err := l.exec(ctx, contract.ActivitySandboxInspect, l.op(), &inspected); err != nil {
			return contract.SandboxLease{}, err
		}
		if !inspected.Exists {
			restored = true
			st.State = "none"
			if st.SnapshotID != "" {
				st.State = "snapshotted"
			}
		}
	}
	configure := ac.SpecDigest != st.SpecDigest
	fromSnapshot := st.SnapshotID != ""
	switch st.State {
	case "none", "snapshotted", "creating":
		restored = restored || st.State == "snapshotted"
		if err := l.create(ctx, ac); err != nil {
			return contract.SandboxLease{}, err
		}
		configure = true
	case "paused":
		var lease contract.SandboxLease
		if err := l.exec(ctx, contract.ActivitySandboxResume, l.op(), &lease); err != nil {
			return lease, err
		}
		st.Lease = &lease
	}
	st.State = "ready"
	// Configure obtains a capability against this persisted identity.
	if err := l.record(ctx, nil); err != nil {
		return contract.SandboxLease{}, err
	}
	if configure {
		if err := l.configure(ctx, ac); err != nil {
			return contract.SandboxLease{}, err
		}
	}
	st.LastTouch = workflow.Now(ctx)
	st.NextRenew = st.LastTouch.Add(time.Duration(ac.TTLSeconds) * time.Second / 2)
	holder := ac.Holder
	if holder == "" {
		holder = ac.RunID
	}
	st.InUse[holder] = st.LastTouch
	var event *bool
	if restored {
		event = new(fromSnapshot)
	}
	// The ready identity is already persisted; maintenance retries this projection.
	_ = l.record(ctx, event)
	l.wake.SendAsync(true)
	return *st.Lease, nil
}
func (l *leaseWorkflow) create(ctx workflow.Context, ac w.AcquireInput) error {
	st := l.st
	if st.State != "creating" {
		st.Gen = max(st.Gen, ac.Gen) + 1
	}
	st.State = "creating"
	if err := l.record(ctx, nil); err != nil {
		return err
	}
	input := contract.SandboxAcquireInput{
		TenantId: l.in.TenantID, IsolationKey: contract.IsolationKey(l.in.IsolationKey),
		Provider: ac.Provider, Profile: ac.Profile, Egress: ac.Egress, Mounts: ac.Mounts, TtlSeconds: ac.TTLSeconds,
		Metadata: contract.SandboxMetadata{
			SessionId: ac.SessionID, WorkspaceId: ac.WorkspaceID, RunId: ac.RunID, LeaseGen: st.Gen,
		},
	}
	if st.SnapshotID != "" {
		input.SnapshotId = new(st.SnapshotID)
	}
	var lease contract.SandboxLease
	if err := l.exec(ctx, contract.ActivitySandboxCreate, input, &lease); err != nil {
		return err
	}
	st.Lease = &lease
	return nil
}
func (l *leaseWorkflow) configure(ctx workflow.Context, ac w.AcquireInput) error {
	var lease contract.SandboxLease
	input := contract.SandboxConfigureInput{
		TenantId: l.in.TenantID, IsolationKey: contract.IsolationKey(l.in.IsolationKey),
		RunId: ac.RunID, Lease: *l.st.Lease, Egress: ac.Egress, Mounts: ac.Mounts,
	}
	if err := l.exec(ctx, contract.ActivitySandboxConfigure, input, &lease); err != nil {
		return err
	}
	l.st.Lease = &lease
	l.st.SpecDigest = ac.SpecDigest
	return nil
}
func (l *leaseWorkflow) wait(ctx workflow.Context) {
	st := l.st
	now := workflow.Now(ctx)
	selector := workflow.NewSelector(ctx)
	timerCtx, cancel := workflow.WithCancel(ctx)
	defer cancel()
	addTimer := func(at time.Time) {
		selector.AddFuture(workflow.NewTimer(timerCtx, max(time.Duration(0), at.Sub(now))), func(workflow.Future) {})
	}
	if !l.st.RetryAt.IsZero() {
		addTimer(l.st.RetryAt)
	} else if st.Lease != nil && st.State == "ready" {
		addTimer(st.NextRenew)
	}
	if l.st.RetryAt.IsZero() {
		for _, key := range workflow.DeterministicKeys(st.InUse) {
			addTimer(st.InUse[key].Add(30 * time.Minute))
		}
	}
	if l.st.RetryAt.IsZero() && st.Lease != nil && st.Lease.Provider == "opensandbox" && len(st.InUse) == 0 {
		switch st.State {
		case "ready":
			addTimer(st.LastTouch.Add(2 * time.Minute))
		case "paused":
			addTimer(st.LastTouch.Add(15 * time.Minute))
		}
	}
	selector.AddReceive(l.wake, func(ch workflow.ReceiveChannel, _ bool) { ch.Receive(ctx, nil) })
	selector.AddReceive(workflow.GetSignalChannel(ctx, w.Touch), func(ch workflow.ReceiveChannel, _ bool) {
		var touch w.TouchInput
		ch.Receive(ctx, &touch)
		l.touch(ctx, touch)
	})
	selector.AddReceive(workflow.GetSignalChannel(ctx, w.Archive), func(ch workflow.ReceiveChannel, _ bool) {
		ch.Receive(ctx, nil)
		st.Archived = true
	})
	selector.Select(ctx)
}
func (l *leaseWorkflow) maintain(ctx workflow.Context) error {
	st := l.st
	if st.Archived {
		return nil
	}
	if l.st.Dirty {
		if err := l.record(ctx, nil); err != nil {
			return err
		}
	}
	if l.st.SnapshotPending {
		return l.snapshot(ctx)
	}
	now := workflow.Now(ctx)
	for _, key := range workflow.DeterministicKeys(st.InUse) {
		if now.Sub(st.InUse[key]) >= 30*time.Minute {
			delete(st.InUse, key)
		}
	}
	if st.Lease == nil {
		return nil
	}
	if st.State == "ready" && !now.Before(st.NextRenew) {
		if err := l.renew(ctx); err != nil {
			return err
		}
	}
	if st.Lease.Provider != "opensandbox" || len(st.InUse) > 0 {
		return nil
	}
	idle := now.Sub(st.LastTouch)
	if idle >= 15*time.Minute && (st.State == "ready" || st.State == "paused") {
		return l.snapshot(ctx)
	}
	if idle >= 2*time.Minute && st.State == "ready" {
		if err := l.exec(ctx, contract.ActivitySandboxPause, l.op(), nil); err != nil {
			return err
		}
		st.State = "paused"
		return l.record(ctx, nil)
	}
	return nil
}
func (l *leaseWorkflow) renew(ctx workflow.Context) error {
	st := l.st
	var lease contract.SandboxLease
	err := l.exec(ctx, contract.ActivitySandboxRenew, contract.SandboxRenewInput{
		TenantId: l.in.TenantID, IsolationKey: contract.IsolationKey(l.in.IsolationKey),
		Provider: st.Lease.Provider, SandboxId: st.Lease.SandboxId, TtlSeconds: st.Spec.TTLSeconds,
	}, &lease)
	if err != nil {
		app, ok := errors.AsType[*temporal.ApplicationError](err)
		if !ok || app.Type() != "NotFound" {
			return err
		}
		st.State = "none"
	} else {
		st.Lease = &lease
	}
	st.NextRenew = workflow.Now(ctx).Add(time.Duration(st.Spec.TTLSeconds) * time.Second / 2)
	return l.record(ctx, nil)
}
func (l *leaseWorkflow) snapshot(ctx workflow.Context) error {
	if !l.st.SnapshotPending {
		var snapshot contract.SandboxSnapshotResult
		if err := l.exec(ctx, contract.ActivitySandboxSnapshot, l.op(), &snapshot); err != nil {
			return err
		}
		l.st.ObsoleteSnapshot = l.st.SnapshotID
		l.st.SnapshotID = snapshot.SnapshotId
		l.st.SnapshotPending = true
	}
	if l.st.ObsoleteSnapshot != "" && l.st.ObsoleteSnapshot != l.st.SnapshotID {
		if err := l.removeSnapshot(ctx, l.st.ObsoleteSnapshot); err != nil {
			return err
		}
		l.st.ObsoleteSnapshot = ""
	}
	if err := l.exec(ctx, contract.ActivitySandboxDestroy, l.op(), nil); err != nil {
		return err
	}
	l.st.SnapshotPending = false
	l.st.State = "snapshotted"
	return l.record(ctx, nil)
}
func (l *leaseWorkflow) archive(ctx workflow.Context) error {
	if l.st.ObsoleteSnapshot != "" && l.st.ObsoleteSnapshot != l.st.SnapshotID {
		if err := l.removeSnapshot(ctx, l.st.ObsoleteSnapshot); err != nil {
			return err
		}
		l.st.ObsoleteSnapshot = ""
	}
	if l.st.Lease != nil {
		if l.st.State != "snapshotted" && l.st.State != "none" && l.st.State != "destroyed" {
			if err := l.exec(ctx, contract.ActivitySandboxDestroy, l.op(), nil); err != nil {
				return err
			}
		}
		if l.st.SnapshotID != "" {
			if err := l.removeSnapshot(ctx, l.st.SnapshotID); err != nil {
				return err
			}
		}
	}
	l.st.State = "destroyed"
	return l.record(ctx, nil)
}

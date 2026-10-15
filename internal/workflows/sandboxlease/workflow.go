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
	in               w.LeaseInput
	st               *w.LeaseCarry
	mutex            workflow.Mutex
	wake             workflow.Channel
	retryAt          time.Time
	backoff          time.Duration
	dirty            bool
	pendingRestored  *bool
	snapshotPending  bool
	obsoleteSnapshot string
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
		if st.Archived {
			if err := workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
				return err
			}
			if err := l.archive(ctx); err == nil {
				return nil
			} else {
				l.retry(ctx, err)
				if err := workflow.Sleep(ctx, l.backoff); err != nil {
					return err
				}
			}
			continue
		}
		if st.Lease != nil && l.retryAt.IsZero() && !l.dirty && !l.snapshotPending &&
			workflow.AllHandlersFinished(ctx) &&
			(st.Touches >= 1000 || workflow.GetInfo(ctx).GetContinueAsNewSuggested()) {
			st.Touches = 0
			in.Carry = st
			return workflow.NewContinueAsNewError(ctx, w.SandboxLeaseWorkflow, in)
		}
		l.wait(ctx)
		if st.Archived || workflow.Now(ctx).Before(l.retryAt) {
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
			l.retryAt, l.backoff = time.Time{}, 0
		}
	}
}

// Provider retries are bounded per Activity; maintenance keeps trying for the lease lifetime.
func (l *leaseWorkflow) retry(ctx workflow.Context, err error) {
	l.backoff = min(15*time.Minute, max(time.Minute, 2*l.backoff))
	l.retryAt = workflow.Now(ctx).Add(l.backoff)
	workflow.GetLogger(ctx).Warn("lease maintenance failed", "error", err, "retry_after", l.backoff)
}
func (l *leaseWorkflow) touch(ctx workflow.Context, in w.TouchInput) {
	l.st.LastTouch = workflow.Now(ctx)
	l.st.Touches++
	if in.InUse {
		l.st.InUse[in.RunID] = l.st.LastTouch
	} else {
		delete(l.st.InUse, in.RunID)
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
	l.dirty = true
	if restored != nil {
		l.pendingRestored = restored
	}
	in := w.SandboxStateInput{
		TenantID: l.in.TenantID, IsolationKey: l.in.IsolationKey, SessionID: l.st.Spec.SessionID,
		Gen: l.st.Gen, State: l.st.State, Lease: l.st.Lease, SnapshotID: l.st.SnapshotID,
		Profile: l.st.Spec.Profile, RunID: l.st.Spec.RunID, Restored: l.pendingRestored, Provider: l.st.Spec.Provider,
		EventN: l.st.Spec.EventN, EventAt: l.st.Spec.EventAt,
	}
	err := workflow.ExecuteActivity(w.Ctl(ctx, "RecordSandboxState"), "RecordSandboxState", in).Get(ctx, nil)
	if err == nil {
		l.dirty = false
		l.pendingRestored = nil
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
			if l.st.Archived || ac.RunID == "" || ac.SessionID == "" || ac.Profile == "" ||
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
		return contract.SandboxLease{}, fmt.Errorf("lease archived")
	}
	if ac.TTLSeconds <= 0 {
		ac.TTLSeconds = 1800
	}
	if l.snapshotPending {
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
	st.InUse[ac.RunID] = st.LastTouch
	var event *bool
	if restored {
		event = new(fromSnapshot)
	}
	err := l.record(ctx, event)
	l.wake.SendAsync(true)
	return *st.Lease, err
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
	if !l.retryAt.IsZero() {
		addTimer(l.retryAt)
	} else if st.Lease != nil && st.State == "ready" {
		addTimer(st.NextRenew)
	}
	if l.retryAt.IsZero() {
		for _, key := range workflow.DeterministicKeys(st.InUse) {
			addTimer(st.InUse[key].Add(30 * time.Minute))
		}
	}
	if l.retryAt.IsZero() && st.Lease != nil && st.Lease.Provider == "opensandbox" && len(st.InUse) == 0 {
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
	if l.dirty {
		if err := l.record(ctx, nil); err != nil {
			return err
		}
	}
	if l.snapshotPending {
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
	if !l.snapshotPending {
		var snapshot contract.SandboxSnapshotResult
		if err := l.exec(ctx, contract.ActivitySandboxSnapshot, l.op(), &snapshot); err != nil {
			return err
		}
		l.obsoleteSnapshot = l.st.SnapshotID
		l.st.SnapshotID = snapshot.SnapshotId
		l.snapshotPending = true
	}
	if l.obsoleteSnapshot != "" && l.obsoleteSnapshot != l.st.SnapshotID {
		if err := l.removeSnapshot(ctx, l.obsoleteSnapshot); err != nil {
			return err
		}
		l.obsoleteSnapshot = ""
	}
	if err := l.exec(ctx, contract.ActivitySandboxDestroy, l.op(), nil); err != nil {
		return err
	}
	l.snapshotPending = false
	l.st.State = "snapshotted"
	return l.record(ctx, nil)
}
func (l *leaseWorkflow) archive(ctx workflow.Context) error {
	if l.obsoleteSnapshot != "" && l.obsoleteSnapshot != l.st.SnapshotID {
		if err := l.removeSnapshot(ctx, l.obsoleteSnapshot); err != nil {
			return err
		}
		l.obsoleteSnapshot = ""
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

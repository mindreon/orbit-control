//go:build e2e

package app

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"

	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/orch"
)

// orbitE2EFaults is the ORBIT_E2E_FAULTS injection layer. The production
// build does not contain this symbol.
var orbitE2EFaults atomic.Value

// E2EBeforeClassify runs inside a not-delivered injection, before the
// error is returned, so a test can move decided_at first.
var E2EBeforeClassify func()

func init() {
	orbitE2EFaults.Store(os.Getenv("ORBIT_E2E_FAULTS"))
}

// SetE2EFault selects the injection for later decide calls in this process.
func SetE2EFault(mode string) { orbitE2EFaults.Store(mode) }

// StopReconcile ends the background delivery pass started with the process.
func (a *App) StopReconcile() { a.stopReconcile() }

func e2eFault() string {
	s, _ := orbitE2EFaults.Load().(string)
	return s
}

// SetSkipResultWrite marks the approval delivered and stores the result
// without applying it. The reconciler applies that stored body.
func (a *App) SetSkipResultWrite(skip bool) { a.skipResultWrite.Store(skip) }

func (a *App) skipResultWriteEnabled() bool {
	return a.skipResultWrite.Load() || e2eFault() == "skip-result-apply"
}

func wrapOrch(o Orchestrator) Orchestrator {
	if o == nil {
		return nil
	}
	return &faultOrch{inner: o, used: map[string]bool{}, cached: map[string]orch.DecideUpdate{}}
}

type faultOrch struct {
	inner  Orchestrator
	mu     sync.Mutex
	used   map[string]bool
	cached map[string]orch.DecideUpdate
}

func (f *faultOrch) Decide(ctx context.Context, roomID, approvalRequestID, turnID, decision string) (orch.DecideUpdate, error) {
	switch e2eFault() {
	case "not-delivered":
		if E2EBeforeClassify != nil {
			E2EBeforeClassify()
		}
		return nil, temporal.NewApplicationError("NOT_DELIVERED", "APPROVAL_UNKNOWN", nil)
	case "fatal-on-accept":
		return nil, temporal.NewApplicationError("limit", limitCode, nil)
	case "unknown-once":
		f.mu.Lock()
		used := f.used[approvalRequestID]
		cached, ok := f.cached[approvalRequestID]
		f.mu.Unlock()
		if used && ok {
			return cached, nil
		}
		upd, err := f.inner.Decide(ctx, roomID, approvalRequestID, turnID, decision)
		if err != nil {
			return nil, err
		}
		f.mu.Lock()
		f.used[approvalRequestID] = true
		f.cached[approvalRequestID] = upd
		f.mu.Unlock()
		return nil, context.DeadlineExceeded
	case "unknown-always":
		f.mu.Lock()
		used := f.used[approvalRequestID]
		if !used {
			f.used[approvalRequestID] = true
		}
		f.mu.Unlock()
		if !used {
			if _, err := f.inner.Decide(ctx, roomID, approvalRequestID, turnID, decision); err != nil {
				return nil, err
			}
		}
		return nil, context.DeadlineExceeded
	case "fatal-on-get":
		upd, err := f.inner.Decide(ctx, roomID, approvalRequestID, turnID, decision)
		if err != nil {
			return nil, err
		}
		return fatalUpdate{inner: upd}, nil
	case "worker-error-on-get":
		upd, err := f.inner.Decide(ctx, roomID, approvalRequestID, turnID, decision)
		if err != nil {
			return nil, err
		}
		return plainUpdate{inner: upd}, nil
	default:
		return f.inner.Decide(ctx, roomID, approvalRequestID, turnID, decision)
	}
}

func (f *faultOrch) DecideConfig(ctx context.Context, roomID string) (orch.DecideConfig, error) {
	return f.inner.DecideConfig(ctx, roomID)
}
func (f *faultOrch) DecideOutcome(ctx context.Context, roomID, approvalRequestID string) (bool, orch.DecideOutcome, error) {
	return f.inner.DecideOutcome(ctx, roomID, approvalRequestID)
}
func (f *faultOrch) ApprovalPending(ctx context.Context, roomID, approvalRequestID string) (bool, error) {
	return f.inner.ApprovalPending(ctx, roomID, approvalRequestID)
}
func (f *faultOrch) StartRoom(ctx context.Context, roomID, kind, permissionPreset string) (orch.RoomView, error) {
	return f.inner.StartRoom(ctx, roomID, kind, permissionPreset)
}
func (f *faultOrch) RunTurn(ctx context.Context, roomID, turnID, message string) (orch.RunTurnResult, error) {
	return f.inner.RunTurn(ctx, roomID, turnID, message)
}
func (f *faultOrch) Steer(ctx context.Context, roomID, turnID, instruction string) error {
	return f.inner.Steer(ctx, roomID, turnID, instruction)
}
func (f *faultOrch) Abort(ctx context.Context, roomID, turnID, reason string) error {
	return f.inner.Abort(ctx, roomID, turnID, reason)
}

type fatalUpdate struct{ inner orch.DecideUpdate }

func (f fatalUpdate) Result(context.Context) (orch.DecideResult, error) {
	return orch.DecideResult{}, temporal.NewApplicationError("limit", limitCode, nil)
}

type plainUpdate struct{ inner orch.DecideUpdate }

func (p plainUpdate) Result(context.Context) (orch.DecideResult, error) {
	return orch.DecideResult{}, errors.New("boom DECIDED_APPROVALS_LIMIT")
}

package workflows_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/sandboxlease"
)

func TestLeaseContinueCarriesPendingMaintenance(t *testing.T) {
	for _, archived := range []bool{false, true} {
		t.Run(map[bool]string{false: "maintenance", true: "archive"}[archived], func(t *testing.T) {
			h := newLeaseHarness(t, "opensandbox")
			at := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
			h.in.Carry = &w.LeaseCarry{State: "paused", Gen: 2, Archived: archived,
				Lease:      &contract.SandboxLease{Provider: "opensandbox", SandboxId: "sandbox"},
				Spec:       w.AcquireInput{Provider: "opensandbox", RunID: "run", SessionID: "session", Profile: "default", TTLSeconds: 1800},
				SnapshotID: "snapshot", RetryAt: at.Add(time.Minute), Backoff: time.Minute, Dirty: true,
				PendingRestored: new(true), SnapshotPending: true, ObsoleteSnapshot: "old-snapshot"}
			h.env.SetContinueAsNewSuggested(true)
			h.env.ExecuteWorkflow(sandboxlease.Run, h.in)
			can, ok := errors.AsType[*workflow.ContinueAsNewError](h.env.GetWorkflowError())
			require.True(t, ok)
			var next w.LeaseInput
			require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &next))
			require.Equal(t, h.in.Carry, next.Carry)
			resumed := newLeaseHarness(t, "opensandbox")
			resumed.in = next
			if !archived {
				resumed.archive(2 * time.Minute)
			}
			resumed.execute(t)
			require.Empty(t, resumed.creates)
			require.NotContains(t, resumed.calls, "snapshot", "pending snapshot must not be recreated")
			require.Contains(t, resumed.calls, "delete")
			require.Equal(t, "destroyed", resumed.records[len(resumed.records)-1].State)
		})
	}
}

func TestLeaseArchiveFailureCanContinue(t *testing.T) {
	h := newLeaseHarness(t, "local")
	failure := temporal.NewNonRetryableApplicationError("unavailable", "fixture", nil)
	h.env.OnActivity(contract.ActivitySandboxDestroy, mock.Anything, mock.Anything).Return(failure)
	h.env.RegisterDelayedCallback(func() { h.acquire(t, "initial", false) }, 0)
	h.archive(time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SetContinueAsNewSuggested(true) }, 2*time.Second)
	h.env.ExecuteWorkflow(sandboxlease.Run, h.in)
	can, ok := errors.AsType[*workflow.ContinueAsNewError](h.env.GetWorkflowError())
	require.True(t, ok)
	var next w.LeaseInput
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &next))
	require.True(t, next.Carry.Archived)
	require.False(t, next.Carry.RetryAt.IsZero())
	require.Equal(t, time.Minute, next.Carry.Backoff)
}

func TestLeaseClosingValidatorType(t *testing.T) {
	h := newLeaseHarness(t, "local")
	h.env.OnActivity(contract.ActivitySandboxDestroy, mock.Anything, mock.Anything).After(10 * time.Second).Return(nil)
	h.env.RegisterDelayedCallback(func() { h.acquire(t, "initial", false) }, 0)
	h.archive(time.Second)
	rejected := false
	h.env.RegisterDelayedCallback(func() {
		h.env.UpdateWorkflow(w.Acquire, "late", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				app, ok := errors.AsType[*temporal.ApplicationError](err)
				require.True(t, ok)
				require.Equal(t, "LEASE_CLOSING", app.Type())
				rejected = true
			}, OnComplete: func(any, error) { t.Error("closing lease accepted acquire") },
		}, w.AcquireInput{Provider: "local", RunID: "late", SessionID: "session", Profile: "default"})
	}, 2*time.Second)
	h.execute(t)
	require.True(t, rejected)
}

func TestLeaseFinalProjectionFailureDoesNotFailAcquire(t *testing.T) {
	h := newLeaseHarness(t, "local")
	failure := temporal.NewNonRetryableApplicationError("unavailable", "fixture", nil)
	ready := 0
	h.env.OnActivity("RecordSandboxState", mock.Anything, mock.Anything).Return(func(_ context.Context, in w.SandboxStateInput) error {
		if in.State == "ready" {
			ready++
			if ready == 2 {
				return failure
			}
		}
		h.records = append(h.records, in)
		return nil
	})
	h.env.RegisterDelayedCallback(func() { h.acquire(t, "initial", false) }, 0)
	h.archive(time.Minute)
	h.execute(t)
	h.env.AssertExpectations(t)
	require.Len(t, h.creates, 1)
	require.GreaterOrEqual(t, len(h.records), 2)
}

func TestCancellationDuringRecovery(t *testing.T) {
	for _, name := range []string{"ReconcileCheckpoint", "AcquireSandbox"} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			if name == "ReconcileCheckpoint" {
				h.run = func(contract.SegmentInput) (contract.SegmentResult, error) {
					return contract.SegmentResult{}, temporal.NewNonRetryableApplicationError("conflict", contract.ErrorCheckpointConflict, nil)
				}
				h.env.OnActivity(name, mock.Anything, mock.Anything).After(2*time.Second).Return(w.ReconcileOutput{}, nil)
			} else {
				h.run = func(contract.SegmentInput) (contract.SegmentResult, error) {
					return contract.SegmentResult{}, temporal.NewNonRetryableApplicationError("lost", contract.ErrorSandboxUnavailable, nil)
				}
				h.env.OnActivity(name, mock.Anything, mock.MatchedBy(func(in w.AcquireSandboxInput) bool { return in.Attempt == 0 })).
					Return(contract.SandboxLease{Provider: "local", SandboxId: "sandbox"}, nil).Once()
				h.env.OnActivity(name, mock.Anything, mock.Anything).After(2*time.Second).Return(contract.SandboxLease{}, nil)
			}
			h.env.RegisterDelayedCallback(func() { h.env.CancelWorkflow() }, time.Second)
			h.execute(t, "cancelled", "CANCELLED")
		})
	}
}

func TestLeaseRenewFailureCanContinue(t *testing.T) {
	h := newLeaseHarness(t, "local")
	h.ttl = 4
	failure := temporal.NewNonRetryableApplicationError("unavailable", "fixture", nil)
	h.env.OnActivity(contract.ActivitySandboxRenew, mock.Anything, mock.Anything).Return(contract.SandboxLease{}, failure)
	h.env.RegisterDelayedCallback(func() { h.acquire(t, "initial", false) }, 0)
	h.env.RegisterDelayedCallback(func() { h.env.SetContinueAsNewSuggested(true) }, 3*time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(w.Touch, w.TouchInput{RunID: "run", InUse: true}) }, 4*time.Second)
	h.env.ExecuteWorkflow(sandboxlease.Run, h.in)
	can, ok := errors.AsType[*workflow.ContinueAsNewError](h.env.GetWorkflowError())
	require.True(t, ok)
	var next w.LeaseInput
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &next))
	require.Equal(t, time.Minute, next.Carry.Backoff)
	require.False(t, next.Carry.RetryAt.IsZero())
	require.Contains(t, next.Carry.InUse, "run")
}

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
	"github.com/mindreon/orbit-control/internal/workflows/agentrun"
	"github.com/mindreon/orbit-control/internal/workflows/sandboxlease"
	"github.com/mindreon/orbit-control/internal/workflows/session"
)

func TestSeparatedContextOverflowsCanBothCompact(t *testing.T) {
	h := newHarness(t)
	h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
		if in.Input.Kind == "compact" {
			r := yielded(in, true)
			r.YieldReason = new(contract.YieldReason("compacted"))
			return r, nil
		}
		if in.Seq == 1 || in.Seq == 4 {
			return contract.SegmentResult{}, temporal.NewNonRetryableApplicationError("context", contract.ErrorContextLengthExceeded, nil)
		}
		if in.Seq < 6 {
			return yielded(in, true), nil
		}
		return completed(in), nil
	}
	var compactSeqs []int
	for {
		h.env.ExecuteWorkflow(agentrun.Run, h.input)
		for _, in := range h.segments {
			if in.Input.Kind == "compact" {
				compactSeqs = append(compactSeqs, in.Seq)
			}
		}
		can, ok := errors.AsType[*workflow.ContinueAsNewError](h.env.GetWorkflowError())
		if !ok {
			require.NoError(t, h.env.GetWorkflowError())
			require.Len(t, h.finished, 1)
			require.Equal(t, "succeeded", h.finished[0].Status)
			break
		}
		var in w.RunInput
		require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &in))
		next := newHarness(t)
		next.input, next.run = in, h.run
		h = next
	}
	require.Equal(t, []int{1, 4}, compactSeqs)
}

func TestLeaseGenerationAcrossExecutions(t *testing.T) {
	first := newLeaseHarness(t, "local")
	first.gen = 1
	first.env.RegisterDelayedCallback(func() { first.acquire(t, "before-idle-exit", false) }, 0)
	first.archive(time.Second)
	first.execute(t)
	require.Equal(t, 2, first.creates[0].Metadata.LeaseGen)
	// A new session execution receives the persisted generation from AcquireSandbox.
	next := newLeaseHarness(t, "local")
	next.gen = first.records[len(first.records)-1].Gen
	next.env.RegisterDelayedCallback(func() { next.acquire(t, "new-message", false) }, 0)
	next.archive(time.Second)
	next.execute(t)
	require.Greater(t, next.creates[0].Metadata.LeaseGen, 2)
	require.Equal(t, contract.SandboxProviderLocal, next.creates[0].Provider)
}

func TestLeaseCANDrainsArchiveAndTouches(t *testing.T) {
	for _, archive := range []bool{true, false} {
		t.Run(map[bool]string{true: "archive", false: "touch"}[archive], func(t *testing.T) {
			h := newLeaseHarness(t, "local")
			h.in.Carry = &w.LeaseCarry{State: "ready", Gen: 1, Touches: 999,
				Lease:     &contract.SandboxLease{Provider: "local", SandboxId: "box"},
				Spec:      w.AcquireInput{Provider: "local", RunID: "run", SessionID: "session", Profile: "default", TTLSeconds: 1800},
				NextRenew: time.Date(2026, 10, 15, 1, 0, 0, 0, time.UTC)}
			h.env.RegisterDelayedCallback(func() {
				h.env.SignalWorkflowSkippingWorkflowTask(w.Touch, w.TouchInput{RunID: "run", InUse: false})
				if archive {
					h.env.SignalWorkflowSkippingWorkflowTask(w.Touch, w.TouchInput{RunID: "late", InUse: true})
					h.env.SignalWorkflow(w.Archive, w.ArchiveInput{})
				} else {
					h.env.SignalWorkflow(w.Touch, w.TouchInput{RunID: "late", InUse: true})
				}
			}, time.Second)
			h.env.ExecuteWorkflow(sandboxlease.Run, h.in)
			if archive {
				require.NoError(t, h.env.GetWorkflowError())
				require.Equal(t, "destroyed", h.records[len(h.records)-1].State)
			} else {
				can, ok := errors.AsType[*workflow.ContinueAsNewError](h.env.GetWorkflowError())
				require.True(t, ok)
				var in w.LeaseInput
				require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &in))
				require.Contains(t, in.Carry.InUse, "late")
			}
		})
	}
}

func TestLeaseMaintenanceRecovers(t *testing.T) {
	h := newLeaseHarness(t, "local")
	h.ttl = 4
	failure := temporal.NewNonRetryableApplicationError("provider unavailable", "fixture", nil)
	h.env.OnActivity(contract.ActivitySandboxRenew, mock.Anything, mock.Anything).Return(contract.SandboxLease{}, failure).Twice()
	h.env.OnActivity(contract.ActivitySandboxRenew, mock.Anything, mock.Anything).
		Return(contract.SandboxLease{Provider: "local", SandboxId: "sandbox"}, nil)
	h.env.RegisterDelayedCallback(func() { h.acquire(t, "initial", false) }, 0)
	h.archive(4 * time.Minute)
	h.execute(t)
	h.env.AssertExpectations(t)
	require.Equal(t, "destroyed", h.records[len(h.records)-1].State)
	// Renewal and projection must recover before archive, rather than terminating the workflow.
	require.Greater(t, len(h.records), 3)
}

func TestSessionIdleExitRetainsConcurrentSubmission(t *testing.T) {
	h := sessionHarnessWithArchiveDelay(t, time.Minute)
	h.env.RegisterDelayedCallback(func() { h.submit(t, "late", "late") }, 7*24*time.Hour+time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(w.Archive, w.ArchiveInput{}) }, 7*24*time.Hour+2*time.Minute)
	h.env.ExecuteWorkflow(session.Run, h.in)
	require.NoError(t, h.env.GetWorkflowError())
	require.Len(t, h.outputs, 1)
	require.Len(t, h.started, 1)
	require.Equal(t, "late", h.started[0].RunID)
}

func TestCancellationDuringCtlPreparation(t *testing.T) {
	for _, name := range []string{"PrepareRun", "StartSegment", "RecordSegment"} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			call := h.env.OnActivity(name, mock.Anything, mock.Anything).After(2 * time.Second)
			switch name {
			case "PrepareRun":
				call.Return(w.PrepareOutput{}, nil)
			case "RecordSegment":
				call.Return(w.RecordSegmentOutput{}, nil)
			default:
				call.Return(nil)
			}
			h.env.RegisterDelayedCallback(func() { h.env.CancelWorkflow() }, time.Second)
			h.execute(t, "cancelled", "CANCELLED")
		})
	}
}

func TestSessionValidatorErrorTypes(t *testing.T) {
	for _, mode := range []string{"QUEUE_FULL", "DUPLICATE_RUN"} {
		t.Run(mode, func(t *testing.T) {
			h := newSessionHarness(t)
			if mode == "QUEUE_FULL" || mode == "DUPLICATE_RUN" {
				h.in.Carry = &w.SessionCarry{Policy: "queue"}
				for n := range 20 {
					h.in.Carry.Queue = append(h.in.Carry.Queue, sessionMessage(string(rune('a'+n))))
				}
			}
			h.env.RegisterDelayedCallback(func() {
				if mode == "SESSION_ARCHIVED" {
					h.env.SignalWorkflow(w.Archive, w.ArchiveInput{})
				}
				msg := sessionMessage("new")
				if mode == "DUPLICATE_RUN" {
					msg = sessionMessage("b")
				}
				h.env.UpdateWorkflow(w.SubmitMessage, "probe", &testsuite.TestUpdateCallback{
					OnReject: func(err error) {
						app, ok := errors.AsType[*temporal.ApplicationError](err)
						require.True(t, ok)
						require.Equal(t, mode, app.Type())
					},
					OnComplete: func(any, error) { t.Error("invalid submission accepted") },
				}, msg)
			}, 0)
			h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(w.Archive, w.ArchiveInput{}) }, time.Second)
			h.env.ExecuteWorkflow(session.Run, h.in)
			require.NoError(t, h.env.GetWorkflowError())
		})
	}
}

func TestLeaseMaintenanceFailuresRetryWithoutLosingCleanup(t *testing.T) {
	for _, name := range []string{contract.ActivitySandboxPause, contract.ActivitySandboxSnapshot,
		contract.ActivitySandboxDestroy, "RecordSandboxState"} {
		t.Run(name, func(t *testing.T) {
			h := newLeaseHarness(t, "opensandbox")
			failure := temporal.NewNonRetryableApplicationError("maintenance unavailable", "fixture", nil)
			switch name {
			case contract.ActivitySandboxSnapshot:
				h.env.OnActivity(name, mock.Anything, mock.Anything).Return(contract.SandboxSnapshotResult{}, failure).Once()
				h.env.OnActivity(name, mock.Anything, mock.Anything).
					Return(contract.SandboxSnapshotResult{SnapshotId: "snapshot"}, nil)
			case "RecordSandboxState":
				h.env.OnActivity(name, mock.Anything, mock.MatchedBy(func(in w.SandboxStateInput) bool { return in.State == "paused" })).
					Return(failure).Once()
				h.env.OnActivity(name, mock.Anything, mock.Anything).Return(func(_ context.Context, in w.SandboxStateInput) error {
					h.records = append(h.records, in)
					return nil
				})
			default:
				h.env.OnActivity(name, mock.Anything, mock.Anything).Return(failure).Once()
				h.env.OnActivity(name, mock.Anything, mock.Anything).Return(nil)
			}
			h.env.RegisterDelayedCallback(func() { h.acquire(t, "initial", false) }, 0)
			h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(w.Touch, w.TouchInput{RunID: "run", InUse: false}) }, time.Second)
			h.archive(18 * time.Minute)
			h.execute(t)
			h.env.AssertExpectations(t)
			require.Equal(t, "destroyed", h.records[len(h.records)-1].State)
			require.Len(t, h.creates, 1)
		})
	}
}

func TestSessionArchiveValidatorHasTypedRejection(t *testing.T) {
	h := sessionHarnessWithArchiveDelay(t, time.Minute)
	rejected := false
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(w.Archive, w.ArchiveInput{}) }, 0)
	h.env.RegisterDelayedCallback(func() {
		h.env.UpdateWorkflow(w.SubmitMessage, "archived", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				app, ok := errors.AsType[*temporal.ApplicationError](err)
				require.True(t, ok)
				require.Equal(t, "SESSION_ARCHIVED", app.Type())
				rejected = true
			},
			OnComplete: func(any, error) { t.Error("archived session accepted a message") },
		}, sessionMessage("late"))
	}, time.Second)
	h.env.ExecuteWorkflow(session.Run, h.in)
	require.NoError(t, h.env.GetWorkflowError())
	require.True(t, rejected)
	require.Empty(t, h.started)
}

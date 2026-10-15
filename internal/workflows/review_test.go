package workflows_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/agentrun"
	"github.com/mindreon/orbit-control/internal/workflows/sandboxlease"
)

func TestCompactRetryStillTooLarge(t *testing.T) {
	h := newHarness(t)
	h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
		if in.Input.Kind == "compact" {
			r := yielded(in, true)
			r.YieldReason = new(contract.YieldReason("compacted"))
			return r, nil
		}
		return contract.SegmentResult{}, temporal.NewNonRetryableApplicationError(
			"context",
			contract.ErrorContextLengthExceeded,
			nil,
		)
	}
	h.execute(t, "failed", "CONTEXT_TOO_LARGE")
	require.Len(t, h.segments, 3)
}
func TestReacquisitionIDsAcrossSegments(t *testing.T) {
	h := newHarness(t)
	var attempts []int
	h.env.OnActivity("AcquireSandbox", mock.Anything, mock.Anything).Return(
		func(_ context.Context, in w.AcquireSandboxInput) (contract.SandboxLease, error) {
			attempts = append(attempts, in.Attempt)
			return contract.SandboxLease{Provider: "local", SandboxId: "box"}, nil
		})
	seen := map[int]bool{}
	h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
		if !seen[in.Seq] {
			seen[in.Seq] = true
			return contract.SegmentResult{},
				temporal.NewNonRetryableApplicationError("lost", contract.ErrorSandboxUnavailable, nil)
		}
		if in.Seq == 1 {
			return yielded(in, true), nil
		}
		return completed(in), nil
	}
	h.execute(t, "succeeded", "")
	require.Equal(t, []int{0, 1, 2}, attempts)
}
func TestCancelDuringStartSegment(t *testing.T) {
	h := newHarness(t)
	h.env.OnActivity("StartSegment", mock.Anything, mock.Anything).Return(nil).After(2 * time.Second)
	h.env.RegisterDelayedCallback(
		func() { h.env.SignalWorkflow(w.Cancel, w.CancelInput{Reason: "stop now"}) },
		time.Second,
	)
	h.execute(t, "cancelled", "CANCELLED")
	require.Empty(t, h.segments)
	require.Equal(t, "stop now", h.finished[0].Reason)
}
func TestSuspensionWhileWaiting(t *testing.T) {
	h := newHarness(t)
	h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
		return interrupted(in, "tool_approval"), nil
	}
	h.env.RegisterDelayedCallback(
		func() { h.env.SignalWorkflow(w.AssetSuspended, w.AssetSuspendedInput{AssetRef: "agent"}) },
		time.Second,
	)
	h.execute(t, "cancelled", "ASSET_SUSPENDED")
	require.Contains(t, h.notices, "cancelled")
}
func TestFailuresAlwaysFinalize(t *testing.T) {
	for _, name := range []string{"SetRunStatus", "ReconcileCheckpoint", "CreateInteraction", "ApplyDecision", "CancelInteraction"} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			failure := temporal.NewNonRetryableApplicationError("failure", "fixture", nil)
			call := h.env.OnActivity(name, mock.Anything, mock.Anything)
			switch name {
			case "CreateInteraction":
				call.Return(w.InteractionOutput{}, failure)
			case "ReconcileCheckpoint":
				call.Return(w.ReconcileOutput{}, failure)
			default:
				call.Return(failure)
			}
			switch name {
			case "SetRunStatus":
				h.input.Carry = &w.RunCarry{
					Status:      "running",
					Paused:      true,
					PendingTurn: contract.SegmentTurn{Kind: "continue"},
					Lease:       contract.SandboxLease{Provider: "local", SandboxId: "box"},
				}
			case "ReconcileCheckpoint":
				h.run = func(contract.SegmentInput) (contract.SegmentResult, error) {
					return contract.SegmentResult{}, temporal.NewNonRetryableApplicationError(
						"conflict",
						contract.ErrorCheckpointConflict,
						nil,
					)
				}
			default:
				h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
					return interrupted(in, "ask_user"), nil
				}
			}
			if name == "CancelInteraction" {
				h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(w.Cancel, w.CancelInput{}) }, time.Second)
			}
			h.env.ExecuteWorkflow(agentrun.Run, h.input)
			require.NoError(t, h.env.GetWorkflowError())
			require.Len(t, h.finished, 1)
			require.Equal(t, "failed", h.finished[0].Status)
		})
	}
}

type leaseHarness struct {
	env          *testsuite.TestWorkflowEnvironment
	in           w.LeaseInput
	calls        []string
	creates      []contract.SandboxAcquireInput
	records      []w.SandboxStateInput
	missing      bool
	renewMissing bool
	ttl          int
}

func newLeaseHarness(t *testing.T, provider contract.SandboxProvider) *leaseHarness {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	at := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	env.SetStartTime(at)
	h := &leaseHarness{env: env, ttl: 1800, in: w.LeaseInput{TenantID: "tenant", IsolationKey: "isolation"}}
	lease := contract.SandboxLease{Provider: provider, SandboxId: "sandbox", ExpiresAt: at.Add(time.Hour)}
	env.RegisterActivityWithOptions(
		func(_ context.Context, in contract.SandboxAcquireInput) (contract.SandboxLease, error) {
			h.calls = append(h.calls, "create")
			h.creates = append(h.creates, in)
			return lease, nil
		},
		activity.RegisterOptions{Name: contract.ActivitySandboxCreate},
	)
	env.RegisterActivityWithOptions(
		func(context.Context, contract.SandboxConfigureInput) (contract.SandboxLease, error) {
			h.calls = append(h.calls, "configure")
			return lease, nil
		},
		activity.RegisterOptions{Name: contract.ActivitySandboxConfigure},
	)
	env.RegisterActivityWithOptions(func(context.Context, contract.SandboxOpInput) (contract.SandboxLease, error) {
		h.calls = append(h.calls, "resume")
		return lease, nil
	}, activity.RegisterOptions{Name: contract.ActivitySandboxResume})
	env.RegisterActivityWithOptions(
		func(context.Context, contract.SandboxOpInput) (contract.SandboxInspectResult, error) {
			h.calls = append(h.calls, "inspect")
			return contract.SandboxInspectResult{Exists: !h.missing}, nil
		},
		activity.RegisterOptions{Name: contract.ActivitySandboxInspect},
	)
	env.RegisterActivityWithOptions(func(context.Context, contract.SandboxRenewInput) (contract.SandboxLease, error) {
		h.calls = append(h.calls, "renew")
		if h.renewMissing {
			return lease, temporal.NewNonRetryableApplicationError("missing", "NotFound", nil)
		}
		return lease, nil
	}, activity.RegisterOptions{Name: contract.ActivitySandboxRenew})
	env.RegisterActivityWithOptions(
		func(context.Context, contract.SandboxOpInput) (contract.SandboxSnapshotResult, error) {
			h.calls = append(h.calls, "snapshot")
			return contract.SandboxSnapshotResult{SnapshotId: "snapshot"}, nil
		},
		activity.RegisterOptions{Name: contract.ActivitySandboxSnapshot},
	)
	for _, name := range []string{contract.ActivitySandboxPause, contract.ActivitySandboxDestroy} {
		env.RegisterActivityWithOptions(func(context.Context, contract.SandboxOpInput) error {
			h.calls = append(h.calls, name)
			return nil
		},
			activity.RegisterOptions{Name: name})
	}
	env.RegisterActivityWithOptions(func(context.Context, contract.SandboxSnapshotDeleteInput) error {
		h.calls = append(h.calls, "delete")
		return nil
	}, activity.RegisterOptions{Name: contract.ActivitySandboxSnapshotDelete})
	env.RegisterActivityWithOptions(func(_ context.Context, in w.SandboxStateInput) error {
		h.records = append(h.records, in)
		return nil
	}, activity.RegisterOptions{Name: "RecordSandboxState"})
	return h
}
func (h *leaseHarness) acquire(t *testing.T, id string, reacquire bool) {
	t.Helper()
	h.env.UpdateWorkflow(w.Acquire, id, &testsuite.TestUpdateCallback{
		OnReject: func(err error) { require.NoError(t, err) }, OnComplete: func(_ any, err error) { require.NoError(t, err) },
	}, w.AcquireInput{RunID: "run", SessionID: "session", Profile: "default", TTLSeconds: h.ttl, SpecDigest: "digest", Reacquire: reacquire})
}
func (h *leaseHarness) archive(after time.Duration) {
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(w.Archive, w.ArchiveInput{}) }, after)
}
func (h *leaseHarness) execute(t *testing.T) {
	t.Helper()
	h.env.ExecuteWorkflow(sandboxlease.Run, h.in)
	require.NoError(t, h.env.GetWorkflowError())
}
func TestLeaseConcurrentAcquire(t *testing.T) {
	h := newLeaseHarness(t, "local")
	h.env.OnActivity(contract.ActivitySandboxCreate, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in contract.SandboxAcquireInput) (contract.SandboxLease, error) {
			h.creates = append(h.creates, in)
			return contract.SandboxLease{Provider: "local", SandboxId: "box"}, nil
		}).After(time.Second).Once()
	h.env.RegisterDelayedCallback(func() {
		h.acquire(t, "one", false)
		h.acquire(t, "two", false)
	}, 0)
	h.archive(1500 * time.Millisecond)
	h.execute(t)
	require.Len(t, h.creates, 1)
	require.Equal(t, []string{"configure", "inspect", contract.ActivitySandboxDestroy}, h.calls)
}
func TestLeaseRenewAndReacquire(t *testing.T) {
	h := newLeaseHarness(t, "local")
	h.renewMissing = true
	h.ttl = 4
	h.env.RegisterDelayedCallback(func() { h.acquire(t, "one", false) }, 0)
	h.env.RegisterDelayedCallback(func() { h.acquire(t, "two", true) }, 3*time.Second)
	h.archive(4 * time.Second)
	h.execute(t)
	require.Equal(
		t,
		[]string{"create", "configure", "renew", "inspect", "create", "configure", contract.ActivitySandboxDestroy},
		h.calls,
	)
	require.Equal(t, 2, h.creates[1].Metadata.LeaseGen)
	require.True(
		t,
		slices.ContainsFunc(h.records, func(r w.SandboxStateInput) bool { return r.Restored != nil && !*r.Restored }),
	)
}
func TestLeaseStaleInUseAndRestore(t *testing.T) {
	h := newLeaseHarness(t, "opensandbox")
	h.env.RegisterDelayedCallback(func() { h.acquire(t, "one", false) }, 0)
	h.env.RegisterDelayedCallback(func() {
		require.Contains(t, h.calls, "snapshot")
		h.acquire(t, "two", true)
	}, 31*time.Minute)
	h.archive(31*time.Minute + time.Second)
	h.execute(t)
	require.NotContains(t, h.calls[:2], contract.ActivitySandboxPause)
	require.Len(t, h.creates, 2)
	require.Equal(t, "snapshot", *h.creates[1].SnapshotId)
	require.Contains(t, h.calls, "delete")
}
func TestLeaseContinueAfterTouches(t *testing.T) {
	h := newLeaseHarness(t, "local")
	sent := 0
	var send func()
	send = func() {
		h.env.SignalWorkflow(w.Touch, w.TouchInput{RunID: "run", InUse: true})
		sent++
		if sent < 1000 {
			h.env.RegisterDelayedCallback(send, time.Millisecond)
		}
	}
	h.env.RegisterDelayedCallback(send, 0)
	h.env.ExecuteWorkflow(sandboxlease.Run, h.in)
	var can *workflow.ContinueAsNewError
	require.True(t, errors.As(h.env.GetWorkflowError(), &can))
	require.Equal(t, 1000, sent)
}

func TestLeaseResumeAndLostSnapshot(t *testing.T) {
	for _, mode := range []string{"resume", "missing"} {
		t.Run(mode, func(t *testing.T) {
			h := newLeaseHarness(t, "opensandbox")
			at := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
			h.in.Carry = &w.LeaseCarry{State: "paused", Gen: 1, SnapshotID: "snapshot", LastTouch: at,
				Lease: &contract.SandboxLease{Provider: "opensandbox", SandboxId: "old", ExpiresAt: at.Add(time.Hour)},
				Spec:  w.AcquireInput{RunID: "run", SessionID: "session", Profile: "default", TTLSeconds: 1800}}
			h.missing = mode == "missing"
			h.env.RegisterDelayedCallback(func() { h.acquire(t, "restore", mode == "missing") }, 0)
			h.archive(time.Second)
			h.execute(t)
			if mode == "resume" {
				require.Contains(t, h.calls, "resume")
				require.Empty(t, h.creates)
			} else {
				require.Len(t, h.creates, 1)
				require.Equal(t, "snapshot", *h.creates[0].SnapshotId)
				require.True(t, slices.ContainsFunc(h.records, func(r w.SandboxStateInput) bool { return r.Restored != nil && *r.Restored }))
			}
		})
	}
}
func TestLeaseSuggestedContinue(t *testing.T) {
	h := newLeaseHarness(t, "local")
	h.env.SetContinueAsNewSuggested(true)
	h.env.ExecuteWorkflow(sandboxlease.Run, h.in)
	require.True(t, workflow.IsContinueAsNewError(h.env.GetWorkflowError()))
}
func TestRunCancellationDuringInteractionAwait(t *testing.T) {
	h := newHarness(t)
	h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
		return interrupted(in, "ask_user"), nil
	}
	h.env.RegisterDelayedCallback(func() { h.env.CancelWorkflow() }, time.Second)
	h.execute(t, "cancelled", "CANCELLED")
}
func TestRunTouchOrderAndFailureCleanup(t *testing.T) {
	h := newHarness(t)
	h.execute(t, "succeeded", "")
	require.Equal(t, []bool{true, false}, h.touches)
	for _, name := range []string{"PrepareRun", "AcquireSandbox", "StartSegment", "RecordSegment"} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			failure := temporal.NewNonRetryableApplicationError("broken", "failure", nil)
			call := h.env.OnActivity(name, mock.Anything, mock.Anything)
			switch name {
			case "PrepareRun":
				call.Return(w.PrepareOutput{}, failure)
			case "AcquireSandbox":
				call.Return(contract.SandboxLease{}, failure)
			case "RecordSegment":
				call.Return(w.RecordSegmentOutput{}, failure)
			default:
				call.Return(failure)
			}
			h.env.ExecuteWorkflow(agentrun.Run, h.input)
			require.NoError(t, h.env.GetWorkflowError())
			require.Len(t, h.finished, 1)
			require.Equal(t, "failed", h.finished[0].Status)
		})
	}
}

func TestRunContinuesWithCompleteCarry(t *testing.T) {
	h := newHarness(t)
	h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) { return yielded(in, true), nil }
	h.env.ExecuteWorkflow(agentrun.Run, h.input)
	can, ok := errors.AsType[*workflow.ContinueAsNewError](h.env.GetWorkflowError())
	require.True(t, ok)
	var in w.RunInput
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &in))
	require.Equal(t, w.RunContinueAfter, in.Carry.CommittedSeq)
	require.Equal(t, w.RunContinueAfter, in.Carry.CheckpointVersion)
	require.Equal(t, 1, in.Carry.Acquisitions)
	require.Equal(t, "box", in.Carry.Lease.SandboxId)
	require.Equal(t, contract.SegmentTurnKind("continue"), in.Carry.PendingTurn.Kind)
	require.Greater(t, in.Carry.EventN, int64(in.Carry.CommittedSeq))
	require.Empty(t, h.finished)
}
func TestPendingInteractionPreventsContinue(t *testing.T) {
	h := newHarness(t)
	h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
		return interrupted(in, "ask_user"), nil
	}
	h.env.RegisterDelayedCallback(func() { h.env.SetContinueAsNewSuggested(true) }, time.Second)
	h.env.RegisterDelayedCallback(func() {
		value, err := h.env.QueryWorkflow(w.Progress)
		require.NoError(t, err)
		var progress w.RunProgress
		require.NoError(t, value.Get(&progress))
		require.Equal(t, "waiting_input", progress.Status)
		h.env.UpdateWorkflow(w.ResolveInteraction, "answer", &testsuite.TestUpdateCallback{
			OnReject:   func(err error) { require.NoError(t, err) },
			OnComplete: func(_ any, err error) { require.NoError(t, err) },
		}, w.ResolveInteractionInput{InteractionID: "approval", Decision: contract.Decision{Kind: "ask_user", Answer: new("yes")}})
	}, 2*time.Second)
	h.env.ExecuteWorkflow(agentrun.Run, h.input)
	require.True(t, workflow.IsContinueAsNewError(h.env.GetWorkflowError()))
	require.Len(t, h.decisions, 1)
	require.Len(t, h.records, 1)
	require.Empty(t, h.finished)
}
func TestResolveRejectsExpiredAndTerminal(t *testing.T) {
	for _, state := range []string{"expired", "terminal"} {
		t.Run(state, func(t *testing.T) {
			h := newHarness(t)
			h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
				return interrupted(in, "tool_approval"), nil
			}
			h.env.OnActivity("ApplyDecision", mock.Anything, mock.Anything).Return(nil).After(5 * time.Second)
			h.env.OnActivity("FinalizeRun", mock.Anything, mock.Anything).Return(nil).After(5 * time.Second)
			at := 72*time.Hour + time.Second
			if state == "terminal" {
				at = 2 * time.Second
				h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(w.Cancel, w.CancelInput{}) }, time.Second)
			}
			rejected := 0
			h.env.RegisterDelayedCallback(func() {
				h.env.UpdateWorkflow(w.ResolveInteraction, "late", &testsuite.TestUpdateCallback{
					OnReject: func(err error) {
						require.ErrorContains(t, err, "INTERACTION_NOT_PENDING")
						rejected++
					},
					OnAccept: func() { t.Error("validator accepted a closed interaction") },
				}, w.ResolveInteractionInput{InteractionID: "approval", Decision: contract.Decision{Kind: "tool_approval"}})
			}, at)
			if state == "expired" {
				h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(w.Cancel, w.CancelInput{}) }, at+time.Second)
			}
			h.env.ExecuteWorkflow(agentrun.Run, h.input)
			require.NoError(t, h.env.GetWorkflowError())
			require.Equal(t, 1, rejected)
		})
	}
}
func TestTwoSteersDuringSegment(t *testing.T) {
	h := newHarness(t)
	h.env.OnActivity(contract.ActivityRunAgentSegment, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in contract.SegmentInput) (contract.SegmentResult, error) {
			h.segments = append(h.segments, in)
			return yielded(in, true), nil
		}).After(2 * time.Second).Once()
	h.env.OnActivity(contract.ActivityRunAgentSegment, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in contract.SegmentInput) (contract.SegmentResult, error) {
			h.segments = append(h.segments, in)
			return completed(in), nil
		})
	h.env.RegisterDelayedCallback(func() {
		for _, id := range []string{"one", "two"} {
			h.env.UpdateWorkflow(w.Steer, id, &testsuite.TestUpdateCallback{
				OnReject:   func(err error) { require.NoError(t, err) },
				OnComplete: func(_ any, err error) { require.NoError(t, err) },
			}, w.SteerInput{MessageID: id, ContentRef: "t/tenant/runs/run/steer/" + id})
		}
	}, time.Second)
	h.execute(t, "succeeded", "")
	require.Len(t, h.segments, 2)
	require.Len(t, h.segments[1].Steer, 2)
	require.Equal(t, "t/tenant/runs/run/steer/one", *h.segments[1].Steer[0].ContentRef)
	require.Equal(t, "t/tenant/runs/run/steer/two", *h.segments[1].Steer[1].ContentRef)
}
func TestActivityOrder(t *testing.T) {
	h := newHarness(t)
	var calls []string
	h.env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		calls = append(calls, info.ActivityType.Name)
	})
	h.execute(t, "succeeded", "")
	require.Equal(t, []string{"PrepareRun", "AcquireSandbox", "StartSegment", contract.ActivityRunAgentSegment,
		"RecordSegment", "FinalizeRun"}, calls)
}

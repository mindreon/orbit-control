package workflows_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"errors"
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
	"github.com/mindreon/orbit-control/internal/workflows/session"
)

type harness struct {
	env       *testsuite.TestWorkflowEnvironment
	input     w.RunInput
	segments  []contract.SegmentInput
	records   []w.RecordSegmentInput
	finished  []w.FinalizeInput
	statuses  []string
	notices   []string
	decisions []w.DecisionInput
	touches   []bool
	acquires  int
	used      int64
	reconcile *contract.SegmentResult
	run       func(contract.SegmentInput) (contract.SegmentResult, error)
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	h := &harness{
		env: env,
		input: w.RunInput{
			V:                  1,
			TenantID:           "tenant",
			WorkspaceID:        "workspace",
			SessionID:          "session",
			RunID:              "run",
			PrincipalID:        "actor",
			MessageID:          "message",
			Member:             "main",
			Lock:               contract.SegmentLockRef{Id: "lock", Digest: "sha256:" + strings.Repeat("0", 64)},
			AgentSessionKey:    "sess:session:000000000000",
			InputRef:           "t/tenant/runs/run/input/message.json",
			EventSubject:       "run.tenant.run",
			BudgetMillicredits: 200000,
			Limits:             w.DefaultLimits(200000),
			Sandbox:            w.SandboxSpec{IsolationKey: "t-tenant-s-session", Profile: "default", TTLSeconds: 1800},
			Tier:               "standard",
		},
	}
	env.RegisterActivityWithOptions(
		func(context.Context, w.PrepareInput) (w.PrepareOutput, error) {
			return w.PrepareOutput{}, nil
		},
		activity.RegisterOptions{Name: "PrepareRun"},
	)
	env.RegisterActivityWithOptions(func(context.Context, w.AcquireSandboxInput) (contract.SandboxLease, error) {
		h.acquires++
		return contract.SandboxLease{Provider: "local", SandboxId: "box"}, nil
	}, activity.RegisterOptions{Name: "AcquireSandbox"})
	env.RegisterActivityWithOptions(
		func(context.Context, w.StartSegmentInput) error {
			return nil
		},
		activity.RegisterOptions{Name: "StartSegment"},
	)
	env.RegisterActivityWithOptions(func(_ context.Context, in contract.SegmentInput) (contract.SegmentResult, error) {
		h.segments = append(h.segments, in)
		if h.run != nil {
			return h.run(in)
		}
		return completed(in), nil
	}, activity.RegisterOptions{Name: contract.ActivityRunAgentSegment})
	env.RegisterActivityWithOptions(func(_ context.Context, in w.RecordSegmentInput) (w.RecordSegmentOutput, error) {
		h.records = append(h.records, in)
		return w.RecordSegmentOutput{RunUsedMillicredits: h.used}, nil
	}, activity.RegisterOptions{Name: "RecordSegment"})
	env.RegisterActivityWithOptions(
		func(_ context.Context, in w.FinalizeInput) error {
			h.finished = append(h.finished, in)
			return nil
		},
		activity.RegisterOptions{Name: "FinalizeRun"},
	)
	env.RegisterActivityWithOptions(func(_ context.Context, in w.StatusInput) error {
		h.statuses = append(h.statuses, in.Status)
		return nil
	}, activity.RegisterOptions{Name: "SetRunStatus"})
	env.RegisterActivityWithOptions(func(_ context.Context, in w.InteractionInput) (w.InteractionOutput, error) {
		return w.InteractionOutput{CreatedAt: in.EventAt, Approvers: []string{"owner"}}, nil
	}, activity.RegisterOptions{Name: "CreateInteraction"})
	env.RegisterActivityWithOptions(
		func(_ context.Context, in w.NotifyInput) error {
			h.notices = append(h.notices, in.Kind)
			return nil
		},
		activity.RegisterOptions{Name: "NotifyInteraction"},
	)
	env.RegisterActivityWithOptions(func(_ context.Context, in w.NotifyInput) error {
		h.notices = append(h.notices, "cancelled")
		return nil
	}, activity.RegisterOptions{Name: "CancelInteraction"})
	env.RegisterActivityWithOptions(
		func(_ context.Context, in w.DecisionInput) error {
			h.decisions = append(h.decisions, in)
			return nil
		},
		activity.RegisterOptions{Name: "ApplyDecision"},
	)
	env.RegisterActivityWithOptions(func(context.Context, w.ReconcileInput) (w.ReconcileOutput, error) {
		return w.ReconcileOutput{Result: h.reconcile}, nil
	}, activity.RegisterOptions{Name: "ReconcileCheckpoint"})
	env.OnSignalExternalWorkflow(
		mock.Anything,
		w.LeaseWorkflowID("tenant", "t-tenant-s-session"),
		"",
		w.Touch,
		mock.Anything,
	).Run(func(args mock.Arguments) { h.touches = append(h.touches, args.Get(4).(w.TouchInput).InUse) }).Return(nil)
	return h
}
func completed(in contract.SegmentInput) contract.SegmentResult {
	return contract.SegmentResult{
		V:                 1,
		Status:            "completed",
		CheckpointVersion: in.ExpectedCheckpointVersion + 1,
		Progress:          contract.SegmentProgress{NewOutput: true},
	}
}
func yielded(in contract.SegmentInput, progress bool) contract.SegmentResult {
	r := completed(in)
	r.Status = "yielded"
	r.Progress.NewOutput = progress
	r.YieldReason = new(contract.YieldReason("iteration_cap"))
	return r
}
func interrupted(in contract.SegmentInput, kind contract.InteractionKind) contract.SegmentResult {
	r := completed(in)
	r.Status = "interrupted"
	r.Interaction = &contract.InteractionRequest{InteractionId: "approval", Kind: kind}
	return r
}
func (h *harness) execute(t *testing.T, status, code string) {
	t.Helper()
	h.env.ExecuteWorkflow(agentrun.Run, h.input)
	require.NoError(t, h.env.GetWorkflowError())
	require.Len(t, h.finished, 1)
	require.Equal(t, status, h.finished[0].Status)
	require.Equal(t, code, h.finished[0].ErrorCode)
}
func TestAgentSegments(t *testing.T) {
	t.Run("completed", func(t *testing.T) {
		h := newHarness(t)
		h.execute(t, "succeeded", "")
		require.Len(t, h.records, 1)
		require.Equal(t, 1, h.segments[0].Seq)
		require.Equal(t, "user_message", string(h.segments[0].Input.Kind))
	})
	t.Run("yield three segments", func(t *testing.T) {
		h := newHarness(t)
		h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
			if in.Seq < 3 {
				return yielded(in, true), nil
			}
			return completed(in), nil
		}
		h.execute(t, "succeeded", "")
		for i, in := range h.segments {
			require.Equal(t, i+1, in.Seq)
			require.Equal(t, i, in.ExpectedCheckpointVersion)
		}
		require.Equal(t, "continue", string(h.segments[1].Input.Kind))
	})
	t.Run("stuck", func(t *testing.T) {
		h := newHarness(t)
		h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
			return yielded(in, false), nil
		}
		h.execute(t, "failed", "STUCK")
		require.Len(t, h.segments, 3)
	})
	t.Run("progress resets", func(t *testing.T) {
		h := newHarness(t)
		h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
			if in.Seq == 6 {
				return completed(in), nil
			}
			return yielded(in, in.Seq == 3), nil
		}
		h.env.ExecuteWorkflow(agentrun.Run, h.input)
		if can, ok := errors.AsType[*workflow.ContinueAsNewError](h.env.GetWorkflowError()); ok {
			var in w.RunInput
			require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &in))
			require.Zero(t, in.Carry.NoProgress)
			next := newHarness(t)
			next.input, next.run = in, h.run
			next.execute(t, "succeeded", "")
		} else {
			require.NoError(t, h.env.GetWorkflowError())
			require.Equal(t, "succeeded", h.finished[0].Status)
		}
	})
	for _, tc := range []struct{ typ, code, status string }{
		{typ: contract.ErrorBudgetExhausted, code: "", status: "budget_exhausted"},
		{typ: contract.ErrorPolicyDenied, code: "POLICY_DENIED", status: "failed"},
		{typ: contract.ErrorInvalidSpec, code: "INVALID_SPEC", status: "failed"},
		{typ: contract.ErrorInvalidSegmentInput, code: "INVALID_SEGMENT_INPUT", status: "failed"},
		{typ: contract.ErrorLockVerificationFailed, code: "LOCK_VERIFICATION_FAILED", status: "failed"},
		{typ: contract.ErrorCheckpointUnreadable, code: "CHECKPOINT_UNREADABLE", status: "failed"},
		{typ: contract.ErrorSegmentSuperseded, code: "SEGMENT_SUPERSEDED", status: "failed"},
		{typ: "other", code: "SEGMENT_FAILED", status: "failed"},
	} {
		t.Run(tc.typ, func(t *testing.T) {
			h := newHarness(t)
			h.run = func(contract.SegmentInput) (contract.SegmentResult, error) {
				return contract.SegmentResult{}, temporal.NewNonRetryableApplicationError("fixture", tc.typ, nil)
			}
			h.execute(t, tc.status, tc.code)
		})
	}
	for _, has := range []bool{true, false} {
		t.Run(
			fmt.Sprintf("checkpoint reconcile %t", has),
			func(t *testing.T) {
				h := newHarness(t)
				if has {
					r := completed(contract.SegmentInput{})
					h.reconcile = &r
				}
				h.run = func(contract.SegmentInput) (contract.SegmentResult, error) {
					return contract.SegmentResult{}, temporal.NewNonRetryableApplicationError(
						"conflict",
						contract.ErrorCheckpointConflict,
						nil,
					)
				}
				if has {
					h.execute(t, "succeeded", "")
				} else {
					h.execute(t, "failed", "CHECKPOINT_CONFLICT")
				}
			},
		)
	}
	for _, fails := range []int{1, 3} {
		t.Run(
			fmt.Sprintf("sandbox retry %d", fails),
			func(t *testing.T) {
				h := newHarness(t)
				attempt := 0
				h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
					attempt++
					if attempt <= fails {
						return contract.SegmentResult{}, temporal.NewNonRetryableApplicationError(
							"lost",
							contract.ErrorSandboxUnavailable,
							nil,
						)
					}
					return completed(in), nil
				}
				if fails == 1 {
					h.execute(t, "succeeded", "")
					require.Equal(t, 2, h.acquires)
					require.Equal(t, h.segments[0].Seq, h.segments[1].Seq)
				} else {
					h.execute(t, "failed", "SANDBOX_UNAVAILABLE")
				}
			},
		)
	}
	for _, repeat := range []bool{false, true} {
		t.Run(
			fmt.Sprintf("compact %t", repeat),
			func(t *testing.T) {
				h := newHarness(t)
				calls := 0
				h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
					calls++
					if calls == 1 || (repeat && calls == 2) {
						return contract.SegmentResult{}, temporal.NewNonRetryableApplicationError(
							"context",
							contract.ErrorContextLengthExceeded,
							nil,
						)
					}
					if in.Input.Kind == "compact" {
						r := yielded(in, true)
						r.YieldReason = new(contract.YieldReason("compacted"))
						return r, nil
					}
					return completed(in), nil
				}
				if repeat {
					h.execute(t, "failed", "CONTEXT_TOO_LARGE")
				} else {
					h.execute(t, "succeeded", "")
					require.Equal(
						t,
						[]int{1, 1, 2},
						[]int{h.segments[0].Seq, h.segments[1].Seq, h.segments[2].Seq},
					)
					require.Equal(t, h.segments[0].Input, h.segments[2].Input)
				}
			},
		)
	}
	t.Run("budget warning and exhausted", func(t *testing.T) {
		h := newHarness(t)
		h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
			if in.Seq == 1 {
				h.used = 160000
			} else {
				require.Equal(t, contract.BudgetNotice("soft_warning"), in.BudgetNotice)
				h.used = 200000
			}
			return yielded(in, true), nil
		}
		h.execute(t, "budget_exhausted", "")
		require.Len(t, h.segments, 2)
	})
	t.Run("segment limit", func(t *testing.T) {
		h := newHarness(t)
		h.input.Limits.MaxSegments = 1
		h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
			return yielded(in, true), nil
		}
		h.execute(t, "failed", "SEGMENT_LIMIT")
	})
}
func TestInteractions(t *testing.T) {
	for _, kind := range []contract.InteractionKind{"tool_approval", "ask_user", "effect_uncertain"} {
		for _, expire := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s expire=%t", kind, expire), func(t *testing.T) {
				h := newHarness(t)
				h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
					if in.Seq == 1 {
						return interrupted(in, kind), nil
					}
					require.Equal(t, contract.SegmentTurnKind("interactive_input"), in.Input.Kind)
					require.Equal(t, kind, in.Input.Decision.Kind)
					return completed(in), nil
				}
				if !expire {
					h.env.RegisterDelayedCallback(func() {
						d := contract.Decision{Kind: kind}
						if kind == "tool_approval" {
							d.Choice = new(contract.DecisionChoice("allow_once"))
						} else if kind == "effect_uncertain" {
							d.Choice = new(contract.DecisionChoice("abandon"))
						} else {
							d.Answer = new("answer")
						}
						h.env.UpdateWorkflow(
							w.ResolveInteraction,
							"resolve",
							&testsuite.TestUpdateCallback{OnComplete: func(_ any, err error) {
								require.NoError(t, err)
							}},
							w.ResolveInteractionInput{InteractionID: "approval", ActorID: "owner", Decision: d},
						)
					}, time.Second)
				}
				h.execute(t, "succeeded", "")
				require.Len(t, h.decisions, 1)
				require.Equal(t, expire, h.decisions[0].Expired)
				if expire {
					require.Equal(t, []string{"remind", "escalate"}, h.notices)
				}
				require.Zero(t, h.finished[0].NeverStarted)
			})
		}
	}
	t.Run("validator and cancellation waiting", func(t *testing.T) {
		h := newHarness(t)
		h.run = func(in contract.SegmentInput) (contract.SegmentResult, error) {
			return interrupted(in, "tool_approval"), nil
		}
		rejects := 0
		h.env.RegisterDelayedCallback(func() {
			for _, in := range []w.ResolveInteractionInput{
				{InteractionID: "wrong", Decision: contract.Decision{Kind: "tool_approval"}},
				{InteractionID: "approval", Decision: contract.Decision{Kind: "ask_user"}},
			} {
				h.env.UpdateWorkflow(
					w.ResolveInteraction,
					in.InteractionID+string(in.Decision.Kind),
					&testsuite.TestUpdateCallback{OnReject: func(err error) {
						require.Error(t, err)
						rejects++
					}},
					in,
				)
			}
			h.env.SignalWorkflow(w.Cancel, w.CancelInput{Reason: "user"})
		}, time.Second)
		h.execute(t, "cancelled", "CANCELLED")
		require.Equal(t, 2, rejects)
		require.Contains(t, h.notices, "cancelled")
	})
}
func TestControls(t *testing.T) {
	for _, signal := range []string{w.Cancel, w.AssetSuspended} {
		t.Run(signal, func(t *testing.T) {
			h := newHarness(t)
			h.env.OnActivity(contract.ActivityRunAgentSegment, mock.Anything, mock.Anything).Return(
				contract.SegmentResult{},
				temporal.NewCanceledError(),
			).After(
				10 * time.Second,
			)
			h.env.RegisterDelayedCallback(func() {
				if signal == w.Cancel {
					h.env.SignalWorkflow(signal, w.CancelInput{Reason: "user_cancel"})
				} else {
					h.env.SignalWorkflow(signal, w.AssetSuspendedInput{AssetRef: "agent"})
				}
			}, time.Second)
			code := "CANCELLED"
			if signal == w.AssetSuspended {
				code = "ASSET_SUSPENDED"
			}
			h.execute(t, "cancelled", code)
		})
	}
	t.Run("pause after segment resume", func(t *testing.T) {
		h := newHarness(t)
		h.env.OnActivity(contract.ActivityRunAgentSegment, mock.Anything, mock.Anything).
			Return(yielded(contract.SegmentInput{}, true), nil).After(2 * time.Second).Once()
		h.env.OnActivity(contract.ActivityRunAgentSegment, mock.Anything, mock.Anything).
			Return(func(_ context.Context, in contract.SegmentInput) (contract.SegmentResult, error) {
				h.segments = append(h.segments, in)
				return completed(in), nil
			})
		h.env.RegisterDelayedCallback(
			func() {
				h.env.SignalWorkflow(w.Pause, w.ActorInput{})
			},
			time.Second,
		)
		h.env.RegisterDelayedCallback(
			func() {
				require.Contains(t, h.statuses, "paused")
				h.env.SignalWorkflow(w.Resume, w.ActorInput{})
			},
			3*time.Second,
		)
		h.execute(t, "succeeded", "")
		require.Equal(t, []string{"paused", "running"}, h.statuses)
	})
	t.Run("steer follow up and bounded queue", func(t *testing.T) {
		h := newHarness(t)
		h.env.OnActivity(contract.ActivityRunAgentSegment, mock.Anything, mock.Anything).
			Return(func(_ context.Context, in contract.SegmentInput) (contract.SegmentResult, error) {
				h.segments = append(h.segments, in)
				return completed(in), nil
			}).After(2 * time.Second).Once()
		h.env.OnActivity(contract.ActivityRunAgentSegment, mock.Anything, mock.Anything).
			Return(func(_ context.Context, in contract.SegmentInput) (contract.SegmentResult, error) {
				h.segments = append(h.segments, in)
				return completed(in), nil
			})
		rejected := 0
		h.env.RegisterDelayedCallback(func() {
			for i := range 21 {
				h.env.UpdateWorkflow(
					w.Steer,
					fmt.Sprint(i),
					&testsuite.TestUpdateCallback{OnReject: func(err error) {
						rejected++
					}},
					w.SteerInput{
						MessageID:  fmt.Sprint(i),
						ContentRef: fmt.Sprintf("t/tenant/runs/run/steer/%d", i),
					},
				)
			}
		}, time.Second)
		h.execute(t, "succeeded", "")
		require.Equal(t, 1, rejected)
		require.Len(t, h.segments, 2)
		require.Len(t, h.segments[1].Steer, 20)
		require.Equal(t, contract.SegmentTurnKind("follow_up"), h.segments[1].Input.Kind)
	})
	t.Run("active time limit", func(t *testing.T) {
		h := newHarness(t)
		h.input.Limits.MaxActiveSeconds = 1
		h.env.OnActivity(contract.ActivityRunAgentSegment, mock.Anything, mock.Anything).Return(
			yielded(contract.SegmentInput{}, true),
			nil,
		).After(
			2 * time.Second,
		)
		h.execute(t, "timed_out", "")
	})
	t.Run("continue as new carry", func(t *testing.T) {
		h := newHarness(t)
		h.input.Carry = &w.RunCarry{
			Status:            "running",
			CommittedSeq:      200,
			CheckpointVersion: 203,
			UsedMillicredits:  12,
			PendingTurn:       contract.SegmentTurn{Kind: "continue"},
		}
		h.env.ExecuteWorkflow(agentrun.Run, h.input)
		require.Empty(t, h.finished)
	})
}
func TestSessionQueue(t *testing.T) {
	for _, mode := range []string{"queue", "interrupt", "cancel_queued", "archive", "full", "idle"} {
		t.Run(
			mode,
			func(t *testing.T) {
				var suite testsuite.WorkflowTestSuite
				env := suite.NewTestWorkflowEnvironment()
				env.RegisterWorkflowWithOptions(
					func(ctx workflow.Context, in w.RunInput) error {
						return workflow.Sleep(ctx, 10*time.Second)
					},
					workflow.RegisterOptions{Name: w.AgentRunWorkflow},
				)
				var finished []w.FinalizeInput
				env.RegisterActivityWithOptions(
					func(_ context.Context, in w.FinalizeInput) error {
						finished = append(finished, in)
						return nil
					},
					activity.RegisterOptions{Name: "FinalizeRun"},
				)
				env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", mock.Anything, mock.Anything).Return(
					nil,
				)
				if mode != "idle" {
					env.RegisterDelayedCallback(func() {
						msg := w.SubmitMessageInput{
							V:     1,
							RunID: "one",
							Lock:  contract.SegmentLockRef{Id: "lock", Digest: "sha256:" + strings.Repeat("0", 64)},
						}
						env.UpdateWorkflow(
							w.SubmitMessage,
							"one",
							&testsuite.TestUpdateCallback{OnComplete: func(v any, e error) {
								require.NoError(t, e)
								require.Equal(t, "started", v.(w.SubmitMessageOutput).Disposition)
							}},
							msg,
						)
					}, 0)
					env.RegisterDelayedCallback(
						func() {
							n := 1
							if mode == "full" {
								n = 21
							}
							for i := range n {
								env.UpdateWorkflow(
									w.SubmitMessage,
									fmt.Sprintf("second-%d", i),
									&testsuite.TestUpdateCallback{OnReject: func(
										err error,
									) {
										require.Equal(t, "full", mode)
									}, OnComplete: func(v any, e error) {
										require.NoError(t, e)
										if mode == "queue" {
											require.Equal(t, 1, v.(w.SubmitMessageOutput).Position)
										}
									}},
									w.SubmitMessageInput{
										V:     1,
										RunID: fmt.Sprintf("second-%d", i),
										Lock: contract.SegmentLockRef{
											Id:     "lock",
											Digest: "sha256:" + strings.Repeat("0", 64),
										},
									},
								)
							}
						},
						time.Second,
					)
					if mode == "cancel_queued" {
						env.RegisterDelayedCallback(func() {
							env.UpdateWorkflow(
								w.CancelRun,
								"cancel",
								&testsuite.TestUpdateCallback{OnComplete: func(v any, e error) {
									require.NoError(t, e)
									require.Equal(t, "dequeued", v.(w.CancelRunOutput).Disposition)
								}},
								w.CancelRunInput{RunID: "second-0"},
							)
						}, 2*time.Second)
					}
					if mode == "archive" || mode == "full" {
						env.RegisterDelayedCallback(
							func() {
								env.SignalWorkflow(w.Archive, w.ArchiveInput{})
							},
							2*time.Second,
						)
					}
				}
				policy := "queue"
				if mode == "interrupt" {
					policy = "interrupt"
				}
				env.ExecuteWorkflow(
					session.Run,
					w.SessionInput{V: 1, TenantID: "tenant", SessionID: "session", Policy: policy},
				)
				require.NoError(t, env.GetWorkflowError())
				if mode == "cancel_queued" || mode == "archive" {
					require.NotEmpty(t, finished)
					require.True(t, finished[0].NeverStarted)
				}
			},
		)
	}
}
func TestLeaseLifecycle(t *testing.T) {
	for _, provider := range []contract.SandboxProvider{"local", "opensandbox"} {
		t.Run(
			string(provider),
			func(t *testing.T) {
				var suite testsuite.WorkflowTestSuite
				env := suite.NewTestWorkflowEnvironment()
				calls := []string{}
				lease := contract.SandboxLease{
					Provider:  provider,
					SandboxId: "sandbox",
					ExpiresAt: time.Now().Add(time.Hour),
				}
				env.RegisterActivityWithOptions(
					func(context.Context, contract.SandboxAcquireInput) (
						contract.SandboxLease,
						error,
					) {
						calls = append(calls, "create")
						return lease, nil
					},
					activity.RegisterOptions{Name: contract.ActivitySandboxCreate},
				)
				env.RegisterActivityWithOptions(
					func(context.Context, contract.SandboxConfigureInput) (
						contract.SandboxLease,
						error,
					) {
						calls = append(calls, "configure")
						return lease, nil
					},
					activity.RegisterOptions{Name: contract.ActivitySandboxConfigure},
				)
				env.RegisterActivityWithOptions(
					func(context.Context, contract.SandboxOpInput) (contract.SandboxLease, error) {
						calls = append(calls, "resume")
						return lease, nil
					},
					activity.RegisterOptions{Name: contract.ActivitySandboxResume},
				)
				env.RegisterActivityWithOptions(
					func(context.Context, contract.SandboxOpInput) (
						contract.SandboxInspectResult,
						error,
					) {
						return contract.SandboxInspectResult{Exists: true}, nil
					},
					activity.RegisterOptions{Name: contract.ActivitySandboxInspect},
				)
				env.RegisterActivityWithOptions(func(context.Context, contract.SandboxRenewInput) (
					contract.SandboxLease,
					error,
				) {
					calls = append(calls, "renew")
					return lease, nil
				}, activity.RegisterOptions{Name: contract.ActivitySandboxRenew})
				env.RegisterActivityWithOptions(
					func(context.Context, contract.SandboxOpInput) (
						contract.SandboxSnapshotResult,
						error,
					) {
						calls = append(calls, "snapshot")
						return contract.SandboxSnapshotResult{SnapshotId: "snapshot"}, nil
					},
					activity.RegisterOptions{Name: contract.ActivitySandboxSnapshot},
				)
				for _, name := range []string{contract.ActivitySandboxPause, contract.ActivitySandboxDestroy} {
					env.RegisterActivityWithOptions(
						func(context.Context, contract.SandboxOpInput) error {
							calls = append(calls, name)
							return nil
						},
						activity.RegisterOptions{Name: name},
					)
				}
				env.RegisterActivityWithOptions(func(context.Context, contract.SandboxSnapshotDeleteInput) error {
					calls = append(calls, "delete_snapshot")
					return nil
				}, activity.RegisterOptions{Name: contract.ActivitySandboxSnapshotDelete})
				env.RegisterActivityWithOptions(
					func(context.Context, w.SandboxStateInput) error {
						return nil
					},
					activity.RegisterOptions{Name: "RecordSandboxState"},
				)
				acquire := func(id string) {
					env.UpdateWorkflow(
						w.Acquire,
						id,
						&testsuite.TestUpdateCallback{OnComplete: func(_ any, err error) {
							require.NoError(t, err)
						}},
						w.AcquireInput{
							RunID:      "run",
							SessionID:  "session",
							Profile:    "default",
							TTLSeconds: 1800,
							SpecDigest: "digest",
						},
					)
				}
				env.RegisterDelayedCallback(func() {
					acquire("one")
					acquire("two")
				}, 0)
				env.RegisterDelayedCallback(
					func() {
						env.SignalWorkflow(w.Touch, w.TouchInput{RunID: "run", InUse: false})
					},
					time.Second,
				)
				env.RegisterDelayedCallback(func() {
					acquire("three")
				}, 16*time.Minute)
				env.RegisterDelayedCallback(
					func() {
						env.SignalWorkflow(w.Archive, w.ArchiveInput{})
					},
					17*time.Minute,
				)
				env.ExecuteWorkflow(
					sandboxlease.Run,
					w.LeaseInput{TenantID: "tenant", IsolationKey: "isolation"},
				)
				require.NoError(t, env.GetWorkflowError())
				if provider == "local" {
					require.Equal(
						t,
						[]string{"create", "configure", "renew", contract.ActivitySandboxDestroy},
						calls,
					)
				} else {
					require.Contains(t, calls, contract.ActivitySandboxPause)
					require.Contains(t, calls, "snapshot")
					require.Contains(t, calls, "delete_snapshot")
				}
			},
		)
	}
}

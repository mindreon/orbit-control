package session

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/telemetry/wfctx"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type parentRecorder struct {
	interceptor.WorkerInterceptorBase
	parents *[]any
}

type parentInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	parents *[]any
}

type parentOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	parents *[]any
}

func (p *parentRecorder) InterceptWorkflow(
	_ workflow.Context, next interceptor.WorkflowInboundInterceptor,
) interceptor.WorkflowInboundInterceptor {
	return &parentInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}, parents: p.parents}
}
func (p *parentInbound) Init(out interceptor.WorkflowOutboundInterceptor) error {
	return p.Next.Init(&parentOutbound{WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: out},
		parents: p.parents})
}
func (p *parentOutbound) ExecuteActivity(ctx workflow.Context, name string, args ...any) workflow.Future {
	if name == "FinalizeRun" {
		*p.parents = append(*p.parents, wfctx.WorkflowSpan(ctx))
	}
	return p.Next.ExecuteActivity(ctx, name, args...)
}
func (p *parentOutbound) ExecuteChildWorkflow(ctx workflow.Context, name string, args ...any) workflow.ChildWorkflowFuture {
	*p.parents = append(*p.parents, wfctx.WorkflowSpan(ctx))
	return p.Next.ExecuteChildWorkflow(ctx, name, args...)
}

func TestQueuedTraceParents(t *testing.T) {
	for _, mode := range []string{"start", "fallback", "drain", "cancel", "cancel-failure", "interrupt-failure"} {
		t.Run(mode, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			var parents []any
			env.SetWorkerOptions(worker.Options{Interceptors: []interceptor.WorkerInterceptor{&parentRecorder{parents: &parents}}})
			env.RegisterWorkflowWithOptions(func(workflow.Context, w.RunInput) error { return nil },
				workflow.RegisterOptions{Name: w.AgentRunWorkflow})
			env.RegisterActivityWithOptions(func(context.Context, w.FinalizeInput) error {
				if mode == "cancel-failure" {
					return temporal.NewNonRetryableApplicationError("failed", "fixture", nil)
				}
				return nil
			}, activity.RegisterOptions{Name: "FinalizeRun"})
			env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", w.Archive, mock.Anything).Return(nil)
			env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", w.Cancel, mock.Anything).
				Return(temporal.NewNonRetryableApplicationError("failed", "fixture", nil))
			env.ExecuteWorkflow(func(ctx workflow.Context) error {
				s := &sessionWorkflow{in: w.SessionInput{V: 1, TenantID: "tenant", SessionID: "session"},
					policy: "queue", parents: map[string]any{}}
				for _, id := range []string{"one", "two"} {
					msg := w.SubmitMessageInput{V: 1, RunID: id, Lock: contract.SegmentLockRef{Id: "lock",
						Digest: "sha256:0123456789012345678901234567890123456789012345678901234567890123"}}
					_, err := s.submit(workflow.WithValue(ctx, wfctx.SpanContextKey, id), msg)
					require.NoError(t, err)
				}
				require.Len(t, s.parents, 2)
				switch mode {
				case "start", "fallback":
					if mode == "fallback" {
						s.parents = map[string]any{}
						ctx = workflow.WithValue(ctx, wfctx.SpanContextKey, "session-root")
					}
					s.startNext(ctx)
					require.NoError(t, workflow.Await(ctx, func() bool { return s.active == "" }))
					s.startNext(ctx)
					require.NoError(t, workflow.Await(ctx, func() bool { return s.active == "" }))
				case "drain":
					require.NoError(t, s.drain(ctx))
				case "cancel", "cancel-failure":
					_, err := s.cancel(workflow.WithValue(ctx, wfctx.SpanContextKey, "cancel-request"), w.CancelRunInput{RunID: "one"})
					if mode == "cancel-failure" {
						require.Error(t, err)
						require.Len(t, s.parents, 2)
						require.Equal(t, "one", s.queue[0].RunID)
						return nil
					}
					require.NoError(t, err)
					require.NotContains(t, s.parents, "one")
					require.Contains(t, s.parents, "two")
					delete(s.parents, "two")
				case "interrupt-failure":
					s.active, s.activeKind, s.external, s.policy = "active", "agent", true, "interrupt"
					_, err := s.submit(workflow.WithValue(ctx, wfctx.SpanContextKey, "three"), w.SubmitMessageInput{V: 1, RunID: "three"})
					require.Error(t, err)
					require.NotContains(t, s.parents, "three", "failed admission must not retain a parent")
					return nil
				}
				require.Empty(t, s.parents)
				return nil
			})
			require.NoError(t, env.GetWorkflowError())
			if mode == "start" || mode == "drain" {
				require.Equal(t, []any{"one", "two"}, parents)
			}
			if mode == "fallback" {
				require.Equal(t, []any{"session-root", "session-root"}, parents)
			}
			if mode == "cancel" {
				require.Equal(t, []any{"cancel-request"}, parents)
			}
		})
	}
}

func TestContinueCarryDoesNotSerializeParents(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Second})
		s := &sessionWorkflow{parents: map[string]any{"one": "ORBIT-CANARY-PARENT"},
			queue: []w.SubmitMessageInput{{RunID: "one", Lock: contract.SegmentLockRef{Id: "lock",
				Digest: "sha256:" + strings.Repeat("0", 64)}}}}
		in := w.SessionInput{V: 1, Carry: &w.SessionCarry{Queue: s.queue, Policy: s.policy}}
		return workflow.NewContinueAsNewError(ctx, w.SessionWorkflow, in)
	})
	var continued *workflow.ContinueAsNewError
	require.True(t, errors.As(env.GetWorkflowError(), &continued))
	var carry w.SessionInput
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(continued.Input, &carry))
	raw, err := json.Marshal(carry)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "ORBIT-CANARY-PARENT")
	require.NotContains(t, string(raw), "parents")
	require.Equal(t, "one", carry.Carry.Queue[0].RunID)
}

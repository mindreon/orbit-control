package workflows_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/platform/telemetry"
	"github.com/mindreon/orbit-control/internal/platform/telemetry/wfctx"
	"github.com/mindreon/orbit-control/internal/platform/temporalx"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/session"
	"github.com/mindreon/orbit-control/internal/workflows/teamrun"
)

// testsuite intentionally supplies nil Update headers. This test-only inbound
// adapter supplies the header a real server delivers; the production interceptor
// still performs all extraction, span creation and downstream injection.
type updateHeaders struct {
	interceptor.WorkerInterceptorBase
}
type updateHeadersInbound struct {
	interceptor.WorkflowInboundInterceptorBase
}

func (*updateHeaders) InterceptWorkflow(
	_ workflow.Context, next interceptor.WorkflowInboundInterceptor,
) interceptor.WorkflowInboundInterceptor {
	return &updateHeadersInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}}
}
func (h *updateHeadersInbound) ExecuteUpdate(ctx workflow.Context, in *interceptor.UpdateInput) (any, error) {
	id := workflow.GetCurrentUpdateInfo(ctx).ID
	if id == "one" || id == "two" {
		digit := "1"
		if id == "two" {
			digit = "2"
		}
		payload, err := converter.GetDefaultDataConverter().ToPayload(map[string]string{
			"traceparent": "00-" + strings.Repeat(digit, 32) + "-0123456789abcdef-01",
			"tracestate":  "vendor=ORBIT-CANARY",
		})
		if err != nil {
			return nil, err
		}
		interceptor.WorkflowHeader(ctx)["_tracer-data"] = payload
	}
	return h.Next.ExecuteUpdate(ctx, in)
}

func TestTraceContinuity(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(telemetry.AllowlistExporter(exporter)))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(old)
		_ = provider.Shutdown(context.Background())
	})
	var suite testsuite.WorkflowTestSuite
	h := &sessionHarness{env: suite.NewTestWorkflowEnvironment(), in: w.SessionInput{
		V: 1, TenantID: "tenant", WorkspaceID: "workspace", SessionID: "session", Policy: "queue"}}
	observations := &headerObservations{}
	interceptors := append([]interceptor.WorkerInterceptor{
		&updateHeaders{}, &cleanHeaderCheck{observations: observations},
	}, teamrun.WorkerOptions().Interceptors...)
	interceptors = append(interceptors, temporalx.TemporalTracing(), &cleanExtractCheck{observations: observations})
	h.env.SetWorkerOptions(worker.Options{Interceptors: interceptors})
	payload, err := converter.GetDefaultDataConverter().ToPayload(map[string]string{
		"traceparent": "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-0123456789abcdef-01",
	})
	require.NoError(t, err)
	h.env.SetHeader(&commonpb.Header{Fields: map[string]*commonpb.Payload{"_tracer-data": payload}})
	// Replace the child's fixture with a short Activity so duplicate interceptor
	// registration produces duplicate RunActivity spans and fails below.
	h.env.RegisterWorkflowWithOptions(func(ctx workflow.Context, in w.RunInput) error {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Second})
		return workflow.ExecuteActivity(ctx, "TraceProbe", in).Get(ctx, nil)
	}, workflow.RegisterOptions{Name: w.AgentRunWorkflow, DisableAlreadyRegisteredCheck: true})
	h.env.RegisterActivityWithOptions(func(ctx context.Context, input w.RunInput) error {
		raw, err := json.Marshal(input)
		require.NoError(t, err)
		require.NotContains(t, string(raw), "traceparent")
		require.NotContains(t, string(raw), strings.Repeat("1", 32))
		require.NotContains(t, string(raw), strings.Repeat("2", 32))
		return nil
	}, activity.RegisterOptions{Name: "TraceProbe"})
	h.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", w.Archive, mock.Anything).Return(nil)
	h.env.RegisterDelayedCallback(func() { h.submit(t, "one", "one") }, 0)
	h.env.RegisterDelayedCallback(func() { h.submit(t, "two", "two") }, time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(w.Archive, w.ArchiveInput{}) }, 3*time.Second)
	h.env.ExecuteWorkflow(session.Run, h.in)
	require.NoError(t, h.env.GetWorkflowError())
	require.NotEmpty(t, observations.children)
	require.NotEmpty(t, observations.activities)
	for _, carrier := range append(observations.children, observations.activities...) {
		require.NotEmpty(t, carrier["traceparent"])
		require.NotContains(t, carrier, "tracestate")
	}
	require.NotEmpty(t, observations.states)
	for _, state := range observations.states {
		require.Empty(t, state, "Update extraction must remove tracestate")
	}
	for _, err := range observations.errors {
		require.NoError(t, err)
	}
	var children, activities []string
	for _, span := range exporter.GetSpans() {
		if span.Name == "StartChildWorkflow:"+w.AgentRunWorkflow {
			children = append(children, span.SpanContext.TraceID().String())
		}
		if span.Name == "RunActivity:TraceProbe" {
			activities = append(activities, span.SpanContext.TraceID().String())
		}
	}
	require.Equal(t, []string{strings.Repeat("1", 32), strings.Repeat("2", 32)}, children)
	require.Equal(t, children, activities)
}

// Observe outbound headers after the production tracing interceptor serializes
// them, before testsuite consumes/removes them. Also check extraction before the
// exporter can clear tracestate, so an Extract-only regression cannot hide.
type cleanHeaderCheck struct {
	interceptor.WorkerInterceptorBase
	observations *headerObservations
}
type cleanHeaderInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	observations *headerObservations
}
type cleanHeaderOutbound struct {
	interceptor.WorkflowOutboundInterceptorBase
	observations *headerObservations
}

func (h *cleanHeaderCheck) InterceptWorkflow(
	_ workflow.Context, next interceptor.WorkflowInboundInterceptor,
) interceptor.WorkflowInboundInterceptor {
	return &cleanHeaderInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next},
		observations: h.observations}
}
func (h *cleanHeaderInbound) Init(out interceptor.WorkflowOutboundInterceptor) error {
	return h.Next.Init(&cleanHeaderOutbound{
		WorkflowOutboundInterceptorBase: interceptor.WorkflowOutboundInterceptorBase{Next: out}, observations: h.observations})
}
func (h *cleanExtractInbound) ExecuteUpdate(ctx workflow.Context, in *interceptor.UpdateInput) (any, error) {
	if span, ok := wfctx.WorkflowSpan(ctx).(interface{ SpanContext() trace.SpanContext }); ok {
		h.observations.states = append(h.observations.states, span.SpanContext().TraceState().String())
	}
	return h.Next.ExecuteUpdate(ctx, in)
}
func collectWorkflowHeader(observations *headerObservations, ctx workflow.Context) map[string]string {
	var carrier map[string]string
	err := converter.GetDefaultDataConverter().FromPayload(interceptor.WorkflowHeader(ctx)["_tracer-data"], &carrier)
	observations.errors = append(observations.errors, err)
	return carrier
}
func (h *cleanHeaderOutbound) ExecuteActivity(ctx workflow.Context, name string, args ...any) workflow.Future {
	h.observations.activities = append(h.observations.activities, collectWorkflowHeader(h.observations, ctx))
	return h.Next.ExecuteActivity(ctx, name, args...)
}
func (h *cleanHeaderOutbound) ExecuteChildWorkflow(ctx workflow.Context, name string, args ...any) workflow.ChildWorkflowFuture {
	h.observations.children = append(h.observations.children, collectWorkflowHeader(h.observations, ctx))
	return h.Next.ExecuteChildWorkflow(ctx, name, args...)
}

type cleanExtractCheck struct {
	interceptor.WorkerInterceptorBase
	observations *headerObservations
}
type cleanExtractInbound struct {
	interceptor.WorkflowInboundInterceptorBase
	observations *headerObservations
}

func (h *cleanExtractCheck) InterceptWorkflow(
	_ workflow.Context, next interceptor.WorkflowInboundInterceptor,
) interceptor.WorkflowInboundInterceptor {
	return &cleanExtractInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next},
		observations: h.observations}
}

type headerObservations struct {
	children, activities []map[string]string
	states               []string
	errors               []error
}

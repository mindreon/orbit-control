package temporalx

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"google.golang.org/grpc"
)

func TestScheduleClientStripsTracing(t *testing.T) {
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(tracetest.NewInMemoryExporter()))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(old)
		_ = provider.Shutdown(context.Background())
	})
	var captured []*workflowservice.CreateScheduleRequest
	var updates []*workflowservice.UpdateScheduleRequest
	capture := func(_ context.Context, _ string, req, reply any, _ *grpc.ClientConn, _ grpc.UnaryInvoker,
		_ ...grpc.CallOption) error {
		switch request := req.(type) {
		case *workflowservice.GetSystemInfoRequest:
			*reply.(*workflowservice.GetSystemInfoResponse) = workflowservice.GetSystemInfoResponse{}
		case *workflowservice.CreateScheduleRequest:
			captured = append(captured, request)
		case *workflowservice.DescribeScheduleRequest:
			*reply.(*workflowservice.DescribeScheduleResponse) = workflowservice.DescribeScheduleResponse{
				Schedule: captured[0].Schedule, Info: &schedulepb.ScheduleInfo{}}
		case *workflowservice.UpdateScheduleRequest:
			updates = append(updates, request)
		}
		return nil
	}
	c, err := client.NewLazyClient(client.Options{HostPort: "127.0.0.1:1", Namespace: "trace-test",
		Interceptors:      []interceptor.ClientInterceptor{TemporalTracing()},
		ConnectionOptions: client.ConnectionOptions{DialOptions: []grpc.DialOption{grpc.WithUnaryInterceptor(capture)}},
	})
	require.NoError(t, err)
	defer c.Close()
	schedules, err := ScheduleClient(t.Context(), c, client.Options{Namespace: "trace-test"})
	require.NoError(t, err)
	defer schedules.Close()
	ctx, span := provider.Tracer("test").Start(t.Context(), "request")
	defer span.End()
	options := client.ScheduleOptions{ID: "test", Spec: client.ScheduleSpec{},
		Action: &client.ScheduleWorkflowAction{ID: "run", Workflow: "ScheduledRunWorkflow", TaskQueue: "ctl"}}
	_, err = schedules.Create(ctx, options)
	require.NoError(t, err)
	require.Len(t, captured, 1)
	require.Equal(t, "trace-test", captured[0].Namespace)
	require.NotContains(t, captured[0].Schedule.Action.GetStartWorkflow().Header.GetFields(), "_tracer-data")
	err = schedules.GetHandle(ctx, "test").Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			return &client.ScheduleUpdate{Schedule: &in.Description.Schedule}, nil
		},
	})
	require.NoError(t, err)
	require.Len(t, updates, 1)
	require.Equal(t, "trace-test", updates[0].Namespace)
	require.NotContains(t, updates[0].Schedule.Action.GetStartWorkflow().Header.GetFields(), "_tracer-data")

	_, err = c.ScheduleClient().Create(ctx, options)
	require.NoError(t, err)
	require.Len(t, captured, 2)
	require.Contains(t, captured[1].Schedule.Action.GetStartWorkflow().Header.GetFields(), "_tracer-data",
		"negative control: the traced client must persist its parent")
}

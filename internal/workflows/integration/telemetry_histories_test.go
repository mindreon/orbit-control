//go:build temporal_integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/teamrun"
)

func recordTracedSession(t *testing.T, c client.Client, queue string) {
	name := "trace-session"
	id := queue + name
	ctx, root := otel.Tracer("record").Start(t.Context(), "session-create")
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: id, TaskQueue: queue}, w.SessionWorkflow,
		w.SessionInput{V: 1, TenantID: "tenant", WorkspaceID: "workspace", SessionID: id, Policy: "queue"})
	require.NoError(t, err)
	root.End()
	var traces []trace.TraceID
	for _, suffix := range []string{"pause", "complete"} {
		ctx, span := otel.Tracer("record").Start(t.Context(), "submit")
		traces = append(traces, span.SpanContext().TraceID())
		in := runInput(suffix)
		msg := w.SubmitMessageInput{V: 1, RunID: id + suffix, Lock: in.Lock, BudgetMillicredits: in.BudgetMillicredits,
			Limits: in.Limits, Sandbox: in.Sandbox}
		handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: run.GetID(), UpdateName: w.SubmitMessage,
			UpdateID: suffix, WaitForStage: client.WorkflowUpdateStageCompleted, Args: []any{msg}})
		require.NoError(t, err)
		require.NoError(t, handle.Get(ctx, nil))
		span.End()
	}
	require.NotEqual(t, traces[0], traces[1])
	require.Eventually(t, func() bool {
		value, err := c.QueryWorkflow(t.Context(), id, "", w.State)
		if err != nil {
			return false
		}
		var state w.SessionState
		return value.Get(&state) == nil && state.ActiveRunID == "" && state.QueueLen == 0
	}, 20*time.Second, 20*time.Millisecond)
	require.NoError(t, c.SignalWorkflow(t.Context(), id, "", w.Archive, w.ArchiveInput{}))
	require.NoError(t, run.Get(t.Context(), nil))
	iterator := c.GetWorkflowHistory(t.Context(), id, run.GetRunID(), false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	var childTraceIDs []string
	var childIDs []string
	for iterator.HasNext() {
		event, err := iterator.Next()
		require.NoError(t, err)
		if child := event.GetStartChildWorkflowExecutionInitiatedEventAttributes(); child != nil {
			childIDs = append(childIDs, child.WorkflowId)
			var carrier map[string]string
			require.NoError(t, converter.GetDefaultDataConverter().FromPayload(child.Header.Fields["_tracer-data"], &carrier))
			childTraceIDs = append(childTraceIDs, carrier["traceparent"][3:35])
		}
	}
	require.Equal(t, []string{traces[0].String(), traces[1].String()}, childTraceIDs)
	for i, id := range childIDs {
		iterator := c.GetWorkflowHistory(t.Context(), id, "", false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
		for iterator.HasNext() {
			event, err := iterator.Next()
			require.NoError(t, err)
			if scheduled := event.GetActivityTaskScheduledEventAttributes(); scheduled != nil {
				var carrier map[string]string
				require.NoError(t, converter.GetDefaultDataConverter().FromPayload(scheduled.Header.Fields["_tracer-data"], &carrier))
				require.Len(t, carrier["traceparent"], 55)
				require.Equal(t, childTraceIDs[i], carrier["traceparent"][3:35])
				require.Len(t, carrier, 1)
			}
		}
	}
	save(t, c, run.GetID(), run.GetRunID(), name)
}

func recordTracedTeam(t *testing.T, c client.Client) {
	queue := "trace-team-" + time.Now().Format("150405.000000")
	f := &fixtures{calls: map[string]int{}}
	ctl := worker.New(c, contract.TaskQueueCtl, teamrun.WorkerOptions())
	registerFixtures(ctl, f)
	registerWP8(ctl, c)
	wait := worker.New(c, w.TaskQueueCtlWait, worker.Options{})
	registerWP8(wait, c)
	agent := worker.New(c, contract.TaskQueueAgentStandard, worker.Options{})
	agent.RegisterActivityWithOptions(wp8Segment(c), activity.RegisterOptions{Name: contract.ActivityRunAgentSegment})
	for _, reg := range []worker.Worker{ctl, wait, agent} {
		require.NoError(t, reg.Start())
		defer reg.Stop()
	}
	ctx, span := otel.Tracer("record").Start(t.Context(), "team-create")
	defer span.End()
	in := teamHistoryInput(queue)
	run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: w.TeamRunWorkflowID("tenant", in.RunID),
		TaskQueue: contract.TaskQueueCtl}, w.TeamRunWorkflow, in)
	require.NoError(t, err)
	require.NoError(t, run.Get(ctx, nil))
	save(t, c, run.GetID(), run.GetRunID(), "trace-team")
}

func recordingProvider(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	old := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(old)
		_ = provider.Shutdown(context.Background())
	})
}

func TestRecordHistoriesTraceTeam(t *testing.T) {
	recordingProvider(t)
	recordTracedTeam(t, devClient(t))
}

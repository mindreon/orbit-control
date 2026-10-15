package workflows_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/agentrun"
	"github.com/mindreon/orbit-control/internal/workflows/sandboxlease"
	"github.com/mindreon/orbit-control/internal/workflows/session"
)

func TestReplay(t *testing.T) {
	paths, err := filepath.Glob("testdata/histories/*.json")
	require.NoError(t, err)
	require.Len(t, paths, 12, "all WP6 branches need real server histories")
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			history := &historypb.History{}
			require.NoError(t, protojson.Unmarshal(raw, history))
			assertHistoryScenario(t, strings.TrimSuffix(filepath.Base(path), ".json"), history)
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflowWithOptions(agentrun.Run, workflow.RegisterOptions{Name: w.AgentRunWorkflow})
			replayer.RegisterWorkflowWithOptions(session.Run, workflow.RegisterOptions{Name: w.SessionWorkflow})
			replayer.RegisterWorkflowWithOptions(sandboxlease.Run, workflow.RegisterOptions{Name: w.SandboxLeaseWorkflow})
			require.NoError(t, replayer.ReplayWorkflowHistoryFromJSONFile(nil, path))
		})
	}
}

func historyInputs[T any](t *testing.T, history *historypb.History, activity string) []T {
	t.Helper()
	var inputs []T
	for _, event := range history.Events {
		scheduled := event.GetActivityTaskScheduledEventAttributes()
		if scheduled != nil && scheduled.ActivityType.Name == activity {
			var input T
			require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(scheduled.Input, &input))
			inputs = append(inputs, input)
		}
	}
	return inputs
}
func assertHistoryScenario(t *testing.T, name string, history *historypb.History) {
	t.Helper()
	var types []enums.EventType
	for _, event := range history.Events {
		types = append(types, event.EventType)
	}
	require.NotEmpty(t, history.Events)
	segments := historyInputs[contract.SegmentInput](t, history, contract.ActivityRunAgentSegment)
	final := historyInputs[w.FinalizeInput](t, history, "FinalizeRun")
	switch name {
	case "can":
		require.Contains(t, types, enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW)
	case "cancel":
		require.Contains(t, types, enums.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED)
		require.Contains(t, types, enums.EVENT_TYPE_ACTIVITY_TASK_CANCELED)
		require.Len(t, final, 1)
		require.Equal(t, "cancelled", final[0].Status)
		require.Equal(t, "CANCELLED", final[0].ErrorCode)
	case "steer":
		require.Contains(t, types, enums.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED)
		require.Len(t, segments, 2)
		require.Equal(t, contract.SegmentTurnKind("follow_up"), segments[1].Input.Kind)
		require.NotEmpty(t, segments[1].Steer)
	case "expiry", "approval":
		decisions := historyInputs[w.DecisionInput](t, history, "ApplyDecision")
		require.Len(t, decisions, 1)
		require.Equal(t, name == "expiry", decisions[0].Expired)
		if name == "expiry" {
			timers := 0
			for _, typ := range types {
				if typ == enums.EVENT_TYPE_TIMER_STARTED {
					timers++
				}
			}
			require.Equal(t, 3, timers, "remind, escalate and expiry must all use real timers")
			notifications := historyInputs[w.NotifyInput](t, history, "NotifyInteraction")
			require.Len(t, notifications, 2)
			require.Equal(t, "remind", notifications[0].Kind)
			require.Equal(t, "escalate", notifications[1].Kind)
		}
	case "compact":
		require.Len(t, segments, 3)
		require.Equal(t, contract.SegmentTurnKind("compact"), segments[1].Input.Kind)
		require.Equal(t, segments[0].Seq, segments[1].Seq)
	case "sandbox":
		acquires := historyInputs[w.AcquireSandboxInput](t, history, "AcquireSandbox")
		require.Len(t, acquires, 2)
		require.Equal(t, acquires[0].Seq, acquires[1].Seq)
		require.Greater(t, acquires[1].Attempt, acquires[0].Attempt)
		require.Len(t, segments, 2)
		require.Equal(t, segments[0].Seq, segments[1].Seq)
	case "lease":
		assertLeaseHistory(t, history)
	case "session":
		children, archives := 0, 0
		for _, event := range history.Events {
			if event.GetStartChildWorkflowExecutionInitiatedEventAttributes() != nil {
				children++
			}
			if signal := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes(); signal != nil && signal.SignalName == w.Archive {
				archives++
			}
		}
		require.Equal(t, 2, children)
		require.Equal(t, 1, archives)
	case "pause":
		statuses := historyInputs[w.StatusInput](t, history, "SetRunStatus")
		require.Len(t, statuses, 2)
		require.Equal(t, "paused", statuses[0].Status)
		require.Equal(t, "running", statuses[1].Status)
	case "yield":
		require.Len(t, segments, 3)
		for i, input := range segments {
			require.Equal(t, i+1, input.Seq)
			if i > 0 {
				require.Equal(t, contract.SegmentTurnKind("continue"), input.Input.Kind)
			}
		}
	case "complete":
		require.Len(t, segments, 1)
		require.Len(t, final, 1)
		require.Equal(t, "succeeded", final[0].Status)
	default:
		t.Fatalf("missing replay assertions for %s", name)
	}
}
func assertLeaseHistory(t *testing.T, history *historypb.History) {
	t.Helper()
	require.NotEmpty(t, historyInputs[contract.SandboxOpInput](t, history, contract.ActivitySandboxPause))
	require.NotEmpty(t, historyInputs[contract.SandboxOpInput](t, history, contract.ActivitySandboxSnapshot))
	require.NotEmpty(t, historyInputs[contract.SandboxOpInput](t, history, contract.ActivitySandboxDestroy))
	creates := historyInputs[contract.SandboxAcquireInput](t, history, contract.ActivitySandboxCreate)
	require.Len(t, creates, 2)
	require.Nil(t, creates[0].SnapshotId)
	require.NotNil(t, creates[1].SnapshotId)
	require.NotEmpty(t, *creates[1].SnapshotId)
	require.Greater(t, creates[1].Metadata.LeaseGen, creates[0].Metadata.LeaseGen)
	for _, in := range creates {
		require.Equal(t, contract.SandboxProviderOpensandbox, in.Provider)
	}
}

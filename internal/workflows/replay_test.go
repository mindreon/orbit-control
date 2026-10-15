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
			replayer.RegisterWorkflowWithOptions(
				agentrun.Run,
				workflow.RegisterOptions{Name: w.AgentRunWorkflow},
			)
			replayer.RegisterWorkflowWithOptions(
				session.Run,
				workflow.RegisterOptions{Name: w.SessionWorkflow},
			)
			replayer.RegisterWorkflowWithOptions(
				sandboxlease.Run,
				workflow.RegisterOptions{Name: w.SandboxLeaseWorkflow},
			)
			require.NoError(t, replayer.ReplayWorkflowHistoryFromJSONFile(nil, path))
		})
	}
}

func assertHistoryScenario(t *testing.T, name string, history *historypb.History) {
	t.Helper()
	var types []enums.EventType
	segments := 0
	var final *w.FinalizeInput
	for _, event := range history.Events {
		types = append(types, event.EventType)
		scheduled := event.GetActivityTaskScheduledEventAttributes()
		if scheduled == nil {
			continue
		}
		if scheduled.ActivityType.Name == contract.ActivityRunAgentSegment {
			segments++
		}
		if scheduled.ActivityType.Name == "FinalizeRun" {
			final = new(w.FinalizeInput)
			require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(scheduled.Input, final))
		}
	}
	switch name {
	case "can":
		require.Contains(t, types, enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW)
	case "cancel":
		require.Contains(t, types, enums.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED)
		require.NotNil(t, final)
		require.Equal(t, "cancelled", final.Status)
		require.Equal(t, "CANCELLED", final.ErrorCode)
	case "steer":
		require.Contains(t, types, enums.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED)
		require.Equal(t, 2, segments, "steer arriving during completion must cause a follow-up segment")
	}
}

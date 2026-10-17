package workflows_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	"github.com/mindreon/orbit-control/internal/workflows/membertask"
	"github.com/mindreon/orbit-control/internal/workflows/sandboxlease"
	"github.com/mindreon/orbit-control/internal/workflows/scheduledrun"
	"github.com/mindreon/orbit-control/internal/workflows/session"
	"github.com/mindreon/orbit-control/internal/workflows/teamrun"
)

func TestReplay(t *testing.T) {
	paths, err := filepath.Glob("testdata/histories/*.json")
	require.NoError(t, err)
	require.Len(t, paths, 22, "WP6 branches and the WP7 subject change need real server histories")
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
			replayer.RegisterWorkflowWithOptions(teamrun.Run, workflow.RegisterOptions{Name: w.TeamRunWorkflow})
			replayer.RegisterWorkflowWithOptions(membertask.Run, workflow.RegisterOptions{Name: w.MemberTaskWorkflow})
			replayer.RegisterWorkflowWithOptions(scheduledrun.Run, workflow.RegisterOptions{Name: w.ScheduledRunWorkflow})
			require.NoError(t, replayer.ReplayWorkflowHistoryWithOptions(nil, history, worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: history.Events[0].GetWorkflowExecutionStartedEventAttributes().GetWorkflowId()}}))
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
	case "lease-maintenance-can", "lease-archive-can":
		require.Contains(t, types, enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW)
		event := history.Events[len(history.Events)-1].GetWorkflowExecutionContinuedAsNewEventAttributes()
		require.NotNil(t, event)
		var input w.LeaseInput
		require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(event.Input, &input))
		require.Equal(t, name == "lease-archive-can", input.Carry.Archived)
		require.Equal(t, "paused", input.Carry.State)
		require.Equal(t, 2, input.Carry.Gen)
		require.Zero(t, input.Carry.Touches)
		require.Equal(t, "snapshot", input.Carry.SnapshotID)
		require.True(t, input.Carry.Dirty)
		require.True(t, input.Carry.SnapshotPending)
		require.Equal(t, "old-snapshot", input.Carry.ObsoleteSnapshot)
		require.Equal(t, new(true), input.Carry.PendingRestored)
		require.False(t, input.Carry.RetryAt.IsZero())
		require.Equal(t, time.Minute, input.Carry.Backoff)
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
	case "session", "session-subject":
		children, archives := 0, 0
		for _, event := range history.Events {
			if child := event.GetStartChildWorkflowExecutionInitiatedEventAttributes(); child != nil {
				children++
				var input w.RunInput
				require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(child.Input, &input))
				if name == "session-subject" {
					require.Equal(t, w.EventSubject(input.TenantID, input.SessionID, input.RunID), input.EventSubject)
				} else {
					require.Equal(t, "run."+input.TenantID+"."+input.RunID, input.EventSubject,
						"histories without the version marker must retain the DefaultVersion subject")
				}
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
	case "teamrun-happy", "teamrun-reject-escalate":
		finals := historyInputs[w.FinalizeTeamInput](t, history, "FinalizeTeamRun")
		require.Len(t, finals, 1)
		require.Equal(t, "succeeded", finals[0].Status)
		require.Equal(t, "Recorded team summary", finals[0].Summary)
		require.Contains(t, types, enums.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED)
		children := childWorkflows(history)
		require.Contains(t, children, w.MemberTaskWorkflowID("tenant", name, "reviewer", 1))
		require.Contains(t, children, w.MemberTaskWorkflowID("tenant", name, "leader", 2))
		require.Contains(t, signalNames(history), w.EndTurn)
	case "teamrun-can-midflight":
		require.Equal(t, enums.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, types[len(types)-1])
		event := history.Events[len(history.Events)-1].GetWorkflowExecutionContinuedAsNewEventAttributes()
		var input w.TeamRunInput
		require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(event.Input, &input))
		require.NotNil(t, input.Carry)
		require.Contains(t, input.Carry.Active, "alice", "a member turn is still running across Continue-As-New")
		require.Contains(t, types, enums.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED, "the wait Activity is cancelled and re-created by the next run")
	case "teamrun-cancel":
		finals := historyInputs[w.FinalizeTeamInput](t, history, "FinalizeTeamRun")
		require.Len(t, finals, 1)
		require.Equal(t, "cancelled", finals[0].Status)
		require.Equal(t, "CANCELLED", finals[0].Code)
		require.Contains(t, signalNames(history), w.Cancel)
	case "membertask-deliver-endturn":
		require.Len(t, segments, 1)
		var received []string
		for _, event := range history.Events {
			if signal := event.GetWorkflowExecutionSignaledEventAttributes(); signal != nil {
				received = append(received, signal.SignalName)
			}
		}
		require.Equal(t, []string{w.Deliver, w.EndTurn}, received)
		require.Equal(t, enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, types[len(types)-1])
	case "scheduledrun-team":
		require.Len(t, childWorkflows(history), 1)
		require.Contains(t, types, enums.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED)
	case "agentrun-unattended":
		require.Len(t, final, 1)
		require.Equal(t, "failed", final[0].Status)
		require.Equal(t, "UNATTENDED_INTERACTION", final[0].ErrorCode)
	default:
		t.Fatalf("missing replay assertions for %s", name)
	}
}
func childWorkflows(history *historypb.History) []string {
	var ids []string
	for _, event := range history.Events {
		if child := event.GetStartChildWorkflowExecutionInitiatedEventAttributes(); child != nil {
			ids = append(ids, child.WorkflowId)
		}
	}
	return ids
}
func signalNames(history *historypb.History) []string {
	var names []string
	for _, event := range history.Events {
		if signal := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes(); signal != nil {
			names = append(names, signal.SignalName)
		}
	}
	return names
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

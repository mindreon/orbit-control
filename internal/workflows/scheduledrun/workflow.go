package scheduledrun

import (
	"errors"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func Run(ctx workflow.Context, in w.ScheduledRunInput) error {
	id := workflow.GetInfo(ctx).WorkflowExecution.ID
	prefix := w.ScheduledRunWorkflowIDBase(in.TenantID, in.ScheduleID) + "-"
	if !strings.HasPrefix(id, prefix) || len(id) != len(prefix)+20 {
		return temporal.NewNonRetryableApplicationError("Invalid schedule fire time", "SCHEDULE_FIRE_TIME_INVALID", nil)
	}
	fireAt, err := time.Parse(time.RFC3339, id[len(prefix):])
	if err != nil {
		return temporal.NewNonRetryableApplicationError("Invalid schedule fire time", "SCHEDULE_FIRE_TIME_INVALID", err)
	}
	var start w.ScheduledStart
	if err = workflow.ExecuteActivity(w.Ctl(ctx, "CreateScheduledSession"), "CreateScheduledSession",
		w.ScheduledSessionInput{TenantID: in.TenantID, ScheduleID: in.ScheduleID, FireAt: fireAt, Now: workflow.Now(ctx)}).Get(ctx, &start); err != nil {
		return err
	}
	if start.Skip != "" {
		return nil
	}
	var runID, sessionID, workspaceID string
	var input any
	name := w.AgentRunWorkflow
	if start.Kind == "team" {
		if start.Team == nil {
			return temporal.NewNonRetryableApplicationError("Missing team input", "INVALID_SPEC", nil)
		}
		runID, sessionID, workspaceID = start.Team.RunID, start.Team.SessionID, start.Team.WorkspaceID
		input, name = start.Team, w.TeamRunWorkflow
	} else {
		if start.Run == nil {
			return temporal.NewNonRetryableApplicationError("Missing agent input", "INVALID_SPEC", nil)
		}
		runID, sessionID, workspaceID = start.Run.RunID, start.Run.SessionID, start.Run.WorkspaceID
		input = start.Run
	}
	child := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: w.RunWorkflowID(start.Kind, in.TenantID, runID),
		TaskQueue: contract.TaskQueueCtl, ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL, WaitForCancellation: true,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		TypedSearchAttributes: temporal.NewSearchAttributes(
			temporal.NewSearchAttributeKeyKeyword("TenantId").ValueSet(in.TenantID),
			temporal.NewSearchAttributeKeyKeyword("WorkspaceId").ValueSet(workspaceID),
			temporal.NewSearchAttributeKeyKeyword("RunKind").ValueSet(start.Kind))})
	// Business failure is projected by the child and must not fail the schedule action.
	_ = workflow.ExecuteChildWorkflow(child, name, input).Get(ctx, nil)
	cleanup, _ := workflow.NewDisconnectedContext(ctx)
	err = workflow.SignalExternalWorkflow(cleanup, w.LeaseWorkflowID(in.TenantID, w.IsolationKey(in.TenantID, sessionID)), "",
		w.Archive, w.ArchiveInput{}).Get(cleanup, nil)
	if _, missing := errors.AsType[*temporal.UnknownExternalWorkflowExecutionError](err); err != nil && !missing {
		return err
	}
	return workflow.ExecuteActivity(w.Ctl(cleanup, "RecordScheduleRun"), "RecordScheduleRun",
		w.RecordScheduleInput{TenantID: in.TenantID, ScheduleID: in.ScheduleID, FireAt: fireAt}).Get(cleanup, nil)
}

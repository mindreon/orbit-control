package runworker

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

// EnsureUsageSchedule receives only the untraced schedule client, never the
// workflow client, so persisted system actions cannot inherit a request trace.
func EnsureUsageSchedule(ctx context.Context, c client.ScheduleClient) error {
	const id = "sys:usage-rollup"
	spec := client.ScheduleSpec{CronExpressions: []string{"5 * * * *"}, TimeZoneName: "UTC"}
	action := &client.ScheduleWorkflowAction{ID: id, Workflow: w.UsageRollupWorkflow,
		TaskQueue: contract.TaskQueueCtl, Args: []any{w.UsageRollupInput{}}, WorkflowRunTimeout: 30 * time.Minute}
	_, err := c.Create(ctx, client.ScheduleOptions{ID: id, Spec: spec, Action: action,
		Overlap: enums.SCHEDULE_OVERLAP_POLICY_SKIP, CatchupWindow: time.Hour})
	if err == nil {
		return nil
	}
	if _, exists := errors.AsType[*serviceerror.AlreadyExists](err); !exists && !errors.Is(err, temporal.ErrScheduleAlreadyRunning) {
		return err
	}
	return c.GetHandle(ctx, id).Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			return &client.ScheduleUpdate{Schedule: &client.Schedule{Spec: &spec, Action: action,
				Policy: &client.SchedulePolicies{Overlap: enums.SCHEDULE_OVERLAP_POLICY_SKIP, CatchupWindow: time.Hour},
				State:  in.Description.Schedule.State}}, nil
		}})
}

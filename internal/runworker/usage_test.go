package runworker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	w "github.com/mindreon/orbit-control/internal/workflows"
)

type usageSchedules struct {
	client.ScheduleClient
	options client.ScheduleOptions
	exists  bool
	updated bool
	state   *client.ScheduleState
}

type usageHandle struct {
	client.ScheduleHandle
	parent *usageSchedules
}

func (s *usageSchedules) Create(_ context.Context, options client.ScheduleOptions) (client.ScheduleHandle, error) {
	s.options = options
	if s.exists {
		return nil, serviceerror.NewAlreadyExists("fixture")
	}
	return &usageHandle{parent: s}, nil
}
func (s *usageSchedules) GetHandle(context.Context, string) client.ScheduleHandle {
	return &usageHandle{parent: s}
}
func (h *usageHandle) Update(_ context.Context, options client.ScheduleUpdateOptions) error {
	out, err := options.DoUpdate(client.ScheduleUpdateInput{
		Description: client.ScheduleDescription{Schedule: client.Schedule{State: h.parent.state}},
	})
	if err != nil {
		return err
	}
	h.parent.updated = true
	if out.Schedule.State != h.parent.state {
		return errors.New("existing schedule state was replaced")
	}
	if out.Schedule.Spec.CronExpressions[0] != "5 * * * *" || out.Schedule.Spec.TimeZoneName != "UTC" ||
		out.Schedule.Policy.CatchupWindow != time.Hour || out.Schedule.Policy.Overlap != enums.SCHEDULE_OVERLAP_POLICY_SKIP {
		return errors.New("incorrect updated policy")
	}
	return nil
}

func TestEnsureUsageSchedule(t *testing.T) {
	for _, exists := range []bool{false, true} {
		c := &usageSchedules{exists: exists, state: &client.ScheduleState{Paused: true, Note: "operator pause"}}
		require.NoError(t, EnsureUsageSchedule(t.Context(), c))
		require.Equal(t, exists, c.updated)
		require.Equal(t, "sys:usage-rollup", c.options.ID)
		require.Equal(t, []string{"5 * * * *"}, c.options.Spec.CronExpressions)
		require.Equal(t, "UTC", c.options.Spec.TimeZoneName)
		require.Equal(t, time.Hour, c.options.CatchupWindow)
		require.Equal(t, enums.SCHEDULE_OVERLAP_POLICY_SKIP, c.options.Overlap)
		action := c.options.Action.(*client.ScheduleWorkflowAction)
		require.Equal(t, w.UsageRollupWorkflow, action.Workflow)
		require.Equal(t, "ctl", action.TaskQueue)
		require.Equal(t, 30*time.Minute, action.WorkflowRunTimeout)
		require.Zero(t, action.WorkflowExecutionTimeout)
		require.Equal(t, []any{w.UsageRollupInput{}}, action.Args)
	}
}

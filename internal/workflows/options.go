package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
)

func Ctl(ctx workflow.Context, name string) workflow.Context {
	d := 30 * time.Second
	attempts := int32(5)
	switch name {
	case "WaitForRun":
		d = 7 * 24 * time.Hour
		attempts = 10
	case "AcquireSandbox":
		d = 6 * time.Minute
		attempts = 3
	case "StartSegment", "SetRunStatus", "RecordSandboxState", "CancelInteraction", "ReconcileCheckpoint":
		d = 10 * time.Second
	case "RecordSegment", "ApplyDecision":
		attempts = 10
	case "FinalizeRun":
		d = time.Minute
		attempts = 10
	}
	opts := workflow.ActivityOptions{
		TaskQueue:           contract.TaskQueueCtl,
		StartToCloseTimeout: d,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    attempts,
		},
	}
	if name == "AcquireSandbox" || name == "WaitForRun" {
		opts.HeartbeatTimeout = 30 * time.Second
	}
	return workflow.WithActivityOptions(ctx, opts)
}
func Agent(ctx workflow.Context, tier string) workflow.Context {
	return workflow.WithActivityOptions(
		ctx,
		workflow.ActivityOptions{
			TaskQueue:           "agent." + tier,
			StartToCloseTimeout: 20 * time.Minute,
			HeartbeatTimeout:    time.Minute,
			WaitForCancellation: true,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:        2 * time.Second,
				BackoffCoefficient:     2,
				MaximumInterval:        time.Minute,
				MaximumAttempts:        3,
				NonRetryableErrorTypes: contract.NonRetryableErrors,
			},
		},
	)
}
func Sandbox(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(
		ctx,
		workflow.ActivityOptions{
			TaskQueue:           contract.TaskQueueSandbox,
			StartToCloseTimeout: 5 * time.Minute,
			HeartbeatTimeout:    30 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:        time.Second,
				BackoffCoefficient:     2,
				MaximumInterval:        30 * time.Second,
				MaximumAttempts:        5,
				NonRetryableErrorTypes: []string{contract.ErrorInvalidSpec},
			},
		},
	)
}

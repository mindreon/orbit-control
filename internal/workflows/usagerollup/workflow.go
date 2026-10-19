// Package usagerollup materializes completed dirty hours without tenant content.
package usagerollup

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func Run(ctx workflow.Context, in w.UsageRollupInput) error {
	if in.Before.IsZero() {
		in.Before = workflow.Now(ctx).UTC().Truncate(time.Hour)
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue: contract.TaskQueueCtl, StartToCloseTimeout: 2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumAttempts: 5},
	})
	for pageNo := 0; ; pageNo++ {
		var page []w.DirtyHour
		if err := workflow.ExecuteActivity(ctx, "ListDirtyHours", w.DirtyPage{
			Before: in.Before, AfterStart: in.AfterStart, AfterTenant: in.AfterTenant, Limit: 500,
		}).Get(ctx, &page); err != nil {
			return err
		}
		if len(page) == 0 {
			return nil
		}
		active := 0
		var failure error
		for _, hour := range page {
			if err := workflow.Await(ctx, func() bool { return active < 8 || failure != nil }); err != nil {
				return err
			}
			if failure != nil {
				break
			}
			active++
			workflow.Go(ctx, func(ctx workflow.Context) {
				err := workflow.ExecuteActivity(ctx, "RollupHour", hour).Get(ctx, nil)
				if err != nil && failure == nil {
					failure = err
				}
				active--
			})
		}
		if err := workflow.Await(ctx, func() bool { return active == 0 }); err != nil {
			return err
		}
		if failure != nil {
			return failure
		}
		last := page[len(page)-1]
		in.AfterStart, in.AfterTenant = last.Hour, last.TenantID
		if pageNo == 19 || workflow.GetInfo(ctx).GetContinueAsNewSuggested() {
			return workflow.NewContinueAsNewError(ctx, w.UsageRollupWorkflow, in)
		}
	}
}

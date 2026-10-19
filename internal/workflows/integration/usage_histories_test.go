//go:build temporal_integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/usagerollup"
)

func registerUsageFixtures(reg worker.Worker) {
	reg.RegisterWorkflowWithOptions(usagerollup.Run, workflow.RegisterOptions{Name: w.UsageRollupWorkflow})
	reg.RegisterActivityWithOptions(func(_ context.Context, in w.DirtyPage) ([]w.DirtyHour, error) {
		if !in.AfterStart.IsZero() {
			return nil, nil
		}
		return []w.DirtyHour{{TenantID: "01234567-1234-1234-1234-0123456789ab", Hour: in.Before.Add(-time.Hour)}}, nil
	}, activity.RegisterOptions{Name: "ListDirtyHours"})
	reg.RegisterActivityWithOptions(func(context.Context, w.DirtyHour) error { return nil }, activity.RegisterOptions{Name: "RollupHour"})
}

func recordUsageRollup(t *testing.T, c client.Client, queue string) {
	run, err := c.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{ID: queue + "usage-rollup", TaskQueue: queue},
		w.UsageRollupWorkflow, w.UsageRollupInput{})
	require.NoError(t, err)
	require.NoError(t, run.Get(t.Context(), nil))
	save(t, c, run.GetID(), run.GetRunID(), "usage-rollup")
}

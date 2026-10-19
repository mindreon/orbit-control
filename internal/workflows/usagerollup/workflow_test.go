package usagerollup

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	w "github.com/mindreon/orbit-control/internal/workflows"
)

func TestPagesAndConcurrency(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	now := time.Date(2026, 10, 19, 12, 30, 0, 0, time.UTC)
	env.SetStartTime(now)
	pages := 0
	env.RegisterActivityWithOptions(func(_ context.Context, in w.DirtyPage) ([]w.DirtyHour, error) {
		require.Equal(t, now.Truncate(time.Hour), in.Before)
		require.Equal(t, 500, in.Limit)
		pages++
		if pages == 3 {
			return nil, nil
		}
		if pages == 2 {
			require.Equal(t, "tenant-15", in.AfterTenant)
		}
		out := []w.DirtyHour{}
		for i := range 16 {
			out = append(out, w.DirtyHour{TenantID: fmt.Sprintf("tenant-%02d", i),
				Hour: now.Add(time.Duration(pages-4) * time.Hour).Truncate(time.Hour)})
		}
		return out, nil
	}, activity.RegisterOptions{Name: "ListDirtyHours"})
	env.RegisterActivityWithOptions(func(context.Context, w.DirtyHour) error { return nil }, activity.RegisterOptions{Name: "RollupHour"})
	active, maximum, calls := 0, 0, 0
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		if info.ActivityType.Name == "RollupHour" {
			active++
			maximum = max(maximum, active)
			calls++
		}
	})
	env.SetOnActivityCompletedListener(func(info *activity.Info, _ converter.EncodedValue, _ error) {
		if info.ActivityType.Name == "RollupHour" {
			active--
		}
	})
	env.ExecuteWorkflow(Run, w.UsageRollupInput{})
	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, 3, pages)
	require.Equal(t, 32, calls)
	require.LessOrEqual(t, maximum, 8)
}

func TestContinueAfterTwentyPages(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(Run, workflow.RegisterOptions{Name: w.UsageRollupWorkflow})
	pages := 0
	before := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	env.RegisterActivityWithOptions(func(_ context.Context, in w.DirtyPage) ([]w.DirtyHour, error) {
		pages++
		require.Equal(t, before, in.Before)
		if pages > 1 {
			require.Equal(t, before.Add(time.Duration(pages-22)*time.Hour), in.AfterStart)
		}
		return []w.DirtyHour{{TenantID: "tenant", Hour: before.Add(time.Duration(pages-21) * time.Hour)}}, nil
	}, activity.RegisterOptions{Name: "ListDirtyHours"})
	env.RegisterActivityWithOptions(func(context.Context, w.DirtyHour) error { return nil }, activity.RegisterOptions{Name: "RollupHour"})
	env.ExecuteWorkflow(w.UsageRollupWorkflow, w.UsageRollupInput{Before: before})
	var can *workflow.ContinueAsNewError
	require.True(t, errors.As(env.GetWorkflowError(), &can))
	require.Equal(t, 20, pages)
	var carry w.UsageRollupInput
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &carry))
	require.Equal(t, before, carry.Before)
	require.Equal(t, before.Add(-time.Hour), carry.AfterStart)
	require.Equal(t, "tenant", carry.AfterTenant)
}

func TestHourFailureStopsPaging(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	pages, calls := 0, 0
	env.RegisterActivityWithOptions(func(context.Context, w.DirtyPage) ([]w.DirtyHour, error) {
		pages++
		return []w.DirtyHour{{TenantID: "tenant", Hour: time.Unix(0, 0)}}, nil
	}, activity.RegisterOptions{Name: "ListDirtyHours"})
	env.RegisterActivityWithOptions(func(context.Context, w.DirtyHour) error {
		calls++
		return temporal.NewApplicationError("fixture", "Retry")
	},
		activity.RegisterOptions{Name: "RollupHour"})
	env.ExecuteWorkflow(Run, w.UsageRollupInput{})
	require.Error(t, env.GetWorkflowError())
	require.Equal(t, 1, pages)
	require.Equal(t, 5, calls)
}

func TestSuggestedContinueAsNew(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(Run, workflow.RegisterOptions{Name: w.UsageRollupWorkflow})
	env.SetContinueAsNewSuggested(true)
	before := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	pages := 0
	env.RegisterActivityWithOptions(func(context.Context, w.DirtyPage) ([]w.DirtyHour, error) {
		pages++
		return []w.DirtyHour{{TenantID: "tenant", Hour: before.Add(-time.Hour)}}, nil
	}, activity.RegisterOptions{Name: "ListDirtyHours"})
	env.RegisterActivityWithOptions(func(context.Context, w.DirtyHour) error { return nil }, activity.RegisterOptions{Name: "RollupHour"})
	env.ExecuteWorkflow(w.UsageRollupWorkflow, w.UsageRollupInput{Before: before})
	var can *workflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &can)
	require.Equal(t, 1, pages)
}

package workflows

import "go.temporal.io/sdk/workflow"

// Record the build's threshold so test histories replay with production builds.
func ContinueThreshold(ctx workflow.Context, threshold int) (int, error) {
	var value int
	err := workflow.SideEffect(ctx, func(workflow.Context) any { return threshold }).Get(&value)
	return value, err
}

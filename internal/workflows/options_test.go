package workflows_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func TestWaitActivitiesUseTheirOwnQueue(t *testing.T) {
	// T-R28 (F65): the long-lived wait Activities run on ctl-wait so they cannot starve ctl.
	queues := func(ctx workflow.Context) (map[string]string, error) {
		out := map[string]string{}
		for _, name := range []string{"WaitForRun", "WaitForTurn", "RecordBoard", "PrepareTeamRun"} {
			out[name] = workflow.GetActivityOptions(w.Ctl(ctx, name)).TaskQueue
		}
		return out, nil
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(queues)
	env.ExecuteWorkflow(queues)
	require.NoError(t, env.GetWorkflowError())
	var got map[string]string
	require.NoError(t, env.GetWorkflowResult(&got))
	require.Equal(t, map[string]string{"WaitForRun": w.TaskQueueCtlWait, "WaitForTurn": w.TaskQueueCtlWait,
		"RecordBoard": contract.TaskQueueCtl, "PrepareTeamRun": contract.TaskQueueCtl}, got)
	require.NotEqual(t, contract.TaskQueueCtl, w.TaskQueueCtlWait)
}

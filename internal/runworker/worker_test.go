package runworker

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/activities/ctl"
)

// registry records registrations; any other Registry method panics through the nil embedded interface.
type registry struct {
	worker.Registry
	activities []string
	workflows  []string
}

func (r *registry) RegisterWorkflowWithOptions(_ any, options workflow.RegisterOptions) {
	r.workflows = append(r.workflows, options.Name)
}
func (r *registry) RegisterActivityWithOptions(_ any, options activity.RegisterOptions) {
	r.activities = append(r.activities, options.Name)
}

func TestWaitWorkerRegistersOnlyTheWaitActivities(t *testing.T) {
	// T-R28 (F65): the ctl-wait worker has exactly the two wait Activities and no Workflows.
	r := &registry{}
	RegisterWait(r, &ctl.Activities{})
	sort.Strings(r.activities)
	require.Equal(t, []string{"WaitForRun", "WaitForTurn"}, r.activities)
	require.Empty(t, r.workflows)
}

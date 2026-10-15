package workflows_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/agentrun"
	"github.com/mindreon/orbit-control/internal/workflows/sandboxlease"
	"github.com/mindreon/orbit-control/internal/workflows/session"
)

func TestReplay(t *testing.T) {
	paths, err := filepath.Glob("testdata/histories/*.json")
	require.NoError(t, err)
	require.Len(t, paths, 12, "all WP6 branches need real server histories")
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflowWithOptions(
				agentrun.Run,
				workflow.RegisterOptions{Name: w.AgentRunWorkflow},
			)
			replayer.RegisterWorkflowWithOptions(
				session.Run,
				workflow.RegisterOptions{Name: w.SessionWorkflow},
			)
			replayer.RegisterWorkflowWithOptions(
				sandboxlease.Run,
				workflow.RegisterOptions{Name: w.SandboxLeaseWorkflow},
			)
			require.NoError(t, replayer.ReplayWorkflowHistoryFromJSONFile(nil, path))
		})
	}
}

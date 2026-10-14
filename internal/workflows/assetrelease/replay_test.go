package assetrelease

import (
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"testing"
)

func TestReplayRelease(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(Run, workflow.RegisterOptions{Name: Name})
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, "testdata/release.json"); err != nil {
		t.Fatal(err)
	}
}

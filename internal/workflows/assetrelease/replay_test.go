package assetrelease

import (
	"path/filepath"
	"testing"

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func TestReplayRelease(t *testing.T) {
	paths, err := filepath.Glob("testdata/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) < 5 {
		t.Fatalf("missing release branch histories: %d", len(paths))
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			replayer := worker.NewWorkflowReplayer()
			replayer.RegisterWorkflowWithOptions(Run, workflow.RegisterOptions{Name: Name})
			if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path); err != nil {
				t.Fatal(err)
			}
		})
	}
}

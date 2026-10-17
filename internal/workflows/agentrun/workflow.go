package agentrun

import (
	"go.temporal.io/sdk/workflow"

	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/segloop"
)

func Run(ctx workflow.Context, in w.RunInput) error {
	return segloop.Run(ctx, in, segloop.Hooks{})
}

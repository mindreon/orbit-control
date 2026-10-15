package agentrun

import (
	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/segloop"
	"go.temporal.io/sdk/workflow"
)

func Run(ctx workflow.Context, in w.RunInput) error { return segloop.Run(ctx, in) }

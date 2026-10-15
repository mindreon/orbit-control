package runworker

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/activities/ctl"
	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/agentrun"
	"github.com/mindreon/orbit-control/internal/workflows/sandboxlease"
	"github.com/mindreon/orbit-control/internal/workflows/session"
)

func Register(reg worker.Registry, a *ctl.Activities) {
	reg.RegisterWorkflowWithOptions(session.Run, workflow.RegisterOptions{Name: w.SessionWorkflow})
	reg.RegisterWorkflowWithOptions(agentrun.Run, workflow.RegisterOptions{Name: w.AgentRunWorkflow})
	reg.RegisterWorkflowWithOptions(sandboxlease.Run, workflow.RegisterOptions{Name: w.SandboxLeaseWorkflow})
	reg.RegisterActivity(a)
}
func New(c client.Client, a *ctl.Activities) worker.Worker {
	worker := worker.New(
		c,
		contract.TaskQueueCtl,
		worker.Options{MaxConcurrentActivityExecutionSize: 64, MaxConcurrentWorkflowTaskExecutionSize: 32},
	)
	Register(worker, a)
	return worker
}

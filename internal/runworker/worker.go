package runworker

import (
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/activities/ctl"
	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/agentrun"
	"github.com/mindreon/orbit-control/internal/workflows/membertask"
	"github.com/mindreon/orbit-control/internal/workflows/sandboxlease"
	"github.com/mindreon/orbit-control/internal/workflows/scheduledrun"
	"github.com/mindreon/orbit-control/internal/workflows/session"
	"github.com/mindreon/orbit-control/internal/workflows/teamrun"
)

func Register(reg worker.Registry, a *ctl.Activities) {
	reg.RegisterWorkflowWithOptions(session.Run, workflow.RegisterOptions{Name: w.SessionWorkflow})
	reg.RegisterWorkflowWithOptions(agentrun.Run, workflow.RegisterOptions{Name: w.AgentRunWorkflow})
	reg.RegisterWorkflowWithOptions(sandboxlease.Run, workflow.RegisterOptions{Name: w.SandboxLeaseWorkflow})
	reg.RegisterWorkflowWithOptions(teamrun.Run, workflow.RegisterOptions{Name: w.TeamRunWorkflow})
	reg.RegisterWorkflowWithOptions(membertask.Run, workflow.RegisterOptions{Name: w.MemberTaskWorkflow})
	reg.RegisterWorkflowWithOptions(scheduledrun.Run, workflow.RegisterOptions{Name: w.ScheduledRunWorkflow})
	reg.RegisterActivity(a)
}
func workerOptions() worker.Options {
	options := teamrun.WorkerOptions()
	options.MaxConcurrentActivityExecutionSize = 64
	options.MaxConcurrentWorkflowTaskExecutionSize = 32
	return options
}
func New(c client.Client, a *ctl.Activities) worker.Worker {
	worker := worker.New(c, contract.TaskQueueCtl, workerOptions())
	Register(worker, a)
	return worker
}

func RegisterWait(reg worker.Registry, a *ctl.Activities) {
	reg.RegisterActivityWithOptions(a.WaitForRun, activity.RegisterOptions{Name: "WaitForRun"})
	reg.RegisterActivityWithOptions(a.WaitForTurn, activity.RegisterOptions{Name: "WaitForTurn"})
}
func NewWait(c client.Client, a *ctl.Activities, concurrency int) worker.Worker {
	if concurrency <= 0 {
		concurrency = 4096
	}
	result := worker.New(c, w.TaskQueueCtlWait, worker.Options{MaxConcurrentActivityExecutionSize: concurrency})
	RegisterWait(result, a)
	return result
}

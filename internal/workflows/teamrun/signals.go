package teamrun

import (
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	w "github.com/mindreon/orbit-control/internal/workflows"
)

const pauseSignal = "TeamPauseState"

type pauseInput struct {
	Name  string
	Actor w.ActorInput
}

// SignalInterceptor merges Pause and Resume before the SDK buffers them by name.
// HandleSignal runs in history order, including signals delivered in one workflow task.
// Restricting this to TeamRun leaves AgentRun's recorded command paths unchanged.
type SignalInterceptor struct {
	interceptor.WorkerInterceptorBase
}

func (*SignalInterceptor) InterceptWorkflow(
	_ workflow.Context, next interceptor.WorkflowInboundInterceptor,
) interceptor.WorkflowInboundInterceptor {
	return &signalInbound{WorkflowInboundInterceptorBase: interceptor.WorkflowInboundInterceptorBase{Next: next}}
}

type signalInbound struct {
	interceptor.WorkflowInboundInterceptorBase
}

func (s *signalInbound) HandleSignal(ctx workflow.Context, in *interceptor.HandleSignalInput) error {
	if workflow.GetInfo(ctx).WorkflowType.Name != w.TeamRunWorkflow || (in.SignalName != w.Pause && in.SignalName != w.Resume) {
		return s.Next.HandleSignal(ctx, in)
	}
	var actor w.ActorInput
	if err := converter.GetDefaultDataConverter().FromPayloads(in.Arg, &actor); err != nil {
		return err
	}
	arg, err := converter.GetDefaultDataConverter().ToPayloads(pauseInput{Name: in.SignalName, Actor: actor})
	if err != nil {
		return err
	}
	return s.Next.HandleSignal(ctx, &interceptor.HandleSignalInput{SignalName: pauseSignal, Arg: arg})
}

// WorkerOptions installs the TeamRun signal-ordering interceptor. It uses Temporal's
// default data converter; this must be updated if a payload codec is introduced.
func WorkerOptions() worker.Options {
	return worker.Options{Interceptors: []interceptor.WorkerInterceptor{&SignalInterceptor{}}}
}

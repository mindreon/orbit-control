package teamrun

import (
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
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

func (r *run) registerPauseSignals(ctx workflow.Context) {
	workflow.Go(ctx, func(ctx workflow.Context) {
		ch := workflow.GetSignalChannel(ctx, pauseSignal)
		for ctx.Err() == nil {
			var in pauseInput
			ch.Receive(ctx, &in)
			r.st.Paused, r.pauseChanged = in.Name == w.Pause, true
			for _, member := range workflow.DeterministicKeys(r.st.Active) {
				r.signal(ctx, r.st.Active[member], in.Name, in.Actor)
			}
		}
	})
}
func (r *run) pausePending(ctx workflow.Context) bool {
	return workflow.GetSignalChannel(ctx, pauseSignal).Len() > 0
}

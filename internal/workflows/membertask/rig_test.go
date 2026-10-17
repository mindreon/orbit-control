package membertask

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

const sec = time.Second

type step struct {
	after  time.Duration
	result contract.SegmentResult
	err    error
}

// rig runs MemberTaskWorkflow with recording Activities and a scripted sequence of segment results.
type rig struct {
	t            *testing.T
	env          *testsuite.TestWorkflowEnvironment
	in           w.MemberTaskInput
	mu           sync.Mutex
	segments     []contract.SegmentInput
	acquires     []w.AcquireSandboxInput
	interactions []w.InteractionInput
	cancelled    []w.NotifyInput
	infos        []activity.Info
	calls        []string
	forbidden    []string
}

func completed(used int) contract.SegmentResult {
	return contract.SegmentResult{V: 1, Status: "completed", CheckpointVersion: used, Progress: contract.SegmentProgress{NewOutput: true}}
}
func yielded(version int) contract.SegmentResult {
	return contract.SegmentResult{V: 1, Status: "yielded", CheckpointVersion: version, Progress: contract.SegmentProgress{NewOutput: true}}
}
func newRig(t *testing.T, steps ...step) *rig {
	t.Helper()
	r := &rig{t: t}
	var suite testsuite.WorkflowTestSuite
	r.env = suite.NewTestWorkflowEnvironment()
	r.env.SetTestTimeout(30 * time.Second)
	r.env.SetStartWorkflowOptions(client.StartWorkflowOptions{ID: w.MemberTaskWorkflowID("tenant", "run", "alice", 2)})
	r.in = w.MemberTaskInput{V: 1, TenantID: "tenant", RunID: "run", SessionID: "session", Member: "alice", Role: "teammate",
		Turn: 2, Kind: "task", Cursor: w.MemberCursor{Seq: 4, CheckpointVersion: 7, EventN: 30},
		Lock:         contract.SegmentLockRef{Id: "lock", Digest: "sha256:" + strings.Repeat("0", 64)},
		EventSubject: "run.tenant.session.run", WorkspaceID: "workspace", PrincipalID: "owner",
		BudgetMillicredits: 10000, Limits: w.DefaultLimits(10000), Tier: "standard",
		Input: contract.SegmentTurn{Kind: "follow_up"}, Sandbox: w.SandboxSpec{IsolationKey: "isolation"},
		Lease: contract.SandboxLease{Provider: "local", SandboxId: "sandbox"}}
	r.forbidden = []string{"PrepareRun", "FinalizeRun"}
	for _, name := range r.forbidden {
		r.env.RegisterActivityWithOptions(func(context.Context, w.RunOp) error { return nil }, activity.RegisterOptions{Name: name})
	}
	r.env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, args converter.EncodedValues) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.calls = append(r.calls, info.ActivityType.Name)
		r.infos = append(r.infos, *info)
		if info.ActivityType.Name == contract.ActivityRunAgentSegment {
			var in contract.SegmentInput
			if args.Get(&in) == nil {
				r.segments = append(r.segments, in)
			}
		}
	})
	r.env.RegisterActivityWithOptions(func(context.Context, w.StartSegmentInput) error { return nil },
		activity.RegisterOptions{Name: "StartSegment"})
	r.env.RegisterActivityWithOptions(func(context.Context, w.RecordSegmentInput) (w.RecordSegmentOutput, error) {
		return w.RecordSegmentOutput{RunUsedMillicredits: 200}, nil
	}, activity.RegisterOptions{Name: "RecordSegment"})
	r.env.RegisterActivityWithOptions(func(_ context.Context, in w.AcquireSandboxInput) (contract.SandboxLease, error) {
		r.mu.Lock()
		r.acquires = append(r.acquires, in)
		r.mu.Unlock()
		return contract.SandboxLease{Provider: "local", SandboxId: "replacement"}, nil
	}, activity.RegisterOptions{Name: "AcquireSandbox"})
	r.env.RegisterActivityWithOptions(func(_ context.Context, in w.InteractionInput) (w.InteractionOutput, error) {
		r.mu.Lock()
		r.interactions = append(r.interactions, in)
		r.mu.Unlock()
		return w.InteractionOutput{}, nil
	}, activity.RegisterOptions{Name: "CreateInteraction"})
	r.env.RegisterActivityWithOptions(func(_ context.Context, in w.NotifyInput) error {
		r.mu.Lock()
		r.cancelled = append(r.cancelled, in)
		r.mu.Unlock()
		return nil
	}, activity.RegisterOptions{Name: "CancelInteraction"})
	r.env.RegisterActivityWithOptions(func(context.Context, w.DecisionInput) error { return nil },
		activity.RegisterOptions{Name: "ApplyDecision"})
	r.env.RegisterActivityWithOptions(func(context.Context, contract.SegmentInput) (contract.SegmentResult, error) {
		return contract.SegmentResult{}, nil
	}, activity.RegisterOptions{Name: contract.ActivityRunAgentSegment})
	r.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", w.Touch, mock.Anything).Return(nil)
	for _, item := range steps {
		call := r.env.OnActivity(contract.ActivityRunAgentSegment, mock.Anything, mock.Anything).Once()
		if item.after > 0 {
			call = call.After(item.after)
		}
		call.Return(item.result, item.err)
	}
	return r
}
func (r *rig) at(after time.Duration, fn func()) { r.env.RegisterDelayedCallback(fn, after) }
func (r *rig) run() (w.TurnResult, error) {
	r.env.ExecuteWorkflow(Run, r.in)
	require.True(r.t, r.env.IsWorkflowCompleted())
	if err := r.env.GetWorkflowError(); err != nil {
		return w.TurnResult{}, err
	}
	var result w.TurnResult
	require.NoError(r.t, r.env.GetWorkflowResult(&result))
	return result, nil
}
func (r *rig) called(name string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, call := range r.calls {
		if call == name {
			n++
		}
	}
	return n
}
func sandboxUnavailable() error {
	return temporal.NewNonRetryableApplicationError("sandbox lost", contract.ErrorSandboxUnavailable, errors.New("lost"))
}
func mail(id, text string) contract.MailboxMessage {
	return contract.MailboxMessage{MessageId: id, From: "leader", Text: new(text), SentAt: time.Unix(1, 0)}
}
func deliverMail(r *rig, id string) {
	r.env.SignalWorkflow(w.Deliver, w.DeliverInput{Mailbox: []contract.MailboxMessage{mail(id, "hello")}})
}
func endTurn(r *rig) { r.env.SignalWorkflow(w.EndTurn, struct{}{}) }

var _ = workflow.Now

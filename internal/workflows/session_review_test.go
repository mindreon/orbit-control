package workflows_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/session"
)

type sessionHarness struct {
	env       *testsuite.TestWorkflowEnvironment
	in        w.SessionInput
	started   []w.RunInput
	cancelled []w.CancelInput
	finished  []w.FinalizeInput
	outputs   []w.SubmitMessageOutput
}

func newSessionHarness(t *testing.T, archiveErrors ...error) *sessionHarness {
	t.Helper()
	return sessionHarnessWithArchiveDelay(t, 0, archiveErrors...)
}
func sessionHarnessWithArchiveDelay(t *testing.T, delay time.Duration, archiveErrors ...error) *sessionHarness {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	h := &sessionHarness{env: suite.NewTestWorkflowEnvironment(), in: w.SessionInput{
		V: 1, TenantID: "tenant", WorkspaceID: "workspace", SessionID: "session", Policy: "queue"}}
	h.env.RegisterWorkflowWithOptions(func(ctx workflow.Context, in w.RunInput) error {
		h.started = append(h.started, in)
		var cancel w.CancelInput
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(workflow.GetSignalChannel(ctx, w.Cancel), func(ch workflow.ReceiveChannel, _ bool) {
			ch.Receive(ctx, &cancel)
			h.cancelled = append(h.cancelled, cancel)
		})
		selector.AddFuture(workflow.NewTimer(ctx, 10*time.Second), func(workflow.Future) {})
		selector.Select(ctx)
		return nil
	}, workflow.RegisterOptions{Name: w.AgentRunWorkflow})
	h.env.RegisterActivityWithOptions(func(_ context.Context, in w.FinalizeInput) error {
		h.finished = append(h.finished, in)
		return nil
	}, activity.RegisterOptions{Name: "FinalizeRun"})
	var archiveError error
	if len(archiveErrors) > 0 {
		archiveError = archiveErrors[0]
	}
	h.env.OnSignalExternalWorkflow(mock.Anything, mock.Anything, "", w.Archive, mock.Anything).Return(archiveError).After(delay)
	return h
}
func sessionMessage(id string) w.SubmitMessageInput {
	return w.SubmitMessageInput{
		V:     1,
		RunID: id,
		Lock:  contract.SegmentLockRef{Id: "lock", Digest: "sha256:" + strings.Repeat("0", 64)},
	}
}
func (h *sessionHarness) submit(t *testing.T, key, id string) {
	t.Helper()
	h.env.UpdateWorkflow(w.SubmitMessage, key, &testsuite.TestUpdateCallback{
		OnReject: func(err error) { require.NoError(t, err) }, OnComplete: func(out any, err error) {
			require.NoError(t, err)
			h.outputs = append(h.outputs, out.(w.SubmitMessageOutput))
		}}, sessionMessage(id))
}
func TestSessionIdentityQueueAndUpdateDeduplication(t *testing.T) {
	h := newSessionHarness(t)
	h.env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, _ workflow.Context, args converter.EncodedValues) {
		var input w.RunInput
		require.NoError(t, args.Get(&input))
		require.Equal(t, w.AgentRunWorkflowID("tenant", input.RunID), info.WorkflowExecution.ID)
		require.Equal(t, "main", string(input.Member))
		require.NotEmpty(t, info.SearchAttributes.IndexedFields["TenantId"])
		require.NotEmpty(t, info.SearchAttributes.IndexedFields["WorkspaceId"])
	})
	h.env.RegisterDelayedCallback(func() {
		h.submit(t, "first", "one")
		h.submit(t, "first", "one")
	}, 0)
	h.env.RegisterDelayedCallback(func() { h.submit(t, "second", "two") }, time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(w.Archive, w.ArchiveInput{}) }, 21*time.Second)
	h.env.ExecuteWorkflow(session.Run, h.in)
	require.NoError(t, h.env.GetWorkflowError())
	require.Len(t, h.started, 2)
	require.Equal(t, "one", h.started[0].RunID)
	require.Equal(t, "two", h.started[1].RunID)
	require.Equal(t, 1, h.outputs[len(h.outputs)-1].Position)
}
func TestSessionActiveCancellationAndInterrupt(t *testing.T) {
	for _, mode := range []string{"cancel", "interrupt", "archive"} {
		t.Run(mode, func(t *testing.T) {
			h := newSessionHarness(t)
			if mode == "interrupt" {
				h.in.Policy = "interrupt"
			}
			h.env.RegisterDelayedCallback(func() { h.submit(t, "one", "one") }, 0)
			h.env.RegisterDelayedCallback(func() {
				switch mode {
				case "cancel":
					h.env.UpdateWorkflow(w.CancelRun, "cancel", &testsuite.TestUpdateCallback{OnComplete: func(out any, err error) {
						require.NoError(t, err)
						require.Equal(t, "cancelling", out.(w.CancelRunOutput).Disposition)
					}}, w.CancelRunInput{RunID: "one", Reason: "stop"})
				case "interrupt":
					h.submit(t, "two", "two")
				case "archive":
					h.env.SignalWorkflow(w.Archive, w.ArchiveInput{})
				}
			}, time.Second)
			h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(w.Archive, w.ArchiveInput{}) }, 12*time.Second)
			h.env.ExecuteWorkflow(session.Run, h.in)
			require.NoError(t, h.env.GetWorkflowError())
			require.Len(t, h.cancelled, 1)
			if mode == "interrupt" {
				require.Len(t, h.started, 2)
				require.Equal(t, "two", h.started[1].RunID)
			}
		})
	}
}
func TestSessionContinueCarriesQueue(t *testing.T) {
	h := newSessionHarness(t)
	for i := range w.SessionContinueAfter {
		h.env.RegisterDelayedCallback(func() { h.submit(t, fmt.Sprint(i), fmt.Sprint(i)) }, time.Duration(i)*10*time.Second)
	}
	h.env.RegisterDelayedCallback(func() { h.submit(t, "carried", "carried") },
		time.Duration(w.SessionContinueAfter-1)*10*time.Second+time.Second)
	h.env.ExecuteWorkflow(session.Run, h.in)
	can, ok := errors.AsType[*workflow.ContinueAsNewError](h.env.GetWorkflowError())
	require.True(t, ok)
	var input w.SessionInput
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(can.Input, &input))
	require.NotNil(t, input.Carry)
	require.Equal(t, "queue", input.Carry.Policy)
	require.Len(t, input.Carry.Queue, 1)
	require.Equal(t, "carried", input.Carry.Queue[0].RunID)
	require.Len(t, h.started, w.SessionContinueAfter)
}
func TestSessionWaitsForCancelHandlerBeforeContinue(t *testing.T) {
	h := newSessionHarness(t)
	h.env.OnActivity("FinalizeRun", mock.Anything, mock.Anything).Return(nil).After(5 * time.Second)
	h.env.RegisterDelayedCallback(func() { h.submit(t, "one", "one") }, 0)
	h.env.RegisterDelayedCallback(func() { h.submit(t, "two", "two") }, time.Second)
	h.env.RegisterDelayedCallback(func() {
		h.env.UpdateWorkflow(w.CancelRun, "cancel", &testsuite.TestUpdateCallback{OnComplete: func(_ any, err error) {
			require.NoError(t, err)
		}}, w.CancelRunInput{RunID: "two"})
		h.env.SetContinueAsNewSuggested(true)
	}, 8*time.Second)
	h.env.ExecuteWorkflow(session.Run, h.in)
	require.True(t, workflow.IsContinueAsNewError(h.env.GetWorkflowError()))
	require.Len(t, h.started, 1)
}
func TestSessionLeaseArchiveFailure(t *testing.T) {
	h := newSessionHarness(t, temporal.NewNonRetryableApplicationError("offline", "unavailable", nil))
	h.env.ExecuteWorkflow(session.Run, h.in)
	require.ErrorContains(t, h.env.GetWorkflowError(), "offline")
}
func TestSessionAlreadyStartedWaitsForExistingRun(t *testing.T) {
	h := newSessionHarness(t)
	h.env.OnWorkflow(w.AgentRunWorkflow, mock.Anything, mock.Anything).Return(&temporal.ChildWorkflowExecutionAlreadyStartedError{})
	waited := false
	h.env.RegisterActivityWithOptions(func(_ context.Context, in w.RunOp) error {
		require.Equal(t, "one", in.RunID)
		waited = true
		return nil
	}, activity.RegisterOptions{Name: "WaitForRun"})
	h.env.RegisterDelayedCallback(func() { h.submit(t, "one", "one") }, 0)
	h.env.ExecuteWorkflow(session.Run, h.in)
	require.NoError(t, h.env.GetWorkflowError())
	require.True(t, waited)
}

func TestArchivedSessionRejectsNewMessages(t *testing.T) {
	h := newSessionHarness(t)
	h.env.OnActivity("FinalizeRun", mock.Anything, mock.Anything).Return(nil).After(5 * time.Second)
	h.env.RegisterDelayedCallback(func() { h.submit(t, "one", "one") }, 0)
	h.env.RegisterDelayedCallback(func() { h.submit(t, "two", "two") }, time.Second)
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(w.Archive, w.ArchiveInput{}) }, 2*time.Second)
	rejected := false
	h.env.RegisterDelayedCallback(func() {
		h.env.UpdateWorkflow(w.SubmitMessage, "late", &testsuite.TestUpdateCallback{
			OnReject: func(err error) {
				require.Error(t, err)
				rejected = true
			},
			OnAccept: func() { t.Error("archived session admitted another run") },
		}, sessionMessage("late"))
	}, 3*time.Second)
	h.env.ExecuteWorkflow(session.Run, h.in)
	require.NoError(t, h.env.GetWorkflowError())
	require.True(t, rejected)
	require.Len(t, h.started, 1)
}

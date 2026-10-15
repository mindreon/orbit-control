package ctl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func TestAcquireWaitsForClosingExecution(t *testing.T) {
	for _, waitFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "closed", true: "cancelled"}[waitFails], func(t *testing.T) {
			tc := mocks.NewClient(t)
			closing := mocks.NewWorkflowUpdateHandle(t)
			ready := mocks.NewWorkflowUpdateHandle(t)
			old := mocks.NewWorkflowRun(t)
			closed := false
			tc.On("NewWithStartWorkflowOperation", mock.Anything, w.SandboxLeaseWorkflow, mock.Anything).Return(nil)
			tc.On("UpdateWithStartWorkflow", mock.Anything, mock.Anything).Return(closing, nil).Once()
			closing.On("Get", mock.Anything, mock.Anything).Return(temporal.NewApplicationError("closing", "LEASE_CLOSING"))
			closing.On("WorkflowID").Return("lease-id")
			closing.On("RunID").Return("old-execution")
			tc.On("GetWorkflow", mock.Anything, "lease-id", "old-execution").Return(old)
			waitErr := error(nil)
			if waitFails {
				waitErr = context.Canceled
			}
			old.On("Get", mock.Anything, mock.Anything).Run(func(mock.Arguments) { closed = true }).Return(waitErr)
			if !waitFails {
				tc.On("UpdateWithStartWorkflow", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
					require.True(t, closed, "UpdateWithStart must follow old execution close")
					opts := args[1].(client.UpdateWithStartWorkflowOptions)
					require.Equal(t, "run:run:acq:0", opts.UpdateOptions.UpdateID)
				}).Return(ready, nil).Once()
				ready.On("Get", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
					*args[1].(*contract.SandboxLease) = contract.SandboxLease{Provider: "local", SandboxId: "new-box"}
				}).Return(nil)
			}
			a := Activities{Temporal: tc}
			lease, err := a.acquireLease(t.Context(), w.AcquireSandboxInput{RunOp: w.RunOp{RunID: "run"}}, w.AcquireInput{})
			if waitFails {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.NoError(t, err)
				require.Equal(t, "new-box", lease.SandboxId)
			}
		})
	}
}

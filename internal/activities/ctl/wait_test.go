package ctl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/mocks"
	"go.temporal.io/sdk/temporal"

	w "github.com/mindreon/orbit-control/internal/workflows"
)

func TestWaitForRunClosedVersusTransportError(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result error
		closed bool
	}{
		{name: "success", closed: true},
		{name: "failed execution", result: &temporal.WorkflowExecutionError{}, closed: true},
		{name: "transport failure", result: serviceerror.NewUnavailable("offline")},
		{name: "cancelled wait", result: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := mocks.NewClient(t)
			run := mocks.NewWorkflowRun(t)
			client.On("GetWorkflow", mock.Anything, w.AgentRunWorkflowID("tenant", "run"), "").Return(run).Once()
			run.On("Get", mock.Anything, nil).Return(tc.result).Once()
			a := &Activities{Temporal: client}
			err := a.WaitForRun(t.Context(), w.RunOp{TenantID: "tenant", RunID: "run"})
			if tc.closed {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.result)
			}
		})
	}
}

package temporalx

import (
	"context"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

const (
	TenantID    = "TenantId"
	WorkspaceID = "WorkspaceId"
	RunKind     = "RunKind"
)

type Client interface {
	Submit(context.Context, w.SessionInput, w.SubmitMessageInput) (w.SubmitMessageOutput, error)
	Update(context.Context, string, string, string, any, any) error
	Signal(context.Context, string, string, any) error
	QuerySession(context.Context, string) (w.SessionState, error)
}
type Service struct{ Client client.Client }

func (s Service) Submit(ctx context.Context, in w.SessionInput, msg w.SubmitMessageInput) (w.SubmitMessageOutput, error) {
	var out w.SubmitMessageOutput
	start := s.Client.NewWithStartWorkflowOperation(
		client.StartWorkflowOptions{
			ID:                       w.SessionWorkflowID(in.TenantID, in.SessionID),
			TaskQueue:                contract.TaskQueueCtl,
			WorkflowIDConflictPolicy: enums.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			WorkflowIDReusePolicy:    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			TypedSearchAttributes: temporal.NewSearchAttributes(
				temporal.NewSearchAttributeKeyKeyword(TenantID).ValueSet(in.TenantID),
				temporal.NewSearchAttributeKeyKeyword(WorkspaceID).ValueSet(in.WorkspaceID),
				temporal.NewSearchAttributeKeyKeyword(RunKind).ValueSet("session"),
			),
		},
		w.SessionWorkflow,
		in,
	)
	handle, err := s.Client.UpdateWithStartWorkflow(
		ctx,
		client.UpdateWithStartWorkflowOptions{
			StartWorkflowOperation: start,
			UpdateOptions: client.UpdateWorkflowOptions{
				UpdateName:   w.SubmitMessage,
				UpdateID:     msg.RunID,
				WaitForStage: client.WorkflowUpdateStageCompleted,
				Args:         []any{msg},
			},
		},
	)
	if err != nil {
		return out, err
	}
	err = handle.Get(ctx, &out)
	return out, err
}
func (s Service) Update(ctx context.Context, id, name, key string, in, out any) error {
	handle, err := s.Client.UpdateWorkflow(
		ctx,
		client.UpdateWorkflowOptions{
			WorkflowID:   id,
			UpdateName:   name,
			UpdateID:     key,
			WaitForStage: client.WorkflowUpdateStageCompleted,
			Args:         []any{in},
		},
	)
	if err != nil {
		return err
	}
	return handle.Get(ctx, out)
}
func (s Service) Signal(ctx context.Context, id, name string, in any) error {
	return s.Client.SignalWorkflow(ctx, id, "", name, in)
}
func (s Service) QuerySession(ctx context.Context, id string) (w.SessionState, error) {
	var out w.SessionState
	value, err := s.Client.QueryWorkflow(ctx, id, "", w.State)
	if err != nil {
		return out, err
	}
	err = value.Get(&out)
	return out, err
}

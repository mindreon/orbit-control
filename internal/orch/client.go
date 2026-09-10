// Package orch is the Temporal client used by control to drive RoomWorkflow.
// When TEMPORAL_ADDRESS is unset, control falls back to direct worker HTTP.
package orch

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"
)

const (
	DefaultTaskQueue   = "orbit"
	WorkflowRoomPrefix = "room:"
)

type Client struct {
	tc        client.Client
	taskQueue string
}

type RoomView struct {
	RoomID                   string `json:"roomId"`
	State                    string `json:"state"`
	SessionID                string `json:"sessionId"`
	Kind                     string `json:"kind"`
	PendingApprovalRequestID string `json:"pendingApprovalRequestId,omitempty"`
}

type RunTurnResult struct {
	Status   string         `json:"status"`
	Approval *ApprovalAsk   `json:"approval,omitempty"`
	Texts    []string       `json:"texts,omitempty"`
}

type ApprovalAsk struct {
	ApprovalRequestID string `json:"approvalRequestId"`
	ToolName          string `json:"toolName"`
	CallID            string `json:"callId,omitempty"`
	Reason            string `json:"reason,omitempty"`
}

type DecideResult struct {
	Decision string         `json:"decision"`
	Turn     *RunTurnResult `json:"turn,omitempty"`
}

func Dial(address, namespace, taskQueue string) (*Client, error) {
	if address == "" {
		return nil, fmt.Errorf("temporal address is empty")
	}
	if namespace == "" {
		namespace = "default"
	}
	if taskQueue == "" {
		taskQueue = DefaultTaskQueue
	}
	tc, err := client.Dial(client.Options{
		HostPort:  address,
		Namespace: namespace,
	})
	if err != nil {
		return nil, err
	}
	return &Client{tc: tc, taskQueue: taskQueue}, nil
}

func (c *Client) Close() {
	if c != nil && c.tc != nil {
		c.tc.Close()
	}
}

func WorkflowID(roomID string) string {
	return WorkflowRoomPrefix + roomID
}

func (c *Client) StartRoom(ctx context.Context, roomID, kind string) (RoomView, error) {
	if kind == "" {
		kind = "solo"
	}
	_, err := c.tc.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       WorkflowID(roomID),
		TaskQueue:                c.taskQueue,
		WorkflowExecutionTimeout: 24 * time.Hour,
	}, "RoomWorkflow", map[string]any{
		"roomId": roomID,
		"kind":   kind,
	})
	if err != nil {
		return RoomView{}, err
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		view, qerr := c.GetView(ctx, roomID)
		if qerr == nil && view.SessionID != "" {
			return view, nil
		}
		select {
		case <-ctx.Done():
			return RoomView{}, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return RoomView{}, fmt.Errorf("timed out waiting for RoomWorkflow session on %s", roomID)
}

func (c *Client) GetView(ctx context.Context, roomID string) (RoomView, error) {
	resp, err := c.tc.QueryWorkflow(ctx, WorkflowID(roomID), "", "getRoomView")
	if err != nil {
		return RoomView{}, err
	}
	var view RoomView
	if err := resp.Get(&view); err != nil {
		return RoomView{}, err
	}
	return view, nil
}

func (c *Client) RunTurn(ctx context.Context, roomID, turnID, message string) (RunTurnResult, error) {
	handle, err := c.tc.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   WorkflowID(roomID),
		UpdateName:   "runTurn",
		WaitForStage: client.WorkflowUpdateStageCompleted,
		Args: []any{map[string]any{
			"turnId":  turnID,
			"message": message,
		}},
	})
	if err != nil {
		return RunTurnResult{}, err
	}
	var out RunTurnResult
	if err := handle.Get(ctx, &out); err != nil {
		return RunTurnResult{}, err
	}
	return out, nil
}

func (c *Client) Decide(ctx context.Context, roomID, turnID, approvalRequestID, decision, resumeTurnID string) (DecideResult, error) {
	handle, err := c.tc.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   WorkflowID(roomID),
		UpdateName:   "decide",
		WaitForStage: client.WorkflowUpdateStageCompleted,
		Args: []any{map[string]any{
			"turnId":             turnID,
			"approvalRequestId":  approvalRequestID,
			"decision":           decision,
			"resumeTurnId":       resumeTurnID,
		}},
	})
	if err != nil {
		return DecideResult{}, err
	}
	var out DecideResult
	if err := handle.Get(ctx, &out); err != nil {
		return DecideResult{}, err
	}
	return out, nil
}

func (c *Client) Abort(ctx context.Context, roomID, turnID, reason string) error {
	return c.tc.SignalWorkflow(ctx, WorkflowID(roomID), "", "abort", map[string]any{
		"turnId": turnID,
		"reason": reason,
	})
}

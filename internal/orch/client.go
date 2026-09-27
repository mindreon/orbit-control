// Package orch is the Temporal client used by control to drive RoomWorkflow.
// When TEMPORAL_ADDRESS is unset, control falls back to direct worker HTTP.
package orch

import (
	"context"
	"encoding/json"
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

// McpHeaderRef is a header name plus the environment variable that holds its value.
type McpHeaderRef struct {
	Name string `json:"name"`
	Env  string `json:"env"`
}

// McpConnectorSpec is the connector payload on RoomWorkflow. Values are absent.
type McpConnectorSpec struct {
	ID         string         `json:"id"`
	Name       string         `json:"name"`
	Transport  string         `json:"transport"`
	Command    string         `json:"command,omitempty"`
	Args       []string       `json:"args,omitempty"`
	EnvRefs    []string       `json:"envRefs,omitempty"`
	URL        string         `json:"url,omitempty"`
	HeaderRefs []McpHeaderRef `json:"headerRefs,omitempty"`
}

type RoomView struct {
	RoomID                   string `json:"roomId"`
	State                    string `json:"state"`
	SessionID                string `json:"sessionId"`
	Kind                     string `json:"kind"`
	PendingApprovalRequestID string `json:"pendingApprovalRequestId,omitempty"`
}

type RunTurnResult struct {
	Status   string       `json:"status"`
	Approval *ApprovalAsk `json:"approval,omitempty"`
	Texts    []string     `json:"texts,omitempty"`
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

func (c *Client) StartRoom(ctx context.Context, roomID, kind, permissionPreset string, connectors []McpConnectorSpec) (RoomView, error) {
	if kind == "" {
		kind = "solo"
	}
	if permissionPreset == "" {
		permissionPreset = "workspace-write"
	}
	input := map[string]any{
		"roomId":           roomID,
		"kind":             kind,
		"permissionPreset": permissionPreset,
	}
	if len(connectors) > 0 {
		input["mcpConnectors"] = connectors
	}
	_, err := c.tc.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       WorkflowID(roomID),
		TaskQueue:                c.taskQueue,
		WorkflowExecutionTimeout: 24 * time.Hour,
	}, "RoomWorkflow", input)
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

// DecideUpdate is a decide Update that RoomWorkflow has accepted.
type DecideUpdate interface {
	// Result waits for the Update to complete, which includes the resumed turn.
	Result(ctx context.Context) (DecideResult, error)
}

type decideUpdate struct{ h client.WorkflowUpdateHandle }

func (d decideUpdate) Result(ctx context.Context) (DecideResult, error) {
	var out DecideResult
	if err := d.h.Get(ctx, &out); err != nil {
		return DecideResult{}, err
	}
	return out, nil
}

// DecideConfig is the decideConfig query. ttlS is the only TTL control uses
// when it classifies NotDelivered.
type DecideConfig struct {
	TTLS       int `json:"ttlS"`
	MaxDecided int `json:"maxDecided"`
}

// DecideOutcome is the small summary decideOutcome returns for a done id.
type DecideOutcome struct {
	Decision     string `json:"decision"`
	AgentID      string `json:"agentId"`
	ResumeTurnID string `json:"resumeTurnId"`
	TurnStatus   string `json:"turnStatus"`
	ErrorCode    string `json:"errorCode,omitempty"`
}

// Decide delivers a decision and returns as soon as RoomWorkflow has accepted
// the decide Update; ctx bounds acceptance only. UpdateID is the
// approvalRequestId (C35). The workflow derives resumeTurnId; the request
// does not send it. Repeated deliveries of one decision share that Update id.
func (c *Client) Decide(ctx context.Context, roomID, approvalRequestID, turnID, decision string) (DecideUpdate, error) {
	handle, err := c.tc.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		UpdateID:     approvalRequestID,
		WorkflowID:   WorkflowID(roomID),
		UpdateName:   "decide",
		WaitForStage: client.WorkflowUpdateStageAccepted,
		Args: []any{map[string]any{
			"turnId":            turnID,
			"approvalRequestId": approvalRequestID,
			"decision":          decision,
		}},
	})
	if err != nil {
		return nil, err
	}
	return decideUpdate{h: handle}, nil
}

func (c *Client) DecideConfig(ctx context.Context, roomID string) (DecideConfig, error) {
	resp, err := c.tc.QueryWorkflow(ctx, WorkflowID(roomID), "", "decideConfig")
	if err != nil {
		return DecideConfig{}, err
	}
	var cfg DecideConfig
	if err := resp.Get(&cfg); err != nil {
		return DecideConfig{}, err
	}
	return cfg, nil
}

// DecideOutcome reports whether decideOutcome has a done entry for the id.
// A query error is returned as-is so the caller can tell "not found" from
// "the query failed".
func (c *Client) DecideOutcome(ctx context.Context, roomID, approvalRequestID string) (bool, DecideOutcome, error) {
	resp, err := c.tc.QueryWorkflow(ctx, WorkflowID(roomID), "", "decideOutcome", approvalRequestID)
	if err != nil {
		return false, DecideOutcome{}, err
	}
	var raw []byte
	if err := resp.Get(&raw); err != nil {
		return false, DecideOutcome{}, err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return false, DecideOutcome{}, nil
	}
	var out DecideOutcome
	if err := json.Unmarshal(raw, &out); err != nil {
		return false, DecideOutcome{}, err
	}
	return true, out, nil
}

// ApprovalPending reports whether getRoomView still lists this id as the
// pending approval. A query error means the caller must not retry.
func (c *Client) ApprovalPending(ctx context.Context, roomID, approvalRequestID string) (bool, error) {
	view, err := c.GetView(ctx, roomID)
	if err != nil {
		return false, err
	}
	return approvalRequestID != "" && view.PendingApprovalRequestID == approvalRequestID, nil
}

func (c *Client) Steer(ctx context.Context, roomID, turnID, instruction string) error {
	return c.tc.SignalWorkflow(ctx, WorkflowID(roomID), "", "steer", map[string]any{
		"turnId":      turnID,
		"instruction": instruction,
	})
}

func (c *Client) Abort(ctx context.Context, roomID, turnID, reason string) error {
	return c.tc.SignalWorkflow(ctx, WorkflowID(roomID), "", "abort", map[string]any{
		"turnId": turnID,
		"reason": reason,
	})
}

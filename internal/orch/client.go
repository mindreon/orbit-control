// Package orch is the Temporal client control uses to drive TaskWorkflow.
package orch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

const (
	DefaultTaskQueue   = "orbit"
	WorkflowTaskPrefix = "task/"
)

type Client struct {
	tc        client.Client
	taskQueue string
}

// TaskWorkflowInput is the v3 start payload. Fields intentionally use the
// contract's snake_case names because the Python Pydantic data converter is
// the wire format for Temporal.
type TaskWorkflowInput struct {
	TaskID                  string         `json:"task_id"`
	TenantID                string         `json:"tenant_id"`
	CreatedBy               map[string]any `json:"created_by"`
	Title                   string         `json:"title"`
	Goal                    string         `json:"goal"`
	Mode                    string         `json:"mode"`
	Profile                 string         `json:"profile"`
	SOP                     string         `json:"sop,omitempty"`
	NodeTypeRegistryVersion int            `json:"node_type_registry_version"`
	Budgets                 map[string]any `json:"budgets"`
	Policy                  any            `json:"policy"`
	// Config is what the task runs with (15 M8). Built by the task package, never by hand, so that "not set" (null) and
	// "none" ([]) survive the trip: the generated contract types drop empty lists.
	Config map[string]any `json:"config,omitempty"`
}

type TaskView struct {
	TaskID           string         `json:"task_id"`
	Status           string         `json:"status"`
	PlanVersion      int            `json:"plan_version"`
	PendingApprovals []string       `json:"pending_approvals"`
	Budgets          map[string]any `json:"budgets"`
	Usage            map[string]any `json:"usage"`
	Config           map[string]any `json:"config"`
}

type TaskPlan struct {
	PlanVersion int              `json:"plan_version"`
	Hash        string           `json:"hash"`
	Nodes       []map[string]any `json:"nodes"`
	Edges       []map[string]any `json:"edges"`
}

func Dial(address, namespace, taskQueue string) (*Client, error) {
	return DialContext(context.Background(), address, namespace, taskQueue)
}

// DialContext is Dial that stops when ctx is cancelled.
func DialContext(ctx context.Context, address, namespace, taskQueue string) (*Client, error) {
	if address == "" {
		return nil, fmt.Errorf("temporal address is empty")
	}
	if namespace == "" {
		namespace = "default"
	}
	if taskQueue == "" {
		taskQueue = DefaultTaskQueue
	}
	tc, err := client.DialContext(ctx, client.Options{
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

func TaskWorkflowID(tenantID, taskID string) string {
	return WorkflowTaskPrefix + tenantID + "/" + taskID
}

func (c *Client) StartTask(ctx context.Context, inp TaskWorkflowInput) (TaskView, error) {
	if inp.Mode == "" {
		inp.Mode = "single"
	}
	if inp.NodeTypeRegistryVersion == 0 {
		inp.NodeTypeRegistryVersion = 1
	}
	if inp.Budgets == nil {
		inp.Budgets = map[string]any{}
	}
	_, err := c.tc.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       TaskWorkflowID(inp.TenantID, inp.TaskID),
		TaskQueue:                "orbit.orch",
		WorkflowExecutionTimeout: 30 * 24 * time.Hour,
	}, "TaskWorkflow", inp)
	if err != nil {
		return TaskView{}, err
	}
	return c.GetTaskView(ctx, inp.TenantID, inp.TaskID)
}

func (c *Client) GetTaskView(ctx context.Context, tenantID, taskID string) (TaskView, error) {
	resp, err := c.tc.QueryWorkflow(ctx, TaskWorkflowID(tenantID, taskID), "", "getTaskView")
	if err != nil {
		return TaskView{}, err
	}
	var view TaskView
	if err := resp.Get(&view); err != nil {
		return TaskView{}, err
	}
	return view, nil
}

func (c *Client) GetTaskPlan(ctx context.Context, tenantID, taskID string) (TaskPlan, error) {
	resp, err := c.tc.QueryWorkflow(ctx, TaskWorkflowID(tenantID, taskID), "", "getPlan")
	if err != nil {
		return TaskPlan{}, err
	}
	var plan TaskPlan
	if err := resp.Get(&plan); err != nil {
		return TaskPlan{}, err
	}
	return plan, nil
}

func (c *Client) UpdateTask(ctx context.Context, tenantID, taskID, updateName, updateID string, arg any) (json.RawMessage, error) {
	acceptedCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	handle, err := c.tc.UpdateWorkflow(acceptedCtx, client.UpdateWorkflowOptions{
		UpdateID:     updateID,
		WorkflowID:   TaskWorkflowID(tenantID, taskID),
		UpdateName:   updateName,
		WaitForStage: client.WorkflowUpdateStageAccepted,
		Args:         []any{arg},
	})
	if err != nil {
		return nil, classifyUpdateError(err)
	}
	if err := refusal(acceptedCtx, handle); err != nil {
		return nil, err
	}
	select {
	case <-acceptedCtx.Done():
		return json.RawMessage(fmt.Sprintf(`{"accepted":false,"command_id":%q}`, updateID)), acceptedCtx.Err()
	default:
		return json.RawMessage(fmt.Sprintf(`{"accepted":true,"command_id":%q}`, updateID)), nil
	}
}

func (c *Client) SignalTask(ctx context.Context, tenantID, taskID, signalName string, arg any) error {
	return c.tc.SignalWorkflow(ctx, TaskWorkflowID(tenantID, taskID), "", signalName, arg)
}

// What the workflow refuses an update for, as control tells them apart. The workflow raises these from its update
// validators, the only place a refusal reaches a caller that waits just for the update to be accepted.
var (
	ErrConfigConflict = errors.New("the task's configuration changed since it was read")
	ErrTaskClosed     = errors.New("task is closed")
)

func classifyUpdateError(err error) error {
	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		switch applicationErr.Type() {
		case "CONFIG_VERSION_CONFLICT":
			return ErrConfigConflict
		case "TASK_CLOSED":
			return ErrTaskClosed
		}
	}
	return err
}

// refusalWait is how long to look for a refusal. A validator's refusal is already part of the handle the SDK returns, so
// reading it is immediate; an update that is still being handled outlasts this and counts as accepted.
const refusalWait = 250 * time.Millisecond

// refusal tells whether the workflow refused an update it was asked to take. UpdateWorkflow returns a handle for a refused
// update just as for an accepted one; the refusal is the handle's error. Only the refusals control knows are reported:
// any other outcome stays what it was, accepted as far as the caller is told.
func refusal(ctx context.Context, handle client.WorkflowUpdateHandle) error {
	waitCtx, cancel := context.WithTimeout(ctx, refusalWait)
	defer cancel()
	var discard any
	err := handle.Get(waitCtx, &discard)
	if err == nil {
		return nil
	}
	if classified := classifyUpdateError(err); classified != err {
		return classified
	}
	return nil
}

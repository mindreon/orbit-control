// Package orch is the Temporal client control uses to drive TaskWorkflow.
package orch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
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
	// BudgetReserved is what running attempts hold of the budget and have not settled. It is live state of the workflow,
	// not something control projects.
	BudgetReserved map[string]any `json:"budget_reserved"`
	Config         map[string]any `json:"config"`
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
	if _, err := c.tc.ExecuteWorkflow(ctx, taskStartOptions(inp.TenantID, inp.TaskID), "TaskWorkflow", inp); err != nil {
		return TaskView{}, err
	}
	return startedView(inp), nil
}

// taskStartOptions are the options TaskWorkflow starts with. There is deliberately no execution or run timeout: the
// execution timeout spans Continue-As-New runs, and a task is a conversation that ends only when it is cancelled
// (approvals wait indefinitely, ADR-0009). The workflow id is the task id, so one task is one workflow: a start for an id
// that is already running attaches to that run instead of starting a second one (a retried create is harmless), and an
// id that has run before is never started again, so a cancelled task cannot be revived by a duplicate create.
func taskStartOptions(tenantID, taskID string) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		ID:                       TaskWorkflowID(tenantID, taskID),
		TaskQueue:                "orbit.orch",
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}
}

// startedView is the TaskView the workflow reports once it has started, built from its input. Creating a task must not
// wait on the orchestrator: a Query needs a workflow task, so it would block or fail whenever no orch worker is polling,
// even though the start itself was accepted and will run when one is. TaskWorkflow moves CREATED to RUNNING as its first
// step with the initial plan at version 1 (task_workflow.py); anything later reaches control through events.
func startedView(inp TaskWorkflowInput) TaskView {
	return TaskView{
		TaskID:           inp.TaskID,
		Status:           "RUNNING",
		PlanVersion:      1,
		PendingApprovals: []string{},
		Budgets:          inp.Budgets,
		Usage:            map[string]any{},
		Config:           inp.Config,
	}
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
	var result map[string]any
	if resultUpdates[updateName] {
		var err error
		if result, err = updateResult(ctx, handle, resultWait); err != nil {
			return nil, err
		}
	} else if err := refusal(acceptedCtx, handle); err != nil {
		return nil, err
	}
	select {
	case <-acceptedCtx.Done():
		return json.RawMessage(fmt.Sprintf(`{"accepted":false,"command_id":%q}`, updateID)), acceptedCtx.Err()
	default:
		return acceptedBody(updateID, result), nil
	}
}

// resultUpdates are the updates whose result the caller needs (RequestProfileSwitchResult, CompleteNodeResult): control
// waits for them to finish, up to resultWait, and returns the result with the acceptance. Every other update is
// answered as soon as it is accepted.
var resultUpdates = map[string]bool{"requestProfileSwitch": true, "completeNode": true}

// resultWait is how long to wait for the result of a resultUpdates update. Past it the caller is told the update was
// accepted, as for any other update, and finds the outcome in the task's events.
const resultWait = 3 * time.Second

// acceptedBody is the answer to an accepted update: accepted and command_id, then the update's result fields when it
// finished in time. The result never replaces accepted or command_id.
func acceptedBody(updateID string, result map[string]any) json.RawMessage {
	body := map[string]any{}
	for key, value := range result {
		body[key] = value
	}
	body["accepted"], body["command_id"] = true, updateID
	raw, _ := json.Marshal(body)
	return raw
}

// updateResult waits up to wait for the update to finish. A refusal, or any other failure the update already has, is
// returned as refusal does; an update still being handled when wait passes has no result yet (nil, nil). A result that is
// not a JSON object is ignored: the update was accepted either way.
func updateResult(ctx context.Context, handle client.WorkflowUpdateHandle, wait time.Duration) (map[string]any, error) {
	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	var result map[string]any
	err := handle.Get(waitCtx, &result)
	if err == nil {
		return result, nil
	}
	classified := classifyUpdateError(err)
	if _, ok := classified.(*Refusal); ok {
		return nil, classified
	}
	if waitCtx.Err() != nil && ctx.Err() == nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) {
		return nil, nil // still being handled
	}
	return nil, err
}

func (c *Client) SignalTask(ctx context.Context, tenantID, taskID, signalName string, arg any) error {
	return c.tc.SignalWorkflow(ctx, TaskWorkflowID(tenantID, taskID), "", signalName, arg)
}

// What the workflow refuses an update for, as callers tell the common ones apart with errors.Is. They are what a
// Refusal of that type matches.
var (
	ErrConfigConflict = errors.New("the task's configuration changed since it was read")
	ErrTaskClosed     = errors.New("task is closed")
)

// Refusal is an update the workflow turned down or failed with an ApplicationError: the type and message the workflow
// raised, which is all control knows about it. The workflow raises them from its update validators (the only place a
// refusal reaches a caller that waits just for the update to be accepted) and, for an update that was accepted, from its
// handler.
type Refusal struct {
	Type    string
	Message string
}

func (r *Refusal) Error() string { return r.Type + ": " + r.Message }

// Is lets errors.Is find the refusals callers act on by name.
func (r *Refusal) Is(target error) bool {
	switch target {
	case ErrConfigConflict:
		return r.Type == "CONFIG_VERSION_CONFLICT"
	case ErrTaskClosed:
		return r.Type == "TASK_CLOSED"
	}
	return false
}

// Status is the HTTP status a refusal is answered with.
func (r *Refusal) Status() int { return RefusalStatus(r.Type) }

// refusalStatuses is the one table from what the workflow refuses with to the HTTP status control answers by. A type that
// is not here is a conflict with the task's state: 409, with the type in the body so the caller can tell.
var refusalStatuses = map[string]int{
	// the request does not fit the task's current state
	"CONFIG_VERSION_CONFLICT": http.StatusConflict,
	"TASK_CLOSED":             http.StatusConflict,
	"INVALID_TRANSITION":      http.StatusConflict,
	"STALE_ATTEMPT":           http.StatusConflict,
	"STALE":                   http.StatusConflict,
	"VERSION_CONFLICT":        http.StatusConflict,
	"FROZEN_NODE":             http.StatusConflict,
	// the request names something the task does not have
	"UNKNOWN_NODE":     http.StatusNotFound,
	"UNKNOWN_APPROVAL": http.StatusNotFound,
	"NOT_FOUND":        http.StatusNotFound,
	// the request is well formed but the schema or the policy does not allow it
	"SCHEMA_INVALID":   http.StatusUnprocessableEntity,
	"NOT_ALLOWED":      http.StatusUnprocessableEntity,
	"POLICY_VIOLATION": http.StatusUnprocessableEntity,
	"POLICY_DENIED":    http.StatusUnprocessableEntity,
}

// RefusalStatus is the HTTP status for a refusal of the given type.
func RefusalStatus(refusalType string) int {
	if status, ok := refusalStatuses[refusalType]; ok {
		return status
	}
	return http.StatusConflict
}

// classifyUpdateError turns an error from the workflow's update handling into a Refusal when it carries an
// ApplicationError, whatever its type; any other error stays as it is.
func classifyUpdateError(err error) error {
	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		return &Refusal{Type: applicationErr.Type(), Message: applicationErr.Message()}
	}
	return err
}

// refusalWait is how long to look for a refusal. A validator's refusal is already part of the handle the SDK returns, so
// reading it is immediate; an update that is still being handled outlasts this and counts as accepted.
const refusalWait = 250 * time.Millisecond

// refusal tells whether the workflow refused an update it was asked to take, or has already failed one it accepted.
// UpdateWorkflow returns a handle for a refused update just as for an accepted one; the refusal is the handle's error.
// Whatever the result already says is reported: a refusal as a *Refusal, any other failure as it is. Only an update that
// has not finished within refusalWait stays accepted as far as the caller is told.
func refusal(ctx context.Context, handle client.WorkflowUpdateHandle) error {
	waitCtx, cancel := context.WithTimeout(ctx, refusalWait)
	defer cancel()
	var discard any
	err := handle.Get(waitCtx, &discard)
	if err == nil {
		return nil
	}
	classified := classifyUpdateError(err)
	if _, ok := classified.(*Refusal); ok {
		return classified
	}
	if waitCtx.Err() != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) {
		return nil // still being handled
	}
	return err
}

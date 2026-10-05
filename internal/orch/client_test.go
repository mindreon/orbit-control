package orch

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
)

func TestTaskStartOptionsHaveNoTimeoutAndOneRunPerTask(t *testing.T) {
	opts := taskStartOptions("tenant", "task_1")
	if opts.ID != "task/tenant/task_1" {
		t.Fatalf("workflow id = %q", opts.ID)
	}
	if opts.WorkflowExecutionTimeout != 0 || opts.WorkflowRunTimeout != 0 || opts.WorkflowTaskTimeout != time.Duration(0) {
		t.Fatalf("a task workflow must not time out: %+v", opts)
	}
	if opts.WorkflowIDConflictPolicy != enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING {
		t.Fatalf("conflict policy = %v", opts.WorkflowIDConflictPolicy)
	}
	if opts.WorkflowIDReusePolicy != enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE {
		t.Fatalf("reuse policy = %v", opts.WorkflowIDReusePolicy)
	}
}

func TestStartedViewIsBuiltFromInputWithoutQuery(t *testing.T) {
	cfg := map[string]any{"config_version": 1}
	view := startedView(TaskWorkflowInput{TaskID: "task_1", Budgets: map[string]any{"tokens": 5}, Config: cfg})
	if view.TaskID != "task_1" || view.Status != "RUNNING" || view.PlanVersion != 1 {
		t.Fatalf("view = %+v", view)
	}
	if view.PendingApprovals == nil || view.Usage == nil || view.Budgets["tokens"] != 5 || view.Config["config_version"] != 1 {
		t.Fatalf("view = %+v", view)
	}
}

type fakeHandle struct{ err error }

func (fakeHandle) WorkflowID() string               { return "task/t/task_1" }
func (fakeHandle) RunID() string                    { return "run" }
func (fakeHandle) UpdateID() string                 { return "cmd" }
func (h fakeHandle) Get(context.Context, any) error { return h.err }

type blockedHandle struct{ fakeHandle }

func (blockedHandle) Get(ctx context.Context, _ any) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestRefusalSurfacesEveryApplicationErrorWithItsType(t *testing.T) {
	cases := map[string]int{
		"CONFIG_VERSION_CONFLICT": http.StatusConflict,
		"TASK_CLOSED":             http.StatusConflict,
		"INVALID_TRANSITION":      http.StatusConflict,
		"FROZEN_NODE":             http.StatusConflict,
		"UNKNOWN_APPROVAL":        http.StatusNotFound,
		"SCHEMA_INVALID":          http.StatusUnprocessableEntity,
		"NOT_ALLOWED":             http.StatusUnprocessableEntity,
		"SOMETHING_NEW":           http.StatusConflict,
	}
	for refusalType, want := range cases {
		err := refusal(context.Background(), fakeHandle{err: temporal.NewNonRetryableApplicationError("no: "+refusalType, refusalType, nil)})
		var got *Refusal
		if !errors.As(err, &got) || got.Type != refusalType || got.Message != "no: "+refusalType {
			t.Fatalf("%s: err = %#v", refusalType, err)
		}
		if got.Status() != want {
			t.Errorf("%s: status = %d, want %d", refusalType, got.Status(), want)
		}
	}
}

func TestRefusalKeepsSentinelsMatching(t *testing.T) {
	err := classifyUpdateError(temporal.NewNonRetryableApplicationError("closed", "TASK_CLOSED", nil))
	if !errors.Is(err, ErrTaskClosed) || errors.Is(err, ErrConfigConflict) {
		t.Fatalf("TASK_CLOSED matches wrongly: %v", err)
	}
	err = classifyUpdateError(temporal.NewNonRetryableApplicationError("stale", "CONFIG_VERSION_CONFLICT", nil))
	if !errors.Is(err, ErrConfigConflict) || errors.Is(err, ErrTaskClosed) {
		t.Fatalf("CONFIG_VERSION_CONFLICT matches wrongly: %v", err)
	}
}

func TestRefusalLeavesAcceptedUpdatesAccepted(t *testing.T) {
	if err := refusal(context.Background(), fakeHandle{}); err != nil {
		t.Fatalf("a completed update is accepted: %v", err)
	}
	start := time.Now()
	if err := refusal(context.Background(), blockedHandle{}); err != nil {
		t.Fatalf("an update still being handled is accepted: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("waited far longer than refusalWait")
	}
}

func TestRefusalReportsAFailureThatIsAlreadyThere(t *testing.T) {
	boom := errors.New("workflow task failed")
	if err := refusal(context.Background(), fakeHandle{err: boom}); !errors.Is(err, boom) {
		t.Fatalf("a failed update must not look accepted: %v", err)
	}
}

type resultHandle struct {
	fakeHandle
	result string
}

func (h resultHandle) Get(_ context.Context, out any) error {
	if h.err != nil {
		return h.err
	}
	return json.Unmarshal([]byte(h.result), out)
}

func TestUpdateResultIsMergedWithAcceptance(t *testing.T) {
	result, err := updateResult(context.Background(), resultHandle{result: `{"effective_attempt_no":2,"needs_approval":false,"approval_id":null,"accepted":false}`}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(acceptedBody("cmd_1", result), &body); err != nil {
		t.Fatal(err)
	}
	if body["accepted"] != true || body["command_id"] != "cmd_1" || body["effective_attempt_no"] != float64(2) || body["needs_approval"] != false {
		t.Fatalf("body = %v", body)
	}
	if got := string(acceptedBody("cmd_2", nil)); got != `{"accepted":true,"command_id":"cmd_2"}` {
		t.Fatalf("without a result: %s", got)
	}
}

func TestUpdateResultTimesOutAsAcceptedAndSurfacesRefusals(t *testing.T) {
	start := time.Now()
	result, err := updateResult(context.Background(), blockedHandle{}, 100*time.Millisecond)
	if err != nil || result != nil || time.Since(start) > 2*time.Second {
		t.Fatalf("a slow update is accepted without a result: %v %v", result, err)
	}
	_, err = updateResult(context.Background(), fakeHandle{err: temporal.NewNonRetryableApplicationError("no", "FROZEN_NODE", nil)}, time.Second)
	var refused *Refusal
	if !errors.As(err, &refused) || refused.Type != "FROZEN_NODE" {
		t.Fatalf("err = %#v", err)
	}
	boom := errors.New("workflow task failed")
	if _, err := updateResult(context.Background(), fakeHandle{err: boom}, time.Second); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := updateResult(cancelled, blockedHandle{}, time.Second); err == nil {
		t.Fatal("a caller that went away must not look like an accepted update")
	}
}

func TestOnlyProfileSwitchAndCompleteNodeWaitForResults(t *testing.T) {
	if !resultUpdates["requestProfileSwitch"] || !resultUpdates["completeNode"] || len(resultUpdates) != 2 {
		t.Fatalf("resultUpdates = %v", resultUpdates)
	}
}

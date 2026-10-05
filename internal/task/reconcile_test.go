package task

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/mindreon/orbit-control/internal/orch"
)

type reconcileClient struct {
	view orch.TaskView
}

func (c *reconcileClient) StartTask(context.Context, orch.TaskWorkflowInput) (orch.TaskView, error) {
	return c.view, nil
}

func (c *reconcileClient) GetTaskView(context.Context, string, string) (orch.TaskView, error) {
	return c.view, nil
}

func (*reconcileClient) GetTaskPlan(context.Context, string, string) (orch.TaskPlan, error) {
	return orch.TaskPlan{}, nil
}

func (*reconcileClient) UpdateTask(context.Context, string, string, string, string, any) (json.RawMessage, error) {
	return json.RawMessage(`{"accepted":true}`), nil
}

func (*reconcileClient) SignalTask(context.Context, string, string, string, any) error { return nil }

func TestReconcileReportsAndRepairsProjection(t *testing.T) {
	client := &reconcileClient{view: orch.TaskView{
		Status:           "RUNNING",
		PlanVersion:      2,
		PendingApprovals: []string{"apr_01"},
		Budgets:          map[string]any{"tokens": float64(100)},
		Usage:            map[string]any{"tokens_in": float64(4)},
	}}
	service := New(client)
	principal := Principal{TenantID: "tenant", UserID: "user"}
	created, err := service.Create(context.Background(), principal, CreateInput{Title: "reconcile", Goal: "repair"})
	if err != nil {
		t.Fatal(err)
	}
	report, err := service.Reconcile(context.Background(), principal, created.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if report.Healthy || len(report.Differences) != 5 || report.Repaired {
		t.Fatalf("unexpected report: %+v", report)
	}
	report, err = service.Reconcile(context.Background(), principal, created.ID, true)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Repaired || report.Healthy {
		t.Fatalf("repair should report the observed drift: %+v", report)
	}
	got, err := service.Get(context.Background(), principal, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "RUNNING" || got.PlanVersion != 2 || len(got.PendingApprovals) != 1 || got.Budgets["tokens"] != float64(100) {
		t.Fatalf("projection was not repaired: %+v", got)
	}
	report, err = service.Reconcile(context.Background(), principal, created.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Healthy || len(report.Differences) != 0 {
		t.Fatalf("repaired projection still differs: %+v", report)
	}
}

func TestDurableEventsMaintainReconcileFields(t *testing.T) {
	client := &reconcileClient{view: orch.TaskView{Status: "RUNNING", PlanVersion: 1}}
	service := New(client)
	principal := Principal{TenantID: "tenant", UserID: "user"}
	created, err := service.Create(context.Background(), principal, CreateInput{Title: "events", Goal: "project"})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []Event{
		{EventID: "evt_request", TaskID: created.ID, Type: "approval.requested", Payload: json.RawMessage(`{"approval_id":"apr_01"}`), Durable: true},
		{EventID: "evt_budget", TaskID: created.ID, Type: "budget.granted", Payload: json.RawMessage(`{"delta":{"tokens":10}}`), Durable: true},
		{EventID: "evt_usage", TaskID: created.ID, Type: "usage.recorded", Payload: json.RawMessage(`{"usage":{"tokens_in":3}}`), Durable: true},
		{EventID: "evt_decide", TaskID: created.ID, Type: "approval.decided", Payload: json.RawMessage(`{"approval_id":"apr_01"}`), Durable: true},
	} {
		if err := service.AppendEvent(event); err != nil {
			t.Fatal(err)
		}
	}
	got, err := service.Get(context.Background(), principal, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.PendingApprovals) != 0 || got.Budgets["tokens"] != float64(10) || got.Usage["tokens_in"] != float64(3) {
		t.Fatalf("event projection fields = %+v", got)
	}
}

// failingQueryClient is an orchestrator that accepts a start but cannot answer a Query, as when no orch worker polls.
type failingQueryClient struct {
	reconcileClient
	queries int
}

func (c *failingQueryClient) GetTaskView(context.Context, string, string) (orch.TaskView, error) {
	c.queries++
	return orch.TaskView{}, context.DeadlineExceeded
}

func TestCreateDoesNotQueryTheWorkflow(t *testing.T) {
	client := &failingQueryClient{}
	service := New(client)
	created, err := service.Create(context.Background(), Principal{TenantID: "tenant", UserID: "user"}, CreateInput{Title: "t", Goal: "g"})
	if err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || client.queries != 0 {
		t.Fatalf("create must not query the workflow: id=%q queries=%d", created.ID, client.queries)
	}
}

func TestReconcileTreatsUnknownCostAndReservationAsNoDifference(t *testing.T) {
	client := &reconcileClient{view: orch.TaskView{
		Status:         "CREATED",
		PlanVersion:    1,
		Budgets:        map[string]any{"tokens": float64(100), "cost_usd_micros": nil},
		Usage:          map[string]any{"tokens_in": float64(0), "tokens_out": float64(0), "cost_usd_micros": nil},
		BudgetReserved: map[string]any{"tokens": float64(40)},
	}}
	service := New(client)
	principal := Principal{TenantID: "tenant", UserID: "user"}
	created, err := service.Create(context.Background(), principal, CreateInput{Title: "t", Goal: "g", Budgets: map[string]any{"tokens": float64(100)}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := service.Reconcile(context.Background(), principal, created.ID, false)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Healthy {
		t.Fatalf("unknown cost or a reservation is not drift: %+v", report)
	}
	// A cost that is known on one side only is drift, and a repair stores no nulls.
	client.view.Usage = map[string]any{"cost_usd_micros": float64(7), "tool_calls": nil}
	report, _ = service.Reconcile(context.Background(), principal, created.ID, true)
	if report.Healthy || !report.Repaired {
		t.Fatalf("a known cost must differ: %+v", report)
	}
	got, _ := service.Get(context.Background(), principal, created.ID)
	if _, ok := got.Usage["tool_calls"]; ok || got.Usage["cost_usd_micros"] != float64(7) {
		t.Fatalf("repaired usage = %v", got.Usage)
	}
}

func TestAddNumbersIgnoresUnknownAmounts(t *testing.T) {
	out := addNumbers(map[string]any{"cost_usd_micros": float64(5)}, map[string]any{"cost_usd_micros": nil, "tokens_in": float64(2)})
	if out["cost_usd_micros"] != float64(5) || out["tokens_in"] != float64(2) {
		t.Fatalf("out = %v", out)
	}
}

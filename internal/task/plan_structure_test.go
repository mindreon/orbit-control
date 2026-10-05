package task

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/mindreon/orbit-control/internal/orch"
)

type planClientStub struct {
	reconcileClient
	nodes []map[string]any
}

func (c *planClientStub) GetTaskPlan(context.Context, string, string) (orch.TaskPlan, error) {
	return orch.TaskPlan{PlanVersion: 3, Nodes: c.nodes}, nil
}

// structureProjection is a projection that owns one task and keeps NodeStructure.
type structureProjection struct {
	ProjectionStore
	structure map[string]NodeStructure
	err       error
}

func (p *structureProjection) GetTask(_ context.Context, pr Principal, id string) (*Task, error) {
	return &Task{ID: id, TenantID: pr.TenantID, CreatedBy: pr.UserID}, nil
}

func (p *structureProjection) NodeStructure(context.Context, Principal, string) (map[string]NodeStructure, error) {
	return p.structure, p.err
}

func planNodes(t *testing.T, structure map[string]NodeStructure, err error, nodes ...map[string]any) []map[string]any {
	t.Helper()
	service := NewWithProjection(&planClientStub{nodes: nodes}, &structureProjection{structure: structure, err: err})
	plan, planErr := service.Plan(context.Background(), Principal{TenantID: "tenant", UserID: "user"}, "task_1")
	if err != nil {
		if !errors.Is(planErr, err) {
			t.Fatalf("plan error = %v, want %v", planErr, err)
		}
		return nil
	}
	if planErr != nil {
		t.Fatal(planErr)
	}
	return plan.Nodes
}

func TestPlanAddsTheProjectedSopStep(t *testing.T) {
	step := json.RawMessage(`{"sop":"release@1","role":"step","total":3,"step_id":"s2","index":2,"subject":"review"}`)
	nodes := planNodes(t,
		map[string]NodeStructure{"n_4": {ParentNodeID: "n_3", SopStep: step}, "n_5": {SopStep: step}},
		nil,
		map[string]any{"node_id": "n_4", "status": "READY"},
		map[string]any{"node_id": "n_5", "status": "READY", "parent_node_id": "n_9"},
		map[string]any{"node_id": "n_6", "status": "READY"},
	)
	body, _ := json.Marshal(nodes)
	want := `[{"node_id":"n_4","parent_node_id":"n_3","sop_step":` + string(step) + `,"status":"READY"},` +
		`{"node_id":"n_5","parent_node_id":"n_9","sop_step":` + string(step) + `,"status":"READY"},` +
		`{"node_id":"n_6","status":"READY"}]`
	if string(body) != want {
		t.Fatalf("plan nodes:\n got %s\nwant %s", body, want)
	}
}

func TestPlanKeepsWhatTheWorkflowSays(t *testing.T) {
	kept := map[string]NodeStructure{"n_4": {ParentNodeID: "n_3", SopStep: json.RawMessage(`{"sop":"old@1","role":"step","total":1}`)}}
	nodes := planNodes(t, kept, nil, map[string]any{
		"node_id": "n_4", "parent_node_id": "n_7", "sop_step": map[string]any{"sop": "live@1", "role": "step", "total": 2},
	})
	body, _ := json.Marshal(nodes[0])
	if want := `{"node_id":"n_4","parent_node_id":"n_7","sop_step":{"role":"step","sop":"live@1","total":2}}`; string(body) != want {
		t.Fatalf("node:\n got %s\nwant %s", body, want)
	}
}

func TestPlanFailsWhenTheProjectionCannotBeRead(t *testing.T) {
	planNodes(t, nil, context.DeadlineExceeded, map[string]any{"node_id": "n_1"})
}

func TestPlanWithoutAStructureReaderIsTheWorkflowsPlan(t *testing.T) {
	service := New(&planClientStub{nodes: []map[string]any{{"node_id": "n_1"}}})
	principal := Principal{TenantID: "tenant", UserID: "user"}
	created, err := service.Create(context.Background(), principal, CreateInput{Title: "t", Goal: "g"})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := service.Plan(context.Background(), principal, created.ID)
	if err != nil || len(plan.Nodes) != 1 || len(plan.Nodes[0]) != 1 {
		t.Fatalf("plan = %+v, err %v", plan, err)
	}
}

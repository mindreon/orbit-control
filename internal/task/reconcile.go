package task

import (
	"context"
	"time"

	"github.com/mindreon/orbit-control/internal/orch"
)

func (s *Service) Plan(ctx context.Context, p Principal, id string) (orch.TaskPlan, error) {
	if _, err := s.Get(ctx, p, id); err != nil {
		return orch.TaskPlan{}, err
	}
	if s.orch == nil {
		return orch.TaskPlan{}, nil
	}
	plan, err := s.orch.GetTaskPlan(ctx, p.TenantID, id)
	if err != nil {
		return plan, err
	}
	return s.withNodeStructure(ctx, p, id, plan)
}

// withNodeStructure adds what the projection kept of each node's place in a compiled SOP (`parent_node_id`, `sop_step`)
// to the workflow's plan: getPlan carries the parent but not the SOP step. A value the workflow's node already has wins.
func (s *Service) withNodeStructure(ctx context.Context, p Principal, id string, plan orch.TaskPlan) (orch.TaskPlan, error) {
	reader, ok := s.projection.(NodeStructureReader)
	if !ok || len(plan.Nodes) == 0 {
		return plan, nil
	}
	structure, err := reader.NodeStructure(ctx, p, id)
	if err != nil {
		return orch.TaskPlan{}, err
	}
	for _, node := range plan.Nodes {
		nodeID, _ := node["node_id"].(string)
		kept, found := structure[nodeID]
		if !found {
			continue
		}
		if node["parent_node_id"] == nil && kept.ParentNodeID != "" {
			node["parent_node_id"] = kept.ParentNodeID
		}
		if node["sop_step"] == nil && len(kept.SopStep) > 0 {
			node["sop_step"] = kept.SopStep
		}
	}
	return plan, nil
}

// Reconcile compares the authoritative workflow Query with the durable task
// projection. With repair=true, the workflow view is written back atomically
// through the projection boundary.
func (s *Service) Reconcile(ctx context.Context, p Principal, id string, repair bool) (TaskReconcileReport, error) {
	if s.orch == nil {
		return TaskReconcileReport{}, ErrReconcileUnavailable
	}
	projected, err := s.Get(ctx, p, id)
	if err != nil {
		return TaskReconcileReport{}, err
	}
	view, err := s.orch.GetTaskView(ctx, p.TenantID, id)
	if err != nil {
		return TaskReconcileReport{}, err
	}
	report := TaskReconcileReport{TaskID: id, Healthy: true, Differences: []TaskReconcileDifference{}}
	addDifference := func(field string, expected, actual any) {
		report.Healthy = false
		report.Differences = append(report.Differences, TaskReconcileDifference{Field: field, Expected: expected, Actual: actual})
	}
	if projected.Status != view.Status {
		addDifference("status", view.Status, projected.Status)
	}
	if projected.PlanVersion != view.PlanVersion {
		addDifference("plan_version", view.PlanVersion, projected.PlanVersion)
	}
	// An unknown value (null) and a missing one are the same, so a view that spells out its nulls and a projection that
	// kept only what events carried do not differ. view.BudgetReserved is deliberately not compared: it is what running
	// attempts hold right now, no projection stores it, and it changes with every attempt that starts or settles.
	viewBudgets, viewUsage := normalizeBudget(view.Budgets), normalizeUsage(view.Usage)
	if !jsonEqual(normalizeBudget(projected.Budgets), viewBudgets) {
		addDifference("budgets", viewBudgets, projected.Budgets)
	}
	if !jsonEqual(normalizeUsage(projected.Usage), viewUsage) {
		addDifference("usage", viewUsage, projected.Usage)
	}
	if !approvalSetEqual(projected.PendingApprovals, view.PendingApprovals) {
		addDifference("pending_approvals", view.PendingApprovals, projected.PendingApprovals)
	}
	if !repair || report.Healthy {
		return report, nil
	}
	projected.Status = view.Status
	projected.PlanVersion = view.PlanVersion
	projected.Budgets = viewBudgets
	projected.Usage = viewUsage
	projected.PendingApprovals = append([]string(nil), view.PendingApprovals...)
	projected.UpdatedAt = time.Now().UTC()
	if s.projection != nil {
		if err := s.projection.UpdateTask(ctx, p, projected); err != nil {
			return TaskReconcileReport{}, err
		}
	}
	s.cacheTask(projected)
	report.Repaired = true
	return report, nil
}

// usageCounters are the Usage fields that count from zero: absent means 0. cost_usd_micros is not one of them, because an
// unknown cost (null or absent) is not a cost of 0.
var usageCounters = []string{"tokens_in", "tokens_out", "tool_calls", "wall_s"}

// normalizeBudget drops the limits that are null: a missing field means no limit at this level.
func normalizeBudget(budget map[string]any) map[string]any {
	out := make(map[string]any, len(budget))
	for key, value := range budget {
		if value != nil {
			out[key] = value
		}
	}
	return out
}

// normalizeUsage drops an unknown cost (null) and the counters that are zero, so that only what was spent is compared.
func normalizeUsage(usage map[string]any) map[string]any {
	out := normalizeBudget(usage)
	for _, key := range usageCounters {
		if n, ok := out[key].(float64); ok && n == 0 {
			delete(out, key)
		}
	}
	return out
}

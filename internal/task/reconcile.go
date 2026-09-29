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
	return s.orch.GetTaskPlan(ctx, p.TenantID, id)
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
	if !jsonEqual(projected.Budgets, view.Budgets) {
		addDifference("budgets", view.Budgets, projected.Budgets)
	}
	if !jsonEqual(projected.Usage, view.Usage) {
		addDifference("usage", view.Usage, projected.Usage)
	}
	if !approvalSetEqual(projected.PendingApprovals, view.PendingApprovals) {
		addDifference("pending_approvals", view.PendingApprovals, projected.PendingApprovals)
	}
	if !repair || report.Healthy {
		return report, nil
	}
	projected.Status = view.Status
	projected.PlanVersion = view.PlanVersion
	projected.Budgets = cloneMap(view.Budgets)
	projected.Usage = cloneMap(view.Usage)
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

package app

import (
	"context"
	"encoding/json"
	"time"

	"github.com/mindreon/orbit-control/internal/store"
)

// ReconcileDeliveredResults runs one database-driven pass for every tenant
// (FM-71). It does not use process memory from the request that delivered.
func (a *App) ReconcileDeliveredResults(ctx context.Context) error {
	ds := a.delivery()
	if ds == nil {
		return nil
	}
	tenants, err := ds.ListTenantIDs(ctx)
	if err != nil {
		return err
	}
	for _, tenantID := range tenants {
		if err := a.reconcileTenant(ctx, tenantID); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}

func (a *App) reconcileTenant(ctx context.Context, tenantID string) error {
	ds := a.delivery()
	rows, err := ds.ListReconcileRows(ctx, tenantID)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		a.reconcileRow(ctx, tenantID, row)
	}
	return nil
}

func (a *App) reconcileRow(ctx context.Context, tenantID string, row store.ReconcileRow) {
	ds := a.delivery()
	appr := row.Approval
	state := appr.DeliveryState
	failed := row.RoomState == string(RoomFailed)

	if state == "delivered" && appr.ResultAttempt == nil {
		a.applySavedOrQuery(ctx, tenantID, appr, row.ResultBody)
		return
	}
	if failed && state == "in_flight" {
		if a.stalled(row.DeliveryUpdatedAt) {
			ok, err := ds.TransitionAt(ctx, tenantID, appr.ID, appr.DeliveryAttempt, row.DeliveryUpdatedAt)
			if err == nil && ok {
				state = "unknown"
				appr.DeliveryState = "unknown"
				a.noteDelivery(appr, "unknown")
			}
		}
	}
	if failed && state == "unknown" {
		found, _, qerr := a.queryOutcome(ctx, appr)
		if qerr == nil && found {
			a.markDeliveredFromQuery(ctx, tenantID, appr)
			return
		}
		a.markUnresolved(ctx, tenantID, appr)
		return
	}
	if state == "in_flight" && a.stalled(row.DeliveryUpdatedAt) {
		ok, err := ds.TransitionAt(ctx, tenantID, appr.ID, appr.DeliveryAttempt, row.DeliveryUpdatedAt)
		if err != nil || !ok {
			return
		}
		state = "unknown"
		appr.DeliveryState = "unknown"
		a.noteDelivery(appr, "unknown")
	}
	if state == "not_delivered" {
		ok, err := ds.ReopenNotDelivered(ctx, tenantID, appr.ID, appr.DeliveryAttempt)
		if err == nil && ok {
			appr.Status = "pending"
			appr.Decision = ""
			appr.DeliveryState = ""
			a.noteDelivery(appr, "")
		}
		return
	}
	if state == "unknown" {
		if appr.DecidedAt != nil && time.Since(*appr.DecidedAt) > a.UnknownTimeout {
			a.markUnresolved(ctx, tenantID, appr)
			return
		}
		found, _, qerr := a.queryOutcome(ctx, appr)
		if qerr == nil && found {
			a.markDeliveredFromQuery(ctx, tenantID, appr)
			return
		}
		if qerr != nil || a.Orch == nil {
			return
		}
		pending, perr := a.Orch.ApprovalPending(ctx, appr.TaskID, appr.ApprovalRequestID)
		if perr != nil || !pending {
			return
		}
		next, ok, err := ds.ClaimRetry(ctx, tenantID, appr.ID, appr.DeliveryAttempt)
		if err != nil || !ok {
			return
		}
		appr.DeliveryAttempt = next
		appr.DeliveryState = "in_flight"
		a.noteDelivery(appr, "in_flight")
		a.retryDelivery(ctx, tenantID, appr)
		return
	}
	if state == "unresolved" && !failed && appr.DecidedAt != nil && time.Since(*appr.DecidedAt) <= 24*time.Hour {
		found, _, qerr := a.queryOutcome(ctx, appr)
		if qerr != nil || !found {
			return
		}
		ok, err := ds.SetDeliveryState(ctx, tenantID, appr.ID, "unresolved", "delivered", appr.DeliveryAttempt)
		if err == nil && ok {
			appr.DeliveryState = "delivered"
			a.noteDelivery(appr, "delivered")
			a.applySavedOrQuery(ctx, tenantID, appr, nil)
		}
	}
}

func (a *App) stalled(updated time.Time) bool {
	if updated.IsZero() {
		return false
	}
	return time.Since(updated) > a.DeliveryTimeout+stalledMargin
}

func (a *App) queryOutcome(ctx context.Context, appr store.ApprovalRecord) (bool, store.DecideResultWrite, error) {
	if a.Orch == nil {
		return false, store.DecideResultWrite{}, nil
	}
	found, outcome, err := a.Orch.DecideOutcome(ctx, appr.TaskID, appr.ApprovalRequestID)
	if err != nil || !found {
		return false, store.DecideResultWrite{}, err
	}
	w := store.DecideResultWrite{
		ApprovalID: appr.ID,
		Attempt:    appr.DeliveryAttempt,
		RoomID:     appr.TaskID,
		UpdateRoom: outcome.TurnStatus == "completed" || outcome.TurnStatus == "",
		RoomState:  string(RoomRunning),
	}
	return true, w, nil
}

func (a *App) markDeliveredFromQuery(ctx context.Context, tenantID string, appr store.ApprovalRecord) {
	ok, err := a.delivery().SetDeliveryState(ctx, tenantID, appr.ID, "unknown", "delivered", appr.DeliveryAttempt)
	if err != nil || !ok {
		return
	}
	appr.DeliveryState = "delivered"
	a.noteDelivery(appr, "delivered")
	a.applySavedOrQuery(ctx, tenantID, appr, nil)
}

func (a *App) markUnresolved(ctx context.Context, tenantID string, appr store.ApprovalRecord) {
	ok, err := a.delivery().SetDeliveryState(ctx, tenantID, appr.ID, "unknown", "unresolved", appr.DeliveryAttempt)
	if err != nil || !ok {
		return
	}
	appr.DeliveryState = "unresolved"
	a.noteDelivery(appr, "unresolved")
}

func (a *App) applySavedOrQuery(ctx context.Context, tenantID string, appr store.ApprovalRecord, body []byte) {
	ds := a.delivery()
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.TurnTimeout)
	defer cancel()
	var w store.DecideResultWrite
	if len(body) > 0 {
		if json.Unmarshal(body, &w) != nil {
			return
		}
	} else {
		loaded, ok, err := ds.LoadResultBody(wctx, tenantID, appr.ID, appr.DeliveryAttempt)
		if err != nil {
			return
		}
		if ok {
			w = loaded
		} else {
			found, queried, qerr := a.queryOutcome(wctx, appr)
			if qerr != nil || !found {
				return
			}
			w = queried
			raw, err := json.Marshal(w)
			if err != nil {
				return
			}
			if _, err := ds.SaveResultBody(wctx, tenantID, appr.ID, appr.DeliveryAttempt, raw); err != nil {
				return
			}
		}
	}
	wrote, err := ds.ApplyDecideResult(wctx, tenantID, w)
	if err == nil && wrote {
		a.publishResult(w)
	}
}

func (a *App) retryDelivery(ctx context.Context, tenantID string, appr store.ApprovalRecord) {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.DeliveryTimeout)
	defer cancel()
	p := Principal{TenantID: tenantID, UserID: ""}
	kind, upd, err := a.deliverOnce(dctx, p, appr, appr.Decision)
	switch kind {
	case "delivered":
		ok, serr := a.delivery().SetDeliveryState(dctx, tenantID, appr.ID, "in_flight", "delivered", appr.DeliveryAttempt)
		if serr != nil || !ok {
			return
		}
		appr.DeliveryState = "delivered"
		a.noteDelivery(appr, "delivered")
		_, _ = a.writeBack(context.WithoutCancel(ctx), p, appr, appr.DeliveryAttempt, func(ctx context.Context) (store.DecideResultWrite, error) {
			return a.fetchResult(ctx, upd, appr, appr.Decision, appr.DeliveryAttempt)
		})
	case "not_delivered":
		ok, _ := a.delivery().ReopenUndelivered(dctx, tenantID, appr.ID, appr.DeliveryAttempt)
		if ok {
			appr.Status = "pending"
			appr.Decision = ""
			a.noteDelivery(appr, "not_delivered")
			a.noteDelivery(appr, "")
		}
	case "fatal":
		_, _ = a.failLimited(context.WithoutCancel(ctx), p, appr, appr.DeliveryAttempt, "in_flight")
	default:
		_ = err
		ok, _ := a.delivery().SetDeliveryState(dctx, tenantID, appr.ID, "in_flight", "unknown", appr.DeliveryAttempt)
		if ok {
			appr.DeliveryState = "unknown"
			a.noteDelivery(appr, "unknown")
		}
	}
}

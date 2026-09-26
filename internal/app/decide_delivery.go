package app

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/worker"
)

const (
	roomFailedMessage  = "decided approvals reached the limit"
	workerErrorMessage = "the resumed turn failed"
)

// pendingWrite is one delivered attempt whose result may still need to be
// written. materialize calls the workflow at most once per process.
type pendingWrite struct {
	mu       sync.Mutex
	tenantID string
	attempt  int
	ready    bool
	write    store.DecideResultWrite
	fetch    func(ctx context.Context) (store.DecideResultWrite, error)
}

func (p *pendingWrite) materialize(ctx context.Context) (store.DecideResultWrite, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ready {
		return p.write, nil
	}
	w, err := p.fetch(ctx)
	if err != nil {
		return store.DecideResultWrite{}, err
	}
	p.write = w
	p.ready = true
	return w, nil
}

func (a *App) delivery() store.DeliveryStore {
	ds, _ := a.Repo.(store.DeliveryStore)
	return ds
}

func (a *App) startReconcile() {
	if a.delivery() == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = a.ReconcileDeliveredResults(ctx)
			cancel()
		}
	}()
}

// ReconcileDeliveredResults writes the result of delivered approvals in the
// process default tenant whose result_attempt is still NULL (FM-61).
func (a *App) ReconcileDeliveredResults(ctx context.Context) error {
	ds := a.delivery()
	if ds == nil {
		return nil
	}
	rows, err := ds.ListUnwrittenDeliveries(ctx, a.DefaultTenant)
	if err != nil {
		return err
	}
	for _, row := range rows {
		if err := a.compensateUnwritten(ctx, row); err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}

func (a *App) compensateUnwritten(ctx context.Context, row store.ApprovalRecord) error {
	ds := a.delivery()
	v, ok := a.pendingWrites.Load(row.ID)
	if !ok {
		return nil
	}
	pw := v.(*pendingWrite)
	if pw.tenantID != a.DefaultTenant || pw.attempt != row.DeliveryAttempt {
		return nil
	}
	w, err := pw.materialize(ctx)
	if err != nil {
		return err
	}
	wrote, err := ds.ApplyDecideResult(ctx, pw.tenantID, w)
	if err != nil {
		return err
	}
	if wrote {
		a.publishResult(pw.tenantID, w)
	}
	return nil
}

// WriteResultAgain repeats the write-back for one delivery_attempt.
// A second call inserts nothing and returns false (FM-61).
func (a *App) WriteResultAgain(ctx context.Context, tenantID, approvalID string, attempt int) (bool, error) {
	ds := a.delivery()
	if ds == nil {
		return false, nil
	}
	v, ok := a.pendingWrites.Load(approvalID)
	if !ok {
		return false, nil
	}
	pw := v.(*pendingWrite)
	if pw.tenantID != tenantID || pw.attempt != attempt {
		return false, nil
	}
	w, err := pw.materialize(ctx)
	if err != nil {
		return false, err
	}
	wrote, err := ds.ApplyDecideResult(ctx, tenantID, w)
	if err != nil {
		return false, err
	}
	if wrote {
		a.publishResult(tenantID, w)
	}
	return wrote, nil
}

func (a *App) decideWithDelivery(ctx context.Context, p Principal, approvalID, decision string) (*Approval, error) {
	ds := a.delivery()
	appr, err := a.Repo.GetApproval(ctx, p.TenantID, p.UserID, approvalID)
	if err != nil {
		return nil, err
	}
	value := decision
	if a.Orch == nil && decision != "reject" {
		value = "allow"
	}
	if appr.Status != "pending" || appr.DeliveryState != "" {
		return a.repeatDecide(appr, value)
	}
	roomRec, err := a.Repo.GetRoomForWorker(ctx, p.TenantID, appr.TaskID)
	if err != nil {
		return nil, err
	}
	a.remember(roomRec)

	attempt, err := ds.ClaimDecision(ctx, p.TenantID, approvalID, value)
	if err != nil {
		if errors.Is(err, store.ErrApprovalNotPending) {
			fresh, gerr := a.Repo.GetApproval(ctx, p.TenantID, p.UserID, approvalID)
			if gerr != nil {
				return nil, gerr
			}
			return a.repeatDecide(fresh, value)
		}
		return nil, err
	}
	appr.Status = "decided"
	appr.Decision = value
	appr.DeliveryState = "in_flight"
	appr.DeliveryAttempt = attempt
	a.noteDelivery(roomRec.TenantID, appr, "in_flight")

	bg := context.WithoutCancel(ctx)
	done := make(chan struct {
		appr *Approval
		err  error
	}, 1)
	go func() {
		out, err := a.finishDelivery(bg, p, appr, value, attempt)
		done <- struct {
			appr *Approval
			err  error
		}{out, err}
	}()
	select {
	case res := <-done:
		return res.appr, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *App) repeatDecide(appr store.ApprovalRecord, value string) (*Approval, error) {
	out := approvalFromRecord(appr)
	switch appr.DeliveryState {
	case "in_flight", "unknown":
		if appr.Decision == value {
			return nil, &DeliveryUnknownError{Approval: out}
		}
		return nil, &StatusError{
			Status: 409, Code: "APPROVAL_DELIVERY_PENDING",
			Message: "approval delivery is pending", Approval: out,
		}
	default:
		return nil, store.ErrApprovalNotPending
	}
}

func (a *App) finishDelivery(ctx context.Context, p Principal, appr store.ApprovalRecord, value string, attempt int) (*Approval, error) {
	ds := a.delivery()
	dctx, cancel := context.WithTimeout(ctx, a.DeliveryTimeout)
	outcome, fetch, err := a.deliver(dctx, appr, value, attempt)
	timedOut := errors.Is(dctx.Err(), context.DeadlineExceeded)
	cancel()
	switch outcome {
	case "fatal":
		return a.failLimited(ctx, p, appr, attempt, "in_flight")
	case "not_delivered":
		ok, rerr := ds.ReopenUndelivered(ctx, p.TenantID, appr.ID, attempt)
		if rerr != nil {
			return nil, rerr
		}
		if !ok {
			fresh, gerr := a.Repo.GetApproval(ctx, p.TenantID, p.UserID, appr.ID)
			if gerr != nil {
				return nil, gerr
			}
			return a.repeatDecide(fresh, value)
		}
		a.noteDelivery(p.TenantID, appr, "not_delivered")
		appr.Status = "pending"
		appr.Decision = ""
		appr.DeliveryState = ""
		a.noteDelivery(p.TenantID, appr, "")
		fresh, gerr := a.Repo.GetApproval(ctx, p.TenantID, p.UserID, appr.ID)
		if gerr != nil {
			fresh = appr
		}
		return nil, &StatusError{
			Status: 503, Code: "APPROVAL_NOT_DELIVERED",
			Message:  "the decision was not delivered; decide again",
			Approval: approvalFromRecord(fresh),
		}
	case "unknown":
		reason := "error"
		if timedOut {
			reason = "timeout"
		}
		a.Log.Printf("WARN alert=decision_delivery_unknown approval=%s reason=%s", appr.ID, reason)
		if ok, serr := ds.SetDeliveryState(ctx, p.TenantID, appr.ID, "in_flight", "unknown", attempt); serr != nil {
			return nil, serr
		} else if ok {
			appr.DeliveryState = "unknown"
			a.noteDelivery(p.TenantID, appr, "unknown")
		}
		fresh, gerr := a.Repo.GetApproval(ctx, p.TenantID, p.UserID, appr.ID)
		if gerr != nil {
			fresh = appr
		}
		return nil, &DeliveryUnknownError{Approval: approvalFromRecord(fresh)}
	}
	if err != nil && outcome != "delivered" {
		return nil, err
	}
	ok, serr := ds.SetDeliveryState(ctx, p.TenantID, appr.ID, "in_flight", "delivered", attempt)
	if serr != nil {
		return nil, serr
	}
	if !ok {
		fresh, gerr := a.Repo.GetApproval(ctx, p.TenantID, p.UserID, appr.ID)
		if gerr != nil {
			return nil, gerr
		}
		return a.repeatDecide(fresh, value)
	}
	appr.DeliveryState = "delivered"
	a.noteDelivery(p.TenantID, appr, "delivered")
	pw := &pendingWrite{tenantID: p.TenantID, attempt: attempt, fetch: fetch}
	a.pendingWrites.Store(appr.ID, pw)
	if a.skipResultWriteEnabled() {
		return approvalFromRecord(appr), nil
	}
	return a.writeBack(ctx, p, appr, attempt, pw)
}

func (a *App) writeBack(ctx context.Context, p Principal, appr store.ApprovalRecord, attempt int, pw *pendingWrite) (*Approval, error) {
	w, err := pw.materialize(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "DECIDED_APPROVALS_LIMIT") {
			return a.failLimited(ctx, p, appr, attempt, "delivered")
		}
		a.Log.Printf("WARN alert=decide_result_failed approval=%s", appr.ID)
		fresh := approvalFromRecord(appr)
		return nil, &StatusError{Status: 502, Code: "WORKER_ERROR", Message: workerErrorMessage, Approval: fresh}
	}
	wrote, err := a.delivery().ApplyDecideResult(ctx, p.TenantID, w)
	if err != nil {
		return nil, err
	}
	if wrote {
		a.publishResult(p.TenantID, w)
	}
	fresh, gerr := a.Repo.GetApproval(ctx, p.TenantID, p.UserID, appr.ID)
	if gerr != nil {
		return approvalFromRecord(appr), nil
	}
	return approvalFromRecord(fresh), nil
}

// failLimited applies T5 (from in_flight) or T11 (from delivered) and fails the room.
func (a *App) failLimited(ctx context.Context, p Principal, appr store.ApprovalRecord, attempt int, from string) (*Approval, error) {
	ds := a.delivery()
	if _, err := ds.SetDeliveryState(ctx, p.TenantID, appr.ID, from, "unresolved", attempt); err != nil {
		return nil, err
	}
	appr.DeliveryState = "unresolved"
	a.noteDelivery(p.TenantID, appr, "unresolved")
	if err := ds.FailRoom(ctx, p.TenantID, appr.TaskID, "DECIDED_APPROVALS_LIMIT", roomFailedMessage); err != nil {
		return nil, err
	}
	a.Publish(appr.TaskID, Event{
		"type": "room.failed", "roomId": appr.TaskID, "code": "DECIDED_APPROVALS_LIMIT",
	})
	fresh, gerr := a.Repo.GetApproval(ctx, p.TenantID, p.UserID, appr.ID)
	if gerr != nil {
		fresh = appr
	}
	return nil, &StatusError{
		Status: 409, Code: "ROOM_FAILED", Message: roomFailedMessage,
		Approval: approvalFromRecord(fresh),
	}
}

// deliver classifies acceptance. fetch is used only after T2.
func (a *App) deliver(ctx context.Context, appr store.ApprovalRecord, value string, attempt int) (string, func(context.Context) (store.DecideResultWrite, error), error) {
	if a.Orch != nil {
		upd, err := a.Orch.Decide(ctx, appr.TaskID, appr.ID, id("tn_"), appr.ApprovalRequestID, value, id("tn_"))
		kind := classifyDelivery(err, errors.Is(ctx.Err(), context.DeadlineExceeded))
		if kind != "delivered" {
			return kind, nil, err
		}
		return "delivered", func(ctx context.Context) (store.DecideResultWrite, error) {
			res, err := upd.Result(ctx)
			if err != nil {
				return store.DecideResultWrite{}, err
			}
			if value == "reject" {
				return closeWrite(appr, attempt), nil
			}
			if res.Turn == nil {
				return store.DecideResultWrite{
					ApprovalID: appr.ID, Attempt: attempt, RoomID: appr.TaskID, UpdateRoom: false,
				}, nil
			}
			return turnWrite(appr, attempt, runTurnFromOrch(*res.Turn)), nil
		}, nil
	}
	if value == "reject" {
		err := a.Worker.Call(ctx, "abort", map[string]any{
			"roomId": appr.TaskID, "sessionId": appr.SessionID, "turnId": id("tn_"), "reason": "rejected",
		}, &worker.AbortedOut{})
		kind := classifyDelivery(err, errors.Is(ctx.Err(), context.DeadlineExceeded))
		if kind != "delivered" {
			return kind, nil, err
		}
		return "delivered", func(context.Context) (store.DecideResultWrite, error) {
			return closeWrite(appr, attempt), nil
		}, nil
	}
	var applied worker.AppliedOut
	err := a.Worker.Call(ctx, "resolveApproval", map[string]any{
		"roomId":            appr.TaskID,
		"sessionId":         appr.SessionID,
		"turnId":            id("tn_"),
		"approvalRequestId": appr.ApprovalRequestID,
		"outcome":           "allowed-once",
	}, &applied)
	kind := classifyDelivery(err, errors.Is(ctx.Err(), context.DeadlineExceeded))
	if kind != "delivered" {
		return kind, nil, err
	}
	return "delivered", func(ctx context.Context) (store.DecideResultWrite, error) {
		var turn worker.RunTurnOut
		if err := a.Worker.Call(ctx, "runTurn", map[string]any{
			"roomId":              appr.TaskID,
			"sessionId":           appr.SessionID,
			"turnId":              id("tn_"),
			"message":             "",
			"resumeAfterApproval": true,
		}, &turn); err != nil {
			return store.DecideResultWrite{}, err
		}
		return turnWrite(appr, attempt, turn), nil
	}, nil
}

func classifyDelivery(err error, timedOut bool) string {
	if err == nil {
		return "delivered"
	}
	if strings.Contains(err.Error(), "DECIDED_APPROVALS_LIMIT") {
		return "fatal"
	}
	if errors.Is(err, ErrNotDelivered) || strings.Contains(err.Error(), "NOT_DELIVERED") {
		return "not_delivered"
	}
	_ = timedOut
	return "unknown"
}

func closeWrite(appr store.ApprovalRecord, attempt int) store.DecideResultWrite {
	return store.DecideResultWrite{
		ApprovalID: appr.ID,
		Attempt:    attempt,
		RoomID:     appr.TaskID,
		RoomState:  string(RoomClosed),
		UpdateRoom: true,
	}
}

func turnWrite(appr store.ApprovalRecord, attempt int, out worker.RunTurnOut) store.DecideResultWrite {
	w := store.DecideResultWrite{
		ApprovalID: appr.ID,
		Attempt:    attempt,
		RoomID:     appr.TaskID,
		RoomState:  string(RoomRunning),
		UpdateRoom: true,
		Texts:      out.Texts,
	}
	if out.Status == "needs_approval" {
		w.RoomState = string(RoomAwaitingApproval)
		next := &store.ApprovalRecord{ID: id("ap_"), TaskID: appr.TaskID, Status: "pending", CreatedAt: time.Now().UTC()}
		if ask := out.Approval; ask != nil {
			next.ApprovalRequestID = ask.ApprovalRequestID
			next.CallID = ask.CallID
			next.ToolName = ask.ToolName
			next.Reason = ask.Reason
		}
		w.Next = next
	}
	return w
}

func (a *App) noteDelivery(tenantID string, appr store.ApprovalRecord, state string) {
	a.Publish(appr.TaskID, Event{
		"type":              "approval.delivery_updated",
		"roomId":            appr.TaskID,
		"approvalRequestId": appr.ApprovalRequestID,
		"status":            appr.Status,
		"decision":          appr.Decision,
		"deliveryState":     state,
	})
	_ = tenantID
}

func (a *App) publishResult(tenantID string, w store.DecideResultWrite) {
	if w.Next != nil {
		a.Publish(w.RoomID, Event{
			"type": "approval.asked", "roomId": w.RoomID, "approvalId": w.Next.ID,
			"toolName": w.Next.ToolName, "reason": w.Next.Reason,
		})
	}
	if w.UpdateRoom && w.RoomState == string(RoomClosed) {
		a.Publish(w.RoomID, Event{"type": "session.status", "roomId": w.RoomID, "status": "closed"})
		a.roomClosed(w.RoomID)
	}
}

package app

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/failtext"
	"github.com/mindreon/orbit-control/internal/orch"
	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/worker"
)

const (
	workerErrorMessage = "the resumed turn failed"
	limitCode          = "DECIDED_APPROVALS_LIMIT"
)

func (a *App) delivery() store.DeliveryStore {
	ds, _ := a.Repo.(store.DeliveryStore)
	return ds
}

func (a *App) startReconcile() {
	if a.delivery() == nil {
		return
	}
	a.reconcileStop = make(chan struct{})
	interval := a.ReconcileInterval
	stop := a.reconcileStop
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), a.TurnTimeout)
				_ = a.ReconcileDeliveredResults(ctx)
				cancel()
			}
		}
	}()
}

func (a *App) stopReconcile() {
	if a.reconcileStop == nil {
		return
	}
	a.reconcileOnce.Do(func() { close(a.reconcileStop) })
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
	roomRec, err := a.Repo.GetRoomForWorker(ctx, p.TenantID, appr.TaskID)
	if err != nil {
		return nil, err
	}
	a.remember(roomRec)
	if roomRec.State == string(RoomFailed) {
		return nil, a.roomFailedStatus(roomRec, appr)
	}
	if appr.Status != "pending" || appr.DeliveryState != "" {
		return a.answerFromRow(ctx, p, appr, value)
	}
	attempt, err := ds.ClaimDecision(ctx, p.TenantID, approvalID, value)
	if err != nil {
		if errors.Is(err, store.ErrApprovalNotPending) {
			return a.answerFromRow(ctx, p, appr, value)
		}
		return nil, err
	}
	appr.Status = "decided"
	appr.Decision = value
	appr.DeliveryState = "in_flight"
	appr.DeliveryAttempt = attempt
	nowAt := time.Now().UTC()
	appr.DecidedAt = &nowAt
	a.noteDelivery(appr, "in_flight")

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

func (a *App) finishDelivery(ctx context.Context, p Principal, appr store.ApprovalRecord, value string, attempt int) (*Approval, error) {
	ds := a.delivery()
	dctx, cancel := context.WithTimeout(ctx, a.DeliveryTimeout)
	kind, upd, err := a.acceptWithRetry(dctx, p, appr, value)
	timedOut := errors.Is(dctx.Err(), context.DeadlineExceeded)
	cancel()
	switch kind {
	case "fatal":
		return a.failLimited(ctx, p, appr, attempt, "in_flight")
	case "not_delivered":
		ok, rerr := ds.ReopenUndelivered(ctx, p.TenantID, appr.ID, attempt)
		if rerr != nil {
			return nil, rerr
		}
		if !ok {
			return a.answerFromRow(ctx, p, appr, value)
		}
		a.noteDelivery(appr, "not_delivered")
		appr.Status = "pending"
		appr.Decision = ""
		appr.DeliveryState = ""
		a.noteDelivery(appr, "")
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
		ok, serr := ds.SetDeliveryState(ctx, p.TenantID, appr.ID, "in_flight", "unknown", attempt)
		if serr != nil {
			return nil, serr
		}
		if !ok {
			return a.answerFromRow(ctx, p, appr, value)
		}
		appr.DeliveryState = "unknown"
		a.noteDelivery(appr, "unknown")
		fresh, gerr := a.Repo.GetApproval(ctx, p.TenantID, p.UserID, appr.ID)
		if gerr != nil {
			fresh = appr
		}
		return nil, &DeliveryUnknownError{Approval: approvalFromRecord(fresh)}
	}
	if err != nil && kind != "delivered" {
		return nil, err
	}
	ok, serr := ds.SetDeliveryState(ctx, p.TenantID, appr.ID, "in_flight", "delivered", attempt)
	if serr != nil {
		return nil, serr
	}
	if !ok {
		return a.answerFromRow(ctx, p, appr, value)
	}
	appr.DeliveryState = "delivered"
	a.noteDelivery(appr, "delivered")
	return a.writeBack(ctx, p, appr, attempt, func(ctx context.Context) (store.DecideResultWrite, error) {
		return a.fetchResult(ctx, upd, appr, value, attempt)
	})
}

func (a *App) acceptWithRetry(ctx context.Context, p Principal, appr store.ApprovalRecord, value string) (string, orch.DecideUpdate, error) {
	backoff := 20 * time.Millisecond
	var last error
	for {
		if ctx.Err() != nil {
			return "unknown", nil, last
		}
		kind, upd, err := a.deliverOnce(ctx, p, appr, value)
		if kind != "unknown" {
			return kind, upd, err
		}
		last = err
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return "unknown", nil, last
		case <-timer.C:
		}
		if backoff < 200*time.Millisecond {
			backoff *= 2
		}
	}
}

func (a *App) deliverOnce(ctx context.Context, p Principal, appr store.ApprovalRecord, value string) (string, orch.DecideUpdate, error) {
	if a.Orch != nil {
		upd, err := a.Orch.Decide(ctx, appr.TaskID, appr.ApprovalRequestID, id("tn_"), value)
		return a.classifyAccept(ctx, p, appr, err), upd, err
	}
	if value == "reject" {
		err := a.Worker.Call(ctx, "abort", map[string]any{
			"roomId": appr.TaskID, "sessionId": appr.SessionID, "turnId": id("tn_"), "reason": "rejected",
		}, &worker.AbortedOut{})
		if err != nil {
			return "unknown", nil, err
		}
		return "delivered", nil, nil
	}
	var applied worker.AppliedOut
	err := a.Worker.Call(ctx, "resolveApproval", map[string]any{
		"roomId":            appr.TaskID,
		"sessionId":         appr.SessionID,
		"turnId":            id("tn_"),
		"approvalRequestId": appr.ApprovalRequestID,
		"outcome":           "allowed-once",
	}, &applied)
	if err != nil {
		return "unknown", nil, err
	}
	return "delivered", nil, nil
}

// classifyAccept maps an acceptance error. NotDelivered requires
// ApplicationError type APPROVAL_UNKNOWN and a successful decideConfig
// whose ttlS/2 still covers decided_at. Everything else uncertain is Unknown.
func (a *App) classifyAccept(ctx context.Context, p Principal, appr store.ApprovalRecord, err error) string {
	if err == nil {
		return "delivered"
	}
	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		return "unknown"
	}
	switch appErr.Type() {
	case limitCode:
		return "fatal"
	case "APPROVAL_UNKNOWN":
		if a.Orch == nil {
			return "unknown"
		}
		cfg, cerr := a.Orch.DecideConfig(ctx, appr.TaskID)
		if cerr != nil || cfg.TTLS <= 0 {
			return "unknown"
		}
		decidedAt := time.Now()
		if fresh, gerr := a.Repo.GetApproval(ctx, p.TenantID, p.UserID, appr.ID); gerr == nil && fresh.DecidedAt != nil {
			decidedAt = *fresh.DecidedAt
		} else if appr.DecidedAt != nil {
			decidedAt = *appr.DecidedAt
		}
		if time.Since(decidedAt) < time.Duration(cfg.TTLS)*time.Second/2 {
			return "not_delivered"
		}
		return "unknown"
	default:
		return "unknown"
	}
}

func (a *App) fetchResult(ctx context.Context, upd orch.DecideUpdate, appr store.ApprovalRecord, value string, attempt int) (store.DecideResultWrite, error) {
	if a.Orch == nil {
		if value == "reject" {
			return closeWrite(appr, attempt), nil
		}
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
	}
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
}

func (a *App) writeBack(ctx context.Context, p Principal, appr store.ApprovalRecord, attempt int, fetch func(context.Context) (store.DecideResultWrite, error)) (*Approval, error) {
	wctx, cancel := context.WithTimeout(ctx, a.TurnTimeout)
	defer cancel()
	w, err := fetch(wctx)
	if err != nil {
		if isDecidedLimit(err) {
			return a.failLimited(ctx, p, appr, attempt, "delivered")
		}
		a.Log.Printf("WARN alert=decide_result_failed approval=%s", appr.ID)
		return nil, &StatusError{
			Status: 502, Code: "WORKER_ERROR", Message: workerErrorMessage,
			Approval: approvalFromRecord(appr),
		}
	}
	if body, merr := json.Marshal(w); merr == nil {
		_, _ = a.delivery().SaveResultBody(wctx, p.TenantID, appr.ID, attempt, body)
	}
	if a.skipResultWriteEnabled() {
		return approvalFromRecord(appr), nil
	}
	wrote, err := a.delivery().ApplyDecideResult(wctx, p.TenantID, w)
	if err != nil {
		return nil, err
	}
	if wrote {
		a.publishResult(w)
	}
	fresh, gerr := a.Repo.GetApproval(ctx, p.TenantID, p.UserID, appr.ID)
	if gerr != nil {
		return approvalFromRecord(appr), nil
	}
	return approvalFromRecord(fresh), nil
}

func isDecidedLimit(err error) bool {
	var appErr *temporal.ApplicationError
	return errors.As(err, &appErr) && appErr.Type() == limitCode
}

func (a *App) failLimited(ctx context.Context, p Principal, appr store.ApprovalRecord, attempt int, from string) (*Approval, error) {
	failure, _ := json.Marshal(RoomFailure{Code: limitCode, Message: failtext.DecidedApprovalsLimit})
	ok, err := a.delivery().FailUnresolved(ctx, p.TenantID, appr.ID, from, attempt, appr.TaskID, failure)
	if err != nil {
		return nil, err
	}
	if !ok {
		return a.answerFromRow(ctx, p, appr, appr.Decision)
	}
	appr.DeliveryState = "unresolved"
	a.noteDelivery(appr, "unresolved")
	a.Publish(appr.TaskID, Event{
		"type":   "room.failed",
		"roomId": appr.TaskID,
		"failure": map[string]string{
			"code":    limitCode,
			"message": failtext.DecidedApprovalsLimit,
		},
	})
	fresh, gerr := a.Repo.GetApproval(ctx, p.TenantID, p.UserID, appr.ID)
	if gerr != nil {
		fresh = appr
	}
	return nil, &StatusError{
		Status: 409, Code: "ROOM_FAILED", Message: failtext.DecidedApprovalsLimit,
		Approval: approvalFromRecord(fresh),
	}
}

// answerFromRow re-reads after a transition matched 0 rows and answers from
// that state. It does not deliver, change the room, or emit (FM-68).
func (a *App) answerFromRow(ctx context.Context, p Principal, appr store.ApprovalRecord, value string) (*Approval, error) {
	fresh, err := a.Repo.GetApproval(ctx, p.TenantID, p.UserID, appr.ID)
	if err != nil {
		return nil, &StatusError{Status: 409, Code: "APPROVAL_NOT_PENDING", Message: "approval is not pending"}
	}
	roomRec, rerr := a.Repo.GetRoomForWorker(ctx, p.TenantID, fresh.TaskID)
	if rerr == nil && roomRec.State == string(RoomFailed) {
		return nil, a.roomFailedStatus(roomRec, fresh)
	}
	out := approvalFromRecord(fresh)
	switch {
	case fresh.DeliveryState == "in_flight" || fresh.DeliveryState == "unknown":
		if fresh.Decision == value {
			return nil, &DeliveryUnknownError{Approval: out}
		}
		return nil, &StatusError{
			Status: 409, Code: "APPROVAL_DELIVERY_PENDING",
			Message: "approval delivery is pending", Approval: out,
		}
	case fresh.DeliveryState == "delivered":
		return out, nil
	case fresh.Status == "pending" && fresh.DeliveryState == "":
		return nil, &StatusError{
			Status: 503, Code: "APPROVAL_NOT_DELIVERED",
			Message: "the decision was not delivered; decide again", Approval: out,
		}
	default:
		return nil, &StatusError{
			Status: 409, Code: "APPROVAL_NOT_PENDING",
			Message: "approval is not pending", Approval: out,
		}
	}
}

func (a *App) roomFailedStatus(room store.RoomRecord, appr store.ApprovalRecord) *StatusError {
	msg := failtext.DecidedApprovalsLimit
	if len(room.Failure) > 0 {
		var failure RoomFailure
		if json.Unmarshal(room.Failure, &failure) == nil && failure.Message != "" {
			msg = failure.Message
		}
	}
	return &StatusError{
		Status: 409, Code: "ROOM_FAILED", Message: msg,
		Approval: approvalFromRecord(appr),
	}
}

func (a *App) ingestRoomFailed(ctx context.Context, rec store.RoomRecord, raw []byte) error {
	ds := a.delivery()
	if ds == nil {
		return invalidf("room.failed requires durable storage")
	}
	var body struct {
		Failure json.RawMessage `json:"failure"`
	}
	_ = json.Unmarshal(raw, &body)
	failure := body.Failure
	if len(failure) == 0 {
		failure = []byte(`{}`)
	}
	_, err := ds.ApplyRoomFailed(ctx, rec.TenantID, rec.ID, failure, store.EventRecord{
		EventUID: id("ev_"),
		TaskID:   rec.ID,
		Type:     "room.failed",
		Source:   "worker",
		Payload:  raw,
	})
	return err
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

func (a *App) noteDelivery(appr store.ApprovalRecord, state string) {
	a.Publish(appr.TaskID, Event{
		"type":              "approval.delivery_updated",
		"roomId":            appr.TaskID,
		"approvalRequestId": appr.ApprovalRequestID,
		"status":            appr.Status,
		"decision":          appr.Decision,
		"deliveryState":     state,
	})
}

func (a *App) publishResult(w store.DecideResultWrite) {
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

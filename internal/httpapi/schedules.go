package httpapi

import (
	"context"
	"encoding/json/jsontext"
	"net/http"

	"go.temporal.io/sdk/client"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/schedules"
	"github.com/mindreon/orbit-control/internal/sessions"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func (h *Handler) scheduleService() schedules.Service {
	if h.Schedules != nil {
		return *h.Schedules
	}
	return schedules.Service{DB: h.DB, Locks: h.sessionService().Locks, DefaultLimit: h.Config.SchedulesDefaultLimit}
}
func (h *Handler) listSchedules(out http.ResponseWriter, r *http.Request) error {
	page, err := httpx.Pagination(r)
	if err != nil {
		return err
	}
	result, err := schedules.List(r.Context(), r.URL.Query().Get("workspace_id"), page)
	if err != nil {
		return err
	}
	httpx.JSON(out, 200, result)
	return nil
}
func (h *Handler) writeSchedule(out http.ResponseWriter, r *http.Request) error {
	if r.Method == http.MethodPost && r.PathValue("id") == "" && r.Header.Get("Idempotency-Key") == "" {
		return httpx.E(400, "IDEMPOTENCY_KEY_REQUIRED", "Idempotency-Key is required")
	}
	return h.twoPhase(out, r, func(ctx context.Context, body []byte) (func(context.Context, *httpx.Recorder) error, error) {
		svc := h.scheduleService()
		id := r.PathValue("id")
		var row schedules.Schedule
		var err error
		status := 200
		switch r.Method {
		case http.MethodPost:
			var in schedules.CreateInput
			if err = decodeBody(body, &in); err != nil {
				return nil, err
			}
			id = ids.Derive(auth.Tenant(ctx), auth.From(ctx).ID+"|schedule|"+r.Header.Get("Idempotency-Key"))
			row, err = svc.Create(ctx, id, in)
			status = 201
		case http.MethodPatch:
			var in schedules.PatchInput
			if err = decodeBody(body, &in); err != nil {
				return nil, err
			}
			row, err = svc.Patch(ctx, id, in)
		case http.MethodDelete:
			row, err = svc.Delete(ctx, id)
		}
		if err != nil {
			return nil, err
		}
		tenant := auth.Tenant(ctx)
		return func(ctx context.Context, rec *httpx.Recorder) error {
			pending, syncErr := svc.Sync(ctx, tenant, id)
			row.SyncPending = pending || syncErr != nil
			httpx.JSON(rec, status, row)
			return nil
		}, nil
	})
}
func (h *Handler) getSchedule(out http.ResponseWriter, r *http.Request) error {
	return h.twoPhase(out, r, func(ctx context.Context, _ []byte) (func(context.Context, *httpx.Recorder) error, error) {
		row, err := schedules.Get(ctx, r.PathValue("id"))
		if err != nil {
			return nil, err
		}
		rows, err := db.Tx(ctx).Query(ctx,
			`SELECT to_jsonb(sr) FROM schedule_runs sr
 WHERE tenant_id=$1 AND schedule_id=$2 ORDER BY fire_at DESC LIMIT 20`, auth.Tenant(ctx), row.ID)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		runs := []jsontext.Value{}
		for rows.Next() {
			var raw []byte
			if err = rows.Scan(&raw); err != nil {
				return nil, err
			}
			runs = append(runs, raw)
		}
		if err = rows.Err(); err != nil {
			return nil, err
		}
		tenant := auth.Tenant(ctx)
		return func(ctx context.Context, rec *httpx.Recorder) error {
			var summary any
			if svc := h.scheduleService(); svc.Temporal != nil {
				if description, e := svc.Temporal.GetHandle(ctx, w.ScheduleID(tenant, row.ID)).Describe(ctx); e == nil {
					var next any
					if len(description.Info.NextActionTimes) > 0 {
						next = description.Info.NextActionTimes[0]
					}
					summary = map[string]any{"next_fire_at": next, "num_actions": description.Info.NumActions,
						"skipped_overlap": description.Info.NumActionsSkippedOverlap, "missed_catchup": description.Info.NumActionsMissedCatchupWindow}
				}
			}
			httpx.JSON(rec, 200, map[string]any{"schedule": row, "runs": runs, "describe": summary})
			return nil
		}, nil
	})
}
func (h *Handler) triggerSchedule(out http.ResponseWriter, r *http.Request) error {
	return h.twoPhase(out, r, func(ctx context.Context, _ []byte) (func(context.Context, *httpx.Recorder) error, error) {
		row, err := h.scheduleService().Manage(ctx, r.PathValue("id"))
		if err != nil {
			return nil, err
		}
		tenant := auth.Tenant(ctx)
		return func(ctx context.Context, rec *httpx.Recorder) error {
			svc := h.scheduleService()
			if svc.Temporal == nil {
				return temporalError(context.Canceled, "")
			}
			if err := svc.Temporal.GetHandle(ctx, w.ScheduleID(tenant, row.ID)).Trigger(ctx, client.ScheduleTriggerOptions{}); err != nil {
				return temporalError(err, "")
			}
			rec.WriteHeader(202)
			return nil
		}, nil
	})
}
func (h *Handler) teamView(out http.ResponseWriter, r *http.Request) error {
	run, session, err := sessions.GetRun(r.Context(), r.PathValue("r"), "session.read")
	if err != nil {
		return err
	}
	if session.EntryKind != "team" {
		return httpx.E(404, "NOT_FOUND", "Team run not found")
	}
	var raw []byte
	err = db.Tx(r.Context()).QueryRow(r.Context(), `SELECT jsonb_build_object('board',t.board,'finished',t.finished,
 'summary',COALESCE(t.summary,''),'members',COALESCE((SELECT jsonb_agg(jsonb_build_object('name',m.name,'role',m.role,
 'status',CASE WHEN m.status='finished' THEN 'finished' WHEN EXISTS(SELECT 1 FROM interactions i WHERE i.tenant_id=m.tenant_id
 AND i.run_id=m.run_id AND i.member=m.name AND i.status IN ('pending','escalated') AND i.kind<>'ask_user') THEN 'waiting_approval'
 WHEN EXISTS(SELECT 1 FROM interactions i WHERE i.tenant_id=m.tenant_id AND i.run_id=m.run_id AND i.member=m.name
 AND i.status IN ('pending','escalated') AND i.kind='ask_user') THEN 'waiting_input' ELSE m.status END,
 'task_id',m.task_id,'turn',m.turn) ORDER BY m.name) FROM team_members m WHERE m.tenant_id=t.tenant_id AND m.run_id=t.run_id),'[]'::jsonb))
 FROM team_runs t WHERE t.tenant_id=$1 AND t.run_id=$2`, auth.Tenant(r.Context()), run.ID).Scan(&raw)
	if err != nil {
		return err
	}
	out.Header().Set("Content-Type", "application/json")
	_, err = out.Write(raw)
	return err
}

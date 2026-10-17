package kernelapi

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/schedules"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type teamResponse struct {
	body    any
	deliver func(context.Context) (any, error)
	syncID  string
}
type teamHandler func(context.Context, auth.CapabilityClaims, *http.Request) (teamResponse, error)
type teamRoute struct {
	handler teamHandler
	team    bool
}

func (h *Handler) teamRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /internal/v1/runs/{run}/team/updates/{name}", h.teamRequest(teamRoute{handler: h.updateTeam, team: true}))
	mux.HandleFunc("GET /internal/v1/runs/{run}/team/board", h.teamRequest(teamRoute{handler: h.readTeamBoard, team: true}))
	mux.HandleFunc("POST /internal/v1/runs/{run}/schedules", h.teamRequest(teamRoute{handler: h.createSchedule, team: false}))
	mux.HandleFunc("GET /internal/v1/runs/{run}/schedules", h.teamRequest(teamRoute{handler: h.listSchedules, team: false}))
	mux.HandleFunc("DELETE /internal/v1/runs/{run}/schedules/{id}", h.teamRequest(teamRoute{handler: h.deleteSchedule, team: false}))
}

// The wrapper authenticates and closes the transaction before any Temporal call.
func (h *Handler) teamRequest(route teamRoute) http.HandlerFunc {
	return func(out http.ResponseWriter, r *http.Request) {
		token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		claims, err := h.Signer.VerifyCapability(token)
		if err != nil {
			httpx.WriteError(out, httpx.E(401, "TOKEN_INVALID", "Invalid capability"))
			return
		}
		if claims.Run != r.PathValue("run") || claims.Purpose != "segment" {
			httpx.WriteError(out, httpx.E(403, "TOKEN_SUPERSEDED", "Capability does not authorize request"))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		var response teamResponse
		err = h.DB.WithTenant(ctx, claims.Tenant, func(tx pgx.Tx) error {
			c := db.Context(ctx, tx)
			if err := check(c, claims); err != nil {
				return err
			}
			if route.team {
				lock, err := getLock(c, claims)
				if err != nil {
					return err
				}
				if lock.Entry.Kind != "team" || !slices.ContainsFunc(lock.Members,
					func(m contract.LockMember) bool { return string(m.Name) == claims.Member }) {
					return httpx.E(403, "TOKEN_SUPERSEDED", "Capability is not a team member")
				}
			} else {
				var principal auth.Principal
				if err := tx.QueryRow(c, `SELECT p.id::text,p.kind FROM runs r JOIN principals p ON p.id=r.created_by
 WHERE r.tenant_id=$1 AND r.id=$2`, claims.Tenant, claims.Run).Scan(&principal.ID, &principal.Kind); err != nil {
					return err
				}
				c = auth.Context(c, principal, claims.Tenant)
				if h.Schedules == nil {
					return httpx.E(503, "TEMPORAL_UNAVAILABLE", "Schedules unavailable")
				}
			}
			var err error
			response, err = route.handler(c, claims, r)
			return err
		})
		if err != nil {
			httpx.WriteError(out, err)
			return
		}
		if response.deliver != nil {
			response.body, err = response.deliver(ctx)
			if err != nil {
				httpx.WriteError(out, err)
				return
			}
		}
		if response.syncID != "" {
			_, _ = h.Schedules.Sync(ctx, claims.Tenant, response.syncID)
		}
		httpx.JSON(out, 200, response.body)
	}
}
func (h *Handler) updateTeam(_ context.Context, c auth.CapabilityClaims, r *http.Request) (teamResponse, error) {
	input, key, err := teamUpdate(r, c.Member)
	if err != nil {
		return teamResponse{}, err
	}
	name := r.PathValue("name")
	return teamResponse{deliver: func(ctx context.Context) (any, error) {
		var result any
		err := h.deliverTeam(ctx, c, name, key, input, &result)
		return result, err
	}}, nil
}
func (h *Handler) readTeamBoard(ctx context.Context, c auth.CapabilityClaims, _ *http.Request) (teamResponse, error) {
	var raw []byte
	if err := db.Tx(ctx).QueryRow(ctx, `SELECT board FROM team_runs WHERE tenant_id=$1 AND run_id=$2`, c.Tenant,
		c.Run).Scan(&raw); err != nil {
		return teamResponse{}, err
	}
	var board contract.TeamBoard
	err := json.Unmarshal(raw, &board)
	return teamResponse{body: board}, err
}
func scheduleWorkspace(ctx context.Context, c auth.CapabilityClaims) (string, error) {
	var workspace string
	err := db.Tx(ctx).QueryRow(ctx, `SELECT s.workspace_id::text FROM runs r JOIN sessions s
 ON s.tenant_id=r.tenant_id AND s.id=r.session_id WHERE r.tenant_id=$1 AND r.id=$2`, c.Tenant, c.Run).Scan(&workspace)
	return workspace, err
}
func (h *Handler) listSchedules(ctx context.Context, c auth.CapabilityClaims, _ *http.Request) (teamResponse, error) {
	workspace, err := scheduleWorkspace(ctx, c)
	if err != nil {
		return teamResponse{}, err
	}
	result, err := schedules.List(ctx, workspace, httpx.Page{Limit: 100})
	return teamResponse{body: result}, err
}
func (h *Handler) createSchedule(ctx context.Context, c auth.CapabilityClaims, r *http.Request) (teamResponse, error) {
	workspace, err := scheduleWorkspace(ctx, c)
	if err != nil {
		return teamResponse{}, err
	}
	var req contract.ScheduleCreateRequest
	if err := decode(r, "ScheduleCreateRequest", &req); err != nil {
		return teamResponse{}, err
	}
	in := schedules.CreateInput{WorkspaceID: workspace, Name: req.Name, Description: req.Description, Cron: req.Cron,
		Timezone: req.Timezone, EntryRef: req.EntryRef, Prompt: req.Prompt, BudgetMillicredits: req.BudgetMillicredits,
		OverlapPolicy: string(req.OverlapPolicy), CatchupWindowSeconds: req.CatchupWindowSeconds}
	id := ids.Derive(c.Run, "schedule:"+req.RequestId)
	result, err := h.Schedules.Create(ctx, id, in)
	return teamResponse{body: result, syncID: id}, err
}
func (h *Handler) deleteSchedule(ctx context.Context, c auth.CapabilityClaims, r *http.Request) (teamResponse, error) {
	workspace, err := scheduleWorkspace(ctx, c)
	if err != nil {
		return teamResponse{}, err
	}
	row, err := schedules.Get(ctx, r.PathValue("id"))
	if err != nil {
		return teamResponse{}, err
	}
	if row.WorkspaceID != workspace {
		return teamResponse{}, httpx.E(404, "SCHEDULE_NOT_FOUND", "Schedule not found")
	}
	result, err := h.Schedules.Delete(ctx, row.ID)
	return teamResponse{body: result, syncID: row.ID}, err
}
func teamUpdate(r *http.Request, member string) (any, string, error) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	if err != nil {
		return nil, "", err
	}
	if len(raw) > 1<<20 {
		return nil, "", httpx.E(413, "BODY_TOO_LARGE", "Request exceeds limit")
	}
	name := r.PathValue("name")
	var body map[string]jsontext.Value
	if err = json.Unmarshal(raw, &body); err != nil {
		return nil, "", httpx.E(422, "INVALID_INPUT", "Invalid update")
	}
	caller, _ := json.Marshal(member)
	body["caller"] = caller
	raw, err = json.Marshal(body)
	if err != nil {
		return nil, "", err
	}
	var input any
	switch name {
	case "CreateTasks":
		input = &contract.CreateTasksInput{}
	case "AssignTask":
		input = &contract.AssignTaskInput{}
	case "SendMessage":
		input = &contract.SendMessageInput{}
	case "CompleteTask":
		input = &contract.CompleteTaskInput{}
	case "SubmitVerdict":
		input = &contract.SubmitVerdictInput{}
	case "FinishTeam":
		input = &contract.FinishTeamInput{}
	default:
		return nil, "", httpx.E(404, "NOT_FOUND", "Unknown team update")
	}
	if err = contract.Validate(name+"Input", raw); err != nil {
		return nil, "", httpx.E(422, "INVALID_INPUT", "Invalid team update")
	}
	if err = json.Unmarshal(raw, input); err != nil {
		return nil, "", httpx.E(422, "INVALID_INPUT", "Invalid team update")
	}
	var identity struct {
		RequestID string `json:"request_id"`
	}
	if err = json.Unmarshal(raw, &identity); err != nil {
		return nil, "", err
	}
	return input, identity.RequestID, nil
}
func (h *Handler) deliverTeam(ctx context.Context, c auth.CapabilityClaims, name, key string, in, out any) error {
	if h.Temporal == nil {
		return teamTimeout()
	}
	for {
		err := h.Temporal.Update(ctx, w.TeamRunWorkflowID(c.Tenant, c.Run), name, key, in, out)
		if err == nil {
			return nil
		}
		if app, ok := errors.AsType[*temporal.ApplicationError](err); ok {
			if _, known := teamErrorCodes[app.Type()]; known {
				return httpx.E(409, app.Type(), app.Message())
			}
		}
		if _, missing := errors.AsType[*serviceerror.NotFound](err); !missing {
			return teamTimeout()
		}
		var ended bool
		if err = h.DB.WithTenant(ctx, c.Tenant, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx, `SELECT ended_at IS NOT NULL FROM runs WHERE tenant_id=$1 AND id=$2`, c.Tenant, c.Run).Scan(&ended)
		}); err != nil {
			return teamTimeout()
		}
		if ended {
			return httpx.E(409, "TEAM_FINISHED", "Team has finished")
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return teamTimeout()
		case <-timer.C:
		}
	}
}
func teamTimeout() *httpx.Error {
	e := httpx.E(503, "TEAM_UPDATE_TIMEOUT", "Team update did not complete")
	e.Retryable = true
	return e
}

var teamErrorCodes = map[string]struct{}{
	"TEAM_NOT_LEADER": {}, "TEAM_NOT_ASSIGNEE": {}, "TEAM_NOT_REVIEWER": {}, "TEAM_NOT_IN_TURN": {},
	"TEAM_UNKNOWN_TASK": {}, "TEAM_UNKNOWN_MEMBER": {}, "TEAM_BAD_TRANSITION": {}, "TEAM_TOO_MANY_TASKS": {},
	"TEAM_MAILBOX_FULL": {}, "TEAM_TEXT_TOO_LONG": {}, "TEAM_FINISHED": {}, "TEAM_UPDATE_TIMEOUT": {},
}

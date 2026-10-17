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

func (h *Handler) teamRoutes(mux *http.ServeMux) {
	for _, pattern := range []string{"POST /internal/v1/runs/{run}/team/updates/{name}",
		"GET /internal/v1/runs/{run}/team/board", "POST /internal/v1/runs/{run}/schedules",
		"GET /internal/v1/runs/{run}/schedules", "DELETE /internal/v1/runs/{run}/schedules/{id}"} {
		mux.HandleFunc(pattern, h.teamRequest)
	}
}
func (h *Handler) teamRequest(out http.ResponseWriter, r *http.Request) {
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
	var input any
	var requestID string
	var board contract.TeamBoard
	var result any
	var syncID string
	isTeam := strings.Contains(r.URL.Path, "/team/")
	update := r.PathValue("name") != ""
	err = h.DB.WithTenant(ctx, claims.Tenant, func(tx pgx.Tx) error {
		c := db.Context(ctx, tx)
		if err := check(c, claims); err != nil {
			return err
		}
		if isTeam {
			lock, err := getLock(c, claims)
			if err != nil {
				return err
			}
			if lock.Entry.Kind != "team" || !slices.ContainsFunc(lock.Members,
				func(m contract.LockMember) bool { return string(m.Name) == claims.Member }) {
				return httpx.E(403, "TOKEN_SUPERSEDED", "Capability is not a team member")
			}
			if update {
				var err error
				input, requestID, err = teamUpdate(r, claims.Member)
				return err
			}
			var raw []byte
			if err := tx.QueryRow(c, `SELECT board FROM team_runs WHERE tenant_id=$1 AND run_id=$2`, claims.Tenant,
				claims.Run).Scan(&raw); err != nil {
				return err
			}
			return json.Unmarshal(raw, &board)
		}
		var principal auth.Principal
		var workspace string
		if err := tx.QueryRow(c, `SELECT p.id::text,p.kind,s.workspace_id::text FROM runs r JOIN sessions s
 ON s.tenant_id=r.tenant_id AND s.id=r.session_id JOIN principals p ON p.id=r.created_by
 WHERE r.tenant_id=$1 AND r.id=$2`, claims.Tenant, claims.Run).Scan(&principal.ID, &principal.Kind, &workspace); err != nil {
			return err
		}
		c = auth.Context(c, principal, claims.Tenant)
		if h.Schedules == nil {
			return httpx.E(503, "TEMPORAL_UNAVAILABLE", "Schedules unavailable")
		}
		switch r.Method {
		case http.MethodGet:
			var err error
			result, err = schedules.List(c, workspace, httpx.Page{Limit: 100})
			return err
		case http.MethodPost:
			var req contract.ScheduleCreateRequest
			if err := decode(r, "ScheduleCreateRequest", &req); err != nil {
				return err
			}
			in := schedules.CreateInput{WorkspaceID: workspace, Name: req.Name, Description: req.Description, Cron: req.Cron,
				Timezone: req.Timezone, EntryRef: req.EntryRef, Prompt: req.Prompt, BudgetMillicredits: req.BudgetMillicredits}
			in.OverlapPolicy = string(req.OverlapPolicy)
			in.CatchupWindowSeconds = req.CatchupWindowSeconds
			syncID = ids.Derive(claims.Run, "schedule:"+req.RequestId)
			var err error
			result, err = h.Schedules.Create(c, syncID, in)
			return err
		case http.MethodDelete:
			row, err := schedules.Get(c, r.PathValue("id"))
			if err != nil {
				return err
			}
			if row.WorkspaceID != workspace {
				return httpx.E(404, "SCHEDULE_NOT_FOUND", "Schedule not found")
			}
			syncID = row.ID
			result, err = h.Schedules.Delete(c, row.ID)
			return err
		}
		return nil
	})
	if err != nil {
		httpx.WriteError(out, err)
		return
	}
	if update {
		err = h.deliverTeam(ctx, claims, r.PathValue("name"), requestID, input, &result)
		if err != nil {
			httpx.WriteError(out, err)
			return
		}
		httpx.JSON(out, 200, result)
		return
	}
	if isTeam {
		httpx.JSON(out, 200, board)
		return
	}
	if syncID != "" {
		_, _ = h.Schedules.Sync(ctx, claims.Tenant, syncID)
	}
	httpx.JSON(out, 200, result)
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
			var code contract.TeamErrorCode
			raw, _ := json.Marshal(app.Type())
			if json.Unmarshal(raw, &code) == nil {
				return httpx.E(409, app.Type(), "Team update rejected")
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

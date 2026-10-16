package httpapi

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/interactions"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/sessions"
	"github.com/mindreon/orbit-control/internal/tenancy"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func (h *Handler) getRun(out http.ResponseWriter, r *http.Request) error {
	run, _, err := sessions.GetRun(r.Context(), r.PathValue("r"), "session.read")
	if err != nil {
		return err
	}
	httpx.JSON(out, 200, run)
	return nil
}
func (h *Handler) runSegments(out http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	run, _, err := sessions.GetRun(ctx, r.PathValue("r"), "session.read")
	if err != nil {
		return err
	}
	result, err := sessions.Segments(ctx, run.ID)
	if err != nil {
		return err
	}
	httpx.JSON(out, 200, result)
	return nil
}
func (h *Handler) streamToken(out http.ResponseWriter, r *http.Request) error {
	run, s, err := sessions.GetRun(r.Context(), r.PathValue("r"), "session.read")
	if err != nil {
		return err
	}
	token, err := h.issueStream(r.Context(), s, run.ID)
	if err != nil {
		return err
	}
	httpx.JSON(out, 200, map[string]any{"stream_token": token, "expires_in": 900})
	return nil
}
func (h *Handler) sessionStreamToken(out http.ResponseWriter, r *http.Request) error {
	s, err := sessions.Get(r.Context(), r.PathValue("s"), "session.read")
	if err != nil {
		return err
	}
	token, err := h.issueStream(r.Context(), s, "")
	if err != nil {
		return err
	}
	httpx.JSON(out, 200, map[string]any{"stream_token": token, "expires_in": 900})
	return nil
}
func (h *Handler) issueStream(ctx context.Context, s sessions.Session, runID string) (string, error) {
	if s.Status == "archived" {
		return "", httpx.E(409, "SESSION_ARCHIVED", "Session is archived")
	}
	scope := "session"
	if runID != "" {
		scope = "run"
	}
	return h.Auth.Stream.Issue(auth.StreamClaims{TenantID: auth.Tenant(ctx), SessionID: s.ID, RunID: runID, Scope: scope})
}

// Two-phase mutations recheck access after delivery before issuing a stream token.
func (h *Handler) issueRunStream(ctx context.Context, runID string) (string, error) {
	var token string
	err := h.DB.WithTenant(ctx, auth.Tenant(ctx), func(tx pgx.Tx) error {
		if err := tenancy.Active(ctx, tx, auth.From(ctx).ID); err != nil {
			return err
		}
		ctx = db.Context(ctx, tx)
		run, s, err := sessions.GetRun(ctx, runID, "session.read")
		if err != nil {
			return err
		}
		token, err = h.issueStream(ctx, s, run.ID)
		return err
	})
	return token, err
}
func (h *Handler) controlRun(out http.ResponseWriter, r *http.Request) error {
	return h.twoPhase(out, r, func(ctx context.Context, body []byte) (func(context.Context, *httpx.Recorder) error, error) {
		run, s, err := sessions.GetRun(ctx, r.PathValue("r"), "run.control")
		if err != nil {
			return nil, err
		}
		if w.Terminal(run.Status) {
			return nil, httpx.E(409, "RUN_FINISHED", "Run has finished")
		}
		tenant, actor := auth.Tenant(ctx), auth.From(ctx).ID
		action := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		if action == "steer" {
			var in struct {
				Text string `json:"text"`
			}
			if err = decodeBody(body, &in); err != nil {
				return nil, err
			}
			if strings.TrimSpace(in.Text) == "" || utf8.RuneCountInString(in.Text) > 8192 {
				return nil, httpx.E(422, "INVALID_INPUT", "Invalid steer text")
			}
			id := ids.New()
			if key := r.Header.Get("Idempotency-Key"); key != "" {
				id = ids.Derive(run.ID, actor+"|steer|"+key)
			}
			return h.prepareSteer(ctx, s, run.ID, id, in.Text, false)
		}
		var in struct {
			Reason string `json:"reason"`
		}
		if len(body) > 0 {
			if err = decodeBody(body, &in); err != nil {
				return nil, err
			}
		}
		if action != "cancel" && run.Status == "queued" {
			return nil, httpx.E(409, "RUN_NOT_ACTIVE", "Queued run cannot be paused or resumed")
		}
		if err = h.logWrite(ctx, "run."+action, "run", run.ID, map[string]any{}); err != nil {
			return nil, err
		}
		return func(ctx context.Context, rec *httpx.Recorder) error {
			if h.Temporal == nil {
				return temporalError(errors.New("no client"), "")
			}
			if action == "cancel" {
				var result w.CancelRunOutput
				if err := h.Temporal.Update(
					ctx,
					w.SessionWorkflowID(tenant, s.ID),
					w.CancelRun,
					"cancel:"+run.ID,
					w.CancelRunInput{RunID: run.ID, ActorID: actor, Reason: in.Reason},
					&result,
				); err != nil {
					return temporalError(err, "RUN_FINISHED")
				}
				httpx.JSON(rec, 202, result)
				return nil
			}
			signal := w.Pause
			if action == "resume" {
				signal = w.Resume
			}
			if err := h.Temporal.Signal(ctx, w.AgentRunWorkflowID(tenant, run.ID), signal, w.ActorInput{ActorID: actor}); err != nil {
				return temporalError(err, "RUN_NOT_ACTIVE")
			}
			rec.WriteHeader(202)
			return nil
		}, nil
	})
}
func (h *Handler) prepareSteer(
	ctx context.Context,
	s sessions.Session,
	runID, messageID, text string,
	submission bool,
) (
	func(context.Context, *httpx.Recorder) error,
	error,
) {
	tenant, actor := auth.Tenant(ctx), auth.From(ctx).ID
	run, _, err := sessions.GetRun(ctx, runID, "run.create")
	if err != nil {
		return nil, err
	}
	if w.Terminal(run.Status) || run.Status == "queued" {
		return nil, httpx.E(409, "RUN_NOT_ACTIVE", "Run is not active")
	}
	ref := "t/" + tenant + "/runs/" + runID + "/steer/" + messageID + ".json"
	content, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		return nil, err
	}
	if err = h.Objects.PutObject(ctx, tenant, ref, content, "application/json"); err != nil {
		return nil, err
	}
	return func(ctx context.Context, rec *httpx.Recorder) error {
		if h.Temporal == nil {
			return temporalError(errors.New("no client"), "")
		}
		var result w.SteerOutput
		if err := h.Temporal.Update(
			ctx,
			w.AgentRunWorkflowID(tenant, runID),
			w.Steer,
			messageID,
			w.SteerInput{MessageID: messageID, ContentRef: ref, ActorID: actor},
			&result,
		); err != nil {
			return temporalError(err, "RUN_NOT_ACTIVE")
		}
		if err := h.DB.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			ctx = db.Context(ctx, tx)

			return sessions.AddSteer(ctx, s.ID, runID, messageID, ref, content)
		}); err != nil {
			return err
		}
		if submission {
			token, err := h.issueRunStream(ctx, runID)
			if err != nil {
				return err
			}
			httpx.JSON(
				rec,
				202,
				map[string]any{
					"run_id":       runID,
					"message_id":   messageID,
					"disposition":  "steered",
					"position":     0,
					"stream_token": token,
				},
			)
		} else {
			httpx.JSON(rec, 202, map[string]any{"message_id": messageID, "queued": result.Queued})
		}
		return nil
	}, nil
}
func (h *Handler) getInteraction(out http.ResponseWriter, r *http.Request) error {
	i, err := interactions.Get(r.Context(), auth.Tenant(r.Context()), r.PathValue("i"))
	if err != nil {
		return err
	}
	if !slices.Contains(i.Approvers, auth.From(r.Context()).ID) {
		return httpx.E(404, "NOT_FOUND", "Interaction not found")
	}
	if _, err = sessions.Get(r.Context(), i.SessionID, "session.read"); err != nil {
		return err
	}
	httpx.JSON(out, 200, i)
	return nil
}
func (h *Handler) listInteractions(out http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	page, err := httpx.Pagination(r)
	if err != nil {
		return err
	}
	result, err := interactions.List(ctx, page, r.URL.Query().Get("status"))
	if err != nil {
		return err
	}
	httpx.JSON(out, 200, result)
	return nil
}

func (h *Handler) resolveInteraction(out http.ResponseWriter, r *http.Request) error {
	return h.twoPhase(out, r, func(ctx context.Context, body []byte) (func(context.Context, *httpx.Recorder) error, error) {
		i, err := interactions.Get(ctx, auth.Tenant(ctx), r.PathValue("i"))
		if err != nil {
			return nil, err
		}
		if _, err = sessions.Get(ctx, i.SessionID, "run.control"); err != nil {
			return nil, err
		}
		actor := auth.From(ctx).ID
		if !slices.Contains(i.Approvers, actor) {
			return nil, httpx.E(403, "FORBIDDEN", "Current principal is not an approver")
		}
		if !interactions.CanResolve(actor, i) {
			return nil, httpx.E(409, "INTERACTION_NOT_PENDING", "Interaction is not pending for this approver")
		}
		if err = contract.Validate("Decision", body); err != nil {
			return nil, httpx.E(422, "INVALID_INPUT", "Invalid decision")
		}
		var decision contract.Decision
		if err = json.Unmarshal(body, &decision); err != nil {
			return nil, err
		}
		if decision.Kind != i.Kind {
			return nil, httpx.E(422, "INVALID_INPUT", "Decision kind does not match interaction")
		}
		tenant := auth.Tenant(ctx)
		return func(ctx context.Context, rec *httpx.Recorder) error {
			if h.Temporal == nil {
				return temporalError(errors.New("no client"), "")
			}
			var result w.ResolveInteractionOutput
			if err := h.Temporal.Update(
				ctx,
				w.AgentRunWorkflowID(tenant, i.RunID),
				w.ResolveInteraction,
				"resolve:"+i.ID,
				w.ResolveInteractionInput{InteractionID: i.ID, ActorID: actor, Decision: decision},
				&result,
			); err != nil {
				return temporalError(err, "INTERACTION_NOT_PENDING")
			}
			i.Status = "resolving"
			httpx.JSON(rec, 200, i)
			return nil
		}, nil
	})
}

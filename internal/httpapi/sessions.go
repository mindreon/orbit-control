package httpapi

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/sessions"
	"github.com/mindreon/orbit-control/internal/tenancy"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func (h *Handler) resolver() locks.Resolver {
	if h.Locks != nil {
		return h.Locks
	}
	return locks.NewResolver(h.Assets)
}
func (h *Handler) createSession(out http.ResponseWriter, r *http.Request) error {
	var in sessions.CreateInput
	if err := httpx.Decode(r, &in); err != nil {
		return err
	}
	result, err := h.sessionService().Create(r.Context(), in)
	if err != nil {
		return err
	}
	httpx.JSON(out, 201, result)
	return nil
}

func (h *Handler) getSession(out http.ResponseWriter, r *http.Request) error {
	s, err := sessions.Get(r.Context(), r.PathValue("s"), "session.read")
	if err != nil {
		return err
	}
	httpx.JSON(out, 200, s)
	return nil
}
func (h *Handler) listSessions(out http.ResponseWriter, r *http.Request) error {
	page, err := httpx.Pagination(r)
	if err != nil {
		return err
	}
	result, err := sessions.List(r.Context(), page, r.URL.Query().Get("workspace_id"), r.URL.Query().Get("status"))
	if err != nil {
		return err
	}
	httpx.JSON(out, 200, result)
	return nil
}

// twoPhase commits admission, objects and replay claims before contacting Temporal.
func (h *Handler) twoPhase(
	out http.ResponseWriter,
	r *http.Request,
	prepare func(context.Context, []byte) (func(context.Context, *httpx.Recorder) error, error),
) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(body) > 1<<20 {
		return httpx.E(413, "BODY_TOO_LARGE", "Request exceeds limit")
	}
	tenant, p := auth.Tenant(r.Context()), auth.From(r.Context())
	store := httpx.IdempotencyStore{Keys: h.Auth.Replay}
	var cached *httpx.Recorder
	var deliver func(context.Context, *httpx.Recorder) error
	err = h.DB.WithTenant(r.Context(), tenant, func(tx pgx.Tx) error {
		ctx := db.Context(r.Context(), tx)
		if err := tenancy.Active(ctx, tx, p.ID); err != nil {
			return err
		}
		var e error
		cached, e = store.Lookup(ctx, tx, tenant, p.ID, r, body)
		if e != nil || cached != nil {
			return e
		}
		deliver, e = prepare(ctx, body)
		return e
	})
	if err != nil {
		return err
	}
	if cached != nil {
		cached.FlushTo(out)
		return nil
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	rec := httpx.NewRecorder()
	if err = deliver(ctx, rec); err != nil {
		return err
	}
	err = h.DB.WithTenant(r.Context(), tenant, func(tx pgx.Tx) error {
		if err := tenancy.Active(r.Context(), tx, p.ID); err != nil {
			return err
		}
		return store.Save(r.Context(), tx, tenant, p.ID, r, rec)
	})
	if err != nil {
		return err
	}
	rec.FlushTo(out)
	return nil
}
func temporalError(err error, conflict string) error {
	if app, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		if app.Type() == "SESSION_ARCHIVED" || app.Type() == "QUEUE_FULL" || app.Type() == "DUPLICATE_RUN" {
			conflict = app.Type()
		}
		return httpx.E(409, conflict, "Workflow no longer accepts this operation")
	}
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return httpx.E(409, conflict, "Workflow is no longer active")
	}
	e := httpx.E(503, "TEMPORAL_UNAVAILABLE", "Workflow service unavailable")
	e.Retryable = true
	return e
}
func decodeBody(body []byte, out any) error {
	if err := json.Unmarshal(body, out, json.RejectUnknownMembers(true)); err != nil {
		return httpx.E(400, "INVALID_JSON", "Invalid JSON body")
	}
	return nil
}
func (h *Handler) submitMessage(out http.ResponseWriter, r *http.Request) error {
	return h.twoPhase(out, r, func(ctx context.Context, body []byte) (func(context.Context, *httpx.Recorder) error, error) {
		return h.prepareSubmission(ctx, r, body, true)
	})
}
func (h *Handler) prepareSubmission(ctx context.Context, r *http.Request, body []byte, allowSteer bool) (
	func(context.Context, *httpx.Recorder) error, error,
) {
	var input struct {
		Text        string         `json:"text"`
		Budget      *int64         `json:"budget_millicredits"`
		Attachments jsontext.Value `json:"attachments"`
	}
	if err := decodeBody(body, &input); err != nil {
		return nil, err
	}
	if len(input.Attachments) > 0 {
		return nil, httpx.E(422, "ATTACHMENTS_UNSUPPORTED", "Attachments are not supported")
	}
	if strings.TrimSpace(input.Text) == "" || utf8.RuneCountInString(input.Text) > 32768 ||
		(input.Budget != nil && (*input.Budget < 1 || *input.Budget > 9007199254740991)) {
		return nil, httpx.E(422, "INVALID_INPUT", "Invalid message or budget")
	}
	s, err := sessions.Get(ctx, r.PathValue("s"), "run.create")
	if err != nil {
		return nil, err
	}
	if s.Status == "archived" {
		return nil, httpx.E(409, "SESSION_ARCHIVED", "Session is archived")
	}
	tenant, principal := auth.Tenant(ctx), auth.From(ctx).ID
	runID := ids.New()
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		runID = ids.Derive(tenant, principal+"|"+s.ID+"|"+key)
	}
	messageID := ids.Derive(runID, "input")
	if allowSteer && s.MessagePolicy == "steer" && h.Temporal != nil {
		deliver, err := h.policySteer(ctx, r, body, s, messageID, input.Text)
		if err != nil || deliver != nil {
			return deliver, err
		}
	}
	var scheduled bool
	if err := db.Tx(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schedule_runs WHERE tenant_id=$1 AND session_id=$2 AND status='running')`,
		tenant, s.ID).Scan(&scheduled); err != nil {
		return nil, err
	}
	if scheduled {
		return nil, httpx.E(409, "SCHEDULED_RUN_ACTIVE", "Scheduled run has not finished archiving")
	}
	msg, err := h.sessionService().BuildStart(ctx, db.Tx(ctx), s, auth.From(ctx), runID, messageID, input.Text, input.Budget)
	if err != nil {
		return nil, err
	}
	return h.submitDelivery(s, tenant, msg), nil
}

type delivery func(context.Context, *httpx.Recorder) error

func (h *Handler) policySteer(
	ctx context.Context,
	r *http.Request,
	body []byte,
	s sessions.Session,
	messageID, text string,
) (delivery, error) {
	tenant := auth.Tenant(ctx)
	// Query does not mutate Temporal. A steer delivery remains outside this transaction.
	{
		state, e := h.Temporal.QuerySession(ctx, w.SessionWorkflowID(tenant, s.ID))
		if e == nil && state.ActiveRunID != "" {
			deliver, err := h.prepareSteer(ctx, s, state.ActiveRunID, messageID, text, true)
			if err != nil {
				if he, ok := errors.AsType[*httpx.Error](err); !ok || he.Code != "RUN_NOT_ACTIVE" {
					return nil, err
				}
			} else {
				return func(ctx context.Context, rec *httpx.Recorder) error {
					err := deliver(ctx, rec)
					if he, ok := errors.AsType[*httpx.Error](err); !ok || he.Code != "RUN_NOT_ACTIVE" {
						return err
					}
					var ordinary func(context.Context, *httpx.Recorder) error
					if err := h.DB.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
						var e error
						ordinary, e = h.prepareSubmission(db.Context(ctx, tx), r, body, false)
						return e
					}); err != nil {
						return err
					}
					return ordinary(ctx, rec)
				}, nil
			}
		}
	}
	return nil, nil
}

func (h *Handler) submitDelivery(s sessions.Session, tenant string, msg w.SubmitMessageInput) delivery {
	runID, messageID := msg.RunID, msg.MessageID
	return func(ctx context.Context, rec *httpx.Recorder) error {
		if h.Temporal == nil {
			return temporalError(errors.New("no client"), "")
		}
		result, e := h.Temporal.Submit(
			ctx,
			w.SessionInput{
				V:           1,
				TenantID:    tenant,
				WorkspaceID: s.WorkspaceID,
				SessionID:   s.ID,
				OwnerID:     s.OwnerID,
				Policy:      s.MessagePolicy,
			},
			msg,
		)
		if app, ok := errors.AsType[*temporal.ApplicationError](e); ok && app.Type() == "DUPLICATE_RUN" {
			e = h.DB.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
				var status string
				if err := tx.QueryRow(ctx, `SELECT id::text,status FROM runs WHERE tenant_id=$1 AND id=$2 AND session_id=$3`,
					tenant, runID, s.ID).Scan(&result.RunID, &status); err != nil {
					return err
				}
				result.Disposition = "started"
				if status == "queued" {
					result.Disposition = "queued"
				}
				return nil
			})
		}
		if e != nil {
			mapped := temporalError(e, "SESSION_UNAVAILABLE")
			if app, ok := errors.AsType[*temporal.ApplicationError](e); ok &&
				(app.Type() == "SESSION_ARCHIVED" || app.Type() == "QUEUE_FULL") {
				if err := h.DB.WithTenant(ctx, tenant, func(tx pgx.Tx) error { return sessions.RejectQueued(db.Context(ctx, tx), runID) }); err != nil {
					return err
				}
			}
			return mapped
		}
		token, e := h.issueStream(ctx, s, runID)
		if e != nil {
			return e
		}
		httpx.JSON(
			rec,
			202,
			map[string]any{
				"run_id":       result.RunID,
				"message_id":   messageID,
				"disposition":  result.Disposition,
				"position":     result.Position,
				"stream_token": token,
			},
		)
		return nil
	}
}
func (h *Handler) patchSession(out http.ResponseWriter, r *http.Request) error {
	return h.twoPhase(out, r, func(ctx context.Context, body []byte) (func(context.Context, *httpx.Recorder) error, error) {
		var input sessions.PatchInput
		if err := decodeBody(body, &input); err != nil {
			return nil, err
		}
		s, err := sessions.Patch(ctx, r.PathValue("s"), input)
		if err != nil {
			return nil, err
		}
		tenant := auth.Tenant(ctx)
		return func(ctx context.Context, rec *httpx.Recorder) error {
			if input.Policy != nil && h.Temporal != nil {
				if err := h.Temporal.Signal(ctx, w.SessionWorkflowID(tenant, s.ID), w.SetPolicy, w.SetPolicyInput{Policy: *input.Policy}); err != nil {
					if _, missing := errors.AsType[*serviceerror.NotFound](err); !missing {
						return temporalError(err, "")
					}
				}
			}
			httpx.JSON(rec, 200, s)
			return nil
		}, nil
	})
}
func (h *Handler) archiveSession(out http.ResponseWriter, r *http.Request) error {
	return h.twoPhase(out, r, func(ctx context.Context, body []byte) (func(context.Context, *httpx.Recorder) error, error) {
		s, err := sessions.Archive(ctx, r.PathValue("s"))
		if err != nil {
			return nil, err
		}
		tenant := auth.Tenant(ctx)
		return func(ctx context.Context, rec *httpx.Recorder) error {
			if h.Temporal != nil {
				if err := h.Temporal.Signal(ctx, w.SessionWorkflowID(tenant, s.ID), w.Archive, w.ArchiveInput{}); err != nil {
					if _, missing := errors.AsType[*serviceerror.NotFound](err); !missing {
						return temporalError(err, "")
					}
				} else {
					httpx.JSON(rec, 202, s)
					return nil
				}

				leaseID := w.LeaseWorkflowID(tenant, w.IsolationKey(tenant, s.ID))
				if err := h.Temporal.Signal(ctx, leaseID, w.Archive, w.ArchiveInput{}); err != nil {
					if _, missing := errors.AsType[*serviceerror.NotFound](err); !missing {
						return temporalError(err, "")
					}
				}
			}
			httpx.JSON(rec, 202, s)
			return nil
		}, nil
	})
}
func (h *Handler) sessionChildren(out http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	s, err := sessions.Get(ctx, r.PathValue("s"), "session.read")
	if err != nil {
		return err
	}
	page, err := httpx.Pagination(r)
	if err != nil {
		return err
	}
	kind := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
	var result any
	switch kind {
	case "messages":
		result, err = sessions.Messages(ctx, s.ID, page)
	case "runs":
		result, err = sessions.Runs(ctx, s.ID, page)
	case "artifacts":
		result, err = sessions.Artifacts(ctx, s.ID, page)
	}
	if err != nil {
		return err
	}
	httpx.JSON(out, 200, result)
	return nil
}
func (h *Handler) getArtifact(out http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if !ids.Valid(r.PathValue("a")) {
		return httpx.E(404, "NOT_FOUND", "Artifact not found")
	}
	ref, err := sessions.ArtifactRef(ctx, r.PathValue("a"))
	if err != nil {
		return err
	}
	url, err := h.Objects.PresignGet(ctx, auth.Tenant(ctx), ref, 5*time.Minute)
	if err != nil {
		return err
	}
	out.Header().Set("Location", url)
	out.WriteHeader(302)
	return nil
}

func (h *Handler) sessionService() sessions.Service {
	return sessions.Service{
		Objects:          h.Objects,
		Locks:            h.resolver(),
		DefaultRunBudget: h.Config.DefaultRunBudget,
	}
}

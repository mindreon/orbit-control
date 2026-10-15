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
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/sessions"
	"github.com/mindreon/orbit-control/internal/tenancy"
	"github.com/mindreon/orbit-control/internal/usage"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
)

func validMessagePolicy(s string) bool { return s == "queue" || s == "steer" || s == "interrupt" }
func (h *Handler) resolver() locks.Resolver {
	if h.Locks != nil {
		return h.Locks
	}
	return locks.NewResolver(h.Assets)
}
func (h *Handler) createSession(out http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	var in struct {
		WorkspaceID   string  `json:"workspace_id"`
		EntryRef      string  `json:"entry_ref"`
		Title         string  `json:"title"`
		MessagePolicy string  `json:"message_policy"`
		ProjectID     *string `json:"project_id"`
		Pinned        bool    `json:"pinned"`
	}
	if err := httpx.Decode(r, &in); err != nil {
		return err
	}
	if in.MessagePolicy == "" {
		in.MessagePolicy = "queue"
	}
	if !validMessagePolicy(in.MessagePolicy) || strings.TrimSpace(in.EntryRef) == "" || len(in.Title) > 256 {
		return httpx.E(422, "INVALID_INPUT", "Invalid session input")
	}
	if _, err := h.workspace(ctx, in.WorkspaceID, "session.create"); err != nil {
		return err
	}
	if _, err := h.resolver().Resolve(ctx, locks.Request{TenantID: auth.Tenant(ctx), LockID: ids.New(), Entry: locks.Entry{Kind: "agent", Ref: in.EntryRef}, Scope: locks.Scope{WorkspaceID: in.WorkspaceID}}); err != nil {
		return httpx.E(422, "LOCK_RESOLUTION_FAILED", "Agent cannot be resolved")
	}
	id := ids.New()
	tx := db.Tx(ctx)
	if _, err := tx.Exec(ctx, `INSERT INTO sessions(id,tenant_id,workspace_id,project_id,owner_id,title,entry_kind,entry_ref,message_policy,status,pinned) VALUES($1,$2,$3,$4,$5,$6,'agent',$7,$8,'idle',$9)`, id, auth.Tenant(ctx), in.WorkspaceID, in.ProjectID, auth.From(ctx).ID, in.Title, in.EntryRef, in.MessagePolicy, in.Pinned); err != nil {
		return err
	}
	if err := tenancy.Bind(ctx, tx, auth.From(ctx), auth.Tenant(ctx), auth.From(ctx).Kind, auth.From(ctx).ID, "session.owner", "session", id); err != nil {
		return err
	}
	if err := h.logWrite(ctx, "session.created", "session", id, map[string]string{"entry_ref": in.EntryRef}); err != nil {
		return err
	}
	s, err := sessions.Get(ctx, id, "session.read")
	if err != nil {
		return err
	}
	httpx.JSON(out, 201, s)
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
	ctx := r.Context()
	page, err := httpx.Pagination(r)
	if err != nil {
		return err
	}
	workspace := r.URL.Query().Get("workspace_id")
	if workspace != "" && !ids.Valid(workspace) {
		return httpx.E(422, "INVALID_INPUT", "Invalid workspace")
	}
	// Session roles may grant access without a workspace binding.
	rows, err := db.Tx(ctx).Query(ctx, `SELECT id::text FROM sessions WHERE ($1='' OR id>NULLIF($1,'')::uuid) AND ($2='' OR workspace_id=NULLIF($2,'')::uuid) AND ($3='' OR status=$3) ORDER BY id`, page.Cursor, workspace, r.URL.Query().Get("status"))
	if err != nil {
		return err
	}
	var candidates []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		candidates = append(candidates, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	items := []sessions.Session{}
	next := ""
	for _, id := range candidates {
		s, e := sessions.Get(ctx, id, "session.read")
		if e != nil {
			if he, ok := errors.AsType[*httpx.Error](e); ok && (he.Status == 403 || he.Status == 404) {
				continue
			}
			return e
		}
		items = append(items, s)
		if len(items) > page.Limit {
			items = items[:page.Limit]
			next = httpx.Cursor(items[len(items)-1].ID)
			break
		}
	}
	httpx.JSON(out, 200, map[string]any{"items": items, "next_cursor": next})
	return nil
}

// twoPhase commits admission, objects and replay claims before contacting Temporal.
func (h *Handler) twoPhase(out http.ResponseWriter, r *http.Request, prepare func(context.Context, []byte) (func(context.Context, *httpx.Recorder) error, error)) error {
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
	if _, ok := errors.AsType[*temporal.ApplicationError](err); ok {
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
		if strings.TrimSpace(input.Text) == "" || utf8.RuneCountInString(input.Text) > 32768 || (input.Budget != nil && (*input.Budget < 1 || *input.Budget > 9007199254740991)) {
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
		// Query does not mutate Temporal. A steer delivery remains outside this transaction.
		if s.MessagePolicy == "steer" && h.Temporal != nil {
			state, e := h.Temporal.QuerySession(ctx, w.SessionWorkflowID(tenant, s.ID))
			if e == nil && state.ActiveRunID != "" {
				return h.prepareSteer(ctx, s, state.ActiveRunID, messageID, input.Text, true)
			}
		}
		tx := db.Tx(ctx)
		if _, err = tx.Exec(ctx, `SELECT id FROM sessions WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenant, s.ID); err != nil {
			return nil, err
		}
		var lock contract.RunLock
		var storedBudget int64
		var raw []byte
		err = tx.QueryRow(ctx, `SELECT l.lock,r.budget_millicredits FROM runs r JOIN run_locks l ON l.id=r.lock_id WHERE r.tenant_id=$1 AND r.id=$2`, tenant, runID).Scan(&raw, &storedBudget)
		if err == nil {
			if err = json.Unmarshal(raw, &lock); err != nil {
				return nil, err
			}
		} else if errors.Is(err, pgx.ErrNoRows) {
			budget := h.Config.DefaultRunBudget
			if budget == 0 {
				budget = 200000
			}
			lockID := ids.Derive(runID, "lock")
			var pinned *string
			var pin bool
			if err = tx.QueryRow(ctx, `SELECT pinned_lock_id::text,pinned FROM sessions WHERE tenant_id=$1 AND id=$2`, tenant, s.ID).Scan(&pinned, &pin); err != nil {
				return nil, err
			}
			if pinned != nil {
				if err = tx.QueryRow(ctx, `SELECT lock FROM run_locks WHERE tenant_id=$1 AND id=$2`, tenant, *pinned).Scan(&raw); err != nil {
					return nil, err
				}
				if err = json.Unmarshal(raw, &lock); err != nil {
					return nil, err
				}
				lock.LockId = lockID
			} else {
				lock, err = h.resolver().Resolve(ctx, locks.Request{TenantID: tenant, LockID: lockID, Entry: locks.Entry{Kind: "agent", Ref: s.EntryRef}, Scope: locks.Scope{WorkspaceID: s.WorkspaceID}})
				if err != nil {
					return nil, httpx.E(422, "LOCK_RESOLUTION_FAILED", "Agent dependencies could not be resolved")
				}
			}
			// Read the pinned agent bytes rather than mutable asset metadata for defaults.
			for _, f := range lock.Entries[lock.Members[0].AgentEntry].Files {
				if f.Path == "agent.json" {
					data, e := h.Objects.Get(ctx, tenant, f.Sha256, 256<<10)
					if e != nil {
						return nil, e
					}
					var manifest contract.AgentManifest
					if e = json.Unmarshal(data, &manifest); e != nil {
						return nil, e
					}
					if manifest.Limits != nil && manifest.Limits.DefaultRunBudgetMillicredits != nil {
						budget = *manifest.Limits.DefaultRunBudgetMillicredits
					}
				}
			}
			if input.Budget != nil {
				budget = *input.Budget
			}
			if budget < 1 {
				return nil, httpx.E(422, "INVALID_INPUT", "Run budget must be positive")
			}
			if err = usage.Admit(ctx, tenant, s.WorkspaceID, principal, budget); err != nil {
				return nil, err
			}
			if _, err = tx.Exec(ctx, `INSERT INTO runs(id,tenant_id,session_id,trigger,status,created_by,message_id,budget_millicredits,temporal_workflow_id) VALUES($1,$2,$3,'user_message','queued',$4,$5,$6,$7)`, runID, tenant, s.ID, principal, messageID, budget, w.AgentRunWorkflowID(tenant, runID)); err != nil {
				return nil, err
			}
			if err = locks.Save(ctx, tx, runID, lock); err != nil {
				return nil, err
			}
			if _, err = tx.Exec(ctx, `UPDATE runs SET lock_id=$3 WHERE tenant_id=$1 AND id=$2`, tenant, runID, lock.LockId); err != nil {
				return nil, err
			}
			if pin && pinned == nil {
				if _, err = tx.Exec(ctx, `UPDATE sessions SET pinned_lock_id=$3 WHERE tenant_id=$1 AND id=$2`, tenant, s.ID, lock.LockId); err != nil {
					return nil, err
				}
			}
			content, e := json.Marshal(map[string]string{"text": input.Text})
			if e != nil {
				return nil, e
			}
			inputRef := "t/" + tenant + "/runs/" + runID + "/input/" + messageID + ".json"
			if e = h.Objects.PutObject(ctx, tenant, inputRef, content, "application/json"); e != nil {
				return nil, e
			}
			if _, e = tx.Exec(ctx, `INSERT INTO messages(id,tenant_id,session_id,run_id,role,author_id,content,content_ref) VALUES($1,$2,$3,$4,'user',$5,$6,$7)`, messageID, tenant, s.ID, runID, principal, content, inputRef); e != nil {
				return nil, e
			}
			if e = h.logWrite(ctx, "run.created", "run", runID, map[string]any{"budget_millicredits": budget}); e != nil {
				return nil, e
			}
			storedBudget = budget
		} else {
			return nil, err
		}
		inputRef := "t/" + tenant + "/runs/" + runID + "/input/" + messageID + ".json"
		permissions := lock.Members[0].EffectivePermissions
		msg := w.SubmitMessageInput{V: 1, RunID: runID, MessageID: messageID, PrincipalID: principal, Trigger: "user_message", Lock: contract.SegmentLockRef{Id: lock.LockId, Digest: lock.Digest}, InputRef: inputRef, BudgetMillicredits: storedBudget, Limits: w.DefaultLimits(storedBudget), Sandbox: w.SandboxSpec{IsolationKey: w.IsolationKey(tenant, s.ID), Profile: permissions.SandboxProfile, Egress: permissions.Network.Egress, TTLSeconds: 1800}}
		return func(ctx context.Context, rec *httpx.Recorder) error {
			if h.Temporal == nil {
				return temporalError(errors.New("no client"), "")
			}
			result, e := h.Temporal.Submit(ctx, w.SessionInput{V: 1, TenantID: tenant, WorkspaceID: s.WorkspaceID, SessionID: s.ID, OwnerID: s.OwnerID, Policy: s.MessagePolicy}, msg)
			if e != nil {
				return temporalError(e, "SESSION_UNAVAILABLE")
			}
			token, e := h.Auth.Signer.IssueStream(tenant, runID, s.ID)
			if e != nil {
				return e
			}
			httpx.JSON(rec, 202, map[string]any{"run_id": result.RunID, "message_id": messageID, "disposition": result.Disposition, "position": result.Position, "stream_token": token})
			return nil
		}, nil
	})
}
func (h *Handler) patchSession(out http.ResponseWriter, r *http.Request) error {
	return h.twoPhase(out, r, func(ctx context.Context, body []byte) (func(context.Context, *httpx.Recorder) error, error) {
		s, err := sessions.Get(ctx, r.PathValue("s"), "session.update")
		if err != nil {
			return nil, err
		}
		var input struct {
			Title  *string `json:"title"`
			Policy *string `json:"message_policy"`
		}
		if err = decodeBody(body, &input); err != nil {
			return nil, err
		}
		if input.Title == nil && input.Policy == nil {
			return nil, httpx.E(422, "INVALID_INPUT", "At least one field required")
		}
		if input.Title != nil && (len(*input.Title) > 256 || strings.TrimSpace(*input.Title) == "") {
			return nil, httpx.E(422, "INVALID_INPUT", "Invalid title")
		}
		if input.Policy != nil && !validMessagePolicy(*input.Policy) {
			return nil, httpx.E(422, "INVALID_INPUT", "Invalid message policy")
		}
		if _, err = db.Tx(ctx).Exec(ctx, `UPDATE sessions SET title=COALESCE($3,title),message_policy=COALESCE($4,message_policy) WHERE tenant_id=$1 AND id=$2`, auth.Tenant(ctx), s.ID, input.Title, input.Policy); err != nil {
			return nil, err
		}
		if err = h.logWrite(ctx, "session.updated", "session", s.ID, input); err != nil {
			return nil, err
		}
		s, err = sessions.Get(ctx, s.ID, "session.read")
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
		s, err := sessions.Get(ctx, r.PathValue("s"), "session.update")
		if err != nil {
			return nil, err
		}
		if _, err = db.Tx(ctx).Exec(ctx, `UPDATE sessions SET status='archived' WHERE tenant_id=$1 AND id=$2`, auth.Tenant(ctx), s.ID); err != nil {
			return nil, err
		}
		if err = h.logWrite(ctx, "session.archived", "session", s.ID, map[string]any{}); err != nil {
			return nil, err
		}
		s.Status = "archived"
		tenant := auth.Tenant(ctx)
		return func(ctx context.Context, rec *httpx.Recorder) error {
			if h.Temporal != nil {
				if err := h.Temporal.Signal(ctx, w.SessionWorkflowID(tenant, s.ID), w.Archive, w.ArchiveInput{}); err != nil {
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
	var items []map[string]any
	switch kind {
	case "messages":
		items, err = sessions.Rows(ctx, `SELECT jsonb_build_object('id',id,'session_id',session_id,'run_id',run_id,'role',role,'text',content->>'text','truncated',content->'truncated','content_ref',content_ref,'created_at',created_at) FROM messages WHERE session_id=$1 AND ($2='' OR (created_at,id)>(SELECT created_at,id FROM messages WHERE id=NULLIF($2,'')::uuid)) ORDER BY created_at,id LIMIT $3`, s.ID, page.Cursor, page.Limit+1)
	case "runs":
		items, err = sessions.Rows(ctx, `SELECT to_jsonb(r)-'tenant_id'-'temporal_workflow_id'||jsonb_build_object('segments_count',(SELECT count(*) FROM run_segments g WHERE g.run_id=r.id),'pending_interaction_id',(SELECT id FROM interactions i WHERE i.run_id=r.id AND i.status IN ('pending','escalated') ORDER BY created_at LIMIT 1)) FROM runs r WHERE session_id=$1 AND ($2='' OR id>NULLIF($2,'')::uuid) ORDER BY id LIMIT $3`, s.ID, page.Cursor, page.Limit+1)
	case "artifacts":
		items, err = sessions.Rows(ctx, `SELECT to_jsonb(a)-'tenant_id'-'object_ref'-'size'||jsonb_build_object('size_bytes',size) FROM artifacts a WHERE session_id=$1 AND ($2='' OR id>NULLIF($2,'')::uuid) ORDER BY id LIMIT $3`, s.ID, page.Cursor, page.Limit+1)
	}
	if err != nil {
		return err
	}
	next := ""
	if len(items) > page.Limit {
		items = items[:page.Limit]
		next = httpx.Cursor(items[len(items)-1]["id"].(string))
	}
	httpx.JSON(out, 200, map[string]any{"items": items, "next_cursor": next})
	return nil
}
func (h *Handler) getArtifact(out http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	if !ids.Valid(r.PathValue("a")) {
		return httpx.E(404, "NOT_FOUND", "Artifact not found")
	}
	var session, ref string
	if err := db.Tx(ctx).QueryRow(ctx, `SELECT session_id::text,object_ref FROM artifacts WHERE id=$1`, r.PathValue("a")).Scan(&session, &ref); err != nil {
		return err
	}
	if _, err := sessions.Get(ctx, session, "session.read"); err != nil {
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

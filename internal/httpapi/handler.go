package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/internalauth"
	"github.com/mindreon/orbit-control/internal/orch"
	"github.com/mindreon/orbit-control/internal/skillhub"
	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/worker"
)

// ErrorBody is the JSON error shape. Keep 401/403 examples in docs/openapi.yaml
// stable for Sentinel.
type ErrorBody struct {
	Error    string        `json:"error"`
	Code     string        `json:"code"`
	Message  string        `json:"message"`
	Approval *app.Approval `json:"approval,omitempty"`
}

type HealthBody struct {
	Status string `json:"status"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, ErrorBody{Error: strings.ToLower(strings.ReplaceAll(code, "_", " ")), Code: code, Message: message})
}

type deliveryAcceptedBody struct {
	Approval      *app.Approval `json:"approval"`
	DeliveryState string        `json:"deliveryState"`
}

func writeDecideErr(lg *log.Logger, w http.ResponseWriter, err error) {
	var unknown *app.DeliveryUnknownError
	if errors.As(err, &unknown) && unknown.Approval != nil {
		writeJSON(w, http.StatusAccepted, deliveryAcceptedBody{
			Approval: unknown.Approval, DeliveryState: unknown.Approval.DeliveryState,
		})
		return
	}
	var statusErr *app.StatusError
	if errors.As(err, &statusErr) {
		writeJSON(w, statusErr.Status, ErrorBody{
			Error:    strings.ToLower(strings.ReplaceAll(statusErr.Code, "_", " ")),
			Code:     statusErr.Code,
			Message:  statusErr.Message,
			Approval: statusErr.Approval,
		})
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	writeAppErr(lg, w, err, approvalNotFound, http.StatusBadRequest, "BAD_REQUEST")
}

func writeBlobErr(lg *log.Logger, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, app.ErrTooLarge):
		writeErr(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "artifact body exceeds the configured limit")
	case errors.Is(err, app.ErrDigestMismatch):
		writeErr(w, http.StatusUnprocessableEntity, "DIGEST_MISMATCH", "X-Content-Digest does not match the body")
	default:
		writeAppErr(lg, w, err, roomNotFound, http.StatusBadRequest, "BAD_REQUEST")
	}
}

func envSeconds(name string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		log.Printf("ignoring %s=%q: want a positive integer number of seconds", name, raw)
		return def
	}
	return time.Duration(n) * time.Second
}

func artifactDirFromEnv() string {
	if dir := strings.TrimSpace(os.Getenv("ORBIT_ARTIFACT_DIR")); dir != "" {
		return dir
	}
	if data := strings.TrimSpace(os.Getenv("ORBIT_DATA_DIR")); data != "" {
		return filepath.Join(data, "artifacts")
	}
	return ""
}

func artifactMaxFromEnv() int64 {
	raw := strings.TrimSpace(os.Getenv("ORBIT_ARTIFACT_MAX_BYTES"))
	if raw == "" {
		return 0
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		log.Printf("ignoring ORBIT_ARTIFACT_MAX_BYTES=%q: want a positive integer", raw)
		return 0
	}
	return n
}

// writeAppErr maps repository sentinels first; anything else keeps the
// route's legacy status. Storage error text stays in server logs.
func writeAppErr(lg *log.Logger, w http.ResponseWriter, err error, notFound string, fallback int, fallbackCode string) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, "NOT_FOUND", notFound)
	case errors.Is(err, app.ErrDecisionDeliveryFailed):
		writeErr(w, http.StatusBadGateway, "DECISION_DELIVERY_FAILED", "the decision is recorded but its delivery to the workflow failed or timed out; it is not retried")
	case errors.Is(err, store.ErrApprovalNotPending):
		writeErr(w, http.StatusConflict, "APPROVAL_NOT_PENDING", "approval is not pending")
	case errors.Is(err, store.ErrIdempotencyKeyReused):
		writeErr(w, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED", "Idempotency-Key was already used with a different request body")
	case errors.Is(err, store.ErrStorage):
		lg.Printf("storage error: %v", err)
		writeErr(w, http.StatusInternalServerError, "INTERNAL", "internal error")
	case errors.Is(err, app.ErrInvalid):
		lg.Printf("bad request: %v", err)
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "the request is invalid")
	default:
		if fallback == http.StatusBadRequest {
			lg.Printf("bad request: %v", err)
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "the request is invalid")
			return
		}
		writeErr(w, fallback, fallbackCode, err.Error())
	}
}

const (
	roomNotFound     = "room not found"
	approvalNotFound = "approval not found"
)

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "content-type, authorization, idempotency-key, x-orbit-request")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
		w.Header().Set("Access-Control-Expose-Headers", "Idempotent-Replayed")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Handlers builds the process's public and internal muxes over one App,
// configured from the environment.
// Worker URL: ORBIT_WORKER_URL (default http://127.0.0.1:8090).
// When TEMPORAL_ADDRESS is set, rooms are driven through RoomWorkflow.
// Storage follows §18.2 (ORBIT_CONTROL_DB_URL); the returned func closes it.
// The internal handler must only be bound to ORBIT_INTERNAL_ADDR.
func Handlers() (public, internal http.Handler, closeStore func(), err error) {
	base := os.Getenv("ORBIT_WORKER_URL")
	if base == "" {
		base = "http://127.0.0.1:8090"
	}
	repo, err := openRepository(context.Background())
	if err != nil {
		return nil, nil, nil, err
	}
	defaultTenant := strings.TrimSpace(os.Getenv("ORBIT_DEFAULT_TENANT"))
	if defaultTenant == "" {
		defaultTenant = app.DefaultTenantID
	}
	if err := repo.CheckTenant(context.Background(), defaultTenant); err != nil {
		repo.Close()
		if errors.Is(err, store.ErrNotFound) {
			return nil, nil, nil, fmt.Errorf("default tenant %q does not exist; create it with the ops role (deploy/postgres/ensure-tenant.sql)", defaultTenant)
		}
		return nil, nil, nil, err
	}
	opts := app.Options{
		Worker: worker.New(base), Repo: repo, DefaultTenant: defaultTenant,
		ArtifactDir: artifactDirFromEnv(), ArtifactMaxBytes: artifactMaxFromEnv(),
		DeliveryTimeout:   envSeconds("ORBIT_DECISION_DELIVERY_TIMEOUT", 30*time.Second),
		ReconcileInterval: envSeconds("ORBIT_DELIVERY_RECONCILE_INTERVAL_S", 10*time.Second),
		UnknownTimeout:    envSeconds("ORBIT_DELIVERY_UNKNOWN_TIMEOUT_S", 600*time.Second),
	}
	if addr := os.Getenv("TEMPORAL_ADDRESS"); addr != "" {
		oc, err := dialOrch(addr, os.Getenv("TEMPORAL_NAMESPACE"), os.Getenv("TEMPORAL_TASK_QUEUE"))
		if err != nil {
			repo.Close()
			return nil, nil, nil, errors.New("temporal dial: " + err.Error())
		}
		opts.Orch = oc
	}
	runtime := app.NewWithOptions(opts)
	runtime.Limits = app.LimitsFromEnv()
	// The catalog is copied in the background. GET /v1/skills only reads the
	// store. ORBIT_SKILLHUB_SYNC=0 turns the copy off.
	if os.Getenv("ORBIT_SKILLHUB_SYNC") != "0" {
		go skillhub.Run(context.Background(), repo, defaultTenant, skillhub.NewClient(""), runtime.Log)
	}
	public = HandlerWithOptions(runtime, Options{
		Auth:           authenticatorFromEnv(defaultTenant),
		AllowedOrigins: allowedOriginsFromEnv(),
	})
	return public, InternalHandler(runtime), repo.Close, nil
}

func dialOrch(addr, namespace, taskQueue string) (*orch.Client, error) {
	var last error
	for attempt := 1; attempt <= 30; attempt++ {
		oc, err := orch.Dial(addr, namespace, taskQueue)
		if err == nil {
			return oc, nil
		}
		last = err
		log.Printf("temporal not ready (%d/30): %v", attempt, err)
		time.Sleep(time.Second)
	}
	return nil, last
}

type Options struct {
	Auth Authenticator
	// AllowedOrigins is the CSRF Origin allowlist (§17.4).
	AllowedOrigins []string
}

// HandlerWith serves runtime with the local dev principal.
func HandlerWith(runtime *app.App) http.Handler {
	return HandlerWithOptions(runtime, Options{
		Auth:           LocalAuthenticator(runtime.DefaultTenant),
		AllowedOrigins: allowedOriginsFromEnv(),
	})
}

type principalHandler func(http.ResponseWriter, *http.Request, app.Principal)

func HandlerWithOptions(runtime *app.App, opts Options) http.Handler {
	if opts.Auth == nil {
		opts.Auth = denyAllAuthenticator
	}
	// authed runs before any repository call, so unauthenticated requests get
	// the same 401 on every path (§17.6).
	authed := func(next principalHandler) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			p, ok := opts.Auth.Authenticate(r)
			if !ok {
				writeUnauthenticated(w)
				return
			}
			next(w, r, p)
		}
	}
	// csrf is applied after authed: 401 → 403 → 404 (§17.4).
	csrf := func(next principalHandler) principalHandler {
		return func(w http.ResponseWriter, r *http.Request, p app.Principal) {
			if !csrfOK(r, opts.AllowedOrigins) {
				writeCSRFRejected(w)
				return
			}
			next(w, r, p)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, HealthBody{Status: "ok"})
	})
	mux.HandleFunc("GET /v1/rooms", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		rooms, err := runtime.ListRooms(r.Context(), p)
		if err != nil {
			writeAppErr(runtime.Log, w, err, roomNotFound, http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": rooms})
	}))
	mux.HandleFunc("POST /v1/rooms", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		// Unknown fields (tenantId, createdBy, deleted_by, …) are ignored.
		var body struct {
			Kind             string `json:"kind"`
			Title            string `json:"title"`
			PermissionPreset string `json:"permissionPreset"`
			PersonaID        string `json:"personaId"`
			GrantID          string `json:"grantId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
			return
		}
		room, replayed, err := runtime.CreateRoom(r.Context(), p, app.CreateRoomInput{
			Kind:             body.Kind,
			Title:            body.Title,
			PermissionPreset: body.PermissionPreset,
			PersonaID:        body.PersonaID,
			GrantID:          body.GrantID,
			IdempotencyKey:   r.Header.Get("Idempotency-Key"),
		})
		if err != nil {
			if room == nil {
				writeAppErr(runtime.Log, w, err, roomNotFound, http.StatusBadRequest, "BAD_REQUEST")
				return
			}
			writeAppErr(runtime.Log, w, err, roomNotFound, http.StatusBadGateway, "WORKER_ERROR")
			return
		}
		if replayed {
			w.Header().Set("Idempotent-Replayed", "true")
		}
		writeJSON(w, http.StatusOK, room)
	}))
	mux.HandleFunc("GET /v1/rooms/{roomId}", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		room, err := runtime.GetRoom(r.Context(), p, r.PathValue("roomId"))
		if err != nil {
			writeAppErr(runtime.Log, w, err, roomNotFound, http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, room)
	}))
	// DELETE is a soft delete (§18.7a). The request body is never read, so
	// deleted_at / deleted_by can only come from the server.
	mux.HandleFunc("DELETE /v1/rooms/{roomId}", authed(csrf(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		if err := runtime.DeleteRoom(r.Context(), p, r.PathValue("roomId")); err != nil {
			writeAppErr(runtime.Log, w, err, roomNotFound, http.StatusInternalServerError, "INTERNAL")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})))
	mux.HandleFunc("GET /v1/rooms/{roomId}/messages", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.ListMessages(r.Context(), p, r.PathValue("roomId"))
		if err != nil {
			writeAppErr(runtime.Log, w, err, roomNotFound, http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	mux.HandleFunc("POST /v1/rooms/{roomId}/messages", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body struct {
			Message string `json:"message"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		room, approval, err := runtime.PostMessage(r.Context(), p, r.PathValue("roomId"), body.Message)
		if err != nil {
			writeAppErr(runtime.Log, w, err, roomNotFound, http.StatusBadRequest, "BAD_REQUEST")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"room": room, "approval": approval})
	}))
	mux.HandleFunc("POST /v1/rooms/{roomId}/abort", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		if err := runtime.AbortRoom(r.Context(), p, r.PathValue("roomId")); err != nil {
			writeAppErr(runtime.Log, w, err, roomNotFound, http.StatusBadRequest, "BAD_REQUEST")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"aborted": true})
	}))
	mux.HandleFunc("POST /v1/rooms/{roomId}/steer", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body struct {
			Instruction string `json:"instruction"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
			return
		}
		accepted, err := runtime.SteerRoom(r.Context(), p, r.PathValue("roomId"), body.Instruction)
		if err != nil {
			writeAppErr(runtime.Log, w, err, roomNotFound, http.StatusBadRequest, "BAD_REQUEST")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"accepted": accepted})
	}))
	mux.HandleFunc("GET /v1/rooms/{roomId}/activity", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.ListActivity(r.Context(), p, r.PathValue("roomId"))
		if err != nil {
			writeAppErr(runtime.Log, w, err, roomNotFound, http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	mux.HandleFunc("GET /v1/rooms/{roomId}/events", authed(streamRoomEvents(runtime)))
	mux.HandleFunc("GET /v1/approvals", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.ListApprovals(r.Context(), p)
		if err != nil {
			writeAppErr(runtime.Log, w, err, approvalNotFound, http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	mux.HandleFunc("POST /v1/approvals/{approvalId}/decide", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body struct {
			Decision string `json:"decision"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		appr, err := runtime.Decide(r.Context(), p, r.PathValue("approvalId"), body.Decision)
		if err != nil {
			writeDecideErr(runtime.Log, w, err)
			return
		}
		writeJSON(w, http.StatusOK, appr)
	}))
	// Internal routes live only on InternalHandler's listener.
	mux.HandleFunc("/internal/", func(w http.ResponseWriter, _ *http.Request) {
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "not found")
	})
	mux.HandleFunc("GET /v1/personas", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.ListPersonas(r.Context(), p.TenantID)
		if err != nil {
			writeAppErr(runtime.Log, w, err, "persona not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	mux.HandleFunc("POST /v1/personas", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body struct {
			Name         string   `json:"name"`
			Instructions string   `json:"instructions"`
			McpIds       []string `json:"mcpConnectorIds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
			return
		}
		persona, err := runtime.CreatePersona(r.Context(), p.TenantID, body.Name, body.Instructions, body.McpIds)
		if err != nil {
			writeAppErr(runtime.Log, w, err, "persona not found", http.StatusBadRequest, "BAD_REQUEST")
			return
		}
		writeJSON(w, http.StatusOK, persona)
	}))
	mux.HandleFunc("GET /v1/mcp-connectors", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.ListMcpConnectors(r.Context(), p.TenantID)
		if err != nil {
			writeAppErr(runtime.Log, w, err, "connector not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	mux.HandleFunc("GET /v1/skills", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		q := r.URL.Query()
		page, _ := strconv.Atoi(q.Get("page"))
		pageSize, _ := strconv.Atoi(q.Get("pageSize"))
		list, err := runtime.ListSkills(r.Context(), p.TenantID, store.SkillCatalogQuery{
			Sort:           q.Get("sortBy"),
			Category:       q.Get("category"),
			Source:         q.Get("source"),
			Keyword:        q.Get("keyword"),
			RequiresAPIKey: q.Get("requiresApiKey"),
			Paid:           q.Get("paid"),
			Page:           page,
			PageSize:       pageSize,
		})
		if err != nil {
			writeAppErr(runtime.Log, w, err, "skill not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, list)
	}))
	mux.HandleFunc("GET /v1/skill-categories", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.ListSkillCategories(r.Context(), p.TenantID)
		if err != nil {
			writeAppErr(runtime.Log, w, err, "skill not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	mux.HandleFunc("POST /v1/mcp-connectors", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body struct {
			Name    string   `json:"name"`
			Command string   `json:"command"`
			Args    []string `json:"args"`
			EnvRefs []string `json:"envRefs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
			return
		}
		connector, err := runtime.CreateMcpConnector(r.Context(), p.TenantID, body.Name, body.Command, body.Args, body.EnvRefs)
		if err != nil {
			writeAppErr(runtime.Log, w, err, "connector not found", http.StatusBadRequest, "BAD_REQUEST")
			return
		}
		writeJSON(w, http.StatusOK, connector)
	}))
	mux.HandleFunc("POST /v1/grants", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Env        map[string]string `json:"env"`
			TTLSeconds int               `json:"ttlSeconds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
			return
		}
		grant, err := runtime.MintGrant(body.Env, body.TTLSeconds)
		if err != nil {
			runtime.Log.Printf("bad request: %v", err)
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "the request is invalid")
			return
		}
		// Public response never echoes secret values — only names + grant id.
		writeJSON(w, http.StatusOK, map[string]any{
			"id":        grant.ID,
			"envNames":  grant.EnvNames,
			"expiresAt": grant.ExpiresAt,
		})
	})
	mux.HandleFunc("GET /v1/secrets", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"items": []any{}})
	})
	mux.HandleFunc("GET /v1/cloud-agents", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"items": runtime.ListCloudAgents()})
	})
	mux.HandleFunc("POST /v1/cloud-agents", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			RepoURL          string `json:"repoUrl"`
			Prompt           string `json:"prompt"`
			Branch           string `json:"branch"`
			PermissionPreset string `json:"permissionPreset"`
			PersonaID        string `json:"personaId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
			return
		}
		job, err := runtime.CreateCloudAgent(r.Context(), app.CreateCloudAgentInput{
			RepoURL:          body.RepoURL,
			Prompt:           body.Prompt,
			Branch:           body.Branch,
			PermissionPreset: body.PermissionPreset,
			PersonaID:        body.PersonaID,
		})
		if err != nil {
			runtime.Log.Printf("bad request: %v", err)
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "the request is invalid")
			return
		}
		writeJSON(w, http.StatusOK, job)
	})
	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, _ *http.Request) {
		writeErr(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "use GET /v1/rooms/{roomId}/events (SSE) in W1")
	})
	return cors(mux)
}

// InternalHandler serves worker → control routes. It must be bound to a
// listener that is not published (ORBIT_INTERNAL_ADDR), never the public one.
func InternalHandler(runtime *app.App) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, HealthBody{Status: "ok"})
	})
	registerE2ERoutes(mux, runtime)
	mux.HandleFunc("POST /internal/events", func(w http.ResponseWriter, r *http.Request) {
		if !internalauth.Authorized(r) {
			writeErr(w, http.StatusUnauthorized, "UNAUTHORIZED", "internal token required")
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, runtime.Limits.IngestMaxBytes))
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeErr(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE",
				"event body exceeds "+strconv.FormatInt(tooLarge.Limit, 10)+" bytes")
			return
		}
		if err != nil {
			runtime.Log.Printf("bad request: %v", err)
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "the request is invalid")
			return
		}
		if err := runtime.Ingest(r.Context(), raw); err != nil {
			writeAppErr(runtime.Log, w, err, roomNotFound, http.StatusBadRequest, "BAD_REQUEST")
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"ok": true})
	})
	mux.HandleFunc("POST /internal/artifact-blobs", func(w http.ResponseWriter, r *http.Request) {
		if !internalauth.Authorized(r) {
			writeErr(w, http.StatusUnauthorized, "UNAUTHORIZED", "internal token required")
			return
		}
		taskID := r.URL.Query().Get("taskId")
		if taskID == "" {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "taskId is required")
			return
		}
		ref, err := runtime.SaveArtifactBlob(taskID, r.Header.Get("X-Content-Digest"), r.Body)
		if err != nil {
			writeBlobErr(runtime.Log, w, err)
			return
		}
		writeJSON(w, http.StatusOK, struct {
			StorageRef string `json:"storageRef"`
		}{StorageRef: ref})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "not found")
	})
	return mux
}

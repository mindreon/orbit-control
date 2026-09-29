package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/artifacts"
	"github.com/mindreon/orbit-control/internal/internalauth"
	"github.com/mindreon/orbit-control/internal/mcpmarket"
	"github.com/mindreon/orbit-control/internal/orch"
	"github.com/mindreon/orbit-control/internal/skillhub"
	"github.com/mindreon/orbit-control/internal/store"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
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

type localArtifactSigner struct{}

func (localArtifactSigner) PresignArtifact(_ context.Context, _ taskruntime.Principal, manifest taskruntime.ArtifactManifest, entry map[string]any) (string, error) {
	ref, _ := entry["blob_ref"].(string)
	if !strings.HasPrefix(ref, "sha256:") || len(ref) != len("sha256:")+64 {
		return "", errors.New("artifact entry has an invalid blob reference")
	}
	return "/v1/artifacts/" + url.PathEscape(manifest.ManifestID) + "/download?name=" + url.QueryEscape(fmt.Sprint(entry["name"])), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
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
	if endpoint := strings.TrimSpace(firstNonEmpty(os.Getenv("ORBIT_OBJECT_STORE_PUBLIC_ENDPOINT"), os.Getenv("ORBIT_OBJECT_STORE_ENDPOINT"))); endpoint != "" {
		signer, err := artifacts.New(endpoint, os.Getenv("ORBIT_OBJECT_STORE_ACCESS_KEY"), os.Getenv("ORBIT_OBJECT_STORE_SECRET_KEY"), os.Getenv("ORBIT_OBJECT_STORE_BUCKET"), os.Getenv("ORBIT_OBJECT_STORE_SECURE") == "1")
		if err != nil {
			repo.Close()
			return nil, nil, nil, err
		}
		opts.ArtifactSigner = signer
	} else if opts.ArtifactDir != "" {
		opts.ArtifactSigner = localArtifactSigner{}
	}
	if addr := os.Getenv("TEMPORAL_ADDRESS"); addr != "" {
		oc, err := dialOrch(addr, os.Getenv("TEMPORAL_NAMESPACE"), os.Getenv("TEMPORAL_TASK_QUEUE"))
		if err != nil {
			repo.Close()
			return nil, nil, nil, errors.New("temporal dial: " + err.Error())
		}
		opts.Orch = oc
		opts.TaskClient = oc
	}
	runtime := app.NewWithOptions(opts)
	runtime.Limits = app.LimitsFromEnv()
	projectorCtx, projectorCancel := context.WithCancel(context.Background())
	if outbox, ok := repo.(taskruntime.OutboxStore); ok {
		go func() {
			projector := &taskruntime.Projector{Store: outbox, Tasks: runtime.Tasks, OnDrop: func(row taskruntime.OutboxRecord, err error) {
				runtime.Log.Printf("runtime projector: dropped outbox row %d (%s): %v", row.ID, row.EventID, err)
			}}
			if err := projector.Run(projectorCtx); err != nil && projectorCtx.Err() == nil {
				runtime.Log.Printf("runtime projector stopped: %v", err)
			}
		}()
	}
	// The plaza snapshot ships with this binary. GET /v1/mcp-market only reads
	// the store. It does not call modelscope.cn.
	if err := mcpmarket.Install(context.Background(), repo, defaultTenant); err != nil {
		runtime.Log.Printf("mcp market: snapshot not stored: %v", err)
	}
	// The catalog is copied in the background. GET /v1/skills only reads the
	// store. ORBIT_SKILLHUB_SYNC=0 turns the copy off.
	if os.Getenv("ORBIT_SKILLHUB_SYNC") != "0" {
		go skillhub.Run(context.Background(), repo, defaultTenant, skillhub.NewClient(""), runtime.Log)
	}
	public = HandlerWithOptions(runtime, Options{
		Auth:           authenticatorFromEnv(defaultTenant),
		AllowedOrigins: allowedOriginsFromEnv(),
		TaskMembers:    splitMembers(os.Getenv("ORBIT_CONTROL_MEMBERS")),
		TaskMemberID:   strings.TrimSpace(os.Getenv("ORBIT_CONTROL_MEMBER_ID")),
	})
	internalHandler := InternalHandlerWithForwarder(runtime, newEphemeralForwarder(Options{
		TaskMembers:        splitMembers(os.Getenv("ORBIT_CONTROL_MEMBERS")),
		TaskMemberID:       strings.TrimSpace(os.Getenv("ORBIT_CONTROL_MEMBER_ID")),
		TaskMemberResolver: nil,
	}))
	return public, internalHandler, func() { projectorCancel(); repo.Close() }, nil
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
	AllowedOrigins     []string
	TaskMembers        []string
	TaskMemberID       string
	TaskMemberResolver func() []string
	TaskMemberRefresh  time.Duration
}

// HandlerWith serves runtime with the local dev principal.
func HandlerWith(runtime *app.App) http.Handler {
	return HandlerWithOptions(runtime, Options{
		Auth:           LocalAuthenticator(runtime.DefaultTenant),
		AllowedOrigins: allowedOriginsFromEnv(),
		TaskMembers:    splitMembers(os.Getenv("ORBIT_CONTROL_MEMBERS")),
		TaskMemberID:   strings.TrimSpace(os.Getenv("ORBIT_CONTROL_MEMBER_ID")),
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
	registerTaskRoutes(mux, runtime, authed)
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
	mux.HandleFunc("GET /v1/mcp-market", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		q := r.URL.Query()
		page, _ := strconv.Atoi(q.Get("page"))
		pageSize, _ := strconv.Atoi(q.Get("pageSize"))
		list, err := runtime.ListMcpMarket(r.Context(), p.TenantID, store.McpMarketQuery{
			Keyword:     q.Get("keyword"),
			Category:    q.Get("category"),
			ServiceType: q.Get("serviceType"),
			NeedsOnline: q.Get("needsOnline"),
			Page:        page,
			PageSize:    pageSize,
		})
		if err != nil {
			writeAppErr(runtime.Log, w, err, "market not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, list)
	}))
	mux.HandleFunc("GET /v1/mcp-market/{id}", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		detail, err := runtime.GetMcpMarket(r.Context(), p.TenantID, r.PathValue("id"))
		if err != nil {
			writeAppErr(runtime.Log, w, err, "market server not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, detail)
	}))
	mux.HandleFunc("GET /v1/mcp-market-categories", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.ListMcpMarketCategories(r.Context(), p.TenantID, r.URL.Query().Get("needsOnline"))
		if err != nil {
			writeAppErr(runtime.Log, w, err, "market not found", http.StatusInternalServerError, "INTERNAL")
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
	writeSkill := func(w http.ResponseWriter, r *http.Request, p app.Principal, handle, slug string) {
		skill, err := runtime.GetSkill(r.Context(), p.TenantID, handle, slug)
		if err != nil {
			writeAppErr(runtime.Log, w, err, "skill not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, skill)
	}
	writeSkillFiles := func(w http.ResponseWriter, r *http.Request, p app.Principal, handle, slug string) {
		client := skillhub.NewClient("")
		files, meta, err := runtime.SkillPage(r.Context(), p.TenantID, handle, slug, client.TextFiles, client.PageCopy)
		if err != nil {
			writeAppErr(runtime.Log, w, err, "skill not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		body := map[string]any{"items": files}
		if len(meta) > 0 {
			body["meta"] = json.RawMessage(meta)
		}
		writeJSON(w, http.StatusOK, body)
	}
	mux.HandleFunc("GET /v1/skills/{handle}/{slug}", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		writeSkill(w, r, p, r.PathValue("handle"), r.PathValue("slug"))
	}))
	mux.HandleFunc("GET /v1/skills/{slug}", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		writeSkill(w, r, p, "", r.PathValue("slug"))
	}))
	mux.HandleFunc("GET /v1/skill-files/{handle}/{slug}", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		writeSkillFiles(w, r, p, r.PathValue("handle"), r.PathValue("slug"))
	}))
	mux.HandleFunc("GET /v1/skill-files/{slug}", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		writeSkillFiles(w, r, p, "", r.PathValue("slug"))
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
			Name        string          `json:"name"`
			Transport   string          `json:"transport"`
			Command     string          `json:"command"`
			Args        []string        `json:"args"`
			EnvRefs     []string        `json:"envRefs"`
			URL         string          `json:"url"`
			HeaderRefs  []app.HeaderRef `json:"headerRefs"`
			DefaultOpen bool            `json:"defaultOpen"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
			return
		}
		connector, err := runtime.CreateMcpConnector(r.Context(), p.TenantID, app.McpConnectorInput{
			Name: body.Name, Transport: body.Transport, Command: body.Command, Args: body.Args,
			EnvRefs: body.EnvRefs, URL: body.URL, HeaderRefs: body.HeaderRefs, DefaultOpen: body.DefaultOpen,
		})
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
	return cors(taskRouterWithResolver(
		mux,
		opts.TaskMembers,
		opts.TaskMemberID,
		memberResolver(opts),
		runtime.Tasks.CloseAllSubscribers,
		opts.TaskMemberRefresh,
	))
}

func splitMembers(raw string) []string {
	items := strings.Split(raw, ",")
	out := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item != "" {
			out = append(out, item)
		}
	}
	return out
}

func taskRouter(next http.Handler, members []string, self string) http.Handler {
	return taskRouterWithResolver(next, members, self, nil, nil, 0)
}

func memberResolver(opts Options) func() []string {
	if opts.TaskMemberResolver != nil {
		return opts.TaskMemberResolver
	}
	if os.Getenv("ORBIT_CONTROL_MEMBERS_FILE") == "" && os.Getenv("ORBIT_CONTROL_MEMBERS_REFRESH_SECONDS") == "" {
		return nil
	}
	return func() []string {
		if path := os.Getenv("ORBIT_CONTROL_MEMBERS_FILE"); path != "" {
			data, err := os.ReadFile(path)
			if err == nil {
				return splitMembers(string(data))
			}
		}
		return splitMembers(os.Getenv("ORBIT_CONTROL_MEMBERS"))
	}
}

func taskRouterWithResolver(
	next http.Handler,
	members []string,
	self string,
	resolveMembers func() []string,
	onMembersChanged func(),
	refresh time.Duration,
) http.Handler {
	if resolveMembers != nil {
		if resolved := resolveMembers(); len(resolved) >= 2 {
			members = resolved
		}
	}
	if len(members) < 2 || self == "" {
		return next
	}
	ring := taskruntime.NewRing(64)
	ring.SetMembers(members)
	proxies := make(map[string]*httputil.ReverseProxy, len(members))
	var proxiesMu sync.RWMutex
	buildProxies := func(items []string) map[string]*httputil.ReverseProxy {
		result := make(map[string]*httputil.ReverseProxy, len(items))
		for _, member := range items {
			target, err := url.Parse(member)
			if err != nil || target.Scheme == "" || target.Host == "" {
				continue
			}
			result[member] = httputil.NewSingleHostReverseProxy(target)
		}
		return result
	}
	proxies = buildProxies(members)
	if resolveMembers != nil {
		interval := refresh
		if interval <= 0 {
			interval = 5 * time.Second
		}
		if raw := os.Getenv("ORBIT_CONTROL_MEMBERS_REFRESH_SECONDS"); raw != "" {
			if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 {
				interval = time.Duration(seconds) * time.Second
			}
		}
		go func() {
			current := append([]string(nil), members...)
			for range time.Tick(interval) {
				nextMembers := resolveMembers()
				if len(nextMembers) < 2 || slices.Equal(current, nextMembers) {
					continue
				}
				ring.SetMembers(nextMembers)
				proxiesMu.Lock()
				proxies = buildProxies(nextMembers)
				proxiesMu.Unlock()
				current = append([]string(nil), nextMembers...)
				if onMembersChanged != nil {
					onMembersChanged()
				}
			}
		}()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Orbit-Task-Forwarded") == "1" {
			next.ServeHTTP(w, r)
			return
		}
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 3 || parts[0] != "v1" || parts[1] != "tasks" {
			next.ServeHTTP(w, r)
			return
		}
		owner, ok := ring.Owner(parts[2])
		proxiesMu.RLock()
		proxy := proxies[owner]
		proxiesMu.RUnlock()
		ownerURL, _ := url.Parse(owner)
		ownerID := ownerURL.Hostname()
		if !ok || owner == self || (ownerID != "" && (ownerID == self || strings.HasPrefix(ownerID, self+"."))) || proxy == nil {
			next.ServeHTTP(w, r)
			return
		}
		r.Header.Set("X-Orbit-Task-Forwarded", "1")
		proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
			writeErr(w, http.StatusBadGateway, "TASK_OWNER_UNAVAILABLE", "task owner is temporarily unavailable")
		}
		proxy.ServeHTTP(w, r)
	})
}

// InternalHandler serves worker → control routes. It must be bound to a
// listener that is not published (ORBIT_INTERNAL_ADDR), never the public one.
func InternalHandler(runtime *app.App) http.Handler {
	return InternalHandlerWithForwarder(runtime, nil)
}

// InternalHandlerWithForwarder is InternalHandler for a control replica that shares tasks with others: an ephemeral
// event for a task another replica owns is handed to that replica (09 §4.1). A nil forwarder keeps every event local.
func InternalHandlerWithForwarder(runtime *app.App, forwarder *ephemeralForwarder) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, HealthBody{Status: "ok"})
	})
	mux.HandleFunc("POST /internal/tasks/reconcile", func(w http.ResponseWriter, r *http.Request) {
		if !internalauth.Authorized(r) {
			writeErr(w, http.StatusUnauthorized, "UNAUTHORIZED", "internal token required")
			return
		}
		var body struct {
			TenantID string `json:"tenant_id"`
			UserID   string `json:"user_id"`
			TaskID   string `json:"task_id"`
			Repair   bool   `json:"repair"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.TenantID == "" || body.UserID == "" || body.TaskID == "" {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "tenant_id, user_id and task_id are required")
			return
		}
		report, err := runtime.Tasks.Reconcile(r.Context(), taskruntime.Principal{TenantID: body.TenantID, UserID: body.UserID}, body.TaskID, body.Repair)
		if err != nil {
			if errors.Is(err, taskruntime.ErrNotFound) {
				writeErr(w, http.StatusNotFound, "NOT_FOUND", "task not found")
				return
			}
			runtime.Log.Printf("task reconcile: %v", err)
			writeErr(w, http.StatusBadGateway, "RECONCILE_FAILED", "task reconciliation failed")
			return
		}
		writeJSON(w, http.StatusOK, report)
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
		var v3 struct {
			Schema     string          `json:"schema"`
			EventID    string          `json:"event_id"`
			TenantID   string          `json:"tenant_id"`
			TaskID     string          `json:"task_id"`
			Type       string          `json:"type"`
			Source     map[string]any  `json:"source"`
			Entity     map[string]any  `json:"entity"`
			Retention  string          `json:"retention"`
			OccurredAt time.Time       `json:"occurred_at"`
			Payload    json.RawMessage `json:"payload"`
		}
		if json.Unmarshal(raw, &v3) == nil && v3.Schema == "orbit.event/3" {
			// Durable events reach the projection only through runtime_outbox (09 §3); this path is for the live stream.
			if v3.Retention != "ephemeral" {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": "BAD_REQUEST", "message": "durable events go through runtime_outbox"})
				return
			}
			if forwarder != nil && forwarder.forward(r.Context(), r, v3.TaskID, raw) {
				writeJSON(w, http.StatusAccepted, map[string]any{"ok": true})
				return
			}
			source := "worker"
			if kind, ok := v3.Source["kind"].(string); ok && kind != "" {
				source = kind
			}
			if err := runtime.Tasks.AppendEvent(taskruntime.Event{EventID: v3.EventID, TenantID: v3.TenantID, TaskID: v3.TaskID, Type: v3.Type, Source: source, Payload: v3.Payload, Occurred: v3.OccurredAt, Durable: v3.Retention != "ephemeral"}); err != nil {
				writeAppErr(runtime.Log, w, err, roomNotFound, http.StatusInternalServerError, "STORAGE_ERROR")
				return
			}
			writeJSON(w, http.StatusAccepted, map[string]any{"ok": true})
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
		ref, err := runtime.SaveArtifactBlob(taskID, r.URL.Query().Get("tenantId"), r.Header.Get("X-Content-Digest"), r.Body)
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

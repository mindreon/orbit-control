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
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
	"github.com/sethvargo/go-retry"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/artifacts"
	"github.com/mindreon/orbit-control/internal/config"
	"github.com/mindreon/orbit-control/internal/internalauth"
	"github.com/mindreon/orbit-control/internal/msmarket"
	"github.com/mindreon/orbit-control/internal/orch"
	"github.com/mindreon/orbit-control/internal/skillstore"
	"github.com/mindreon/orbit-control/internal/store"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// ErrorBody is the JSON error shape. Keep 401/403 examples in docs/openapi.yaml
// stable for Sentinel.
type ErrorBody struct {
	Error   string `json:"error"`
	Code    string `json:"code"`
	Message string `json:"message"`
	// Field and Reason say which part of the request was refused and why; set on validation refusals that have one.
	Field  string `json:"field,omitempty"`
	Reason string `json:"reason,omitempty"`
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

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, ErrorBody{Error: strings.ToLower(strings.ReplaceAll(code, "_", " ")), Code: code, Message: message})
}

// writeFieldErr answers a validation refusal that names its field. It reports whether err was one.
func writeFieldErr(w http.ResponseWriter, err error) bool {
	var fe *app.FieldError
	if !errors.As(err, &fe) {
		return false
	}
	writeJSON(w, http.StatusBadRequest, ErrorBody{
		Error: strings.ToLower(strings.ReplaceAll(fe.Code, "_", " ")), Code: fe.Code, Message: fe.Reason, Field: fe.Field, Reason: fe.Reason,
	})
	return true
}

func writeBlobErr(lg *log.Logger, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, app.ErrTooLarge):
		writeErr(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "artifact body exceeds the configured limit")
	case errors.Is(err, app.ErrDigestMismatch):
		writeErr(w, http.StatusUnprocessableEntity, "DIGEST_MISMATCH", "X-Content-Digest does not match the body")
	default:
		writeAppErr(lg, w, err, "not found", http.StatusBadRequest, "BAD_REQUEST")
	}
}

// writeAppErr maps repository sentinels first; anything else keeps the
// route's legacy status. Storage error text stays in server logs.
func writeAppErr(lg *log.Logger, w http.ResponseWriter, err error, notFound string, fallback int, fallbackCode string) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, "NOT_FOUND", notFound)
	case errors.Is(err, store.ErrStorage):
		lg.Printf("storage error: %v", err)
		writeErr(w, http.StatusInternalServerError, "INTERNAL", "internal error")
	case writeFieldErr(w, err):
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

func init() {
	gin.SetMode(gin.ReleaseMode)
}

func newEngine() *gin.Engine {
	engine := gin.New()
	engine.Use(gin.Recovery())
	return engine
}

// withCORS answers preflight before the task router, matching the previous
// hand-written headers: any origin, and the headers the web client sends.
func withCORS(next http.Handler) http.Handler {
	engine := gin.New()
	engine.Use(cors.New(cors.Config{
		AllowAllOrigins: true,
		AllowMethods:    []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:    []string{"Content-Type", "Authorization", "Idempotency-Key", "X-Orbit-Request", "Last-Event-ID"},
	}))
	engine.NoRoute(func(c *gin.Context) {
		next.ServeHTTP(c.Writer, c.Request)
	})
	return engine
}

func ginAdapt(next http.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		for _, param := range c.Params {
			c.Request.SetPathValue(param.Key, param.Value)
		}
		next(c.Writer, c.Request)
	}
}

type artifactNameQuery struct {
	Name string `form:"name" binding:"required"`
}

func bindJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
		return false
	}
	if err := validateStruct(dst); err != nil {
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "the request is invalid")
		return false
	}
	return true
}

func validateStruct(dst any) error {
	if binding.Validator == nil {
		return nil
	}
	err := binding.Validator.ValidateStruct(dst)
	if err == nil {
		return nil
	}
	var invalid *validator.InvalidValidationError
	if errors.As(err, &invalid) {
		return nil
	}
	return err
}

func mutates(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

// Handlers builds the process's public and internal muxes over one App,
// configured from the environment.
// When TEMPORAL_ADDRESS is set, tasks are driven through TaskWorkflow.
// Storage follows §18.2 (ORBIT_CONTROL_DB_URL); the returned func closes it.
// The internal handler must only be bound to ORBIT_INTERNAL_ADDR.
func Handlers() (public, internal http.Handler, closeStore func(), err error) {
	cfg := config.Load()
	repo, err := openRepository(context.Background())
	if err != nil {
		return nil, nil, nil, err
	}
	defaultTenant := cfg.DefaultTenant
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
		Repo: repo, DefaultTenant: defaultTenant,
		ArtifactDir: cfg.ArtifactDirPath(), ArtifactMaxBytes: cfg.ArtifactMaxBytes,
	}
	if endpoint := cfg.ObjectStoreURL(); endpoint != "" {
		signer, err := artifacts.New(endpoint, cfg.ObjectStoreAccessKey, cfg.ObjectStoreSecretKey, cfg.ObjectStoreBucket, cfg.ObjectStoreSecure)
		if err != nil {
			repo.Close()
			return nil, nil, nil, err
		}
		opts.ArtifactSigner = signer
	} else if opts.ArtifactDir != "" {
		opts.ArtifactSigner = localArtifactSigner{}
	}
	if cfg.TemporalAddress != "" {
		oc, err := dialOrch(context.Background(), cfg.TemporalAddress, cfg.TemporalNamespace, cfg.TemporalTaskQueue)
		if err != nil {
			repo.Close()
			return nil, nil, nil, errors.New("temporal dial: " + err.Error())
		}
		opts.TaskClient = oc
	}
	// A skills directory that is set but unusable stops the start: an empty library would hide the mistake.
	skills, err := skillstore.Open(cfg.SkillsDir)
	if err != nil {
		repo.Close()
		return nil, nil, nil, errors.New("skills directory: " + err.Error())
	}
	opts.Skills = skills
	runtime := app.NewWithOptions(opts)
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
	// The ModelScope snapshot ships with this binary, so GET /v1/skills,
	// /v1/mcp-market and /v1/agents only read the store. The copy runs in the
	// background; a restart with an unchanged snapshot skips it. When
	// ORBIT_CATALOG_DIR provides skills_text.json.gz, the file text is filled too.
	go msmarket.InstallAll(context.Background(), repo, defaultTenant, cfg.CatalogDir, runtime.Log)
	public = HandlerWithOptions(runtime, Options{
		Auth:           authenticatorFromEnv(defaultTenant),
		AllowedOrigins: cfg.AllowedOrigins,
		TaskMembers:    cfg.TaskMembers,
		TaskMemberID:   cfg.TaskMemberID,
		Models:         cfg.Models,
		DefaultModel:   cfg.DefaultModel,
	})
	internalHandler := InternalHandlerWithForwarder(runtime, newEphemeralForwarder(Options{
		TaskMembers:        cfg.TaskMembers,
		TaskMemberID:       cfg.TaskMemberID,
		TaskMemberResolver: nil,
	}))
	return public, internalHandler, func() { projectorCancel(); repo.Close() }, nil
}

// dialOrch retries Temporal with exponential backoff. ctx cancels the wait
// and the in-flight dial.
func dialOrch(ctx context.Context, addr, namespace, taskQueue string) (*orch.Client, error) {
	backoff := retry.WithMaxRetries(29, retry.WithMaxDuration(30*time.Second, retry.WithCappedDuration(5*time.Second, retry.NewExponential(time.Second))))
	var client *orch.Client
	err := retry.Do(ctx, backoff, func(ctx context.Context) error {
		oc, dialErr := orch.DialContext(ctx, addr, namespace, taskQueue)
		if dialErr != nil {
			log.Printf("temporal not ready: %v", dialErr)
			return retry.RetryableError(dialErr)
		}
		client = oc
		return nil
	})
	if err != nil {
		return nil, err
	}
	return client, nil
}

type Options struct {
	Auth Authenticator
	// AllowedOrigins is the CSRF Origin allowlist (§17.4).
	AllowedOrigins     []string
	TaskMembers        []string
	TaskMemberID       string
	TaskMemberResolver func() []string
	TaskMemberRefresh  time.Duration
	// Models is the deployment's model catalog (ORBIT_MODELS) and DefaultModel its default (ORBIT_MODEL_NAME);
	// GET /v1/models serves them to the task's model picker.
	Models       []string
	DefaultModel string
}

// HandlerWith serves runtime with the local dev principal.
func HandlerWith(runtime *app.App) http.Handler {
	cfg := config.Load()
	return HandlerWithOptions(runtime, Options{
		Auth:           LocalAuthenticator(runtime.DefaultTenant),
		AllowedOrigins: cfg.AllowedOrigins,
		TaskMembers:    cfg.TaskMembers,
		TaskMemberID:   cfg.TaskMemberID,
		Models:         cfg.Models,
		DefaultModel:   cfg.DefaultModel,
	})
}

type principalHandler func(http.ResponseWriter, *http.Request, app.Principal)

func HandlerWithOptions(runtime *app.App, opts Options) http.Handler {
	if opts.Auth == nil {
		opts.Auth = denyAllAuthenticator
	}
	// authed runs before any repository call, so unauthenticated requests get
	// the same 401 on every path (§17.6).
	authed := func(next principalHandler) gin.HandlerFunc {
		return func(c *gin.Context) {
			for _, param := range c.Params {
				c.Request.SetPathValue(param.Key, param.Value)
			}
			principal, ok := opts.Auth.Authenticate(c.Request)
			if !ok {
				writeUnauthenticated(c.Writer)
				return
			}
			if mutates(c.Request.Method) && len(opts.AllowedOrigins) > 0 && !csrfOK(c.Request, opts.AllowedOrigins) {
				writeCSRFRejected(c.Writer)
				return
			}
			next(c.Writer, c.Request, principal)
		}
	}
	engine := newEngine()
	engine.GET("/health", ginAdapt(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, HealthBody{Status: "ok"})
	}))
	registerTaskRoutes(engine, runtime, authed)
	// Internal routes live only on InternalHandler's listener.
	notFound := ginAdapt(func(w http.ResponseWriter, _ *http.Request) {
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "not found")
	})
	engine.Any("/internal", notFound)
	engine.Any("/internal/*path", notFound)
	engine.GET("/v1/personas", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.ListPersonas(r.Context(), p.TenantID)
		if err != nil {
			writeAppErr(runtime.Log, w, err, "persona not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	engine.POST("/v1/personas", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body struct {
			Name         string   `json:"name" binding:"required"`
			Instructions string   `json:"instructions"`
			McpIds       []string `json:"mcpConnectorIds"`
		}
		if !bindJSON(w, r, &body) {
			return
		}
		persona, err := runtime.CreatePersona(r.Context(), p.TenantID, body.Name, body.Instructions, body.McpIds)
		if err != nil {
			writeAppErr(runtime.Log, w, err, "persona not found", http.StatusBadRequest, "BAD_REQUEST")
			return
		}
		writeJSON(w, http.StatusOK, persona)
	}))
	engine.GET("/v1/mcp-connectors", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.ListMcpConnectors(r.Context(), p.TenantID)
		if err != nil {
			writeAppErr(runtime.Log, w, err, "connector not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	engine.GET("/v1/mcp-market", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		q := r.URL.Query()
		page, _ := strconv.Atoi(q.Get("page"))
		pageSize, _ := strconv.Atoi(q.Get("pageSize"))
		list, err := runtime.ListMcpMarket(r.Context(), p.TenantID, store.McpMarketQuery{
			Keyword:     q.Get("keyword"),
			Category:    q.Get("category"),
			ServiceType: q.Get("serviceType"),
			NeedsOnline: q.Get("needsOnline"),
			Source:      q.Get("source"),
			Page:        page,
			PageSize:    pageSize,
		})
		if err != nil {
			writeAppErr(runtime.Log, w, err, "market not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, list)
	}))
	engine.GET("/v1/mcp-market/:id", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		detail, err := runtime.GetMcpMarket(r.Context(), p.TenantID, r.PathValue("id"))
		if err != nil {
			writeAppErr(runtime.Log, w, err, "market server not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, detail)
	}))
	writeIcon := func(w http.ResponseWriter, r *http.Request, p app.Principal, fetch func(context.Context, string) (string, []byte, error)) {
		contentType, data, err := fetch(r.Context(), p.TenantID)
		if err != nil {
			writeAppErr(runtime.Log, w, err, "icon not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "private, max-age=86400")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}
	engine.GET("/v1/mcp-market/:id/icon", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		id := r.PathValue("id")
		writeIcon(w, r, p, func(ctx context.Context, tenantID string) (string, []byte, error) {
			return runtime.McpMarketIcon(ctx, tenantID, id)
		})
	}))
	engine.GET("/v1/mcp-market-categories", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.ListMcpMarketCategories(r.Context(), p.TenantID, r.URL.Query().Get("needsOnline"))
		if err != nil {
			writeAppErr(runtime.Log, w, err, "market not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	engine.GET("/v1/skills", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		q := r.URL.Query()
		page, _ := strconv.Atoi(q.Get("page"))
		pageSize, _ := strconv.Atoi(q.Get("pageSize"))
		list, err := runtime.ListSkills(r.Context(), p.TenantID, store.SkillCatalogQuery{
			Sort:     q.Get("sortBy"),
			Category: q.Get("category"),
			Source:   q.Get("source"),
			Keyword:  q.Get("keyword"),
			Page:     page,
			PageSize: pageSize,
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
		files, err := runtime.SkillTextFiles(r.Context(), p.TenantID, handle, slug)
		if err != nil {
			writeAppErr(runtime.Log, w, err, "skill not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": files})
	}
	engine.GET("/v1/skills/:handle/:slug", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		writeSkill(w, r, p, r.PathValue("handle"), r.PathValue("slug"))
	}))
	engine.GET("/v1/skills/:handle", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		writeSkill(w, r, p, "", r.PathValue("handle"))
	}))
	engine.GET("/v1/skill-files/:handle/:slug", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		writeSkillFiles(w, r, p, r.PathValue("handle"), r.PathValue("slug"))
	}))
	engine.GET("/v1/skill-files/:handle", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		writeSkillFiles(w, r, p, "", r.PathValue("handle"))
	}))
	engine.GET("/v1/skills/:handle/:slug/icon", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		handle, slug := r.PathValue("handle"), r.PathValue("slug")
		writeIcon(w, r, p, func(ctx context.Context, tenantID string) (string, []byte, error) {
			return runtime.SkillIcon(ctx, tenantID, handle, slug)
		})
	}))
	engine.GET("/v1/skills/:handle/icon", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		handle := r.PathValue("handle")
		writeIcon(w, r, p, func(ctx context.Context, tenantID string) (string, []byte, error) {
			return runtime.SkillIcon(ctx, tenantID, handle, "")
		})
	}))
	engine.GET("/v1/agents/:handle/:slug/icon", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		handle, slug := r.PathValue("handle"), r.PathValue("slug")
		writeIcon(w, r, p, func(ctx context.Context, tenantID string) (string, []byte, error) {
			return runtime.AgentIcon(ctx, tenantID, handle, slug)
		})
	}))
	engine.GET("/v1/agents/:handle/icon", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		handle := r.PathValue("handle")
		writeIcon(w, r, p, func(ctx context.Context, tenantID string) (string, []byte, error) {
			return runtime.AgentIcon(ctx, tenantID, handle, "")
		})
	}))
	engine.GET("/v1/models", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		writeModels(w, opts.Models, opts.DefaultModel)
	}))
	engine.GET("/v1/skill-categories", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.ListSkillCategories(r.Context(), p.TenantID)
		if err != nil {
			writeAppErr(runtime.Log, w, err, "skill not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	engine.GET("/v1/agents", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		q := r.URL.Query()
		page, _ := strconv.Atoi(q.Get("page"))
		pageSize, _ := strconv.Atoi(q.Get("pageSize"))
		list, err := runtime.ListAgents(r.Context(), p.TenantID, store.AgentCatalogQuery{
			Sort:      q.Get("sortBy"),
			Catalogue: q.Get("catalogue"),
			Keyword:   q.Get("keyword"),
			Page:      page,
			PageSize:  pageSize,
		})
		if err != nil {
			writeAppErr(runtime.Log, w, err, "agent not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, list)
	}))
	writeAgent := func(w http.ResponseWriter, r *http.Request, p app.Principal, handle, slug string) {
		agent, err := runtime.GetAgent(r.Context(), p.TenantID, handle, slug)
		if err != nil {
			writeAppErr(runtime.Log, w, err, "agent not found", http.StatusInternalServerError, "INTERNAL")
			return
		}
		writeJSON(w, http.StatusOK, agent)
	}
	engine.GET("/v1/agents/:handle/:slug", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		writeAgent(w, r, p, r.PathValue("handle"), r.PathValue("slug"))
	}))
	engine.GET("/v1/agents/:handle", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		writeAgent(w, r, p, "", r.PathValue("handle"))
	}))
	engine.POST("/v1/mcp-connectors", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body struct {
			Name        string          `json:"name" binding:"required"`
			Transport   string          `json:"transport"`
			Command     string          `json:"command"`
			Args        []string        `json:"args"`
			EnvRefs     []string        `json:"envRefs"`
			URL         string          `json:"url"`
			HeaderRefs  []app.HeaderRef `json:"headerRefs"`
			DefaultOpen bool            `json:"defaultOpen"`
		}
		if !bindJSON(w, r, &body) {
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
	return withCORS(taskRouterWithResolver(
		engine,
		opts.TaskMembers,
		opts.TaskMemberID,
		memberResolver(opts),
		runtime.Tasks.CloseAllSubscribers,
		opts.TaskMemberRefresh,
	))
}

func splitMembers(raw string) []string { return config.SplitCSV(raw) }

func taskRouter(next http.Handler, members []string, self string) http.Handler {
	return taskRouterWithResolver(next, members, self, nil, nil, 0)
}

func memberResolver(opts Options) func() []string {
	if opts.TaskMemberResolver != nil {
		return opts.TaskMemberResolver
	}
	cfg := config.Load()
	if cfg.MembersFile == "" && !cfg.MembersRefreshConfigured {
		return nil
	}
	return func() []string {
		current := config.Load()
		if current.MembersFile != "" {
			data, err := os.ReadFile(current.MembersFile)
			if err == nil {
				return splitMembers(string(data))
			}
		}
		return current.TaskMembers
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
		if seconds := config.Load().MembersRefreshSeconds; seconds > 0 {
			interval = time.Duration(seconds) * time.Second
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
	engine := newEngine()
	engine.GET("/health", ginAdapt(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, HealthBody{Status: "ok"})
	}))
	registerInternalSkills(engine, runtime)
	registerInternalExpertSkills(engine, runtime)
	engine.POST("/internal/tasks/reconcile", ginAdapt(func(w http.ResponseWriter, r *http.Request) {
		if !internalauth.Authorized(r) {
			writeErr(w, http.StatusUnauthorized, "UNAUTHORIZED", "internal token required")
			return
		}
		var body struct {
			TenantID string `json:"tenant_id" binding:"required"`
			UserID   string `json:"user_id" binding:"required"`
			TaskID   string `json:"task_id" binding:"required"`
			Repair   bool   `json:"repair"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || validateStruct(&body) != nil {
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
	}))
	engine.POST("/internal/events", ginAdapt(func(w http.ResponseWriter, r *http.Request) {
		if !internalauth.Authorized(r) {
			writeErr(w, http.StatusUnauthorized, "UNAUTHORIZED", "internal token required")
			return
		}
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, runtime.IngestMaxBytes))
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
				writeAppErr(runtime.Log, w, err, "task not found", http.StatusInternalServerError, "STORAGE_ERROR")
				return
			}
			writeJSON(w, http.StatusAccepted, map[string]any{"ok": true})
			return
		}
		writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "event schema must be orbit.event/3")
	}))
	engine.POST("/internal/artifact-blobs", ginAdapt(func(w http.ResponseWriter, r *http.Request) {
		if !internalauth.Authorized(r) {
			writeErr(w, http.StatusUnauthorized, "UNAUTHORIZED", "internal token required")
			return
		}
		tenant := struct {
			TenantID string `form:"tenantId" binding:"required"`
		}{TenantID: r.URL.Query().Get("tenantId")}
		if validateStruct(&tenant) != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "tenantId is required")
			return
		}
		ref, err := runtime.SaveArtifactBlob(r.URL.Query().Get("tenantId"), r.Header.Get("X-Content-Digest"), r.Body)
		if err != nil {
			writeBlobErr(runtime.Log, w, err)
			return
		}
		writeJSON(w, http.StatusOK, struct {
			StorageRef string `json:"storageRef"`
		}{StorageRef: ref})
	}))
	engine.NoRoute(ginAdapt(func(w http.ResponseWriter, _ *http.Request) {
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "not found")
	}))
	return engine
}

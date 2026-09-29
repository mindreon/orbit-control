package httpapi

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/store"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

func registerTaskRoutes(router gin.IRoutes, runtime *app.App, authed func(principalHandler) gin.HandlerFunc) {
	toPrincipal := func(p app.Principal) taskruntime.Principal {
		return taskruntime.Principal{TenantID: p.TenantID, UserID: p.UserID}
	}
	commandID := func(raw string) string {
		if strings.TrimSpace(raw) != "" {
			return raw
		}
		id, err := uuid.NewV7()
		if err != nil {
			panic(err)
		}
		return id.String()
	}
	writeTaskErr := func(w http.ResponseWriter, err error) {
		switch {
		case errors.Is(err, taskruntime.ErrNotFound):
			writeErr(w, http.StatusNotFound, "NOT_FOUND", "task not found")
		case errors.Is(err, taskruntime.ErrClosed):
			writeErr(w, http.StatusConflict, "TASK_CLOSED", "task is closed")
		case errors.Is(err, taskruntime.ErrIdempotencyConflict):
			writeErr(w, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED", "command id was already used with a different request")
		case errors.Is(err, store.ErrStorage):
			runtime.Log.Printf("task storage error: %v", err)
			writeErr(w, http.StatusInternalServerError, "STORAGE_ERROR", "task storage is temporarily unavailable")
		default:
			runtime.Log.Printf("bad task request: %v", err)
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "the request is invalid")
		}
	}

	router.GET("/v1/tasks", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.Tasks.List(r.Context(), toPrincipal(p))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	router.GET("/v1/profiles", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.Tasks.ListProfiles(r.Context(), toPrincipal(p))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	router.POST("/v1/profiles", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body struct {
			ProfileID string         `json:"profile_id" binding:"required"`
			Version   int            `json:"version" binding:"min=1"`
			Spec      map[string]any `json:"spec"`
		}
		if !bindJSON(w, r, &body) {
			return
		}
		profile, err := runtime.Tasks.RegisterProfile(r.Context(), toPrincipal(p), taskruntime.Profile{ProfileID: body.ProfileID, Version: body.Version, Spec: body.Spec})
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, profile)
	}))
	router.GET("/v1/profiles/:profileRef", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		profile, err := runtime.Tasks.GetProfile(r.Context(), toPrincipal(p), r.PathValue("profileRef"))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, profile)
	}))
	router.GET("/v1/policy", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		policy, err := runtime.Tasks.GetTenantPolicy(r.Context(), toPrincipal(p))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, policy)
	}))
	router.PUT("/v1/policy", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body taskruntime.Policy
		if !bindJSON(w, r, &body) {
			return
		}
		policy, err := runtime.Tasks.SetTenantPolicy(r.Context(), toPrincipal(p), body)
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, policy)
	}))
	router.GET("/v1/sops", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.Tasks.ListSOPs(r.Context(), toPrincipal(p))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	router.POST("/v1/sops", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body struct {
			SOPID   string                `json:"sop_id" binding:"required"`
			Version int                   `json:"version" binding:"min=1"`
			Steps   []taskruntime.SOPStep `json:"steps"`
		}
		if !bindJSON(w, r, &body) {
			return
		}
		sop, err := runtime.Tasks.RegisterSOP(r.Context(), toPrincipal(p), taskruntime.SOP{SOPID: body.SOPID, Version: body.Version, Steps: body.Steps})
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, sop)
	}))
	router.POST("/v1/tasks", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body struct {
			Title   string             `json:"title" binding:"required"`
			Goal    string             `json:"goal" binding:"required"`
			Mode    string             `json:"mode"`
			Profile string             `json:"profile"`
			SOP     string             `json:"sop"`
			Budgets map[string]any     `json:"budgets"`
			Policy  taskruntime.Policy `json:"policy"`
		}
		if !bindJSON(w, r, &body) {
			return
		}
		t, err := runtime.Tasks.Create(r.Context(), toPrincipal(p), taskruntime.CreateInput{Title: body.Title, Goal: body.Goal, Mode: body.Mode, Profile: body.Profile, SOP: body.SOP, Budgets: body.Budgets, Policy: body.Policy})
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, t)
	}))
	router.GET("/v1/tasks/:taskId", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		t, err := runtime.Tasks.Get(r.Context(), toPrincipal(p), r.PathValue("taskId"))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, t)
	}))
	router.GET("/v1/tasks/:taskId/plan", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		plan, err := runtime.Tasks.Plan(r.Context(), toPrincipal(p), r.PathValue("taskId"))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, plan)
	}))
	router.GET("/v1/tasks/:taskId/artifacts", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.Tasks.Manifests(r.Context(), toPrincipal(p), r.PathValue("taskId"))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	router.GET("/v1/artifacts/:manifestId", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		manifest, err := runtime.Tasks.GetManifest(r.Context(), toPrincipal(p), r.PathValue("manifestId"))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, manifest)
	}))
	router.GET("/v1/artifacts/:manifestId/url", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		name := artifactNameQuery{Name: strings.TrimSpace(r.URL.Query().Get("name"))}
		if validateStruct(&name) != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "artifact entry name is required")
			return
		}
		url, err := runtime.Tasks.PresignArtifact(r.Context(), toPrincipal(p), r.PathValue("manifestId"), name.Name)
		if err != nil {
			if strings.Contains(err.Error(), "not configured") {
				writeErr(w, http.StatusServiceUnavailable, "ARTIFACT_STORE_UNAVAILABLE", "artifact storage is temporarily unavailable")
				return
			}
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"url": url})
	}))
	router.GET("/v1/artifacts/:manifestId/download", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		query := artifactNameQuery{Name: strings.TrimSpace(r.URL.Query().Get("name"))}
		if validateStruct(&query) != nil || runtime.ArtifactDir == "" {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "artifact entry name is required")
			return
		}
		name := query.Name
		manifest, err := runtime.Tasks.GetManifest(r.Context(), toPrincipal(p), r.PathValue("manifestId"))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		for _, entry := range manifest.Entries {
			if entryName, _ := entry["name"].(string); entryName != name {
				continue
			}
			ref, _ := entry["blob_ref"].(string)
			digest := strings.TrimPrefix(ref, "sha256:")
			if len(digest) != 64 {
				break
			}
			if _, err := hex.DecodeString(digest); err != nil {
				break
			}
			path := filepath.Join(runtime.ArtifactDir, p.TenantID, digest)
			if _, err := os.Stat(path); err != nil {
				break
			}
			http.ServeFile(w, r, path)
			return
		}
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "artifact entry not found")
	}))
	router.GET("/v1/tasks/:taskId/events", authed(streamTaskEvents(runtime)))
	router.POST("/v1/tasks/:taskId/messages", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body map[string]any
		if !bindJSON(w, r, &body) {
			return
		}
		id := commandID(stringValue(body, "command_id", "commandId"))
		clientID := commandID(stringValue(body, "client_message_id", "clientMessageId"))
		payload := map[string]any{"command_id": id, "client_message_id": clientID, "text": stringValue(body, "text", "message"), "delivery": stringValue(body, "delivery")}
		if payload["delivery"] == "" {
			payload["delivery"] = "queue"
		}
		raw, err := runtime.Tasks.Update(r.Context(), toPrincipal(p), r.PathValue("taskId"), "sendMessage", id, payload)
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, json.RawMessage(raw))
	}))
	router.POST("/v1/tasks/:taskId/control", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body map[string]any
		if !bindJSON(w, r, &body) {
			return
		}
		id := commandID(stringValue(body, "command_id", "commandId"))
		payload := map[string]any{"command_id": id, "action": stringValue(body, "action"), "reason": stringValue(body, "reason")}
		raw, err := runtime.Tasks.Update(r.Context(), toPrincipal(p), r.PathValue("taskId"), "control", id, payload)
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, json.RawMessage(raw))
	}))
	router.POST("/v1/tasks/:taskId/budget", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body map[string]any
		if !bindJSON(w, r, &body) {
			return
		}
		id := commandID(stringValue(body, "command_id", "commandId"))
		body["command_id"] = id
		raw, err := runtime.Tasks.Update(r.Context(), toPrincipal(p), r.PathValue("taskId"), "grantBudget", id, body)
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, json.RawMessage(raw))
	}))
	router.POST("/v1/tasks/:taskId/profile", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body map[string]any
		if !bindJSON(w, r, &body) {
			return
		}
		id := commandID(stringValue(body, "command_id", "commandId"))
		body["command_id"] = id
		raw, err := runtime.Tasks.Update(r.Context(), toPrincipal(p), r.PathValue("taskId"), "requestProfileSwitch", id, body)
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, json.RawMessage(raw))
	}))
	router.POST("/v1/tasks/:taskId/approvals/:approvalId", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body map[string]any
		if !bindJSON(w, r, &body) {
			return
		}
		id := commandID(stringValue(body, "command_id", "commandId"))
		payload := map[string]any{"command_id": id, "approval_id": r.PathValue("approvalId"), "decision": stringValue(body, "decision"), "comment": stringValue(body, "comment")}
		raw, err := runtime.Tasks.Update(r.Context(), toPrincipal(p), r.PathValue("taskId"), "decideApproval", id, payload)
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, json.RawMessage(raw))
	}))
	router.POST("/v1/tasks/:taskId/plan", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body map[string]any
		if !bindJSON(w, r, &body) {
			return
		}
		id := commandID(stringValue(body, "command_id", "commandId"))
		body["command_id"] = id
		if _, ok := body["task_id"]; !ok {
			body["task_id"] = r.PathValue("taskId")
		}
		raw, err := runtime.Tasks.Update(r.Context(), toPrincipal(p), r.PathValue("taskId"), "submitPlanChange", id, body)
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, json.RawMessage(raw))
	}))
}

func stringValue(body map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := body[key].(string); ok {
			return value
		}
	}
	return ""
}

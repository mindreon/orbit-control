package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/store"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

func registerTaskRoutes(mux *http.ServeMux, runtime *app.App, authed func(principalHandler) http.HandlerFunc) {
	toPrincipal := func(p app.Principal) taskruntime.Principal {
		return taskruntime.Principal{TenantID: p.TenantID, UserID: p.UserID}
	}
	commandID := func(raw string) string {
		if strings.TrimSpace(raw) != "" {
			return raw
		}
		sum := sha256.Sum256([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
		return hex.EncodeToString(sum[:])
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
			writeErr(w, http.StatusInternalServerError, "STORAGE_ERROR", "task storage is temporarily unavailable")
		default:
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "the request is invalid")
		}
	}

	mux.HandleFunc("GET /v1/tasks", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.Tasks.List(r.Context(), toPrincipal(p))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	mux.HandleFunc("GET /v1/profiles", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.Tasks.ListProfiles(r.Context(), toPrincipal(p))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	mux.HandleFunc("POST /v1/profiles", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body struct {
			ProfileID string         `json:"profile_id"`
			Version   int            `json:"version"`
			Spec      map[string]any `json:"spec"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
			return
		}
		profile, err := runtime.Tasks.RegisterProfile(r.Context(), toPrincipal(p), taskruntime.Profile{ProfileID: body.ProfileID, Version: body.Version, Spec: body.Spec})
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, profile)
	}))
	mux.HandleFunc("GET /v1/profiles/{profileRef}", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		profile, err := runtime.Tasks.GetProfile(r.Context(), toPrincipal(p), r.PathValue("profileRef"))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, profile)
	}))
	mux.HandleFunc("GET /v1/sops", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.Tasks.ListSOPs(r.Context(), toPrincipal(p))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	mux.HandleFunc("POST /v1/sops", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body struct {
			SOPID   string                `json:"sop_id"`
			Version int                   `json:"version"`
			Steps   []taskruntime.SOPStep `json:"steps"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
			return
		}
		sop, err := runtime.Tasks.RegisterSOP(r.Context(), toPrincipal(p), taskruntime.SOP{SOPID: body.SOPID, Version: body.Version, Steps: body.Steps})
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, sop)
	}))
	mux.HandleFunc("POST /v1/tasks", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body struct {
			Title   string         `json:"title"`
			Goal    string         `json:"goal"`
			Mode    string         `json:"mode"`
			Profile string         `json:"profile"`
			SOP     string         `json:"sop"`
			Budgets map[string]any `json:"budgets"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
			return
		}
		t, err := runtime.Tasks.Create(r.Context(), toPrincipal(p), taskruntime.CreateInput{Title: body.Title, Goal: body.Goal, Mode: body.Mode, Profile: body.Profile, SOP: body.SOP, Budgets: body.Budgets})
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, t)
	}))
	mux.HandleFunc("GET /v1/tasks/{taskId}", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		t, err := runtime.Tasks.Get(r.Context(), toPrincipal(p), r.PathValue("taskId"))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, t)
	}))
	mux.HandleFunc("GET /v1/tasks/{taskId}/plan", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		plan, err := runtime.Tasks.Plan(r.Context(), toPrincipal(p), r.PathValue("taskId"))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, plan)
	}))
	mux.HandleFunc("GET /v1/tasks/{taskId}/artifacts", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.Tasks.Manifests(r.Context(), toPrincipal(p), r.PathValue("taskId"))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	mux.HandleFunc("GET /v1/artifacts/{manifestId}", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		manifest, err := runtime.Tasks.GetManifest(r.Context(), toPrincipal(p), r.PathValue("manifestId"))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, manifest)
	}))
	mux.HandleFunc("GET /v1/artifacts/{manifestId}/url", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		name := strings.TrimSpace(r.URL.Query().Get("name"))
		if name == "" {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "artifact entry name is required")
			return
		}
		url, err := runtime.Tasks.PresignArtifact(r.Context(), toPrincipal(p), r.PathValue("manifestId"), name)
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
	mux.HandleFunc("GET /v1/artifacts/{manifestId}/download", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		name := strings.TrimSpace(r.URL.Query().Get("name"))
		if name == "" || runtime.ArtifactDir == "" {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "artifact entry name is required")
			return
		}
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
	mux.HandleFunc("GET /v1/tasks/{taskId}/events", authed(streamTaskEvents(runtime)))
	mux.HandleFunc("POST /v1/tasks/{taskId}/messages", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
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
	mux.HandleFunc("POST /v1/tasks/{taskId}/control", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
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
	mux.HandleFunc("POST /v1/tasks/{taskId}/budget", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
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
	mux.HandleFunc("POST /v1/tasks/{taskId}/profile", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
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
	mux.HandleFunc("POST /v1/tasks/{taskId}/approvals/{approvalId}", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
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
	mux.HandleFunc("POST /v1/tasks/{taskId}/plan", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
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

package httpapi

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
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
		case errors.Is(err, taskruntime.ErrConfigConflict):
			writeErr(w, http.StatusConflict, "CONFIG_VERSION_CONFLICT", "the task's configuration changed since it was read; read it and retry")
		case errors.Is(err, taskruntime.ErrIdempotencyConflict):
			writeErr(w, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED", "command id was already used with a different request")
		case errors.Is(err, taskruntime.ErrCommandInProgress):
			w.Header().Set("Retry-After", "1")
			writeErr(w, http.StatusConflict, "IN_PROGRESS", "this command id is still being processed; retry with the same command id")
		case errors.Is(err, store.ErrStorage):
			runtime.Log.Printf("task storage error: %v", err)
			writeErr(w, http.StatusInternalServerError, "STORAGE_ERROR", "task storage is temporarily unavailable")
		case writeOrchRefusal(w, err):
		case writeFieldErr(w, err):
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
	writeExpertErr := func(w http.ResponseWriter, err error) {
		switch {
		case errors.Is(err, taskruntime.ErrNotFound):
			writeErr(w, http.StatusNotFound, "NOT_FOUND", "expert not found")
		case errors.Is(err, taskruntime.ErrIdempotencyConflict):
			writeErr(w, http.StatusConflict, "EXPERT_VERSION_TAKEN", "another update took the next version; read the expert and retry")
		default:
			writeTaskErr(w, err)
		}
	}
	type expertBody struct {
		Name         string   `json:"name"`
		Instructions string   `json:"instructions"`
		Soul         string   `json:"soul"`
		Model        string   `json:"model"`
		ConnectorIDs []string `json:"connector_ids"`
		SkillIDs     []string `json:"skill_ids"`
		Kind         string   `json:"kind"`
		Leader       string   `json:"leader"`
		Members      []struct {
			Role        string `json:"role"`
			Expert      string `json:"expert"`
			Description string `json:"description"`
			Label       string `json:"label"`
		} `json:"members"`
	}
	expertInput := func(b expertBody) app.ExpertInput {
		in := app.ExpertInput{Name: b.Name, Instructions: b.Instructions, Soul: b.Soul, Model: b.Model, ConnectorIDs: b.ConnectorIDs, SkillIDs: b.SkillIDs, Kind: b.Kind, Leader: b.Leader}
		for _, member := range b.Members {
			in.Members = append(in.Members, app.TeamMemberInput{Role: member.Role, Expert: member.Expert, Description: member.Description, Label: member.Label})
		}
		return in
	}
	router.GET("/v1/experts", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		items, err := runtime.ListExperts(r.Context(), toPrincipal(p))
		if err != nil {
			writeExpertErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	}))
	router.POST("/v1/experts", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body expertBody
		if !bindJSON(w, r, &body) {
			return
		}
		expert, err := runtime.CreateExpert(r.Context(), toPrincipal(p), expertInput(body))
		if err != nil {
			writeExpertErr(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, expert)
	}))
	router.POST("/v1/experts/from-agent", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body struct {
			Handle string `json:"handle"`
			Slug   string `json:"slug"`
		}
		if !bindJSON(w, r, &body) {
			return
		}
		imported, err := runtime.ExpertFromAgent(r.Context(), toPrincipal(p), body.Handle, body.Slug)
		if err != nil {
			writeExpertErr(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, imported)
	}))
	router.POST("/v1/experts/import-personas", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		imported, skipped, err := runtime.ImportPersonas(r.Context(), toPrincipal(p))
		if err != nil {
			writeExpertErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]int{"imported": imported, "skipped": skipped})
	}))
	router.GET("/v1/experts/:expertId", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		expert, err := runtime.GetExpert(r.Context(), toPrincipal(p), r.PathValue("expertId"))
		if err != nil {
			writeExpertErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, expert)
	}))
	// versionQuery reads ?version=N; absent means the latest (0). Anything else that is not a positive whole number is a 400.
	versionQuery := func(w http.ResponseWriter, r *http.Request) (int, bool) {
		raw := r.URL.Query().Get("version")
		if raw == "" {
			return 0, true
		}
		version, err := strconv.Atoi(raw)
		if err != nil || version < 1 {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "version must be a positive whole number")
			return 0, false
		}
		return version, true
	}
	router.GET("/v1/experts/:expertId/files", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		version, ok := versionQuery(w, r)
		if !ok {
			return
		}
		version, files, err := runtime.ExpertFiles(r.Context(), toPrincipal(p), r.PathValue("expertId"), version)
		if err != nil {
			writeExpertErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"version": version, "files": files})
	}))
	router.GET("/v1/experts/:expertId/files/*filePath", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		version, ok := versionQuery(w, r)
		if !ok {
			return
		}
		file, err := runtime.ExpertFile(r.Context(), toPrincipal(p), r.PathValue("expertId"), version, strings.TrimPrefix(r.PathValue("filePath"), "/"))
		if err != nil {
			writeExpertErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, file)
	}))
	router.PUT("/v1/experts/:expertId", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body expertBody
		if !bindJSON(w, r, &body) {
			return
		}
		expert, err := runtime.UpdateExpert(r.Context(), toPrincipal(p), r.PathValue("expertId"), expertInput(body))
		if err != nil {
			writeExpertErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, expert)
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
			SOPID       string                `json:"sop_id" binding:"required"`
			Version     int                   `json:"version" binding:"min=1"`
			Name        string                `json:"name"`
			Description string                `json:"description"`
			Steps       []taskruntime.SOPStep `json:"steps"`
		}
		if !bindJSON(w, r, &body) {
			return
		}
		sop, err := runtime.Tasks.RegisterSOP(r.Context(), toPrincipal(p), taskruntime.SOP{
			SOPID: body.SOPID, Version: body.Version, Name: body.Name, Description: body.Description, Steps: body.Steps,
		})
		var invalid *taskruntime.InvalidSOPError
		if errors.As(err, &invalid) {
			writeErr(w, http.StatusBadRequest, "INVALID_SOP", invalid.Reason)
			return
		}
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
			Config  *taskConfigBody    `json:"config"`
		}
		if !bindJSON(w, r, &body) {
			return
		}
		input := taskruntime.CreateInput{Title: body.Title, Goal: body.Goal, Mode: body.Mode, Profile: body.Profile, SOP: body.SOP, Budgets: body.Budgets, Policy: body.Policy}
		if body.Config != nil {
			// Resolved before the task exists, so a refusal leaves nothing behind.
			resolved, err := runtime.ResolveTaskConfig(r.Context(), toPrincipal(p), body.Config.request())
			if err != nil {
				writeTaskErr(w, err)
				return
			}
			input.Config = &resolved
		}
		t, err := runtime.Tasks.Create(r.Context(), toPrincipal(p), input)
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
	router.DELETE("/v1/tasks/:taskId", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		if err := runtime.Tasks.Delete(r.Context(), toPrincipal(p), r.PathValue("taskId")); err != nil {
			writeTaskErr(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	router.GET("/v1/tasks/:taskId/config", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		view, err := runtime.Tasks.TaskConfig(r.Context(), toPrincipal(p), r.PathValue("taskId"))
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, view)
	}))
	router.PUT("/v1/tasks/:taskId/config", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body struct {
			taskConfigBody
			CommandID         string `json:"command_id"`
			BaseConfigVersion int    `json:"base_config_version"`
		}
		if !bindJSON(w, r, &body) {
			return
		}
		if body.BaseConfigVersion < 1 {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "base_config_version is required: the version you read")
			return
		}
		resolved, err := runtime.ResolveTaskConfig(r.Context(), toPrincipal(p), body.taskConfigBody.request())
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		result, err := runtime.Tasks.UpdateTaskConfig(r.Context(), toPrincipal(p), r.PathValue("taskId"), commandID(body.CommandID), body.BaseConfigVersion, resolved)
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
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
		// Without a client_message_id the command id stands in for it (10 §1). A random one would change the request
		// hash on every retry of the same command_id and turn a safe retry into a 409.
		clientID := stringValue(body, "client_message_id", "clientMessageId")
		if strings.TrimSpace(clientID) == "" {
			clientID = id
		}
		payload := map[string]any{"command_id": id, "client_message_id": clientID, "text": stringValue(body, "text", "message"), "delivery": stringValue(body, "delivery")}
		if payload["delivery"] == "" {
			payload["delivery"] = "queue"
		}
		mentions, ok := stringList(body["mentions"])
		if !ok || len(mentions) > taskruntime.MaxMentions {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "mentions must be a list of at most 8 roles")
			return
		}
		// Left out of the payload when empty, so a message without mentions hashes as it always did. Only the shape is
		// settled here; the team check runs for a command seen for the first time, so a replay never depends on it.
		mentions = taskruntime.NormalizeMentions(mentions)
		if len(mentions) > 0 {
			payload["mentions"] = mentions
		}
		taskID := r.PathValue("taskId")
		raw, err := runtime.Tasks.UpdateChecked(r.Context(), toPrincipal(p), taskID, "sendMessage", id, payload, func(ctx context.Context) error {
			return runtime.Tasks.CheckMentions(ctx, toPrincipal(p), taskID, mentions)
		})
		if err != nil {
			var refused *taskruntime.MentionError
			if errors.As(err, &refused) {
				writeJSON(w, http.StatusUnprocessableEntity, ErrorBody{
					Error: strings.ToLower(strings.ReplaceAll(refused.Code, "_", " ")), Code: refused.Code, Message: refused.Reason, Field: refused.Field, Reason: refused.Reason,
				})
				return
			}
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
		// What the workflow takes is checked here, where a refusal reaches the caller: the node and the reason must be
		// named, and the profile must be the tenant's own. An unknown one is refused before anything is sent (11 §3).
		toProfile := stringValue(body, "to_profile")
		if !nodeRef.MatchString(stringValue(body, "node_id")) || strings.TrimSpace(stringValue(body, "reason")) == "" {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "node_id and reason are required")
			return
		}
		if err := runtime.CheckProfile(r.Context(), toPrincipal(p), toProfile); err != nil {
			switch {
			case errors.Is(err, app.ErrUnknownProfile):
				writeErr(w, http.StatusUnprocessableEntity, "UNKNOWN_PROFILE", "to_profile is not a profile of this tenant")
			case errors.Is(err, app.ErrInvalid):
				writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "to_profile must be a versioned reference like coder@3")
			default:
				writeTaskErr(w, err)
			}
			return
		}
		id := commandID(stringValue(body, "command_id", "commandId"))
		body["command_id"] = id
		raw, err := runtime.Tasks.Update(r.Context(), toPrincipal(p), r.PathValue("taskId"), "requestProfileSwitch", id, body)
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, resultStatus(raw, "effective_attempt_no"), json.RawMessage(raw))
	}))
	router.POST("/v1/tasks/:taskId/nodes/:nodeId/complete", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body map[string]any
		if !bindJSON(w, r, &body) {
			return
		}
		nodeID := r.PathValue("nodeId")
		if !nodeRef.MatchString(nodeID) {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "the node id is invalid")
			return
		}
		if refused := completeNodeRefusal(r.Context(), runtime, toPrincipal(p), r.PathValue("taskId"), nodeID); refused != nil {
			if refused.err != nil {
				writeTaskErr(w, refused.err)
				return
			}
			writeErr(w, refused.status, refused.code, refused.message)
			return
		}
		id := commandID(stringValue(body, "command_id", "commandId"))
		payload := map[string]any{"command_id": id, "node_id": nodeID, "reason": stringValue(body, "reason")}
		raw, err := runtime.Tasks.Update(r.Context(), toPrincipal(p), r.PathValue("taskId"), "completeNode", id, payload)
		if err != nil {
			writeTaskErr(w, err)
			return
		}
		writeJSON(w, resultStatus(raw, "node_id"), json.RawMessage(raw))
	}))
	router.POST("/v1/tasks/:taskId/approvals/:approvalId", authed(func(w http.ResponseWriter, r *http.Request, p app.Principal) {
		var body map[string]any
		if !bindJSON(w, r, &body) {
			return
		}
		id := commandID(stringValue(body, "command_id", "commandId"))
		// "Always": also allow what the approval offered for the rest of the task.
		always, _ := body["always"].(bool)
		payload := map[string]any{"command_id": id, "approval_id": r.PathValue("approvalId"), "decision": stringValue(body, "decision"), "comment": stringValue(body, "comment"), "always": always}
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

// resultStatus is 200 when the update's answer carries its result (the field the result always has), and 202 when the
// update was only accepted. It reads the stored body, so a replay answers with the status of the first response.
func resultStatus(raw json.RawMessage, resultField string) int {
	var body map[string]json.RawMessage
	if json.Unmarshal(raw, &body) == nil {
		if _, ok := body[resultField]; ok {
			return http.StatusOK
		}
	}
	return http.StatusAccepted
}

// nodeRef is the id of a node as the workflow mints it: n_ and 26 characters of Crockford base32.
var nodeRef = regexp.MustCompile(`^n_[0-9A-HJKMNP-TV-Z]{26}$`)

// held are the task statuses in which a person may complete a node by hand: the agents are not working.
var held = map[string]bool{"TAKEN_OVER": true, "PAUSED": true, "PAUSED_NEEDS_REVIEW": true}

type refusal struct {
	status  int
	code    string
	message string
	err     error
}

// completeNodeRefusal says why the node cannot be completed by hand now, or nil. The workflow's validator is the judge and
// refuses the same things; its refusal reaches an API caller that waits only for the update to be accepted as a 202, so the
// obvious cases are answered here. A node that is already complete is let through: it is how a repeat of a command that was
// applied is answered from the command ledger instead of refused.
func completeNodeRefusal(ctx context.Context, runtime *app.App, p taskruntime.Principal, taskID, nodeID string) *refusal {
	task, err := runtime.Tasks.Get(ctx, p, taskID)
	if err != nil {
		return &refusal{err: err}
	}
	plan, err := runtime.Tasks.Plan(ctx, p, taskID)
	if err != nil {
		return &refusal{err: err}
	}
	if len(plan.Nodes) > 0 {
		var node map[string]any
		for _, candidate := range plan.Nodes {
			if candidate["node_id"] == nodeID {
				node = candidate
			}
		}
		if node == nil {
			return &refusal{status: http.StatusNotFound, code: "NOT_FOUND", message: "node not found"}
		}
		status, _ := node["status"].(string)
		if frozen, _ := node["frozen"].(bool); frozen || status == "COMPLETED" || status == "SKIPPED" {
			return nil
		}
		if status == "RUNNING" || status == "VERIFYING" {
			return &refusal{status: http.StatusConflict, code: "NODE_RUNNING", message: "an attempt is running on this node; take the task over first"}
		}
	}
	if !held[task.Status] {
		return &refusal{status: http.StatusConflict, code: "INVALID_TRANSITION", message: "pause or take over the task before completing a node"}
	}
	return nil
}

func stringValue(body map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := body[key].(string); ok {
			return value
		}
	}
	return ""
}

// taskConfigBody is the configuration as a request states it. A list that is absent or null keeps the expert's
// defaults; an empty one removes them, which encoding/json keeps apart (nil against an empty slice).
type taskConfigBody struct {
	Expert  string `json:"expert"`
	TeamRef string `json:"team_ref"`
	// Model is a pointer so an update can tell "clear the model" (an explicit "") from "absent".
	Model        *string  `json:"model"`
	Skills       []string `json:"skills"`
	ConnectorIDs []string `json:"connector_ids"`
	Mode         string   `json:"mode"`
}

func (b taskConfigBody) request() app.TaskConfigRequest {
	req := app.TaskConfigRequest{Expert: b.Expert, TeamRef: b.TeamRef, Skills: b.Skills, ConnectorIDs: b.ConnectorIDs, Mode: b.Mode}
	if b.Model != nil {
		req.Model = *b.Model
	}
	return req
}

// stringList reads an optional JSON list of strings. Absent or null is an empty list; anything else is not ok.
func stringList(value any) ([]string, bool) {
	if value == nil {
		return nil, true
	}
	items, ok := value.([]any)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok {
			return nil, false
		}
		out = append(out, text)
	}
	return out, true
}

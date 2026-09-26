package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/internalauth"
	"github.com/mindreon/orbit-control/internal/orch"
	"github.com/mindreon/orbit-control/internal/worker"
)

// ErrorBody is the JSON error shape. Keep 401/403 examples in docs/openapi.yaml
// stable for Sentinel (those statuses are not emitted in W1).
type ErrorBody struct {
	Error   string `json:"error"`
	Code    string `json:"code"`
	Message string `json:"message"`
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

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "content-type, authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Handlers builds the process's public and internal muxes over one App.
// Worker URL: ORBIT_WORKER_URL (default http://127.0.0.1:8090).
// When TEMPORAL_ADDRESS is set, rooms are driven through RoomWorkflow.
func Handlers() (public, internal http.Handler) {
	base := os.Getenv("ORBIT_WORKER_URL")
	if base == "" {
		base = "http://127.0.0.1:8090"
	}
	w := worker.New(base)
	runtime := app.New(w)
	if addr := os.Getenv("TEMPORAL_ADDRESS"); addr != "" {
		oc, err := dialOrch(addr, os.Getenv("TEMPORAL_NAMESPACE"), os.Getenv("TEMPORAL_TASK_QUEUE"))
		if err != nil {
			panic("temporal dial: " + err.Error())
		}
		runtime = app.NewWithOrch(w, oc)
	}
	runtime.Limits = app.LimitsFromEnv()
	return HandlerWith(runtime), InternalHandler(runtime)
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

func HandlerWith(runtime *app.App) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, HealthBody{Status: "ok"})
	})
	mux.HandleFunc("GET /v1/rooms", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"items": runtime.ListRooms()})
	})
	mux.HandleFunc("POST /v1/rooms", func(w http.ResponseWriter, r *http.Request) {
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
		room, err := runtime.CreateRoom(r.Context(), app.CreateRoomInput{
			Kind:             body.Kind,
			Title:            body.Title,
			PermissionPreset: body.PermissionPreset,
			PersonaID:        body.PersonaID,
			GrantID:          body.GrantID,
		})
		if err != nil {
			if room == nil {
				writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
				return
			}
			writeErr(w, http.StatusBadGateway, "WORKER_ERROR", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, room)
	})
	mux.HandleFunc("GET /v1/rooms/{roomId}", func(w http.ResponseWriter, r *http.Request) {
		room, ok := runtime.GetRoom(r.PathValue("roomId"))
		if !ok {
			writeErr(w, http.StatusNotFound, "NOT_FOUND", "room not found")
			return
		}
		writeJSON(w, http.StatusOK, room)
	})
	mux.HandleFunc("GET /v1/rooms/{roomId}/messages", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"items": runtime.ListMessages(r.PathValue("roomId"))})
	})
	mux.HandleFunc("POST /v1/rooms/{roomId}/messages", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Message string `json:"message"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		room, approval, err := runtime.PostMessage(r.Context(), r.PathValue("roomId"), body.Message)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"room": room, "approval": approval})
	})
	mux.HandleFunc("POST /v1/rooms/{roomId}/abort", func(w http.ResponseWriter, r *http.Request) {
		if err := runtime.AbortRoom(r.Context(), r.PathValue("roomId")); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"aborted": true})
	})
	mux.HandleFunc("POST /v1/rooms/{roomId}/steer", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Instruction string `json:"instruction"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
			return
		}
		accepted, err := runtime.SteerRoom(r.Context(), r.PathValue("roomId"), body.Instruction)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"accepted": accepted})
	})
	mux.HandleFunc("GET /v1/rooms/{roomId}/activity", func(w http.ResponseWriter, r *http.Request) {
		roomID := r.PathValue("roomId")
		if _, ok := runtime.GetRoom(roomID); !ok {
			writeErr(w, http.StatusNotFound, "NOT_FOUND", "room not found")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": runtime.ListActivity(roomID)})
	})
	mux.HandleFunc("GET /v1/rooms/{roomId}/events", streamRoomEvents(runtime))
	mux.HandleFunc("GET /v1/approvals", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"items": runtime.ListApprovals()})
	})
	mux.HandleFunc("POST /v1/approvals/{approvalId}/decide", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Decision string `json:"decision"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		appr, err := runtime.Decide(r.Context(), r.PathValue("approvalId"), body.Decision)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, appr)
	})
	// Internal routes live only on InternalHandler's listener.
	mux.HandleFunc("/internal/", func(w http.ResponseWriter, _ *http.Request) {
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "not found")
	})
	mux.HandleFunc("GET /v1/personas", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"items": runtime.ListPersonas()})
	})
	mux.HandleFunc("POST /v1/personas", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Name         string   `json:"name"`
			Instructions string   `json:"instructions"`
			McpIds       []string `json:"mcpConnectorIds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
			return
		}
		persona, err := runtime.CreatePersona(body.Name, body.Instructions, body.McpIds)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, persona)
	})
	mux.HandleFunc("GET /v1/mcp-connectors", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"items": runtime.ListMcpConnectors()})
	})
	mux.HandleFunc("POST /v1/mcp-connectors", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Name     string   `json:"name"`
			Command  string   `json:"command"`
			Args     []string `json:"args"`
			EnvRefs  []string `json:"envRefs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
			return
		}
		connector, err := runtime.CreateMcpConnector(body.Name, body.Command, body.Args, body.EnvRefs)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, connector)
	})
	mux.HandleFunc("POST /v1/grants", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Env       map[string]string `json:"env"`
			TTLSeconds int              `json:"ttlSeconds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", "request body must be valid JSON")
			return
		}
		grant, err := runtime.MintGrant(body.Env, body.TTLSeconds)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
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
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
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
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		if err := runtime.Ingest(raw); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"ok": true})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeErr(w, http.StatusNotFound, "NOT_FOUND", "not found")
	})
	return mux
}

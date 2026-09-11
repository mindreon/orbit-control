package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/mindreon/orbit-control/internal/app"
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
		w.Header().Set("Access-Control-Allow-Headers", "content-type")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Handler is the default process mux.
// Worker URL: ORBIT_WORKER_URL (default http://127.0.0.1:8090).
// When TEMPORAL_ADDRESS is set, rooms are driven through RoomWorkflow.
func Handler() http.Handler {
	base := os.Getenv("ORBIT_WORKER_URL")
	if base == "" {
		base = "http://127.0.0.1:8090"
	}
	w := worker.New(base)
	if addr := os.Getenv("TEMPORAL_ADDRESS"); addr != "" {
		oc, err := orch.Dial(addr, os.Getenv("TEMPORAL_NAMESPACE"), os.Getenv("TEMPORAL_TASK_QUEUE"))
		if err != nil {
			panic("temporal dial: " + err.Error())
		}
		return HandlerWith(app.NewWithOrch(w, oc))
	}
	return HandlerWith(app.New(w))
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
			Kind  string `json:"kind"`
			Title string `json:"title"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		room, err := runtime.CreateRoom(r.Context(), body.Kind, body.Title)
		if err != nil {
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
	mux.HandleFunc("GET /v1/rooms/{roomId}/events", func(w http.ResponseWriter, r *http.Request) {
		roomID := r.PathValue("roomId")
		flusher, ok := w.(http.Flusher)
		if !ok {
			writeErr(w, http.StatusInternalServerError, "SSE_UNSUPPORTED", "streaming unsupported")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		ch, cancel := runtime.Subscribe(roomID)
		defer cancel()
		_, _ = io.WriteString(w, ": connected\n\n")
		flusher.Flush()
		for {
			select {
			case <-r.Context().Done():
				return
			case raw, ok := <-ch:
				if !ok {
					return
				}
				_, _ = io.WriteString(w, "data: "+string(raw)+"\n\n")
				flusher.Flush()
			}
		}
	})
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
	mux.HandleFunc("POST /internal/events", func(w http.ResponseWriter, r *http.Request) {
		var ev app.Event
		if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
			writeErr(w, http.StatusBadRequest, "BAD_REQUEST", err.Error())
			return
		}
		runtime.Ingest(ev)
		writeJSON(w, http.StatusAccepted, map[string]any{"ok": true})
	})
	mux.HandleFunc("GET /v1/personas", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"items": []any{}})
	})
	mux.HandleFunc("GET /v1/secrets", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"items": []any{}})
	})
	mux.HandleFunc("GET /v1/cloud-agents", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"items": []any{}})
	})
	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, _ *http.Request) {
		writeErr(w, http.StatusNotImplemented, "NOT_IMPLEMENTED", "use GET /v1/rooms/{roomId}/events (SSE) in W1")
	})
	return cors(mux)
}

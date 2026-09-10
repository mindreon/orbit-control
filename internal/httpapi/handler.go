package httpapi

import (
	"encoding/json"
	"net/http"
)

// ErrorBody is the W0 JSON error shape. Keep in sync with docs/openapi.yaml
// components.schemas.ErrorBody (including 401/403 stubs for Sentinel).
type ErrorBody struct {
	Error   string `json:"error"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

const notImplementedMessage = "W0 skeleton: this endpoint is not implemented"

// Handler returns a mux with no database, auth, Temporal, or LLM.
// Every route, including GET /health, responds 501 Not Implemented.
func Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", NotImplemented)
	mux.HandleFunc("/", NotImplemented)
	return mux
}

// NotImplemented writes the shared 501 stub body. Safe to call without a DB.
func NotImplemented(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotImplemented)
	_ = json.NewEncoder(w).Encode(ErrorBody{
		Error:   "not implemented",
		Code:    "NOT_IMPLEMENTED",
		Message: notImplementedMessage,
	})
}

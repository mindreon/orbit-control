package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthReturns501WithoutDB(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)

	Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("GET /health status = %d, want %d", rec.Code, http.StatusNotImplemented)
	}

	var body ErrorBody
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Code != "NOT_IMPLEMENTED" {
		t.Fatalf("code = %q, want NOT_IMPLEMENTED", body.Code)
	}
	if body.Error != "not implemented" {
		t.Fatalf("error = %q, want not implemented", body.Error)
	}
}

func TestStubAPIPathsReturn501(t *testing.T) {
	paths := []string{
		"/v1/rooms",
		"/v1/approvals",
		"/v1/personas",
		"/v1/secrets",
		"/v1/cloud-agents",
		"/ws",
	}
	for _, path := range paths {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusNotImplemented {
			t.Fatalf("GET %s status = %d, want %d", path, rec.Code, http.StatusNotImplemented)
		}
	}
}

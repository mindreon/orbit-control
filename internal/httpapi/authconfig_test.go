package httpapi

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/mindreon/orbit-control/internal/platform/config"
)

func TestAuthConfigIAM(t *testing.T) {
	h := &Handler{Config: config.Config{AuthMode: "iam", IAMBaseURL: "https://iam.example/iam/api/v1", Issuer: "https://idp.example"}}
	w := httptest.NewRecorder()
	if err := h.authConfig(w, httptest.NewRequest("GET", "/v1/auth/config", nil)); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got["mode"] != "iam" {
		t.Fatalf("iam mode must return only {mode:iam}, got %s", w.Body.String())
	}
}

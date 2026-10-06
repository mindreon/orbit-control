package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/store"
)

// The files of an expert version are read through /v1/experts/{id}/files, and its bundle skills are served to the worker
// through /internal/experts/{id}/{version}/skills/{name}.
//
// How it can go wrong, written down before the code:
//   - the worker endpoint answers without the internal token, or for a tenant that does not own the expert;
//   - a file path with ".." or an unknown version reads something else;
//   - the soul and the unbound servers are missing from the expert's JSON.

func TestExpertFilesAndWorkerSkillEndpoint(t *testing.T) {
	runtime := app.NewWithOptions(app.Options{})
	h := HandlerWith(runtime)
	do := func(method, path, body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, internalReq(method, path, body))
		return rec
	}
	created := do(http.MethodPost, "/v1/experts", `{"name":"Writer","instructions":"write","soul":"kind"}`)
	if created.Code != http.StatusCreated || !strings.Contains(created.Body.String(), `"soul":"kind"`) || !strings.Contains(created.Body.String(), `"mcp_unbound":[]`) {
		t.Fatalf("create = %d: %s", created.Code, created.Body.String())
	}
	var expert struct {
		ID string `json:"expert_id"`
	}
	_ = json.Unmarshal(created.Body.Bytes(), &expert)

	list := do(http.MethodGet, "/v1/experts/"+expert.ID+"/files", "")
	var listed struct {
		Version int `json:"version"`
		Files   []struct {
			Path, SHA256 string
			Size         int
		} `json:"files"`
	}
	_ = json.Unmarshal(list.Body.Bytes(), &listed)
	if list.Code != http.StatusOK || listed.Version != 1 || len(listed.Files) != 3 || strings.Contains(list.Body.String(), "synthesized") {
		t.Fatalf("files = %d: %s", list.Code, list.Body.String())
	}
	file := do(http.MethodGet, "/v1/experts/"+expert.ID+"/files/SOUL.md?version=1", "")
	if file.Code != http.StatusOK || !strings.Contains(file.Body.String(), `"content":"kind"`) || !strings.Contains(file.Body.String(), `"path":"SOUL.md"`) {
		t.Fatalf("file = %d: %s", file.Code, file.Body.String())
	}
	for path, want := range map[string]int{
		"/v1/experts/" + expert.ID + "/files/nope.md":      http.StatusNotFound,
		"/v1/experts/" + expert.ID + "/files/../AGENTS.md": http.StatusNotFound,
		"/v1/experts/" + expert.ID + "/files?version=9":    http.StatusNotFound,
		"/v1/experts/" + expert.ID + "/files?version=zero": http.StatusBadRequest,
		"/v1/experts/expert_none/files":                    http.StatusNotFound,
	} {
		if got := do(http.MethodGet, path, "").Code; got != want {
			t.Errorf("%s = %d, want %d", path, got, want)
		}
	}
	empty := do(http.MethodPost, "/v1/experts", `{"name":"Writer","instructions":" "}`)
	if empty.Code != http.StatusBadRequest || !strings.Contains(empty.Body.String(), "AGENTS_MD_REQUIRED") {
		t.Errorf("empty instructions = %d: %s", empty.Code, empty.Body.String())
	}

	// The worker endpoint: no token configured means loopback only.
	if err := runtime.Repo.ReplaceAgents(context.Background(), "default", []store.AgentRecord{{
		ID: "h/a", Handle: "h", Slug: "a", Name: "Imported", FilesKnown: true,
		Files: []store.SkillFile{{Path: "AGENTS.md", Body: "do"}, {Path: "skills/pdf/SKILL.md", Body: "---\nname: pdf\n---\nx"}, {Path: "skills/pdf/t.md", Body: "t"}},
	}}); err != nil {
		t.Fatal(err)
	}
	from := do(http.MethodPost, "/v1/experts/from-agent", `{"handle":"h","slug":"a"}`)
	var imported struct {
		Expert struct {
			ID string `json:"expert_id"`
		} `json:"expert"`
		McpUnbound   []any `json:"mcp_unbound"`
		SkippedFiles []any `json:"skipped_files"`
	}
	_ = json.Unmarshal(from.Body.Bytes(), &imported)
	if from.Code != http.StatusCreated || imported.McpUnbound == nil || imported.SkippedFiles == nil {
		t.Fatalf("from-agent = %d: %s", from.Code, from.Body.String())
	}
	ih := InternalHandler(runtime)
	get := func(remote bool, path string) *httptest.ResponseRecorder {
		req := internalReq(http.MethodGet, path, "")
		if remote {
			req.RemoteAddr = "203.0.113.7:9"
		}
		rec := httptest.NewRecorder()
		ih.ServeHTTP(rec, req)
		return rec
	}
	base := "/internal/experts/" + imported.Expert.ID + "/1/skills/pdf"
	ok := get(false, base+"?tenant_id=default")
	if ok.Code != http.StatusOK || !strings.Contains(ok.Body.String(), `"name":"pdf"`) || !strings.Contains(ok.Body.String(), `"path":"SKILL.md"`) || !strings.Contains(ok.Body.String(), `"body":"t"`) {
		t.Fatalf("bundle skill = %d: %s", ok.Code, ok.Body.String())
	}
	if got := get(true, base+"?tenant_id=default").Code; got != http.StatusUnauthorized {
		t.Errorf("a remote caller = %d", got)
	}
	for _, path := range []string{
		base,                      // no tenant
		base + "?tenant_id=other", // another tenant's expert
		"/internal/experts/" + imported.Expert.ID + "/2/skills/pdf?tenant_id=default",
		"/internal/experts/" + imported.Expert.ID + "/abc/skills/pdf?tenant_id=default",
		"/internal/experts/" + imported.Expert.ID + "/1/skills/nope?tenant_id=default",
	} {
		if got := get(false, path).Code; got != http.StatusNotFound {
			t.Errorf("%s = %d", path, got)
		}
	}
}

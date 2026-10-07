package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/app"
)

func call(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, internalReq(method, path, body))
	return rec
}

func TestSettingsRoutesDefaultsRoundTripAndValidation(t *testing.T) {
	h := HandlerWith(app.NewWithOptions(app.Options{}))
	const defaults = `{"permissions":{"default_preset":"default","custom":{"write_scope":"workspace","auto_edits":true,"auto_commands":false,"auto_builtin":false}}}`
	if rec := call(h, http.MethodGet, "/v1/settings", ""); rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != defaults {
		t.Fatalf("defaults = %d %s", rec.Code, rec.Body.String())
	}
	for _, body := range []string{
		`{"permissions":{"default_preset":"yolo"}}`,
		`{"permissions":{"custom":{"write_scope":"everywhere"}}}`,
		`not json`,
	} {
		if rec := call(h, http.MethodPut, "/v1/settings", body); rec.Code != http.StatusBadRequest {
			t.Errorf("PUT %s = %d %s", body, rec.Code, rec.Body.String())
		}
	}
	const saved = `{"permissions":{"default_preset":"custom","custom":{"write_scope":"none","auto_edits":false,"auto_commands":true,"auto_builtin":true}}}`
	if rec := call(h, http.MethodPut, "/v1/settings", saved); rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != saved {
		t.Fatalf("PUT = %d %s", rec.Code, rec.Body.String())
	}
	if rec := call(h, http.MethodGet, "/v1/settings", ""); strings.TrimSpace(rec.Body.String()) != saved {
		t.Fatalf("GET after PUT = %s", rec.Body.String())
	}
}

func TestTaskPermissionsAreResolvedFromSettingsAndValidated(t *testing.T) {
	h := HandlerWith(app.NewWithOptions(app.Options{}))
	config := func(id string) string {
		rec := call(h, http.MethodGet, "/v1/tasks/"+id+"/config", "")
		var view struct {
			Permissions map[string]any `json:"permissions"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &view); err != nil {
			t.Fatal(err)
		}
		out, _ := json.Marshal(view.Permissions)
		return string(out)
	}
	create := func(body string) (int, string) {
		rec := call(h, http.MethodPost, "/v1/tasks", body)
		var task struct {
			ID string `json:"task_id"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &task)
		return rec.Code, task.ID
	}

	if code, id := create(`{"title":"a","goal":"g"}`); code != http.StatusCreated || config(id) != `{"preset":"default"}` {
		t.Errorf("no config, no settings = %d %s", code, config(id))
	}
	if code, _ := create(`{"title":"a","goal":"g","config":{"permissions":{"preset":"yolo"}}}`); code != http.StatusBadRequest {
		t.Errorf("unknown preset on create = %d", code)
	}
	call(h, http.MethodPut, "/v1/settings", `{"permissions":{"default_preset":"auto","custom":{"write_scope":"none","auto_commands":true}}}`)
	code, id := create(`{"title":"a","goal":"g"}`)
	if code != http.StatusCreated || config(id) != `{"preset":"auto"}` {
		t.Errorf("no config takes the default preset = %d %s", code, config(id))
	}
	if _, id := create(`{"title":"a","goal":"g","config":{"permissions":null}}`); config(id) != `{"preset":"auto"}` {
		t.Errorf("null permissions take the default preset: %s", config(id))
	}
	_, id = create(`{"title":"a","goal":"g","config":{"permissions":{"preset":"custom","auto_builtin":true}}}`)
	if got := config(id); got != `{"auto_builtin":true,"auto_commands":true,"auto_edits":true,"preset":"custom","write_scope":"none"}` {
		t.Errorf("custom = %s", got)
	}

	if rec := call(h, http.MethodPut, "/v1/tasks/"+id+"/config", `{"base_config_version":1,"permissions":{"preset":"full"}}`); rec.Code != http.StatusOK {
		t.Fatalf("update = %d %s", rec.Code, rec.Body.String())
	}
	if got := config(id); got != `{"preset":"full"}` {
		t.Errorf("after update = %s", got)
	}
	rec := call(h, http.MethodPut, "/v1/tasks/"+id+"/config", `{"base_config_version":2,"permissions":{"preset":"custom","write_scope":"x"}}`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"field":"permissions.write_scope"`) {
		t.Errorf("bad write_scope on update = %d %s", rec.Code, rec.Body.String())
	}
}

// A client that only changes something else (here the mode) must not change how much the agent may do, even when the
// user's own default has moved since; only an explicit permissions value does.
func TestConfigUpdateWithoutPermissionsKeepsTheCurrentOnes(t *testing.T) {
	h := HandlerWith(app.NewWithOptions(app.Options{}))
	call(h, http.MethodPut, "/v1/settings", `{"permissions":{"default_preset":"auto","custom":{"write_scope":"none","auto_commands":true}}}`)
	rec := call(h, http.MethodPost, "/v1/tasks", `{"title":"a","goal":"g","config":{"permissions":{"preset":"custom","auto_builtin":true}}}`)
	var task struct {
		ID string `json:"task_id"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &task)
	path := "/v1/tasks/" + task.ID + "/config"
	permissions := func() string {
		var view struct {
			Version     int            `json:"config_version"`
			Mode        string         `json:"mode"`
			Permissions map[string]any `json:"permissions"`
		}
		_ = json.Unmarshal(call(h, http.MethodGet, path, "").Body.Bytes(), &view)
		out, _ := json.Marshal(view.Permissions)
		return string(out)
	}
	const custom = `{"auto_builtin":true,"auto_commands":true,"auto_edits":true,"preset":"custom","write_scope":"none"}`
	if got := permissions(); got != custom {
		t.Fatalf("created = %s", got)
	}

	// The user's default moves on; a config update without permissions still keeps the task's custom spec.
	call(h, http.MethodPut, "/v1/settings", `{"permissions":{"default_preset":"full"}}`)
	if rec := call(h, http.MethodPut, path, `{"base_config_version":1,"mode":"plan"}`); rec.Code != http.StatusOK {
		t.Fatalf("update = %d %s", rec.Code, rec.Body.String())
	}
	if got := permissions(); got != custom {
		t.Errorf("absent permissions on update = %s, want the current custom spec", got)
	}
	if rec := call(h, http.MethodPut, path, `{"base_config_version":2,"mode":"ask","permissions":null}`); rec.Code != http.StatusOK {
		t.Fatalf("update with null = %d %s", rec.Code, rec.Body.String())
	}
	if got := permissions(); got != custom {
		t.Errorf("null permissions on update = %s, want the current custom spec", got)
	}

	// An explicit value changes it; {preset:"custom"} takes the user's custom rules (the PUT above reset them to the defaults).
	if rec := call(h, http.MethodPut, path, `{"base_config_version":3,"permissions":{"preset":"request"}}`); rec.Code != http.StatusOK {
		t.Fatalf("explicit update = %d %s", rec.Code, rec.Body.String())
	}
	if got := permissions(); got != `{"preset":"request"}` {
		t.Errorf("explicit preset = %s", got)
	}
	if rec := call(h, http.MethodPut, path, `{"base_config_version":4,"model":"glm-5"}`); rec.Code != http.StatusOK {
		t.Fatalf("model-only update = %d %s", rec.Code, rec.Body.String())
	}
	if got := permissions(); got != `{"preset":"request"}` {
		t.Errorf("a model-only update changed the preset: %s", got)
	}
	if rec := call(h, http.MethodPut, path, `{"base_config_version":5,"permissions":{"preset":"custom"}}`); rec.Code != http.StatusOK {
		t.Fatalf("custom update = %d %s", rec.Code, rec.Body.String())
	}
	if got := permissions(); got != `{"auto_builtin":false,"auto_commands":false,"auto_edits":true,"preset":"custom","write_scope":"workspace"}` {
		t.Errorf("custom takes the user's rules (the settings were replaced, so these are the defaults): %s", got)
	}
}

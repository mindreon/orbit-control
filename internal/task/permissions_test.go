package task

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	v3 "github.com/mindreon/orbit-control/internal/contract/v3"
)

func strp(s string) *string { return &s }
func boolp(b bool) *bool    { return &b }

// What goes to the workflow decodes strictly into the contract's PermissionSpec, a spec without fields sends only its
// preset, and no spec at all sends no key (absent is the default preset).
func TestWorkflowConfigCarriesPermissions(t *testing.T) {
	in := ConfigInput{Mode: "default", Permissions: &Permissions{Preset: PresetCustom, WriteScope: strp("none"),
		AutoEdits: boolp(false), AutoCommands: boolp(true), AutoBuiltin: boolp(false)}}
	raw, _ := json.Marshal(in.workflowConfig(1))
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var cfg v3.TaskConfig
	if err := dec.Decode(&cfg); err != nil {
		t.Fatalf("TaskConfig does not accept what control sends: %v\n%s", err, raw)
	}
	got := cfg.Permissions
	if got == nil || got.Preset != "custom" || *got.WriteScope != "none" || *got.AutoEdits || !*got.AutoCommands || *got.AutoBuiltin {
		t.Errorf("permissions: %+v", got)
	}

	alone := ConfigInput{Mode: "default", Permissions: &Permissions{Preset: PresetFull}}.workflowConfig(1)
	if !reflect.DeepEqual(alone["permissions"], map[string]any{"preset": "full"}) {
		t.Errorf("a preset is sent alone: %#v", alone["permissions"])
	}
	if _, has := (ConfigInput{Mode: "default"}).workflowConfig(1)["permissions"]; has {
		t.Error("no permissions sends no key")
	}
}

func TestConfigViewReadsPermissionsBackWithTheDefaultForNone(t *testing.T) {
	custom := &Permissions{Preset: PresetCustom, WriteScope: strp("workspace"), AutoEdits: boolp(true), AutoCommands: boolp(false), AutoBuiltin: boolp(true)}
	in := ConfigInput{Mode: "default", Permissions: custom}
	view := configFromWorkflow(map[string]any{"config_version": float64(2), "permissions": in.workflowConfig(2)["permissions"]})
	if !reflect.DeepEqual(view.Permissions, custom) {
		t.Errorf("round trip through the workflow's JSON: %+v", view.Permissions)
	}
	raw, _ := json.Marshal(in.workflowConfig(2))
	var decoded map[string]any
	_ = json.Unmarshal(raw, &decoded)
	if view := configFromWorkflow(decoded); !reflect.DeepEqual(view.Permissions, custom) {
		t.Errorf("decoded JSON: %+v", view.Permissions)
	}
	for name, got := range map[string]ConfigView{
		"workflow without permissions": configFromWorkflow(map[string]any{"config_version": float64(1)}),
		"input without permissions":    ConfigInput{Mode: "default"}.view(1),
	} {
		if got.Permissions == nil || got.Permissions.Preset != PresetDefault {
			t.Errorf("%s reads as the default preset: %+v", name, got.Permissions)
		}
	}
}

func TestLocalConfigKeepsPermissions(t *testing.T) {
	s := New(nil)
	p := Principal{TenantID: "t", UserID: "u"}
	task, err := s.Create(context.Background(), p, CreateInput{Title: "t", Goal: "g"})
	if err != nil {
		t.Fatal(err)
	}
	spec := &Permissions{Preset: PresetAuto}
	if _, err := s.UpdateTaskConfig(context.Background(), p, task.ID, "cmd-1", 1, ConfigInput{Permissions: spec}); err != nil {
		t.Fatal(err)
	}
	view, err := s.TaskConfig(context.Background(), p, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if view.Version != 2 || view.Permissions == nil || view.Permissions.Preset != PresetAuto {
		t.Errorf("view: %+v", view)
	}
}

func TestUserSettingsDefaultsAndPerUserReplace(t *testing.T) {
	s := New(nil)
	ctx := context.Background()
	alice := Principal{TenantID: "t", UserID: "alice"}
	got, err := s.UserSettings(ctx, alice)
	if err != nil || !reflect.DeepEqual(got, DefaultUserSettings()) {
		t.Fatalf("never saved: %+v %v", got, err)
	}
	want := DefaultCustomPermissions()
	if want.WriteScope != "workspace" || !want.AutoEdits || want.AutoCommands || want.AutoBuiltin {
		t.Errorf("custom defaults: %+v", want)
	}
	saved := UserSettings{Permissions: PermissionSettings{DefaultPreset: PresetAuto, Custom: CustomPermissions{WriteScope: "none", AutoCommands: true}}}
	if err := s.SetUserSettings(ctx, alice, saved); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.UserSettings(ctx, alice); !reflect.DeepEqual(got, saved) {
		t.Errorf("read back: %+v", got)
	}
	for _, other := range []Principal{{TenantID: "t", UserID: "bob"}, {TenantID: "u", UserID: "alice"}} {
		if got, _ := s.UserSettings(ctx, other); !reflect.DeepEqual(got, DefaultUserSettings()) {
			t.Errorf("%+v sees another user's settings: %+v", other, got)
		}
	}
}

package app

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/mindreon/orbit-control/internal/store/memstore"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// A task's permissions are resolved by control so the worker never reads the user's settings.
//
// How it can go wrong, written down before the code:
//   - an unknown preset or write scope is stored, or answered without saying which field is wrong;
//   - a request without permissions ignores the user's default preset, or a user with no settings gets anything but the
//     default preset;
//   - "custom" without rules does not take the user's rules, or a field the request states loses to the user's;
//   - a preset other than custom keeps rule fields the worker would ignore;
//   - a user's settings leak to another user, or a refused save changes them.

func permsApp() *App { return &App{Repo: memstore.New(), Tasks: taskruntime.New(nil)} }

var permsUser = taskruntime.Principal{TenantID: "t", UserID: "u"}

func ptr[T any](v T) *T { return &v }

func saveSettings(t *testing.T, a *App, req UserSettingsRequest) {
	t.Helper()
	if _, err := a.SaveUserSettings(context.Background(), permsUser, req); err != nil {
		t.Fatalf("save settings: %v", err)
	}
}

func settingsReq(preset, scope string, edits, commands, builtin *bool) UserSettingsRequest {
	var req UserSettingsRequest
	req.Permissions = &struct {
		DefaultPreset string `json:"default_preset"`
		Custom        *struct {
			WriteScope   string `json:"write_scope"`
			AutoEdits    *bool  `json:"auto_edits"`
			AutoCommands *bool  `json:"auto_commands"`
			AutoBuiltin  *bool  `json:"auto_builtin"`
		} `json:"custom"`
	}{DefaultPreset: preset, Custom: &struct {
		WriteScope   string `json:"write_scope"`
		AutoEdits    *bool  `json:"auto_edits"`
		AutoCommands *bool  `json:"auto_commands"`
		AutoBuiltin  *bool  `json:"auto_builtin"`
	}{WriteScope: scope, AutoEdits: edits, AutoCommands: commands, AutoBuiltin: builtin}}
	return req
}

func TestResolvePermissionsWithoutSettings(t *testing.T) {
	a := permsApp()
	for name, requested := range map[string]*taskruntime.Permissions{
		"absent":            nil,
		"default":           {Preset: "default"},
		"default with junk": {Preset: "default", AutoEdits: ptr(false)},
	} {
		got, err := a.ResolvePermissions(context.Background(), permsUser, requested)
		if err != nil || !reflect.DeepEqual(got, &taskruntime.Permissions{Preset: "default"}) {
			t.Errorf("%s: %+v %v", name, got, err)
		}
	}
	got, err := a.ResolvePermissions(context.Background(), permsUser, &taskruntime.Permissions{Preset: "custom"})
	want := &taskruntime.Permissions{Preset: "custom", WriteScope: ptr("workspace"), AutoEdits: ptr(true), AutoCommands: ptr(false), AutoBuiltin: ptr(false)}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("custom with no settings takes the default rules: %+v %v", got, err)
	}
}

func TestResolvePermissionsFromTheUsersSettings(t *testing.T) {
	a := permsApp()
	saveSettings(t, a, settingsReq("auto", "none", ptr(false), ptr(true), ptr(true)))
	ctx := context.Background()

	got, _ := a.ResolvePermissions(ctx, permsUser, nil)
	if !reflect.DeepEqual(got, &taskruntime.Permissions{Preset: "auto"}) {
		t.Errorf("absent takes the default preset, alone: %+v", got)
	}
	if got, _ = a.ResolvePermissions(ctx, permsUser, &taskruntime.Permissions{Preset: "request"}); !reflect.DeepEqual(got, &taskruntime.Permissions{Preset: "request"}) {
		t.Errorf("an explicit preset wins: %+v", got)
	}
	got, _ = a.ResolvePermissions(ctx, permsUser, &taskruntime.Permissions{Preset: "custom"})
	want := &taskruntime.Permissions{Preset: "custom", WriteScope: ptr("none"), AutoEdits: ptr(false), AutoCommands: ptr(true), AutoBuiltin: ptr(true)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("custom takes the user's rules: %+v", got)
	}
	got, _ = a.ResolvePermissions(ctx, permsUser, &taskruntime.Permissions{Preset: "custom", WriteScope: ptr("workspace"), AutoCommands: ptr(false)})
	want = &taskruntime.Permissions{Preset: "custom", WriteScope: ptr("workspace"), AutoEdits: ptr(false), AutoCommands: ptr(false), AutoBuiltin: ptr(true)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("stated fields win, the rest is the user's: %+v", got)
	}

	saveSettings(t, a, settingsReq("custom", "workspace", ptr(true), nil, nil))
	got, _ = a.ResolvePermissions(ctx, permsUser, nil)
	want = &taskruntime.Permissions{Preset: "custom", WriteScope: ptr("workspace"), AutoEdits: ptr(true), AutoCommands: ptr(false), AutoBuiltin: ptr(false)}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("a custom default preset brings the user's rules: %+v", got)
	}
	other := taskruntime.Principal{TenantID: "t", UserID: "someone-else"}
	if got, _ = a.ResolvePermissions(ctx, other, nil); !reflect.DeepEqual(got, &taskruntime.Permissions{Preset: "default"}) {
		t.Errorf("another user is not affected: %+v", got)
	}
}

func TestResolvePermissionsRefusesUnknownValues(t *testing.T) {
	a := permsApp()
	for _, c := range []struct {
		name  string
		spec  *taskruntime.Permissions
		field string
	}{
		{"unknown preset", &taskruntime.Permissions{Preset: "yolo"}, "permissions.preset"},
		{"empty preset", &taskruntime.Permissions{}, "permissions.preset"},
		{"unknown write scope", &taskruntime.Permissions{Preset: "custom", WriteScope: ptr("everywhere")}, "permissions.write_scope"},
		{"unknown write scope on a preset", &taskruntime.Permissions{Preset: "auto", WriteScope: ptr("")}, "permissions.write_scope"},
	} {
		_, err := a.ResolvePermissions(context.Background(), permsUser, c.spec)
		var fe *FieldError
		if !errors.As(err, &fe) || fe.Field != c.field || !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", c.name, err)
		}
	}
}

func TestResolveTaskConfigCarriesPermissions(t *testing.T) {
	a := permsApp()
	saveSettings(t, a, settingsReq("full", "", nil, nil, nil))
	got, err := a.ResolveTaskConfig(context.Background(), permsUser, TaskConfigRequest{})
	if err != nil || !reflect.DeepEqual(got.Permissions, &taskruntime.Permissions{Preset: "full"}) {
		t.Errorf("empty request: %+v %v", got.Permissions, err)
	}
	got, err = a.ResolveTaskConfig(context.Background(), permsUser, TaskConfigRequest{Permissions: &taskruntime.Permissions{Preset: "request"}})
	if err != nil || got.Permissions.Preset != "request" {
		t.Errorf("explicit: %+v %v", got.Permissions, err)
	}
	if _, err = a.ResolveTaskConfig(context.Background(), permsUser, TaskConfigRequest{Permissions: &taskruntime.Permissions{Preset: "nope"}}); !errors.Is(err, ErrInvalid) {
		t.Errorf("unknown preset: %v", err)
	}
}

func TestSaveUserSettingsValidatesAndFillsDefaults(t *testing.T) {
	a := permsApp()
	ctx := context.Background()
	got, err := a.UserSettings(ctx, permsUser)
	if err != nil || !reflect.DeepEqual(got, taskruntime.DefaultUserSettings()) {
		t.Fatalf("never saved: %+v %v", got, err)
	}
	for name, req := range map[string]UserSettingsRequest{
		"preset":      settingsReq("yolo", "", nil, nil, nil),
		"write scope": settingsReq("custom", "everywhere", nil, nil, nil),
	} {
		if _, err := a.SaveUserSettings(ctx, permsUser, req); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if got, _ = a.UserSettings(ctx, permsUser); !reflect.DeepEqual(got, taskruntime.DefaultUserSettings()) {
		t.Errorf("a refused save changed the settings: %+v", got)
	}
	saved, err := a.SaveUserSettings(ctx, permsUser, settingsReq("custom", "none", ptr(false), ptr(true), nil))
	want := taskruntime.UserSettings{Permissions: taskruntime.PermissionSettings{DefaultPreset: "custom", Custom: taskruntime.CustomPermissions{WriteScope: "none", AutoCommands: true}}}
	if err != nil || !reflect.DeepEqual(saved, want) {
		t.Errorf("saved: %+v %v", saved, err)
	}
	// A full replace: leaving the body empty goes back to the defaults.
	if saved, _ = a.SaveUserSettings(ctx, permsUser, UserSettingsRequest{}); !reflect.DeepEqual(saved, taskruntime.DefaultUserSettings()) {
		t.Errorf("empty body: %+v", saved)
	}
}

func TestResolveTaskConfigUpdateKeepsTheTasksPermissions(t *testing.T) {
	a := permsApp()
	ctx := context.Background()
	saveSettings(t, a, settingsReq("custom", "none", ptr(false), ptr(true), ptr(true)))
	task, err := a.Tasks.Create(ctx, permsUser, taskruntime.CreateInput{Title: "t", Goal: "g"})
	if err != nil {
		t.Fatal(err)
	}
	custom := &taskruntime.Permissions{Preset: "custom", WriteScope: ptr("workspace"), AutoEdits: ptr(true), AutoCommands: ptr(false), AutoBuiltin: ptr(false)}
	in, _ := a.ResolveTaskConfig(ctx, permsUser, TaskConfigRequest{Permissions: custom})
	if _, err := a.Tasks.UpdateTaskConfig(ctx, permsUser, task.ID, "c1", 1, in); err != nil {
		t.Fatal(err)
	}
	// The user's settings differ from the task's spec; an update without permissions must keep the task's.
	got, err := a.ResolveTaskConfigUpdate(ctx, permsUser, task.ID, TaskConfigRequest{Mode: "plan"})
	if err != nil || !reflect.DeepEqual(got.Permissions, custom) || got.Mode != "plan" {
		t.Errorf("kept: %+v %v", got.Permissions, err)
	}
	got, _ = a.ResolveTaskConfigUpdate(ctx, permsUser, task.ID, TaskConfigRequest{Permissions: &taskruntime.Permissions{Preset: "auto"}})
	if !reflect.DeepEqual(got.Permissions, &taskruntime.Permissions{Preset: "auto"}) {
		t.Errorf("explicit preset: %+v", got.Permissions)
	}
	got, _ = a.ResolveTaskConfigUpdate(ctx, permsUser, task.ID, TaskConfigRequest{Permissions: &taskruntime.Permissions{Preset: "custom"}})
	want := &taskruntime.Permissions{Preset: "custom", WriteScope: ptr("none"), AutoEdits: ptr(false), AutoCommands: ptr(true), AutoBuiltin: ptr(true)}
	if !reflect.DeepEqual(got.Permissions, want) {
		t.Errorf("custom takes the user's rules: %+v", got.Permissions)
	}
	if _, err := a.ResolveTaskConfigUpdate(ctx, permsUser, "task_nope", TaskConfigRequest{}); !errors.Is(err, taskruntime.ErrNotFound) {
		t.Errorf("unknown task: %v", err)
	}
}

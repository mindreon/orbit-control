package task

import (
	"context"
)

// How much a task's agent may do without asking (contract v3 PermissionSpec) and the per-user defaults it is resolved
// from. The worker enforces a task's Permissions; control only validates, resolves and forwards them, so the worker never
// reads user settings.

const (
	PresetDefault = "default"
	PresetRequest = "request"
	PresetAuto    = "auto"
	PresetFull    = "full"
	PresetCustom  = "custom"

	WriteScopeNone      = "none"
	WriteScopeWorkspace = "workspace"
)

// ValidPreset reports whether name is one of the five presets.
func ValidPreset(name string) bool {
	switch name {
	case PresetDefault, PresetRequest, PresetAuto, PresetFull, PresetCustom:
		return true
	}
	return false
}

// ValidWriteScope reports whether name is a write scope.
func ValidWriteScope(name string) bool { return name == WriteScopeNone || name == WriteScopeWorkspace }

// Permissions is a task's permission spec. The fields beside Preset are only meaningful for the "custom" preset; a nil
// field is one the caller did not state.
type Permissions struct {
	Preset       string  `json:"preset"`
	WriteScope   *string `json:"write_scope,omitempty"`
	AutoEdits    *bool   `json:"auto_edits,omitempty"`
	AutoCommands *bool   `json:"auto_commands,omitempty"`
	AutoBuiltin  *bool   `json:"auto_builtin,omitempty"`
}

// workflowValue is the spec as the workflow's TaskConfig.permissions has it; nil (absent) means the default preset.
func (p *Permissions) workflowValue() any {
	if p == nil {
		return nil
	}
	out := map[string]any{"preset": p.Preset}
	if p.WriteScope != nil {
		out["write_scope"] = *p.WriteScope
	}
	if p.AutoEdits != nil {
		out["auto_edits"] = *p.AutoEdits
	}
	if p.AutoCommands != nil {
		out["auto_commands"] = *p.AutoCommands
	}
	if p.AutoBuiltin != nil {
		out["auto_builtin"] = *p.AutoBuiltin
	}
	return out
}

// permissionsFromWorkflow reads a spec out of a workflow config (decoded JSON) or an update payload; absent is nil.
func permissionsFromWorkflow(raw any) *Permissions {
	switch fields := raw.(type) {
	case *Permissions:
		return fields
	case map[string]any:
		out := &Permissions{}
		out.Preset, _ = fields["preset"].(string)
		if scope, ok := fields["write_scope"].(string); ok {
			out.WriteScope = &scope
		}
		out.AutoEdits = boolField(fields, "auto_edits")
		out.AutoCommands = boolField(fields, "auto_commands")
		out.AutoBuiltin = boolField(fields, "auto_builtin")
		return out
	}
	return nil
}

func boolField(fields map[string]any, key string) *bool {
	if value, ok := fields[key].(bool); ok {
		return &value
	}
	return nil
}

// orDefault is the spec a reader sees: a task with none runs the default preset.
func (p *Permissions) orDefault() *Permissions {
	if p == nil || p.Preset == "" {
		return &Permissions{Preset: PresetDefault}
	}
	return p
}

// CustomPermissions are the rules of the "custom" preset, all stated.
type CustomPermissions struct {
	WriteScope   string `json:"write_scope"`
	AutoEdits    bool   `json:"auto_edits"`
	AutoCommands bool   `json:"auto_commands"`
	AutoBuiltin  bool   `json:"auto_builtin"`
}

// DefaultCustomPermissions is what a user who never saved settings gets for "custom".
func DefaultCustomPermissions() CustomPermissions {
	return CustomPermissions{WriteScope: WriteScopeWorkspace, AutoEdits: true}
}

// PermissionSettings is a user's permission defaults: the preset a new task starts with and what "custom" means.
type PermissionSettings struct {
	DefaultPreset string            `json:"default_preset"`
	Custom        CustomPermissions `json:"custom"`
}

// UserSettings is what GET and PUT /v1/settings carry.
type UserSettings struct {
	Permissions PermissionSettings `json:"permissions"`
}

// DefaultUserSettings is what a user who never saved settings has.
func DefaultUserSettings() UserSettings {
	return UserSettings{Permissions: PermissionSettings{DefaultPreset: PresetDefault, Custom: DefaultCustomPermissions()}}
}

// UserSettingsStore is implemented by a projection that keeps user settings. found is false when the user never saved any.
type UserSettingsStore interface {
	GetUserSettings(ctx context.Context, p Principal) (settings UserSettings, found bool, err error)
	SetUserSettings(ctx context.Context, p Principal, settings UserSettings) error
}

// UserSettings reads the caller's settings; a user who never saved any gets the defaults.
func (s *Service) UserSettings(ctx context.Context, p Principal) (UserSettings, error) {
	if store, ok := s.projection.(UserSettingsStore); ok {
		settings, found, err := store.GetUserSettings(ctx, p)
		if err != nil {
			return UserSettings{}, err
		}
		if !found {
			return DefaultUserSettings(), nil
		}
		return settings, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if settings, ok := s.userSettings[p.TenantID+"\x00"+p.UserID]; ok {
		return settings, nil
	}
	return DefaultUserSettings(), nil
}

// SetUserSettings replaces the caller's settings with settings, which the caller has already validated.
func (s *Service) SetUserSettings(ctx context.Context, p Principal, settings UserSettings) error {
	if store, ok := s.projection.(UserSettingsStore); ok {
		return store.SetUserSettings(ctx, p, settings)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userSettings[p.TenantID+"\x00"+p.UserID] = settings // dev only: without a store they last as long as the process
	return nil
}

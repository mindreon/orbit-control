package app

import (
	"context"

	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// validatePermissions checks the values of a permission spec a caller stated. field is the request path it came from.
func validatePermissions(field string, spec *taskruntime.Permissions) error {
	if spec == nil {
		return nil
	}
	if !taskruntime.ValidPreset(spec.Preset) {
		return invalidField("PERMISSIONS_PRESET_INVALID", field+".preset", "preset must be default, request, auto, full or custom")
	}
	if spec.WriteScope != nil && !taskruntime.ValidWriteScope(*spec.WriteScope) {
		return invalidField("PERMISSIONS_WRITE_SCOPE_INVALID", field+".write_scope", "write_scope must be none or workspace")
	}
	return nil
}

// ResolvePermissions turns what a task's request said about permissions into what the task is stored with, so the worker
// never reads the user's settings. A request without permissions takes the user's default preset; "custom" is filled
// from the user's custom rules, and a field the request states wins over them. Any other preset is stored alone. A user
// with no saved settings gets the default preset.
func (a *App) ResolvePermissions(ctx context.Context, p taskruntime.Principal, requested *taskruntime.Permissions) (*taskruntime.Permissions, error) {
	if err := validatePermissions("permissions", requested); err != nil {
		return nil, err
	}
	settings, err := a.Tasks.UserSettings(ctx, p)
	if err != nil {
		return nil, err
	}
	preset := settings.Permissions.DefaultPreset
	if requested != nil {
		preset = requested.Preset
	}
	if preset != taskruntime.PresetCustom {
		return &taskruntime.Permissions{Preset: preset}, nil
	}
	custom := settings.Permissions.Custom
	out := &taskruntime.Permissions{Preset: preset, WriteScope: &custom.WriteScope, AutoEdits: &custom.AutoEdits, AutoCommands: &custom.AutoCommands, AutoBuiltin: &custom.AutoBuiltin}
	if requested != nil {
		if requested.WriteScope != nil {
			out.WriteScope = requested.WriteScope
		}
		if requested.AutoEdits != nil {
			out.AutoEdits = requested.AutoEdits
		}
		if requested.AutoCommands != nil {
			out.AutoCommands = requested.AutoCommands
		}
		if requested.AutoBuiltin != nil {
			out.AutoBuiltin = requested.AutoBuiltin
		}
	}
	return out, nil
}

// UserSettingsRequest is what PUT /v1/settings carries. A field that is left out takes its default, so the body is the
// whole settings document, not a patch.
type UserSettingsRequest struct {
	Permissions *struct {
		DefaultPreset string `json:"default_preset"`
		Custom        *struct {
			WriteScope   string `json:"write_scope"`
			AutoEdits    *bool  `json:"auto_edits"`
			AutoCommands *bool  `json:"auto_commands"`
			AutoBuiltin  *bool  `json:"auto_builtin"`
		} `json:"custom"`
	} `json:"permissions"`
}

// UserSettings reads the caller's settings; the defaults when they never saved any.
func (a *App) UserSettings(ctx context.Context, p taskruntime.Principal) (taskruntime.UserSettings, error) {
	return a.Tasks.UserSettings(ctx, p)
}

// SaveUserSettings validates req, replaces the caller's settings with it and returns what was saved.
func (a *App) SaveUserSettings(ctx context.Context, p taskruntime.Principal, req UserSettingsRequest) (taskruntime.UserSettings, error) {
	settings := taskruntime.DefaultUserSettings()
	if req.Permissions != nil {
		if preset := req.Permissions.DefaultPreset; preset != "" {
			if !taskruntime.ValidPreset(preset) {
				return settings, invalidField("PERMISSIONS_PRESET_INVALID", "permissions.default_preset", "default_preset must be default, request, auto, full or custom")
			}
			settings.Permissions.DefaultPreset = preset
		}
		if custom := req.Permissions.Custom; custom != nil {
			if custom.WriteScope != "" {
				if !taskruntime.ValidWriteScope(custom.WriteScope) {
					return settings, invalidField("PERMISSIONS_WRITE_SCOPE_INVALID", "permissions.custom.write_scope", "write_scope must be none or workspace")
				}
				settings.Permissions.Custom.WriteScope = custom.WriteScope
			}
			if custom.AutoEdits != nil {
				settings.Permissions.Custom.AutoEdits = *custom.AutoEdits
			}
			if custom.AutoCommands != nil {
				settings.Permissions.Custom.AutoCommands = *custom.AutoCommands
			}
			if custom.AutoBuiltin != nil {
				settings.Permissions.Custom.AutoBuiltin = *custom.AutoBuiltin
			}
		}
	}
	if err := a.Tasks.SetUserSettings(ctx, p, settings); err != nil {
		return settings, err
	}
	return settings, nil
}

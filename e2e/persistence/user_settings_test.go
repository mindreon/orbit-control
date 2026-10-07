//go:build e2e

package persistence

import (
	"context"
	"net/http"
	"testing"
)

// A user's settings (migration 00029, user_settings): the permission defaults a new task starts with and the rules of the
// "custom" preset. Control resolves them into a task's config, so a worker never reads them.
//
// How it can go wrong, written down before the code:
//   - one user reads or overwrites another user's settings, in the same tenant or in another one;
//   - a user who never saved gets anything but the defaults, or an error;
//   - a saved value is lost or changed when read again, or an invalid one is stored;
//   - a task created or reconfigured does not take the user's default preset or custom rules;
//   - orbit_worker can read the table, or orbit_app can delete from it.

const settingsContract = "USER-SETTINGS"

func TestUserSettingsRoundTripResolutionAndIsolation(t *testing.T) {
	a := startServer(t, serverOpts{tenant: "t-set-a", maxConns: 4})
	other := startServer(t, serverOpts{tenant: "t-set-b", maxConns: 4})
	alice, bob := userCSRF("u-set-alice"), userCSRF("u-set-bob")
	const defaults = `{"permissions":{"default_preset":"default","custom":{"write_scope":"workspace","auto_edits":true,"auto_commands":false,"auto_builtin":false}}}`

	a.check(t, "USER-SETTINGS/defaults", settingsContract, "a user who never saved gets the defaults", httpReq{
		Method: http.MethodGet, Path: "/v1/settings", Headers: alice,
	}, httpExp{Status: http.StatusOK, BodyEquals: defaults + "\n"})

	for _, c := range []struct{ id, body string }{
		{"bad-preset", `{"permissions":{"default_preset":"yolo"}}`},
		{"bad-write-scope", `{"permissions":{"default_preset":"custom","custom":{"write_scope":"everywhere"}}}`},
	} {
		a.check(t, "USER-SETTINGS/refuse-"+c.id, settingsContract, "an invalid value is refused", httpReq{
			Method: http.MethodPut, Path: "/v1/settings", Headers: alice, Body: c.body,
		}, httpExp{Status: http.StatusBadRequest, BodyIncludes: []string{`"field":"permissions.`}})
	}
	a.check(t, "USER-SETTINGS/still-defaults", settingsContract, "a refused save changes nothing", httpReq{
		Method: http.MethodGet, Path: "/v1/settings", Headers: alice,
	}, httpExp{Status: http.StatusOK, BodyEquals: defaults + "\n"})

	const saved = `{"permissions":{"default_preset":"custom","custom":{"write_scope":"none","auto_edits":false,"auto_commands":true,"auto_builtin":false}}}`
	a.check(t, "USER-SETTINGS/save", settingsContract, "save returns the saved body", httpReq{
		Method: http.MethodPut, Path: "/v1/settings", Headers: alice, Body: saved,
	}, httpExp{Status: http.StatusOK, BodyEquals: saved + "\n"})
	a.check(t, "USER-SETTINGS/read-back", settingsContract, "the saved settings read back the same", httpReq{
		Method: http.MethodGet, Path: "/v1/settings", Headers: alice,
	}, httpExp{Status: http.StatusOK, BodyEquals: saved + "\n"})
	a.check(t, "USER-SETTINGS/replace", settingsContract, "a second save replaces the first as a whole", httpReq{
		Method: http.MethodPut, Path: "/v1/settings", Headers: alice, Body: `{"permissions":{"default_preset":"auto"}}`,
	}, httpExp{Status: http.StatusOK, BodyEquals: `{"permissions":{"default_preset":"auto","custom":{"write_scope":"workspace","auto_edits":true,"auto_commands":false,"auto_builtin":false}}}` + "\n"})
	a.check(t, "USER-SETTINGS/save-custom-again", settingsContract, "save the custom settings again", httpReq{
		Method: http.MethodPut, Path: "/v1/settings", Headers: alice, Body: saved,
	}, httpExp{Status: http.StatusOK, BodyEquals: saved + "\n"})

	a.check(t, "USER-SETTINGS/other-user", settingsContract, "another user of the tenant still has the defaults", httpReq{
		Method: http.MethodGet, Path: "/v1/settings", Headers: bob,
	}, httpExp{Status: http.StatusOK, BodyEquals: defaults + "\n"})
	other.check(t, "USER-SETTINGS/other-tenant", settingsContract, "the same user id in another tenant has the defaults", httpReq{
		Method: http.MethodGet, Path: "/v1/settings", Headers: alice,
	}, httpExp{Status: http.StatusOK, BodyEquals: defaults + "\n"})

	// Resolution at creation and update.
	made := a.check(t, "USER-SETTINGS/create-takes-default", settingsContract, "a task without permissions takes the user's default preset (custom, with the user's rules)", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks", Headers: alice, Body: `{"title":"p","goal":"g"}`,
	}, httpExp{Status: http.StatusCreated})
	cfg := "/v1/tasks/" + decodeField(t, made.Body, "task_id") + "/config"
	a.check(t, "USER-SETTINGS/read-custom", settingsContract, "the task's config carries the expanded custom rules", httpReq{
		Method: http.MethodGet, Path: cfg, Headers: alice,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"permissions":{"preset":"custom","write_scope":"none","auto_edits":false,"auto_commands":true,"auto_builtin":false}`}})
	a.check(t, "USER-SETTINGS/update-explicit", settingsContract, "an explicit field wins over the user's custom rules; a non-custom preset is stored alone", httpReq{
		Method: http.MethodPut, Path: cfg, Headers: alice, Body: `{"base_config_version":1,"permissions":{"preset":"custom","auto_edits":true}}`,
	}, httpExp{Status: http.StatusOK})
	a.check(t, "USER-SETTINGS/read-explicit", settingsContract, "auto_edits is the request's, the rest the user's", httpReq{
		Method: http.MethodGet, Path: cfg, Headers: alice,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"permissions":{"preset":"custom","write_scope":"none","auto_edits":true,"auto_commands":true,"auto_builtin":false}`}})
	a.check(t, "USER-SETTINGS/update-keeps", settingsContract, "an update without permissions keeps the task's current spec", httpReq{
		Method: http.MethodPut, Path: cfg, Headers: alice, Body: `{"base_config_version":2,"mode":"plan"}`,
	}, httpExp{Status: http.StatusOK})
	a.check(t, "USER-SETTINGS/read-kept", settingsContract, "the custom spec is unchanged", httpReq{
		Method: http.MethodGet, Path: cfg, Headers: alice,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"mode":"plan"`, `"permissions":{"preset":"custom","write_scope":"none","auto_edits":true,"auto_commands":true,"auto_builtin":false}`}})
	a.check(t, "USER-SETTINGS/update-full", settingsContract, "a non-custom preset replaces the rules", httpReq{
		Method: http.MethodPut, Path: cfg, Headers: alice, Body: `{"base_config_version":3,"permissions":{"preset":"full","auto_edits":false}}`,
	}, httpExp{Status: http.StatusOK})
	a.check(t, "USER-SETTINGS/read-full", settingsContract, "only the preset is stored", httpReq{
		Method: http.MethodGet, Path: cfg, Headers: alice,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"permissions":{"preset":"full"}`}})
	a.check(t, "USER-SETTINGS/update-bad-preset", settingsContract, "an unknown preset is refused on update", httpReq{
		Method: http.MethodPut, Path: cfg, Headers: alice, Body: `{"base_config_version":4,"permissions":{"preset":"yolo"}}`,
	}, httpExp{Status: http.StatusBadRequest, BodyIncludes: []string{`"field":"permissions.preset"`}})

	plain := a.check(t, "USER-SETTINGS/create-other-user", settingsContract, "a user with no settings gets the default preset", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks", Headers: bob, Body: `{"title":"p","goal":"g"}`,
	}, httpExp{Status: http.StatusCreated})
	a.check(t, "USER-SETTINGS/read-default", settingsContract, "the default preset, stored alone", httpReq{
		Method: http.MethodGet, Path: "/v1/tasks/" + decodeField(t, plain.Body, "task_id") + "/config", Headers: bob,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"permissions":{"preset":"default"}`}})

	// Grants: the worker has none, and the app role can neither delete nor rewrite identity columns.
	ctx := context.Background()
	worker, app := newPool(t, workerURL, 2), newPool(t, appURL, 2)
	_, werr := worker.Exec(ctx, `SELECT count(*) FROM user_settings`)
	_, derr := app.Exec(ctx, `DELETE FROM user_settings`)
	_, uerr := app.Exec(ctx, `UPDATE user_settings SET user_id = user_id`)
	iso(t, "USER-SETTINGS/grants", settingsContract, []string{"FM-28"},
		"orbit_worker cannot read user_settings; orbit_app cannot delete from it or update its key columns",
		sqlReq{Role: "orbit_worker, orbit_app", SQL: "SELECT / DELETE / UPDATE user_settings"},
		map[string]any{"workerSelect": "42501", "appDelete": "42501", "appUpdateKey": "42501"},
		map[string]any{"workerSelect": sqlState(werr), "appDelete": sqlState(derr), "appUpdateKey": sqlState(uerr)},
		sqlState(werr) == "42501" && sqlState(derr) == "42501" && sqlState(uerr) == "42501")
}

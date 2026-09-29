//go:build e2e

package persistence

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

const apiCommandContract = "A7 / 10 §1 API command idempotency"

func createTaskVia(t *testing.T, srv *server, user string) string {
	t.Helper()
	created := srv.check(t, "API-COMMAND/create-task", apiCommandContract, "create a task", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks", Headers: userCSRF(user), Body: `{"title":"ledger","goal":"idempotent commands"}`,
	}, httpExp{Status: http.StatusCreated, BodyIncludes: []string{`"task_id"`}})
	var body struct {
		ID string `json:"task_id"`
	}
	if err := json.Unmarshal([]byte(created.Body), &body); err != nil || body.ID == "" {
		t.Fatalf("decode created task: %v %s", err, created.Body)
	}
	return body.ID
}

// Two control replicas share one PostgreSQL. What one accepted, the other replays or refuses, and a restarted
// replica still knows it (A7).
func TestAPICommandIdempotencyAcrossReplicasAndRestart(t *testing.T) {
	const tenant, testUser = "t-api-command", "u-api-command"
	a := startServer(t, serverOpts{tenant: tenant, maxConns: 4})
	b := startServer(t, serverOpts{tenant: tenant, maxConns: 4})
	taskID := createTaskVia(t, a, testUser)
	path := "/v1/tasks/" + taskID + "/control"
	pause := `{"command_id":"cmd-across-replicas","action":"pause"}`
	cancel := `{"command_id":"cmd-across-replicas","action":"cancel"}`

	first := a.check(t, "API-COMMAND/replica-a-accepts", apiCommandContract, "replica A accepts the command", httpReq{
		Method: http.MethodPost, Path: path, Headers: userCSRF(testUser), Body: pause,
	}, httpExp{Status: http.StatusAccepted, BodyIncludes: []string{`"accepted":true`}})
	b.check(t, "API-COMMAND/replica-b-replays", apiCommandContract, "replica B replays the stored response for the same body", httpReq{
		Method: http.MethodPost, Path: path, Headers: userCSRF(testUser), Body: pause,
	}, httpExp{Status: http.StatusAccepted, BodyEquals: first.Body})
	b.check(t, "API-COMMAND/replica-b-conflict", apiCommandContract, "replica B answers 409 for another body under the same command_id", httpReq{
		Method: http.MethodPost, Path: path, Headers: userCSRF(testUser), Body: cancel,
	}, httpExp{Status: http.StatusConflict, BodyIncludes: []string{"IDEMPOTENCY_KEY_REUSED"}})

	restarted := startServer(t, serverOpts{tenant: tenant, maxConns: 4})
	restarted.check(t, "API-COMMAND/restart-conflict", apiCommandContract, "a fresh process still answers 409 for another body", httpReq{
		Method: http.MethodPost, Path: path, Headers: userCSRF(testUser), Body: cancel,
	}, httpExp{Status: http.StatusConflict, BodyIncludes: []string{"IDEMPOTENCY_KEY_REUSED"}})
	restarted.check(t, "API-COMMAND/restart-replay", apiCommandContract, "a fresh process replays the stored response", httpReq{
		Method: http.MethodPost, Path: path, Headers: userCSRF(testUser), Body: pause,
	}, httpExp{Status: http.StatusAccepted, BodyEquals: first.Body})

	owner := newPool(t, ownerURL, 2)
	var status, scope string
	err := owner.QueryRow(context.Background(),
		`SELECT status, scope FROM idempotency_ledger WHERE tenant_id = $1 AND key = $2`,
		tenant, tenant+"/"+taskID+"/cmd-across-replicas").Scan(&status, &scope)
	record(t, caseInput{ID: "API-COMMAND/ledger-row", Contract: apiCommandContract, Kind: "e2e",
		Description: "the accepted command is one succeeded api_command ledger row",
		Request:     map[string]string{"table": "idempotency_ledger"},
		Expected:    map[string]any{"status": "succeeded", "scope": "api_command"},
		Actual:      map[string]any{"status": status, "scope": scope, "error": sqlState(err)},
		Pass:        err == nil && status == "succeeded" && scope == "api_command"})
}

// A claim left behind by a dead process blocks the command only until its lease passes; a released claim can be
// retried at once (FM-86).
func TestAPICommandInProgressAndTakeover(t *testing.T) {
	const tenant, testUser = "t-api-lease", "u-api-lease"
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4})
	owner := newPool(t, ownerURL, 2)
	taskID := createTaskVia(t, srv, testUser)
	path := "/v1/tasks/" + taskID + "/control"
	body := `{"command_id":"cmd-lease","action":"pause"}`
	key := tenant + "/" + taskID + "/cmd-lease"
	send := func(id, desc string, status int, includes ...string) httpAct {
		return srv.check(t, id, apiCommandContract, desc, httpReq{
			Method: http.MethodPost, Path: path, Headers: userCSRF(testUser), Body: body,
		}, httpExp{Status: status, BodyIncludes: includes})
	}
	// setRow puts the row back to what a process that died mid-command leaves behind.
	setRow := func(assign string) {
		t.Helper()
		if _, err := owner.Exec(context.Background(),
			`UPDATE idempotency_ledger SET `+assign+` WHERE tenant_id = $1 AND key = $2`, tenant, key); err != nil {
			t.Fatalf("set ledger row: %v", err)
		}
	}
	send("API-COMMAND/lease-first", "the command is accepted", http.StatusAccepted, `"accepted":true`)

	setRow(`status = 'started', result_ref = NULL, owner = 'dead-control', last_seen = now()`)
	blocked := send("API-COMMAND/lease-in-progress", "a live claim answers 409 IN_PROGRESS", http.StatusConflict, "IN_PROGRESS")
	setRow(`last_seen = now() - interval '1 hour'`)
	send("API-COMMAND/lease-takeover", "a claim past its lease is taken over and the command runs again", http.StatusAccepted, `"accepted":true`)
	send("API-COMMAND/lease-replay", "the taken-over command is now replayable", http.StatusAccepted, `"accepted":true`)

	setRow(`status = 'started', result_ref = NULL, owner = NULL, last_seen = now()`)
	send("API-COMMAND/lease-released", "a released claim is retried at once", http.StatusAccepted, `"accepted":true`)

	var status string
	var holder *string
	err := owner.QueryRow(context.Background(), `SELECT status, owner FROM idempotency_ledger WHERE tenant_id = $1 AND key = $2`,
		tenant, key).Scan(&status, &holder)
	record(t, caseInput{ID: "API-COMMAND/lease-final-row", Contract: apiCommandContract, Kind: "e2e", FailureModes: []string{"FM-86"},
		Description: "after takeover the row is succeeded and owned by the new process, not the dead one",
		Request:     map[string]string{"table": "idempotency_ledger"},
		Expected:    map[string]any{"status": "succeeded", "ownedByDeadProcess": false, "inProgressBodyMentionsRetry": true},
		Actual: map[string]any{"status": status, "ownedByDeadProcess": holder != nil && *holder == "dead-control",
			"inProgressBodyMentionsRetry": strings.Contains(blocked.Body, "retry"), "error": sqlState(err)},
		Pass: err == nil && status == "succeeded" && (holder == nil || *holder != "dead-control") && strings.Contains(blocked.Body, "retry")})
}

// ISO-24: what orbit_app may change in idempotency_ledger.
func TestISO24APICommandLedgerGrants(t *testing.T) {
	const tenant = "t-iso24"
	ctx := context.Background()
	opsEnsureTenant(t, tenant)
	owner := newPool(t, ownerURL, 2)
	app := newPool(t, appURL, 2)
	if _, err := owner.Exec(ctx, `INSERT INTO idempotency_ledger (scope, key, tenant_id, request_hash, status)
		VALUES ('api_command', $1 || '/task/cmd', $1, 'h', 'started') ON CONFLICT DO NOTHING`, tenant); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	for _, set := range []string{`request_hash = 'rewritten'`, `key = 'rewritten'`, `scope = 'side_effect'`, `tenant_id = 'other'`, `first_seen = now()`} {
		sql := `UPDATE idempotency_ledger SET ` + set + ` WHERE tenant_id = $1`
		_, err := execAsApp(ctx, app, tenant, sql, tenant)
		column := strings.SplitN(set, " ", 2)[0]
		iso(t, "ISO-24/update-denied-"+column, apiCommandContract, []string{"FM-85"},
			"orbit_app UPDATE of idempotency_ledger."+column+" (outside its column grant) is denied",
			sqlReq{Role: "orbit_app", Tenant: tenant, SQL: sql},
			map[string]any{"error": "permission denied"}, map[string]any{"error": privilegeDenied(err)},
			privilegeDenied(err) == "permission denied")
	}
	const deleteSQL = `DELETE FROM idempotency_ledger WHERE tenant_id = $1`
	_, delErr := execAsApp(ctx, app, tenant, deleteSQL, tenant)
	iso(t, "ISO-24/delete-denied", apiCommandContract, []string{"FM-85"},
		"orbit_app cannot DELETE ledger rows",
		sqlReq{Role: "orbit_app", Tenant: tenant, SQL: deleteSQL},
		map[string]any{"error": "permission denied"}, map[string]any{"error": privilegeDenied(delErr)},
		privilegeDenied(delErr) == "permission denied")
	const okSQL = `UPDATE idempotency_ledger SET status = 'succeeded', result_ref = '{}', owner = NULL, last_seen = now() WHERE tenant_id = $1`
	n, okErr := execAsApp(ctx, app, tenant, okSQL, tenant)
	iso(t, "ISO-24/update-allowed-claim-columns", apiCommandContract, []string{"FM-85"},
		"orbit_app can write the columns a claim, completion, release and takeover use",
		sqlReq{Role: "orbit_app", Tenant: tenant, SQL: okSQL},
		map[string]any{"rows": 1, "error": "ok"}, map[string]any{"rows": n, "error": sqlState(okErr)},
		okErr == nil && n == 1)
}

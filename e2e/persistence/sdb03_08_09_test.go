//go:build e2e

package persistence

import (
	"context"
	"encoding/json"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"
)

const plantedDBPassword = "e2e-planted-db-password-7f3a"

func checkAt(t *testing.T, base, id, contract, desc string, req httpReq, exp httpExp) httpAct {
	t.Helper()
	act := sendTo(t, base, req)
	record(t, caseInput{ID: id, Contract: contract, Description: desc, Request: req, Expected: exp, Actual: act, Pass: exp.matches(act)})
	return act
}

func appDBVar() envVar {
	return dbURLVar("ORBIT_CONTROL_DB_URL", appURL, "<orbit_app DB URL>")
}

// S-DB-3: data written through the API survives a control restart.
func TestSDB03RestartKeepsData(t *testing.T) {
	const c = "S-DB-3"
	const tenant = "t-sdb3"
	opsEnsureTenant(t, tenant)
	ownerPool := newPool(t, ownerURL, 2)
	workerPool := newPool(t, workerURL, 2)
	boot := func() (vars []envVar, internalBase string) {
		internal := freePort(t)
		vars = []envVar{
			appDBVar(),
			{Name: "ORBIT_DEFAULT_TENANT", Value: tenant},
			{Name: "ORBIT_DATA_DIR", Value: t.TempDir(), Display: "<temp dir>"},
			{Name: "PORT", Value: freePort(t)},
			{Name: "ORBIT_INTERNAL_ADDR", Value: "127.0.0.1:" + internal, Display: "127.0.0.1:<port>"},
		}
		return vars, "http://127.0.0.1:" + internal
	}
	v1, _ := boot()
	p1 := startBinary(t, v1)
	healthy := p1.waitHealthy(15 * time.Second)
	record(t, caseInput{ID: "S-DB-3/start-1", Contract: c, Description: "orbit-control binary starts against Postgres",
		Request:  procReq{Binary: "orbit-control", Env: shownEnv(v1), Probe: "GET /health until 200"},
		Expected: map[string]any{"healthy": true, "output": "storage: postgres"},
		Actual:   map[string]any{"healthy": healthy, "output": outputLines(p1.output.String())},
		Pass:     healthy && strings.Contains(p1.output.String(), "storage: postgres")})
	if !healthy {
		t.FailNow()
	}

	created := checkAt(t, p1.base, "S-DB-3/create-task", c, "create a task (binary local mode, local-dev principal)",
		httpReq{Method: "POST", Path: "/v1/tasks", Body: `{"title":"survives restart","goal":"keep the projection"}`},
		httpExp{Status: 201, BodyIncludes: []string{`"task_id"`, `"status":"CREATED"`}})
	var task struct {
		ID string `json:"task_id"`
	}
	if err := json.Unmarshal([]byte(created.Body), &task); err != nil || task.ID == "" {
		t.Fatalf("no task id in %q", created.Body)
	}
	alias(task.ID, "<task-sdb3>")
	checkAt(t, p1.base, "S-DB-3/create-persona", c, "create an assistant in the catalog",
		httpReq{Method: "POST", Path: "/v1/personas", Body: `{"name":"Reviewer","instructions":"be careful"}`},
		httpExp{Status: 200, BodyIncludes: []string{`"name":"Reviewer"`}})

	// Durable events reach the projection only through runtime_outbox, as orbit_worker writes them.
	event := `{"schema":"orbit.event/3","event_id":"evt_01SDB3000000000000000000001","tenant_id":"` + tenant + `","task_id":"` + task.ID + `","type":"task.status_changed","source":{"kind":"workflow","id":"w"},"entity":{"kind":"task","id":"` + task.ID + `","version":1},"retention":"durable","occurred_at":"2026-09-29T00:00:00Z","payload":{"from_status":"CREATED","to_status":"RUNNING"}}`
	if _, err := execAsApp(context.Background(), workerPool, tenant,
		`INSERT INTO runtime_outbox (tenant_id, task_id, event_id, body) VALUES ($1, $2, $3, $4::jsonb)`,
		tenant, task.ID, "evt_01SDB3000000000000000000001", event); err != nil {
		t.Fatalf("append to runtime_outbox: %v", err)
	}
	waitForProjection(t, &server{base: p1.base, logs: &syncBuffer{}}, task.ID, nil, `"status":"RUNNING"`)
	eventsBefore := ownerScalar[int64](t, ownerPool, `SELECT count(*) FROM task_events WHERE task_id = $1`, task.ID)

	p1.stop()
	v2, _ := boot()
	p2 := startBinary(t, v2)
	healthy = p2.waitHealthy(15 * time.Second)
	record(t, caseInput{ID: "S-DB-3/restart", Contract: c, Description: "kill the process and start a fresh one on the same database",
		Request:  procReq{Binary: "orbit-control", Env: shownEnv(v2), Probe: "SIGKILL the first process, start a second, GET /health until 200"},
		Expected: map[string]any{"firstStopped": true, "secondHealthy": true},
		Actual:   map[string]any{"firstStopped": p1.cmd.ProcessState != nil, "secondHealthy": healthy},
		Pass:     p1.cmd.ProcessState != nil && healthy})
	if !healthy {
		t.FailNow()
	}
	checkAt(t, p2.base, "S-DB-3/restart-keeps-task", c, "the task is still there with its projected state",
		httpReq{Method: "GET", Path: "/v1/tasks/" + task.ID},
		httpExp{Status: 200, BodyIncludes: []string{task.ID, `"status":"RUNNING"`, `"title":"survives restart"`}})
	checkAt(t, p2.base, "S-DB-3/list-after-restart", c, "the task is listed",
		httpReq{Method: "GET", Path: "/v1/tasks"}, httpExp{Status: 200, BodyIncludes: []string{task.ID}})
	checkAt(t, p2.base, "S-DB-3/persona-after-restart", c, "the assistant is still in the catalog",
		httpReq{Method: "GET", Path: "/v1/personas"}, httpExp{Status: 200, BodyIncludes: []string{`"name":"Reviewer"`}})
	eventsAfter := ownerScalar[int64](t, ownerPool, `SELECT count(*) FROM task_events WHERE task_id = $1`, task.ID)
	record(t, caseInput{ID: "S-DB-3/events-after-restart", Contract: c, Kind: "e2e",
		Description: "the projected events are still in Postgres after the restart",
		Steps:       []string{"count task_events before the restart", "restart", "count task_events"},
		Request:     map[string]string{"sql": "SELECT count(*) FROM task_events WHERE task_id = <task>"},
		Expected:    map[string]any{"atLeastOne": true, "unchanged": true},
		Actual:      map[string]any{"before": eventsBefore, "after": eventsAfter},
		Pass:        eventsBefore > 0 && eventsAfter == eventsBefore})
}

// S-DB-8: prod configuration without ORBIT_CONTROL_DB_URL refuses to start.
func TestSDB08ProdRequiresDatabase(t *testing.T) {
	const c = "S-DB-8"
	for _, variant := range []struct{ id, name, value string }{
		{"orbit-env-prod", "ORBIT_ENV", "prod"},
		{"auth-mode-oidc", "ORBIT_AUTH_MODE", "oidc"},
	} {
		vars := []envVar{{Name: variant.name, Value: variant.value}, {Name: "PORT", Value: freePort(t)}}
		p := startBinary(t, vars)
		code := p.waitExit(15 * time.Second)
		out := p.output.String()
		record(t, caseInput{ID: "S-DB-8/" + variant.id, Contract: c, Description: variant.name + "=" + variant.value + " without ORBIT_CONTROL_DB_URL exits non-zero and never falls back to memory",
			Request:  procReq{Binary: "orbit-control", Env: shownEnv(vars), Probe: "wait for exit (15s)"},
			Expected: map[string]any{"exitedNonZero": true, "outputIncludes": "refusing to start with in-memory storage", "outputExcludes": []string{"storage: in-memory", "listening on"}},
			Actual:   map[string]any{"exitedNonZero": code > 0, "output": outputLines(out)},
			Pass: code > 0 && strings.Contains(out, "refusing to start with in-memory storage") &&
				!strings.Contains(out, "storage: in-memory") && !strings.Contains(out, "listening on")})
	}
	vars := []envVar{{Name: "PORT", Value: freePort(t)}, {Name: "ORBIT_DATA_DIR", Value: t.TempDir(), Display: "<temp dir>"}}
	p := startBinary(t, vars)
	healthy := p.waitHealthy(15 * time.Second)
	record(t, caseInput{ID: "S-DB-8/dev-baseline", Contract: c, Description: "outside prod configuration the same binary starts with in-memory storage (shows the refusal is config-driven)",
		Request:  procReq{Binary: "orbit-control", Env: shownEnv(vars), Probe: "GET /health until 200"},
		Expected: map[string]any{"healthy": true, "outputIncludes": "storage: in-memory"},
		Actual:   map[string]any{"healthy": healthy, "output": outputLines(p.output.String())},
		Pass:     healthy && strings.Contains(p.output.String(), "storage: in-memory")})
}

// S-DB-9: no DB URL, user, password or token in logs or error responses.
func TestSDB09NoSecretsInLogsOrErrors(t *testing.T) {
	const c = "S-DB-9"
	u, _ := url.Parse(appURL)
	ou, _ := url.Parse(ownerURL)
	pu, _ := url.Parse(opsURL)
	opsPW, _ := pu.User.Password()
	hostPort := u.Host
	appUser, ownerUser := u.User.Username(), ou.User.Username()
	appPW, _ := u.User.Password()
	ownerPW, _ := ou.User.Password()
	closed := freePort(t)

	forbidden := func(extra ...string) []string {
		return append([]string{"postgres://", "postgresql://", hostPort, net.JoinHostPort(u.Hostname(), closed), appUser, ownerUser, appPW, ownerPW, opsPW, plantedDBPassword, "password authentication"}, extra...)
	}
	shownForbidden := []string{"<postgres URL scheme>", "<db host:port>", "<orbit_app user>", "<orbit_owner user>", "<env passwords>", "<planted-db-password>", "password authentication"}
	clean := func(out string) bool {
		for _, f := range forbidden() {
			if f != "" && strings.Contains(out, f) {
				return false
			}
		}
		return true
	}
	// Output is recorded only after it passed the leak check; otherwise the
	// report says so without echoing it.
	shown := func(out string) any {
		if clean(out) {
			return outputLines(out)
		}
		return "output withheld: it contained a forbidden value"
	}

	for _, probe := range []struct {
		id, desc, code string
		vars           []envVar
	}{
		{"db-wrong-password", "DB connection with a wrong password", "code=28P01", []envVar{
			dbURLVar("ORBIT_CONTROL_DB_URL", withPassword(appURL, plantedDBPassword), "<orbit_app DB URL with planted wrong password>")}},
		{"db-unreachable", "DB host that refuses connections", "code=connect_failed", []envVar{
			dbURLVar("ORBIT_CONTROL_DB_URL", withPort(appURL, closed), "<orbit_app DB URL, closed port>")}},
		{"migrate-wrong-password", "migrate-on-start with a wrong owner password", "code=28P01", []envVar{
			appDBVar(),
			{Name: "ORBIT_CONTROL_MIGRATE_ON_START", Value: "1"},
			dbURLVar("ORBIT_CONTROL_MIGRATE_DB_URL", withPassword(ownerURL, plantedDBPassword), "<orbit_owner DB URL with planted wrong password>")}},
	} {
		vars := append(probe.vars, envVar{Name: "PORT", Value: freePort(t)})
		p := startBinary(t, vars)
		code := p.waitExit(20 * time.Second)
		out := p.output.String()
		record(t, caseInput{ID: "S-DB-9/" + probe.id, Contract: c, Description: probe.desc + ": exits non-zero, logs only a host placeholder and an error code",
			Request:  procReq{Binary: "orbit-control", Env: shownEnv(vars), Probe: "wait for exit (20s), capture stdout+stderr"},
			Expected: map[string]any{"exitedNonZero": true, "outputIncludes": []string{"host=<db-host>", probe.code}, "outputExcludes": shownForbidden},
			Actual:   map[string]any{"exitedNonZero": code > 0, "output": shown(out)},
			Pass:     code > 0 && strings.Contains(out, "host=<db-host>") && strings.Contains(out, probe.code) && clean(out)})
	}

	// Runtime storage failure behind the HTTP API.
	srv := startServer(t, serverOpts{tenant: "t-sdb9", maxConns: 2})
	srv.check(t, "S-DB-9/runtime/setup", c, "the API works before the storage failure",
		httpReq{Method: "GET", Path: "/v1/tasks", Headers: user("u-sdb9")}, httpExp{Status: 200})
	srv.appPool.Close()
	act := sendTo(t, srv.base, httpReq{Method: "GET", Path: "/v1/tasks", Headers: user("u-sdb9")})
	logs := srv.logs.String()
	record(t, caseInput{ID: "S-DB-9/runtime-storage-failure", Contract: c,
		Description: "a storage failure mid-request returns a generic 500; neither the response nor the server log carries a DB URL, user or password",
		Steps:       []string{"close the server's DB pool", "GET /v1/tasks as u-sdb9", "inspect response body and server log"},
		Request:     httpReq{Method: "GET", Path: "/v1/tasks", Headers: user("u-sdb9")},
		Expected:    map[string]any{"status": 500, "body": `{"error":"storage error","code":"STORAGE_ERROR","message":"task storage is temporarily unavailable"}`, "logIncludes": "host=<db-host>", "excludes": shownForbidden},
		Actual:      map[string]any{"status": act.Status, "body": shown(act.Body), "log": shown(logs)},
		Pass: act.Status == 500 && strings.TrimSpace(act.Body) == `{"error":"storage error","code":"STORAGE_ERROR","message":"task storage is temporarily unavailable"}` &&
			strings.Contains(logs, "host=<db-host>") && clean(act.Body) && clean(logs)})
}

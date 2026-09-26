//go:build e2e

package persistence

import (
	"encoding/json"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mindreon/orbit-control/internal/app"
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
	wk := stubWorker(t, nil)
	vars := func() []envVar {
		return []envVar{
			appDBVar(),
			{Name: "ORBIT_DEFAULT_TENANT", Value: tenant},
			{Name: "ORBIT_WORKER_URL", Value: wk.URL, Display: "<stub worker>"},
			{Name: "ORBIT_DATA_DIR", Value: t.TempDir(), Display: "<temp dir>"},
			{Name: "PORT", Value: freePort(t)},
		}
	}
	v1 := vars()
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

	create := httpReq{Method: "POST", Path: "/v1/rooms", Headers: map[string]string{"Idempotency-Key": "e2e-sdb3"}, Body: `{"kind":"solo","title":"survives restart"}`}
	room := roomID(t, checkAt(t, p1.base, "S-DB-3/create", c, "create a task (binary local mode, local-dev principal)", create, httpExp{Status: 200}))
	alias(room, "<room-sdb3>")
	posted := checkAt(t, p1.base, "S-DB-3/message", c, "post a message; the stub worker parks an approval",
		httpReq{Method: "POST", Path: "/v1/rooms/" + room + "/messages", Body: `{"message":"list files"}`},
		httpExp{Status: 200, BodyIncludes: []string{`"approval":{`}})
	var pb struct {
		Approval *app.Approval `json:"approval"`
	}
	_ = json.Unmarshal([]byte(posted.Body), &pb)
	if pb.Approval == nil {
		t.Fatalf("no approval: %s", posted.Body)
	}
	alias(pb.Approval.ID, "<approval-sdb3>")

	p1.stop()
	v2 := vars()
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
	checkAt(t, p2.base, "S-DB-3/room-after-restart", c, "the task is still there with its state",
		httpReq{Method: "GET", Path: "/v1/rooms/" + room},
		httpExp{Status: 200, BodyIncludes: []string{room, `"state":"awaiting_approval"`, `"title":"survives restart"`}})
	checkAt(t, p2.base, "S-DB-3/list-after-restart", c, "the task is listed",
		httpReq{Method: "GET", Path: "/v1/rooms"}, httpExp{Status: 200, BodyIncludes: []string{room}})
	checkAt(t, p2.base, "S-DB-3/messages-after-restart", c, "user and assistant messages are still there",
		httpReq{Method: "GET", Path: "/v1/rooms/" + room + "/messages"},
		httpExp{Status: 200, BodyIncludes: []string{`"role":"user","text":"list files"`, `"role":"assistant","text":"stub reply"`}})
	checkAt(t, p2.base, "S-DB-3/approval-after-restart", c, "the pending approval is still there",
		httpReq{Method: "GET", Path: "/v1/approvals"}, httpExp{Status: 200, BodyIncludes: []string{pb.Approval.ID, `"status":"pending"`}})
	checkAt(t, p2.base, "S-DB-3/idempotency-after-restart", c, "Idempotency-Key replay after restart returns the same task",
		create, httpExp{Status: 200, BodyIncludes: []string{room}, Headers: map[string]string{"Idempotent-Replayed": "true"}})
	checkAt(t, p2.base, "S-DB-3/decide-after-restart", c, "the approval can be decided after restart",
		httpReq{Method: "POST", Path: "/v1/approvals/" + pb.Approval.ID + "/decide", Body: `{"decision":"allow"}`},
		httpExp{Status: 200, BodyIncludes: []string{`"status":"decided"`, `"decision":"allow"`}})

	const phase2 = "phase 2 (§18.4 per-task seq in one transaction); owner instruction: after Last-Event-ID PR #15 merges"
	blocked(t, "S-DB-3/events-after-restart", c, "e2e", "events written before the restart are replayed from Postgres after it", phase2,
		[]string{"ingest worker events", "restart", "GET /v1/rooms/{id}/activity"}, "events present after restart")
	blocked(t, "S-DB-3/last-event-seq-continues", c, "e2e", "last_event_seq keeps increasing across the restart", phase2,
		[]string{"ingest N events", "restart", "ingest one more", "read seq"}, "seq N+1")
	blocked(t, "S-DB-3/artifacts-after-restart", c, "e2e", "artifacts written before the restart are readable after it",
		"§16 control artifact PR (ingest + read API) and phase 2 /internal/artifact-blobs",
		[]string{"ingest artifact.created", "restart", "GET /v1/artifacts/{id}"}, "artifact metadata and content present")
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
	wk := stubWorker(t, nil)
	srv := startServer(t, serverOpts{tenant: "t-sdb9", maxConns: 2, workerURL: wk.URL})
	srv.check(t, "S-DB-9/runtime/setup", c, "the API works before the storage failure",
		httpReq{Method: "GET", Path: "/v1/rooms", Headers: user("u-sdb9")}, httpExp{Status: 200})
	srv.appPool.Close()
	act := sendTo(t, srv.base, httpReq{Method: "GET", Path: "/v1/rooms", Headers: user("u-sdb9")})
	logs := srv.logs.String()
	record(t, caseInput{ID: "S-DB-9/runtime-storage-failure", Contract: c,
		Description: "a storage failure mid-request returns a generic 500; neither the response nor the server log carries a DB URL, user or password",
		Steps:       []string{"close the server's DB pool", "GET /v1/rooms as u-sdb9", "inspect response body and server log"},
		Request:     httpReq{Method: "GET", Path: "/v1/rooms", Headers: user("u-sdb9")},
		Expected:    map[string]any{"status": 500, "body": `{"error":"internal","code":"INTERNAL","message":"internal error"}`, "logIncludes": "host=<db-host>", "excludes": shownForbidden},
		Actual:      map[string]any{"status": act.Status, "body": shown(act.Body), "log": shown(logs)},
		Pass: act.Status == 500 && strings.TrimSpace(act.Body) == `{"error":"internal","code":"INTERNAL","message":"internal error"}` &&
			strings.Contains(logs, "host=<db-host>") && clean(act.Body) && clean(logs)})

	blocked(t, "S-DB-9/session-error-path", c, "e2e", "invalid or expired session cookie: no cookie value or token in logs/response",
		"auth PR (§17; ordered after this PR by §17.9): no session handling exists yet", nil, "401 without cookie/token echo")
	blocked(t, "S-DB-9/oidc-error-path", c, "e2e", "OIDC callback failure: no code, token, client secret in logs/response",
		"auth PR (§17; ordered after this PR by §17.9): no OIDC flow exists yet", nil, "login failure without token echo")
}

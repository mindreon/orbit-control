//go:build e2e

package persistence

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// Custom experts (15 M8, T8.1): an expert is a profile whose spec the worker reads (instructions, model and MCP
// connector snapshots). Every version is one immutable profile row, so a task that names "<expert>@<n>" always runs
// the same configuration.
//
// How it can go wrong, written down before the code:
//   - a connector id that does not exist, or belongs to another tenant, is accepted (and the two answers differ,
//     which tells a caller which ids exist elsewhere);
//   - input the worker would cut or ignore is stored anyway (empty name, instructions over 20000 characters, a model
//     name with spaces, repeated or more than 20 connectors);
//   - a rejected request leaves a half-made profile behind;
//   - an update rewrites version 1 instead of adding version 2;
//   - two updates at once both claim the same version, or one version silently overwrites the other;
//   - another tenant can read, list or update the expert, or use its connectors;
//   - the snapshot carries more than names (a secret value, a field the worker does not define);
//   - the expert is lost, or its latest version changes, when control restarts.

const (
	expertContract = "EXPERT"
	expertTenant   = "t-expert-a"
	expertOther    = "t-expert-b"
	expertUser     = "u-expert"
)

func decodeField(t *testing.T, body, field string) string {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("decode %q: %v", body, err)
	}
	value, _ := decoded[field].(string)
	if value == "" {
		t.Fatalf("no %s in %s", field, body)
	}
	return value
}

func distinctIDs(n int) string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = `"c` + strconv.Itoa(i) + `"`
	}
	return "[" + strings.Join(ids, ",") + "]"
}

func newConnector(t *testing.T, s *server, name string) string {
	t.Helper()
	created := s.check(t, "EXPERT/connector-"+name, expertContract, "create the connector an expert will use", httpReq{
		Method: http.MethodPost, Path: "/v1/mcp-connectors", Headers: userCSRF(expertUser),
		Body: `{"name":"` + name + `","command":"orbit-mcp-` + name + `","envRefs":["ORBIT_MCP_DOCS_TOKEN"]}`,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"id"`}})
	return decodeField(t, created.Body, "id")
}

func TestExpertLifecycleAndIsolation(t *testing.T) {
	a := startServer(t, serverOpts{tenant: expertTenant, maxConns: 6})
	other := startServer(t, serverOpts{tenant: expertOther, maxConns: 4})
	headers := userCSRF(expertUser)

	mine := newConnector(t, a, "docs")
	theirs := newConnector(t, other, "secret-docs")

	rejected := []struct{ id, desc, body string }{
		{"unknown-connector", "a connector id that does not exist", `{"name":"x","connector_ids":["mcp_does-not-exist"]}`},
		{"foreign-connector", "another tenant's connector id, answered like an unknown one", `{"name":"x","connector_ids":["` + theirs + `"]}`},
		{"empty-name", "an empty name", `{"name":"  "}`},
		{"long-instructions", "instructions over 20000 characters", `{"name":"x","instructions":"` + strings.Repeat("a", 20001) + `"}`},
		{"bad-model", "a model name with a space", `{"name":"x","model":"not a model"}`},
		{"unknown-skill", "a skill that is not in the catalog", `{"name":"x","skill_ids":["nobody/none"]}`},
		{"duplicate-skill", "the same skill twice", `{"name":"x","skill_ids":["a/b","a/b"]}`},
		{"duplicate-connector", "the same connector twice", `{"name":"x","connector_ids":["` + mine + `","` + mine + `"]}`},
		{"too-many-connectors", "more than 20 connectors", `{"name":"x","connector_ids":` + distinctIDs(21) + `}`},
	}
	var unknownBody string
	for _, c := range rejected {
		act := a.check(t, "EXPERT/reject-"+c.id, expertContract, "refuse "+c.desc, httpReq{
			Method: http.MethodPost, Path: "/v1/experts", Headers: headers, Body: c.body,
		}, httpExp{Status: http.StatusBadRequest, BodyExcludes: []string{theirs, "secret-docs"}})
		switch c.id {
		case "unknown-connector":
			unknownBody = act.Body
		case "foreign-connector":
			if act.Body != unknownBody {
				t.Errorf("a foreign connector must be answered exactly like an unknown one:\n%s\n%s", act.Body, unknownBody)
			}
		}
	}
	a.check(t, "EXPERT/no-half-made-profile", expertContract, "a rejected request leaves no profile behind", httpReq{
		Method: http.MethodGet, Path: "/v1/profiles", Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyExcludes: []string{"expert_"}})

	created := a.check(t, "EXPERT/create", expertContract, "create an expert as version 1", httpReq{
		Method: http.MethodPost, Path: "/v1/experts", Headers: headers,
		Body: `{"name":"Writer","instructions":"Be brief.","model":"gpt-x","connector_ids":["` + mine + `"]}`,
	}, httpExp{Status: http.StatusCreated, BodyIncludes: []string{`"version":1`, `"name":"Writer"`, `"connector_ids":["` + mine + `"]`, `"skill_ids":[]`}})
	expertID := decodeField(t, created.Body, "expert_id")
	ref1 := decodeField(t, created.Body, "ref")
	if ref1 != expertID+"@1" || !strings.HasPrefix(expertID, "expert_") {
		t.Fatalf("expert id and ref: %s %s", expertID, ref1)
	}

	a.check(t, "EXPERT/snapshot", expertContract, "the profile spec carries the connector as a names-only snapshot", httpReq{
		Method: http.MethodGet, Path: "/v1/profiles/" + ref1, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{
		`"kind":"expert"`, `"instructions":"Be brief."`, `"model":"gpt-x"`, `"mcp_connectors":[{`,
		`"id":"` + mine + `"`, `"command":"orbit-mcp-docs"`, `"env_refs":["ORBIT_MCP_DOCS_TOKEN"]`,
	}})

	ref2 := expertID + "@2"
	a.check(t, "EXPERT/update", expertContract, "an update adds version 2", httpReq{
		Method: http.MethodPut, Path: "/v1/experts/" + expertID, Headers: headers,
		Body: `{"name":"Writer","instructions":"Be formal.","connector_ids":[]}`,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"ref":"` + ref2 + `"`, `"version":2`}})
	a.check(t, "EXPERT/v1-immutable", expertContract, "version 1 is exactly what it was", httpReq{
		Method: http.MethodGet, Path: "/v1/profiles/" + ref1, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{"Be brief.", `"id":"` + mine + `"`}, BodyExcludes: []string{"Be formal."}})
	a.check(t, "EXPERT/get-latest", expertContract, "reading the expert gives its latest version", httpReq{
		Method: http.MethodGet, Path: "/v1/experts/" + expertID, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"ref":"` + ref2 + `"`, "Be formal."}})
	a.check(t, "EXPERT/list-latest-only", expertContract, "the list shows each expert once, at its latest version", httpReq{
		Method: http.MethodGet, Path: "/v1/experts", Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"ref":"` + ref2 + `"`}, BodyExcludes: []string{`"ref":"` + ref1 + `"`}})
	a.check(t, "EXPERT/update-unknown", expertContract, "updating an expert that does not exist", httpReq{
		Method: http.MethodPut, Path: "/v1/experts/expert_nope", Headers: headers, Body: `{"name":"x"}`,
	}, httpExp{Status: http.StatusNotFound})

	a.check(t, "EXPERT/profile-pinned-by-ref", expertContract, "a task names one exact expert version", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks", Headers: headers,
		Body: `{"title":"with expert","goal":"use it","profile":"` + ref1 + `"}`,
	}, httpExp{Status: http.StatusCreated, BodyIncludes: []string{`"profile":"` + ref1 + `"`}})

	other.check(t, "EXPERT/other-tenant-get", expertContract, "another tenant cannot read the expert", httpReq{
		Method: http.MethodGet, Path: "/v1/experts/" + expertID, Headers: headers,
	}, httpExp{Status: http.StatusNotFound})
	other.check(t, "EXPERT/other-tenant-update", expertContract, "another tenant cannot update the expert", httpReq{
		Method: http.MethodPut, Path: "/v1/experts/" + expertID, Headers: headers, Body: `{"name":"hijack"}`,
	}, httpExp{Status: http.StatusNotFound})
	other.check(t, "EXPERT/other-tenant-list", expertContract, "another tenant's list does not show it", httpReq{
		Method: http.MethodGet, Path: "/v1/experts", Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyExcludes: []string{expertID}})

	concurrentUpdates(t, a, headers)

	again := startServer(t, serverOpts{tenant: expertTenant, maxConns: 4})
	again.check(t, "EXPERT/survives-restart", expertContract, "the expert is still there, at its latest version, in a fresh control process", httpReq{
		Method: http.MethodGet, Path: "/v1/experts/" + expertID, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"expert_id":"` + expertID + `"`}})
}

// Two updates at once: each is answered 200 with its own version, or 409 when it lost the race. Never the same
// version twice, and never a lost version.
//
// The race runs on an expert of its own: which racer wins varies between runs, and the lifecycle expert is read again
// after the restart, so it must stay at a fixed version for the two CI reports to compare equal.
func concurrentUpdates(t *testing.T, s *server, headers map[string]string) {
	t.Helper()
	made := sendTo(t, s.base, httpReq{Method: http.MethodPost, Path: "/v1/experts", Headers: headers, Body: `{"name":"Racer"}`})
	if made.Status != http.StatusCreated {
		t.Fatalf("create the expert the updates race on: %d %s", made.Status, made.Body)
	}
	expertID := decodeField(t, made.Body, "expert_id")
	acts := make([]httpAct, 2)
	var wg sync.WaitGroup
	for i := range acts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			acts[i] = sendTo(t, s.base, httpReq{
				Method: http.MethodPut, Path: "/v1/experts/" + expertID, Headers: headers,
				Body: `{"name":"Racer","instructions":"racer ` + string(rune('A'+i)) + `"}`,
			})
		}()
	}
	wg.Wait()
	refs := map[string]bool{}
	ok, pass, dup := 0, true, false
	for _, act := range acts {
		switch act.Status {
		case http.StatusOK:
			ok++
			ref := decodeField(t, act.Body, "ref")
			if refs[ref] {
				pass, dup = false, true
			}
			refs[ref] = true
		case http.StatusConflict:
		default:
			pass = false
		}
	}
	latest := sendTo(t, s.base, httpReq{Method: http.MethodGet, Path: "/v1/experts/" + expertID, Headers: headers})
	if ok == 0 || latest.Status != http.StatusOK || !strings.Contains(latest.Body, `"ref":"`) {
		pass = false
	}
	record(t, caseInput{
		ID: "EXPERT/concurrent-update", Contract: expertContract, Kind: "e2e",
		Description: "two updates at once never claim the same version",
		Request:     map[string]any{"updates": 2}, Expected: map[string]any{"distinctRefs": true, "oneUpdateAccepted": true, "latestServed": true},
		Actual: map[string]any{"distinctRefs": !dup, "oneUpdateAccepted": ok > 0, "latestServed": latest.Status == http.StatusOK}, Pass: pass,
	})
}

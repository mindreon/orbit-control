//go:build e2e

package persistence

import (
	"context"
	"net/http"
	"testing"
)

// The SOP registry (06 §2, 05 §5): definitions v2 with a name, steps that depend on each other and run as experts, and v1
// definitions that stay valid as linear v2 ones.
//
// How it can go wrong, written down before the code:
//   - a definition that is not a DAG (a cycle, an unknown or a duplicate id, a step that waits for itself) is stored and
//     fails only when a task reaches it;
//   - a field the runtime's contract does not know is stored, and the runtime refuses the definition when it reads it;
//   - a v1 definition stored before ids and dependencies existed conflicts with itself when it is registered again, or reads
//     back differently from how the runtime compiles it;
//   - "no dependencies written" (the step before it) and "an empty list" (starts at once) come out the same;
//   - a version is not immutable: the same id and version with another name or other steps replaces the stored one.

const sopContract = "SOP-REGISTRY (06 §2, migration 00026)"

func TestSOPRegistryV2AndV1Compatibility(t *testing.T) {
	const tenant = "t-sop-registry"
	a := startServer(t, serverOpts{tenant: tenant, maxConns: 4})
	headers := userCSRF("u-sop-registry")
	post := func(body string) httpReq {
		return httpReq{Method: http.MethodPost, Path: "/v1/sops", Headers: headers, Body: body}
	}

	// A v1 definition: bare strings and the three v1 fields. It reads as a linear v2 one.
	a.check(t, "SOP-REGISTRY/v1-linear", sopContract, "a v1 definition is registered and reads back as a linear v2 one", post(
		`{"sop_id":"v1","version":1,"steps":["draft",{"subject":"review","description":"check it","max_attempts":2}]}`,
	), httpExp{Status: http.StatusCreated, BodyIncludes: []string{
		`"name":"v1"`, `"id":"s1"`, `"id":"s2"`, `"depends_on":[]`, `"depends_on":["s1"]`, `"max_attempts":2`, `"ref":"v1@1"`,
	}})
	a.check(t, "SOP-REGISTRY/v1-again", sopContract, "the same v1 definition again is a no-op", post(
		`{"sop_id":"v1","version":1,"steps":["draft",{"subject":"review","description":"check it","max_attempts":2}]}`,
	), httpExp{Status: http.StatusCreated})
	a.check(t, "SOP-REGISTRY/v1-explicit-is-same", sopContract, "the same definition with its defaults spelled out is the same version", post(
		`{"sop_id":"v1","version":1,"name":"v1","steps":[{"id":"s1","subject":"draft","description":"draft","max_attempts":3},`+
			`{"id":"s2","subject":"review","description":"check it","max_attempts":2,"depends_on":["s1"]}]}`,
	), httpExp{Status: http.StatusCreated})
	a.check(t, "SOP-REGISTRY/immutable-steps", sopContract, "other steps under the same version conflict", post(
		`{"sop_id":"v1","version":1,"steps":["draft"]}`,
	), httpExp{Status: http.StatusConflict, BodyIncludes: []string{"IDEMPOTENCY_KEY_REUSED"}})
	a.check(t, "SOP-REGISTRY/immutable-name", sopContract, "another name under the same version conflicts", post(
		`{"sop_id":"v1","version":1,"name":"renamed","steps":["draft",{"subject":"review","description":"check it","max_attempts":2}]}`,
	), httpExp{Status: http.StatusConflict})

	// A definition stored before migration 00026: no name, steps without ids or dependencies. Registering it again with the
	// same steps is a no-op, and listing reads it as the v2 definition it is.
	owner := newPool(t, ownerURL, 1)
	if _, err := owner.Exec(context.Background(),
		`INSERT INTO sop_definitions (tenant_id, sop_id, version, steps) VALUES ($1, 'legacy', 1, '[{"subject":"one","description":"one","max_attempts":3},{"subject":"two","description":"two","max_attempts":3}]')`,
		tenant); err != nil {
		t.Fatalf("seed a legacy definition: %v", err)
	}
	a.check(t, "SOP-REGISTRY/legacy-again", sopContract, "a definition stored before v2 is the same as itself registered again", post(
		`{"sop_id":"legacy","version":1,"steps":["one","two"]}`,
	), httpExp{Status: http.StatusCreated, BodyIncludes: []string{`"name":"legacy"`, `"id":"s2"`, `"depends_on":["s1"]`}})

	// v2: a graph, an executor, a verifier, artifacts, approvals.
	graph := `{"sop_id":"graph","version":1,"name":"Release","description":"ship a release","steps":[` +
		`{"id":"draft","subject":"draft","description":"write the notes","executor":"writer@2"},` +
		`{"id":"lint","subject":"lint","depends_on":[]},` +
		`{"id":"review","subject":"review","depends_on":["draft","lint"],"max_attempts":5,` +
		`"verifier":{"instructions":"every claim has a source","expert":"auditor@1"},"output_schema_ref":"schema://review/1",` +
		`"required_artifacts":[{"name":"review.md","media_type":"text/markdown"}],"human_approval":"after"},` +
		`{"subject":"publish","depends_on":["review"],"human_approval":"before"}]}`
	a.check(t, "SOP-REGISTRY/v2-graph", sopContract, "a v2 definition keeps its graph, executors, verifier and approvals", post(graph),
		httpExp{Status: http.StatusCreated, BodyIncludes: []string{
			`"name":"Release"`, `"description":"ship a release"`, `"executor":"writer@2"`, `"depends_on":[]`,
			`"depends_on":["draft","lint"]`, `"expert":"auditor@1"`, `"human_approval":"after"`, `"id":"s4"`, `"min_count":1`,
		}})
	a.check(t, "SOP-REGISTRY/list", sopContract, "the registry lists the definitions with their v2 fields", httpReq{
		Method: http.MethodGet, Path: "/v1/sops", Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"ref":"graph@1"`, `"ref":"legacy@1"`, `"name":"Release"`, `"depends_on":["draft","lint"]`}})

	refused := []struct{ id, desc, body string }{
		{"cycle", "a cycle", `{"sop_id":"x","version":1,"steps":[{"id":"a","subject":"a","depends_on":["b"]},{"id":"b","subject":"b","depends_on":["a"]}]}`},
		{"self", "a step that waits for itself", `{"sop_id":"x","version":1,"steps":[{"id":"a","subject":"a","depends_on":["a"]}]}`},
		{"unknown-dependency", "a dependency on an unknown step", `{"sop_id":"x","version":1,"steps":[{"id":"a","subject":"a","depends_on":["zzz"]}]}`},
		{"duplicate-id", "a duplicate id", `{"sop_id":"x","version":1,"steps":[{"id":"a","subject":"a"},{"id":"a","subject":"b"}]}`},
		{"taken-default-id", "a step whose position-id was taken", `{"sop_id":"x","version":1,"steps":[{"id":"s2","subject":"a"},{"subject":"b"}]}`},
		{"bad-id", "an id that is not an id", `{"sop_id":"x","version":1,"steps":[{"id":"a b","subject":"a"}]}`},
		{"bad-approval", "a human_approval that is not before or after", `{"sop_id":"x","version":1,"steps":[{"subject":"a","human_approval":"during"}]}`},
		{"bad-executor", "an executor that is not a versioned ref", `{"sop_id":"x","version":1,"steps":[{"subject":"a","executor":"writer"}]}`},
		{"bad-schema-ref", "an output schema that is not a schema ref", `{"sop_id":"x","version":1,"steps":[{"subject":"a","output_schema_ref":"report"}]}`},
		{"too-many-attempts", "more attempts than a step may have", `{"sop_id":"x","version":1,"steps":[{"subject":"a","max_attempts":21}]}`},
		{"no-steps", "no steps", `{"sop_id":"x","version":1,"steps":[]}`},
		{"blank-subject", "a blank subject", `{"sop_id":"x","version":1,"steps":[{"subject":"  "}]}`},
	}
	for _, c := range refused {
		a.check(t, "SOP-REGISTRY/refuse-"+c.id, sopContract, "refuse "+c.desc, post(c.body), httpExp{Status: http.StatusBadRequest, BodyIncludes: []string{"INVALID_SOP"}})
	}
	a.check(t, "SOP-REGISTRY/refuse-unknown-field", sopContract, "refuse a field the runtime's contract does not know", post(
		`{"sop_id":"x","version":1,"steps":[{"subject":"a","retries":3}]}`,
	), httpExp{Status: http.StatusBadRequest})
	a.check(t, "SOP-REGISTRY/nothing-stored", sopContract, "a refused definition is not in the registry", httpReq{
		Method: http.MethodGet, Path: "/v1/sops", Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyExcludes: []string{`"ref":"x@1"`}})
}

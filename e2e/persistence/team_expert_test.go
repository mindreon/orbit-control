//go:build e2e

package persistence

import (
	"net/http"
	"strings"
	"testing"
)

// Team experts (15 M8, T8.6): an expert of kind "team" is a leader and 1 to 8 members, each a pinned version of a single
// expert. A task that selects one runs with config.team and the leader's expert; selecting a single expert clears it.
//
// How it can go wrong, written down before the code:
//   - a team with no member, nine members, a repeated or malformed role, a leader who is nobody's role, or members that
//     are not versioned refs is stored;
//   - a team carries instructions, a model, connectors or skills, and a single expert carries members or a leader;
//   - a member that is unknown, another tenant's, a team or the team itself is accepted, or the answers for an unknown
//     and a foreign member differ (which tells a caller what exists elsewhere);
//   - a refused team leaves a half-made profile behind;
//   - an update rewrites version 1, or turns a team into a single expert or back;
//   - another tenant can read, list or update the team, or select it for a task;
//   - selecting a team leaves config.expert on the team instead of the leader's expert, loses the members, or leaves
//     them out of what the page reads (roles and names);
//   - choosing a single expert afterwards keeps the old team;
//   - the team is lost when control restarts.

const (
	teamContract = "TEAM-EXPERT"
	teamTenant   = "t-team-a"
	teamOther    = "t-team-b"
)

func TestTeamExpertLifecycleAndIsolation(t *testing.T) {
	a := startServer(t, serverOpts{tenant: teamTenant, maxConns: 6})
	other := startServer(t, serverOpts{tenant: teamOther, maxConns: 4})
	headers := userCSRF(expertUser)

	single := func(s *server, id, name string) (expertID, ref string) {
		made := s.check(t, "TEAM-EXPERT/"+id, teamContract, "create the single expert "+name, httpReq{
			Method: http.MethodPost, Path: "/v1/experts", Headers: headers, Body: `{"name":"` + name + `","instructions":"do ` + name + `"}`,
		}, httpExp{Status: http.StatusCreated, BodyIncludes: []string{`"kind":"expert"`}})
		return decodeField(t, made.Body, "expert_id"), decodeField(t, made.Body, "ref")
	}
	writerID, writer := single(a, "writer", "Writer")
	_, reviewer := single(a, "reviewer", "Reviewer")
	_, theirs := single(other, "foreign", "SecretAgent")

	member := func(role, ref string) string { return `{"role":"` + role + `","expert":"` + ref + `"}` }
	team := func(leader string, members ...string) string {
		return `{"name":"Crew","kind":"team","leader":"` + leader + `","members":[` + strings.Join(members, ",") + `]}`
	}
	nine := make([]string, 9)
	for i := range nine {
		nine[i] = member("r"+string(rune('a'+i)), writer)
	}
	rejected := []struct{ id, desc, body, code, field string }{
		{"no-members", "a team without members", team("lead"), "TEAM_NO_MEMBERS", "members"},
		{"nine-members", "a team of nine", team("ra", nine...), "TEAM_TOO_MANY_MEMBERS", "members"},
		{"duplicate-role", "one role twice", team("lead", member("lead", writer), member("lead", reviewer)), "TEAM_DUPLICATE_ROLE", "members[1].role"},
		{"leader-not-member", "a leader who is nobody's role", team("boss", member("lead", writer)), "TEAM_LEADER_NOT_MEMBER", "leader"},
		{"bad-role", "a role that is not lowercase", team("Lead", member("Lead", writer)), "ROLE_INVALID", "members[0].role"},
		{"unversioned", "a member expert without a version", team("lead", member("lead", writerID)), "TEAM_MEMBER_REF_INVALID", "members[0].expert"},
		{"unknown-member", "a member that does not exist", team("lead", member("lead", "expert_nobody@1")), "TEAM_MEMBER_NOT_FOUND", "members[0].expert"},
		{"foreign-member", "another tenant's expert as a member", team("lead", member("lead", theirs)), "TEAM_MEMBER_NOT_FOUND", "members[0].expert"},
		{"default-member", "the built-in default as a member", team("lead", member("lead", "default@1")), "TEAM_MEMBER_NOT_FOUND", "members[0].expert"},
		{"team-instructions", "instructions on a team", `{"name":"Crew","kind":"team","instructions":"x","leader":"lead","members":[` + member("lead", writer) + `]}`, "FIELD_NOT_ALLOWED", "instructions"},
		{"single-with-members", "members on a single expert", `{"name":"x","members":[` + member("lead", writer) + `]}`, "FIELD_NOT_ALLOWED", "members"},
		{"long-label", "a label over 40 characters", team("lead", `{"role":"lead","expert":"`+writer+`","label":"`+strings.Repeat("a", 41)+`"}`), "LABEL_TOO_LONG", "members[0].label"},
		{"unknown-kind", "a kind that is not expert or team", `{"name":"x","kind":"crew"}`, "KIND_INVALID", "kind"},
	}
	var unknownBody string
	for _, c := range rejected {
		act := a.check(t, "TEAM-EXPERT/reject-"+c.id, teamContract, "refuse "+c.desc, httpReq{
			Method: http.MethodPost, Path: "/v1/experts", Headers: headers, Body: c.body,
		}, httpExp{Status: http.StatusBadRequest, BodyIncludes: []string{`"code":"` + c.code + `"`, `"field":"` + c.field + `"`}, BodyExcludes: []string{theirs, "SecretAgent"}})
		switch c.id {
		case "unknown-member":
			unknownBody = act.Body
		case "foreign-member":
			if act.Body != unknownBody {
				t.Errorf("a foreign member must be answered exactly like an unknown one:\n%s\n%s", act.Body, unknownBody)
			}
		}
	}
	a.check(t, "TEAM-EXPERT/no-half-made-team", teamContract, "a refused team leaves no profile behind", httpReq{
		Method: http.MethodGet, Path: "/v1/experts", Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyExcludes: []string{`"kind":"team"`}})

	created := a.check(t, "TEAM-EXPERT/create", teamContract, "create a team as version 1, its members with their names", httpReq{
		Method: http.MethodPost, Path: "/v1/experts", Headers: headers,
		Body: team("reviewer", `{"role":"writer","expert":"`+writer+`","description":"drafts","label":"撰稿人"}`, member("reviewer", reviewer)),
	}, httpExp{Status: http.StatusCreated, BodyIncludes: []string{
		`"kind":"team"`, `"version":1`, `"leader":"reviewer"`,
		`"role":"writer","expert":"` + writer + `","name":"Writer","description":"drafts","label":"撰稿人"`,
		`"role":"reviewer","expert":"` + reviewer + `","name":"Reviewer"`,
	}})
	teamID := decodeField(t, created.Body, "expert_id")
	ref1 := decodeField(t, created.Body, "ref")

	a.check(t, "TEAM-EXPERT/profile-spec", teamContract, "the profile spec is a team with no worker configuration", httpReq{
		Method: http.MethodGet, Path: "/v1/profiles/" + ref1, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"kind":"team"`, `"leader":"reviewer"`}, BodyExcludes: []string{`"instructions"`, `"mcp_connectors"`}})
	a.check(t, "TEAM-EXPERT/list", teamContract, "the list shows the team beside the single experts", httpReq{
		Method: http.MethodGet, Path: "/v1/experts", Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"expert_id":"` + teamID + `"`, `"kind":"team"`, `"kind":"expert"`}})

	ref2 := teamID + "@2"
	a.check(t, "TEAM-EXPERT/update", teamContract, "an update adds version 2 and keeps the kind", httpReq{
		Method: http.MethodPut, Path: "/v1/experts/" + teamID, Headers: headers,
		Body: `{"name":"Crew 2","leader":"writer","members":[` + member("writer", writer) + `]}`,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"ref":"` + ref2 + `"`, `"kind":"team"`, `"leader":"writer"`}, BodyExcludes: []string{`"role":"reviewer"`}})
	a.check(t, "TEAM-EXPERT/v1-immutable", teamContract, "version 1 is exactly what it was", httpReq{
		Method: http.MethodGet, Path: "/v1/profiles/" + ref1, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"leader":"reviewer"`, `"name":"Crew"`, `"role":"reviewer"`}})
	a.check(t, "TEAM-EXPERT/get-latest", teamContract, "reading the team gives its latest version", httpReq{
		Method: http.MethodGet, Path: "/v1/experts/" + teamID, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"ref":"` + ref2 + `"`, `"name":"Crew 2"`}})
	a.check(t, "TEAM-EXPERT/to-single", teamContract, "a team does not turn into a single expert", httpReq{
		Method: http.MethodPut, Path: "/v1/experts/" + teamID, Headers: headers, Body: `{"name":"x","kind":"expert"}`,
	}, httpExp{Status: http.StatusBadRequest})
	a.check(t, "TEAM-EXPERT/to-team", teamContract, "a single expert does not turn into a team", httpReq{
		Method: http.MethodPut, Path: "/v1/experts/" + writerID, Headers: headers, Body: team("lead", member("lead", reviewer)),
	}, httpExp{Status: http.StatusBadRequest})
	a.check(t, "TEAM-EXPERT/self", teamContract, "a team cannot contain itself", httpReq{
		Method: http.MethodPut, Path: "/v1/experts/" + teamID, Headers: headers, Body: `{"name":"Loop","leader":"me","members":[` + member("me", ref1) + `]}`,
	}, httpExp{Status: http.StatusBadRequest})
	a.check(t, "TEAM-EXPERT/nested", teamContract, "a team cannot be a member of another team", httpReq{
		Method: http.MethodPost, Path: "/v1/experts", Headers: headers, Body: team("lead", member("lead", ref1)),
	}, httpExp{Status: http.StatusBadRequest})

	other.check(t, "TEAM-EXPERT/other-tenant-get", teamContract, "another tenant cannot read the team", httpReq{
		Method: http.MethodGet, Path: "/v1/experts/" + teamID, Headers: headers,
	}, httpExp{Status: http.StatusNotFound})
	other.check(t, "TEAM-EXPERT/other-tenant-update", teamContract, "another tenant cannot update the team", httpReq{
		Method: http.MethodPut, Path: "/v1/experts/" + teamID, Headers: headers, Body: team("lead", member("lead", theirs)),
	}, httpExp{Status: http.StatusNotFound})
	other.check(t, "TEAM-EXPERT/other-tenant-list", teamContract, "another tenant's list does not show it", httpReq{
		Method: http.MethodGet, Path: "/v1/experts", Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyExcludes: []string{teamID}})

	// A task selects the team: config.expert is the leader's expert and config.team carries the members.
	madeTask := a.check(t, "TEAM-EXPERT/cfg-create", teamContract, "a task created with a team runs as the leader's expert", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks", Headers: headers,
		Body: `{"title":"crew","goal":"g","config":{"expert":"` + teamID + `@1"}}`,
	}, httpExp{Status: http.StatusCreated, BodyIncludes: []string{`"profile":"` + reviewer + `"`}})
	cfgPath := "/v1/tasks/" + decodeField(t, madeTask.Body, "task_id") + "/config"
	a.check(t, "TEAM-EXPERT/cfg-config", teamContract, "the configuration reads back the leader, the members and their names", httpReq{
		Method: http.MethodGet, Path: cfgPath, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{
		`"config_version":1`, `"expert":"` + reviewer + `"`, `"team_ref":"` + teamID + `@1"`, `"team":{"ref":"` + teamID + `@1","leader":"reviewer"`,
		`"role":"writer","expert":"` + writer + `","name":"Writer","description":"drafts","label":"撰稿人"`,
		`"role":"reviewer","expert":"` + reviewer + `","name":"Reviewer"`,
	}})
	a.check(t, "TEAM-EXPERT/cfg-keep-by-team-ref", teamContract, "saving the read-back expert with team_ref keeps the team", httpReq{
		Method: http.MethodPut, Path: cfgPath, Headers: headers, Body: `{"base_config_version":1,"expert":"` + reviewer + `","team_ref":"` + teamID + `@1"}`,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"config_version":2`}})
	a.check(t, "TEAM-EXPERT/cfg-keep-read", teamContract, "and the team is still there", httpReq{
		Method: http.MethodGet, Path: cfgPath, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"team_ref":"` + teamID + `@1"`}})
	a.check(t, "TEAM-EXPERT/cfg-team-ref-mismatch", teamContract, "another expert beside team_ref is refused", httpReq{
		Method: http.MethodPut, Path: cfgPath, Headers: headers, Body: `{"base_config_version":2,"expert":"` + writer + `","team_ref":"` + teamID + `@1"}`,
	}, httpExp{Status: http.StatusBadRequest, BodyIncludes: []string{`"code":"EXPERT_TEAM_MISMATCH"`}})
	a.check(t, "TEAM-EXPERT/cfg-team-ref-single", teamContract, "team_ref of a single expert is refused", httpReq{
		Method: http.MethodPut, Path: cfgPath, Headers: headers, Body: `{"base_config_version":2,"team_ref":"` + writer + `"}`,
	}, httpExp{Status: http.StatusBadRequest, BodyIncludes: []string{`"code":"TEAM_REF_NOT_TEAM"`, `"field":"team_ref"`}})
	a.check(t, "TEAM-EXPERT/cfg-select-v2", teamContract, "selecting another version of the team replaces the members", httpReq{
		Method: http.MethodPut, Path: cfgPath, Headers: headers, Body: `{"base_config_version":2,"expert":"` + ref2 + `"}`,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"config_version":3`}})
	a.check(t, "TEAM-EXPERT/cfg-config-v2", teamContract, "the task now has version 2 of the team", httpReq{
		Method: http.MethodGet, Path: cfgPath, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"config_version":3`, `"expert":"` + writer + `"`, `"team_ref":"` + ref2 + `"`, `"team":{"ref":"` + ref2 + `","leader":"writer"`}, BodyExcludes: []string{`"role":"reviewer"`}})
	a.check(t, "TEAM-EXPERT/cfg-single", teamContract, "choosing a single expert clears the team", httpReq{
		Method: http.MethodPut, Path: cfgPath, Headers: headers, Body: `{"base_config_version":3,"expert":"` + reviewer + `"}`,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"config_version":4`}})
	a.check(t, "TEAM-EXPERT/cfg-single-read", teamContract, "and it reads back without one", httpReq{
		Method: http.MethodGet, Path: cfgPath, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"expert":"` + reviewer + `"`}, BodyExcludes: []string{`"team"`, `"team_ref"`}})

	foreignSelect := other.check(t, "TEAM-EXPERT/other-tenant-select", teamContract, "another tenant cannot select the team", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks", Headers: headers,
		Body: `{"title":"x","goal":"g","config":{"expert":"` + teamID + `@1"}}`,
	}, httpExp{Status: http.StatusBadRequest, BodyExcludes: []string{"Writer", writer}})
	unknownSelect := other.check(t, "TEAM-EXPERT/other-tenant-select-unknown", teamContract, "and is answered as for an expert that does not exist", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks", Headers: headers,
		Body: `{"title":"x","goal":"g","config":{"expert":"expert_nobody@1"}}`,
	}, httpExp{Status: http.StatusBadRequest})
	if foreignSelect.Body != unknownSelect.Body {
		t.Errorf("a foreign team must be answered exactly like an unknown expert:\n%s\n%s", foreignSelect.Body, unknownSelect.Body)
	}

	again := startServer(t, serverOpts{tenant: teamTenant, maxConns: 4})
	again.check(t, "TEAM-EXPERT/survives-restart", teamContract, "the team is still there, at its latest version, in a fresh control process", httpReq{
		Method: http.MethodGet, Path: "/v1/experts/" + teamID, Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"ref":"` + ref2 + `"`, `"kind":"team"`, `"role":"writer"`}})
}

// max_review_rounds (0 to 20; 0 turns review off) is part of the tenant policy and the task policy.
func TestPolicyMaxReviewRounds(t *testing.T) {
	a := startServer(t, serverOpts{tenant: "t-policy-rounds", maxConns: 4})
	headers := userCSRF(expertUser)
	for _, bad := range []string{"-1", "21"} {
		a.check(t, "TEAM-EXPERT/policy-rounds-refused-"+bad, teamContract, "max_review_rounds "+bad+" is refused", httpReq{
			Method: http.MethodPut, Path: "/v1/policy", Headers: headers, Body: `{"max_review_rounds":` + bad + `}`,
		}, httpExp{Status: http.StatusBadRequest})
	}
	a.check(t, "TEAM-EXPERT/policy-rounds-zero", teamContract, "0 (review off) is stored and read back", httpReq{
		Method: http.MethodPut, Path: "/v1/policy", Headers: headers, Body: `{"max_review_rounds":0}`,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"max_review_rounds":0`}})
	a.check(t, "TEAM-EXPERT/policy-rounds-read", teamContract, "the tenant policy reads back", httpReq{
		Method: http.MethodGet, Path: "/v1/policy", Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"max_review_rounds":0`}})
	a.check(t, "TEAM-EXPERT/cfg-policy-rounds-refused", teamContract, "a task policy over 20 is refused", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks", Headers: headers, Body: `{"title":"x","goal":"g","policy":{"max_review_rounds":21}}`,
	}, httpExp{Status: http.StatusBadRequest})
	a.check(t, "TEAM-EXPERT/cfg-policy-rounds", teamContract, "a task policy with max_review_rounds is accepted", httpReq{
		Method: http.MethodPost, Path: "/v1/tasks", Headers: headers, Body: `{"title":"x","goal":"g","policy":{"max_review_rounds":3}}`,
	}, httpExp{Status: http.StatusCreated, BodyIncludes: []string{`"max_review_rounds":3`}})
}

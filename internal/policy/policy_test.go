package policy

import (
	"slices"
	"strings"
	"testing"
)

func TestEveryRoleAndAction(t *testing.T) {
	expected := map[string][]string{
		"tenant.owner":         {"tenant.read", "tenant.manage", "member.read", "member.manage", "role.manage", "workspace.create", "workspace.read", "workspace.update", "workspace.members", "project.manage", "group.manage", "asset.read", "asset.create", "asset.update", "asset.publish", "asset.review", "asset.use", "session.read", "session.create", "session.update", "session.share", "run.create", "run.control", "audit.read", "apikey.read", "apikey.manage", "model.manage", "secret.manage", "policy.manage", "quota.manage"},
		"tenant.admin":         {"tenant.read", "member.read", "member.manage", "role.manage", "workspace.create", "workspace.read", "workspace.update", "workspace.members", "project.manage", "group.manage", "asset.read", "asset.create", "asset.update", "asset.publish", "asset.review", "asset.use", "session.read", "session.create", "session.update", "session.share", "run.create", "run.control", "audit.read", "apikey.read", "apikey.manage", "model.manage", "secret.manage", "policy.manage", "quota.manage"},
		"tenant.auditor":       {"tenant.read", "audit.read"},
		"tenant.reviewer":      {"tenant.read", "asset.read", "asset.review"},
		"tenant.member":        {"tenant.read", "workspace.create"},
		"ws.admin":             {"workspace.read", "workspace.update", "workspace.members", "project.manage", "asset.read", "asset.create", "asset.update", "asset.publish", "asset.use", "session.read", "session.create", "session.update", "session.share", "run.create", "run.control"},
		"ws.editor":            {"workspace.read", "asset.read", "asset.create", "asset.update", "asset.publish", "asset.use", "session.read", "session.create", "session.update", "run.create", "run.control"},
		"ws.member":            {"workspace.read", "asset.read", "asset.use", "session.create", "run.create"},
		"ws.viewer":            {"workspace.read", "asset.read", "session.read"},
		"asset.owner":          {"asset.read", "asset.update", "asset.publish", "asset.use", "asset.share"},
		"asset.maintainer":     {"asset.read", "asset.update", "asset.publish", "asset.use"},
		"asset.user":           {"asset.read", "asset.use"},
		"session.owner":        {"session.read", "session.update", "session.share", "run.create", "run.control"},
		"session.collaborator": {"session.read", "session.update", "run.create", "run.control"},
		"session.viewer":       {"session.read"},
	}
	cases := []struct {
		role            string
		allowed, denied []string
	}{}
	actions := []string{"platform.read", "unknown"}
	for _, allowed := range expected {
		actions = append(actions, allowed...)
	}
	for _, allowed := range Matrix {
		actions = append(actions, allowed...)
	}
	for role, allowed := range expected {
		denied := []string{}
		for _, action := range actions {
			if !slices.Contains(allowed, action) {
				denied = append(denied, action)
			}
		}
		cases = append(cases, struct {
			role            string
			allowed, denied []string
		}{role, allowed, denied})
		actual := slices.Clone(Matrix[role])
		want := slices.Clone(allowed)
		slices.Sort(actual)
		slices.Sort(want)
		if !slices.Equal(actual, want) {
			t.Errorf("%s matrix got %v want %v", role, actual, want)
		}
	}
	if len(cases) != len(Matrix) {
		t.Fatal("untested role")
	}
	for _, c := range cases {
		t.Run(c.role, func(t *testing.T) {
			scope, _, _ := strings.Cut(c.role, ".")
			if scope == "ws" {
				scope = "workspace"
			}
			resource := Resource{Type: scope, ID: "scope"}
			binding := []Binding{{c.role, scope, "scope"}}
			tenant := "tenant"
			if scope == "tenant" {
				tenant = "scope"
			}
			for _, a := range c.allowed {
				if !Allows(binding, a, resource, tenant) {
					t.Errorf("denied %s", a)
				}
			}
			for _, a := range c.denied {
				if Allows(binding, a, resource, tenant) {
					t.Errorf("allowed %s", a)
				}
			}
			if Allows(binding, c.allowed[0], Resource{Type: "wrong", ID: "other"}, "other-tenant") {
				t.Error("cross-scope access")
			}
		})
	}
}
func TestWorkspaceInheritanceAndDefaultDeny(t *testing.T) {
	b := []Binding{{"ws.admin", "workspace", "w"}}
	if !Allows(b, "session.update", Resource{Type: "session", ID: "s", WorkspaceID: "w"}, "t") {
		t.Fatal("no workspace inheritance")
	}
	if Allows(b, "session.update", Resource{Type: "session", ID: "s", WorkspaceID: "other"}, "t") {
		t.Fatal("cross-workspace access")
	}
	if Allows(nil, "workspace.read", Resource{Type: "workspace", ID: "w"}, "t") {
		t.Fatal("not default deny")
	}
	if ValidRole("tenant.owner", "workspace") || ValidRole("unknown", "tenant") {
		t.Fatal("invalid binding allowed")
	}
}

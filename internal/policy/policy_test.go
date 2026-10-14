package policy

import (
	"strings"
	"testing"
)

func TestEveryRoleAndAction(t *testing.T) {
	// Independent minimal grants establish the intended least-privilege boundaries.
	cases := []struct {
		role            string
		allowed, denied []string
	}{
		{"tenant.owner", []string{"tenant.manage", "member.manage", "workspace.update", "audit.read"}, []string{"platform.read"}},
		{"tenant.admin", []string{"member.manage", "model.manage", "secret.manage", "policy.manage", "quota.manage"}, []string{"tenant.manage", "platform.read"}},
		{"tenant.auditor", []string{"audit.read"}, []string{"member.manage", "workspace.read", "secret.manage"}},
		{"tenant.reviewer", []string{"asset.review", "asset.read"}, []string{"asset.update", "workspace.read", "audit.read"}},
		{"tenant.member", []string{"workspace.create"}, []string{"workspace.read", "member.manage", "asset.review"}},
		{"ws.admin", []string{"workspace.members", "workspace.update", "project.manage"}, []string{"member.manage", "asset.review"}},
		{"ws.editor", []string{"asset.publish", "asset.update", "session.create"}, []string{"workspace.members", "workspace.update"}},
		{"ws.member", []string{"session.create", "asset.use"}, []string{"asset.publish", "workspace.members"}},
		{"ws.viewer", []string{"workspace.read", "session.read"}, []string{"session.create", "run.create", "asset.use"}},
		{"asset.owner", []string{"asset.share", "asset.publish", "asset.update"}, []string{"workspace.read", "asset.review"}},
		{"asset.maintainer", []string{"asset.publish", "asset.update"}, []string{"asset.share", "asset.review"}},
		{"asset.user", []string{"asset.read", "asset.use"}, []string{"asset.update", "asset.publish"}},
		{"session.owner", []string{"session.share", "session.update", "run.control"}, []string{"asset.read"}},
		{"session.collaborator", []string{"session.update", "run.create"}, []string{"session.share"}},
		{"session.viewer", []string{"session.read"}, []string{"session.update", "run.create"}},
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

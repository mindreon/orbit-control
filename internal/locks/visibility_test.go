package locks

import (
	"testing"

	"github.com/mindreon/orbit-control/internal/contract"
)

func TestVisibilityComposition(t *testing.T) {
	for _, tc := range []struct {
		name   string
		layers []Visibility
		want   bool
	}{
		{name: "inherit",
			layers: nil,
			want:   true},
		{name: "tenant allow",
			layers: []Visibility{{Allow: []string{"skill"}}},
			want:   true},
		{name: "author cannot widen admin ceiling",
			layers: []Visibility{{Allow: []string{"other"}},
				{Allow: []string{"skill"}}},
			want: false},
		{name: "filter",
			layers: []Visibility{{Allow: []string{"other"}}},
			want:   false},
		{name: "deny wins",
			layers: []Visibility{{Deny: []string{"skill"}},
				{Allow: []string{"skill"}}},
			want: false},
		{name: "all layers",
			layers: []Visibility{{},
				{},
				{Allow: []string{"skill"}},
				{Deny: []string{"skill"}}},
			want: false}} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Visible("skill", tc.layers...); got != tc.want {
				t.Fatalf("got %v", got)
			}
		})
	}
}
func TestPermissionIntersection(t *testing.T) {
	p := contract.EffectivePermissions{
		SandboxProfile: "default",
		Network:        contract.NetworkPermissions{Egress: []string{"api.example.com", "evil.test"}},
		EffectClasses:  []contract.EffectClass{"external_write", "read_only"},
		Secrets:        []contract.SecretRequirement{{Name: "KEY"}},
	}
	cap := contract.EffectivePermissions{
		SandboxProfile: "default",
		Network:        contract.NetworkPermissions{Egress: []string{"*.example.com"}},
		EffectClasses:  []contract.EffectClass{"read_only"},
	}
	got := Intersect(p, cap)
	if len(got.Network.Egress) != 1 || len(got.Secrets) != 1 || len(got.EffectClasses) != 1 {
		t.Fatal(got)
	}
}

func TestUnconfiguredPermissionDimensionsAreUnrestricted(t *testing.T) {
	p := contract.EffectivePermissions{
		SandboxProfile: "default",
		ToolsBuiltin:   []string{"shell"},
		DataScopes:     []string{"session_files"},
		Secrets:        []contract.SecretRequirement{{Name: "KEY"}},
		EffectClasses:  []contract.EffectClass{"sandbox_write"},
	}
	cap := contract.EffectivePermissions{Network: contract.NetworkPermissions{Egress: []string{"*.example.test"}}}
	got := Intersect(p, cap)
	if len(got.ToolsBuiltin) != 1 || len(got.DataScopes) != 1 || len(got.Secrets) != 1 || len(got.EffectClasses) != 1 ||
		got.SandboxProfile != "default" {
		t.Fatal("unspecified cap denied unrelated permissions", got)
	}
	cap.ToolsBuiltin = []string{}
	cap.SandboxProfile = "restricted"
	got = Intersect(p, cap)
	if len(got.ToolsBuiltin) != 0 || got.SandboxProfile != "" {
		t.Fatal("explicit cap ignored", got)
	}
	if Visible("skill", Visibility{Allow: []string{}}) {
		t.Fatal("explicit empty allow ignored")
	}
	if Visible("skill", Visibility{Allow: []string{"skill"}}, Visibility{Allow: []string{"other"}}, Visibility{Allow: []string{"skill"}}) {
		t.Fatal("author widened workspace cap")
	}
}

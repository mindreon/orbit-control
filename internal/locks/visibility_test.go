package locks

import (
	"github.com/mindreon/orbit-control/internal/contract"
	"testing"
)

func TestVisibilityComposition(t *testing.T) {
	for _, tc := range []struct {
		name   string
		layers []Visibility
		want   bool
	}{
		{"inherit", nil, true}, {"tenant allow", []Visibility{{Allow: []string{"skill"}}}, true}, {"union", []Visibility{{Allow: []string{"other"}}, {Allow: []string{"skill"}}}, true}, {"filter", []Visibility{{Allow: []string{"other"}}}, false}, {"deny wins", []Visibility{{Deny: []string{"skill"}}, {Allow: []string{"skill"}}}, false}, {"all layers", []Visibility{{}, {}, {Allow: []string{"skill"}}, {Deny: []string{"skill"}}}, false}} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Visible("skill", tc.layers...); got != tc.want {
				t.Fatalf("got %v", got)
			}
		})
	}
}
func TestPermissionIntersection(t *testing.T) {
	p := contract.EffectivePermissions{SandboxProfile: "default", Network: contract.NetworkPermissions{Egress: []string{"api.example.com", "evil.test"}}, EffectClasses: []contract.EffectClass{"external_write", "read_only"}, Secrets: []contract.SecretRequirement{{Name: "KEY"}}}
	cap := contract.EffectivePermissions{SandboxProfile: "default", Network: contract.NetworkPermissions{Egress: []string{"*.example.com"}}, EffectClasses: []contract.EffectClass{"read_only"}}
	got := Intersect(p, cap)
	if len(got.Network.Egress) != 1 || len(got.Secrets) != 0 || len(got.EffectClasses) != 1 {
		t.Fatal(got)
	}
}

package assetrelease

import (
	"testing"

	"github.com/mindreon/orbit-control/internal/contract"
)

func TestPermissionExpansion(t *testing.T) {
	empty := contract.EffectivePermissions{}
	for _, p := range []contract.EffectivePermissions{{Network: contract.NetworkPermissions{Egress: []string{"api.test"}}},
		{Secrets: []contract.SecretRequirement{{Name: "KEY"}}},
		{EffectClasses: []contract.EffectClass{"external_write"}}} {
		if !Expanded(empty, p) {
			t.Fatal("missed expansion")
		}
	}
	before := contract.EffectivePermissions{
		Network:       contract.NetworkPermissions{Egress: []string{"*.example.com"}},
		EffectClasses: []contract.EffectClass{"read_only", "privileged"},
	}
	after := contract.EffectivePermissions{
		Network:       contract.NetworkPermissions{Egress: []string{"api.example.com"}},
		EffectClasses: []contract.EffectClass{"read_only"},
	}
	if Expanded(before, after) {
		t.Fatal("narrowing marked expanded")
	}
}

func TestPermissionExpansionSets(t *testing.T) {
	before := contract.EffectivePermissions{
		SandboxProfile: "default",
		EffectClasses:  []contract.EffectClass{"network_read"},
	}
	for _, after := range []contract.EffectivePermissions{
		{SandboxProfile: "default", EffectClasses: []contract.EffectClass{"sandbox_write"}},
		{SandboxProfile: "default", ToolsBuiltin: []string{"shell"}},
		{SandboxProfile: "default", DataScopes: []string{"workspace_shared"}},
		{SandboxProfile: "elevated"},
	} {
		if !Expanded(before, after) {
			t.Fatal("permission expansion missed", after)
		}
	}
}

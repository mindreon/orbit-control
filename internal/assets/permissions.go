package assets

import (
	"slices"

	"github.com/mindreon/orbit-control/internal/contract"
)

func UnionPermissions(a, b contract.EffectivePermissions) contract.EffectivePermissions {
	unique := func(v []string) []string {
		slices.Sort(v)
		return slices.Compact(v)
	}
	a.Network.Egress = unique(append(slices.Clone(a.Network.Egress), b.Network.Egress...))
	a.DataScopes = unique(append(slices.Clone(a.DataScopes), b.DataScopes...))
	a.ToolsBuiltin = unique(append(slices.Clone(a.ToolsBuiltin), b.ToolsBuiltin...))
	for _, s := range b.Secrets {
		if !slices.Contains(a.Secrets, s) {
			a.Secrets = append(a.Secrets, s)
		}
	}
	for _, e := range b.EffectClasses {
		if !slices.Contains(a.EffectClasses, e) {
			a.EffectClasses = append(a.EffectClasses, e)
		}
	}
	return a
}

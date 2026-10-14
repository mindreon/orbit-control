// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// Visibility union/deny precedence adapted from jiuwenswarm; see NOTICE.
package locks

import (
	"context"
	"github.com/mindreon/orbit-control/internal/contract"
	"slices"
)

type Visibility struct {
	Allow []string `json:"allow,omitempty"`
	Deny  []string `json:"deny,omitempty"`
}

func Visible(ref string, layers ...Visibility) bool {
	var allow, deny []string
	for _, l := range layers {
		allow = append(allow, l.Allow...)
		deny = append(deny, l.Deny...)
	}
	return !slices.Contains(deny, ref) && (len(allow) == 0 || slices.Contains(allow, ref))
}
func Intersect(p, cap contract.EffectivePermissions) contract.EffectivePermissions {
	filter := func(values, allowed []string) []string {
		out := []string{}
		for _, v := range values {
			if slices.Contains(allowed, v) {
				out = append(out, v)
			}
		}
		return out
	}
	egress := []string{}
	for _, v := range p.Network.Egress {
		for _, a := range cap.Network.Egress {
			ok, err := contract.MatchGlob(a, v)
			if err == nil && (ok || v == a) {
				egress = append(egress, v)
				break
			}
		}
	}
	p.Network.Egress = egress
	p.DataScopes = filter(p.DataScopes, cap.DataScopes)
	p.ToolsBuiltin = filter(p.ToolsBuiltin, cap.ToolsBuiltin)
	secrets := []contract.SecretRequirement{}
	for _, s := range p.Secrets {
		if slices.Contains(cap.Secrets, s) {
			secrets = append(secrets, s)
		}
	}
	p.Secrets = secrets
	effects := []contract.EffectClass{}
	for _, e := range p.EffectClasses {
		if slices.Contains(cap.EffectClasses, e) {
			effects = append(effects, e)
		}
	}
	p.EffectClasses = effects
	// Profiles are names rather than an ordered resource hierarchy. A mismatching
	// policy denies this profile; the resolver rejects the resulting empty name.
	if cap.SandboxProfile != p.SandboxProfile {
		p.SandboxProfile = ""
	}
	return p
}

// NarrowPermissions applies the same active tenant/workspace/principal caps used
// during lock resolution. Release permission-diff checks use it for both sides
// of the comparison so policy-denied declarations do not count as effective rights.
func NarrowPermissions(ctx context.Context, p contract.EffectivePermissions, workspace string) (contract.EffectivePermissions, error) {
	x := &resolution{ctx: ctx, scope: Scope{WorkspaceID: workspace}}
	if err := x.policies(); err != nil {
		return p, err
	}
	for _, cap := range x.caps {
		p = Intersect(p, cap)
	}
	return p, nil
}

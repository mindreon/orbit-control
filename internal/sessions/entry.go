package sessions

import (
	"context"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
)

// ResolveEntry probes with the caller's visibility, never a raw asset lookup.
func ResolveEntry(ctx context.Context, resolver locks.Resolver, workspace, ref, kind string) (contract.RunLock, error) {
	resolve := func(kind string) (contract.RunLock, error) {
		return resolver.Resolve(ctx, locks.Request{TenantID: auth.Tenant(ctx), LockID: ids.New(),
			Entry: locks.Entry{Kind: contract.EntryKind(kind), Ref: ref}, Scope: locks.Scope{WorkspaceID: workspace}})
	}
	if kind != "" {
		if kind != "agent" && kind != "team" {
			return contract.RunLock{}, httpx.E(422, "INVALID_INPUT", "Invalid entry kind")
		}
		lock, err := resolve(kind)
		if err != nil {
			return lock, httpx.E(422, "LOCK_RESOLUTION_FAILED", "Entry cannot be resolved")
		}
		return lock, nil
	}
	agent, agentErr := resolve("agent")
	team, teamErr := resolve("team")
	if agentErr == nil && teamErr == nil {
		return contract.RunLock{}, httpx.E(422, "ENTRY_KIND_AMBIGUOUS", "Specify entry_kind for this reference")
	}
	if agentErr == nil {
		return agent, nil
	}
	if teamErr == nil {
		return team, nil
	}
	return agent, httpx.E(422, "LOCK_RESOLUTION_FAILED", "Agent cannot be resolved")
}

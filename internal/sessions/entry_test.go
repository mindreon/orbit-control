package sessions

import (
	"context"
	"encoding/json/v2"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/objstore"
)

type kindResolver map[contract.EntryKind]contract.RunLock

func (r kindResolver) Resolve(_ context.Context, in locks.Request) (contract.RunLock, error) {
	lock, ok := r[in.Entry.Kind]
	if !ok {
		return contract.RunLock{}, httpx.E(404, "NOT_FOUND", "Entry not found")
	}
	return lock, nil
}

const tenant = "11111111-1111-4111-8111-111111111111"

func tenantContext() context.Context {
	return auth.Context(context.Background(), auth.Principal{ID: tenant, Kind: "user"}, tenant)
}
func errorCode(t *testing.T, err error) string {
	t.Helper()
	var e *httpx.Error
	require.ErrorAs(t, err, &e)
	return e.Code
}

func TestEntryKindResolution(t *testing.T) {
	// T-P9 (F56, F74): an unqualified reference that resolves as both kinds is ambiguous, but a team the caller
	// cannot see is indistinguishable from no team, so existence is never revealed.
	agent := contract.RunLock{Entry: contract.LockEntryRef{Kind: "agent", Ref: "x"}}
	team := contract.RunLock{Entry: contract.LockEntryRef{Kind: "team", Ref: "x"}}
	_, err := ResolveEntry(tenantContext(), kindResolver{"agent": agent, "team": team}, "w", "x", "")
	require.Equal(t, "ENTRY_KIND_AMBIGUOUS", errorCode(t, err))
	hidden, err := ResolveEntry(tenantContext(), kindResolver{"agent": agent}, "w", "x", "")
	require.NoError(t, err)
	require.Equal(t, contract.EntryKind("agent"), hidden.Entry.Kind)
	chosen, err := ResolveEntry(tenantContext(), kindResolver{"agent": agent, "team": team}, "w", "x", "team")
	require.NoError(t, err)
	require.Equal(t, contract.EntryKind("team"), chosen.Entry.Kind)
	_, err = ResolveEntry(tenantContext(), kindResolver{"team": team}, "w", "x", "agent")
	require.Equal(t, "LOCK_RESOLUTION_FAILED", errorCode(t, err))
	_, err = ResolveEntry(tenantContext(), kindResolver{"agent": agent}, "w", "x", "other")
	require.Equal(t, "INVALID_INPUT", errorCode(t, err))
	_, err = ResolveEntry(tenantContext(), kindResolver{}, "w", "x", "")
	require.Equal(t, "LOCK_RESOLUTION_FAILED", errorCode(t, err))
}

type blobs struct {
	objstore.Store
	data map[string][]byte
}

func (b blobs) Get(_ context.Context, _, digest string, _ int64) ([]byte, error) {
	if raw, ok := b.data[digest]; ok {
		return raw, nil
	}
	return nil, errors.New("missing blob")
}

func TestTeamDefaultBudgetScalesWithMembers(t *testing.T) {
	// T-P9 (F74): a team's default budget is the leader agent's default times min(members, 4); an explicit
	// request wins; an agent run is unscaled.
	raw, err := os.ReadFile("../../contract/v2/examples/AgentManifest.v2.json")
	require.NoError(t, err)
	var manifest contract.AgentManifest
	require.NoError(t, json.Unmarshal(raw, &manifest))
	manifest.Limits = &contract.AgentLimits{DefaultRunBudgetMillicredits: new(int64(30000))}
	raw, err = json.Marshal(manifest)
	require.NoError(t, err)
	digest := "sha256:" + strings.Repeat("a", 64)
	entry := contract.LockEntry{Files: []contract.LockFile{{Path: "agent.json", Sha256: digest}}}
	service := Service{Objects: fakeObjects{blobs: blobs{data: map[string][]byte{digest: raw}}}, DefaultRunBudget: 1}
	lock := func(kind contract.EntryKind, count int) contract.RunLock {
		members := []contract.LockMember{{Name: "lead", Role: "leader", AgentEntry: 0}}
		for range count - 1 {
			members = append(members, contract.LockMember{Name: "m", Role: "teammate", AgentEntry: 0})
		}
		return contract.RunLock{Entry: contract.LockEntryRef{Kind: kind}, Entries: []contract.LockEntry{entry}, Members: members}
	}
	for _, tc := range []struct {
		kind    contract.EntryKind
		members int
		want    int64
	}{{"agent", 1, 30000}, {"team", 3, 90000}, {"team", 6, 120000}} {
		got, err := service.submissionBudget(tenantContext(), lock(tc.kind, tc.members), nil)
		require.NoError(t, err)
		require.Equal(t, tc.want, got, "%s with %d members", tc.kind, tc.members)
	}
	requested := int64(5000)
	got, err := service.submissionBudget(tenantContext(), lock("team", 3), &requested)
	require.NoError(t, err)
	require.Equal(t, requested, got)
}

type fakeObjects struct {
	objstore.ObjectStore
	blobs blobs
}

func (f fakeObjects) Get(ctx context.Context, tenant, digest string, limit int64) ([]byte, error) {
	return f.blobs.Get(ctx, tenant, digest, limit)
}

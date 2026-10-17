package integration

import (
	"context"
	"encoding/json/v2"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
)

// visibilityResolver models the lock resolver's visibility boundary: the caller cannot distinguish a private
// team they do not own from a missing team. The real resolver's RBAC is covered by the asset integration suite.
type visibilityResolver struct {
	agent, team       contract.RunLock
	teamVisible       bool
	privateTeamExists bool
}

func (r visibilityResolver) Resolve(ctx context.Context, in locks.Request) (contract.RunLock, error) {
	if in.Entry.Kind == "team" {
		if !r.privateTeamExists || !r.teamVisible {
			return contract.RunLock{}, httpx.E(404, "NOT_FOUND", "Entry not found")
		}
		return runResolver{lock: r.team}.Resolve(ctx, in)
	}
	return runResolver{lock: r.agent}.Resolve(ctx, in)
}
func testTeamEntryHTTP(t *testing.T, d *db.DB, admin *pgx.Conn) {
	f := newRunFixture(t, d, admin)
	agent, team := readLock(t, "RunLock.agent.json"), readLock(t, "RunLock.team.json")
	resolver := &visibilityResolver{agent: agent, team: team}
	h, token := f.api(t, agent, &teamTemporal{}, func(h *httpapi.Handler) { h.Locks = resolver })
	send := func(kind string) (int, []byte) {
		input := map[string]string{"workspace_id": f.workspace, "entry_ref": "same/reference", "title": "Entry"}
		if kind != "" {
			input["entry_kind"] = kind
		}
		raw, err := json.Marshal(input)
		require.NoError(t, err)
		return f.call(t, h, token, "POST", "/v1/sessions", string(raw), "")
	}
	normalize := func(raw []byte) []byte {
		var value map[string]any
		require.NoError(t, json.Unmarshal(raw, &value))
		// Session identities and clocks are generated per create, independent of the entry's visibility.
		for _, key := range []string{"id", "created_at", "last_activity_at"} {
			delete(value, key)
		}
		out, err := json.Marshal(value, json.Deterministic(true))
		require.NoError(t, err)
		return out
	}
	status, absent := send("")
	require.Equal(t, 201, status, string(absent))
	resolver.privateTeamExists = true
	status, hidden := send("")
	require.Equal(t, 201, status, string(hidden))
	require.Equal(t, string(normalize(absent)), string(normalize(hidden)))
	resolver.teamVisible = true
	status, raw := send("")
	require.Equal(t, 422, status, string(raw))
	require.Equal(t, "ENTRY_KIND_AMBIGUOUS", errorCode(t, raw))
	status, raw = send("team")
	require.Equal(t, 201, status, string(raw))
	var created struct {
		Kind string `json:"entry_kind"`
	}
	require.NoError(t, json.Unmarshal(raw, &created))
	require.Equal(t, "team", created.Kind)
}

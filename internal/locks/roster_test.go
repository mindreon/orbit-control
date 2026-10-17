package locks

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/contract"
)

func roster(names ...string) []contract.LockMember {
	members := make([]contract.LockMember, len(names))
	for i, name := range names {
		members[i] = contract.LockMember{Name: contract.MemberName(name), Role: "teammate"}
	}
	members[0].Role = "leader"
	return members
}

func TestTeamRosterValidation(t *testing.T) {
	// T-P10 (F55): expanded team rosters must be addressable member names with exactly one leader, at most 16.
	require.NoError(t, validateTeamRoster(roster("lead", "alice", "bob_2", "r.1-x")))
	many := make([]string, 0, 17)
	for i := range 17 {
		many = append(many, fmt.Sprintf("m%d", i))
	}
	for name, members := range map[string][]contract.LockMember{
		"duplicate":      roster("lead", "alice", "alice"),
		"reserved main":  roster("lead", "main"),
		"reserved team":  roster("lead", "team"),
		"too long":       roster("lead", strings.Repeat("a", 65)),
		"bad pattern":    roster("lead", "-leading"),
		"bad characters": roster("lead", "a b"),
		"over 16":        roster(many...),
		"no leader":      {{Name: "a", Role: "teammate"}, {Name: "b", Role: "teammate"}},
		"two leaders":    {{Name: "a", Role: "leader"}, {Name: "b", Role: "leader"}},
	} {
		require.Error(t, validateTeamRoster(members), name)
	}
	require.NoError(t, validateTeamRoster(roster(many[:16]...)), "exactly 16 members are allowed")
	require.NoError(t, validateTeamRoster(roster("lead", strings.Repeat("a", 64))))
}

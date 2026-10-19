package usage

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/contract"
)

func TestClampOccurredAt(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		offset  time.Duration
		clamped bool
	}{
		{offset: -time.Hour, clamped: false},
		{offset: 5 * time.Minute, clamped: false},
		{offset: -time.Hour - time.Nanosecond, clamped: true},
		{offset: 5*time.Minute + time.Nanosecond, clamped: true},
	} {
		value, clamped := ClampOccurredAt(now.Add(test.offset), now)
		require.Equal(t, test.clamped, clamped)
		if clamped {
			require.Equal(t, now, value)
		} else {
			require.Equal(t, now.Add(test.offset), value)
		}
	}
}

func TestReportQuantitySQL(t *testing.T) {
	for _, meter := range []bool{false, true} {
		o := ReportOptions{Granularity: "day"}
		if meter {
			o.GroupBy = []string{"meter"}
		}
		query := ReportSQL(o)
		require.Equal(t, meter, strings.Contains(query, "sum(r.quantity)"))
		if meter {
			require.Contains(t, query, "r.meter")
		} else {
			require.Contains(t, query, "NULL::text AS quantity")
		}
	}
}

func TestReportOptions(t *testing.T) {
	now := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	o := ReportOptions{}
	require.NoError(t, o.Validate(now))
	require.Equal(t, now, o.To)
	require.Equal(t, now.Add(-7*24*time.Hour), o.From)
	for _, bad := range []ReportOptions{
		{Scope: "tenant", ID: "01234567-1234-1234-1234-0123456789ab"},
		{Scope: "other"}, {Scope: "workspace"}, {Granularity: "year"}, {GroupBy: []string{"bad"}},
		{From: now.Add(-94 * 24 * time.Hour), To: now}, {From: now.Add(-time.Minute), To: now},
	} {
		require.Error(t, bad.Validate(now))
	}
}

func TestAssetRef(t *testing.T) {
	lock := contract.RunLock{Entry: contract.LockEntryRef{Kind: "team", Ref: "test/team", Version: "1.0.0"},
		Entries: []contract.LockEntry{{Kind: "agent", Ref: "test/agent", Version: "2.0.0"}},
		Members: []contract.LockMember{{Name: "alice", AgentEntry: 0}, {Name: "invalid", AgentEntry: 1}}}
	require.Equal(t, "agent:test/agent@2.0.0", *AssetRef(lock, "alice"))
	for _, member := range []string{"main", "team", "unknown"} {
		require.Equal(t, "team:test/team@1.0.0", *AssetRef(lock, member))
	}
	require.Nil(t, AssetRef(lock, "invalid"))
	require.Nil(t, AssetRef(contract.RunLock{}, "main"))
}

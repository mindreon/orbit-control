package sessions

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEventQuery(t *testing.T) {
	q, err := ParseEventQuery(url.Values{})
	require.NoError(t, err)
	require.Equal(t, 200, q.Limit)
	require.Zero(t, q.After)
	for _, raw := range []string{
		"after=-1", "after=+1", "after=18446744073709551616", "limit=0", "limit=501", "limit=bad",
		"types=message.final&exclude=reasoning.delta", "types=unknown", "exclude=", "types=run.started&types=run.failed",
	} {
		t.Run(raw, func(t *testing.T) {
			q, err := url.ParseQuery(raw)
			require.NoError(t, err)
			_, err = ParseEventQuery(q)
			require.Error(t, err)
		})
	}
	q, err = ParseEventQuery(url.Values{"after": {"42"}, "limit": {"500"}, "exclude": {"reasoning.delta,sandbox.output"}})
	require.NoError(t, err)
	require.EqualValues(t, 42, q.After)
	require.Equal(t, 500, q.Limit)
	require.True(t, q.Filter.Allows("message.final"))
	require.False(t, q.Filter.Allows("reasoning.delta"))
}

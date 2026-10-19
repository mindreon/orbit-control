package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlatformOperators(t *testing.T) {
	for _, raw := range []string{"", "01234567-1234-1234-1234-0123456789ab", "invalid", "01234567-1234-1234-1234-0123456789ab,"} {
		t.Setenv("ORBIT_PLATFORM_OPERATORS", raw)
		c, err := Load()
		if raw == "invalid" || len(raw) > 0 && raw[len(raw)-1] == ',' {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, raw != "", c.PlatformOperators["01234567-1234-1234-1234-0123456789ab"])
	}
	t.Setenv("ORBIT_PLATFORM_OPERATORS", "")
	t.Setenv("ORBIT_OPS_TRACE_URL_TEMPLATE", "https://example.test/{content}")
	_, err := Load()
	require.Error(t, err)
}

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetricLimits(t *testing.T) {
	t.Setenv("ORBIT_METRICS_TENANT_LIMIT", "")
	t.Setenv("ORBIT_METRICS_MODEL_LIMIT", "")
	tenant, model, err := metricLimits()
	require.NoError(t, err)
	require.Equal(t, 500, tenant)
	require.Equal(t, 50, model)
	t.Setenv("ORBIT_METRICS_TENANT_LIMIT", "1")
	t.Setenv("ORBIT_METRICS_MODEL_LIMIT", "2")
	tenant, model, err = metricLimits()
	require.NoError(t, err)
	require.Equal(t, 1, tenant)
	require.Equal(t, 2, model)
	for _, name := range []string{
		"ORBIT_METRICS_TENANT_LIMIT", "ORBIT_METRICS_MODEL_LIMIT",
	} {
		t.Setenv(name, "0")
		_, _, err = metricLimits()
		require.ErrorContains(t, err, name)
		t.Setenv(name, "1")
	}
}

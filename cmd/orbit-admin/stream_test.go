package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/ids"
)

func TestDevelopmentStreamToken(t *testing.T) {
	t.Setenv("ORBIT_ENV", "dev")
	require.ErrorContains(t, issueStreamToken(config.Config{Environment: "prod"}, nil), "ORBIT_ENV=dev")
	t.Setenv("ORBIT_ENV", "prod")
	path := t.TempDir() + "/stream.key"
	_, err := auth.IssueDevelopmentStreamToken(config.Config{Environment: "dev"}, path, "test", nil, auth.StreamClaims{}, -time.Second)
	require.ErrorContains(t, err, "must already exist")
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
	_, err = auth.LoadSigner(path)
	require.NoError(t, err)
	claims := auth.StreamClaims{TenantID: ids.New(), SessionID: ids.New(), Scope: "session"}
	token, err := auth.IssueDevelopmentStreamToken(config.Config{Environment: "dev"}, path, "test", nil, claims, -time.Second)
	require.NoError(t, err)
	require.NotEmpty(t, token)
	require.ErrorContains(t, issueStreamToken(config.Config{}, []string{"--ttl", "bad"}), "invalid value")
	options, err := parseAdminOptions([]string{"maintenance", "prune", "--control-events-before", "2026-01-01T00:00:00Z",
		"--run-events-before", "2026-01-01T00:00:00Z", "--rejects-before", "2026-01-01T00:00:00Z"})
	require.NoError(t, err)
	require.Equal(t, "2026-01-01T00:00:00Z", options.controlEventsBefore)
	require.Equal(t, "2026-01-01T00:00:00Z", options.runEventsBefore)
	require.Equal(t, "2026-01-01T00:00:00Z", options.rejectsBefore)
}

package devseed

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTeamFilesUseBundleRootForLeader(t *testing.T) {
	files, err := teamFiles()
	require.NoError(t, err)
	require.NotEmpty(t, files["AGENTS.md"])
	_, ok := files["agent.json"]
	require.True(t, ok)

	var manifest struct {
		Roles []struct {
			Name  string `json:"name"`
			Role  string `json:"role"`
			Agent string `json:"agent"`
		} `json:"roles"`
	}
	require.NoError(t, json.Unmarshal(files["team.json"], &manifest))

	leaders := 0
	for _, role := range manifest.Roles {
		if role.Role == "leader" {
			leaders++
			require.Equal(t, "leader", role.Name)
			require.Equal(t, ".", role.Agent)
		}
		if !strings.HasPrefix(role.Agent, "./") {
			continue
		}
		prefix := strings.TrimPrefix(role.Agent, "./")
		require.NotEmpty(t, files[prefix+"/AGENTS.md"])
		_, ok = files[prefix+"/agent.json"]
		require.True(t, ok)
	}
	require.Equal(t, 1, leaders)
}

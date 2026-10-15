package contract

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/platform/jcs"
)

func TestSharedEffectKeyVector(t *testing.T) {
	var vector struct {
		RunID             string         `json:"run_id"`
		Member            string         `json:"member"`
		CheckpointVersion int            `json:"checkpoint_version"`
		Tool              string         `json:"tool"`
		Args              map[string]any `json:"args"`
		Canonical         string         `json:"canonical"`
		Digest            string         `json:"digest"`
	}
	readFixture(t, "effect-key.json", &vector)
	raw, err := jcs.Marshal([]any{vector.RunID, vector.Member, vector.CheckpointVersion, vector.Tool, vector.Args})
	require.NoError(t, err)
	require.Equal(t, vector.Canonical, string(raw))
	digest, err := ObjectDigest(raw)
	require.NoError(t, err)
	const expected = "sha256:503ead8a47ee146545bab266e988259acea4ad59b797cf3ecb764b40ddf9ab67"
	require.Equal(t, expected, digest)
	require.Equal(t, expected, vector.Digest)
}

package devseed

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModelRequiresEveryVariable(t *testing.T) {
	names := []string{"ORBIT_SEED_MODEL_BASE_URL", "ORBIT_SEED_MODEL_API_KEY", "ORBIT_SEED_MODEL_NAME"}
	for _, missing := range names {
		t.Run(missing, func(t *testing.T) {
			for _, name := range names {
				t.Setenv(name, "fixture")
			}
			t.Setenv(missing, "")
			_, err := ModelFromEnv()
			require.EqualError(t, err, missing+" is required")
		})
	}
}

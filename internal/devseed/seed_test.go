package devseed

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/secrets"
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

func TestNonDevelopmentSeedRejected(t *testing.T) {
	for _, environment := range []string{"", "prod", "staging"} {
		_, err := Seed(t.Context(), nil, nil, secrets.Keys{}, Model{}, environment)
		require.EqualError(t, err, "seed-dev requires ORBIT_ENV=dev")
		err = WaitPublished(t.Context(), nil, nil, Result{}, environment)
		require.EqualError(t, err, "seed-dev requires ORBIT_ENV=dev")
	}
}

package main

import (
	"testing"

	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/secrets"
)

func TestSeedDevelopmentRejectsNonDev(t *testing.T) {
	for _, environment := range []string{"", "prod", "staging"} {
		err := seedDevelopment(t.Context(), nil, config.Config{Environment: environment}, secrets.Keys{}, []string{"seed-dev"})
		if err == nil || err.Error() != "seed-dev requires ORBIT_ENV=dev" {
			t.Fatalf("%q: %v", environment, err)
		}
	}
}

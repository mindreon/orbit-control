package auth

import (
	"fmt"
	"os"
	"time"

	"github.com/mindreon/orbit-control/internal/platform/config"
)

// IssueDevelopmentStreamToken supports negative TTLs for gateway acceptance.
// It never creates a key and requires explicit development mode.
func IssueDevelopmentStreamToken(
	c config.Config, path, kid string, keys map[string][]byte, claims StreamClaims, ttl time.Duration,
) (string, error) {
	if c.Environment != "dev" {
		return "", fmt.Errorf("issue-stream-token requires ORBIT_ENV=dev")
	}
	if ttl > 15*time.Minute {
		return "", fmt.Errorf("stream token TTL cannot exceed 15m")
	}
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("ORBIT_STREAM_KEY_FILE must already exist")
	}
	key, err := LoadSigner(path)
	if err != nil {
		return "", err
	}
	signer, err := NewStreamSigner(key.key, kid, keys)
	if err != nil {
		return "", err
	}
	return signer.issue(claims, ttl)
}

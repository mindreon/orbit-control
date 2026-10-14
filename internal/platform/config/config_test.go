package config

import (
	"strings"
	"testing"
)

func TestDevBindGate(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:8080", "[::1]:8080", "localhost:8080", ":8080", "0.0.0.0:8080"} {
		t.Run(addr, func(t *testing.T) {
			c := Config{DatabaseURL: "postgres://app@localhost/orbit", Addr: addr, AuthMode: "dev", KeyFile: "key"}
			e := c.ValidateAPI()
			loopback := strings.HasPrefix(addr, "127") || strings.HasPrefix(addr, "[::1]") || strings.HasPrefix(addr, "localhost")
			if (e == nil) != loopback {
				t.Fatal(e)
			}
			c.AllowDevNetwork = true
			if e = c.ValidateAPI(); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestMissingEnvNames(t *testing.T) {
	c := Config{AuthMode: "oidc"}
	if e := c.ValidateAPI(); e == nil || !strings.Contains(e.Error(), "ORBIT_DATABASE_URL") {
		t.Fatal(e)
	}
}

package config

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestDevBindGate(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:8080", "[::1]:8080", "localhost:8080", ":8080", "0.0.0.0:8080"} {
		t.Run(addr, func(t *testing.T) {
			c := Config{DatabaseURL: "postgres://app@localhost/orbit", Addr: addr, AuthMode: "dev", KeyFile: "key", CapabilityKeyFile: "capability", CapabilityKeyID: "1", SecretsKeyVersion: 1, SecretsKEKs: map[int][]byte{1: make([]byte, 32)}}
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

func TestKeyAndOriginConfiguration(t *testing.T) {
	t.Setenv("ORBIT_PERSONAL_TENANTS", "")
	t.Setenv("ORBIT_SECRETS_KEK", "")
	t.Setenv("ORBIT_SECRETS_KEK_PREVIOUS", "")
	t.Setenv("ORBIT_SECRETS_KEY_VERSION", "")
	t.Setenv("ORBIT_CAPABILITY_PUBLIC_KEYS", "")
	c, err := Load()
	if err != nil || c.PersonalTenants {
		t.Fatal(c, err)
	}
	encoded := base64.StdEncoding.EncodeToString(make([]byte, 32))
	t.Setenv("ORBIT_PERSONAL_TENANTS", "true")
	t.Setenv("ORBIT_PUBLIC_ORIGIN", "https://orbit.example")
	t.Setenv("ORBIT_SECRETS_KEK", encoded)
	t.Setenv("ORBIT_SECRETS_KEY_VERSION", "2")
	t.Setenv("ORBIT_SECRETS_KEK_PREVIOUS", "1:"+encoded)
	t.Setenv("ORBIT_CAPABILITY_PUBLIC_KEYS", "previous:"+encoded)
	c, err = Load()
	if err != nil || !c.PersonalTenants || c.SecretsKeyVersion != 2 || len(c.SecretsKEKs) != 2 || len(c.CapabilityPublicKeys) != 1 {
		t.Fatal(c, err)
	}
	for _, test := range []struct{ name, value string }{
		{"ORBIT_PUBLIC_ORIGIN", "https://orbit.example/path"}, {"ORBIT_SECRETS_KEK", "bad"}, {"ORBIT_SECRETS_KEY_VERSION", "0"}, {"ORBIT_SECRETS_KEK_PREVIOUS", "2:" + encoded}, {"ORBIT_CAPABILITY_PUBLIC_KEYS", "kid:short"}, {"ORBIT_PERSONAL_TENANTS", "yes"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(test.name, test.value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), test.name) {
				t.Fatal(err)
			}
		})
	}
}

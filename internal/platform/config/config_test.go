package config

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestDevBindGate(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:8080", "[::1]:8080", "localhost:8080", ":8080", "0.0.0.0:8080"} {
		t.Run(addr, func(t *testing.T) {
			c := Config{
				DatabaseURL:       "postgres://app@localhost/orbit",
				Addr:              addr,
				AuthMode:          "dev",
				KeyFile:           "key",
				CapabilityKeyFile: "capability",
				CapabilityKeyID:   "1",
				StreamKeyFile:     "stream.key",
				StreamKeyID:       "1",
				SecretsKeyVersion: 1,
				SecretsKEKs:       map[int][]byte{1: make([]byte, 32)},
			}
			e := c.ValidateAPI()
			loopback := strings.HasPrefix(addr, "127") || strings.HasPrefix(addr, "[::1]") ||
				strings.HasPrefix(addr, "localhost")
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
	if err != nil || !c.PersonalTenants || c.SecretsKeyVersion != 2 || len(c.SecretsKEKs) != 2 ||
		len(c.CapabilityPublicKeys) != 1 {
		t.Fatal(c, err)
	}
	for _, test := range []struct{ name, value string }{
		{name: "ORBIT_PUBLIC_ORIGIN",
			value: "https://orbit.example/path"},
		{name: "ORBIT_SECRETS_KEK",
			value: "bad"},
		{name: "ORBIT_SECRETS_KEY_VERSION",
			value: "0"},
		{name: "ORBIT_SECRETS_KEK_PREVIOUS",
			value: "2:" +
				encoded},
		{name: "ORBIT_CAPABILITY_PUBLIC_KEYS",
			value: "kid:short"},
		{name: "ORBIT_PERSONAL_TENANTS",
			value: "yes"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv(test.name, test.value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), test.name) {
				t.Fatal(err)
			}
		})
	}
}

func TestAssetWorkerConfiguration(t *testing.T) {
	c := Config{
		DatabaseURL:       "postgres://orbit_app@localhost/orbit",
		TemporalAddress:   "localhost:7233",
		TemporalNamespace: "orbit-dev",
	}
	if err := c.ValidateWorker(); err != nil {
		t.Fatal("worker should not require OIDC or HTTP credentials", err)
	}
	c.TemporalCertFile = "client.pem"
	if err := c.ValidateWorker(); err == nil {
		t.Fatal("unpaired TLS cert accepted")
	}
	c.TemporalKeyFile = "client.key"
	if err := c.ValidateWorker(); err == nil {
		t.Fatal("ignored TLS credentials")
	}
	c.TemporalTLS = true
	if err := c.ValidateWorker(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ORBIT_TEMPORAL_TLS", "ORBIT_OBJECT_TLS"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(name, "invalid")
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), name) {
				t.Fatal(err)
			}
		})
	}
	c = Config{
		ObjectEndpoint:    "localhost:9000",
		ObjectBucket:      "orbit",
		ObjectAccessKey:   "test",
		ObjectSecretKey:   "test",
		AssetKeyFile:      "asset.key",
		AssetKeyID:        "1",
		KeyFile:           "auth.key",
		CapabilityKeyFile: "cap.key",
	}
	if err := c.ValidateAssets(); err != nil {
		t.Fatal(err)
	}
	c.AssetKeyFile = c.KeyFile
	if err := c.ValidateAssets(); err == nil {
		t.Fatal("shared asset/auth key allowed")
	}
}

func TestSinkConfiguration(t *testing.T) {
	for _, test := range []struct{ name, value string }{
		{name: "ORBIT_SINK_ENABLED", value: "yes"},
		{name: "ORBIT_OUTBOX_INTERVAL", value: "0s"},
		{name: "ORBIT_OUTBOX_INTERVAL", value: "bad"},
	} {
		t.Run(test.name+test.value, func(t *testing.T) {
			t.Setenv(test.name, test.value)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), test.name) {
				t.Fatal(err)
			}
		})
	}
}

func TestStreamConfiguration(t *testing.T) {
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if err = c.validateStreamKeys(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{c.KeyFile, c.CapabilityKeyFile, c.AssetKeyFile, ".orbit-data/../.orbit-data/signing.key"} {
		next := c
		next.StreamKeyFile = path
		if err := next.validateStreamKeys(); err == nil {
			t.Fatal("shared stream key accepted")
		}
	}
	for _, test := range []struct{ name, value string }{
		{name: "ORBIT_STREAM_PUBLIC_KEYS", value: "kid:bad"},
		{name: "ORBIT_STREAM_PUBLIC_KEYS", value: "kid:" + base64.StdEncoding.EncodeToString(make([]byte, 32)) +
			",kid:" + base64.StdEncoding.EncodeToString(make([]byte, 32))},
		{name: "ORBIT_NATS_STREAM_MAX_BYTES", value: "-1"},
	} {
		t.Run(test.name+test.value, func(t *testing.T) {
			t.Setenv(test.name, test.value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), test.name) {
				t.Fatal(err)
			}
		})
	}
}

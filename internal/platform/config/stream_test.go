package config

import (
	"crypto/ed25519"
	"encoding/base64"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStreamGatewayConfig(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	t.Setenv("ORBIT_NATS_URL", "nats://gateway:password@localhost:4222")
	t.Setenv("ORBIT_STREAM_PUBLIC_KEYS", "test:"+base64.StdEncoding.EncodeToString(pub))
	// Irrelevant invalid private/control configuration must not affect this binary.
	t.Setenv("ORBIT_DATABASE_URL", "invalid")
	t.Setenv("ORBIT_SECRETS_KEK", "invalid")
	t.Setenv("ORBIT_STREAM_KEY_FILE", "/must/not/read")
	c, err := LoadStreamGateway()
	require.NoError(t, err)
	require.Equal(t, 10000, c.MaxConnections)
	require.Equal(t, 4<<20, c.BufferBytes)
	require.Equal(t, 15*time.Second, c.Heartbeat)
	for _, v := range []struct{ key, value string }{
		{key: "ORBIT_STREAM_PUBLIC_KEYS", value: ""}, {key: "ORBIT_NATS_URL", value: "invalid"},
		{key: "ORBIT_STREAM_BUFFER_BYTES", value: "42"}, {key: "ORBIT_STREAM_BUFFER_FRAMES", value: "-1"},
		{key: "ORBIT_STREAM_HEARTBEAT", value: "0s"}, {key: "ORBIT_STREAM_WRITE_TIMEOUT", value: "bad"},
	} {
		t.Run(v.key, func(t *testing.T) {
			t.Setenv(v.key, v.value)
			_, err := LoadStreamGateway()
			require.ErrorContains(t, err, v.key)
		})
	}
	for _, tc := range []struct {
		value string
		ok    bool
	}{{value: "65536"}, {value: "131071"}, {value: "131072", ok: true}} {
		t.Run("ORBIT_STREAM_BUFFER_BYTES="+tc.value, func(t *testing.T) {
			t.Setenv("ORBIT_STREAM_BUFFER_BYTES", tc.value)
			_, err := LoadStreamGateway()
			if tc.ok {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "ORBIT_STREAM_BUFFER_BYTES must be at least 131072")
		})
	}
}

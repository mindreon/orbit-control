package main

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEdgeCoverage(t *testing.T) {
	dir := t.TempDir()
	api := dir + "/openapi.yaml"
	require.NoError(t, os.WriteFile(api, []byte(`paths:
  /v1/runs/{r}/events:
    x-served-by: stream-gateway
  /v1/sessions/{s}/events:
    x-served-by: stream-gateway
  /v1/sessions/{s}/history:
    get: {}
`), 0600))
	for _, test := range []struct {
		name, config string
		valid        bool
	}{
		{name: "complete", config: "@stream path_regexp stream ^/v1/(runs|sessions)/[^/]+/events$", valid: true},
		{name: "missing-session", config: "@stream path_regexp stream ^/v1/runs/[^/]+/events$"},
		{name: "too-broad", config: "@stream path_regexp stream ^/v1/"},
		{name: "no-matcher", config: "handle /v1/* { reverse_proxy control:8080 }"},
		{name: "invalid", config: "@stream path_regexp stream ["},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := dir + "/" + test.name
			require.NoError(t, os.WriteFile(path, []byte(test.config), 0600))
			err := check(path, api)
			if test.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	require.Error(t, check(dir+"/absent", api))
}

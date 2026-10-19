package objstore

import (
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/platform/ids"
)

func TestOpenObject(t *testing.T) {
	tenant := ids.New()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Query().Has("location") {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/missing") {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(404)
			_, _ = io.WriteString(w, `<Error><Code>NoSuchKey</Code><Message>missing</Message></Error>`)
			return
		}
		require.Equal(t, "/bucket/t/"+tenant+"/artifacts/data", r.URL.Path)
		w.Header().Set("Content-Length", "5")
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Last-Modified", "Mon, 19 Oct 2026 12:00:00 GMT")
		w.Header().Set("ETag", `"test"`)
		_, _ = io.WriteString(w, "bytes")
	}))
	defer server.Close()
	s, err := New(strings.TrimPrefix(server.URL, "http://"), "bucket", "test-access", "test-secret", false)
	require.NoError(t, err)
	body, size, err := s.OpenObject(t.Context(), tenant, "t/"+tenant+"/artifacts/data")
	require.NoError(t, err)
	require.Equal(t, int64(5), size)
	raw, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, "bytes", string(raw))
	require.NoError(t, body.Close())
	body, _, err = s.OpenObject(t.Context(), tenant, "t/"+tenant+"/artifacts/missing")
	require.Error(t, err, "lazy GetObject failure is returned before committing HTTP headers")
	require.True(t, errors.Is(err, fs.ErrNotExist), "NoSuchKey must be classified as a missing object")
	require.Nil(t, body)
	before := requests
	body, _, err = s.OpenObject(t.Context(), tenant, "t/"+ids.New()+"/artifacts/data")
	require.Error(t, err)
	require.Nil(t, body)
	require.Equal(t, before, requests, "cross-tenant keys never reach storage")
}

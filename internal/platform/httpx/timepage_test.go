package httpx

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/platform/ids"
)

func TestTimeCursorRoundTrip(t *testing.T) {
	at := time.Date(2026, 10, 19, 12, 1, 2, 123456000, time.FixedZone("offset", 3600))
	id := ids.New()
	page, err := TimePagination(httptest.NewRequest("GET", "/list?limit=1&cursor="+TimeCursor(at, id), nil))
	require.NoError(t, err)
	got, key, err := TimeKey(page.Cursor)
	require.NoError(t, err)
	require.True(t, at.Equal(got))
	require.Equal(t, id, key)
	require.Equal(t, 1, page.Limit)
	for _, cursor := range []string{"bad", "2026-10-19T12:00:00Z|bad", Cursor(id), Cursor("bad|" + id)} {
		_, err = TimePagination(httptest.NewRequest("GET", "/list?cursor="+cursor, nil))
		require.Error(t, err)
	}
}

type watchedBody struct {
	io.Reader
	reads, closed int
}

func (b *watchedBody) Read(p []byte) (int, error) {
	b.reads++
	return b.Reader.Read(p)
}
func (b *watchedBody) Close() error {
	b.closed++
	return nil
}

func TestStreamResponseDoesNotBuffer(t *testing.T) {
	body := &watchedBody{Reader: strings.NewReader("streamed bytes")}
	inner, outer := NewRecorder(), NewRecorder()
	inner.Header().Set("Content-Type", "text/plain")
	StreamResponse(inner, body)
	inner.FlushTo(outer)
	require.Zero(t, body.reads)
	require.Empty(t, inner.Body)
	require.Empty(t, outer.Body)
	out := httptest.NewRecorder()
	outer.FlushTo(out)
	require.Equal(t, "streamed bytes", out.Body.String())
	require.Equal(t, "text/plain", out.Header().Get("Content-Type"))
	require.Equal(t, 1, body.closed)
}

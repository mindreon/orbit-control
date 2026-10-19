package httpapi

import (
	"encoding/json/v2"
	"mime"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/sessions"
)

func TestArtifactHeaders(t *testing.T) {
	for _, media := range []string{"image/png", "image/jpeg", "image/gif", "image/webp", "image/avif", "application/pdf",
		"text/plain", "text/markdown", "text/csv", "TEXT/PLAIN; charset=UTF-8", "image/x+xml", "IMAGE/SVG+XML",
		"image/svg+xml", "text/html", "application/javascript", "application/octet-stream", "bad mime"} {
		for _, inline := range []bool{false, true} {
			t.Run(media+"/"+map[bool]string{false: "download", true: "preview"}[inline], func(t *testing.T) {
				header := make(http.Header)
				artifactHeaders(header, sessions.Artifact{Path: "outputs/报告.txt", Mime: media, SizeBytes: 1 << 40}, inline)
				typ, params, err := mime.ParseMediaType(media)
				if err != nil {
					typ, params = "application/octet-stream", nil
				}
				safe := slices.Contains([]string{"image/png", "image/jpeg", "image/gif", "image/webp", "image/avif",
					"application/pdf", "text/plain", "text/markdown", "text/csv"}, typ)
				disposition := "attachment"
				if inline && safe {
					disposition = "inline"
				}
				require.Equal(t, disposition+"; filename*=UTF-8''%E6%8A%A5%E5%91%8A.txt", header.Get("Content-Disposition"))
				require.Equal(t, mime.FormatMediaType(typ, params), header.Get("Content-Type"))
				csp := ""
				if disposition == "inline" && typ != "application/pdf" {
					csp = "default-src 'none'"
				}
				require.Equal(t, csp, header.Get("Content-Security-Policy"))
				require.Equal(t, "1099511627776", header.Get("Content-Length"))
				require.Equal(t, "private, no-store", header.Get("Cache-Control"))
				require.Equal(t, "nosniff", header.Get("X-Content-Type-Options"))
			})
		}
	}
}

func TestAssetFilter(t *testing.T) {
	for _, query := range []string{"owner=other", "owner=", "ref=", "ref=one", "ref=/name", "ref=pub/",
		"ref=pub/a/b", "ref=../name", "ref=pub/name@1"} {
		r := httptest.NewRequest("GET", "/v1/assets?"+query, nil)
		_, err := assetFilter(r)
		require.Error(t, err, query)
	}
	r := httptest.NewRequest("GET", "/v1/assets?owner=me&ref=Publisher/Name&kind=agent", nil)
	r = r.WithContext(auth.Context(r.Context(), auth.Principal{ID: "principal"}, "tenant"))
	f, err := assetFilter(r)
	require.NoError(t, err)
	require.Equal(t, "principal", f.Owner)
	require.Equal(t, "Publisher", f.Publisher)
	require.Equal(t, "Name", f.Name)
	require.Equal(t, "agent", f.Kind)
}

func TestDependentPagination(t *testing.T) {
	for _, query := range []string{"limit=51", "cursor=invalid", "limit=0"} {
		_, err := dependentPage(httptest.NewRequest("GET", "/v1/assets/id/dependents?"+query, nil))
		require.Error(t, err)
	}
	p, err := dependentPage(httptest.NewRequest("GET", "/v1/assets/id/dependents?limit=1", nil))
	require.NoError(t, err)
	require.Equal(t, 1, p.Limit)
}

func TestAdminMetadataAllowlists(t *testing.T) {
	for _, projection := range []string{modelMetadata, secretMetadata, quotaMetadata} {
		for _, forbidden := range []string{"ciphertext", "length(", "octet_length", "params", "base_url",
			"secret_id", "key_version", "updated_at"} {
			require.NotContains(t, projection, forbidden)
		}
	}
	require.Contains(t, secretMetadata, "'created_at',created_at")
	require.Contains(t, secretMetadata, "'scope',scope")
	require.Contains(t, modelMetadata, "'is_default',is_default")
	require.Contains(t, quotaMetadata, "'limit',\"limit\"::text")
}

func TestWP9PublicResourceFields(t *testing.T) {
	at := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	raw, err := json.Marshal(sessions.Run{CreatedAt: at})
	require.NoError(t, err)
	require.Contains(t, string(raw), `"created_at":"2026-10-19T12:00:00Z"`)
	display, description := "Expert", "Description"
	raw, err = json.Marshal(assets.Asset{DisplayName: &display, Description: &description})
	require.NoError(t, err)
	require.Contains(t, string(raw), `"display_name":"Expert"`)
	require.Contains(t, string(raw), `"description":"Description"`)
	raw, err = json.Marshal(assets.Asset{})
	require.NoError(t, err)
	require.Contains(t, string(raw), `"display_name":null`)
	require.Contains(t, string(raw), `"description":null`)
}

package kernelapi

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/telemetry"
)

func TestServiceCredentials(t *testing.T) {
	telemetry.InitializeMetrics()
	digest := sha256.Sum256([]byte("fixture-service-credential"))
	expected := hex.EncodeToString(digest[:])
	require.True(t, ServiceCredentialMatches("fixture-service-credential", expected))
	for _, want := range []string{"", "bad", strings.Repeat("0", 64), expected[:62]} {
		require.False(t, ServiceCredentialMatches("fixture-service-credential", want))
	}
	h := (&Handler{ServiceTokenSHA256: expected}).Router()
	for _, header := range []string{"", "Basic fixture-service-credential", "Bearer wrong"} {
		req := httptest.NewRequest("POST", "/internal/v1/tokens", strings.NewReader(`{}`))
		req.Header.Set("Authorization", header)
		out := httptest.NewRecorder()
		h.ServeHTTP(out, req)
		require.Equal(t, 401, out.Code)
		require.NotContains(t, out.Body.String(), "fixture-service-credential")
	}
}
func TestCapabilityRequestBoundaries(t *testing.T) {
	telemetry.InitializeMetrics()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := auth.NewCapabilitySigner(key, "test", nil)
	require.NoError(t, err)
	run := ids.New()
	claims := auth.CapabilityClaims{Tenant: ids.New(), Run: run, Member: "main", Seq: 1, Attempt: 1, Purpose: "segment"}
	valid, err := signer.IssueCapability(claims)
	require.NoError(t, err)
	sandbox := claims
	sandbox.Purpose = "sandbox"
	sandbox.LeaseID = "isolation"
	sandboxToken, err := signer.IssueCapability(sandbox)
	require.NoError(t, err)
	parsed, err := signer.VerifyCapability(valid)
	require.NoError(t, err)
	parsed.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Hour))
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, parsed)
	token.Header["kid"] = "test"
	expired, err := token.SignedString(key)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, token, path string
		status            int
	}{
		{name: "missing", path: run + "/policy", status: 401},
		{name: "expired", token: expired, path: run + "/policy", status: 401},
		{name: "run mismatch", token: valid, path: ids.New() + "/policy", status: 403},
		{name: "purpose mismatch", token: sandboxToken, path: run + "/policy", status: 403},
		{name: "tampered", token: valid + "x", path: run + "/lock", status: 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/internal/v1/runs/"+tc.path, nil)
			req.Header.Set("Authorization", "Bearer "+tc.token)
			out := httptest.NewRecorder()
			(&Handler{Signer: signer}).Router().ServeHTTP(out, req)
			require.Equal(t, tc.status, out.Code)
		})
	}
}

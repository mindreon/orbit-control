package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/mindreon/orbit-control/internal/platform/config"
)

func TestOIDCDiscoveryVerificationAndJWKSCache(t *testing.T) {
	_, key, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	var fetches atomic.Int32
	var issuer string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			_ = json.NewEncoder(w).Encode(map[string]any{"issuer": issuer, "jwks_uri": issuer + "/jwks", "authorization_endpoint": issuer + "/authorize", "token_endpoint": issuer + "/token", "id_token_signing_alg_values_supported": []string{"EdDSA"}})
		case "/jwks":
			fetches.Add(1)
			_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: key.Public(), KeyID: "key-1", Algorithm: "EdDSA", Use: "sig"}}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	issuer = server.URL
	c := config.Config{AuthMode: "oidc", Issuer: issuer, Audience: "orbit-api", ClientID: "web", KeyFile: t.TempDir() + "/signing.key"}
	a, e := New(t.Context(), nil, c, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if e != nil {
		t.Fatal(e)
	}
	sign := func(claims jwt.RegisteredClaims, k ed25519.PrivateKey) string {
		token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
		token.Header["kid"] = "key-1"
		s, e := token.SignedString(k)
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	valid := registered(issuer, "subject-1", "orbit-api", time.Hour)
	token := sign(valid, key)
	for range 2 {
		v, e := a.verifier.Verify(t.Context(), token)
		if e != nil || v.Subject != "subject-1" {
			t.Fatal(e)
		}
	}
	if fetches.Load() != 1 {
		t.Fatalf("JWKS not cached: %d", fetches.Load())
	}
	for _, test := range []string{"issuer", "audience", "expiry", "signature"} {
		t.Run(test, func(t *testing.T) {
			c := valid
			k := key
			switch test {
			case "issuer":
				c.Issuer = "https://attacker.test"
			case "audience":
				c.Audience = jwt.ClaimStrings{"wrong"}
			case "expiry":
				c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Hour))
			case "signature":
				_, k, e = ed25519.GenerateKey(rand.Reader)
				if e != nil {
					t.Fatal(e)
				}
			}
			if _, e = a.verifier.Verify(t.Context(), sign(c, k)); e == nil {
				t.Fatal("invalid access token accepted")
			}
		})
	}
}

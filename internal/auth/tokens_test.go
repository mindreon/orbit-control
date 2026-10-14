package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/mindreon/orbit-control/internal/platform/ids"
)

func testSigner(t *testing.T) *Signer {
	t.Helper()
	_, k, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	return NewSigner(k)
}
func TestTokenBoundaries(t *testing.T) {
	dev := testSigner(t)
	original := testSigner(t)
	signer, err := NewCapabilitySigner(original.key, "old", nil)
	if err != nil {
		t.Fatal(err)
	}
	claims := CapabilityClaims{Tenant: ids.New(), Run: ids.New(), Member: "leader", Seq: 3, Attempt: 1, Purpose: "segment", LeaseID: ids.New()}
	token, err := signer.IssueCapability(claims)
	if err != nil {
		t.Fatal(err)
	}
	got, err := signer.VerifyCapability(token)
	if err != nil || got.Tenant != claims.Tenant || got.Run != claims.Run || got.Member != claims.Member || got.Seq != 3 || got.Purpose != "segment" || got.LeaseID != claims.LeaseID || got.Subject != "run:"+claims.Run || got.ExpiresAt.Sub(got.IssuedAt.Time) != 5*time.Minute {
		t.Fatal(got, err)
	}
	if _, err = dev.VerifyDev(token); err == nil {
		t.Fatal("capability accepted as dev token")
	}
	devToken, _ := dev.DevToken(Principal{ID: ids.New()})
	if _, err = signer.VerifyCapability(devToken); err == nil {
		t.Fatal("dev token accepted as capability")
	}
	next := testSigner(t)
	rotated, err := NewCapabilitySigner(next.key, "new", map[string][]byte{"old": original.key.Public().(ed25519.PublicKey)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = rotated.VerifyCapability(token); err != nil {
		t.Fatal("rotation lost old key", err)
	}
	fresh, err := rotated.IssueCapability(claims)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = rotated.VerifyCapability(fresh); err != nil {
		t.Fatal(err)
	}
	if _, err = signer.VerifyCapability(fresh); err == nil {
		t.Fatal("accepted unknown kid")
	}
	for _, name := range []string{"expired", "audience", "future-nbf", "missing-nbf", "signature", "missing-kid"} {
		t.Run(name, func(t *testing.T) {
			c := got
			c.RegisteredClaims = registered(CapabilityIssuer, "run:"+c.Run, "kernel", 5*time.Minute)
			c.NotBefore = c.IssuedAt
			key := original.key
			kid := "old"
			switch name {
			case "expired":
				c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute))
			case "audience":
				c.Audience = jwt.ClaimStrings{"orbit-api"}
			case "future-nbf":
				c.NotBefore = jwt.NewNumericDate(time.Now().Add(time.Hour))
			case "missing-nbf":
				c.NotBefore = nil
			case "signature":
				key = next.key
			case "missing-kid":
				kid = ""
			}
			jwtToken := jwt.NewWithClaims(jwt.SigningMethodEdDSA, c)
			if kid != "" {
				jwtToken.Header["kid"] = kid
			}
			bad, e := jwtToken.SignedString(key)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = signer.VerifyCapability(bad); e == nil {
				t.Fatal("accepted invalid capability")
			}
		})
	}
}
func TestDevSigningKeyPersistsAndRestricted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "signing.key")
	s, e := LoadSigner(path)
	if e != nil {
		t.Fatal(e)
	}
	token, e := s.DevToken(Principal{ID: ids.New(), Email: "dev@example.test"})
	if e != nil {
		t.Fatal(e)
	}
	s2, e := LoadSigner(path)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s2.VerifyDev(token); e != nil {
		t.Fatal(e)
	}
	if e = os.Chmod(path, 0644); e != nil {
		t.Fatal(e)
	}
	if _, e = LoadSigner(path); e == nil {
		t.Fatal("world-readable key accepted")
	}
}
func TestAPIKeyHash(t *testing.T) {
	token, prefix, hash, e := GenerateAPIKey()
	if e != nil {
		t.Fatal(e)
	}
	if !strings.HasPrefix(token, "orbit_"+prefix+"_") {
		t.Fatal("format")
	}
	expected := sha256.Sum256([]byte(token))
	if string(hash) != string(expected[:]) {
		t.Fatal("wrong hash")
	}
	other, otherPrefix, _, e := GenerateAPIKey()
	if e != nil || other == token || otherPrefix == prefix {
		t.Fatal("not random")
	}
}

func TestCapabilityPurposeAndAttempt(t *testing.T) {
	signer, err := NewCapabilitySigner(testSigner(t).key, "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	valid := CapabilityClaims{Tenant: ids.New(), Run: ids.New(), Member: "main", Seq: 1, Attempt: 2, Purpose: "sandbox", LeaseID: "lease-1"}
	token, err := signer.IssueCapability(valid)
	if err != nil {
		t.Fatal(err)
	}
	got, err := signer.VerifyCapability(token)
	if err != nil || got.Attempt != 2 || got.Purpose != "sandbox" || got.LeaseID != "lease-1" {
		t.Fatal("lost sandbox binding", err)
	}
	for _, change := range []func(*CapabilityClaims){
		func(c *CapabilityClaims) { c.Attempt = 0 },
		func(c *CapabilityClaims) { c.Purpose = "worker" },
		func(c *CapabilityClaims) { c.LeaseID = "" },
	} {
		bad := valid
		change(&bad)
		if _, err := signer.IssueCapability(bad); err == nil {
			t.Fatal("accepted invalid capability")
		}
	}
}

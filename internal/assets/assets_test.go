package assets

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/Masterminds/semver/v3"

	"github.com/mindreon/orbit-control/internal/assets/bundle"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/jcs"
)

func TestTransitionMatrix(t *testing.T) {
	states := []string{
		"draft",
		"submitted",
		"checking",
		"check_failed",
		"in_review",
		"changes_requested",
		"approved",
		"published",
		"deprecated",
		"suspended",
		"yanked",
		"rejected",
		"unknown",
	}
	edges := map[string]bool{}
	for _, edge := range []string{"submitted/draft",
		"checking/draft",
		"in_review/draft",
		"draft/submitted",
		"submitted/checking",
		"checking/check_failed",
		"check_failed/draft",
		"checking/in_review",
		"checking/approved",
		"in_review/changes_requested",
		"changes_requested/draft",
		"in_review/rejected",
		"in_review/approved",
		"approved/published",
		"published/deprecated",
		"published/suspended",
		"suspended/published",
		"deprecated/yanked",
		"suspended/yanked"} {
		edges[edge] = true
	}
	for _, from := range states {
		for _, to := range states {
			if got := Transition(from, to) == nil; got != edges[from+"/"+to] {
				t.Errorf("%s -> %s = %v", from, to, got)
			}
		}
	}
}
func TestSignatureTampering(t *testing.T) {
	pub, key, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	s := Signer{Key: key, ID: "test", Public: map[string]ed25519.PublicKey{"test": pub}}
	a := Asset{Kind: "agent", Publisher: "acme", Name: "assistant"}
	v := Version{
		Version:     "1.0.0",
		Digest:      "sha256:001",
		Permissions: contract.EffectivePermissions{SandboxProfile: "default"},
	}
	raw, _ := json.Marshal(struct {
		Effective contract.EffectivePermissions `json:"effective_permissions"`
	}{Effective: v.Permissions})
	v.PermissionsJCS, e = jcs.Canonicalize(raw)
	if e != nil {
		t.Fatal(e)
	}
	v.IdentityJCS, e = SigningIdentity(a, v.Version, v.Digest, v.PermissionsJCS)
	if e != nil {
		t.Fatal(e)
	}
	v.Signature, e = s.Sign(a, v)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Verify(a, v); e != nil {
		t.Fatal(e)
	}
	for name, mutate := range map[string]func(*Asset, *Version){"digest": func(a *Asset, v *Version) {
		v.Digest += "1"
	},
		"permissions": func(a *Asset, v *Version) {
			v.Permissions.Network.Egress = []string{"evil.test"}
		},
		"publisher": func(a *Asset, v *Version) {
			a.Publisher = "other"
		},
		"version": func(a *Asset, v *Version) {
			v.Version = "2.0.0"
		},
		"identity bytes": func(a *Asset, v *Version) {
			v.IdentityJCS = nil
		},
		"permission bytes": func(a *Asset, v *Version) {
			v.PermissionsJCS = nil
		}} {
		t.Run(name, func(t *testing.T) {
			a, v := a, v
			mutate(&a, &v)
			if s.Verify(a, v) == nil {
				t.Fatal("tampering accepted")
			}
		})
	}
	file := filepath.Join(t.TempDir(), "assets.key")
	if _, e = LoadSigner(file, "1", false, nil); e == nil {
		t.Fatal("production generated key")
	}
	if _, e = LoadSigner(file, "1", true, nil); e != nil {
		t.Fatal(e)
	}
	if e = os.Chmod(file, 0644); e != nil {
		t.Fatal(e)
	}
	if _, e = LoadSigner(file, "1", true, nil); e == nil {
		t.Fatal("unsafe key permissions accepted")
	}
}
func TestSemverLibrary(t *testing.T) {
	for _, tc := range []struct {
		c, v string
		ok   bool
	}{{c: "^1.2",
		v:  "1.9.0",
		ok: true},
		{c: "^1.2",
			v:  "2.0.0",
			ok: false},
		{c: ">=1.0 <2.0",
			v:  "1.5.0",
			ok: true},
		{c: "~1.2.0",
			v:  "1.3.0",
			ok: false},
		{c: "*",
			v:  "1.0.0-beta.1",
			ok: false},
		{c: "1.2.3",
			v:  "1.2.4",
			ok: false}} {
		c, e := semver.NewConstraint(tc.c)
		if e != nil {
			t.Fatal(e)
		}
		v, e := semver.StrictNewVersion(tc.v)
		if e != nil {
			t.Fatal(e)
		}
		if c.Check(v) != tc.ok {
			t.Fatal(tc)
		}
	}
	if _, e := semver.NewConstraint("not a range"); e == nil {
		t.Fatal("invalid constraint")
	}
}

func TestManifestDependenciesAndMCPPermissions(t *testing.T) {
	m, p, err := ParseManifest(
		[]bundle.File{
			{
				Path: "mcp.json",
				Data: []byte(
					"{\"mcpServers\":{\"remote\":{\"url\":\"https://api.example.test\",\"transport\":\"sse\",\"headers\":{" +
						"\"Authorization\":\"Bearer ${secret:KEY}\"},\"tools\":{\"send\":{\"effectClass\":\"external_write\"}}}," +
						"\"catalog\":{\"ref\":\"acme/mail@^2.0\"}}}",
				),
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Dependencies) != 1 || m.Dependencies[0].Constraint != "^2.0" || len(p.Secrets) != 1 ||
		p.Secrets[0].Name != "KEY" ||
		len(p.Network.Egress) != 1 ||
		len(p.EffectClasses) != 1 {
		t.Fatal(m, p)
	}
}

func TestStoredSigningPreimagePreservesUnknownFields(t *testing.T) {
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	s := Signer{Key: key, ID: "test", Public: map[string]ed25519.PublicKey{"test": pub}}
	a := Asset{Kind: "team", Publisher: "acme", Name: "team"}
	v := Version{
		Version:     "1.0.0",
		Digest:      "sha256:001",
		Permissions: contract.EffectivePermissions{SandboxProfile: "default"},
	}
	raw, _ := json.Marshal(map[string]any{"effective_permissions": v.Permissions, "future_permission": "retained"})
	v.PermissionsJCS, err = jcs.Canonicalize(raw)
	if err != nil {
		t.Fatal(err)
	}
	v.IdentityJCS, err = SigningIdentity(a, v.Version, v.Digest, v.PermissionsJCS)
	if err != nil {
		t.Fatal(err)
	}
	var identity map[string]any
	if err = json.Unmarshal(v.IdentityJCS, &identity); err != nil {
		t.Fatal(err)
	}
	identity["future_identity"] = "retained"
	raw, _ = json.Marshal(identity)
	v.IdentityJCS, err = jcs.Canonicalize(raw)
	if err != nil {
		t.Fatal(err)
	}
	v.Signature, err = s.Sign(a, v)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Verify(a, v); err != nil {
		t.Fatal("future fields broke stored signature", err)
	}
	v.IdentityJCS = append(v.IdentityJCS, ' ')
	if s.Verify(a, v) == nil {
		t.Fatal("noncanonical signing bytes accepted")
	}
}

package assets

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json/v2"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/jcs"
)

type Signer struct {
	Key    ed25519.PrivateKey
	ID     string
	Public map[string]ed25519.PublicKey
}
type Identity struct {
	Kind              contract.AssetKind `json:"kind"`
	Publisher         string             `json:"publisher"`
	Name              string             `json:"name"`
	Version           string             `json:"version"`
	ContentDigest     string             `json:"contentDigest"`
	PermissionsDigest string             `json:"permissionsDigest"`
}

// SigningIdentity creates the immutable canonical preimage at upload time.
// Verification consumes these stored bytes, preserving future optional fields.
func SigningIdentity(a Asset, version, digest string, permissions []byte) ([]byte, error) {
	d, err := contract.ObjectDigest(permissions)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(Identity{a.Kind, a.Publisher, a.Name, version, digest, d})
	if err != nil {
		return nil, err
	}
	return jcs.Canonicalize(b)
}
func signedBytes(a Asset, v Version) ([]byte, error) {
	if len(v.IdentityJCS) == 0 || len(v.PermissionsJCS) == 0 {
		return nil, fmt.Errorf("asset signing preimage missing")
	}
	canonical, err := jcs.Canonicalize(v.IdentityJCS)
	if err != nil || !bytes.Equal(canonical, v.IdentityJCS) {
		return nil, fmt.Errorf("asset signing preimage is not canonical")
	}
	var identity Identity
	if err = json.Unmarshal(v.IdentityJCS, &identity); err != nil {
		return nil, err
	}
	canonicalPermissions, err := jcs.Canonicalize(v.PermissionsJCS)
	if err != nil || !bytes.Equal(canonicalPermissions, v.PermissionsJCS) {
		return nil, fmt.Errorf("asset permissions preimage is not canonical")
	}
	var stored struct {
		Effective contract.EffectivePermissions            `json:"effective_permissions"`
		Members   map[string]contract.EffectivePermissions `json:"members"`
	}
	if err := json.Unmarshal(v.PermissionsJCS, &stored); err != nil {
		return nil, err
	}
	known, _ := json.Marshal(stored, json.Deterministic(true))
	current, _ := json.Marshal(struct {
		Effective contract.EffectivePermissions            `json:"effective_permissions"`
		Members   map[string]contract.EffectivePermissions `json:"members"`
	}{v.Permissions, v.Manifest.MemberPermissions}, json.Deterministic(true))
	if !bytes.Equal(known, current) {
		return nil, fmt.Errorf("asset permissions mismatch")
	}
	digest, err := contract.ObjectDigest(v.PermissionsJCS)
	if err != nil {
		return nil, err
	}
	if identity != (Identity{a.Kind, a.Publisher, a.Name, v.Version, v.Digest, digest}) {
		return nil, fmt.Errorf("asset signing identity mismatch")
	}
	return v.IdentityJCS, nil
}
func (s *Signer) Sign(a Asset, v Version) (string, error) {
	b, err := signedBytes(a, v)
	if err != nil {
		return "", err
	}
	return s.ID + ":" + base64.StdEncoding.EncodeToString(ed25519.Sign(s.Key, b)), nil
}
func (s *Signer) Verify(a Asset, v Version) error {
	id, encoded, ok := strings.Cut(v.Signature, ":")
	sig, err := base64.StdEncoding.DecodeString(encoded)
	b, e := signedBytes(a, v)
	if !ok || err != nil || e != nil || len(s.Public[id]) != ed25519.PublicKeySize || !ed25519.Verify(s.Public[id], b, sig) {
		return fmt.Errorf("asset signature verification failed")
	}
	return nil
}
func LoadSigner(file, id string, dev bool, public map[string][]byte) (*Signer, error) {
	b, err := os.ReadFile(file)
	if os.IsNotExist(err) && dev {
		_, key, e := ed25519.GenerateKey(rand.Reader)
		if e != nil {
			return nil, e
		}
		der, e := x509.MarshalPKCS8PrivateKey(key)
		if e != nil {
			return nil, e
		}
		if e = os.MkdirAll(filepath.Dir(file), 0700); e != nil {
			return nil, e
		}
		f, e := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if os.IsExist(e) {
			return LoadSigner(file, id, dev, public)
		}
		if e != nil {
			return nil, e
		}
		_, e = f.Write(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
		ce := f.Close()
		if e != nil {
			return nil, e
		}
		if ce != nil {
			return nil, ce
		}
		return LoadSigner(file, id, false, public)
	}
	if err != nil {
		return nil, fmt.Errorf("ORBIT_ASSET_SIGNING_KEY_FILE cannot be read")
	}
	st, err := os.Stat(file)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
		return nil, fmt.Errorf("ORBIT_ASSET_SIGNING_KEY_FILE must be regular mode 0600")
	}
	block, _ := pem.Decode(b)
	if block == nil {
		return nil, fmt.Errorf("invalid asset signing PEM")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("invalid asset signing key")
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("asset signing key must be Ed25519")
	}
	if id == "" || strings.Contains(id, ":") {
		return nil, fmt.Errorf("invalid asset signing key ID")
	}
	s := &Signer{Key: key, ID: id, Public: map[string]ed25519.PublicKey{}}
	for k, v := range public {
		if len(v) != 32 {
			return nil, fmt.Errorf("invalid asset verification key")
		}
		s.Public[k] = ed25519.PublicKey(v)
	}
	s.Public[id] = key.Public().(ed25519.PublicKey)
	return s, nil
}

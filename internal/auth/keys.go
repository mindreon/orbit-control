package auth

import (
	"crypto/ed25519"
	"fmt"
	"maps"
	"slices"

	"github.com/golang-jwt/jwt/v5"
)

func signingPublicKeys(key ed25519.PrivateKey, kid string, publicKeys map[string][]byte, kind string) (map[string][]byte, error) {
	if len(key) != ed25519.PrivateKeySize || kid == "" {
		return nil, fmt.Errorf("invalid %s signing key or kid", kind)
	}
	keys := make(map[string][]byte, len(publicKeys)+1)
	maps.Copy(keys, publicKeys)
	pub := key.Public().(ed25519.PublicKey)
	if previous, ok := keys[kid]; ok && !pub.Equal(ed25519.PublicKey(previous)) {
		return nil, fmt.Errorf("%s kid conflicts with configured public key", kind)
	}
	keys[kid] = pub
	return keys, nil
}

func kidKeyfunc(publicKeys map[string][]byte, kind string) (jwt.Keyfunc, error) {
	keys := make(map[string]ed25519.PublicKey, len(publicKeys))
	for id, key := range publicKeys {
		if id == "" || len(key) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("invalid %s public key", kind)
		}
		keys[id] = ed25519.PublicKey(slices.Clone(key))
	}
	return func(token *jwt.Token) (any, error) {
		kid, ok := token.Header["kid"].(string)
		key, known := keys[kid]
		if !ok || !known {
			return nil, fmt.Errorf("unknown %s kid", kind)
		}
		return key, nil
	}, nil
}

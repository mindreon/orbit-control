// Package secrets encrypts tenant credentials and derives purpose-separated keys.
package secrets

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"fmt"

	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
)

type Keys struct {
	Current int
	KEKs    map[int][]byte
}

func (k Keys) DeriveDEK(tenant, purpose string, version int) ([]byte, error) {
	if !ids.Valid(tenant) || (purpose != "secrets" && purpose != "checkpoints") || len(k.KEKs[version]) != 32 {
		return nil, fmt.Errorf("invalid tenant, purpose or key version")
	}
	return hkdf.Key(sha256.New, k.KEKs[version], []byte(tenant), "orbit:"+purpose+":v1", 32)
}
func (k Keys) aead(tenant string, version int) (cipher.AEAD, error) {
	key, err := k.DeriveDEK(tenant, "secrets", version)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCMWithRandomNonce(block)
}
func (k Keys) Encrypt(tenant, name, scope string, plain []byte) ([]byte, error) {
	g, err := k.aead(tenant, k.Current)
	if err != nil {
		return nil, err
	}
	return g.Seal(nil, nil, plain, []byte(tenant+"\x00"+name+"\x00"+scope)), nil
}
func (k Keys) Decrypt(tenant, name, scope string, version int, encrypted []byte) ([]byte, error) {
	g, err := k.aead(tenant, version)
	if err != nil {
		return nil, err
	}
	return g.Open(nil, nil, encrypted, []byte(tenant+"\x00"+name+"\x00"+scope))
}
func (k Keys) Put(ctx context.Context, tenant, actor, name, scope string, plain []byte) (string, error) {
	encrypted, err := k.Encrypt(tenant, name, scope, plain)
	if err != nil {
		return "", err
	}
	var id string
	err = db.Tx(ctx).
		QueryRow(ctx, "INSERT INTO secrets(id,tenant_id,name,scope,ciphertext,key_version,created_by) "+
			"VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(tenant_id,name,scope) DO UPDATE SET "+
			("ciphertext=EXCLUDED.ciphertext,key_version=EXCLUDED.key_version "+
				"RETURNING id::text"),
			ids.New(),
			tenant,
			name,
			scope,
			encrypted,
			k.Current,
			actor).
		Scan(&id)
	return id, err
}
func (k Keys) Get(ctx context.Context, tenant, id string) ([]byte, error) {
	var name, scope string
	var version int
	var encrypted []byte
	err := db.Tx(ctx).
		QueryRow(ctx, `SELECT name,scope,key_version,ciphertext FROM secrets WHERE tenant_id=$1 AND id=$2`, tenant, id).
		Scan(&name, &scope, &version, &encrypted)
	if err != nil {
		return nil, err
	}
	return k.Decrypt(tenant, name, scope, version, encrypted)
}

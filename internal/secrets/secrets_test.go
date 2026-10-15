package secrets

import (
	"bytes"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"testing"
)

func TestTenantEncryption(t *testing.T) {
	tenant := ids.New()
	keys := Keys{1, map[int][]byte{1: bytes.Repeat([]byte{7}, 32)}}
	encrypted, e := keys.Encrypt(tenant, "KEY", "tenant", []byte("sensitive"))
	if e != nil {
		t.Fatal(e)
	}
	plain, e := keys.Decrypt(tenant, "KEY", "tenant", 1, encrypted)
	if e != nil || string(plain) != "sensitive" {
		t.Fatal("roundtrip", e)
	}
	for _, v := range []struct{ tenant, name string }{{ids.New(), "KEY"}, {tenant, "OTHER"}} {
		if _, e = keys.Decrypt(v.tenant, v.name, "tenant", 1, encrypted); e == nil {
			t.Fatal("cross identity decrypt")
		}
	}
	a, _ := keys.DeriveDEK(tenant, "checkpoints", 1)
	b, _ := keys.DeriveDEK(tenant, "checkpoints", 1)
	c, _ := keys.DeriveDEK(tenant, "secrets", 1)
	if !bytes.Equal(a, b) || bytes.Equal(a, c) {
		t.Fatal("purpose separation")
	}
	keys.KEKs[1] = bytes.Repeat([]byte{8}, 32)
	if _, e = keys.Decrypt(tenant, "KEY", "tenant", 1, encrypted); e == nil {
		t.Fatal("wrong KEK")
	}
}

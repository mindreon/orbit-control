package ids

import (
	"crypto/sha1"

	"uuid"
)

func New() string { return uuid.NewV7().String() }
func Valid(s string) bool {
	_, err := uuid.Parse(s)
	return err == nil
}

// Derive implements UUIDv5 with a fixed Orbit namespace; parts are unambiguous.
func Derive(namespace, name string) string {
	ns := sha1.Sum([]byte("orbit:v2:" + namespace))
	h := sha1.New()
	h.Write(ns[:16])
	h.Write([]byte(name))
	var u uuid.UUID
	copy(u[:], h.Sum(nil))
	u[6] = (u[6] & 15) | 0x50
	u[8] = (u[8] & 63) | 0x80
	return u.String()
}

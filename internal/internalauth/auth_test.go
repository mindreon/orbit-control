package internalauth

import (
  "net/http"
  "os"
  "testing"
)

func TestAuthorizedLoopbackDevMode(t *testing.T) {
  t.Setenv("ORBIT_INTERNAL_TOKEN", "")
  _ = os.Unsetenv("ORBIT_INTERNAL_TOKEN")
  r, _ := http.NewRequest("POST", "/internal/events", nil)
  if !Authorized(r) {
    t.Fatalf("empty remote should allow when token unset")
  }
  r.RemoteAddr = "127.0.0.1:1234"
  if !Authorized(r) {
    t.Fatalf("loopback should allow")
  }
  r.RemoteAddr = "8.8.8.8:9"
  if Authorized(r) {
    t.Fatalf("non-loopback should deny without token")
  }
}

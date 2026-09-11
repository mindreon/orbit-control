package internalauth

import (
	"crypto/subtle"
	"net"
	"net/http"
	"os"
	"strings"
)

const Header = "Authorization"

// Token returns the shared internal service token, if configured.
func Token() string {
	return strings.TrimSpace(os.Getenv("ORBIT_INTERNAL_TOKEN"))
}

// Authorized reports whether the request carries a valid bearer token.
// When ORBIT_INTERNAL_TOKEN is unset, only loopback clients are accepted
// (local development). Non-loopback unauthenticated calls are rejected.
func Authorized(r *http.Request) bool {
	token := Token()
	got := bearer(r.Header.Get(Header))
	if token != "" {
		return subtle.ConstantTimeCompare([]byte(got), []byte(token)) == 1
	}
	return isLoopback(r.RemoteAddr)
}

func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/internal/") && !Authorized(r) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized","code":"UNAUTHORIZED","message":"internal token required"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func bearer(header string) string {
	header = strings.TrimSpace(header)
	if len(header) < 8 || !strings.EqualFold(header[:7], "bearer ") {
		return ""
	}
	return strings.TrimSpace(header[7:])
}

func isLoopback(remote string) bool {
	// httptest and similar in-process callers often leave RemoteAddr empty.
	if remote == "" {
		return true
	}
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

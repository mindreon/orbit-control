package streamgw

import (
	"encoding/json/v2"
	"net/http"
	"strings"
	"time"

	"github.com/mindreon/orbit-control/internal/auth"
)

func (s *Server) authorize(w http.ResponseWriter, r *http.Request) (auth.StreamClaims, bool) {
	q := r.URL.Query()
	token := q.Get("token")
	q.Del("token")
	r.URL.RawQuery = q.Encode()
	if values, present := r.Header["Authorization"]; present {
		token = ""
		if len(values) == 1 {
			if value, ok := strings.CutPrefix(values[0], "Bearer "); ok {
				token = value
			}
		}
	}
	claims, err := s.verifier.Verify(token)
	// JWT clock tolerance must never extend the actual connection lifetime.
	if err != nil || !time.Now().Before(claims.ExpiresAt.Time) || claims.NotBefore.Time.After(time.Now()) {
		s.metrics.add("streamgw_auth_failures_total{reason=\"invalid\"}", 1)
		writeError(w, 401, "TOKEN_INVALID", "Invalid or expired stream token")
		return claims, false
	}
	allowed := claims.Scope == "run" && claims.RunID == r.PathValue("r") && r.PathValue("r") != "" ||
		claims.Scope == "session" && claims.SessionID == r.PathValue("s") && r.PathValue("s") != ""
	if !allowed {
		s.metrics.add("streamgw_auth_failures_total{reason=\"forbidden\"}", 1)
		writeError(w, 403, "STREAM_FORBIDDEN", "Stream token does not authorize this path")
		return claims, false
	}
	return claims, true
}

func subject(c auth.StreamClaims) string {
	prefix := "run." + c.TenantID + "." + c.SessionID + "."
	if c.Scope == "session" {
		return prefix + ">"
	}
	return prefix + c.RunID
}

// Keep the public error shape without importing control's HTTP/DB middleware.
func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	raw, err := json.Marshal(map[string]any{"error": map[string]any{
		"code": code, "message": message, "retryable": false, "details": map[string]any{},
	}})
	if err == nil {
		_, _ = w.Write(raw)
	}
}

package httpapi

import (
	"net/http"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/config"
)

// Authenticator resolves the caller from the request (§17). The OIDC session
// authenticator from the auth PR plugs in here; tenant and user never come
// from request bodies.
type Authenticator interface {
	Authenticate(r *http.Request) (app.Principal, bool)
}

type AuthenticatorFunc func(r *http.Request) (app.Principal, bool)

func (f AuthenticatorFunc) Authenticate(r *http.Request) (app.Principal, bool) { return f(r) }

const LocalUserID = "local-dev"

// LocalAuthenticator is the dev principal used when ORBIT_AUTH_MODE is not
// oidc.
func LocalAuthenticator(tenantID string) Authenticator {
	return AuthenticatorFunc(func(*http.Request) (app.Principal, bool) {
		return app.Principal{TenantID: tenantID, UserID: LocalUserID}, true
	})
}

// denyAllAuthenticator is used for ORBIT_AUTH_MODE=oidc until the OIDC session
// authenticator exists: every /v1 call is 401, never the dev principal.
var denyAllAuthenticator = AuthenticatorFunc(func(*http.Request) (app.Principal, bool) {
	return app.Principal{}, false
})

func authenticatorFromEnv(defaultTenant string) Authenticator {
	if config.Load().OIDC() {
		return denyAllAuthenticator
	}
	return LocalAuthenticator(defaultTenant)
}

// csrfOK implements §17.4: the Origin must be allowed AND X-Orbit-Request: 1
// must be present.
func csrfOK(r *http.Request, allowed []string) bool {
	if r.Header.Get("X-Orbit-Request") != "1" {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	for _, o := range allowed {
		if origin == o {
			return true
		}
	}
	return false
}

func writeUnauthenticated(w http.ResponseWriter) {
	writeErr(w, http.StatusUnauthorized, "UNAUTHENTICATED", "authentication required")
}

func writeCSRFRejected(w http.ResponseWriter) {
	writeErr(w, http.StatusForbidden, "CSRF_REJECTED", "state-changing requests need an allowed Origin and X-Orbit-Request: 1")
}

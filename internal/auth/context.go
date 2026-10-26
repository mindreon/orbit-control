package auth

import "context"

type Principal struct {
	PlatformOperator bool     `json:"-"`
	ID               string   `json:"id"`
	Kind             string   `json:"kind"`
	Email            string   `json:"email"`
	DisplayName      string   `json:"display_name"`
	Scopes           []string `json:"-"`
	EmailVerified    bool     `json:"-"`
	APIKey           bool     `json:"-"`
	// IAMWorkspace is the workspace mapped from the token's current baize project
	// in iam mode; empty otherwise. IAMRestricted marks an iam user token: its
	// access is limited to IAMWorkspace, so with no current project there is no
	// workspace data at all. Both travel with the principal (see IAM) so
	// re-contexted request paths keep the restriction; API keys never set them.
	IAMWorkspace  string `json:"-"`
	IAMRestricted bool   `json:"-"`
}
type identity struct {
	Principal Principal
	Tenant    string
}
type contextKey struct{}

func Context(ctx context.Context, p Principal, tenant string) context.Context {
	return context.WithValue(ctx, contextKey{}, identity{Principal: p, Tenant: tenant})
}
func From(ctx context.Context) Principal {
	v, _ := ctx.Value(contextKey{}).(identity)
	return v.Principal
}
func Tenant(ctx context.Context) string {
	v, _ := ctx.Value(contextKey{}).(identity)
	return v.Tenant
}

// IAM reports the iam-mode project restriction carried by the principal itself,
// so it survives every re-context that keeps the principal (BuildStart,
// invitation acceptance). restricted is false outside iam mode, for API keys and
// for background callers (schedules), whose principals come from DB rows;
// workspace is the current baize project's workspace, empty with no project.
func IAM(ctx context.Context) (workspace string, restricted bool) {
	p := From(ctx)
	return p.IAMWorkspace, p.IAMRestricted
}

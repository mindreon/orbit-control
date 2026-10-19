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

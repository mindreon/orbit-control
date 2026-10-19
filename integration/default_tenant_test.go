package integration

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/tenancy"
)

func TestDefaultTenantLogin(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	ctx := t.Context()
	tenant := usageTenant(t, admin)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	configuration := func(t *testing.T, slug string) config.Config {
		t.Helper()
		t.Setenv("ORBIT_DEFAULT_TENANT", slug)
		t.Setenv("ORBIT_PERSONAL_TENANTS", "false")
		c, err := config.Load()
		require.NoError(t, err)
		c.AuthMode = "dev"
		c.KeyFile = t.TempDir() + "/dev.key"
		c.CapabilityKeyFile = t.TempDir() + "/capability.key"
		c.StreamKeyFile = t.TempDir() + "/stream.key"
		c.SecretsKeyVersion = 1
		c.SecretsKEKs = map[int][]byte{1: make([]byte, 32)}
		return c
	}
	service := func(t *testing.T, slug string) (*auth.Service, http.Handler) {
		t.Helper()
		c := configuration(t, slug)
		a, err := auth.New(ctx, d, c, logger)
		require.NoError(t, err)
		t.Cleanup(a.Close)
		return a, (&httpapi.Handler{DB: d, Auth: a, Config: c, Logger: logger}).Router()
	}
	login := func(t *testing.T, h http.Handler, email string) (string, map[string]any) {
		t.Helper()
		status, result, raw := publicRequest(t, h, "", "", "POST", "/v1/auth/dev/token", fmt.Sprintf(`{"email":%q}`, email))
		require.Equal(t, 200, status, raw)
		token := result["access_token"].(string)
		status, result, raw = publicRequest(t, h, token, "", "GET", "/v1/me", "")
		require.Equal(t, 200, status, raw)
		return token, result
	}
	t.Run("new dev login and repeated login", func(t *testing.T) {
		_, h := service(t, tenant)
		email := "default-" + ids.New() + "@example.test"
		token, me := login(t, h, email)
		require.Len(t, me["tenants"], 1)
		require.Equal(t, tenant, me["tenants"].([]any)[0].(map[string]any)["id"])
		pid := me["principal"].(map[string]any)["id"].(string)
		status, selected, raw := publicRequest(t, h, token, tenant, "GET", "/v1/me", "")
		require.Equal(t, 200, status, raw)
		require.Equal(t, []any{"tenant.member"}, selected["roles"])
		var joined time.Time
		require.NoError(t, admin.QueryRow(ctx,
			"SELECT joined_at FROM memberships WHERE tenant_id=$1 AND principal_id=$2", tenant, pid).Scan(&joined))
		_, me = login(t, h, email)
		require.Len(t, me["tenants"], 1)
		var count int
		require.NoError(t, admin.QueryRow(ctx,
			"SELECT count(*) FROM memberships WHERE tenant_id=$1 AND principal_id=$2 AND joined_at=$3",
			tenant, pid, joined).Scan(&count))
		require.Equal(t, 1, count)
		require.NoError(t, admin.QueryRow(ctx,
			"SELECT count(*) FROM role_bindings WHERE tenant_id=$1 AND principal_id=$2", tenant, pid).Scan(&count))
		require.Equal(t, 1, count)
		_, err := admin.Exec(ctx, "UPDATE memberships SET status='suspended',source='direct' WHERE tenant_id=$1 AND principal_id=$2",
			tenant, pid)
		require.NoError(t, err)
		_, me = login(t, h, email)
		require.Empty(t, me["tenants"])
		var membershipStatus, source string
		require.NoError(t, admin.QueryRow(ctx,
			"SELECT status,source FROM memberships WHERE tenant_id=$1 AND principal_id=$2 AND joined_at=$3",
			tenant, pid, joined).Scan(&membershipStatus, &source))
		require.Equal(t, "suspended", membershipStatus)
		require.Equal(t, "direct", source)
	})
	t.Run("existing principal and membership are preserved", func(t *testing.T) {
		a, _ := service(t, "")
		email := "existing-" + ids.New() + "@example.test"
		p, err := a.JIT(ctx, auth.DevIssuer, email, email, "Existing", true)
		require.NoError(t, err)
		_, h := service(t, tenant)
		_, me := login(t, h, email)
		require.Len(t, me["tenants"], 1)
		_, err = admin.Exec(ctx, "UPDATE role_bindings SET role='tenant.reviewer' WHERE tenant_id=$1 AND principal_id=$2", tenant, p.ID)
		require.NoError(t, err)
		token, _ := login(t, h, email)
		status, selected, raw := publicRequest(t, h, token, tenant, "GET", "/v1/me", "")
		require.Equal(t, 200, status, raw)
		require.Equal(t, []any{"tenant.reviewer"}, selected["roles"])
	})
	t.Run("OIDC JIT concurrent first logins", func(t *testing.T) {
		a, _ := service(t, tenant)
		subject := ids.New()
		var wg sync.WaitGroup
		errs := make(chan error, 5)
		for range 5 {
			wg.Go(func() {
				_, err := a.JIT(ctx, "https://idp.example", subject, "oidc-"+subject+"@example.test", "OIDC", true)
				errs <- err
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			require.NoError(t, err)
		}
		p, err := a.JIT(ctx, "https://idp.example", subject, "oidc-"+subject+"@example.test", "OIDC", true)
		require.NoError(t, err)
		tenants, err := tenancy.Tenants(ctx, d, p.ID)
		require.NoError(t, err)
		require.Equal(t, []tenancy.Tenant{{ID: tenant, Slug: tenant, Name: "rollup fixture"}}, tenants)
	})
	t.Run("unset leaves login without tenants", func(t *testing.T) {
		_, h := service(t, "")
		_, me := login(t, h, "unset-"+ids.New()+"@example.test")
		require.Empty(t, me["tenants"])
	})
	t.Run("slug created after startup", func(t *testing.T) {
		// seed-dev creates the dev tenant after control starts, so startup must not require it.
		id := ids.New()
		_, h := service(t, id)
		status, _, raw := publicRequest(t, h, "", "", "POST", "/v1/auth/dev/token",
			fmt.Sprintf(`{"email":%q}`, "late-"+ids.New()+"@example.test"))
		require.NotEqual(t, 200, status, raw)
		_, err := admin.Exec(ctx, `INSERT INTO tenants(id,slug,name)VALUES($1,$1::uuid::text,'late fixture')`, id)
		require.NoError(t, err)
		_, me := login(t, h, "late-"+ids.New()+"@example.test")
		require.Len(t, me["tenants"], 1)
	})
}

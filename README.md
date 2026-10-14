# Orbit v2 — control

The Go 1.25 control plane is Orbit's sole public HTTP API. WP1 supplies identity,
tenants, workspaces, RBAC and append-only auditing. It uses `net/http`, pgx v5,
handwritten SQL and goose migrations. Agent execution belongs to orbit-runtime;
Workflow, kernel, assets and streaming implementations arrive in later packages.
No v1 data compatibility or in-memory database fallback is provided.

```
cmd/orbit-control/       api process (only the api role is implemented)
cmd/orbit-admin/         migrations, provisioning and audit verification
internal/platform/      config, logging, pgx transactions, UUIDv7, HTTP middleware
internal/auth/          OIDC/JWKS, dev Ed25519, API keys, run capabilities
internal/tenancy/       tenants, memberships, groups, workspaces and projects
internal/policy/        centralized Authorizer and explicit built-in role matrix
internal/audit/         canonical JSON / SHA-256 tenant hash chains
internal/httpapi/       standard-library routes; OpenAPI correspondence test
internal/market/snapshots/  preserved ModelScope data; importer belongs to WP5
contract/v2/schema/     WP0 JSON Schema source of truth
internal/contract/      generated WP0 types, never hand-edit
migrations/            embedded goose SQL: R1 schema, grants, RLS, partitions
integration/           disposable real-Postgres migration/RLS/HTTP suite
```

## Run

Infra initializes `orbit_owner` (migration role with BYPASSRLS), `orbit_app` and
`orbit_worker` (both NOBYPASSRLS) and the database owned by `orbit_owner`.
Migrations do not create roles. Use the app role for the API and owner role for
the admin CLI. Keep the admin database URL out of the API environment.

```sh
export ORBIT_ADMIN_DATABASE_URL='postgres://orbit_owner:...@127.0.0.1:55441/orbit?sslmode=disable'
go run ./cmd/orbit-admin migrate up
export ORBIT_DATABASE_URL='postgres://orbit_app:...@127.0.0.1:55441/orbit?sslmode=disable'
export ORBIT_AUTH_MODE=dev
export ORBIT_HTTP_ADDR=127.0.0.1:55442
go run ./cmd/orbit-control api
curl -fsS http://127.0.0.1:55442/ready
```

`migrate up` ensures monthly partitions for the current UTC month and the next
two months. Re-run it monthly (even when there are no new migrations). It moves
matching rows from default partitions before attaching new partitions. `migrate
down` reverses the latest migration and deletes its data; use only deliberately.

## Configuration

Startup validation names the invalid environment variable. Secrets and database
URLs are not logged. Configuration uses environment variables only.

| Variable | Meaning / default |
| --- | --- |
| `ORBIT_DATABASE_URL` | Required app PostgreSQL URL for `api` |
| `ORBIT_ADMIN_DATABASE_URL` | Required owner PostgreSQL URL for admin commands |
| `ORBIT_HTTP_ADDR` | `127.0.0.1:8080` |
| `ORBIT_AUTH_MODE` | `oidc`; explicitly use `dev` for the built-in issuer |
| `ORBIT_OIDC_ISSUER` | Required discovery issuer in OIDC mode |
| `ORBIT_OIDC_CLIENT_ID` | Required browser PKCE client ID in OIDC mode |
| `ORBIT_OIDC_AUDIENCE` | Required JWT audience in OIDC mode |
| `ORBIT_SIGNING_KEY_FILE` | `.orbit-data/signing.key`; generated Ed25519 PKCS8, mode 0600 |
| `ORBIT_ALLOW_DEV_AUTH_ON_NETWORK` | `1` permits a non-loopback dev listener; default denied |
| `ORBIT_LOG_FORMAT` | `json` or `text`, default `json` |
| `ORBIT_CORS_ORIGINS` | Comma-separated exact origins; default denies cross-origin calls |
| `ORBIT_TEST_DATABASE_URL` | Superuser URL to a **disposable** test database |
| `ORBIT_REQUIRE_PG_TESTS` | `1` makes missing PG configuration fail instead of skip |

Share the signing key securely across API replicas. It also derives the key used
to encrypt 24-hour idempotent replay responses, including newly issued API keys.
Changing this key invalidates dev/capability tokens and outstanding replay data.

## Authentication and API

[docs/openapi.yaml](docs/openapi.yaml) is the OpenAPI 3.1 contract. Lists return
`{items: [...], next_cursor: "..."}`; an empty cursor means no more results.
`limit` defaults to 50 (maximum 100). Lists sort by UUIDv7, with opaque cursors.
Tenant writes support `Idempotency-Key`; reusing a key with a different body,
method or request URI returns 422. Expiration is 24 hours. The response, audit
and business change commit in one tenant transaction. Expired records no longer
block reuse. Errors always have `{error:{code,message,retryable,details}}`.

- `GET /v1/auth/config` supplies the web login configuration.
- OIDC uses discovery and cached JWKS to verify JWT `iss`, `aud`, signature and
  expiry. The web supplies its PKCE access token as `Authorization: Bearer ...`.
  Identity mapping is issuer plus subject, never a client-provided principal ID.
- Dev mode logs a startup warning. `POST /v1/auth/dev/token` accepts
  `{email, display_name?}` and returns `{access_token, expires_in}`. It is disabled
  in OIDC mode. It is deliberately restricted to trusted development networks.
- JIT creates a global identity; an identity with no memberships receives a
  personal tenant. Existing invited or suspended members receive no new tenant.
  Invited membership is activated explicitly by an administrator through PATCH.
- Authenticated requests select a tenant with `X-Orbit-Tenant`; active membership
  is required. `/v1/me` and `/v1/tenants` also work without that header. `/v1/me`
  returns exactly `{principal,tenants,current_tenant,roles}`; `current_tenant` is
  null without a header, and `roles` contains direct and group-inherited roles.
- API keys use the same Bearer header plus `X-Orbit-Tenant`; only their SHA-256
  hash is stored in `api_keys`. Their explicit action scopes intersect RBAC;
  expiry, principal status and membership are checked on every call.
  `last_used_at` updates asynchronously at most once per minute per key.
- `IssueCapability` / `VerifyCapability` in `internal/auth` bind a five-minute
  `aud=kernel`, `sub=run:<UUID>` token to its tenant/run. WP6 uses these functions.

Tenant admin can manage members, API keys and audit; workspace creators get
`ws.admin`. Tenant members require workspace roles to read another workspace.
Workspace roles inherit to workspace children; asset/session roles apply only
to their own resource. Platform roles and Cedar are outside WP1.
The complete action matrix is in `internal/policy/policy.go` and tested.
Groups and projects have internal transactional provisioning operations for use
by later APIs; this package adds only the public paths requested for WP1.

## Admin CLI

Admin operations use the privileged owner connection and append audit records.
Creating a user without `--tenant` atomically provisions its personal tenant.
The default identity issuer is `orbit:dev`, and subject defaults to email.
For OIDC provisioning, pass the exact discovery issuer and provider subject.

```sh
go run ./cmd/orbit-admin migrate status
go run ./cmd/orbit-admin seed --email dev@orbit.local
go run ./cmd/orbit-admin create-user --email alice@example.org --name Alice
# Use IDs returned by the preceding commands:
go run ./cmd/orbit-admin create-tenant --principal UUID --slug engineering --name Engineering
go run ./cmd/orbit-admin create-user --email bob@example.org --tenant UUID --actor OWNER_UUID --role tenant.member
go run ./cmd/orbit-admin grant-role --tenant UUID --principal USER_UUID --actor OWNER_UUID --role tenant.admin
go run ./cmd/orbit-admin grant-role --tenant UUID --principal GROUP_UUID --principal-type group --actor OWNER_UUID --role ws.editor --scope-type workspace --scope WORKSPACE_UUID
go run ./cmd/orbit-admin audit verify --tenant UUID
```

Auditing serializes appends using a tenant advisory transaction lock. Each record
hashes the previous hash followed by canonical JSON (sorted object keys, UTC
microsecond timestamp). Verification explicitly limits itself to the selected
tenant even on the owner connection. App has INSERT/SELECT only on audit and
usage ledger; worker has SELECT/INSERT/UPDATE only on the three plan §5 tables.
Its effect updates cannot rewrite immutable inputs or completed effects.
Every tenant-bearing table, including partition children, enables FORCE RLS.
Global principal/tenant writes are explicit; narrow SECURITY DEFINER functions
only discover the authenticated principal's tenant memberships.

## Validation

Go tests need no external services unless PG integration is enabled. The PG
suite performs **up/down/up**, so never point it at a shared or production DB.
Infra/test bootstrap must create the three roles before it runs. A superuser
connection lets the suite exercise actual role permissions using SET ROLE; all
business fixtures and HTTP requests run as `orbit_app`.

```sh
gofmt -l .
go vet ./...
go test ./...
ORBIT_REQUIRE_PG_TESTS=1 ORBIT_TEST_DATABASE_URL='postgres://postgres:...@127.0.0.1:55441/orbit?sslmode=disable' go test ./...
bash scripts/check-contracts-gen.sh
bash scripts/check-openapi.sh
go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...
```

The integration suite discovers all tenant tables through information_schema;
it checks ownership, ENABLE/FORCE RLS, real app inserts, tenant B invisibility,
forbidden updates and inserts, direct partitions, append-only rights, and worker
access. HTTP coverage includes login, workspace creation, members, RBAC, group
inheritance, cursor pagination, API key scopes/revocation, encrypted replay,
concurrent duplicate writes and concurrent audit chains. OIDC tests use a local
JWT/JWKS provider with invalid signature/issuer/audience/expiry cases.
Dependencies and validation tools are version-pinned. No third-party source was
ported in WP1; market snapshot data is preserved unchanged.

The WP1 check used Go 1.25.14 for compatibility and PostgreSQL tests. Dependency
vulnerabilities were fixed with `x/text v0.41.0` and `go-jose/v4 v4.1.4`.
`govulncheck v1.1.4` finds no vulnerabilities on the host Go 1.27.2, but reports
nine standard-library vulnerabilities on Go 1.25.14 (GO-2026-6617, -6613, -6612,
-6611, -6610, -6608, -6607, -6605, -6603), with fixes listed in Go 1.26.9.
The requested Go 1.25 baseline is retained; the CI security gate intentionally
remains blocking until the baseline is upgraded or a patched 1.25 build exists.
The container and CI compiler are pinned to 1.25.14 rather than an older patch.

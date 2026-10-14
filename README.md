# Orbit v2 — control

The Go 1.27 control plane is Orbit's sole public HTTP API. WP1 supplies identity,
tenants, workspaces, RBAC and append-only auditing. It uses `net/http`, pgx v5,
handwritten SQL and goose migrations. Agent execution belongs to orbit-runtime;
Workflow, kernel, assets and streaming implementations arrive in later packages.
No v1 data compatibility or in-memory database fallback is provided.

```
cmd/orbit-control/       api process (only the api role is implemented)
cmd/orbit-admin/         migrations, provisioning and audit verification
internal/platform/      config, logging, pgx transactions, UUIDv7, HTTP middleware
internal/auth/          OIDC/JWKS, dev Ed25519, API keys, run capabilities
internal/tenancy/       tenants, memberships, groups and workspaces
internal/policy/        centralized Authorizer and explicit built-in role matrix
internal/audit/         RFC 8785 JCS / SHA-256 tenant hash chains
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
export ORBIT_PERSONAL_TENANTS=true
export ORBIT_HTTP_ADDR=127.0.0.1:55442
export ORBIT_PUBLIC_ORIGIN=http://127.0.0.1:55442
# Generate once and persist securely for all replicas and subsequent restarts.
export ORBIT_SECRETS_KEK="$(openssl rand -base64 32)"
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
| `ORBIT_SIGNING_KEY_FILE` | Dev JWT key; `.orbit-data/signing.key`, generated Ed25519 PKCS8, mode 0600 |
| `ORBIT_CAPABILITY_KEY_FILE` | Independent capability key; `.orbit-data/capability.key`, Ed25519 PKCS8, mode 0600 |
| `ORBIT_CAPABILITY_KEY_ID` | Active capability JWT `kid`, default `1` |
| `ORBIT_CAPABILITY_PUBLIC_KEYS` | Additional verification keys for rotation, comma-separated `kid:base64` raw 32-byte public keys |
| `ORBIT_SECRETS_KEK` | Required base64 32-byte KEK; replay encryption is purpose-derived via HKDF-SHA256 |
| `ORBIT_SECRETS_KEY_VERSION` | Positive active KEK version, default `1`; stored with each replay response |
| `ORBIT_SECRETS_KEK_PREVIOUS` | Decryption KEKs for rotation, comma-separated `version:base64` 32-byte keys |
| `ORBIT_PERSONAL_TENANTS` | `false`; `true` provisions a personal tenant on first JIT login; dev example enables it |
| `ORBIT_PUBLIC_ORIGIN` | Exact edge public origin (scheme + host + optional port); same-origin requests are allowed |
| `ORBIT_ALLOW_DEV_AUTH_ON_NETWORK` | `1` permits a non-loopback dev listener; default denied |
| `ORBIT_LOG_FORMAT` | `json` or `text`, default `json` |
| `ORBIT_CORS_ORIGINS` | Comma-separated exact cross-origin allowlist; unlisted origins get no ACAO, and unsafe methods return 403 |
| `ORBIT_TEST_DATABASE_URL` | Superuser URL to a **disposable** test database |
| `ORBIT_REQUIRE_PG_TESTS` | `1` makes missing PG configuration fail instead of skip |

Use [.env.example](.env.example) for development settings. Share the active signing
keys and KEK securely across replicas. Dev JWT, capability JWT and replay keys are
independent. Rotate capabilities by choosing a new key file and `kid`, retaining
old public keys for verification until outstanding five-minute tokens expire.
Rotate replay encryption by incrementing `ORBIT_SECRETS_KEY_VERSION`, supplying
the new KEK and retaining old KEKs in `ORBIT_SECRETS_KEK_PREVIOUS` for at least the
24-hour replay lifetime. Unknown key versions fail closed.

CORS always sets `Vary: Origin`. The public origin and allowlisted origins are
accepted; allowlisted browser calls receive ACAO and preflights have a 600-second
Max-Age. Unlisted GET/HEAD/OPTIONS receive no ACAO; other unlisted methods are
rejected. Panic diagnostics include stack and type; sensitive structured fields
and unstructured panic strings are redacted.

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
- Existing OIDC identities are read without JIT writes. First JIT identity creation,
  optional personal tenant provisioning and its login audit commit atomically.
  OIDC token refreshes do not create login audit entries or replay markers.
- With `ORBIT_PERSONAL_TENANTS=false`, a first login authenticates without any
  tenant. Admins create/list/revoke email invitations under `/v1/admin/invitations`.
  `POST /v1/invitations/{token}:accept` needs a logged-in human with a verified
  email matching the invitation (case-insensitive), without a tenant header.
  Tokens are random bearer secrets stored as hashes; acceptance is single-use,
  expires as configured, and cannot reactivate or escalate an existing membership.
  Invitation creation, revocation and acceptance are audited. Direct additions
  always record server-selected `source=direct`.
- Authenticated requests select a tenant with `X-Orbit-Tenant`; active membership
  is required. `/v1/me` and `/v1/tenants` also work without that header. `/v1/me`
  returns exactly `{principal,tenants,current_tenant,roles}`; `current_tenant` is
  null without a header, and `roles` contains direct and group-inherited roles.
- API keys use the same Bearer header plus `X-Orbit-Tenant`; only their SHA-256
  hash is stored in `api_keys`. Their explicit action scopes intersect RBAC;
  expiry, principal status and membership are checked on every call.
  `last_used_at` updates asynchronously at most once per minute per key.
- API key issuance requires `apikey.manage`; a human may issue only for themself.
  `tenant.manage` also permits creation/listing of service principals at
  `/v1/admin/service-principals` and issuance for those services. Requested scopes
  must fit both issuer and recipient effective permissions, including issuer key
  scopes. Any excess returns 403. Revoke via `DELETE /v1/admin/api-keys/{k}`.
- `CapabilitySigner.IssueCapability(CapabilityClaims)` signs five-minute tokens
  (`aud=kernel`, `sub=run:<UUID>`, `kid`) with tenant/run/member/seq/purpose and an
  optional lease_id. `VerifyCapability(token)` returns those claims after signature,
  audience, expiry and not-before checks. Callers must compare current run state.

Tenant admin can manage members, API keys and audit; workspace creators get
`ws.admin`. Tenant members require workspace roles to read another workspace.
Workspace roles inherit to workspace children; asset/session roles apply only
to their own resource. Platform roles and Cedar are outside WP1.
The complete action matrix is in `internal/policy/policy.go` and tested.
Unreadable workspaces return 404; readable workspaces without write permission
return 403. Groups can be provisioned through the admin CLI below.

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
go run ./cmd/orbit-admin create-group --tenant UUID --actor OWNER_UUID --name Reviewers
go run ./cmd/orbit-admin add-group-member --tenant UUID --actor OWNER_UUID --group GROUP_UUID --principal USER_UUID
go run ./cmd/orbit-admin audit verify --tenant UUID
# Always removes expired HTTP replays. An explicit cutoff also prunes usage:
go run ./cmd/orbit-admin maintenance prune
go run ./cmd/orbit-admin maintenance prune --usage-before 2025-10-01T00:00:00Z
```

Auditing serializes appends using a tenant advisory transaction lock. The hash
format is `SHA256(previous_hash_bytes || JCS(record))`, with an initial previous
hash of 32 zero bytes and RFC 8785 UTF-8 canonicalization. `record` includes id,
tenant_id, actor_id, actor_kind, action, target_type, target_id, detail, ip,
user_agent and occurred_at (UTC RFC3339 with microsecond precision). JCS numbers
use IEEE 754 binary64; represent larger exact integers as JSON strings. The JCS
package is checked against RFC 8785 Appendix B and Unicode/invalid-input vectors.

`audit verify` explicitly filters the tenant even with the owner connection and
returns `{"tenant_id":"...","head":{"count":N,"hash":"<latest SHA-256 hex>"}}`.
Periodically retain this head in an external trusted store to detect tail
truncation; internal chain verification alone cannot detect a removed tail.

App has INSERT/SELECT only on audit and usage ledger; worker has
SELECT/INSERT/UPDATE only on the three plan §5 tables. Its effect updates cannot
rewrite immutable inputs or completed effects. Every tenant-bearing table,
including partition children, enables FORCE RLS. Empty or absent tenant context
returns no rows. Global identity/tenant discovery is explicit; all tenant writes
use `WithTenant`, including provisioning a new tenant.

`usage.RecordUsage` claims `usage_keys` with `ON CONFLICT DO NOTHING RETURNING`
then inserts the ledger row in the same transaction. Duplicates across months
are successful no-ops. `usage_keys.created_at` records the corresponding ledger
occurrence time so both use the same retention boundary. `maintenance prune`
deletes expired `idempotency_keys`; with `--usage-before`, it also removes usage
ledger rows and their unused keys before the same cutoff. Run it regularly using
the owner connection and the deployment's usage retention policy.

`run_events.ts` is mandatory and comes from the immutable contract RunEvent,
never the insertion clock. Event IDs are `<run>:<member>:<seq>:<attempt>:<n>`.
Replay deduplicates by `(tenant_id,run_id,event_id,ts)`; historical reads and
cursors order by `(ts,event_id)`. `seq` need not be globally unique. Monthly
partition boundaries use UTC regardless of the administrator's session timezone.

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

The compiler and CI are pinned to Go 1.27.2 (`go 1.27.0` plus toolchain directive).
`govulncheck v1.1.4` is the pinned security gate.

Contract boundary validation uses `contract.Validate` to accept future fields on
`x-extra=ignore` objects; `contract.ValidateStrict` checks producer output against
the original schemas. Both return only JSON paths and rule names on invalid input.

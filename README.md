# Orbit v2 — control

The Go 1.27 control plane is Orbit's sole public HTTP API. WP1 supplies identity,
tenants, workspaces, RBAC and append-only auditing. It uses `net/http`, pgx v5,
handwritten SQL and goose migrations. Agent execution belongs to orbit-runtime;
WP5 adds asset storage, releases and run-lock resolution; kernel and streaming
implementations arrive in later packages.
No v1 data compatibility or in-memory database fallback is provided.

```
cmd/orbit-control/       api | worker | all (API and ctl worker)
cmd/orbit-admin/         migrations, provisioning and audit verification
internal/platform/      config, logging, pgx transactions, UUIDv7, HTTP middleware
internal/auth/          OIDC/JWKS, dev Ed25519, API keys, run capabilities
internal/tenancy/       tenants, memberships, groups and workspaces
internal/policy/        centralized Authorizer and explicit built-in role matrix
internal/assets/        immutable versions, bounded bundles, Ed25519 signatures
internal/locks/         signed dependency resolution and per-member run locks
internal/workflows/assetrelease/  ctl release Workflow, Activities and dispatcher
internal/audit/         RFC 8785 JCS / SHA-256 tenant hash chains
internal/httpapi/       standard-library routes; OpenAPI correspondence test
internal/market/snapshots/  preserved ModelScope data; gzip NDJSON import sources
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
# Set ORBIT_OBJECT_* to the provisioned MinIO bucket/control credentials.
# See .env.example; worker and all also need ORBIT_TEMPORAL_*.
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
| `ORBIT_OBJECT_ENDPOINT` | Required S3/MinIO host:port (without URL scheme) |
| `ORBIT_OBJECT_BUCKET` | Provisioned bucket, default `orbit`; startup does not create it |
| `ORBIT_OBJECT_ACCESS_KEY` / `ORBIT_OBJECT_SECRET_KEY` | Required control storage credentials |
| `ORBIT_OBJECT_TLS` | `false`; set `true` for HTTPS |
| `ORBIT_ASSET_UPLOAD_CONCURRENCY` | Maximum concurrent upload pipelines per API process, default `2`, range `1–32` |
| `ORBIT_ASSET_SIGNING_KEY_FILE` | Independent Ed25519 PKCS8 PEM, `.orbit-data/assets.key`; regular file, mode 0600; generated only in dev |
| `ORBIT_ASSET_SIGNING_KEY_ID` | Signature key ID, default `1` |
| `ORBIT_ASSET_PUBLIC_KEYS` | Verification rotation keys, comma-separated `kid:base64` raw public keys |
| `ORBIT_TEMPORAL_ADDRESS` | Worker/all Temporal host:port, default `127.0.0.1:7233`; dev stack `127.0.0.1:57233` |
| `ORBIT_TEMPORAL_NAMESPACE` | Default `default`; dev stack `orbit-dev` |
| `ORBIT_TEMPORAL_TLS` | `false`; enables TLS with server certificate verification |
| `ORBIT_TEMPORAL_TLS_CA_FILE` | Optional private CA PEM; otherwise system roots |
| `ORBIT_TEMPORAL_TLS_CERT_FILE` / `ORBIT_TEMPORAL_TLS_KEY_FILE` | Optional paired client certificate/key for mTLS |
| `ORBIT_TEMPORAL_TLS_SERVER_NAME` | Optional certificate server-name override |
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
  (`aud=kernel`, `sub=run:<UUID>`, `kid`) with tenant/run/purpose and
  member/seq/attempt for segment tokens or lease_id for sandbox tokens.
  `VerifyCapability(token)` returns those claims after signature,
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

`contract.RunLockDigest` omits the top-level `lock_id` and `digest` before JCS
hashing; `BundleDigest` checks each entry’s sorted file digests. Sandbox renewal
uses `SandboxRenewInput` with required positive `ttl_seconds`; operations with no
output use the shared `EmptyResult`, whose Go zero value encodes as `{}`.

## Assets (WP5-control)

`orbit-control worker` registers `AssetReleaseWorkflow` and asset Activities on
`ctl`. `all` runs the API and that worker in one process. These Go ctl Activities
use **orbit_app**, since their writes include asset tables and audit. The
separate Python execution worker remains restricted to the three WP1 tables.
Worker-only startup requires database, object storage, asset signing and Temporal
configuration, without OIDC client or HTTP listener configuration.

Register `agent`, `skill`, `mcp_server` or `team` at `POST /v1/assets`. Visibility
is `private`, `workspace` or `tenant`. Upload a ZIP/tar.gz binary (or a single
multipart `archive` file) to
`POST /v1/assets/{a}/versions?version=1.2.3` with `Idempotency-Key`. Contents are
immutable from creation; retrying the same HTTP request returns the saved
response. Use a new version for changed content. File browsing uses
`.../{v}/files` and `.../{v}/file?path=AGENTS.md`; files are downloaded as attachments
with a detected media type and a 20 MiB read limit. Lists use opaque cursors;
file/channel/grant cursors encode their stable composite key rather than a UUID.

Archive limits are 512 files, 20 MiB per binary file, 50 MiB total/input,
256 KiB per text file, 4 MiB text total, depth 32 and expansion ratio 200.
Paths are relative POSIX paths; absolute/parent/dot paths, backslashes, links,
special files, duplicate names, file/directory conflicts and case conflicts are
rejected, including excluded entries. Unicode NFC/NFD name collisions are rejected;
PAX global metadata headers from `git archive` are ignored within the archive budget. No archive path is written to disk.
Normalized ZIPs have fixed metadata and sorted entries. Publishing/checksum
excludes `.git`, `.archive`, `__pycache__`, `.pytest_cache`, `node_modules`, `out`,
`.venv`, `.pyc`, OS metadata and credential filenames/extensions, plus `.env*`
except `.env.example`/`.env.template`. Exclusions are returned in the upload audit.
Per-file blobs and the normalized archive are stored at
`t/<tenant>/blobs/<bare sha256>`; `blob_ref` identifies the archive by its byte
hash, while `content_digest` is the contract's file-set hash. They intentionally
have different preimages. Referenced immutable file blobs remain tenant-local. Blob writes finish before
the metadata/idempotency transaction opens. Failed transactions can leave
unreferenced content-addressed blobs. Upload concurrency is bounded before body
reading and held until commit; decoded file bytes are released after staging.

Submission `.../{v}:submit` freezes visibility and queues the release durably.
The dispatcher starts `asset:{tenant}:{asset_version_id}` only after commit.
Five runtime checks use the generated `AssetCheck*` names on `asset-check`;
`AssetCheckPermissionDiff` runs on `ctl`. Control validates manifest schemas and
extracts dependencies/declared permissions; runtime owns semantic bundle checks.
No sandbox trial, evaluation, LLM semantic review, public market or evolution is
implemented. All six reports must succeed before approval. Permission expansion
forces review, including a first release requesting network/secrets/effects.
Private and exempt-workspace releases otherwise skip manual approval.

Workspace settings `assets.review_required` (default true) and `assets.approvals`
(default 1, range 1–20) configure workspace release review. Tenant tool-rule policy
body `asset_review.approvals` sets the default approval count. Tenant-visible
assets always require human review with max(tenant, workspace) approvals.
Workspace review settings apply directly only to workspace-visible assets; private
assets ignore them. Workspace editors may review workspace
assets; tenant reviewers review tenant assets and escalated private releases.
The owner, submitter and uploader cannot review. Each reviewer votes once per submission;
any committed rejection wins, including a rejection queued concurrently with the
last approval. Decisions are persisted with audit before `SubmitReview` Update
delivery; HTTP returns 202 for delivery, not a completed publication.
A 48-hour durable timer writes one SLA reminder. `Withdraw` cancels pending work;
the §5.4 author-withdraw action returns `submitted`, `checking` or `in_review`
to `draft` and appends `asset.version.withdrawn`; the same immutable version may
be submitted again with a new generation. Approved versions cannot be withdrawn with this operation.
`NotifyAuthor` uses an idempotent **audit event**, not a separate notification table.

`Sign` uses Ed25519 over JCS of
`{kind,publisher,name,version,contentDigest,permissionsDigest}`; permissionsDigest
is the JCS object digest of the stored effective and per-member permissions. Keep old public keys configured
until all versions signed by them are retired. `Publish` commits version state,
channel and audit atomically. Its saga handles ambiguous commits by restoring the
previous channel only if it still points at this release, and restoring the
approved state/latest-version projection. This restoration is an explicit saga
operation; normal state changes always use the lifecycle transition table.

`POST /v1/assets/{a}/channels/{c}` sets/rolls back stable, beta or a custom channel.
Only signed published/deprecated targets are accepted. `:transition` exposes
only draft reset, deprecation, suspension, yank and reviewer-authorized restore;
it cannot approve or publish a new version. Asset DELETE archives the registration.
`asset_grants` cannot cross tenant/workspace visibility boundaries. Installation
uses a semver range, optional channel and optional pinned version ID; unavailable
pins/channels fail rather than silently selecting another version.

`locks.Resolver` is an interface; `locks.NewResolver(assetService)` implements it
and resolves recursively, verifies
signatures/file-set digests and rejects cycles, incompatible constraints and
unavailable versions. Without a pin/channel, it selects the highest satisfying
version; later conflicts fail explicitly without solver backtracking.
`scope.WorkspaceID` selects workspace installations;
`Resolve(ctx, locks.Request{TenantID, LockID, Entry, Scope})` only resolves.
The caller supplies LockID; WP6 uses `uuid5(run_id,"lock")`. After inserting the
run, call `locks.Save(ctx, tx, runID, lock)` in that same authenticated tenant
transaction to persist the complete RunLock and audit.
`POST /v1/workspaces/{w}/locks:preview` supplies a fresh UUID and never saves. The caller
provides the authenticated principal/tenant and tenant transaction context.
Embedded agents/skills become derived entries verified by their signed parent's
file set. Embedded entry refs (such as `acme/team/members/reviewer`) are
synthetic identities and their paths are relative to the embedded bundle; do not
look them up by version/ref in asset_files. WP6 can call
`resolver.Files(ctx, lock, entryIndex)` to read and verify each file by its sha256
from tenant-local blob storage. Authorize the run/lock before reading its files.
A single agent has member `main`; team counts expand to `name-1`, etc.
Team sharedSkills apply to every member. Active tenant/workspace tool-rule policy
bodies may provide `effective_permissions`, `skills_visibility` and per-principal
`users[principal_id]` overrides; workspace settings also accept `skills_visibility`.
Tenant/workspace administrator allow lists intersect as ceilings; author
agent/team/member allow lists can only narrow them. Deny always wins. An omitted
dimension means unrestricted; an explicit empty allow/permission array denies
that dimension. An omitted sandbox profile does not restrict profiles; a
configured conflicting profile denies resolution. Team member visibility files
use `members/<role-name>/skills-visibility.json`, shared by all counted instances;
embedded members also apply their own bundle-root visibility file. Tenant/workspace/user caps only narrow
member permissions. RunLock digests use the unchanged WP0b helper.

### Release recovery and administration

Infrastructure failure preserves the current nonterminal business state and
records a payload-free `release_error` code. The dispatcher isolates delivery
errors per tenant/version, retries transient errors, and pauses permanent errors
(`release_retryable=false`) until an operator requests recovery. A nonterminal
submitted/checking/in_review/approved version without a running Workflow is
restarted with the same Workflow ID and generation. Approved recovery proceeds
to Sign/Publish; in-review recovery hydrates committed votes. Generation checks
fence old submissions. The Publish saga alone may restore published to approved
after an ambiguous commit. Business rejection remains terminal.

```sh
go run ./cmd/orbit-admin asset retry-release --tenant UUID --version UUID
```

Workspace installation/uninstallation requires `workspace.update` and an asset
visible to that entire workspace; tenant installation requires `policy.manage`
and tenant visibility. Narrowing visibility atomically revokes and audits grants
outside the new ceiling. `POST /v1/assets/{a}:restore` restores suspended/archived
asset metadata to active. Version transitions retain the §5.4 lifecycle guards.
`orbit_app` cannot delete asset_versions/asset_files or update asset_files.

Team release permissions include the root and every embedded member agent.json,
with per-member declarations retained in the manifest. Permission-diff checks
compare every member's tools, data scopes, effects, secrets, network and sandbox
profile, including newly requested rights already held by another member.
Signatures cover the stored canonical JCS identity and the complete canonical
permission payload, preserving unknown future fields without re-marshalling them.

### Market snapshot import

The preserved snapshots are **gzip NDJSON**, not JSON arrays: agents has 654 rows
with files/prompts, MCP has 12,544 README catalog rows, and skills has 83,159
metadata-only rows. Import supports agents with embedded instructions/files and
MCP rows with an explicit portable JSON configuration in their README. Metadata-only
skills and MCP rows without a convertible config are counted as unsupported;
no executable content is invented or fetched from third-party URLs. Agents retain
snapshot files; MCP conversion copies only runtime fields, converts endpoint,
header/env/argument credentials to `${secret:NAME}`, and normalizes transport names.
A portable config contains exactly one server, valid env/header keys and a global
endpoint address; shared/CGNAT and other special-use IP ranges are rejected.
Unsupported local commands/paths are rejected. Existing imports are skipped by
stable source-derived name. Import commits each row independently.

```sh
go run ./cmd/orbit-admin market import --tenant UUID --snapshot internal/market/snapshots/agents.json.gz
# Optional --actor UUID; default is an active direct human tenant owner.
```

Imported rows pass through draft creation and normal submission, then remain
pending until the normal checks and human review publish them. Admin import uses
explicit tenant filters even with owner credentials. It never signs or approves.
Apache-2.0 adaptations are registered in NOTICE.

### Asset validation

```sh
go run go.temporal.io/sdk/contrib/tools/workflowcheck@v0.6.0 ./internal/workflows/...
# Complete PG suite, including assets/RLS/channel audit (disposable database only):
ORBIT_REQUIRE_PG_TESTS=1 ORBIT_TEST_DATABASE_URL='postgres://...' go test ./...
# Add these to that command for J5 against a real Temporal dev server and MinIO:
# ORBIT_REQUIRE_ASSET_E2E=1 ORBIT_TEST_TEMPORAL_ADDRESS=127.0.0.1:57233
# ORBIT_TEST_TEMPORAL_NAMESPACE=orbit-control-wp5-test
# ORBIT_TEST_OBJECT_ENDPOINT=127.0.0.1:59100 ORBIT_TEST_OBJECT_BUCKET=...
# ORBIT_TEST_OBJECT_ACCESS_KEY=... ORBIT_TEST_OBJECT_SECRET_KEY=...
```

J5 starts a Go fake asset-check worker in the isolated test namespace (registered
if absent), exercises upload/check/review/publish/install/preview/rollback and
replays the resulting real history. `ORBIT_UPDATE_REPLAY=1` refreshes the checked-in
fixtures (approval, rejection, multiple reviewers, withdrawal and compensation). Testsuite covers branch behavior and virtual-time SLA; SDK testsuite has
no history export, so the replay fixture comes from the real-server Go harness.
`go test ./...` replays all checked-in fixtures offline. `workflowcheck v0.6.0` supports the
pinned Go toolchain; v0.3.0's old x/tools cannot compile on Go 1.27.

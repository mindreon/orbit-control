# AGENTS.md — orbit-control v2

Read README.md and `../docs/v2/implementation-plan.md` before changes. Orbit
control is the only public HTTP API. Agent execution belongs to orbit-runtime.

## Layout and boundaries

- Go 1.27, `net/http` method-aware ServeMux, pgx v5 + handwritten SQL, goose SQL.
  No gin, gorm, viper, agent runtime or LLM SDK.
- `cmd/orbit-control api` is the only current service role. Add worker/all only
  when Workflow implementations exist. `cmd/orbit-admin` handles provisioning,
  migrations and tenant audit verification.
- Create only packages used by the current work. See README for the v2 layout,
  CLI commands and all environment variables.
- Public paths/error/pagination/security shapes go in `docs/openapi.yaml` first;
  route table and OpenAPI must match exactly. Contract v2 schemas remain the
  source of truth; use `scripts/gen-contracts.sh`, never edit generated types.
- Preserve `internal/market/snapshots/*.json.gz`; WP5 owns loading these data.

## Persistence, authorization and auditing

- All tenant operations run through `db.WithTenant`, which sets
  `app.tenant_id` transaction-locally. Global identities/tenant discovery use
  `db.Global` explicitly. Never fall back to an in-memory store or AutoMigrate.
- Migrations execute as orbit_owner; infra creates roles. Tenant tables and
  partition children must ENABLE + FORCE RLS with USING and WITH CHECK.
  App/worker must never be owners or BYPASSRLS. Worker may access only the
  three plan §5 tables. Audit/usage ledger are append-only for app.
- Centralize decisions in `policy.Authorizer`. API-key scopes intersect RBAC;
  group bindings inherit; tenant selection always checks active membership.
- Business mutations, audit appends and encrypted idempotent responses share
  one transaction. Audit hashes use canonical JSON and tenant advisory locks.
  Include explicit tenant filters when owner/admin access could bypass RLS.
- No secret plaintext in logs, fixtures, examples or replay storage. Signing
  keys are 0600. Dev auth refuses non-loopback unless explicitly opted in.

## Verify and deliver

`gofmt -l .`, `go vet ./...`, `go test ./...`, contracts and OpenAPI checks must
pass. PG tests skip without `ORBIT_TEST_DATABASE_URL`, fail when
`ORBIT_REQUIRE_PG_TESTS=1`, and destructively migrate a disposable database.
Run them through actual app/worker roles. Run pinned govulncheck, migration CLI
and `/ready` startup smoke before declaring WP1 complete. No migration/role
checks against shared production databases.

State assumptions before implementing, keep changes surgical, choose the
smallest solution, and verify the requested behavior with meaningful failure
cases. No TODOs, empty future packages or speculative abstractions. Work only
in this v2 worktree. Make small commits explaining why; do not push. Preserve
Apache-2.0 attribution and register NOTICE if porting third-party source.

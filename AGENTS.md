# AGENTS.md — orbit-control

One-pager for humans and coding agents contributing here.

## What this repo is

The sole **public HTTP API** for Orbit: the task center (TaskWorkflow), profiles, SOPs, policy, artifacts, and the
tenant catalog. Read [README.md](./README.md) before adding files.

The agent loop is **AgentScope on orbit-worker**, not this process. Do not add agent runtimes or LLM SDKs here.

## Rules

- Postgres persistence follows contract §18: gorm over pgx behind `store.Repository`, goose migrations, no
  AutoMigrate. Tenant tables are reached only through `inTenant`, which sets `app.tenant_id` first. No data
  backward-compatibility shims.
- Persistence is verified by E2E (`e2e/persistence`, build tag `e2e`) through the HTTP API against real Postgres,
  producing `artifacts/e2e-persistence-report.json`. An isolated SQL/role/static check needs an entry in
  `docs/persistence-failure-modes.md` committed before the check.
- **No tenant secret plaintext** in logs, fixtures, or OpenAPI examples.
- `go test ./...` must pass without external services (the Postgres E2E suite is behind the `e2e` build tag).

## Where things go

| Change | Put it here | Not here |
| --- | --- | --- |
| Public path or error shape | `docs/openapi.yaml` first | orch / worker repos |
| HTTP mux | `internal/httpapi` | New frameworks |
| Process entrypoint | `cmd/orbit-control` | Multiple public binaries |
| Contract v3 types | `scripts/sync-contracts.sh` from orbit-runtime | Hand edits to `internal/contract/v3` |

`orbit-web` must keep calling **only** this API.

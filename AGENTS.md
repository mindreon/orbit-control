# AGENTS.md — orbit-control

One-pager for humans and coding agents contributing here.

## What this repo is

Sole **public HTTP/WS API** for Orbit: tenants, accounts, personas, rooms, approvals, secrets, billing, cloud-agent jobs.

Read [README.md](./README.md) and [ARCHITECTURE.md](./ARCHITECTURE.md) before adding files.

The agent loop is **dsh on orbit-worker**, not this process. Do not add dsh, Pi, or LLM SDKs here.

## W1 rules

- In-memory rooms, messages, HITL, and SSE are allowed. Still no OAuth.
- Postgres persistence follows contract §18 only: pgx + goose, behind
  `store.Repository`. DB tests must skip when `ORBIT_TEST_DB_URL` is unset.
- Temporal is optional: set `TEMPORAL_ADDRESS` to drive `RoomWorkflow` (orbit-orch); otherwise control calls orbit-worker over HTTP.
- **No tenant secret plaintext** in logs, fixtures, or OpenAPI examples.
- Do not import dsh, Pi, or LLM SDKs here.

## Where things go

| Change | Put it here | Not here |
| --- | --- | --- |
| Public path or error shape | `docs/openapi.yaml` first | orch / worker repos |
| HTTP mux / stub status | `internal/httpapi` | New frameworks, ORMs |
| Process entrypoint | `cmd/orbit-control` | Multiple public binaries |
| Boundary / trust rules | `ARCHITECTURE.md` | Ad-hoc comments only |

`orbit-web` must keep calling **only** this API. Orchestration contract changes belong in [orbit-orch](https://github.com/mindreon/orbit-orch).

## Before you PR

1. Mark new public paths `501` / unimplemented until a later wave implements them.
2. Keep `401` / `403` stub schemas stable — Sentinel will assert them.
3. `go test ./...` must pass without external services (Postgres tests skip).
4. Do not add `go.mod` requires for dsh, Pi, or LLM SDKs. The only
   infrastructure dependencies are the Temporal SDK (optional path) and pgx +
   goose (contract §18).

## Cross-links

- Public contract: [docs/openapi.yaml](./docs/openapi.yaml)
- Web UI consumer: [orbit-web](https://github.com/mindreon/orbit-web)
- Orch contracts: [orbit-orch](https://github.com/mindreon/orbit-orch)
- Worker: [orbit-worker](https://github.com/mindreon/orbit-worker)

# AGENTS.md — orbit-control

One-pager for humans and coding agents contributing here.

## What this repo is

Sole **public HTTP/WS API** for Orbit: tenants, accounts, personas, rooms, approvals, secrets, billing.

Read [README.md](./README.md) and [ARCHITECTURE.md](./ARCHITECTURE.md) before adding files.

## W0 rules (still in force until a later wave removes them)

- **Zero business logic.** Docs, contract stubs, and the `501` health mux only.
- **No real auth, DB, LLM, or Temporal client.**
- **No tenant secret plaintext** in logs, fixtures, or OpenAPI examples.
- Do not implement OAuth, migrations-with-data, or encryption yet.

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
3. `go test ./...` must stay dependency-free (stdlib only).
4. Do not add `go.mod` requires for Temporal, LLM SDKs, or database drivers in W0.

## Cross-links

- Public contract: [docs/openapi.yaml](./docs/openapi.yaml)
- Web UI consumer: [orbit-web](https://github.com/mindreon/orbit-web)
- Orch contracts: [orbit-orch](https://github.com/mindreon/orbit-orch)
- Worker: [orbit-worker](https://github.com/mindreon/orbit-worker)

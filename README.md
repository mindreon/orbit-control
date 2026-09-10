# orbit-control

Orbit **control plane**. This repository is the **sole public HTTP and WebSocket API** for Orbit.

W0 status: skeleton only. No real auth, database, LLM, or Temporal client. Endpoints return `501 Not Implemented`.

## Role

`orbit-control` owns the product surface that other Orbit services must not expose:

| Domain | Responsibility (planned) |
| --- | --- |
| Tenants | Tenant lifecycle and isolation boundary |
| Accounts | Human and service identities inside a tenant |
| Personas | Agent persona definitions and assignment |
| Rooms | Conversation / work rooms |
| Approvals | Human-in-the-loop (HITL) approval records |
| Secrets | Tenant secrets — **encrypted at rest only here** |
| Billing | Plans, usage, and entitlements |
| HTTP + WS API | The only public API browsers and clients call |

This repo does **not** run orchestration workflows, execute worker jobs, or render the web UI.

## Sibling repositories

| Repo | Role | Talks to control? |
| --- | --- | --- |
| [orbit-web](https://github.com/mindreon/orbit-web) | Browser UI. **Only** talks to this control API (HTTP + WS). | Yes (client) |
| [orbit-orch](https://github.com/mindreon/orbit-orch) | Orchestration. Publishes **internal contracts** that control may call; never a public API. | Internal only |
| [orbit-worker](https://github.com/mindreon/orbit-worker) | Job / sandbox execution. Never owns tenant secrets. | Internal only |

Trust and secret boundaries are in [ARCHITECTURE.md](./ARCHITECTURE.md). Contributor rules are in [AGENTS.md](./AGENTS.md).

## Public API contract

Draft OpenAPI 3 stub (all paths unimplemented):

- [docs/openapi.yaml](./docs/openapi.yaml)

Stub groups: `health`, `rooms`, `messages`, `approvals`, `personas`, `secrets`, `cloud-agents`, plus a WebSocket upgrade path. `401` / `403` response shapes are included as placeholders for later Sentinel tests.

## Layout

```
cmd/orbit-control/   HTTP process entrypoint
internal/httpapi/    Tiny mux + 501 stub (no DB)
docs/openapi.yaml    Public HTTP/WS contract stub
```

## Run the W0 stub

Requires Go 1.22+. No database or env secrets.

```bash
go test ./...
go run ./cmd/orbit-control
```

Default listen address is `:8080` (override with `PORT`). Every route, including `GET /health`, returns:

```json
{"error":"not implemented","code":"NOT_IMPLEMENTED","message":"W0 skeleton: this endpoint is not implemented"}
```

Example:

```bash
curl -i http://127.0.0.1:8080/health
```

## Non-goals (W0)

- Real OAuth / session auth
- Database migrations or persisted data
- LLM calls
- Temporal client or workflow workers
- Decrypting or storing tenant secrets outside this service (and not even here yet)

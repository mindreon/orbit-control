# orbit-control

Orbit **control plane**. This repository is the **sole public HTTP and WebSocket API** for Orbit.

W1 status: rooms, messages, HITL approvals, and SSE events are implemented in-memory and talk to orbit-worker over HTTP. No database, no OAuth, no Temporal client yet. Other resource groups still return empty lists.

## Role

`orbit-control` owns the product surface that other Orbit services must not expose:

| Domain | Responsibility (planned) |
| --- | --- |
| Tenants | Tenant lifecycle and isolation boundary |
| Accounts | Human and service identities inside a tenant |
| Personas | Agent persona definitions (rendered to dsh presets **on the worker**, not here) |
| Rooms | Conversation / work rooms (`solo` super-agent, `collab` multi-agent) |
| Approvals | Human-in-the-loop (HITL) approval records |
| Secrets | Tenant secrets — **encrypted at rest only here** |
| Billing | Plans, usage, and entitlements |
| Cloud agents | Headless jobs (clone / turn / PR) — scheduled via orch, executed on worker |
| HTTP + WS API | The only public API browsers and clients call |

This repo does **not** run orchestration workflows, execute worker jobs, host dsh, or render the web UI.

## Sibling repositories

| Repo | Role | Talks to control? |
| --- | --- | --- |
| [orbit-web](https://github.com/mindreon/orbit-web) | Browser UI. **Only** talks to this control API (HTTP + WS). | Yes (client) |
| [orbit-orch](https://github.com/mindreon/orbit-orch) | Orchestration. Publishes **internal contracts** that control may call; never a public API. | Internal only |
| [orbit-worker](https://github.com/mindreon/orbit-worker) | Job / sandbox / **dsh ACP** execution. Never owns tenant secrets. | Internal only (grants + event ingest) |

Trust and secret boundaries are in [ARCHITECTURE.md](./ARCHITECTURE.md). Contributor rules are in [AGENTS.md](./AGENTS.md). Agent runtime choice (dsh, not Pi) lives in [orbit-orch tech-selection](https://github.com/mindreon/orbit-orch/blob/main/docs/tech-selection.md).

## Public API contract

Draft OpenAPI 3 (W1 rooms/HITL live; remaining groups empty or 501):

- [docs/openapi.yaml](./docs/openapi.yaml)

Stub groups: `health`, `rooms`, `messages`, `approvals`, `personas`, `secrets`, `cloud-agents`, plus a WebSocket upgrade path. `401` / `403` response shapes are included as placeholders for later Sentinel tests.

## Layout

```
cmd/orbit-control/     HTTP process entrypoint
internal/httpapi/      Mux (rooms, HITL, SSE, empty lists)
internal/app/          In-memory Room FSM
internal/worker/       HTTP client to orbit-worker activities
docs/openapi.yaml      Public HTTP/WS contract
```

## Run W1

Requires Go 1.22+. Point it at a running orbit-worker host.

```bash
go test ./...
ORBIT_WORKER_URL=http://127.0.0.1:8090 go run ./cmd/orbit-control
curl -s http://127.0.0.1:8080/health
# {"status":"ok"}
```

Default listen address is `:8080` (override with `PORT`).

## Non-goals (W1)

- Real OAuth / session auth
- Database migrations or persisted data
- LLM calls or dsh in this process (those live on orbit-worker)
- Temporal client or workflow workers
- Decrypting or storing tenant secrets outside this service (and not even here yet)

## Temporal (optional)

When `TEMPORAL_ADDRESS` is set (e.g. `127.0.0.1:7233`), control starts
`RoomWorkflow` on task queue `orbit` (override with `TEMPORAL_TASK_QUEUE`)
instead of calling worker HTTP for session lifecycle. orbit-orch must run a
workflow worker and orbit-worker must run an activity worker on the same queue.

Without Temporal, control keeps the W1 direct-HTTP path to orbit-worker.


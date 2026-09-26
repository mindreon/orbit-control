# orbit-control

Orbit **control plane**. This repository is the **sole public HTTP and WebSocket API** for Orbit.

W1 status: rooms, messages, HITL approvals, steer, bounded execution history,
and SSE events are implemented. Rooms, messages, approvals and idempotency keys
are stored in Postgres when `ORBIT_CONTROL_DB_URL` is set, in memory otherwise
(dev/test only). A Room pins a dsh permission preset
and exposes its runtime snapshot. Session lifecycle talks to orbit-worker over
HTTP by default, or via Temporal `RoomWorkflow` when `TEMPORAL_ADDRESS` is set.
No OAuth. Other resource groups still return empty lists.

## Container image

Pushes to `main` publish `ghcr.io/mindreon/orbit-control` (`main`, `latest`, short SHA).
`orbit-infra` pulls that image by configurable `ORBIT_IMAGE_TAG`.

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

Trust and secret boundaries are in [ARCHITECTURE.md](./ARCHITECTURE.md).
The enterprise-workbench capability analysis is in
[docs/dsh-workbench-blueprint.md](./docs/dsh-workbench-blueprint.md).
Contributor rules are in [AGENTS.md](./AGENTS.md). Agent runtime choice (dsh,
not Pi) lives in [orbit-orch tech-selection](https://github.com/mindreon/orbit-orch/blob/main/docs/tech-selection.md).

## Public API contract

Draft OpenAPI 3 (W1 rooms/HITL live; remaining groups empty or 501):

- [docs/openapi.yaml](./docs/openapi.yaml)

Stub groups: `health`, `rooms`, `messages`, `approvals`, `personas`, `secrets`, `cloud-agents`, plus a WebSocket upgrade path. `401` / `403` response shapes are included as placeholders for later Sentinel tests.

## Layout

```
cmd/orbit-control/     HTTP process entrypoint
internal/httpapi/      Mux (rooms, HITL, steer, activity, SSE, empty lists)
internal/app/          In-memory Room FSM + bounded activity timeline
internal/worker/       HTTP client to orbit-worker activities
internal/orch/         Optional Temporal client (RoomWorkflow Updates)
internal/store/        Repository interface; memstore, pgstore, goose migrations
deploy/postgres/       One-time role + database bootstrap (superuser)
docs/openapi.yaml      Public HTTP/WS contract
```

## Postgres (optional in dev)

```bash
psql -v ON_ERROR_STOP=1 -v owner_password=... -v app_password=... -v ops_password=... \
  -f deploy/postgres/bootstrap-roles.sql "$SUPERUSER_URL"
ORBIT_CONTROL_MIGRATE_DB_URL=... go run ./cmd/orbit-control   # once, with ORBIT_CONTROL_MIGRATE_ON_START=1
psql -v ON_ERROR_STOP=1 -v tenant_id=default \
  -f deploy/postgres/ensure-tenant.sql "$ORBIT_CONTROL_OPS_DB_URL"   # tenants are created by orbit_ops only
ORBIT_CONTROL_DB_URL=postgres://orbit_app:...@127.0.0.1:5432/orbit_control \
ORBIT_CONTROL_MIGRATE_DB_URL=postgres://orbit_owner:...@127.0.0.1:5432/orbit_control \
ORBIT_CONTROL_MIGRATE_ON_START=1 go run ./cmd/orbit-control
```

`DELETE /v1/rooms/{id}` checks CSRF against `ORBIT_ALLOWED_ORIGINS` (comma
separated).

## Persistence E2E (contract §18)

The S-DB-11 / S-DB-13 suite lives in `e2e/persistence` (build tag `e2e`). It
drives the scenarios through the HTTP API against a real Postgres and writes
`artifacts/e2e-persistence-report.json`, which CI uploads. Isolated SQL, role
and static checks are limited to those listed in
[docs/persistence-failure-modes.md](docs/persistence-failure-modes.md).

```bash
docker compose -f e2e/compose.yaml up -d --wait
ORBIT_TEST_DB_URL=postgres://orbit_app:e2e-app@127.0.0.1:55432/orbit_control?sslmode=disable \
ORBIT_TEST_MIGRATE_DB_URL=postgres://orbit_owner:e2e-owner@127.0.0.1:55432/orbit_control?sslmode=disable \
ORBIT_TEST_OPS_DB_URL=postgres://orbit_ops:e2e-ops@127.0.0.1:55432/orbit_control?sslmode=disable \
go test -tags e2e -count=1 ./e2e/...
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
- LLM calls or dsh in this process (those live on orbit-worker)
- Running Temporal workers here (orch hosts workflows; worker hosts activities)
- Decrypting or storing tenant secrets outside this service (and not even here yet)

## Temporal (optional)

When `TEMPORAL_ADDRESS` is set (e.g. `127.0.0.1:7233`), control starts
`RoomWorkflow` on task queue `orbit` (override with `TEMPORAL_TASK_QUEUE`)
instead of calling worker HTTP for session lifecycle. orbit-orch must run a
workflow worker and orbit-worker must run an activity worker on the same queue.

Without Temporal, control keeps the W1 direct-HTTP path to orbit-worker.


# orbit-control

Orbit **control plane**: the sole public HTTP API for Orbit. Browsers only talk to this service.

It owns the task center (TaskWorkflow): task creation and control, the plan graph, agent profiles, SOPs, the tenant
policy layer, artifacts and the per-task SSE event stream. It also serves the tenant catalog (assistants, connectors,
skills, MCP market). Tasks are stored in Postgres (`ORBIT_CONTROL_DB_URL`, gorm over pgx, goose migrations) and driven
through Temporal (`TEMPORAL_ADDRESS`). Without a database it keeps state in memory (dev and tests only).

This repo does **not** run workflows, execute agents, host a sandbox or render the UI. Those live in
[orbit-runtime](https://github.com/mindreon/orbit-runtime) (orch + worker) and
[orbit-web](https://github.com/mindreon/orbit-web).

## Layout

```
cmd/orbit-control/     process entrypoint (public + internal listeners)
internal/httpapi/      mux: /v1/tasks, profiles, policy, SOPs, artifacts, catalog; /internal/* for the worker
internal/app/          the task service wiring and the catalog (assistants, connectors, skills, MCP market)
internal/task/         task service, Projector (runtime_outbox → task projection), SSE fan-out, ownership ring
internal/orch/         Temporal client (TaskWorkflow start, query, update, signal)
internal/store/        Repository interface; memstore, pgstore (gorm), goose migrations
internal/contract/v3/  contract v3 types generated from orbit-runtime (scripts/sync-contracts.sh)
deploy/postgres/       one-time role + database bootstrap (superuser)
docs/openapi.yaml      the public HTTP contract
```

## Run

Requires Go 1.23+.

```bash
go test ./...
go run ./cmd/orbit-control
curl -s http://127.0.0.1:8080/health   # {"status":"ok"}
```

The public listener binds `127.0.0.1:$PORT` (default `8080`); `ORBIT_PUBLIC_ADDR` overrides the full address. Routes
for the worker (`/internal/*`) are served only on `ORBIT_INTERNAL_ADDR` (default `127.0.0.1:8081`); the public listener
answers them with `404`. Do not publish the internal port.

Set `ORBIT_LOG_FORMAT=json` for JSON logs.

### Postgres

```bash
# Passwords may be SCRAM verifiers, so plaintext never reaches the server:
#   app_v=$(printf '%s\n' "$APP_PASSWORD" | python3 deploy/postgres/scram-verifier.py)
psql -v ON_ERROR_STOP=1 -v owner_password=... -v app_password="$app_v" -v ops_password=... -v worker_password=... \
  -f deploy/postgres/bootstrap-roles.sql "$SUPERUSER_URL"
psql -v ON_ERROR_STOP=1 -v tenant_id=default \
  -f deploy/postgres/ensure-tenant.sql "$ORBIT_CONTROL_OPS_DB_URL"   # tenants are created by orbit_ops only
ORBIT_CONTROL_DB_URL=postgres://orbit_app:...@127.0.0.1:5432/orbit_control \
ORBIT_CONTROL_MIGRATE_DB_URL=postgres://orbit_owner:...@127.0.0.1:5432/orbit_control \
ORBIT_CONTROL_MIGRATE_ON_START=1 go run ./cmd/orbit-control
```

Control connects as `orbit_app` (not an owner, no BYPASSRLS). Every tenant statement runs in a transaction that first
sets `app.tenant_id`; row-level security is the backstop. Migrations run as `orbit_owner` only.

### Temporal

With `TEMPORAL_ADDRESS` set (e.g. `127.0.0.1:7233`), control starts TaskWorkflow on the `orbit.orch` queue
(`TEMPORAL_TASK_QUEUE` overrides it). orbit-runtime runs the workflow and activity workers.

## Task events (SSE)

`GET /v1/tasks/{taskId}/events` streams a task's events. Durable events carry `id: <seq>`, a per-task counter; live
frames (model deltas) carry no id. Reconnect with `Last-Event-ID` to resume from the durable log; a cursor that cannot
be resumed gets `409 EVENT_CURSOR_EXPIRED`. Durable events reach control only through `runtime_outbox`, written by the
worker and projected by the Projector; `POST /internal/events` accepts live frames only.

## Deploy gate: no external exposure before user auth (§17)

Control has **no user authentication yet**. Every `/v1` path is open to anyone who can reach it, and CORS is `*`.
This service must not be deployed to an externally reachable environment until the auth layer lands.

The gate is enforced at startup: `orbit-control` **refuses to start** when the public listener's address is not
loopback. For an environment that is not externally reachable (e.g. a local Compose network) set
`ORBIT_PUBLIC_ADDR=:8080` **and** `ORBIT_ALLOW_UNAUTHENTICATED_BIND=1`; control logs a warning. Production
configuration must never set `ORBIT_ALLOW_UNAUTHENTICATED_BIND`.

`ORBIT_INGEST_MAX_BYTES` (default 1 MiB) caps one `POST /internal/events` body; larger bodies get `413`.

## Persistence E2E (contract §18)

The suite lives in `e2e/persistence` (build tag `e2e`). It drives the HTTP API against a real Postgres and writes
`artifacts/e2e-persistence-report.json`, which CI uploads. Isolated SQL, role and static checks are limited to those
listed in [docs/persistence-failure-modes.md](docs/persistence-failure-modes.md).

```bash
docker compose -f e2e/compose.yaml up -d --wait
ORBIT_TEST_DB_URL=postgres://orbit_app:e2e-app@127.0.0.1:55432/orbit_control?sslmode=disable \
ORBIT_TEST_MIGRATE_DB_URL=postgres://orbit_owner:e2e-owner@127.0.0.1:55432/orbit_control?sslmode=disable \
ORBIT_TEST_OPS_DB_URL=postgres://orbit_ops:e2e-ops@127.0.0.1:55432/orbit_control?sslmode=disable \
ORBIT_TEST_WORKER_DB_URL=postgres://orbit_worker:e2e-worker@127.0.0.1:55432/orbit_control?sslmode=disable \
go test -tags e2e -count=1 ./e2e/...
```

The full stack (Temporal, control, orbit-runtime, MinIO, orbit-web) is exercised by orbit-web's `pnpm test:stack`.

## CI credentials

The `env:` of `.github/workflows/ci.yml` may contain **only one-time credentials** that exist solely for the throwaway
Postgres service container of that job (`ci-superuser`, `ci-owner`, `ci-app`, `ci-ops`). Never put a real, shared or
reusable value there. Such values belong in GitHub secrets and must not be needed by this job at all.

## Container image

Pushes to `main` publish `ghcr.io/mindreon/orbit-control` (`main`, `latest`, short SHA). `orbit-infra` pulls it by
`ORBIT_IMAGE_TAG`.

## Non-goals

- Real OAuth / session auth (not yet)
- LLM calls or an agent runtime in this process
- Running Temporal workers here (orbit-runtime hosts them)

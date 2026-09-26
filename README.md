# orbit-control

Orbit **control plane**. This repository is the **sole public HTTP and WebSocket API** for Orbit.

W1 status: rooms, messages, HITL approvals, steer, bounded execution history,
and SSE events are implemented in-memory. A Room pins a dsh permission preset
and exposes its runtime snapshot. Session lifecycle talks to orbit-worker over
HTTP by default, or via Temporal `RoomWorkflow` when `TEMPORAL_ADDRESS` is set.
No database, no OAuth. Other resource groups still return empty lists.

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

The public listener binds `127.0.0.1:$PORT` (default port `8080`);
`ORBIT_PUBLIC_ADDR` overrides the full address. Worker → control routes
(`/internal/*`, e.g. `POST /internal/events`) are served only on a separate
internal listener, `ORBIT_INTERNAL_ADDR` (default `127.0.0.1:8081`); the public
listener answers `/internal/*` with `404`. Point the worker's
`ORBIT_EVENT_INGEST_URL` at the internal address (in Compose, set
`ORBIT_INTERNAL_ADDR=:8081` on control and use `http://control:8081/internal/events`),
and do not publish that port.

## Deploy gate: no external exposure before user auth (§17)

Control has **no user authentication or authorization yet**. Every `/v1`
path is open to anyone who can reach it: `GET /v1/rooms` lists every room id,
`authorizeRoomStream` allows every caller, SSE replay returns any room's
history, and CORS is `*`.

- **(a)** This service must **not** be deployed to any externally reachable
  environment until the §17 auth PR has merged.
- **(b)** The §17 auth PR must add **E-LE-5** to the real-stack E2E: an SSE
  reconnect with `Last-Event-ID` and no session gets `401`, one with another
  tenant's session gets `403`/`404`, and in both cases zero events are
  replayed (no `text/event-stream` response is opened).

The gate is enforced at startup: `orbit-control` **refuses to start** when the
public listener's address is not loopback (`127.0.0.1`, `::1`, `localhost`).
The container image therefore does not serve outside itself by default. For an
environment that is not externally reachable (e.g. a local Compose network),
set `ORBIT_PUBLIC_ADDR=:8080` **and** `ORBIT_ALLOW_UNAUTHENTICATED_BIND=1`;
control logs a warning at startup. Production configuration must never set
`ORBIT_ALLOW_UNAUTHENTICATED_BIND`; the §17 auth PR replaces this switch with
a real auth check.

## Resource limits

| Variable | Default | Effect |
| --- | --- | --- |
| `ORBIT_SSE_MAX_STREAMS_PER_ROOM` | `32` | Further SSE connections to that room get `429 STREAM_LIMIT_ROOM` before any stream opens. |
| `ORBIT_SSE_MAX_STREAMS_PER_CLIENT` | `16` | Per peer IP (proxy headers are not trusted); over it, `429 STREAM_LIMIT_CLIENT`. |
| `ORBIT_SSE_WRITE_TIMEOUT` | `10s` | Deadline for each SSE write; a stalled reader's stream is closed. |
| `ORBIT_SSE_MAX_CONSECUTIVE_LAGS` | `3` | A reader that overflows its live buffer this many times in a row gets `reset` (`lagging`) and the stream closes. |
| `ORBIT_INGEST_MAX_BYTES` | `1048576` | Larger `POST /internal/events` bodies get `413 PAYLOAD_TOO_LARGE`; nothing is stored. |
| `ORBIT_CLOSED_ROOM_LOG_TTL` | `15m` | A closed room's event log (activity and replay) is freed this long after it closes; `0` frees it at once. |

When a room closes (abort, or a reject that closes it), its open SSE streams
deliver what is buffered and end.

## Room events (SSE) and resume

`GET /v1/rooms/{roomId}/events` streams the room's events; `GET
/v1/rooms/{roomId}/activity` lists the retained ones. Both use one shape, an
`EventEnvelope`: `{id, type, taskId, ts, source, payload}`. Worker events
(`/internal/events`, orbit-runtime A1 `OrbitEvent`, all 15 types) are passed
through unchanged as `payload`, so camelCase fields like `delta`, `blockId`,
`argsPreview`, `toolState`, `truncated`, `agentPath`, and `failure` reach
clients; unknown types are rejected with 400.

Every SSE message has `id: <id>`: the event's id from one process-global
counter in the in-memory event store (`app.EventLog`), shared by all rooms.
Within a room ids strictly increase but are not contiguous. The counter
restarts with control.

To resume after a disconnect, send the last id you received as the
`Last-Event-ID` header (EventSource does this on auto-reconnect) or as
`?lastEventId=` (the header wins if both are set). Control replays this room's
retained events after that id, in order, then continues live with no gaps or
duplicates. Without a cursor the stream is live-only.

A cursor that is malformed, is not an event of this room (including any id
issued before a control restart), or is older than the retained window (last
500 events per room) gets a `reset` envelope instead
(`{"type":"reset",…,"payload":{"reason":"malformed|unknown|expired","lastId":N}}`):
refetch the room, messages, and activity, then keep reading.

`assistant.delta` drafts are live-only: never stored, never replayed, and
never counted against the 500-event window, so they cannot evict tool or
approval records.
Heartbeat comments are sent about every 15s. See
[docs/openapi.yaml](./docs/openapi.yaml) and
[docs/contract-notes/sse-resume.md](./docs/contract-notes/sse-resume.md).

P0 supports a single control instance only: the replay-to-live handoff uses
this process's in-memory fan-out. For multiple replicas, live fan-out moves to
Postgres LISTEN/NOTIFY or NATS, while replay logic stays unchanged.

```bash
curl -N -H 'Last-Event-ID: 42' \
  http://127.0.0.1:8080/v1/rooms/rm_0123456789abcdef/events
```

### End-to-end checks

QA sign-off (E-LE-1 to E-LE-4 and E-LE-6; E-LE-5 comes with the §17 auth PR)
runs against the real stack: a Temporal dev
server, `orbit-orch` and `orbit-worker` as containers of the
[orbit-runtime](https://github.com/mindreon/orbit-runtime) image published for
a pinned runtime commit (run by digest), and two `orbit-control` processes
built from this tree. It writes `artifacts/e2e-real-stack.json` (control
commit, component versions including the runtime image digest, and per case
id, steps, expected, actual, pass; no timestamps, ports, or random ids). The
`e2e` workflow runs it twice on the PR head, requires byte-identical reports,
scans the reports and all process logs for secrets, and uploads both.
Requires Docker.

```bash
# runtime commit pinned as ORBIT_RUNTIME_REF in .github/workflows/e2e.yml
go run ./e2e/realstack run -runtime-commit f8addbccc8f3ffc360ddccf3717f4d094c39f7ee
go run ./e2e/realstack scan artifacts/e2e-real-stack.json e2e-logs/*.log
```

What each case covers, and which failure modes have no automated test, is in
[docs/testing/last-event-id-failure-modes.md](./docs/testing/last-event-id-failure-modes.md).

## Non-goals (W1)

- Real OAuth / session auth
- Database migrations or persisted data
- LLM calls or dsh in this process (those live on orbit-worker)
- Running Temporal workers here (orch hosts workflows; worker hosts activities)
- Decrypting or storing tenant secrets outside this service (and not even here yet)

## Temporal (optional)

When `TEMPORAL_ADDRESS` is set (e.g. `127.0.0.1:7233`), control starts
`RoomWorkflow` on task queue `orbit` (override with `TEMPORAL_TASK_QUEUE`)
instead of calling worker HTTP for session lifecycle. orbit-orch must run a
workflow worker and orbit-worker must run an activity worker on the same queue.

Without Temporal, control keeps the W1 direct-HTTP path to orbit-worker.


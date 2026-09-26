# Contract note: event envelope and SSE resume via `Last-Event-ID`

Addendum to Orbit contract v2.1 §16.3 ("依赖：control 的 `/events` SSE 必须支持
`Last-Event-ID` 续传"). Worker event payloads are exactly v2.1 §2.1 /
orbit-runtime A1 `schema/OrbitEvent.json`; this note fixes how control wraps,
stores, streams, and replays them.

## Envelope

`/v1/rooms/{roomId}/activity` items and every SSE `data:` are one shape:

```json
{"id":42,"type":"tool.result","taskId":"rm_0123456789abcdef","ts":"2026-09-26T14:00:00.123456+00:00","source":"worker","payload":{…}}
```

| Field | Meaning |
| --- | --- |
| `id` | Global event id; also the SSE `id:`. Absent on live-only envelopes (`assistant.delta`, `reset`). |
| `type` | Event type (same as `payload.type` for worker events). |
| `taskId` | Room id (room = task, C29). |
| `ts` | `payload.occurredAt` when present, else control's receive time. |
| `source` | `worker` (ingested) or `control` (control-originated). |
| `payload` | Worker: the ingested body, unchanged (whitespace compacted). Control: its own fields plus `roomId`, `occurredAt`, `runtime`, `permissionPreset`. Reset: `{reason, lastId}`. |

**BREAKING for orbit-web.** The former flat `ActivityEvent` (`sequence`,
`text`, `toolName`, `role`, `status`, `reason` at top level) is gone; read
`id` and `payload.*` instead.

## Event passthrough (A1 compat)

- `/internal/events` accepts every `OrbitEvent.type` in the A1 schema:
  `session.status`, `assistant.message`, `assistant.delta`, `tool.call`,
  `tool.result`, `approval.asked`, `approval.resolved`, `question.asked`,
  `question.answered`, `todo.updated`, `usage`, `agent.started`,
  `agent.finished`, `agent.spawn_rejected`, `turn.failed`. Anything else is
  `400`, as before.
- Control validates only routing fields: a JSON object, a known `type`,
  string `roomId`/`sessionId` that map to a known room, and RFC 3339
  `occurredAt` when present. The body is then kept unchanged as `payload`, so
  fields control does not interpret (`delta`, `blockId`, `seq`,
  `activityAttempt`, `argsPreview`, `toolState`, `truncated`, `agentId`,
  `agentPath`, `turnId`, `failure`, usage counters, and any future field)
  reach clients. SSE frames are not HTML-escaped, so payload bytes match what
  the worker sent.
- The wire form is camelCase. orbit-runtime emits it since PR #4 head
  `6e8e3ef` (`model_dump(by_alias=True, exclude_none=True)`); at `2c56c2a` the
  worker still dumped snake_case field names.

## Stream

`GET /v1/rooms/{roomId}/events` — `Content-Type: text/event-stream`,
`Cache-Control: no-cache`, `X-Accel-Buffering: no`. Comment lines
`: connected` (on open) and `: heartbeat` (about every 15s) carry no id.
Every message is a default SSE `message` event; clients switch on
`data.type`.

| `data.type` | Stored | Replayed | Envelope `id` |
| --- | --- | --- | --- |
| everything except below | yes | yes | yes |
| `assistant.delta` | no | never | none |
| `reset` | no | no | none |

## Ids

- `id: <n>` on every message, where `n` is `EventEnvelope.id`: the event's id
  from **one process-global monotonic counter** in control's in-memory event
  store, shared by all rooms. There are no per-room counters or per-room
  locks; reads filter by room.
- Within a room ids strictly increase; they are not contiguous (other rooms'
  events take the ids in between).
- The store sits behind `app.EventLog` (`MemoryEventLog` in W1). From §18 on,
  ids are a per-task increasing `seq`; ids are not comparable across tasks.
- **Traffic exposure (W1 only).** Because the counter is shared, the gaps
  between one room's ids reveal how many events all other rooms produced in
  between. This goes away with the per-task `seq` of §18.
- **Restart.** The counter lives only in the control process. After a restart
  every previously issued id is `unknown` and yields `reset`. Rooms are in
  memory too, so today the room itself is usually gone after a restart and the
  stream answers `404` before opening; either way the client refetches.
- `assistant.delta` and `reset` frames repeat the id of the last durable event
  of this room the connection has delivered (for `reset`, the room's latest
  event id), so a browser's `lastEventId` always names an event of this room.

## Resume

1. Cursor: `Last-Event-ID` header, else `lastEventId` query parameter. The
   header wins because EventSource auto-reconnect updates the header but not
   the URL. An empty value means no cursor: live-only, same as before.
2. The cursor must be `0` (before the room's first event) or a retained event
   id of the requested room. An id that belongs to another room is `unknown`,
   so task A's id can never select task B's events.
3. Handoff: subscribe to the live buffer, read this room's retained events with
   `id > cursor`, write them in order, then drain the buffer skipping
   `id <= last written`. If the live buffer ever overflows, the stream re-reads
   history from its cursor before delivering the next buffered frame, so there
   are no gaps.

**Deployment constraint.** P0 supports a single control instance only; for
multiple replicas, live fan-out moves to Postgres LISTEN/NOTIFY or NATS, while
replay logic stays unchanged.

**Store requirement.** An `EventLog` must make events visible in id order;
replay reads `id > cursor` and would skip an id that became visible late. W1
allocates, stores, and fans out under one lock, so this holds.

## `reset`

Sent as the first message when the cursor cannot be honoured:

```json
{"type":"reset","taskId":"rm_0123456789abcdef","ts":"2026-09-26T14:00:00Z","source":"control","payload":{"reason":"expired","lastId":812}}
```

| `payload.reason` | When |
| --- | --- |
| `malformed` | Not a canonical decimal id (e.g. `abc`, `042`, `-1`, `rm_x:5`). |
| `unknown` | Not an event of this room: another room's id (whatever its age), beyond the latest issued id, or issued before a control restart. |
| `expired` | One of this room's own events that has been evicted from the retained window (activity keeps the last 500 per room). |
| `lagging` | The reader overflowed its live buffer `ORBIT_SSE_MAX_CONSECUTIVE_LAGS` times in a row; the stream closes after this frame. |

`payload.lastId` is this room's latest event id (0 if none) and the frame's
SSE `id:`. Client action: drop local room state, refetch
`GET /v1/rooms/{roomId}`, `/messages`, and `/activity`, then keep reading the
same connection (for `lagging`, reconnect with `Last-Event-ID: lastId`). It
continues live after `lastId`; dedupe the refetch against the stream by `id`.

## When a stream ends or is refused

- `429` `STREAM_LIMIT_ROOM` / `STREAM_LIMIT_CLIENT` before any stream opens,
  when the room or the client (peer IP) is at its stream cap.
- The room closes (abort, or a reject that closes it): buffered frames are
  delivered, then the stream ends. A stream opened on a closed room replays
  and ends. `ORBIT_CLOSED_ROOM_LOG_TTL` after the close, the room's event log
  is freed: `/activity` returns no items, and a resume with any earlier id
  gets `reset` `unknown` with `lastId` 0, after which the stream ends.
- A write stalls longer than `ORBIT_SSE_WRITE_TIMEOUT`: the stream is closed.
- `reset` `lagging`, above.
- An event store error: control sends `retry: 10000` (reconnect in 10 s)
  and closes, instead of dropping the connection for an immediate reconnect
  with the same id.

## `assistant.delta`

Live-only, as in v2.1 §2.1: not written to activity or audit, takes no id and
no slot in the 500-event window (so it cannot evict tool or approval
records), and is never replayed. After a reconnect the client clears partial
drafts; the final text arrives as a durable `assistant.message`. A delta that
was buffered behind durable events already delivered by replay is dropped
rather than shown out of order, and deltas dropped while a stream catches up
after a buffer overflow are gone too. **Neither drop is signalled**: clients
must treat deltas as best-effort and the persisted `assistant.message` as the
authoritative text of a turn.

## Auth (§17)

Not in this change, so replay has **no authorization** today: any caller can
list room ids (`GET /v1/rooms`) and replay any room, and CORS is `*`. Until
the §17 auth PR merges this endpoint must not be externally reachable;
control refuses to start on a non-loopback public bind (README "Deploy
gate"). `authorizeRoomStream` in `internal/httpapi/events.go` is where
authentication (401) and authorization (403/404) will run, before the room
lookup, before any `text/event-stream` header, and before replay. The auth PR
must add E-LE-5: reconnect without a session → 401; another tenant's session
→ 403/404; zero events replayed in both. CORS is unchanged here. The browser `EventSource` API cannot set headers on a new
connection, so a client that recreates its `EventSource` passes the cursor as
`?lastEventId=`.

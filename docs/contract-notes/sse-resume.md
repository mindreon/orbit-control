# Contract note: SSE resume via `Last-Event-ID`

Addendum to Orbit contract v2.1 §16.3 ("依赖：control 的 `/events` SSE 必须支持
`Last-Event-ID` 续传"). Event payload shapes are unchanged from v2.1 §2.1; this
note only fixes the stream framing, ids, and the `reset` frame.

## Stream

`GET /v1/rooms/{roomId}/events` — `Content-Type: text/event-stream`,
`Cache-Control: no-cache`, `X-Accel-Buffering: no`. Comment lines
`: connected` (on open) and `: heartbeat` (about every 15s) carry no id.

Every message is a default SSE `message` event. Clients switch on `data.type`:

| `data.type` | Schema | Durable | Replayed |
| --- | --- | --- | --- |
| any `ActivityEvent.type` | `ActivityEvent` | yes | yes |
| `assistant.delta` | `AssistantDeltaEvent` | no | never |
| `reset` | `StreamReset` | no | no |

## Ids

- `id: <sequence>` on every message, where `sequence` is
  `ActivityEvent.sequence`: the event's id from **one global sequence** shared
  by all rooms. There are no per-room counters or per-room locks.
- Global ids are monotonic, so within a room they strictly increase; they are
  not contiguous (other rooms' events take the ids in between).
- Durable table (target): `docs/schema/room_events.sql` —
  `id bigint GENERATED ALWAYS AS IDENTITY`, `task_id`, and an index on
  `(task_id, id)` for filtered replay. W1 control has no database driver; its
  file store keeps the same shape (a global id recovered from the audit logs
  at startup, and each room's events ordered by id).
- `assistant.delta` and `reset` repeat the id of the last durable event of
  this room the connection has delivered (for `reset`, the room's latest event
  id), so a browser's `lastEventId` always names an event of this room.

## Resume

1. Cursor: `Last-Event-ID` header, else `lastEventId` query parameter. The
   header wins because EventSource auto-reconnect updates the header but not
   the URL. An empty value means no cursor: live-only, same as before.
2. The cursor must be `0` (before the room's first event) or the id of an event
   of the requested room (`WHERE task_id = $room AND id = $cursor`). A valid
   global id that belongs to another room is `unknown`, so task A's id can
   never select task B's events.
3. Handoff: subscribe to the live buffer, read this room's retained history
   with `id > cursor` (`WHERE task_id = $room AND id > $cursor ORDER BY id`),
   write it in order, then drain the buffer skipping `id <= last written`. If
   the live buffer ever overflows, the stream re-reads history from its cursor
   before delivering the next buffered frame, so there are no gaps.

**Deployment constraint.** P0 supports a single control instance only; for
multiple replicas, live fan-out moves to Postgres LISTEN/NOTIFY or NATS, while
replay logic stays unchanged.

**Postgres caveat.** Identity ids are allocated at insert but become visible
at commit, so concurrent writers can commit id N+1 before N. When events move
to Postgres, inserts must be serialized (or replay must stop below the oldest
in-flight id); otherwise `id > cursor` can skip an event. W1 allocates,
persists, and fans out under one lock, so ids are visible in order.

## `reset`

Sent as the first message when the cursor cannot be honoured:

```json
{"type":"reset","roomId":"rm_0123456789abcdef","reason":"expired","sequence":812,"occurredAt":"2026-09-26T14:00:00Z"}
```

| `reason` | When |
| --- | --- |
| `malformed` | Not a canonical decimal id (e.g. `abc`, `042`, `-1`, `rm_x:5`). |
| `unknown` | Not an event of this room (e.g. another room's id), or beyond the global head. |
| `expired` | An event of this room older than the earliest retained event (activity keeps the last 500 per room). |

`sequence` is this room's latest event id (0 if none). Client action: drop
local room state, refetch `GET /v1/rooms/{roomId}`, `/messages`, and
`/activity`, then keep reading the same connection. It continues live after
`sequence`; dedupe the refetch against the stream by `sequence`.

## `assistant.delta`

Live-only, as in v2.1 §2.1: not written to activity or audit, so it has no
`sequence` and is never replayed. After a reconnect the client clears partial
drafts; the final text arrives as a durable `assistant.message`. A delta that
was buffered behind durable events already delivered by replay is dropped
rather than shown out of order.

## Auth (§17)

Not in this change. `authorizeRoomStream` in `internal/httpapi/events.go` is
where authentication (401) and authorization (404) will run, before the room
lookup, before any `text/event-stream` header, and before replay. CORS is
unchanged. The browser `EventSource` API cannot set headers on a new
connection, so a client that recreates its `EventSource` passes the cursor as
`?lastEventId=`.

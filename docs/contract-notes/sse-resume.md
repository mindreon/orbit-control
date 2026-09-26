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

- `id: <roomId>:<sequence>` on every message.
- `sequence` is `ActivityEvent.sequence`: per room, strictly increasing by 1
  per durable event, persisted with each audit line and recovered from the
  audit log when control first touches the room.
- The room prefix scopes the id to one task. An id from task A sent to task B
  is `unknown` and yields a `reset`; it is never read as a B sequence.
- `assistant.delta` and `reset` repeat the id of the last durable event the
  connection has delivered (for `reset`, the room head), so a browser's
  `lastEventId` always names a durable event.

## Resume

1. Cursor: `Last-Event-ID` header, else `lastEventId` query parameter. The
   header wins because EventSource auto-reconnect updates the header but not
   the URL. An empty value means no cursor: live-only, same as before.
2. Control subscribes to the live buffer, then reads retained history with
   `sequence > cursor`, writes it in order, then drains the buffer skipping
   `sequence <= last written`. If the live buffer ever overflows, the stream
   re-reads history from its cursor, so there are no gaps.
3. Only the requested room's events are read; nothing crosses rooms.

## `reset`

Sent as the first message when the cursor cannot be honoured:

```json
{"type":"reset","roomId":"rm_0123456789abcdef","reason":"expired","sequence":812,"occurredAt":"2026-09-26T14:00:00Z"}
```

| `reason` | When |
| --- | --- |
| `malformed` | Not exactly `<roomId>:<canonical decimal>` (e.g. `42`, `rm_x:abc`, `rm_x:042`). |
| `unknown` | Well-formed but another room's id, or `sequence` beyond this room's head. |
| `expired` | Older than the earliest retained event (activity keeps the last 500 per room). |

Client action: drop local room state, refetch `GET /v1/rooms/{roomId}`,
`/messages`, and `/activity`, then keep reading the same connection. It
continues live after `sequence`; dedupe the refetch against the stream by
`sequence`.

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

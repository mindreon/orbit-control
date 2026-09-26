# Last-Event-ID: failure modes and how each is verified

This PR has no isolation (unit) tests for Last-Event-ID; this list is
documentation only. Verification is the real-stack E2E, `go run
./e2e/realstack run`:

- a Temporal dev server (Temporal CLI pinned in the runner);
- `orbit-orch` and `orbit-worker` as containers of the orbit-runtime image
  published for the commit pinned in `.github/workflows/e2e.yml`, run by
  digest, one pair with the streaming mock model and one in `real` mode
  against an OpenAI-compatible stub;
- two `orbit-control` processes built from the tree under test.

Rooms are driven only through control's public API. The report,
`artifacts/e2e-real-stack.json`, is organized by case id (E-LE-1..E-LE-4, E-LE-6; E-LE-5 belongs to the §17 auth PR)
(steps, expected, actual, pass) and names the control head SHA and every
component version, including the runtime image digest. CI runs it twice on
the PR head, `cmp`s the reports, secret-scans the reports and all process
logs (the runner's `scan` and pinned gitleaks), and uploads reports and logs.

| # | Failure mode | Verified by |
| --- | --- | --- |
| F1 | Replay returns events of another task, or events with id <= cursor | E-LE-1 (assembled ids equal the room's `/activity` after the cursor) |
| F2 | Cursor from task A selects task B's events | E-LE-1 (`resumeWithUnusableIds.otherRoomsLastId`: `reset` / `unknown`) |
| F3 | Duplicate or missing event after a mid-stream disconnect and resume | E-LE-1 (drops on a turn's `tool.call` and first `assistant.delta`, resumes with Last-Event-ID) |
| F4 | An event published after subscribe but before the history read is delivered twice | No automated test. The endpoint has no way to pause between subscribe and history read. |
| F5 | An event published during replay (after the history snapshot) is lost | No automated test, for the same reason. |
| F6 | A message without `id:`, or an SSE id that differs from `data.id` | E-LE-1 (`messagesWithoutId`, `liveOnlyIdNotLastStored`) |
| F7 | Malformed / unknown cursor silently skipped instead of `reset` | E-LE-1 (`resumeWithUnusableIds`: `abc`, another room's id, `999999999`) |
| F8 | Cursor older than the retained window silently skipped (`expired`) | No automated test (needs more than 500 stored events in one room). |
| F9 | `assistant.delta` replayed, stored, or evicting durable records | E-LE-1 (deltas never enter the assembled sequence); E-LE-3 (700 streamed parts in one turn) |
| F10 | Header vs query cursor precedence wrong; query fallback ignored | No automated test. |
| F11 | Pre-restart id accepted after control restart | No automated test. |
| F12 | Missing SSE headers or heartbeat | E-LE-1 (`sseHeaders`). Heartbeat: no automated test. |
| F13 | Live buffer overflow drops frames silently | No automated test (needs a slow reader on a full 256-slot buffer). Verified manually with a forced 2-slot buffer (see PR). |
| F14 | A worker event field is dropped or re-shaped on the way to SSE (`assistant.delta`, `usage`, `turn.failed`) | E-LE-2 (every SSE payload byte-compared with the body the worker posted; A1 fields checked) |
| F15 | Unknown event type accepted by ingest | E-LE-2 (400 on both controls' internal listeners) |
| F16 | `/internal/*` reachable on the public listener | E-LE-4 (404 for POST/GET `/internal/events` and any `/internal/*` path; activity unchanged) |
| F17 | Unbounded SSE subscriptions per room or per client | E-LE-6 (third stream on a room and fifth from one client get 429 before any stream opens; a closed stream frees its slot) |
| F18 | Oversized ingest body stored or streamed | E-LE-6 (413 `PAYLOAD_TOO_LARGE`, activity unchanged, nothing on the open stream; a body just under the limit is stored) |
| F19 | Streams stay open after their room closes | E-LE-6 (abort ends the open stream after a `session.status`; a new stream on the closed room ends at once) |
| F20 | A stalled reader pins a handler (no write deadline) | No automated test. |
| F21 | A reader that keeps overflowing is caught up forever | No automated test (the `lagging` reset after `ORBIT_SSE_MAX_CONSECUTIVE_LAGS`). |
| F22 | Closed rooms' event logs are never freed | No automated test (`ORBIT_CLOSED_ROOM_LOG_TTL`, default 15 min). |
| F23 | Another room's id older than this room's eviction point reported as `expired` | No automated test (needs 500+ events in the room plus an older foreign id); E-LE-1 covers a recent foreign id. |
| F24 | Event store error makes clients reconnect at once with the same id | No automated test (`retry: 10000` before close). |
| F25 | Unauthenticated or cross-tenant replay | **Open until §17.** The auth PR must add E-LE-5 (no session → 401, other tenant → 403/404, zero events replayed). Until then control refuses a non-loopback public bind unless `ORBIT_ALLOW_UNAUTHENTICATED_BIND=1` (verified by hand; no automated test). |

E-LE-3 adds 110 ms to the recording proxy's forwarding of that room's
`assistant.delta` posts. The mock model streams instantly, and AgentScope
limits one message to about 500 size-flushed deltas (32k-token context, 4
bytes per token), so the slow ingest is what makes the worker's 100 ms
coalescing flush every part. Payloads are forwarded unchanged.

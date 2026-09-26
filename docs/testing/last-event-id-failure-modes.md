# Last-Event-ID: failure modes and how each is verified

Verification is end-to-end first. Two suites drive the real `orbit-control`
binary only over HTTP; CI (`.github/workflows/e2e.yml`) runs both on the PR
head and uploads their reports.

- **Real stack (QA sign-off E-LE-1..4)** — `go run ./e2e/realstack run`:
  Temporal dev server, `orbit-orch` and `orbit-worker` from orbit-runtime
  (mock and real model modes), two `orbit-control` processes. Writes
  `artifacts/e2e-real-stack.json` (commit, component versions, per case id,
  steps, expected, actual, pass). CI runs it twice and requires identical
  reports, then secret-scans them (`realstack scan` and gitleaks).
- **Stub worker (SSE contract)** — `go run ./e2e/lasteventid`: resets,
  restart, headers, heartbeat, concurrent live switch. Writes
  `artifacts/e2e-last-event-id.json` (`case`, `expected`, `actual`, `pass`).

A failure mode is isolation-tested only when the real endpoint cannot force it.

| # | Failure mode | Verified by |
| --- | --- | --- |
| F1 | Replay returns events of another task, or events with id <= cursor | Real stack E-LE-1; stub `replay_after_id_same_task`, `no_cross_task_reads` |
| F2 | Cursor from task A selects task B's events | Stub `reset_unknown_other_task` |
| F3 | Duplicate or missing event after a mid-stream disconnect and resume | Real stack E-LE-1 (disconnect on a turn's tool.call / first assistant.delta, resume with Last-Event-ID); stub `live_switch_no_dup_no_gap` (concurrent publisher) |
| F4 | An event published after subscribe but before the history read is delivered twice | Isolation: `TestEventsResumeReplaysThenGoesLiveWithoutGapsOrDuplicates` (`afterSubscribe` hook). The real endpoint has no way to pause between subscribe and history read, so E2E can hit this window only by chance. |
| F5 | An event published during replay (after the history snapshot) is lost | Isolation: same test (`afterReplayFrame` hook), for the same reason. Stub F3 exercises it probabilistically. |
| F6 | A message without `id:`, or an SSE id that differs from `data.id` | Real stack E-LE-1; stub `id_on_every_message` |
| F7 | Malformed / unknown / evicted cursor silently skipped instead of `reset` | Stub `reset_*` cases |
| F8 | Stream does not continue live after a reset | Stub `reset_*` cases (next published event arrives) |
| F9 | `assistant.delta` replayed, stored, or evicting durable records | Real stack E-LE-3 (700 streamed parts in one turn); stub `delta_not_replayed` |
| F10 | Header vs query cursor precedence wrong; query fallback ignored | Stub `query_fallback`, `header_wins_over_query` |
| F11 | Pre-restart id accepted after control restart | Stub `restart_forgets_ids` (restarts the real process). Isolation: `TestEventIDsAreGlobalAndUnknownAfterRestart`, because rooms do not survive a restart, so only an in-process test can keep the room while resetting the counter. |
| F12 | Missing SSE headers or heartbeat; history replayed without a cursor | Stub `sse_headers_heartbeat_live_only` |
| F13 | Live buffer overflow drops frames silently | Not reproducible through the endpoint (needs a slow reader on a full 256-slot buffer). Covered by the lag check before every buffered frame; verified manually with a forced 2-slot buffer (see PR). No automated test. |
| F14 | A worker event field is dropped or re-shaped on the way to SSE (`assistant.delta`, `usage`, `turn.failed`) | Real stack E-LE-2: every SSE payload is byte-compared with the body the worker posted (recording proxy), and the A1 fields are checked |
| F15 | Unknown event type accepted by ingest | Real stack E-LE-2 (400 on both controls' internal listeners) |
| F16 | `/internal/*` reachable on the public listener | Real stack E-LE-4 (404 for POST/GET `/internal/events` and any `/internal/*` path; activity unchanged) |

The real-stack E-LE-3 turn adds 110 ms to the recording proxy's forwarding of
that room's `assistant.delta` posts. The mock model streams instantly, and
AgentScope limits one message to about 500 size-flushed deltas (32k-token
context, 4 bytes per token), so the slow ingest is what makes the worker's
100 ms coalescing flush every part. Payloads are forwarded unchanged.

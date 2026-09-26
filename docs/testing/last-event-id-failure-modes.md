# Last-Event-ID: failure modes and how each is verified

Verification is end-to-end first: `go run ./e2e/lasteventid` builds and starts
the real `orbit-control` binary, drives it only over HTTP (`/v1/rooms`,
`/internal/events`, `/v1/rooms/{id}/events`, `/activity`), and writes
`artifacts/e2e-last-event-id.json` (`case`, `expected`, `actual`, `pass`). CI
uploads that file (`.github/workflows/e2e.yml`).

A failure mode is isolation-tested only when the real endpoint cannot force it.

| # | Failure mode | Verified by |
| --- | --- | --- |
| F1 | Replay returns events of another task, or events with id <= cursor | E2E `replay_after_id_same_task`, `no_cross_task_reads` |
| F2 | Cursor from task A selects task B's events | E2E `reset_unknown_other_task` |
| F3 | Duplicate or missing event at the replay→live switch under load | E2E `live_switch_no_dup_no_gap` (concurrent publisher, staggered resumes) |
| F4 | An event published after subscribe but before the history read is delivered twice | Isolation: `TestEventsResumeReplaysThenGoesLiveWithoutGapsOrDuplicates` (`afterSubscribe` hook). The real endpoint has no way to pause between subscribe and history read, so E2E can hit this window only by chance. |
| F5 | An event published during replay (after the history snapshot) is lost | Isolation: same test (`afterReplayFrame` hook), for the same reason. E2E F3 exercises it probabilistically. |
| F6 | A message without `id:`, or an SSE id that differs from `data.id` | E2E `id_on_every_message` (every message of every E2E stream) |
| F7 | Malformed / unknown / evicted cursor silently skipped instead of `reset` | E2E `reset_*` cases |
| F8 | Stream does not continue live after a reset | E2E `reset_*` cases (next published event arrives) |
| F9 | `assistant.delta` replayed, stored, or evicting durable records | E2E `delta_not_replayed` |
| F10 | Header vs query cursor precedence wrong; query fallback ignored | E2E `query_fallback`, `header_wins_over_query` |
| F11 | Pre-restart id accepted after control restart | E2E `restart_forgets_ids` (restarts the real process). Isolation: `TestEventIDsAreGlobalAndUnknownAfterRestart`, because rooms do not survive a restart, so only an in-process test can keep the room while resetting the counter. |
| F12 | Missing SSE headers or heartbeat; history replayed without a cursor | E2E `sse_headers_heartbeat_live_only` |
| F13 | Live buffer overflow drops frames silently | Not reproducible through the endpoint (needs a slow reader on a full 256-slot buffer). Covered by the lag check before every buffered frame; verified manually with a forced 2-slot buffer (see PR). No automated test. |

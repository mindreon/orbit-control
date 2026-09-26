-- Durable room (task) events: the table behind SSE ids and Last-Event-ID replay.
--
-- Not applied in W1: control has no database driver yet (AGENTS.md). The file
-- store mirrors this shape — one global id sequence across all rooms, and a
-- per-room, id-ordered list standing in for the (task_id, id) index.

CREATE TABLE room_events (
    -- Single global sequence; this is the public SSE `id:`.
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    -- roomId (room = task, contract C29).
    task_id     text        NOT NULL,
    -- ActivityEvent.id (worker eventId, used for ingest dedup).
    event_id    text        NOT NULL,
    type        text        NOT NULL,
    payload     jsonb       NOT NULL,
    occurred_at timestamptz NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX room_events_task_id_id ON room_events (task_id, id);

-- Replay after a cursor:
--   SELECT id, payload FROM room_events WHERE task_id = $1 AND id > $2 ORDER BY id;
-- Cursor belongs to this task (otherwise reset "unknown"):
--   SELECT EXISTS (SELECT 1 FROM room_events WHERE task_id = $1 AND id = $2);
--
-- Identity values are allocated at INSERT but become visible at COMMIT, so with
-- concurrent writers a reader can see id N+1 before N commits. Inserts must be
-- serialized (or replay must stop below the oldest in-flight id); otherwise
-- "id > cursor" replay can skip an event.

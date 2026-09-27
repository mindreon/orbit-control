# Stuck room or approval

Use this when a task stays on `awaiting_approval`, an approval stays
`in_flight` or `unknown`, or a room is `failed` and a client keeps
retrying decide. Read the database. Do not paste client error text into a
ticket: HTTP 400 responses use a fixed message and do not include the
server's `err.Error()` string (FM-72).

## What you can see

```sql
SELECT a.id, a.status, a.decision, a.delivery_state, a.delivery_attempt,
       a.result_attempt, a.decided_at, a.delivery_updated_at,
       (a.result_body IS NOT NULL) AS has_result_body,
       r.state AS room_state, r.failure
  FROM approvals a
  JOIN rooms r ON r.id = a.task_id AND r.tenant_id = a.tenant_id
 WHERE a.id = :approval_id;
```

`failure.message` on a failed room is one of the fixed sentences in
contract §10.2. It is not a provider error and not a Go error string.

## What control does on its own

The reconciler runs every `ORBIT_DELIVERY_RECONCILE_INTERVAL_S` seconds
(default 10), for every tenant:

- `in_flight` older than `ORBIT_DECISION_DELIVERY_TIMEOUT` plus 30 seconds
  moves to `unknown` (T3).
- `unknown` is queried with `decideOutcome`. A done outcome becomes
  `delivered` (T7). If the workflow still lists the id as pending, the row
  is retried (T6) with the same `approvalRequestId` as Update id.
- `unknown` older than `ORBIT_DELIVERY_UNKNOWN_TIMEOUT_S` (default 600
  seconds) becomes `unresolved` (T8).
- A leftover `not_delivered` row returns to `pending` (T9). The client
  may decide again.
- `delivered` with a saved result and a null `result_attempt` is written
  once. A restarted control reads that row from the database. It does not
  need the process that accepted the decision.

On a room that is already `failed`: a stalled `in_flight` still goes to
`unknown` first; `unknown` is queried once; a done outcome is `delivered`;
anything else, including a failed query, is `unresolved` immediately. The
reconciler does not retry that room with T6.

## What not to do

Do not `UPDATE` delivery columns by hand. The trigger
`orbit_approvals_delivery_transition` rejects any transition that is not
in the T1–T12 table (`P0001`, `approval is not pending`), for every role.
`orbit_app` also cannot `DISABLE` or `DROP` that trigger (FM-74).

Do not delete the room to "unstick" an approval. Soft delete hides the
task. A failed room stays readable. Decide on it returns **409
`ROOM_FAILED`**. The user starts a new task.

Abort cancels approvals that are still `pending` (T10). It does not cancel
`in_flight`. Wait for the reconciler, or call abort only when the user
wants the task stopped.

## Client symptoms

| What the client sees | What it means |
|---|---|
| 202 and `deliveryState: unknown` | The decision may already have been accepted. The same decision again is 202 and is not sent twice. A different decision is 409 `APPROVAL_DELIVERY_PENDING`. |
| 503 `APPROVAL_NOT_DELIVERED` | The workflow never received this id, inside `ttlS/2` from `decideConfig`. The approval is pending again. |
| 409 `ROOM_FAILED` | The room is failed. `failure.code` is on the room. Retrying decide will not resume it. |
| 409 `APPROVAL_NOT_PENDING` | The approval is delivered, unresolved, or cancelled. |
| 502 `WORKER_ERROR` | The decision was accepted and the resumed turn failed for a reason other than the approvals limit. The fixed message is the one to show. |
| 400 `the request is invalid` | The body or a field was rejected. The response does not quote the internal error. |

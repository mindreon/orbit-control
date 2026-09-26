# Persistence failure modes (contract §18, C32 rev2)

Signed contract: `orbit-contract-draft-v2.md` §18, sha256
`053a37bb093e06602a50f6412cd57eb3491161f039b567880ba6650ec6965229`.

## How persistence is verified

1. **E2E first.** Every S-DB-13 scenario that a client can observe is driven
   through the HTTP API (`/v1/*`, `/internal/events`) against a real
   Postgres 16, with the production mux, `app` and `pgstore`. The suite lives
   in `e2e/persistence` (build tag `e2e`) and writes
   `artifacts/e2e-persistence-report.json`. For every case the report records
   the id, request, expected result, actual result and pass/fail. CI uploads
   the report.
2. **Isolated checks only where HTTP cannot see the property.** They are
   listed below, each with the failure modes it exists to catch. An isolated
   check without an entry here must not be added. Report cases of kind
   `isolated` point at an `FM-*` id from this file.

What the E2E suite stubs, and why:

- **Session authenticator.** It reads `X-E2E-User`, because the §17 OIDC
  session does not exist yet. The tenant is fixed per scenario and never read
  from the request.
- **orbit-worker activities.** A stub HTTP server stands in for the worker.
- **Temporal, for S-DB-13 (k) only.** The contract says to stub Temporal
  there.

No unit tests are added for persistence.

## Isolated checks and the failures they catch

### ISO-1 — S-DB-11 (a): static scan of SQL shapes

Scans every `.go` string literal (AST) and every `.sql` file in the repo.
Before the scan it runs known-bad samples through the checker, so a rule
that silently stops matching fails the check.

- **FM-1.** A session-level `SET app.tenant_id = …` is written. The GUC
  survives the transaction, and a pooled connection serves the next
  request with the previous tenant, so it reads that tenant's rows.
- **FM-2.** `SET LOCAL app.tenant_id = '` + id + `'` is written, or built with
  `Sprintf`. `SET` cannot take bind parameters, so the tenant id is
  concatenated into the SQL, which is SQL injection.
- **FM-3.** `set_config('app.tenant_id', …, false)` makes the GUC session
  scoped, the same leak as FM-1. A literal or non-`$1` tenant argument means
  the value is not bound.
- **FM-4.** A new `SECURITY DEFINER` function is added outside the
  allowlist. Every definer function runs with its owner's rights, and this
  one would bypass RLS without review.
- **FM-5.** `deleted_at` / `deleted_by` are written from Go or from another
  SQL function instead of `orbit_soft_delete_room`. From `orbit_app` such a
  write fails with an RLS error (500 instead of 204). From a privileged role
  it skips the tenant / creator / liveness checks.

Why HTTP cannot catch these: each is a code-shape property. It only shows up
through HTTP under a specific pool interleaving or a malicious input, and
never reliably. Blind spots: SQL assembled from non-literal variables, and
statements run by superusers outside the repo.

### ISO-2 — S-DB-11 (b): GUC does not survive on a pooled connection

The server runs with a pool of one connection. Request A (HTTP) is served
on it. Then a raw query on the same backend (same `pg_backend_pid`) must
see an empty `app.tenant_id` and 0 rows. This also holds after a
request whose transaction rolled back (HTTP 404).

- **FM-6.** The repository sets the tenant outside a transaction, or the
  transaction commits on one path and leaves the GUC set on another
  (error / rollback path).
- **FM-7.** The pool's connection reset does not clear custom GUCs. FM-1/FM-3
  then leak even when the code shape looks right.

Why HTTP cannot catch these: HTTP only shows the effect when the next
request happens to be for a different tenant without its own
`set_config`. The leak itself is connection state.

### ISO-3 — S-DB-13 (b): RLS backstop hides a deleted task from raw SQL

As `orbit_app`, with the tenant set and **no** `deleted_at` predicate,
selecting by `task_id` from rooms, events, messages, turns, approvals,
idempotency_keys and artifacts returns 0 rows. Selecting
`artifact_versions` by `artifact_id` also returns 0 rows. The surviving
task's rows stay visible.

- **FM-8.** A table is missing `ENABLE` / `FORCE ROW LEVEL SECURITY`, or a
  new task-child table ships without the `EXISTS (live room)` policy. The
  repository's own filter hides this until one query forgets it.
- **FM-9.** A policy uses `current_setting('app.tenant_id')` without
  `missing_ok`. That errors when the GUC is unset instead of matching
  nothing. A policy on USING only, without WITH CHECK, lets deleted-task
  rows be written.
- **FM-10.** The `artifact_versions` policy does not chain through
  `artifacts`, so version rows of a deleted task stay readable.

Why HTTP cannot catch these: the API always goes through repository queries
that already filter `deleted_at IS NULL`. The backstop is only observable
when those filters are bypassed.

### ISO-4 — S-DB-13 (e)(f): audit columns, read as the owner role

- **FM-11.** `deleted_by` is taken from the request body (`u-evil`) instead
  of the session. The same failure applies if `deleted_at` is taken from the
  body.
- **FM-12.** The soft delete reports 1 row but does not actually set
  `deleted_at` / `deleted_by`, for example because the function writes the
  wrong column. The row would then reappear.

Why HTTP cannot catch these: by design no API returns a deleted row. Only
the owner role (BYPASSRLS, audit) can read it.

### ISO-5 — S-DB-13 (g): no physical delete path

As the owner role, `DELETE FROM rooms WHERE id = <deleted>` must fail with
`23503` (foreign-key RESTRICT). The shared blob file and the other task's
version row that references it stay intact.

- **FM-13.** A child FK is `CASCADE`, `SET NULL` or missing. A privileged
  hard delete would then silently erase messages, events, approvals and
  artifact history.
- **FM-14.** Deletion removes content-addressed blob files that another
  task still references.

Why HTTP cannot catch these: P0 has no hard-delete or purge API.

### ISO-6 — S-DB-13 (h): room-scoped approval rules

As `orbit_app`: 0 rows for `approval_rules WHERE room_id = <deleted>`. The
persona-scoped rule and other rooms' rules stay visible.

- **FM-15.** The `approval_rules` policy lacks the room condition. A deleted
  task's allow-always rule would keep being listed and matched.
- **FM-16.** A persona-scoped rule is hidden by mistake (the policy requires
  a room for every rule).

Why HTTP cannot catch these: control has no `/v1/approval-rules` route yet.
Move this to E2E when that route lands.

### ISO-7 — S-DB-13 (i): `orbit_soft_delete_room` privileges and properties

- `pg_proc`: `prosecdef = true`. The owner is `orbit_definer`, which is
  NOLOGIN, BYPASSRLS and not a superuser. `proconfig` is exactly
  `search_path=pg_catalog, public`. The ACL is non-NULL and has no PUBLIC
  entry. `orbit_app` has EXECUTE.
- Calling the function as `orbit_owner`, or as `orbit_definer` (via
  `SET ROLE`), fails with `42501 insufficient_privilege`.
- Called as `orbit_app`, it returns 0 when there is no tenant, a foreign
  tenant, a non-creator user, or an already-deleted room.
- As `orbit_app`, `DELETE FROM rooms` affects 0 rows.

Failure modes:

- **FM-17.** The function is not `SECURITY DEFINER`. The soft delete then
  fails with `new row violates row-level security policy` (500), which is
  the H1 problem.
- **FM-18.** `EXECUTE` stays with PUBLIC, which is the Postgres default on
  `CREATE FUNCTION`. Any role, including future service roles, could then
  soft-delete any creator's task in its tenant.
- **FM-19.** The function is executable by the owner or definer roles. A
  migration or debugging session could then soft-delete as an arbitrary
  `p_user`, bypassing the session.
- **FM-20.** The `search_path` is not fixed, or the function uses
  unqualified table names. A caller-controlled schema could then shadow
  `rooms`, and the definer would operate on the attacker's object.
- **FM-21.** The function skips one of its own checks (tenant GUC,
  `created_by = p_user`, `deleted_at IS NULL`) and relies on the caller.
- **FM-22.** A DELETE policy is added to `rooms`, which makes physical
  deletes by `orbit_app` possible.

Why HTTP cannot catch these: HTTP always calls the function correctly, as
`orbit_app` and with the session user. The failures above are about who
else can call it and what happens when they do.

Known limit: superusers bypass every privilege check. That is outside this
contract and not asserted.

### ISO-8 — S-DB-13 (j), and abort ordering for §18.7a: owner-role reads

- `rooms.last_event_seq` of the deleted task is unchanged after late worker
  events. The events themselves return 404 through HTTP (E2E).
- While the workflow abort is in flight, the room is still live
  (`deleted_at IS NULL`). The stub worker or Temporal reads this via the owner role.

Failure modes:

- **FM-23.** A late event increments the sequence (or writes a row) before
  answering 404, which leaves holes or ghost events.
- **FM-24.** The soft delete happens before the abort. Agents keep running on
  a task the user already deleted, and their events race the delete.

Why HTTP cannot catch these: the sequence counter and the relative order of
the abort and the delete are not exposed by any response.

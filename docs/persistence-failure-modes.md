# Persistence failure modes (contract §18, C32 rev2)

Signed contract: `orbit-contract-draft-v2.md` §18, sha256
`053a37bb093e06602a50f6412cd57eb3491161f039b567880ba6650ec6965229`.

## How persistence is verified

1. **E2E first.** Every S-DB scenario that a client or operator can observe
   is driven through the real interfaces against a real Postgres 16:
   - the HTTP API (`/v1/*`, `/internal/events`) using the production mux,
     `app` and `pgstore` over TCP;
   - the built `orbit-control` binary as a separate process, for the
     start, restart and refusal cases (S-DB-3, S-DB-8, S-DB-9).

   The suite lives in `e2e/persistence` (build tag `e2e`).
2. **Isolated checks only where the property is not observable through those
   interfaces.** They are listed below, each with the failure modes it exists
   to catch. An isolated check without an entry here must not be added, and
   the entry must be committed **before** the check's code. The suite's
   `process/commit-order/*` cases verify this from git history: every `FM-*`
   id that the checks reference must have been added to this file in a
   commit that is a strict ancestor of the commit that first used the id in
   `e2e/`.
3. **Static checks.** S-DB-1 and S-DB-11 (a) stay static (ISO-9, ISO-1).
4. **Blocked cases.** A contract case whose feature is outside this PR by
   contract ordering or owner instruction is still written to the report,
   with `status: "blocked"`, `pass: false` and `blockedBy`. The report's
   top-level `gate` is `pass` only when there are no failed and no blocked
   cases.
5. **Report.** `artifacts/e2e-persistence-report.json` holds, per case:
   id, contract clause, kind, steps, request, expected, actual, status and
   pass. It also records the commit SHA, component versions and a
   fingerprint of the normalized cases.
   - Volatile values are replaced with stable placeholders before recording:
     generated ids, timestamps, the planted secrets. Two runs on the same
     commit must therefore produce identical `cases`, and CI runs the suite
     twice and diffs them.
   - The report is secret-scanned before it is written, and again by CI
     before upload. It must contain no DB URL, no DB user/password from the
     environment and no planted secret.

What the E2E suite stubs, and why:

- **Session authenticator (in-process HTTP only).** It reads `X-E2E-User`,
  because the §17 OIDC session does not exist yet. The tenant is fixed per
  scenario and never read from the request. The binary runs in its real
  local mode.
- **orbit-worker activities.** A stub HTTP server stands in for the worker.
- **Temporal, for S-DB-13 (k) only.** The contract says to stub Temporal
  there.

No unit tests are added for persistence.

## Coverage of S-DB-1 … S-DB-13

| Case | How | Blocked parts (reported as `blocked`) |
|---|---|---|
| S-DB-1 | static, ISO-9 | — |
| S-DB-2 | E2E cross-tenant over HTTP, plus ISO-10 | — |
| S-DB-3 | E2E through the binary: create, restart, read back | events persisted with `last_event_seq` continuing, and artifacts: phase 2 (per-task `seq`, §18.4) and §16 control artifact PR |
| S-DB-4 | ISO-11 (schema) | a real login's `sessions` row holds only the hash: auth PR (§17, after this PR per §17.9) |
| S-DB-5 | — | artifact version ingest (200/200/200/409/409): §16 control artifact PR |
| S-DB-6 | ISO-12 (migrations) | — |
| S-DB-7 | — | per-task `seq`, SubscribeAndReplay: phase 2, after Last-Event-ID PR #15 |
| S-DB-8 | E2E through the binary | — |
| S-DB-9 | E2E through the binary (DB connect and migrate failures) and over HTTP (runtime storage failure) | session and OIDC error paths: auth PR |
| S-DB-10 | ISO-13 (DB constraint) | streamed turn not persisting deltas: phase 2 (events are not persisted yet) |
| S-DB-11 | (a) static ISO-1; (b) E2E + ISO-2 | — |
| S-DB-12 | — | `/internal/artifact-blobs` and the internal listener: phase 2 |
| S-DB-13 | E2E + ISO-3…8 | none at the storage level; see ISO-6 for routes control does not have yet |

## Privilege decisions (review M1, M2, L1)

`orbit_app` gets the least DML that the code paths need. Every exception
below has a written reason; a new grant needs a new line here first.

| Table | orbit_app | Reason for anything beyond SELECT/INSERT |
|---|---|---|
| `tenants` | SELECT | Tenants are created by the ops role `orbit_ops`, which has access to `tenants` only, via `deploy/postgres/ensure-tenant.sql`. Control only checks at startup that its default tenant exists, and refuses to start if it does not. |
| `rooms` | SELECT, INSERT, UPDATE on some columns, DELETE | UPDATE covers only `kind, title, state, permission_preset, runtime, session_id, persona_id, delegation, failure, last_event_seq, updated_at`. The excluded columns are `id`, `tenant_id`, `created_by`, `created_at`, `deleted_at` and `deleted_by`; soft delete writes the last two only through `orbit_soft_delete_room`. DELETE stays granted only because S-DB-13 (i) (signed text) requires `DELETE FROM rooms` by the app role to *affect 0 rows*, which RLS guarantees because there is no DELETE policy. Revoking it would turn that into a privilege error. That is an owner decision; the rest of this file assumes the contract text. |
| `events`, `messages`, `turns`, `approvals`, `artifacts`, `artifact_versions` | SELECT, INSERT, UPDATE; **no DELETE** | Task history. P0 never deletes it (§18.7a, no purge). |
| `users`, `personas`, `mcp_connectors`, `cloud_agent_jobs` | SELECT, INSERT, UPDATE; **no DELETE** | No P0 code path deletes these rows. A future delete API must add its reason here first. |
| `approval_rules` | SELECT, INSERT, UPDATE, DELETE | Revoking a rule deletes its row (§18.3; C23 / S-Rb-8). |
| `idempotency_keys` | SELECT, INSERT, UPDATE, DELETE | Expiry cleanup: an expired key for the same user is deleted before it is reused (`pgstore.createRoomOnce`). The `EXISTS(live room)` policy still hides keys of deleted tasks, so M1-1 is unaffected. |
| `sessions` | SELECT, INSERT, UPDATE, DELETE | Logout destroys the server session (§17.3), and expired sessions are cleaned up by `expires_at`. |
| `oidc_login_state` | SELECT, INSERT, UPDATE, DELETE | One-time use: the callback consumes the row with `DELETE … RETURNING` (§18.3), and expired rows are cleaned up. |

**Decision: `sessions` and `oidc_login_state` have no RLS.** A session is
looked up before the tenant is known, and login state belongs to no tenant
(§18.5). This is acceptable only because both tables hold hashes, never
raw tokens:

- `sessions.id_hash` is `sha256(session id)`, and `pre_session_hash` is
  sha256 as well. Both are 32 bytes and enforced by CHECK.
- The only code that touches these tables is `internal/store/auth`. It
  hashes the raw id inside the package, so a raw token never reaches SQL.
  S-DB-1 enforces both the allowlist and the package boundary.

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

### ISO-9 — S-DB-1: every repository method is tenant-scoped (static)

The check parses `internal/store` with the Go AST.

- The `store.Repository` interface and every exported method of
  `pgstore.Store` and `memstore.Store` must take a `tenantID` parameter and
  use it in the body.
- Every SQL literal in `pgstore` that reads or writes a [T] table must
  contain `tenant_id`.
- The allowlist is `Close` only (lifecycle, touches no table). The auth-only
  methods for `sessions` / `oidc_login_state` join it with the auth PR.
  Allowlist changes need Sentinel review.

Failure modes:

- **FM-25.** A repository method without a tenant parameter becomes a
  cross-tenant read or write path. RLS only saves it if the GUC happens to
  be unset.
- **FM-26.** A method takes `tenantID` but never uses it, for example because
  it passes a constant or another tenant to `set_config`.
- **FM-27.** A query against a [T] table omits `tenant_id` and relies on
  RLS alone. That breaks the two-layer rule of §18.5.

Why it must be static: correct code and missing-predicate code return the
same rows while RLS is intact, so no runtime observation separates them.

### ISO-10 — S-DB-2: RLS on every [T] table, role attributes

Every [T] table is seeded for tenants A and B (as `orbit_app`, each under its
own tenant). Then:

- as the owner (BYPASSRLS), the rows of both tenants are visible;
- as `orbit_app` with no GUC, and with the GUC set to `''` or an unknown
  tenant, every [T] table returns 0 rows;
- with the GUC set to A, only A's rows are visible, and likewise for B;
- in `pg_roles` / `pg_class`, `orbit_app` is not SUPERUSER, not BYPASSRLS
  and owns no [T] table, and every [T] table has RLS enabled and forced with
  at least one policy.

Failure modes:

- **FM-28.** A [T] table has no RLS, has RLS without FORCE, or has no
  policy.
- **FM-29.** A policy compares the wrong column, adds an `OR`, or matches
  NULL/'' tenant values, so tenant A sees B's rows.
- **FM-30.** The app role is an owner or BYPASSRLS, for example because
  migrations ran as the app role or the bootstrap granted the attribute.
  RLS then silently does nothing.
- **FM-31.** An unset or empty GUC matches rows, because the policy is
  written without `missing_ok`, or with `coalesce(..., tenant_id)`, or
  similar.

Why HTTP cannot catch these: the HTTP layer always sets a valid tenant and
filters by it. The cross-tenant E2E cases prove the query layer; only raw
SQL proves the RLS layer underneath it.

### ISO-11 — S-DB-4: no raw session ids or IdP tokens can be stored (schema)

- No column in `public` has a name matching token, secret, password,
  refresh, access, cookie, credential or api_key.
- `sessions.id_hash` and `oidc_login_state.pre_session_hash` accept only
  32-byte values (sha256). A 16-byte value fails with `23514`.
- `sessions` and `oidc_login_state` have no RLS (pre-login, §18.5).

Failure modes:

- **FM-32.** A column for IdP tokens (refresh/access/id token) or secrets is
  added. P0 must not store them.
- **FM-33.** The raw session id, or any non-sha256 value, is stored in
  `sessions.id_hash`. Anyone with DB read access could then hijack the
  session.
- **FM-34.** RLS is enabled on the pre-login tables. Session lookup happens
  before the tenant is known, so it would fail or tempt a bypass.

Why HTTP cannot catch these: no code path writes sessions until the auth
PR. The schema is the guard until then.

### ISO-12 — S-DB-6: migrations are idempotent and reversible

The owner role runs, in order:

1. down to 0 and drop the goose version table (a truly empty schema);
2. up;
3. up again (0 applied, same version, identical schema fingerprint);
4. down to 0 (no tables, policies or functions left);
5. up (identical schema fingerprint).

Failure modes:

- **FM-35.** Up is not idempotent: a re-run fails, or it changes the schema.
- **FM-36.** Down leaves objects behind (policies, functions, grants), and
  they break or skew the next up.
- **FM-37.** Up → down → up yields a different schema from a fresh up
  (drift between the up and down sections).

Why HTTP cannot catch these: migration tooling has no HTTP surface.

### ISO-13 — S-DB-10: `assistant.delta` cannot be persisted (constraint)

As `orbit_app`, inserting an `events` row with `type = 'assistant.delta'`
for a live task fails with `23514`. A `tool.call` row for the same task is
accepted.

- **FM-38.** Streaming deltas get persisted. That bloats `events` and makes
  replay resend token fragments.

Why HTTP cannot catch this yet: control does not persist events until
phase 2. The constraint is the storage-level guarantee in the meantime.

### ISO-14 — review M1: history cannot be deleted by `orbit_app`

As `orbit_app`, with the owning tenant set, `DELETE` on `events`, `messages`,
`turns`, `approvals`, `artifacts`, `artifact_versions`, `users`, `personas`,
`mcp_connectors`, `cloud_agent_jobs` and `tenants` fails with `42501`, and
the owner's row count is unchanged.

As a positive control, DELETE still works where it is kept for cleanup:
`idempotency_keys`, `sessions`, `oidc_login_state` and `approval_rules`.

- **FM-39.** A bug or an injected statement in control deletes task history.
  The only thing standing in the way is the absence of a code path.
- **FM-40.** DELETE is granted on a table without a documented cleanup
  reason (grant creep, for example `GRANT … ON ALL TABLES`).

Why HTTP cannot catch these: no API deletes these rows, so only the
privilege itself can be observed.

### ISO-15 — review M2: tenants are created by ops, not by control

- As `orbit_app`, `INSERT`, `UPDATE` and `DELETE` on `tenants` fail with
  `42501`; `SELECT` works.
- `orbit_ops` can insert a tenant (through the real
  `deploy/postgres/ensure-tenant.sql`). It cannot read `rooms`, and it is
  neither owner nor BYPASSRLS.
- E2E through the binary: with its default tenant missing, control exits
  non-zero with a clear message and the tenant count is unchanged. After ops
  creates the tenant, control starts.

Failure modes:

- **FM-41.** The app role can create or rename tenants. A compromised
  control could then mint tenant ids and set `app.tenant_id` to them.
- **FM-42.** Control silently creates its default tenant at startup, so a
  typo in `ORBIT_DEFAULT_TENANT` yields a fresh empty tenant instead of an
  error.
- **FM-43.** The ops role has access beyond `tenants`.

Why HTTP cannot catch these: they are privilege and startup behaviour. The
startup part *is* E2E, through the binary.

### ISO-16 — review M2: raw tokens are not findable in the database

The owner role scans every text, varchar, jsonb, array and bytea column of
every table in `public` for a raw value.

- **Idempotency-Key (E2E).** After `POST /v1/rooms` with a planted
  `Idempotency-Key`, the raw key is found nowhere. Its sha256 is found in
  exactly one place, `idempotency_keys.key_hash` (positive control).
- **Session id (isolated).** After creating a session through
  `internal/store/auth` with a planted raw id, the raw id is found nowhere.
  Its sha256 is found in exactly one place, `sessions.id_hash`, and
  `Lookup`/`Delete` work by raw id. There is no HTTP login until the auth
  PR, so this part stays isolated. The HTTP-login variant stays blocked
  (S-DB-4).
- **Scanner control.** A planted room title is found by the scanner, which
  proves the scan can find a raw value.

Failure modes:

- **FM-44.** A raw session id or raw Idempotency-Key is persisted in some
  column (a new column, a JSON payload, a log table). Anyone with DB read
  access could then replay it.
- **FM-45.** A value passes the 32-byte CHECK but is not the sha256 of the
  raw token. For example, a 32-character raw token stored as bytes would
  satisfy the CHECK.

### ISO-17 — review L1: `orbit_app` cannot rewrite ownership columns

As `orbit_app`, `UPDATE rooms SET` any of `created_by`, `tenant_id`,
`deleted_at`, `deleted_by`, `id` or `created_at` fails with `42501`, and
the owner reads the old value. `UPDATE rooms SET state` (a granted
column) succeeds.

- **FM-46.** Control can move a task to another creator (ownership
  takeover) or another tenant, or can un-delete or forge a delete outside
  `orbit_soft_delete_room`.

Why HTTP cannot catch this: the API never writes these columns, because
request bodies are ignored.

### ISO-9 addendum — pre-login table boundary (S-DB-1)

- **FM-47.** `sessions` or `oidc_login_state` is read or written outside
  `internal/store/auth` (the tables have no RLS, so every access path must
  be in the reviewed package). The S-DB-1 static check rejects SQL naming
  these tables in any other non-test Go package under `internal/`. The
  `auth.Sessions` methods are the allowlisted exceptions to the `tenantID`
  rule.

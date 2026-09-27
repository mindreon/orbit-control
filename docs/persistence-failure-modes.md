# Persistence failure modes (contract §18, C32 rev3)

Contract: `orbit-contract-draft-v2.md` §18.

- **C32 rev3** (Celestial: rooms DELETE revoked from `orbit_app`). The
  contract of record is `docs/contracts/orbit-contract-v2.md`: 986 lines,
  sha256 `113aebd89914a1008d1c57457572ef38e5de35c74e88ce2f8b8410997f9af90b`.
  `process/contract-file-sha256` asserts this value on every run.
- Previous signed version (C32 rev2): sha256
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
| S-DB-3 | E2E through the binary: create, restart, read back; events and `last_event_seq` (FM-62) | artifact metadata read API: §16 control artifact PR |
| S-DB-4 | ISO-11 (schema) | a real login's `sessions` row holds only the hash: auth PR (§17, after this PR per §17.9) |
| S-DB-5 | — | artifact version ingest (200/200/200/409/409): §16 control artifact PR |
| S-DB-6 | ISO-12 (migrations) | — |
| S-DB-7 | E2E: 32 writers, 1000 events, per-task seq (FM-62) | injected writes between subscribe and replay: handoff hooks not exposed |
| S-DB-8 | E2E through the binary | — |
| S-DB-9 | E2E through the binary (DB connect and migrate failures) and over HTTP (runtime storage failure) | session and OIDC error paths: auth PR |
| S-DB-10 | ISO-13 (DB constraint) and E2E ingest (FM-62): `assistant.delta` is not stored | — |
| S-DB-11 | (a) static ISO-1; (b) E2E + ISO-2 | — |
| S-DB-12 | E2E (FM-63): blob ingest on the internal listener; public listener returns 404 | — |
| S-DB-13 | E2E + ISO-3…8 | none at the storage level; see ISO-6 for routes control does not have yet |
| S-DB-14 (Sentinel follow-up on #16; not yet in the contract text) | ISO-18, connected as `orbit_app` | — |

## Privilege decisions (review M1, M2, L1)

`orbit_app` gets the least DML that the code paths need. Every exception
below has a written reason; a new grant needs a new line here first.

| Table | orbit_app | Reason for anything beyond SELECT/INSERT |
|---|---|---|
| `tenants` | SELECT | Tenants are created by the ops role `orbit_ops`, which has **SELECT and INSERT only** (no UPDATE, review L-a), via `deploy/postgres/ensure-tenant.sql` (`INSERT … ON CONFLICT DO NOTHING`). `tenants.id` has `CHECK (id <> '')`. Control only checks at startup that its default tenant exists, and refuses to start if it does not. |
| `rooms` | SELECT, INSERT, UPDATE (`state, session_id, updated_at, last_event_seq, failure`); **no DELETE** | UPDATE covers only the columns the code writes (review M-c). `last_event_seq` is incremented in the same transaction as `INSERT events` (§18.4, FM-62). `failure` is written when a room becomes `failed` with `failure.code = DECIDED_APPROVALS_LIMIT` (C34 T5/T11). **C32 rev3 (Celestial):** no DELETE privilege and no DELETE policy, so `DELETE FROM rooms` fails with `42501 permission denied`. Soft delete writes `deleted_at` / `deleted_by` only through `orbit_soft_delete_room`, which runs as `orbit_definer`. |
| `approvals` | SELECT, INSERT, UPDATE (`status, decision, decided_at, delivery_state, delivery_updated_at, delivery_attempt, result_attempt, result_body`); **no DELETE** | The claim and every later delivery transition are conditional UPDATEs (C34 T1–T12, FM-64). `result_attempt` is the write-back idempotency key (FM-61): it is set to `delivery_attempt` in the same transaction as the resumed turn's messages, and only when it is still NULL. `result_body` (FM-71) stores that write so a restarted process can apply it without an in-memory cache. The `BEFORE UPDATE` trigger `orbit_approvals_delivery_transition` allows only those transitions, for every role. `status` and `decision` have CHECK constraints (FM-73). |
| `sessions` | SELECT, INSERT, UPDATE (`last_seen_at`), DELETE | `auth.Sessions.Lookup` refreshes `last_seen_at`. DELETE is kept because logout destroys the server session (§17.3) and expired sessions are cleaned up. |
| `events`, `messages`, `artifact_versions` | SELECT, INSERT; **no UPDATE, no DELETE** | Immutable history (review M-c): a written event, message or artifact version is never rewritten. |
| `turns`, `artifacts` | SELECT, INSERT; **no UPDATE, no DELETE** | No P0 code path updates them. Phase 2 / the §16 PR add their columns here first (`turns.status`/`finished_at`, `artifacts.latest_version`/`updated_at`). |
| `users`, `personas`, `mcp_connectors`, `cloud_agent_jobs` | SELECT, INSERT; **no UPDATE, no DELETE** | No P0 code path updates or deletes these rows (`UpsertUser` is `INSERT … ON CONFLICT DO NOTHING`). |
| `mcp_market_servers` | SELECT, INSERT, UPDATE, DELETE | Shared plaza display rows, not tenant history and not under RLS. Process start replaces the shipped snapshot: `DELETE` the previous rows, then `INSERT` the new copy. No launch command, hosted URL, or secret is stored. |
| `mcp_market_categories` | SELECT, INSERT, UPDATE | The fourteen plaza labels. Startup upserts them. Rows are not deleted. |
| `mcp_market_details` | SELECT, INSERT, UPDATE, DELETE | Readme and tool list for one plaza card. Startup replaces the whole table with the shipped snapshot. No hosted URL, launch secret, or discussion thread is stored. |
| `approval_rules` | SELECT, INSERT, DELETE; **no UPDATE** | Revoking a rule deletes its row (§18.3; C23 / S-Rb-8). Rules are never edited in place. |
| `idempotency_keys` | SELECT, INSERT, DELETE; **no UPDATE** | Expiry cleanup: an expired key for the same user is deleted before it is reused (`pgstore.createRoomOnce`). The `EXISTS(live room)` policy still hides keys of deleted tasks, so M1-1 is unaffected. |
| `oidc_login_state` | SELECT, INSERT, DELETE; **no UPDATE** | One-time use: the callback consumes the row with `DELETE … RETURNING` (§18.3), and expired rows are cleaned up. |

**No TRUNCATE, REFERENCES or TRIGGER for `orbit_app` on any table.** The
migration revokes them explicitly from `orbit_app` and `PUBLIC`. The
reasons:

- TRUNCATE is not subject to RLS, so one statement would remove every
  tenant's rows.
- TRIGGER would let the app attach code that runs on every tenant's writes.
- REFERENCES is never needed by the app.

**Decision: `sessions` and `oidc_login_state` have no RLS.** A session is
looked up before the tenant is known, and login state belongs to no tenant
(§18.5). This is acceptable only because both tables hold hashes, never
raw tokens:

- `sessions.id_hash` is `sha256(session id)`, and `pre_session_hash` is
  sha256 as well. Both are 32 bytes and enforced by CHECK.
- The only code that touches these tables is `internal/store/auth`. It
  hashes the raw id inside the package, so a raw token never reaches SQL.
  S-DB-1 enforces both the allowlist and the package boundary.

## Auth PR security requirements (review L-b; documented, not implemented here)

- **Session ids.** The auth PR switches from `sha256(id)` to:
  - a server-generated 32-byte random id (`crypto/rand`), base64url in
    the `orbit_session` cookie;
  - stored as `HMAC-SHA256(server_key, id)`. The key comes from a
    control-only secret (for example `ORBIT_SESSION_HMAC_KEY`), never from
    the database, and is rotated by keeping the previous key for lookups
    during one absolute session lifetime.

  The output is still 32 bytes, so `sessions_id_hash_sha256` holds. A DB
  dump alone no longer allows offline id confirmation.
  `internal/store/auth` is the only place that computes it. Client-chosen
  ids are never accepted.
- **Session lifetime.**
  - Idle timeout 12 h: the row is rejected when `last_seen_at` is older.
  - Absolute timeout 7 d: `expires_at`.
  - Logout deletes the row.
  - A periodic cleanup runs `DELETE FROM sessions WHERE expires_at < now()`
    (index `sessions_expires_at_idx`).
- **`oidc_login_state.pkce_verifier` and `nonce`.** These are the only
  secrets stored in clear, because the callback must present the verifier to
  the IdP and compare the nonce.
  - Lifetime is 10 minutes (`expires_at`, §17.2).
  - The row is consumed exactly once on callback (`DELETE … RETURNING`), on
    success or failure.
  - Expired rows are removed by the same periodic cleanup
    (`DELETE FROM oidc_login_state WHERE expires_at < now()`, index
    `oidc_login_state_expires_at_idx`).
  - Neither value is ever logged.
  - `pre_session_hash` follows the same HMAC rule as session ids.

## Bootstrap passwords (review L-c)

`deploy/postgres/bootstrap-roles.sql` sets these session-local before any
`CREATE ROLE … PASSWORD`:

- `log_statement = 'none'`
- `log_min_error_statement = 'panic'`
- `log_min_duration_statement = -1`

That way even a failing statement is not written to the server log with
the password.

Operators should pass **SCRAM-SHA-256 verifiers** instead of plaintext:

- `deploy/postgres/scram-verifier.py` computes a verifier locally.
- Postgres stores a value already in `SCRAM-SHA-256$…` form as-is.
- The plaintext therefore never reaches the server.

CI does exactly this. It also runs Postgres with `log_statement = 'ddl'`
and scans the server log with `scripts/ci-secret-scan.sh`. The scan has a
**positive control** (review Low-2), a CI step that runs before the real
scan:

1. The step writes a Postgres-style log line into a temporary directory
   that exists only inside CI and is never uploaded. The line is
   `CREATE ROLE … PASSWORD '<value>'`. It does this twice: once with a
   freshly generated one-time fake password, and once with the CI app
   password.
2. It runs the same script on that directory and requires it to report
   both files.

So "the log scan would catch a plaintext password" is demonstrated on every
run. The script prints only file names, never the matched value.

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
- Calling the function as any role other than `orbit_app` fails with
  `42501 permission denied`. The roles tried are `orbit_owner`,
  `orbit_definer` (via `SET ROLE`) and `orbit_ops`.
- Called as `orbit_app`, it returns 0 when there is no tenant, a foreign
  tenant, a non-creator user, or an already-deleted room.
- As `orbit_app`, `DELETE FROM rooms` fails with `42501 permission denied`
  (C32 rev3), and the owner's room count is unchanged.
- As `orbit_app`, `TRUNCATE rooms` fails with `42501 permission denied`,
  and the owner's room count is unchanged. TRUNCATE bypasses RLS, so only
  the privilege protects the table (FM-52).
- Soft delete through the function keeps working: `S-DB-13/delete` returns
  204 and `S-DB-13(k)/*` pass.

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
- **FM-22.** DELETE is re-granted on `rooms`, or a DELETE policy is added.
  Either one moves `orbit_app` one step closer to physical deletes. With
  both, deletes of rooms without children succeed.

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

As `orbit_app`, with the owning tenant set, `DELETE` on `rooms` (C32 rev3),
`events`, `messages`, `turns`, `approvals`, `artifacts`, `artifact_versions`,
`users`, `personas`, `mcp_connectors`, `cloud_agent_jobs` and `tenants` fails
with `42501 permission denied`, and the owner's row count is unchanged.

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

### ISO-18 — S-DB-14: the app role cannot escape RLS (catalog self-check)

Connected **as `orbit_app`** (the credentials control runs with), query
`pg_class` / `pg_roles` / `pg_has_role` and assert all of the following:

- `current_user` is `orbit_app`.
- `orbit_app` owns no table in `public`.
- `orbit_app` has neither BYPASSRLS nor SUPERUSER.
- Every table in `public` with RLS enabled also has FORCE ROW LEVEL
  SECURITY. The check also requires that at least the 13 [T] tables have
  RLS, so it cannot pass vacuously.
- `orbit_app` is not a member of any role that owns a `public` table or has
  BYPASSRLS / SUPERUSER. Role attributes are not inherited, but `SET ROLE`
  to such a role would bypass RLS all the same.
- `has_table_privilege(orbit_app, <table>, 'TRUNCATE' | 'REFERENCES' |
  'TRIGGER')` is false for every table in `public`.

Failure modes:

- **FM-48.** A table becomes owned by `orbit_app`, for example because
  migrations ran with the app credentials or an `ALTER TABLE … OWNER TO`
  slipped in. RLS then relies on FORCE alone, and the owner can drop
  policies or disable RLS.
- **FM-49.** `orbit_app` gains BYPASSRLS or SUPERUSER (ops change, bootstrap
  edit). Every policy is silently ignored.
- **FM-50.** A table has RLS enabled without FORCE. That table is
  unprotected against its owner, and the regression is invisible while the
  app role is not the owner.
- **FM-51.** `orbit_app` is granted membership in the owner, definer or a
  superuser role, which gives the same bypass through `SET ROLE`.
- **FM-52.** TRUNCATE is granted on a table. TRUNCATE ignores RLS: one
  statement removes every tenant's rows. With CASCADE and the privilege on
  the children, it removes whole task histories too.
- **FM-53.** REFERENCES or TRIGGER is granted on a table. TRIGGER lets the
  app role attach code that runs on every tenant's writes, and REFERENCES is
  a privilege the app never needs.

Why HTTP cannot catch these: every API response looks identical until
someone exploits the bypass. The check deliberately runs as `orbit_app`, so
it verifies what the running service can do, not what the migrator
believes.

### ISO-19 — review M-c: history cannot be rewritten; UPDATE is column-scoped

As `orbit_app`, with the owning tenant set, every table gets an UPDATE of a
column **outside** its grant. The new value is a real, valid value, so no FK,
RLS policy or CHECK could reject it first. Every such UPDATE fails with
`42501 permission denied`, and the owner reads the old value.

| Tables | Column updated |
|---|---|
| `events`, `messages`, `artifact_versions` | any column (no UPDATE at all) |
| `rooms` | `title` |
| `approvals` | `tool_name` |
| `sessions` | `expires_at` |
| `turns`, `artifacts`, `users`, `personas`, `mcp_connectors`, `cloud_agent_jobs`, `approval_rules`, `idempotency_keys`, `oidc_login_state`, `tenants` | a plain column |

Granted columns keep working: `rooms.state` (REVIEW-L1), approval decisions
(E2E M-d), and `sessions.last_seen_at` (auth `Lookup`).

- **FM-54.** A bug or an injected statement in control rewrites history (an
  event payload, a message text, an artifact digest or storage ref). The
  append-only story of §18 then silently breaks.
- **FM-55.** UPDATE is granted table-wide, so control can change columns that
  no code path writes: tool names on approvals, a room's title or preset,
  session expiry.

Why HTTP cannot catch these: no API writes these columns, so only the
privilege can be observed.

### ISO-15 addendum — review L-a: ops cannot rename tenants; empty id refused

- As `orbit_ops`, `UPDATE tenants SET name = …` fails with
  `42501 permission denied`. `INSERT` still works, via `ensure-tenant.sql`.
- Inserting a tenant with `id = ''` fails with `23514`.

Failure modes:

- **FM-56.** The ops role can rename or re-key tenants after creation.
  Tenant identity should be immutable once rows reference it.
- **FM-57.** A tenant with an empty id exists. After a transaction-local
  `set_config` ends, `current_setting('app.tenant_id', true)` returns `''`.
  Rows of an empty-id tenant would then match every "unset" query and break
  the "no GUC → 0 rows" guarantee.

### ISO-20 — review Low-1: a decided approval is frozen (trigger)

`BEFORE UPDATE ON approvals FOR EACH ROW` runs
`orbit_approvals_frozen_once_decided()`. When `OLD.status <> 'pending'` it
raises `P0001 approval is not pending`. This holds for every role
(`orbit_app`, the owner, BYPASSRLS), because triggers are not subject to
RLS or column privileges.

E2E (isolated SQL on an approval that was decided through the API): as
`orbit_app`, with the tenant set, both of these fail with that error, and
the owner reads `decided:allow` unchanged:

- `UPDATE approvals SET status = 'pending', decision = ''` (reopen);
- `UPDATE approvals SET decision = 'reject'` (flip).

The pending → decided transition through the API keeps working (REVIEW-Md).

- **FM-58.** A decided approval is rewritten, either reopened or flipped
  between allow and reject, by a bug, a replayed request or direct SQL. The
  audit trail and the delivered decision then disagree.

### F2 — phase 1 has no reopen; phase 2 must not double-deliver (documented, not implemented)

- **FM-59.** If control reopens an approval after `Orch.Decide`, or the
  worker's `resolveApproval`, *timed out*, the decision may already have
  been delivered. A retry then delivers it a second time.

How each phase handles it:

- **Before Low-1.** `App.Decide` reopened the approval on any delivery error,
  including timeouts, so FM-59 was possible.
- **Phase 1 now.** The Low-1 trigger forbids any update of a decided
  approval, so the reopen path is removed. When the delivery call fails
  or exceeds the delivery timeout, the request returns **502** with this
  fixed body:

  ```json
  {"error":"decision delivery failed","code":"DECISION_DELIVERY_FAILED","message":"the decision is recorded but its delivery to the workflow failed or timed out; it is not retried"}
  ```

  - The delivery call is `resolveApproval` on the direct path and the
    **acceptance** of the `decide` Update on the Temporal path (N1 below:
    the resumed turn is not part of delivery).
  - The delivery timeout is `app.Options.DeliveryTimeout`, 30 s by default.
  - The body never carries worker or Temporal error text.
  - Control logs `WARN alert=decision_delivery_failed approval=<id> reason=timeout|error`.
  - **The approval stays `decided`**, and every resubmit returns **409
    `APPROVAL_NOT_PENDING`**.
  - There is no double delivery: control delivers at most once. The cost is
    that a decision which truly was not delivered stays stuck until an
    operator acts.
  - E2E: `REVIEW-F2/phase1-timeout-at-most-once`, direct worker path and
    Temporal path.
  - **This 502 is temporary phase-1 behavior.** Phase 2 MUST replace it;
    see "Phase 2: delivery-state response" below.
- **Phase 2**, together with the real worker. Reopen only on errors that
  prove non-delivery: a connection refused before the request was sent, or
  an explicit "not applied" response. Never reopen on a timeout or an
  ambiguous error. Deliver with the approval id as the idempotency /
  dedupe key, so the worker and RoomWorkflow apply a decision at most once.
  The reopen needs a narrowly scoped path the Low-1 trigger allows, for
  example a SECURITY DEFINER function added to the S-DB-11 allowlist, with
  its own FM entry first.
- **E2E** (`REVIEW-F2/delivered-but-timeout-once`, FM-75). The e2e fault
  layer delivers once, then reports `Unknown` on that first attempt. The
  retry uses the same Update id and returns the accepted update without a
  second delivery. The response is **200**, `delivery_state` is `delivered`,
  and the stub records one Decide. This is the same injection as S-ID-3.

### N1 — Temporal decide: the delivery timeout bounds acceptance only

- **FM-60.** On the Temporal path, `DeliveryTimeout` (30 s) wrapped the
  whole `decide` Update: `UpdateWorkflow` with `WaitForStage: Completed`
  and then `handle.Get`. The Update completes only after the resumed turn
  finishes. So a resumed turn longer than `DeliveryTimeout` made the decide
  request return **502 `DECISION_DELIVERY_FAILED`** although the workflow
  had accepted the decision, and `applyTurnResult` was skipped: the turn's
  assistant texts were not persisted, a follow-up approval the turn asked
  for was not created, and the room stayed `awaiting_approval`.

The fix splits delivery from waiting for the turn result:

1. **Delivery = acceptance.** Control calls `UpdateWorkflow` with
   `WaitForStage: Accepted` and **`UpdateID` = `approvalRequestId`** (C35;
   the workflow derives `resumeTurnId`, and the decide request does not
   send that field). The acceptance wait is bounded by
   `ORBIT_DECISION_DELIVERY_TIMEOUT`. Only this step is delivery.
2. **Turn result on a background context.** After acceptance, control calls
   `handle.Get` under `context.WithTimeout(bg, turnTimeout)`, where `bg` is
   `context.WithoutCancel` of the request. That wait is not
   `ORBIT_DECISION_DELIVERY_TIMEOUT` (S-ID-13).
3. **A failure after acceptance is not a delivery failure.** `handle.Get`
   returning `ApplicationError` type `DECIDED_APPROVALS_LIMIT` is T11
   (FM-66). Any other `handle.Get` failure is **502 `WORKER_ERROR`** with
   fixed text (FM-66). It is not a string match on the error.
4. **`UpdateID` = `approvalRequestId`.** Repeated deliveries of one
   decision, including in-request retries and reconciler retries, share
   that id.

Phase 2 closes the remaining gap (FM-61, below). Writing the resumed turn
back no longer uses the HTTP request context. A client that disconnects
after the decision is claimed does not cancel that write, and a later
reconcile pass compensates a `delivered` row whose result was never written.

**Direct worker path.** N1 does not apply there. `DeliveryTimeout` bounds
only `resolveApproval`; the resume `runTurn` already runs under the request
context and is followed by `applyTurnResult`. No code change, but it gets
the same E2E so a regression on either path fails.

E2E (`REVIEW-N1/accepted-then-slow-turn/orch` and `/worker`):
`DeliveryTimeout` 200 ms; the stub accepts the decision at once (the
`decide` Update, or `resolveApproval`) and returns the resumed turn only
after 1 s. Expected: **200** with the approval `decided:allow`, no
`decision_delivery_failed` log, the resumed turn's assistant text in
`GET /v1/rooms/{id}/messages`, and the room `running`, not
`awaiting_approval`. `REVIEW-F2/phase1-timeout-at-most-once/orch` now holds
the stub's **acceptance** past `DeliveryTimeout`, and still expects the
502, 409 on resubmit, and at most one delivery. The reverse check (restore
the whole-call timeout) makes the `/orch` case fail with 502.

### Phase 2: C34 delivery state (FM-61, FM-64)

C34 is in `docs/contracts/orbit-contract-v2.md` (§2.4, merged with #18).
Phase 2 follows that section. The phase-1 502 `DECISION_DELIVERY_FAILED`
for an unconfirmed delivery is retired: that outcome is **202**
`{"approval", "deliveryState":"unknown"}` (a success body, not `ErrorBody`).

`approvals` gains `delivery_state`, `delivery_updated_at`, and
`delivery_attempt` (C34 §2.4.2) plus `result_attempt` (FM-61, below).
The public `status` value stays `pending` | `decided` | `cancelled`. C34's
words "allowed" and "rejected" are this row's `decision` (`allow` /
`reject`) while `status = 'decided'`. `ErrorBody` gains an optional
`approval` object. While `delivery_state` is `in_flight` or `unknown`, the
same decision again is **202** and is not re-delivered; a different
decision is **409 `APPROVAL_DELIVERY_PENDING`** and the body includes
`approval`. `DECIDED_APPROVALS_LIMIT` from the accepted update's result is
**409 `ROOM_FAILED`**: the approval goes `delivered` → `unresolved` (T11),
and the room becomes `failed` with
`failure.code = DECIDED_APPROVALS_LIMIT`.

- **FM-61.** The resumed turn is written back on a background context
  (`context.WithoutCancel` of the request, then
  `context.WithTimeout(bg, turnTimeout)`), not on the HTTP request context.
  Disconnecting the client after the decision is claimed does not cancel
  delivery or the write-back. The write is idempotent on `delivery_attempt`:
  one transaction sets `result_attempt = delivery_attempt` only when
  `result_attempt IS NULL` and `delivery_state = 'delivered'`, and in that
  same transaction inserts the resumed turn's messages and updates the room.
  A second write with the same attempt affects 0 rows and inserts nothing.
  Before that write, control stores the result in `approvals.result_body`
  (FM-71) so a later process can apply it. The reconciler does not keep an
  in-process cache of pending writes. E2E:
  `FM-60/disconnect-after-approve/{orch,worker}`,
  `FM-60/duplicate-writeback`, `FM-60/reconcile-unwritten`.
- **FM-64.** `BEFORE UPDATE` trigger `orbit_approvals_delivery_transition`
  allows only C34 transitions T1–T12, plus the FM-61 `result_attempt`
  assignment (delivery columns unchanged). Every other update raises
  `P0001` / `approval is not pending`, for every role. T2, T3, T4, T5 and
  T11 match `delivery_attempt`, so a late result cannot land on a newer
  `in_flight`. The trigger sets `delivery_updated_at = now()` on each
  delivery transition and ignores a caller-supplied value. E2E:
  `C34/transitions` (each allowed transition, a stale attempt affecting 0
  rows, and a forbidden rewrite raising `P0001`).

### Phase 2: per-task event seq and internal routes (FM-62, FM-63)

- **FM-62.** A durable event and its per-task seq are written in one
  transaction: `UPDATE rooms SET last_event_seq = last_event_seq + 1 …
  RETURNING last_event_seq`, then `INSERT INTO events`. The SSE id is that
  seq. `assistant.delta` is still not stored. `POST /internal/events` for a
  soft-deleted room, or for a room whose state is `closed` or `failed`,
  returns **404**, does not insert a row, and does not move
  `last_event_seq`. Control's own closing `session.status` may still be
  stored; the 404 is for events that arrive afterwards.
- **FM-63.** `POST /internal/artifact-blobs?taskId=` is served only on the
  internal listener. The tenant and the task come from the database; a
  tenant in the query or headers is ignored. The path is
  `{artifactDir}/{tenant}/{sha256}` using the sha256 control computed. A
  missing or ill-formed `X-Content-Digest` is **400**. A mismatch is
  **422** and the temp file is removed. A body over
  `ORBIT_ARTIFACT_MAX_BYTES` is **413**, counted while streaming, and
  leaves no temp file. On the public listener every `/internal/*` path,
  including `/internal/events` and `/internal/artifact-blobs`, is **404**.

## R1: C35 delivery follow-up (FM-65 … FM-76)

Contract of record: `docs/contracts/orbit-contract-v2.md` at the merged
commit `53ee38c053b48a58e7035940bbf050510554eac2` (C35). The suite hashes
that blob from git. It does not hard-code the hash, and it does not name
an unmerged commit.

`UpdateID` on the decide Update is `approvalRequestId`. The request body
does not contain `resumeTurnId`. In-request retries of `Unknown` use that
same id, with bounded backoff, and stop at
`ORBIT_DECISION_DELIVERY_TIMEOUT` (default 30s). Locks are held only inside
each transition `UPDATE`. Temporal RPC, retries, and waits run without a
row lock.

The reconciler interval is `ORBIT_DELIVERY_RECONCILE_INTERVAL_S` (default
10). The unknown deadline is `ORBIT_DELIVERY_UNKNOWN_TIMEOUT_S` (default
600). S-ID-9 (c) sets that variable to 5. None of the three is a fixed
10s sleep in the reconciler loop. `turnTimeout` (default 2 minutes) bounds
`handle.Get` and the result write, and is separate from the delivery
timeout (S-ID-13).

NotDelivered is only `ApplicationError` type `APPROVAL_UNKNOWN` when
`decideConfig.ttlS` was read successfully and `now() - decided_at < ttlS/2`
(FM-69). A failed `decideConfig` query is Unknown. There is no match on
the text `NOT_DELIVERED`.

- **FM-65.** T5 or T11 and the room failure are one transaction. The
  approval `UPDATE` and `rooms.state = 'failed'` commit together, or both
  roll back. Zero rows on the approval update means the caller lost the
  race: the transaction rolls back, the room is not marked failed, and
  control does not deliver, emit, or take any later step (contract §2.4.2).
  E2E: `S-ID-18/t5-zero-rows`, `S-ID-18/t11-zero-rows`.
- **FM-66.** T11 runs only when `errors.As` yields
  `*temporal.ApplicationError` and `Type() == "DECIDED_APPROVALS_LIMIT"`.
  Every other `handle.Get` failure is **502 `WORKER_ERROR`** with fixed
  text, including errors whose message happens to contain that code.
  E2E: `S-ID-8/t11-application-error`, `FM-66/other-get-is-502`.
- **FM-67.** Ingest of `room.failed` marks the room failed and stores
  `failure` from the event's `{code, message}` in one transaction. A second
  ingest of the same event does not write the room again: `failure` stays
  byte-for-byte the first value, and `last_event_seq` does not move. The
  event does not change approval rows. A later decide on that room is
  **409 `ROOM_FAILED`**. On a failed room the reconciler sends a stalled
  `in_flight` row through T3 first; an `unknown` row is queried once via
  `decideOutcome`. A done outcome becomes `delivered` (T7). A missing
  outcome or a failed query becomes `unresolved` (T8) in that pass, with
  no T6 retry. E2E: `S-ID-8/ingest-room-failed`,
  `S-ID-8/ingest-room-failed-twice`, `S-ID-8/decide-after-room-failed`,
  `S-ID-8/failed-room-outcome-delivered`,
  `S-ID-8/failed-room-outcome-unresolved`. The runtime half of S-ID-8
  (1024 carried-over ids) stays blocked: `S-ID-8/runtime-carry-over`.
- **FM-68.** When any in-request transition `UPDATE` affects 0 rows (F4,
  S-ID-18), control does not deliver, change the room, or emit an event.
  It re-reads the row and answers from that state: `in_flight` or `unknown`
  with the same decision is **202**; a different decision is **409
  `APPROVAL_DELIVERY_PENDING`**; `delivered` is **200**; `unresolved` or
  `cancelled` is **409 `APPROVAL_NOT_PENDING`**; a failed room is **409
  `ROOM_FAILED`**; a T9 reopen to pending is **503
  `APPROVAL_NOT_DELIVERED`**. The response is never **500**. E2E:
  `S-ID-18/t2`, `S-ID-18/t3`, `S-ID-18/t5`, `S-ID-18/t11`.
- **FM-69.** T4 is decided from `decideConfig.ttlS`, compared with `ttlS/2`.
  E2E: `FM-69/inside-window`, `FM-69/config-query-fails`,
  `FM-69/outside-window`.
- **FM-70.** `failure.message` is one of the §10.2 sentences, imported from
  `internal/failtext` (generated from that table). A `room.failed` event
  control itself emits carries `failure: {code, message}`. E2E:
  `FM-70/messages-match-section-10-2`, and the message on
  `S-ID-8/t11-application-error`.
- **FM-71.** Reconciliation is driven by the database, for every tenant,
  not by an in-process `pendingWrites` map. Each pass reads `unknown`
  rows, stalled `in_flight` rows (T3, matching the attempt and
  `delivery_updated_at`, older than the delivery timeout plus 30s), leftover
  `not_delivered` rows (T9), and `unresolved` rows from T8 whose
  `decided_at` is inside 24 hours (T12 when `decideOutcome` is found).
  Abort cancels pending approvals (T10) and does not cancel `in_flight`.
  A restarted control applies a saved `result_body` for every tenant.
  A leftover `not_delivered` row (T4 committed without the same-transaction
  T9) is reopened by that pass: status `pending`, `decision` empty,
  `decided_at` null, `delivery_state` null, and one
  `approval.delivery_updated` event whose `deliveryState` is empty.
  E2E: `FM-71/restart-reconcile-from-database` (assertions read only the
  database), `FM-71/all-tenants`, `FM-71/leftover-not-delivered-t9`,
  `S-ID-12/t10-abort`.
- **FM-72.** A **400** body uses a fixed message. It does not return
  `err.Error()`. Operators use `docs/runbooks/stuck-room.md` when a room
  or approval stops moving. E2E: `FM-72/bad-request-hides-cause`.
- **FM-73.** `approvals.status` is `pending`, `decided`, or `cancelled`.
  `approvals.decision` is `''`, `allow`, or `reject`. Other values fail
  the CHECK. E2E: `FM-73/status-check`, `FM-73/decision-check`.
- **FM-74.** `orbit_app` cannot `DISABLE TRIGGER` or `DROP TRIGGER` on
  `orbit_approvals_delivery_transition` (SQLSTATE `42501`). E2E:
  `FM-74/disable-trigger`, `FM-74/drop-trigger`.
- **FM-75.** `ORBIT_E2E_FAULTS` and the symbol `orbitE2EFaults` exist only
  in a build with `-tags e2e`. `WriteResultAgain` is compiled only in that
  same build. The production binary contains neither symbol. CI builds
  both binaries and checks both symbols. `POST /internal/e2e/reconcile` is
  registered only in the e2e build. E2E: `FM-75/production-binary-has-no-fault-symbol`,
  `FM-75/production-binary-has-no-write-result-again`,
  and `REVIEW-F2/delivered-but-timeout-once`.
- **FM-76.** In-request retries of an `Unknown` accept use bounded backoff.
  The first wait is 20ms. Each later wait doubles, and no wait is longer
  than 200ms. The loop stops when the delivery context ends. That context
  is `ORBIT_DECISION_DELIVERY_TIMEOUT`. The waits, added together, do not
  run past that timeout. E2E: `FM-76/retry-bounded-by-delivery-timeout`.

S-ID cases this process runs through application code:

| Case | Id |
|---|---|
| S-ID-3 | `S-ID-3/unknown-once-same-update-id` |
| S-ID-4 | `S-ID-4/not-delivered` |
| S-ID-5 | `S-ID-5/unknown-then-workflow-ended` |
| S-ID-6 | `S-ID-6/reconcile-vs-reopen` |
| S-ID-8 control, including the C35 ingest lines | the `S-ID-8/*` ids under FM-66 and FM-67 |
| S-ID-9 (a) (b) (c) | `S-ID-9/a-unknown-then-delivered`, `S-ID-9/b-same-decision`, `S-ID-9/b-different-decision`, `S-ID-9/c-unknown-timeout` |
| S-ID-12 | `S-ID-12/t1` … `S-ID-12/t12`, `S-ID-12/stale-attempt-t2`, `S-ID-12/crash-before-t4-commit`, `S-ID-12/forbidden-delivered`, `S-ID-12/forbidden-unresolved`, `S-ID-12/forbidden-t9-decision`, `S-ID-12/forbidden-skip-in-flight`, `S-ID-12/forbidden-pending-unknown`, `S-ID-12/forbidden-cancelled-pending`, `S-ID-12/forbidden-t12-after-t11` |
| S-ID-13 | `S-ID-13/resume-slower-than-delivery-timeout` |
| S-ID-18 | the `S-ID-18/*` ids under FM-65 and FM-68 |

These stay `blocked`, each with `blockedBy`, and are not skipped silently.
They belong to the orbit-runtime §2.4 work. orbit-runtime#11 is not merged,
so real-stack keeps the merged runtime pin already in
`.github/workflows/e2e.yml`. These rows do not name an unmerged commit:

- `S-ID-2/repeat-delivery`
- `S-ID-7/continue-as-new`
- `S-ID-8/runtime-carry-over`
- `S-ID-10/ttl-cleanup`
- `S-ID-11/child-resolve`
- `S-ID-14/child-limit`
- `S-ID-15/shared-limit`
- `S-ID-16/fatal-before-end`
- `S-ID-17/room-failed-once`
- `C3/real-temporal-decide`
- `C3/real-temporal-fm60-disconnect`

`C3/real-temporal-decide` and `C3/real-temporal-fm60-disconnect` are
implemented under `e2e/realstack` and run only when
`ORBIT_RUNTIME_DECIDE_E2E=1`. That variable stays unset until the runtime
pin is the merge commit of orbit-runtime#11.

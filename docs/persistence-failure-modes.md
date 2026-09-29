# Persistence failure modes (contract §18)

Contract: orbit-infra `docs/architecture` (03 domain model, 09 events, 10 ledger, 12 work breakdown) and the contract
v3 schema in `internal/contract/v3/contracts.json`.

## How persistence is verified

1. **E2E first.** Every S-DB scenario that a client or operator can observe is driven through the real interfaces
   against a real Postgres 16:
   - the HTTP API (`/v1/*`, `/internal/*`) using the production mux, `app` and `pgstore` over TCP;
   - the built `orbit-control` binary as a separate process, for the start, restart and refusal cases (S-DB-3, S-DB-8,
     S-DB-9).

   The suite lives in `e2e/persistence` (build tag `e2e`).
2. **Isolated checks only where the property is not observable through those interfaces.** They are listed below,
   each with the failure modes it exists to catch. An isolated check without an entry here must not be added, and the
   entry must be committed **before** the check's code. The suite's `process/commit-order/*` cases verify this from
   git history: every `FM-*` id that the checks reference must have been added to this file in a commit that is a
   strict ancestor of the commit that first used the id in `e2e/`.
3. **Static checks.** S-DB-1 and S-DB-11 (a) stay static (ISO-9, ISO-1).
4. **Blocked cases.** A contract case whose feature is outside the current scope is still written to the report, with
   `status: "blocked"`, `pass: false` and `blockedBy`. The report's top-level `gate` is `pass` only when there are no
   failed and no blocked cases.
5. **Report.** `artifacts/e2e-persistence-report.json` holds, per case: id, contract clause, kind, steps, request,
   expected, actual, status and pass. It also records the commit SHA, component versions and a fingerprint of the
   normalized cases.
   - Volatile values are replaced with stable placeholders before recording: generated ids, timestamps. Two runs on
     the same commit must therefore produce identical `cases`, and CI runs the suite twice and diffs them.
   - The report is secret-scanned before it is written, and again by CI before upload. It must contain no DB URL, no
     DB user/password from the environment and no planted secret.

What the E2E suite stubs, and why:

- **Authenticator (in-process HTTP only).** It reads `X-E2E-User`, because user authentication does not exist yet. The
  tenant is fixed per scenario and never read from the request. The binary runs in its real local mode.

No unit tests are added for persistence.

## Coverage of S-DB-1 … S-DB-14

| Case | How |
|---|---|
| S-DB-1 | static, ISO-9 |
| S-DB-2 | E2E cross-tenant over HTTP (tasks, assistants), plus ISO-10 |
| S-DB-3 | E2E through the binary: create a task and an assistant, project an event through `runtime_outbox`, restart, read back |
| S-DB-4 | ISO-11 (schema) |
| S-DB-6 | ISO-12 (migrations) |
| S-DB-8 | E2E through the binary |
| S-DB-9 | E2E through the binary (DB connect and migrate failures) and over HTTP (runtime storage failure) |
| S-DB-11 | (a) static ISO-1; (b) E2E + ISO-2 |
| S-DB-12 | E2E (FM-63): blob ingest on the internal listener; public listener returns 404 |
| S-DB-14 | ISO-18, connected as `orbit_app` |
| ISO-21, ISO-22 | task runtime tables and the `orbit_worker` role |

## Privilege decisions

`orbit_app` gets the least DML that the code paths need. Every exception below has a written reason; a new grant needs a
new line here first.

| Table | orbit_app | Reason for anything beyond SELECT/INSERT |
|---|---|---|
| `tenants` | SELECT | Tenants are created by the ops role `orbit_ops`, which has **SELECT and INSERT only** (no UPDATE), via `deploy/postgres/ensure-tenant.sql` (`INSERT … ON CONFLICT DO NOTHING`). `tenants.id` has `CHECK (id <> '')`. Control only checks at startup that its default tenant exists, and refuses to start if it does not. |
| `personas`, `mcp_connectors` | SELECT, INSERT; **no UPDATE, no DELETE** | No code path updates or deletes these rows. |
| `skill_catalog`, `skill_categories` | SELECT, INSERT, UPDATE | Shared SkillHub catalog, not tenant history and not under RLS. A background sync upserts it. |
| `mcp_market_servers` | SELECT, INSERT, UPDATE, DELETE | Shared plaza display rows, not tenant history and not under RLS. Process start replaces the shipped snapshot: `DELETE` the previous rows, then `INSERT` the new copy. No launch command, hosted URL, or secret is stored. |
| `mcp_market_categories` | SELECT, INSERT, UPDATE | The fourteen plaza labels. Startup upserts them. Rows are not deleted. |
| `mcp_market_details` | SELECT, INSERT, UPDATE, DELETE | Readme and tool list for one plaza card. Startup replaces the whole table with the shipped snapshot. |

The task runtime tables have their own section below.

**No TRUNCATE, REFERENCES or TRIGGER for `orbit_app` on any table.** The migration revokes them explicitly from
`orbit_app` and `PUBLIC`. The reasons:

- TRUNCATE is not subject to RLS, so one statement would remove every tenant's rows.
- TRIGGER would let the app attach code that runs on every tenant's writes.
- REFERENCES is never needed by the app.

## Task runtime tables and `orbit_worker` (migration 00013)

Migration 00013 adds the TaskWorkflow tables from orbit-infra
`docs/architecture` (03 domain model, 09 events, 10 ledger, 12 Step 2). The
control task service reads and writes the task, profile, event, and manifest projections. An UPDATE or DELETE for a later code path
(Projector, reconciliation, GC) needs a new line here first, like every other
grant.

A new login role, **`orbit_worker`**, is what orbit-runtime's activity
worker uses for `orbit_control` (U4, ADR-0012). It is `NOSUPERUSER
NOBYPASSRLS`, owns nothing, and has no privilege on the catalog, so a
compromised worker cannot read personas or connectors. It sets the
tenant with `set_config('app.tenant_id', $1, true)`, the same way control
does (ISO-1 rules apply to its SQL as well).

| Table | RLS | orbit_app | orbit_worker | Reason |
|---|---|---|---|---|
| `agent_profiles` | tenant | SELECT, INSERT | SELECT | Versions are immutable; a change is a new version (11 §2). |
| `tenant_policy` | tenant | SELECT, INSERT, UPDATE (`spec, updated_at`); **no DELETE** | SELECT | One row per tenant: the outermost policy layer (05 §6). It can only be tightened further by a task or a profile, never loosened by them. A worker reads it at the start of each attempt; it never writes it. |
| `sop_definitions` | tenant | SELECT, INSERT; **no UPDATE, no DELETE** | SELECT | An SOP is an ordered list of step names; a version is immutable, a change is a new version, and a task locks `sop_id@version` (06 §2). The `sop_step` activity reads the definition it was started with; it never writes one. |
| `node_type_registry` | **none** | SELECT | SELECT | Global, not tenant data. Seeded by the migration; `team_stage` is disabled in v1 (A20). |
| `tasks` | tenant | SELECT, INSERT, UPDATE (`status, plan_version, budgets, usage, pending_approvals, updated_at`); `policy` is written at creation and never updated | — | The v3 control service creates tasks, applies durable runtime projections, and may repair a projection from the authoritative workflow Query during T4.5 reconciliation. |
| `task_nodes`, `task_approvals` | tenant | SELECT, INSERT | — | Projections written by the Projector in control. Their UPDATE columns are added with the Projector (orbit-infra 12 Step 7). |
| `stage_attempts` | tenant | SELECT, INSERT | SELECT, DELETE | Control owns the projection; the worker maintenance schedule deletes attempts left STARTING or RUNNING for more than 24 hours. |
| `plan_versions`, `task_messages`, `task_events` | tenant | SELECT, INSERT; **no UPDATE, no DELETE** | — | Immutable history, like `events` and `messages`. |
| `runtime_outbox` | **none** | SELECT, UPDATE (`projected_at`) | INSERT, SELECT (`event_id`) | The Projector reads every tenant's rows in one pass (09 §3), so RLS would stall it. Rows carry `tenant_id`, and the Projector writes them into tenant tables under that tenant. The worker only appends. `INSERT … ON CONFLICT (event_id) DO NOTHING` (the idempotent publish, 04 §3) needs SELECT on the conflict column, so the worker may read `event_id` and nothing else; it still cannot read a body or another tenant's task id. Cleanup after 7 days is a later grant. |
| `idempotency_ledger` | tenant | SELECT, INSERT, UPDATE (`status, result_ref, owner, last_seen`) (migration 00017); **no DELETE** | SELECT, INSERT, UPDATE (`status, result_ref, owner, last_seen`) | The worker moves a side effect from `started` to `succeeded` or `failed_permanent` (10 §1). Control keeps API commands (`scope = 'api_command'`, key `tenant/task/command_id`) in the same table: it INSERTs `started` with the request hash and its instance id as `owner`, writes the response into `result_ref` and sets `succeeded`, and on a failed orchestrator call sets `owner` to NULL so the same `command_id` can be retried. A `started` row whose `owner` is NULL, or whose `last_seen` is older than the lease, is taken over by the next request (UPDATE of `owner, last_seen` guarded by that condition). `key`, `request_hash`, `scope`, `tenant_id` and `first_seen` are never updated. The grant is column-level and cannot be limited to one scope, so the scope filter is in the code (ISO-24). Rows are never deleted. |
| `checkpoints` | tenant | SELECT | SELECT, INSERT, UPDATE (`committed_in_history`), DELETE | The commit protocol (08 §3) marks a checkpoint once workflow history holds its ref; the maintenance GC deletes stale uncommitted rows and their blobs. |
| `artifact_manifests` | tenant | SELECT, INSERT | SELECT, INSERT; **no UPDATE, no DELETE** | The control Projector creates the immutable projection from `artifact.manifest_created`; workers may also write the manifest before the event arrives (03 §9). |
| `workspace_leases` | tenant | SELECT | SELECT, INSERT, UPDATE (`sandbox_id, expires_at, released_at`) | Heartbeats renew a lease; release and the reaper set `released_at` (08 §1). A partial unique index allows one live write lease per workspace key. `backend` is `docker` or `opensandbox` in production; `local` is the filesystem backend the dev stack and the acceptance E2E run with. |

`orbit_worker` also gets no TRUNCATE, REFERENCES or TRIGGER anywhere, for the
reasons listed above for `orbit_app`.

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
- **FM-4.** A `SECURITY DEFINER` function is added. The allowlist is empty. Every definer function runs with its owner's rights, and this
  one would bypass RLS without review.

Why HTTP cannot catch these: each is a code-shape property. It only shows up
through HTTP under a specific pool interleaving or a malicious input, and
never reliably. Blind spots: SQL assembled from non-literal variables, and
statements run by superusers outside the repo.

### ISO-2 — S-DB-11 (b): GUC does not survive on a pooled connection

The server runs with a pool of one connection. Request A (HTTP) is served
on it. Then a raw query on the same backend (same `pg_backend_pid`) must
see an empty `app.tenant_id` and 0 rows. This also holds after a
request whose transaction rolled back (HTTP 404 for a task that does not exist).

- **FM-6.** The repository sets the tenant outside a transaction, or the
  transaction commits on one path and leaves the GUC set on another
  (error / rollback path).
- **FM-7.** The pool's connection reset does not clear custom GUCs. FM-1/FM-3
  then leak even when the code shape looks right.

Why HTTP cannot catch these: HTTP only shows the effect when the next
request happens to be for a different tenant without its own
`set_config`. The leak itself is connection state.

### ISO-9 — S-DB-1: every repository method is tenant-scoped (static)

The check parses `internal/store` with the Go AST.

- The `store.Repository` interface and every exported method of
  `pgstore.Store` and `memstore.Store` must take a `tenantID` parameter and
  use it in the body.
- Every SQL literal in `pgstore` that reads or writes a [T] table must
  contain `tenant_id`.
- The allowlist holds `Close` (lifecycle, touches no table), the runtime outbox methods (cross-tenant by design) and
  the task projection methods, which take a tenant-scoped `Principal`. Allowlist changes need Sentinel review.

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

Every [T] table (task runtime, SOP, policy and catalog) is seeded for tenants A and B (as the owner). Then:

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

### ISO-11 — S-DB-4: no IdP tokens or secrets can be stored (schema)

No column in `public` has a name matching token, secret, password, refresh, access, cookie, credential or api_key.

- **FM-32.** A column for IdP tokens (refresh/access/id token) or secrets is added. Control must not store them.

Why HTTP cannot catch this: no code path writes such a column until an auth layer exists. The schema is the guard.

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

### ISO-18 — S-DB-14: the app role cannot escape RLS (catalog self-check)

Connected **as `orbit_app`** (the credentials control runs with), query
`pg_class` / `pg_roles` / `pg_has_role` and assert all of the following:

- `current_user` is `orbit_app`.
- `orbit_app` owns no table in `public`.
- `orbit_app` has neither BYPASSRLS nor SUPERUSER.
- Every table in `public` with RLS enabled also has FORCE ROW LEVEL
  SECURITY. The check also requires that every [T] table has RLS, so it
  cannot pass vacuously.
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

### ISO-21 — task runtime tables: tenant isolation and immutable history

The migration creates the rows below as the owner for two tenants, then
connects as `orbit_app` and as `orbit_worker`:

- every tenant table from migration 00013 has RLS **and** FORCE RLS, and
  `runtime_outbox` and `node_type_registry` are the only new tables without
  RLS;
- with tenant A set, each role reads only tenant A's rows from every tenant
  table it may SELECT; with no tenant set it reads none;
- `UPDATE` and `DELETE` on `plan_versions`, `task_messages`, `task_events`
  and `artifact_manifests` fail with `42501` for both roles, and the owner
  reads the old value;
- a second live write lease for the same workspace key fails with `23505`;
- registry version 1 has six node types and `team_stage` is disabled.

- **FM-77.** A new task table is created without RLS or without FORCE RLS.
  Control or the worker then reads another tenant's tasks, events or
  checkpoints with a plain query.
- **FM-78.** Task history (a committed plan version, a user message, a
  durable event, an artifact manifest) is rewritten or deleted. Replay,
  reconciliation and the audit trail then disagree with what happened.
- **FM-79.** Two attempts hold a write lease on the same task workspace at
  once. They write the same sandbox concurrently and the head snapshot no
  longer matches either attempt (orbit-infra 08 §1, serial writes).
- **FM-80.** `team_stage` is enabled in registry version 1, so an agent can
  create a Team node before phase 2 has its bounded-stage design (A20).
- **FM-81.** `runtime_outbox` gets RLS. The Projector reads it without a
  tenant, sees no rows, and every task's events stop without an error.

Why HTTP cannot catch these: no API serves these tables yet, and the
properties are about roles and constraints rather than a request.

### ISO-22 — `orbit_worker` has only its grants

Connected **as `orbit_worker`**:

- `current_user` is `orbit_worker`; it has neither SUPERUSER nor BYPASSRLS,
  owns no table, and is not a member of a role that does;
- it has no TRUNCATE, REFERENCES or TRIGGER on any `public` table;
- it has no privilege at all on the catalog tables and on the control-only
  task tables (`tasks`, `task_nodes`, `task_approvals`,
  `plan_versions`, `task_messages`, `task_events`);
- an UPDATE of a column outside its grant on `idempotency_ledger`,
  `checkpoints` or `workspace_leases` fails with `42501`.

- **FM-82.** `orbit_worker` can read the catalog or control's projections. A
  worker compromised through a tool or sandbox escape then reads personas,
  MCP connector settings or other tenants' tasks.
- **FM-83.** `orbit_worker` has SUPERUSER, BYPASSRLS, TRUNCATE or TRIGGER,
  or owns a table. One bad statement then crosses every tenant.
- **FM-84.** `orbit_worker` can UPDATE columns no design path writes (for
  example a ledger key, a checkpoint's blob ref, a lease's holder), so a
  retried activity can rewrite what another attempt recorded.

Why HTTP cannot catch these: the worker has no HTTP surface in control; the
properties are privileges.

### ISO-23 — S-DB-12: artifact blob ingest

- **FM-63.** `POST /internal/artifact-blobs?tenantId=` is served only on the internal listener. A missing tenant is
  **400**; a tenant that is a path (`..`, a separator) is **404**. The path is `{artifactDir}/{tenant}/{sha256}` using the
  sha256 control computed. A missing or ill-formed `X-Content-Digest` is **400**. A mismatch is **422** and the temp
  file is removed. A body over `ORBIT_ARTIFACT_MAX_BYTES` is **413**, counted while streaming, and leaves no temp file.
  On the public listener every `/internal/*` path, including `/internal/events` and `/internal/artifact-blobs`, is
  **404**.

### ISO-24 — API command ledger: what `orbit_app` may change

Connected as `orbit_app` with a tenant set, on rows of `idempotency_ledger` seeded by the owner:

- UPDATE of `key`, `request_hash`, `scope`, `tenant_id` or `first_seen` fails with `42501`;
- DELETE fails with `42501`;
- UPDATE of `status`, `result_ref`, `owner` and `last_seen` works (the columns control's command claim, completion,
  release and takeover write).

- **FM-85.** `orbit_app` can rewrite `request_hash` (or `key`) of a ledger row. A different request body then replays
  the first command's stored result instead of returning 409 (A7), or a row is moved onto another command's key.
- **FM-86.** A control process that dies after inserting an `api_command` row as `started` leaves it behind, and the
  same `command_id` returns IN_PROGRESS forever. The row carries an owner and a lease (`last_seen`); once the lease has
  passed, or the owner released the row after a failed orchestrator call, the next request takes it over and sends the
  Update again (the Update ID is the `command_id`, so the workflow deduplicates it).

Why HTTP cannot catch FM-85: the API never exposes which columns the role could write. FM-86 is covered end to end (two
`Service` instances on one database, one abandoned mid-command), not by an isolated check.

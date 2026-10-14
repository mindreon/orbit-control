# Contract v2

JSON Schema (2020-12) files in `schema/` are the source of truth for everything that crosses a process or language boundary between orbit-control (Go) and orbit-runtime (Python): Temporal activity inputs and outputs, the run event envelope, the run lock, the control internal API, and the bundle manifest files. orbit-runtime keeps a byte-identical copy in `schema/v2`.

| File | Types |
| --- | --- |
| `segment_input.json`, `segment_result.json` | `SegmentInput`, `SegmentResult` |
| `interaction.json` | `InteractionRequest`, `Decision` |
| `run_event.json` | `RunEvent` and one data type per event type |
| `lock.json` | `RunLock` |
| `sandbox.json` | sandbox lifecycle activity inputs and results |
| `team_updates.json` | team workflow Update inputs and outputs, `TeamBoard` |
| `kernel_api.json` | bodies of `/internal/v1/*` |
| `asset_check.json` | WP5 asset-check inputs, findings and reports |
| `registry.json` | shared Temporal names and type bindings |
| `common.json` | `EffectClass`, `Usage`, `Artifact` |
| `agent_manifest.json`, `skills_manifest.json`, `mcp_manifest.json`, `team_manifest.json` | `agent.json`, `skills.json`, `mcp.json`, `team.json` |

Rules for schema files:

- One `$id` per file. Types live in `$defs`; `x-exports` lists the ones other code builds or parses directly, and each of them needs a valid example. The rest are parts of those.
- Type names are unique across all files, because the generated Go types share one package.
- Every object sets `additionalProperties: false`; every property, definition and file has a `description`.
- Conditions between fields (a status that requires a member, an event type that selects `data`) are `if/then` under `allOf`. The generators ignore them; the tests validate against the schemas.

`examples/<Type>.<case>.json` must validate against the type; `examples/invalid/<Type>.<case>.json` must not. Every exported type has a valid example, and every event type has one.

Changing the contract: edit the schema and the examples, run `scripts/gen-contracts.sh`, run `go test ./internal/contract/...`, then in orbit-runtime run `scripts/sync-contracts.sh ../orbit-control`. Commit both repositories in the same round. `scripts/check-contracts-gen.sh` fails when the generated Go file is stale.

WP0b wire rules:

- `v` is required and currently 1 for segment inputs, results and locks. Object
  schemas mark `x-extra: ignore` for consumers and `forbid` for authored manifests;
  Python generation follows this annotation. Boundary `Validate(kind, raw)` still
  validates the exact published schema, including formats and conditional rules.
- Arrays that may be empty are optional. Go omits empty slices; Python uses an
  independent `default_factory=list`. Nonempty arrays remain constrained.
- Every cost is an integer field ending in `_millicredits`, capped at 2^53-1.
  `Usage.cost_millicredits` estimates a model's segment cost; the control ledger is
  authoritative. Model pricing rounds each token category up per million tokens.
- `x-sensitive` fields generate transparent Go `Secret` strings and Python
  `Field(repr=False)`. Go formatting and nested slog values redact them; wire JSON
  deliberately preserves them. Capability/credential responses never enter history.
- Worker service credentials exchange `TokenRequest` for a five-minute capability
  at `POST /internal/v1/tokens`; sandbox purpose additionally requires a lease.
  The control handler must check running state/current uncommitted seq on issue
  and on every capability use. Segment input contains no capability. Production
  uses an internal listener and mTLS (WP6 D6: R1 service credential, R2 mTLS).
- Members and mounts use zero-based indexes into `RunLock.entries`. A single agent
  has exactly one member named `main`. Team votes use any-reject veto semantics.
- Object and bundle digest algorithms are specified in `lock.json`; paths sort by
  UTF-8 bytes and file hashes always use `sha256:<64 lowercase hex>` on the wire.
  Argument patterns use the Go `path.Match` grammar. `fixtures/digest.json` includes
  RFC 8785 Appendix B vectors; `fixtures/glob.json` includes syntax errors.
- `registry.json` registers cross-language queues, Activity input/output bindings,
  team Updates and application errors. Go-only Workflow APIs stay outside it (D2).
  Sandbox lifecycle outputs and asset-check bindings are explicit.

`fixtures/go-wire.json` is emitted by Go for every exported type: zero values,
minimal valid constructions, and all example round trips. Most zero values must
fail validation because required identities/versions are absent; the fixture
records that expectation, while Python parses and schema-validates every valid
payload. After intentional changes, run:

```sh
ORBIT_UPDATE_CONTRACT_FIXTURES=1 go test ./internal/contract -run TestGoWireFixtures
```

Ordinary Go tests reject stale fixture bytes. Copy schemas, examples **and
fixtures** to runtime, whose tests consume these bytes with pydantic and the schema
validator. Runtime's `to_json` uses aliases, `exclude_none=True` and omits defaults;
this prevents default empty arrays from violating conditional manifest forms.

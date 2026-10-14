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
| `common.json` | `EffectClass`, `Usage`, `Artifact` |
| `agent_manifest.json`, `skills_manifest.json`, `mcp_manifest.json`, `team_manifest.json` | `agent.json`, `skills.json`, `mcp.json`, `team.json` |

Rules for schema files:

- One `$id` per file. Types live in `$defs`; `x-exports` lists the ones other code builds or parses directly, and each of them needs a valid example. The rest are parts of those.
- Type names are unique across all files, because the generated Go types share one package.
- Every object sets `additionalProperties: false`; every property, definition and file has a `description`.
- Conditions between fields (a status that requires a member, an event type that selects `data`) are `if/then` under `allOf`. The generators ignore them; the tests validate against the schemas.

`examples/<Type>.<case>.json` must validate against the type; `examples/invalid/<Type>.<case>.json` must not. Every exported type has a valid example, and every event type has one.

Changing the contract: edit the schema and the examples, run `scripts/gen-contracts.sh`, run `go test ./internal/contract/...`, then in orbit-runtime run `scripts/sync-contracts.sh ../orbit-control`. Commit both repositories in the same round. `scripts/check-contracts-gen.sh` fails when the generated Go file is stale.

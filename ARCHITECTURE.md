# Architecture — orbit-control

W0 process is still a listen-and-`501` skeleton for most resource groups. **W1** implements in-memory rooms, messages, HITL decide, and SSE, calling orbit-worker over HTTP. This file is the **target** boundary map after the dsh runtime choice.

## What this service is

`orbit-control` is Orbit's **only public HTTP and WebSocket API**.

Browsers, CLIs, and third-party clients reach Orbit through this repo. Sibling services orchestrate and execute work; they are not public API surfaces. DeepSeek Harness (dsh) is hosted only on the worker, behind Temporal Activities. It is not a public API and not a GitHub ingress.

## Product surfaces (what the API must eventually express)

| Surface | Public objects | Not here |
| --- | --- | --- |
| **Super single-agent** | Room `kind=solo`, messages, personas, WS events, HITL approvals | Agent loop |
| **Multi-agent** | Room `kind=collab`, per-room agent catalog on WS | Temporal child per subagent (V1) |
| **Cloud Agent** | `/v1/cloud-agents` jobs, cancel, approval reuse | Vendor compute SDKs; dsh webhooks |

## Sibling map

```
                    ┌─────────────────┐
   humans / CLIs ──►│   orbit-web     │
                    └────────┬────────┘
                             │ HTTP + WS only
                             ▼
                    ┌─────────────────┐
                    │  orbit-control  │  ◄── sole public API
                    │  (this repo)    │     secret ciphertext
                    └────────┬────────┘
             Temporal start/Signal/Update
             + internal event ingest (not public)
              ┌──────────────┴──────────────┐
              ▼                             ▼
     ┌─────────────────┐           ┌─────────────────┐
     │   orbit-orch    │           │  orbit-worker   │
     │  Room / Cloud   │           │  dsh ACP host   │
     │  workflows      │           │                 │
     └─────────────────┘           └─────────────────┘
```

| Repo | Owns | Must not own |
| --- | --- | --- |
| **orbit-control** (this repo) | Public HTTP/WS, tenants, accounts, personas, rooms, approvals, **tenant secret ciphertext**, billing, **internal event ingest from worker** | Worker sandboxes, Temporal workflow bodies, UI rendering, dsh processes |
| [orbit-web](https://github.com/mindreon/orbit-web) | Browser UI | Direct calls to orch/worker/dsh; tenant secret plaintext; a second public API |
| [orbit-orch](https://github.com/mindreon/orbit-orch) | Internal orchestration **contracts** and workflow scheduling | Public HTTP/WS; tenant secret storage; dsh/Pi/LLM |
| [orbit-worker](https://github.com/mindreon/orbit-worker) | Ephemeral job execution; `dsh --profile acp` | Public HTTP/WS; tenant secret persistence |

## Hard boundaries

### 1. Secrets are encrypted only here

- Tenant secrets (API keys, tokens, BYOK material) are **written, encrypted, and stored only in orbit-control**.
- `orbit-orch` and `orbit-worker` **never own** tenant secrets: no secret tables, no long-lived copies, no encryption keys of record.
- Workers may receive a **short-lived, scoped grant** from control at session/job start. That grant is not a secret store. The worker passes **credential references** (env names) into dsh config and values only via child environment.
- `orbit-web` never sees secret plaintext. It calls control; control returns metadata (name, last-four, status), not values.

W0 does not implement encryption. This rule is the contract for later waves.

### 2. Web only talks to control

- `orbit-web` uses **only** this repo's HTTP and WebSocket API ([docs/openapi.yaml](./docs/openapi.yaml)).
- Web must not open sockets to orch, worker, or dsh, and must not embed those URLs.
- If the UI needs room events, approvals, agent catalog, or job status, control fans those out over the public WS/HTTP surface.

### 3. Orch, worker, and dsh are not public APIs

- Orchestration contracts live in **orbit-orch** (internal). Control is a consumer of those contracts, not a duplicate public schema.
- Worker APIs are internal. External clients do not address workers.
- dsh `web` / SDK / ACP ports must never be published. Adding a public route in orch or worker is a boundary violation. Put it in this OpenAPI instead.

### 4. HITL records live here; the park lives in Temporal

- `POST /v1/approvals/{id}/decide` is the public HITL verb.
- Control maps decide → Temporal Update `approve` or Signal `abort` on the Room / CloudAgentJob workflow.
- The worker does not auto-answer dsh `session/request_permission` on default presets.
- `401` / `403` bodies in the OpenAPI stub are reserved for later **Sentinel** authz tests. Do not silently change those shapes.

### 5. Live events: worker → control, not through the workflow

Token streams and tool lifecycle are too chatty for Temporal.

A later wave adds an **internal** ingest (service auth, not listed as a public path in OpenAPI) that accepts **Orbit events** only (see OpenAPI `OrbitEvent`). Control persists what the UI needs and fans `/ws`. Raw ACP `session/update` frames are rejected.

## Contract links

| Contract | Home | How control uses it |
| --- | --- | --- |
| Public HTTP/WS | [docs/openapi.yaml](./docs/openapi.yaml) in **this repo** | Source of truth for web and Sentinel |
| Orchestration / workflow contracts | [orbit-orch](https://github.com/mindreon/orbit-orch) | Control starts / Signals / Updates work; does not re-own the orch schema |
| Agent runtime choice | [orbit-orch tech-selection](https://github.com/mindreon/orbit-orch/blob/main/docs/tech-selection.md) | dsh on the worker; this API stays runtime-agnostic |
| Web client | [orbit-web](https://github.com/mindreon/orbit-web) | Generated or hand-written client of this OpenAPI only |
| Worker execution | [orbit-worker](https://github.com/mindreon/orbit-worker) | Receives jobs + scoped grants; reports status + events inward |

## W0 intentionally missing

No OAuth, no database, no LLM, no Temporal client, no KMS, no dsh. The process in `cmd/orbit-control` is a listen-and-`501` skeleton so the package layout and contract file exist.

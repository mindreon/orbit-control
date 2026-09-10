# Architecture — orbit-control

W0 document. Boundaries and ownership only. No runtime topology, no data model, no workflow graphs.

## What this service is

`orbit-control` is Orbit's **only public HTTP and WebSocket API**.

Browsers, CLIs, and third-party clients reach Orbit through this repo. Sibling services exist to orchestrate and execute work; they are not public API surfaces.

## Sibling map

```
                    ┌─────────────────┐
   humans / CLIs ──►│   orbit-web     │
                    └────────┬────────┘
                             │ HTTP + WS only
                             ▼
                    ┌─────────────────┐
                    │  orbit-control  │  ◄── sole public API
                    │  (this repo)    │
                    └────────┬────────┘
                             │ internal contracts (not public HTTP)
              ┌──────────────┴──────────────┐
              ▼                             ▼
     ┌─────────────────┐           ┌─────────────────┐
     │   orbit-orch    │           │  orbit-worker   │
     │  (workflows)    │           │  (execution)    │
     └─────────────────┘           └─────────────────┘
```

| Repo | Owns | Must not own |
| --- | --- | --- |
| **orbit-control** (this repo) | Public HTTP/WS, tenants, accounts, personas, rooms, approvals, **tenant secret ciphertext**, billing | Worker sandboxes, Temporal workflows, UI rendering |
| [orbit-web](https://github.com/mindreon/orbit-web) | Browser UI | Direct calls to orch/worker; tenant secret plaintext; a second public API |
| [orbit-orch](https://github.com/mindreon/orbit-orch) | Internal orchestration **contracts** and workflow scheduling | Public HTTP/WS; tenant secret storage |
| [orbit-worker](https://github.com/mindreon/orbit-worker) | Ephemeral job execution | Public HTTP/WS; tenant secret persistence |

## Hard boundaries

### 1. Secrets are encrypted only here

- Tenant secrets (API keys, tokens, BYOK material) are **written, encrypted, and stored only in orbit-control**.
- `orbit-orch` and `orbit-worker` **never own** tenant secrets: no secret tables, no long-lived copies, no encryption keys of record.
- Workers may receive a **short-lived, scoped grant** from control at job start. That grant is not a secret store.
- `orbit-web` never sees secret plaintext. It calls control; control returns metadata (name, last-four, status), not values.

W0 does not implement encryption. This rule is the contract for later waves.

### 2. Web only talks to control

- `orbit-web` uses **only** this repo's HTTP and WebSocket API ([docs/openapi.yaml](./docs/openapi.yaml)).
- Web must not open sockets to orch or worker, and must not embed orch/worker URLs.
- If the UI needs room events, approvals, or agent status, control fans those out over the public WS/HTTP surface.

### 3. Orch and worker are not public APIs

- Orchestration contracts live in **orbit-orch** (internal). Control is a consumer of those contracts, not a duplicate public schema.
- Worker APIs are internal. External clients do not address workers.
- Adding a public route in orch or worker is a boundary violation. Put it in this OpenAPI instead.

## Contract links

| Contract | Home | How control uses it |
| --- | --- | --- |
| Public HTTP/WS | [docs/openapi.yaml](./docs/openapi.yaml) in **this repo** | Source of truth for web and Sentinel |
| Orchestration / workflow contracts | [orbit-orch](https://github.com/mindreon/orbit-orch) | Control starts / observes work; does not re-own the orch schema |
| Web client | [orbit-web](https://github.com/mindreon/orbit-web) | Generated or hand-written client of this OpenAPI only |
| Worker execution | [orbit-worker](https://github.com/mindreon/orbit-worker) | Receives jobs + scoped grants; reports status inward |

`401` / `403` bodies in the OpenAPI stub are reserved for later **Sentinel** authz tests. Do not silently change those shapes.

## W0 intentionally missing

No OAuth, no database, no LLM, no Temporal client, no KMS. The process in `cmd/orbit-control` is a listen-and-`501` skeleton so the package layout and contract file exist.

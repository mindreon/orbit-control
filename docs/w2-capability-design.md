# W2 能力设计：安全、组合、审计、多 Agent

> **不要按本文的 dsh / ACP / Cordis 组装施工。** 信任边界仍然有效。文中那套 dsh、ACP、Cordis 拼装已经过时，不能按原文实现。

> 结论先行：**需要做轻量架构与产品设计**，否则这五项会互相踩边界。
> 设计目标是把信任边界写清楚，再实现可验证切片；**验收用自动化测试，不用视频**。

## 1. 产品问题（为什么要做）

W1 已证明：Rooms → HITL → steer → 标准事件 在 dsh/ACP 上能跑通。
但还不能进共享环境：

1. 内部 HTTP 无服务身份；
2. worker/dsh 子进程继承整份宿主环境；
3. Persona / MCP / Grant 只有文档，没有可执行组合；
4. 审计与会话状态掉进程即丢；
5. 文件沙箱 ≠ 网络/资源隔离；
6. Multi-agent / Cloud Agent 仍是空壳；
7. UI 验收不能依赖人工录屏。

## 2. 架构原则（dsh，不是 Pi）

```text
orbit-web ──HTTP/SSE──► orbit-control ──Activity/Signal──► orbit-orch
                              │                                 │
                              │ internal auth                   │
                              ▼                                 ▼
                     short-lived grants                 Activity calls
                              │                                 │
                              └──────────► orbit-worker ────────┘
                                              │
                                              │ allowlisted env
                                              │ session-private Cordis patch
                                              │ optional bwrap
                                              ▼
                                         dsh --profile acp
```

硬边界：

| 规则 | 含义 |
| --- | --- |
| Secret ownership | 明文密钥只短暂出现在 control mint 与 worker 子进程 env；永不进 patch / 日志 / OpenAPI |
| Composition ownership | control 存 Persona/MCP/Grant **元数据**；worker 编译 Cordis patch |
| Isolation ownership | dsh `workspace-write` = 文件效果；bwrap/cgroup/egress = infra/execution world |
| Public surface | web 只打 control；`/internal/*` 必须服务鉴权 |
| Protocol | ACP `mcpServers` 仍为空；企业 MCP 走 worker Cordis composition |

## 3. 产品切片与验收标准

### 3.1 服务间认证 + 密钥环境最小化

**产品**：共享环境中，未持有内部 token 的调用方不能驱动 worker，也不能灌事件。

**实现**：

- 共享密钥 `ORBIT_INTERNAL_TOKEN`
- Header：`Authorization: Bearer <token>`
- 覆盖：control `POST /internal/events`、worker `POST /internal/activities/*`、control→worker 出站
- 未设置 token：仅允许 loopback 开发模式（显式记录）
- dsh 子进程 env = allowlist + `DSH_PERMISSION_MODE` + grant/LLM 所需 env 名对应的值

**验收（自动化）**：无 token 调 internal → 401；有 token → 200；子进程 env 不含无关宿主密钥。

### 3.2 Persona / MCP / Grant → Cordis patch

**产品**：创建 Room 可选 Persona；Grant 只传引用；MCP 以审核过的 connector 绑定进入执行快照。

**对象**：

```text
Persona { id, name, instructions, mcpConnectorIds[], toolAllowlist? }
McpConnector { id, name, transport, command|url, envRefs[] }
Grant { id, expiresAt, env: { NAME: value } }  // value 只在 mint/resolve 短暂存在
ExecutionSnapshot { personaId?, permissionPreset, mcpConnectorIds[], grantId?, isolation }
```

**Worker**：写 `$SESSION/orbit.cordis.patch.yml`（无密文）+ 子进程 env（有密文）。

**验收**：patch 含 persona/MCP 引用且无密钥；子进程能读到 grant env；ACP 仍传 `mcpServers: []`。

### 3.3 持久化审计 + 会话恢复

**产品**：重启 control 后仍能查房间活动；worker 重启后可用 checkpoint 报告会话丢失并让 orch 重开。

**W2 存储**（无新 DB 驱动）：`ORBIT_DATA_DIR` 下 JSON/JSONL。

```text
$data/audit/<roomId>.jsonl
$data/rooms/<roomId>.json
$data/sessions/<sessionId>.json   # worker checkpoint：roomId、cwd、preset、pid、updatedAt
```

**验收**：写入活动 → 重启 control 进程 → `GET /v1/rooms/{id}/activity` 仍返回；worker checkpoint 可读。

### 3.4 bwrap / 网络 / 资源强隔离

**产品**：Room runtime 明示隔离级别；Cloud Agent 默认更强。

| 级别 | 含义 |
| --- | --- |
| `process` | 仅独立进程 + dsh 文件策略（W1） |
| `bwrap` | bubblewrap：独立 mount namespace、可选 `--unshare-net`、绑定 workspace |
| `cgroup` | CPU/内存/进程数上限（infra 清单，W2.1） |

**验收**：`ORBIT_ISOLATION_MODE=bwrap` 且主机有 `bwrap` 时，子进程经 bwrap 启动；缺失时 fail closed 或显式降级标记为 `process`（由 `ORBIT_ISOLATION_STRICT` 控制）。

### 3.5 Multi-agent + Cloud Agent

**产品**：

- `collab` Room：投影 `agent.started` / `agent.finished`（dsh 子代理目录事件）
- Cloud Agent：`clone → openSession → runTurn* → pushBranch → openPr`，复用 HITL/steer

**验收**：workflow/activity 契约测试；mock 路径可跑通状态机；真实 git 在无凭证时跳过。

### 3.6 自动化 UI/API 测试

**产品工程要求**：每个 PR 用脚本回归，不录视频。

- control：`go test ./...`
- worker：`npm test`
- orch：`npm test`
- web：lint/build + Playwright **无 video** 的 API/UI smoke（或纯 fetch e2e）

## 4. 实施顺序

1. 内部认证 + env allowlist  
2. Persona/MCP/Grant 元数据 + Cordis patch  
3. 文件审计 / session checkpoint  
4. bwrap 启动器 + infra 清单  
5. collab 事件投影 + CloudAgentJob 骨架  
6. 自动化测试加固  

## 5. 非目标（本轮不做）

- 生产 KMS / Postgres / OAuth
- 把 MCP JSON 塞进 ACP `session/new`
- 以 Pi / dsh web 作为公共入口
- 用视频代替自动化验收

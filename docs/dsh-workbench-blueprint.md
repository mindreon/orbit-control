# Orbit 企业 AI 工作台蓝图（dsh）

这份蓝图借鉴常见企业 AI 工作台的分层方式，但不照搬以 Pi Agent
为核心的运行时。Orbit 的 agent kernel 是 **DeepSeek Harness（dsh）**，
只由 `orbit-worker` 通过 **ACP stdio** 托管；浏览器、控制面和编排层只使用
Orbit 自己的契约。

## 1. 目标分层

| 层 | Orbit 中的职责 | 所属仓库 |
| --- | --- | --- |
| 产品入口 | 对话任务、会话历史、审批、成果物、Cloud Agent | `orbit-web` |
| 控制与权限 | tenant、workspace、persona、资源绑定、策略、凭据元数据、审计 | `orbit-control` |
| 持久编排 | Room / Cloud Agent 状态机、重试、超时、HITL 停泊 | `orbit-orch` |
| Agent runtime | dsh session、skill/tool/subagent、ACP 适配、策略快照 | `orbit-worker` |
| 执行资源 | workspace、文件/命令沙箱、网络出口、资源限额、企业连接器 | worker execution world / `orbit-infra` |
| 治理横切面 | 授权、审计、可观测性、告警、配置与发布 | control + infra |

边界中的关键数据流是：

```text
User
  -> Workspace membership
  -> Persona / capability bindings
  -> Tool and data grants
  -> Orbit policy decision (allow / deny / ask)
  -> immutable execution snapshot
  -> RoomWorkflow / CloudAgentJob
  -> orbit-worker
  -> one dsh --profile acp process per session
  -> sandboxed effects + normalized Orbit events
```

`immutable execution snapshot`（不可变执行快照）表示一次 session 启动时已经
确定的 persona、权限预设、模型路由、工具和凭据引用。运行中的后台配置变化
不能悄悄扩大该 session 的权限；需要显式重开或形成可审计的策略变更事件。

## 2. 与 Pi 型参考架构的差异

1. **dsh 不是整个“运行层”**。任务状态、重试和人工审批停泊属于 Temporal；
   dsh 负责 agent loop、工具、subagent 与 session log。
2. **不嵌入 dsh Web/SDK**。Orbit 使用 `dsh --profile acp` 子进程。ACP 类型
   只存在于 worker 的 adapter 内，不能泄漏到公共 OpenAPI。
3. **能力通过 Cordis composition 装配**。Skill、tool、模型和 connector 应
   编译成 worker 私有的 session patch，不应在 control 中直接 import dsh 插件。
4. **策略有两道边界**：
   - control policy：某个用户是否能对某个 workspace/resource 发起操作；
   - dsh permission：具体工具调用是否受沙箱约束、是否需要一次性批准。
5. **ACP 当前不接受动态 `mcpServers`**。企业 MCP 连接不能简单塞进
   `session/new`；worker 必须把已审核的连接器编译到 session-private Cordis
   composition，并只注入凭据引用。
6. **沙箱模式不是完整网络隔离**。dsh 的 `workspace-write` 主要约束文件效果；
   网络出口、CPU/内存/时间和进程隔离仍需要 execution world / infra 承担。

## 3. 当前能力与缺口

状态说明：`可用` 表示已有端到端路径；`部分` 表示已有契约或单层实现；
`缺失` 表示还没有可使用的产品路径。

| 参考能力 | 状态 | 当前事实 | 需要补齐 |
| --- | --- | --- | --- |
| 对话任务 / 历史会话 | 可用 | Room、message、SSE 为内存实现 | 持久化、搜索、归档 |
| HITL 审批 | 可用 | dsh ACP permission → control approval | 超时、批量策略、审批人范围 |
| 任务记录 / 审计 | 部分 | worker 有标准事件，control 仅实时转发 | 有界留存、查询 API、可读时间线 |
| 运行时权限预设 | 部分 | 公共契约有字段，但此前未传到 dsh 子进程 | 创建校验、Temporal 透传、session 快照 |
| 中途 steer / cancel | 部分 | worker 有 Activity，abort 已公开 | 补齐公开 steer 和 Temporal Signal |
| 工作空间 | 缺失 | 只有 worker 的临时 cwd | workspace 对象、成员、资源绑定、文件索引 |
| Persona / Skill | 部分 | persona 是空列表；dsh 有 composition seam | CRUD、版本、发布、执行快照 |
| 企业工具 / MCP | 缺失 | 尚无连接器目录 | 审核、版本、workspace binding、worker 编译 |
| 企业数据 / 知识 | 缺失 | 无 datasource / retrieval 对象 | 连接器、索引、引用与数据权限 |
| 模型管理 | 部分 | worker 可从环境选择一个模型路由 | provider 元数据、配额、workspace policy |
| 凭据管理 | 部分 | 边界已定义，API 仍为空 | control 加密存储、短期 grant、轮换审计 |
| 成果物 | 缺失 | assistant text 之外无 artifact | blob metadata、来源、下载授权、保留策略 |
| 多 Agent | 部分 | `collab` 和事件名已定义 | dsh subagent 事件投影、roster UI、限深策略 |
| Cloud Agent | 部分 | 契约和 Activity 名已预留 | clone/run/push/PR/heartbeat 与隔离环境 |
| 监控告警 | 缺失 | 仅进程日志 | metrics、trace、SLO、告警路由 |
| 系统管理 | 缺失 | 环境变量配置 | 版本化配置、发布、回滚、运行状态 |

## 4. 本轮纵向切片

本轮先完成后续所有企业能力都会依赖的两个基础闭环：

### 4.1 可验证的运行策略

- Room 创建时选择 `solo|collab` 和
  `workspace-write|danger-full-access`。
- control 校验并保存选择；direct HTTP 和 Temporal 两条路径都传给 worker。
- worker 在启动每个 dsh 子进程时设置该 session 专属的
  `DSH_PERMISSION_MODE`，不修改进程级全局环境。
- Room 返回 runtime snapshot，让 UI 明确显示 kernel、协议、隔离边界和权限。
- 未知预设 fail closed，不启动子进程。

### 4.2 可查询的执行轨迹

- worker 发出的事件包含稳定事件 ID、发生时间、room/turn 关联和 dsh runtime
  元数据。
- control 只接受 allowlist 中的标准 Orbit 事件，拒绝原始 ACP 或孤儿事件。
- control 为每个 Room 保留有界的内存时间线，并提供只读查询 API。
- web 把原始 JSON 改为 session、tool、approval、agent 与 usage 的可读时间线。
- steer 从公共 API 到 Temporal Signal / worker Activity 打通并进入审计轨迹。

这不是完整的企业治理，但它先建立了“策略真的生效”和“效果可以追溯”两条
基础不变量。Workspace、Skill、Data、Credential 和 Artifact 之后都应复用同一
execution snapshot 与 audit event 模型，而不是各自再造一套调用链。

## 5. 后续顺序

1. **Workspace + membership + policy binding**：形成真正的租户资源边界。
2. **Persona / Skill / Tool catalog 的版本化快照**：control 管业务对象，worker
   编译 dsh Cordis composition。
3. **Credential grant**：control 加密保存，worker 只接收短期引用和值的子进程
   环境，不记录明文。
4. **Artifact + file metadata**：把聊天输出升级为可引用、可授权、可留存的成果。
5. **Collab roster**：投影 dsh subagent 生命周期，加入深度、工具和预算限制。
6. **Cloud Agent execution world**：clone/push/PR 与 heartbeat，叠加网络和资源
   隔离；不能把 dsh 自带 webhook 当公共入口。
7. **持久化与治理**：数据库、对象存储、审计保留、指标/trace、告警、配置发布。

每一步都必须继续遵守：`orbit-web -> orbit-control` 是唯一公共调用路径；
`orbit-orch` 不 import dsh/Pi/LLM；tenant secret 明文不进入日志、OpenAPI 示例、
Temporal history 或 dsh patch。

# Orbit 合并契约草案 v2.1（P0）

状态：v2.1 **已签字**（Sentinel）；签字后追加 C28（来自 orbit-runtime#3）、C29（术语对齐，只改文档）、C30（产物事件与读取接口，v2.1 按 Sentinel 评审修订）、C31（登录鉴权与任务归属，**草案，未签字**）和 C32（control 持久化存储，已按 Sentinel 评审修订（C32 rev），任务删除改为软删除（C32 rev2），**仍是草案，需要 Celestial 和 Sentinel 签字**）；C33 按 orbit-runtime#4（A1）对齐事件契约；C34 新增 `state_unreadable` 错误码和 decide 投递幂等（F2），需要 Celestial 和 Sentinel 签字，见变更记录（v1 保留在 `/workspace/orbit-contract-draft-v1.md`）　日期：2026-09-26　作者：Celestial（架构）　读者：Forge（实现）、Sentinel（验收）、orbit-web
基线（已核对代码）：orbit-runtime `7b800eb`、orbit-control `846d27e`、orbit-web `c7b8717`、orbit-infra `1abb9a6`。
技术基线：AgentScope 2.0.8（runtime）+ Vite 8（web），见 `orbit-adr-draft.md`。产品范围：`/workspace/fe-study/orbit-product-scope.md`（2026-09-26 已确认）。

写法约定：所有"现状"都带 `repo:path:line`；"变更"用 pydantic 字段级 diff（`+` 新增、`~` 修改）；Sentinel 验收写成 `S-编号`。新增字段都有默认值，旧 payload 照常反序列化（向后兼容），不兼容的地方单独标 **BREAKING**。

---

## 变更记录（v2 + v2.1）

来源缩写：**AS** = AgentScope study `/workspace/agentscope-study/agentscope-orbit-notes.md`（§6.3、§6.7、§7）；**R** = room 内已定事项（Nexus、Sentinel、Forge）；**S** = Sentinel 评审（S-M1..3 是签字阻塞项，S-4..7 为中、低优先级，S-Q 是对待决问题的裁定）；**N** = Nexus 产品规则；**F** = Forge 实现意见。

| # | 变更 | 位置 | 来源 |
|---|---|---|---|
| C1 | approval_request_id 和 question_id 改成由 `room_id+agent_id+turn_id+call_id` 确定性生成的摘要。所有 turn_id 在会话内必须唯一，子 Agent 的循环 turn_id 也要唯一。decide 或 answer 一个过期 id 返回 409。新增验收 S-ID-1 | §2、§3、§6.2、§7 | S-M1 |
| C2 | 子 Agent 权限快照改成"随 room 当前快照刷新"：每次 childRequest/childResponse 交互都带上 room 当前快照；control 发出的每个 Update（包括新增的 `permissionChanged`，在规则新建或撤销时发送）都会让 room 把新快照 signal 给所有活跃子 Agent。新增验收 S-6-3 | §2、§6.1、§8 | S-M2 |
| C3 | 风险声明默认拒绝：每个工具必须显式声明 `orbit_risk`，缺失时注册失败，运行时按 `PERMISSION_DENIED` 处理；CI 枚举所有已注册工具并检查。`ToolRisk` 增加 `sensitive`（见 C6） | §2、§7 | S-M3 |
| C4 | ingest 必须鉴权：只要连接了 Temporal，`ORBIT_INTERNAL_TOKEN` 就必填，不再允许 loopback 放行。decide/answer 之前，control 先用 query 确认 id 在 room workflow 的 pending 列表里，否则返回 409 | §6.2、§7、§3 | S-4 |
| C5 | `argument_pattern` 匹配前先规范化路径，`*` 不跨越 `/`，越出工作区的路径永远不匹配。"总是允许"默认只记当前的精确参数；任意参数 `{}` 需要用户做二次选择（`anyArguments:true`） | §8 | S-5、N2 |
| C6 | destructive 和扣费类工具一律不提供"总是允许"。为了让"总是允许"有适用对象，新增 `sensitive` 风险级（有外部副作用、可逆，停车，可以记住）；`gated_echo` 从 destructive 改成 sensitive | §2、§7、§8 | N2、S-5 |
| C7 | 原待决问题 Q2 升级为硬要求：danger-full-access（BYPASS）下 always_ask 工具仍然必须发 `approval.asked`；如果 AgentScope 绕过了，由 Orbit 包装层拦截。验收 S-Ra-5 放在 PR-A2 | §7、§12、§13 | S-6 |
| C8 | 审批卡参数先脱敏（疑似密钥替换成 `[REDACTED]`，不会让整张卡失败），再截断；前端只按纯文本渲染 | §2、§7、§14 | S-7 |
| C9 | 拒绝语义按产品规则写死：工具不执行，任务不关闭，Agent 收到"用户拒绝"后继续；审批卡变成 `rejected` 且不可再点；"停止"是另一个动作。Approval.status 改为 `pending|allowed|rejected|cancelled` | §7、§14 | N1 |
| C10 | Q4 已裁定：P0 接受 control 重启后规则丢失（丢失的方向是更安全的，会重新询问）；Idempotency key 必须持久化，并在启动时重新加载 | §9、§13 | S-Q4 |
| C11 | Q6 已裁定：P0 允许子 Agent 调 `ask_user`，追问卡上显示发起的子 Agent；room 的 pending 问题改成列表 | §2、§3 | S-Q6 |
| C12 | heartbeat 改成 activity 内的后台定时任务，不依赖模型输出；取消时同时取消 heartbeat 任务和模型请求。新增验收 S-8-4（模型调用阻塞 45 秒不触发超时） | §6.3 | F2 |
| C13 | PR-A 拆成 A1（事件 camelCase、agentId、activity 内工具事件）、A2（快照、闸门、拒绝、ask_user、todo_write）、A3（子 Agent 循环、委派上限、级联取消），依次合并，都向后兼容；PR-B 最后合并。真模型 PR 先合并，A1 在它之上 rebase | §12 | F1、F3 |
| C14 | §10 的模型环境变量改成占位，标注"与真模型 PR 对齐"（变量名由 Forge 给出） | §10 | F3 |
| C15 | 现状补充：worker→ingest 已经带 Bearer token（`orbit_worker/events.py:41`，`main.py:54`），control 也会校验（`handler.go:219`），但 token 未设置时 loopback 可以免鉴权（`internal/internalauth/auth.go:21-28`） | §1 | 核对代码 |
| C16 | **并行停车**：`TurnResult` 改为返回列表 `approvals[]`、`externals[]`，以 `agent.state.get_awaiting_tool_calls()` 为准。每个审批按自己的 id 单独决定，绝不"一个结论作用于全部"：今天 `_confirm_event` 把同一个 outcome 作用到所有 ASKING 调用上（`runtime.py:331-345`），审批只取了 `tool_calls[0]`（`:265`），external 被后面的事件覆盖（`:272-278`）。只回填一部分时必须重新发事件并继续停车，不能判成 `continue`。新增 §2.2、S-PP-1..3 | §1、§2、§2.2 | AS §6.3-1、§6.7、§7.4 |
| C17 | **取消**：`_agent()` 显式设置 `ReActConfig(interruption_raise_cancelled_error=True)`（名字已在调研 §6.3-4、§7.5 核对），让 Temporal 的取消表现为 `CancelledError`，而不是正常完成；配合 10 秒后台 heartbeat；re-raise 之前保存 interrupted 状态。S-8-5 增加判据：Temporal history 里该 activity 是 Canceled，不是 Completed | §6.3、§12 A3 | AS §6.3-4、§7.5；R（Sentinel） |
| C18 | 从 `ModelCallEndEvent` 发 `usage` 事件（model、各类 token、latencyMs、agentPath），放在 A1 | §2.1、§12 A1 | AS §6.3-7、§6.7 |
| C19 | 委派只用 Temporal 子工作流，**禁止使用 `agentscope.app` 的 Team 或调度器**：它的模板允许 worker 权限比 leader 宽，而且会引入第二套持久化和调度。子 Agent 权限 = 父 Agent、任务（room）与模板三者的**交集**（preset 取更严的，allow 规则取交集，deny 规则取并集）；运行中仍然随 room 刷新（C2），每次刷新都重新取交集 | §4.3、§6.1 | AS §6.4、§7.10-7 |
| C20 | 事件：`AgentRef` 增加 `agent_path`（例如 `main/ag-x/ag-y`，parent 指针不能直接给出完整路径）；新增 `assistant.delta` 流式事件（只推 SSE，不入库）；control 发的 `approval.decided` 更名为 `approval.resolved`（带 `outcome, decidedBy`）；`tool.call/tool.result` 增加 `state, argsPreview`。`plan.updated`、`sop.step.*` 推迟到 §15 SOP（P1，不在 A1–A3） | §2、§2.1、§6、§7、§15 | AS §6.7；R（Nexus：SOP 属于 P1，场景 5） |
| C21 | 执行粒度：每个 turn 一个 activity；只有有副作用、需要审批、长时间运行或跨 agent 的工具做成 external；工具副作用使用从 call_id 派生的幂等键；检查点放 blob store，payload 里只带引用；长 room 用 continue-as-new；LLM、sandbox、gateway 分开 task queue。新增 §2.3 | §2.3、§12 | AS §7.2–7.9 |
| C22 | 产品规则：每个调用一张审批卡，逐个决定；**P0 不提供"全部允许"**（API 也不接受批量决定） | §2.2、§14 | R（Nexus） |
| C23 | S-Rb-8（High）：`DELETE /v1/approval-rules/{id}` 必须同时删除 `approval-rules/<id>.json`；验收：撤销 → 重启 control → GET 里没有这条规则 → gated_echo 再次询问 | §8 | R（Sentinel） |
| C24 | A2 新增验收 S-Ra-9、S-Ra-10、S-Ra-11（两次 gated_echo → 两张卡；只批第一张 → 只执行第一次；一允许一拒绝 → 各自生效）；worker 拒绝同一 turn 内重复的 call_id | §2.2、§7、§12 | R（Sentinel） |
| C25 | §7 表格拆分：**内置工具**没有 `orbit_risk` → 注册失败，运行时拒绝（CI 检查）；**动态 MCP 工具**没有标注 → 按 destructive + always_ask 处理（会停车，不提供"总是允许"）。任何人都不能放宽默认拒绝检查 | §7、§13 | R（Sentinel、Nexus） |
| C26 | 撤销规则从下一轮生效（§8）；撤销成功后前端必须显示"从下一轮生效" | §8、§14 | R（Nexus） |
| C27 | PR-B 验收：不带 internal token 调 `/v1/grants` 返回 401（原待决问题 Q3 裁定为 internal-only） | §11、§12、§13 | R（Sentinel） |
| C28 | **签字后追加（来自 orbit-runtime#3）**：真模型配置与模型失败语义。§10 的占位环境变量换成真实名字（`ORBIT_MODEL_MODE/BASE_URL/API_KEY/NAME/TIMEOUT_SECONDS`）；real 模式配置缺失时 worker 启动即退出；`TurnStatus` 增加 `failed`（state_version 不变，不回退 mock）；新增 `turn.failed` 事件（字段包在 `failure` 对象里，同时发一条人类可读的 `session.status "turn failed: …"`；对齐 #3 head `3ec4204`），`message` 必须是 Nexus 固定文案之一；TurnResult 带 `errorCode/retryable`，TurnResult、事件和 Update 返回都带 `modelMode/modelName`；失败卡规则（上线规则：A2 幂等键合并并且 S-Ra-10 签字之前，不显示"重新批准"，只显示"这一步没完成，请停止后重新发起"和"停止"按钮）；`turn.failed` 的 `failure.message` 必须是固定文案之一，不能是 provider 原文；P0 不自动重试模型错误；已知限制交给 A2 的幂等键处理；PR-B 上线前 real 模式只能用于 dev。验收 S-RM-1 | §2、§2.1、§10、§12、§14 | orbit-runtime#3（签字后追加） |
| C29 | 术语对齐（**只改文档**，签字条件不变）：层级改为 租户/组织 > 项目 > 任务组（可选，`taskGroup` 只在文档里预留，不在 A1–A3 范围内）> 任务（room = 任务 = 一个对话 + 一个工作空间）；废除「空间 = 协作容器」，团队空间只是资料库里的文档概念；不再区分两种房间，多 Agent = 在同一个任务里用 @ 拉入子 Agent（C19 子工作流），`kind=collab` 只为兼容而保留；侧栏「项目（含任务组）」置灰，标「未接入」，没有接口；交集写法统一为"父 Agent、任务（room）、模板"。任何 schema 都不增加 `space` 或 `taskGroup` 字段 | §0、§6.1、§14、C19 | Nexus 术语修订（用户 2026-09-26 确认，`/workspace/fe-study/orbit-product-scope.md`、`orbit-product-scope.terms.diff`） |
| C30 | 新增 §16「产物事件与读取接口 (C30)」：`artifact.created` / `artifact.updated`，每个版本持久化成功后只发一次；eventId 是确定性的；control 拒绝重复或更低的 version；mimeType 由服务端嗅探；事件不带内容；新增读取接口 `GET /v1/artifacts/{artifactId}` 和 `/versions/{version}`，所有无权访问的情况一律返回 404，并规定 nosniff、attachment 加 CSP sandbox、隔离 origin 预览、大小上限 `ORBIT_ARTIFACT_MAX_BYTES`；子 Agent 产物的访问不超过父任务（S-9）。拆分：runtime 事件 PR 放在 A1 之后，另有一个 control 接口加鉴权测试的 PR | §16、§2.1 | Nexus、Sentinel（安全测试为必做项）；字段参考 SourceWeft `packages/contracts`（Apache-2.0，只借鉴设计） |
| C30a | §16 按 Sentinel 评审修订：(1) 幂等规则：eventId 和 contentDigest 都相同的重发返回 200 且不做任何事；同一 version 但 digest 不同，或者 version 更低，返回 409 `artifact_version_conflict`，S-AR-1 和 S-AR-3 的测试数据相应重写；(2) P0 预览路径：前端用带鉴权的读取接口 fetch 内容，再放进 `sandbox="allow-scripts"` 的 srcdoc iframe（opaque origin，外加 meta CSP），替换原来的"独立 origin"要求（推迟到 P1）；docx 先过 DOMPurify，PDF 用 pdf.js 并设 `isEvalSupported:false`，xlsx 不用 `xlsx@0.18.5`，放在 Web Worker 里解析并限制大小；S-AR-8 重写；(3) 依赖：`/events` SSE 支持 `Last-Event-ID` 续传；(4) 未登录一律返回 401（**Sentinel 已确认**）：鉴权中间件在任何数据库查询之前拒绝，状态码和响应体字节完全相同；§16.2 第 1 条分成两类；S-AR-5 拆成 5a 和 5b；SSE 未登录时在建立流之前就返回 401；S-AR-8 加强（禁止的 sandbox 标志、CSP 必须是 srcdoc 的第一个节点、必须通过 DOM 属性给 srcdoc 赋值） | §16 | Sentinel 评审 |
| C31 | 新增 §17「登录鉴权与任务归属 (C31)」（**草案**，P0，Nexus）：control 作为 BFF，走 OIDC Authorization Code + PKCE；Postgres 保存服务端 session，cookie 为 `orbit_session`；CSRF 防护采用 Origin 校验加 `X-Orbit-Request: 1` 头；所有持久化实体都带 `tenantId`，任务另带 `createdBy`；P0 的授权规则是"同租户，并且是创建者"，否则 404；未登录访问 /v1（包括 SSE）在查库之前返回 401（Sentinel 已确认）；session 固定攻击防护（登录后重新生成 session id）；OIDC 校验 state、nonce 和 PKCE；`tenantId/createdBy` 只取自 session 和任务记录；OpenAPI 增加 `cookieAuth`；测试 S-AUTH-*；上线顺序：auth PR 在 A1 之后、和 A2 并行，control 的产物读取 PR 在 auth 之后 | §17、§16 | Nexus（P0 决定）；未签字 |
| C32 | 新增 §18「control 持久化存储 (C32，草案)」：Postgres（复用 compose 里的 postgres 服务，单独建库 `orbit_control`，`ORBIT_CONTROL_DB_URL`）；迁移用 goose（有 up/down，CI 跑 up→down→up，prod 只向前）；表结构按 control `846d27e` 的真实字段；**事件游标用每个任务自己的 `seq`**（在任务行上加锁递增，没有空洞），不用全局 BIGSERIAL；查询层按 tenant 过滤，再加 Postgres RLS 兜底；session 只存 sha256，P0 不存 IdP token；产物内容放本地卷，按内容寻址；内存存储只用于测试，prod 缺 DB 时拒绝启动。测试 S-DB-*。顺序：放在 A1 之后，和 A2 并行，在 auth PR 之前 | §18、§16.3、§17.9 | Nexus（范围）、Sentinel 评审（游标、RLS、session、迁移）；**草案，属于地基升级，需要 Celestial 和 Sentinel 签字** |
| C32 rev | §18 按 Sentinel 评审修订（方向已同意，**尚未签字**）：(HIGH 1) 重放与实时流的交接顺序改成"先订阅并缓冲 → 查库补发 → 冲刷缓冲时丢掉 seq ≤ 已补发最大值的事件"，内存存储接口也按这个顺序，S-DB-7 增加两个写入注入点；(HIGH 2) 租户 GUC 一律用 `SELECT set_config('app.tenant_id', $1, true)`，禁止会话级 `SET` 和字符串拼接的 SET，新增 S-DB-11；(MEDIUM 3) `/internal/artifact-blobs` 的边界：租户和任务由 control 查库得出，路径只用 control 自己算的 sha256，边读边计数、超限 413、临时文件加 fsync 加原子 rename，只绑定在内部监听地址上，新增 S-DB-12；(LOW a) `idempotency_keys` 主键改成 (tenant_id, created_by, key_hash)；(LOW b) 删除任务时硬删除并 `ON DELETE CASCADE`，blob 文件不删，孤儿 GC 放到 P1，新增 S-DB-13 | §18 | Sentinel 评审（C32 未签字） |
| C32 rev2 | §18 任务删除从硬删除改成**软删除**（Nexus 产品决定，Sentinel 同意）：`rooms` 增加 `deleted_at/deleted_by`；rooms 的 RLS 增加 `deleted_at IS NULL`，子表 RLS 增加 `EXISTS(未删除的 room)`（单一真相来源，不在子表冗余 deleted_at）；子表外键改成 `ON DELETE RESTRICT`；`DELETE /v1/rooms/{id}` 保留，改成软删除（先 abort，204，重复删除或不存在返回 404；abort 失败也照常删除）；rooms 的 RLS 按命令分开写，软删除 UPDATE 不带 RETURNING（Sentinel 复审 H1）；删除后迟到的 worker 事件和 Idempotency-Key 重放都返回 404（M1）；删除时断开这个任务的 SSE，之后用 Last-Event-ID 重连返回 404；Idempotency-Key 重放遇到已删除的任务返回 404；P0 不提供恢复和彻底清除；S-DB-13 重写 | §18.3、§18.5、§18.7a、§18.8 | Nexus（产品决定）、Sentinel 同意、Celestial（子表用 EXISTS 方案）；C32 仍未签字 |
| C33 | 对齐 orbit-runtime#4（A1）：§2.1 把 `toolState/argsPreview/blockId/delta/seq/activityAttempt` 和 usage 字段从 `TurnFailure` 挪回 `OrbitEvent`（v2.1 文档笔误），`TurnFailure` 只保留原来的 5 个字段；`OrbitEvent.model_mode` 改成 `Literal["mock","real"]`，没有空串默认值；`tool.result` 的 text 截断到 4KB 并带 `truncated:true`；argsPreview 脱敏并且不超过 256 字符；新增 S-M-4（流式脱敏）；control 要原样透传校验通过的 worker 事件，`assistant.delta` 不占用 500 条活动历史的名额（在 Last-Event-ID 的 control PR 里交付） | §2.1、§10.4、§16.3 | orbit-runtime#4（A1） |
| C34 | (a) 新增 `TurnErrorCode = ModelErrorCode ∪ {"state_unreadable"}`（orbit-runtime#6）：worker 读不出会话保存的 AgentState 时使用；closeSession 和 abort 遇到它按幂等成功处理，openSession 和 runTurn 返回失败；新增固定文案。(b) 新增 §2.4 decide 投递幂等（orbit-control#16 复审 F2）：decide 用 Workflow Update 投递，`updateId = approvalRequestId`；turn id 由 approval id 推导；超时用同一个 updateId 重试，不重开审批；只有确定没送达才允许有条件地重开；已处理 id 集合随 continue-as-new 带过去，硬上限 1024，超过就失败并报警。新增 S-ID-2 到 S-ID-8、S-SU-1 到 S-SU-3；decide 在投递状态未知时返回 202 `{deliveryState:"unknown"}`，`docs/openapi.yaml` 同步 | §2、§2.3、§2.4、§10.2、§10.4 | Sentinel（F2 复审和四条验收要求）、orbit-runtime#6 |

---

## 0. 术语与范围

层级（2026-09-26 用户确认，见 `orbit-product-scope.md`「术语」）：**租户/组织 > 项目 > 任务组（可选）> 任务**。任务 = 一个 room = 一个对话 + 一个云端工作空间。

| 产品术语 | 契约/代码里的名字 | 本草案 |
|---|---|---|
| 任务 Task（一个房间 = 一个对话 + 一个工作空间） | room（`/v1/rooms`、`RoomWorkflow`、`room_id`） | **范围内**。API 和 Temporal 继续叫 room，UI 叫「任务」；1 个任务 = 1 个 room = 1 个 `RoomWorkflow`（workflow id `room:<roomId>`，`orbit-control/internal/orch/client.go:75-77`）。 |
| 主 Agent / 子 Agent | 主 session + `AgentRunWorkflow` 子会话 | 范围内（P0，S2 多 Agent 委派）。多 Agent 就是**在同一个任务里用 @ 拉入子 Agent**，实现方式是 C19 的 Temporal 子工作流；产品上**不再区分两种房间**。 |
| 任务台 | 一个 room 的消息、活动和审批视图 | 范围内。 |
| 租户 / 组织 | 不存在 | **不在范围内**。 |
| 项目 Project（资源和数据边界，承载成员、指令、连接器、专家、技能、项目资料） | 不存在 | **不在范围内**（S3，P1）。本草案不新增 project 级字段；只在 R-b 为 `scope` 留扩展位。 |
| 任务组（可选，代码名 `taskGroup`） | 不存在 | **只在文档里预留**，不在 A1–A3 范围内，任何 schema、模型、接口都不增加 `taskGroup` 字段。 |
| `kind=solo/collab`（现有代码，`orbit-control/internal/app/app.go:172`、`openapi.yaml` RoomKind） | `Room.kind`、`RoomWorkflowInput.kind` | 为了兼容保留，**产品上不再区分**：`collab` 不代表另一种房间，多 Agent 统一走"同一个任务 + @ 子 Agent（C19）"。本草案不新增房间类型。 |
| 云端工作空间（每个任务自己的云端文件区和沙箱） | worker isolation（`ORBIT_ISOLATION_MODE`） | 不变。 |
| 团队空间（资料库里的共享区，权限分查看、编辑、管理、无权限） | 不存在 | **只是文档概念**，不在范围内。「空间 = Agent 协作容器」的定义**已作废**，契约里不出现 `space` 字段。 |
| Cloud Agent / 定时（S4） | `CloudAgentJob`、`/v1/cloud-agents` | P1。本草案只修 spec 漂移（§9）。 |

---

## 1. 现状核对（后面各节都基于这些事实）

1. **worker 事件用 snake_case，control 只读 camelCase，所以 worker 事件目前全部被拒。** `OrbitEvent` 没有别名（`orbit-runtime/packages/orbit_contracts/src/orbit_contracts/models.py:159-180`）；`HttpEventIngest` 发出的是 `model_dump(mode="json")`（`orbit_worker/events.py:35`），也就是 `room_id`、`session_id`、`occurred_at`。control 的 `Ingest` 读的是 `roomId`、`sessionId`、`occurredAt`（`orbit-control/internal/app/app.go:586-592`），拿不到 roomId 就返回 400 "event is not associated with a known room"（`app.go:606-607`）。worker 只把失败计数（`events.py:43-46`）。另外工具名写在 `text` 里（`runtime.py:322-328`），control 读的是 `toolName`（`app.go:675`）。
2. **`awaiting_external` 不是停车状态。** `_after_turn` 在同一个 handler 里同步循环执行外部工具（`orbit_orch/workflows.py:333-340`），这个状态只在执行期间出现。今天唯一真正等人的状态是 `awaiting_approval`（`workflows.py:472-475`，由 `decide` Update 解除，`:219-239`）。control 的 `RoomState` 里连 `awaiting_external` 都没有（`app.go:20-25`），spec 也没有（`docs/openapi.yaml:650-652`）。
3. **`agent_spawn` 是外部工具，走 Temporal 子工作流（路径 a），不是 activity 里的 AgentScope 子 Agent。** 这个工具 `is_external_tool=True`（`orbit_worker/tools.py:9-26,78-96`），AgentScope 发出 `RequireExternalExecutionEvent`，得到 `TurnResult(status=needs_external)`（`runtime.py:272-290`）；RoomWorkflow 调 `start_child_workflow(AgentRunWorkflow.run, …)`（`workflows.py:388-406`），然后用 `deliverToolResult` 把 `{workflow_id,status}` 交回去（`:342-363`）。已知缺口：
   - `AgentRunWorkflow` 只跑**一次** `runTurn`，然后不看 status 就 close（`workflows.py:112-124`）。子 Agent 遇到审批或外部工具时会被丢掉：审批没人看到，子会话直接关闭。子 Agent 不能再 spawn，所以今天实际最多只有 1 层。
   - `incoming` signal 是空操作（`:92-94`）；`dissolve` 只在 openSession 之后检查一次（`:109`）。
   - fan-out 数的是 `len(self._children)`，也就是**历史累计**，不是并发数（`:389`）。深度判断是 `self._depth + 1 >= self._max_depth`，room 的 `_depth` 恒为 0（`:156,391`）。
   - 子工作流 id 是 `{room wf id}/agent/{call_id}`（`:393`），call_id 由模型生成，可能重复。`persona` 参数被忽略（`:398`）；子 Agent 直接继承 room preset（`:399`）。
   - 没有显式 `parent_close_policy`，用的是 SDK 默认值（TERMINATE）。abort 会调 `_dissolve`（signal 加 `handle.cancel()`，`:427-435,451-467`），但 activity 不发 heartbeat（全仓库没有 `heartbeat`），正在运行的子 `runTurn` 收不到取消，activity 内的工具还会继续执行。被取消的子工作流也不会走 `_close`，所以不会发 `agent.finished`。
   - `agent_wait` 等的是整个 room 的**所有**子 Agent，不只是调用者自己的子 Agent（`:416-425`）。
4. **子 Agent 事件没有父子关系。** `agent.started` 在 `open_session` 里发出，`text=session_id`（`runtime.py:126`）；`agent.finished` 在 `close_session` 里发出（`:226`）。事件上没有 parent、agent id 或 persona 字段（`models.py:159-180`）。`GET /v1/rooms/{roomId}/agents` 在 spec 里标了 `x-unimplemented: true`（`openapi.yaml:298-315`），`handler.go` 没有注册这个路由。
5. **权限只在 openSession 设置一次。** `PermissionContext(mode=_PRESETS[preset])`（`runtime.py:56-60,105-106`）。之后每个 activity 都从 blob 重建 Agent（`:230-239`），不会重新下发策略。`_ExternalTool.check_permissions` 一律 ALLOW（`tools.py:17-26`），只有 `gateway_charge` 返回 ASK（`:45-54`）。`RunTurnInput` 里没有权限字段（`models.py:51-56`）。
6. **拒绝审批时两边的语义不一致。** 进 worker 时，reject 转成 `ConfirmResult(confirmed=False)`，继续驱动 Agent（`runtime.py:331-345`），workflow 回到 running（`workflows.py:469-489`）。control 这边 reject 后把 room 设成 `closed`，还发出 `session.status closed`（`app.go:428-438`）。spec 写的是"reject maps to worker abort"（`openapi.yaml:353-356`）。拒绝的结果没有专门的错误码。
7. **建房间失败时的状态。** Temporal 模式下，`StartRoom` 失败或 30 秒内等不到 session（`orch/client.go:79-111`）时，room 被设成 `closed` 并返回（`app.go:197-204`）。handler 回 502 `WORKER_ERROR`，body 里没有 roomId（`handler.go:113-119`；web 为此专门做了"无 id"提示，`orbit-web/src/lib/rooms.ts:109-115`）。失败的 room 不会落盘，因为 `persistRoom` 只在 `publish` 里调用（`app.go:689`）。control 启动时也从不读回 FileStore（`ReadJSON`/`ReadJSONL` 没有调用方），**control 的所有状态实际都在内存里**。没有 Idempotency-Key；CORS 只放行 `content-type, authorization`（`handler.go:43`）。
8. **模型。** worker 永远用 `MockChatModel()`（`runtime.py:235`），代码里不读 `ORBIT_MODEL_*`（全仓库 `rg` 没有结果）。orbit-infra 说"设置 `ORBIT_MODEL_API_KEY` 就用真模型"（`orbit-infra/README.md:79`、`compose.yaml:100`），和代码不符。真模型 PR（`ORBIT_MODEL_MODE`）在另一个分支进行中。
9. **persona、grant 在 Temporal 模式下不会下发。** `StartRoom` 只传 `roomId/kind/permissionPreset`（`orch/client.go:90-94`）。persona、connector 和 **grantEnv 明文**只在直连 worker 的 HTTP 模式里放进 payload（`app.go:214-252`）。但 Python worker 根本没有 `/internal/activities/*`，它的健康服务对任何请求都回 `200 ok`（`orbit_worker/main.py:35-46`），所以直连模式对当前 runtime 已经失效。
12. **并行停车处理有误（v2.1，AS §6.3-1）**：`_drive` 只取 `event.tool_calls[0]` 生成审批（`runtime.py:264-271`），多个 `RequireExternalExecutionEvent` 时 external 只留下最后一个（`:272-278`）；`_confirm_event` 把同一个 outcome 作用到**所有** ASKING 调用上（`:331-345`），批准一个就等于批准全部；只回填部分 external 时 AgentScope 返回 `finished_reason=None`，`_drive` 判为 `continue`（`:285-294`），workflow 退出外部工具循环，剩下的调用永远卡住。
11. **ingest 鉴权（v2 补充）**：`HttpEventIngest` 会发 `Authorization: Bearer <ORBIT_INTERNAL_TOKEN>`（`orbit_worker/events.py:41`，token 来自 `main.py:54`）；control 的 `/internal/events` 调 `internalauth.Authorized` 校验（`handler.go:219`），但 token 为空时对 loopback 请求直接放行（`internal/internalauth/auth.go:21-28`）。
10. `CreateCloudAgent` 只是记一条 `queued` 记录，不会启动 `CloudAgentJob`（`internal/app/catalog.go:197-230`）。

---

## 2. 公共模型变更（`orbit_contracts/models.py`）

```python
# ---- 新增类型 ----
+ AgentFinishReason = Literal["completed", "aborted", "dissolved", "failed"]
+ ToolErrorCode = Literal[
+     "APPROVAL_REJECTED",          # R-a：人工拒绝，工具未执行
+     "PERMISSION_DENIED",          # R-a：策略直接拒绝（例如 read-only 下的写操作），不停车
+     "DELEGATION_LIMIT_EXCEEDED",  # 第 6 节：委派超限
+     "QUESTION_CANCELLED",         # R3：问题因 abort 作废
+     "SESSION_ABORTED",            # 第 6 节：会话已中止，拒绝一切后续工具
+ ]
+ ToolRisk = Literal["read", "write", "sensitive", "destructive"]   # v2：没有默认值，每个工具必须显式声明（§7）

+ class AgentRef(BaseModel):                     # R1：所有 agent 维度数据的身份
+     agent_id: str = "main"                     # room 内唯一；主 Agent 恒为 "main"
+     parent_agent_id: str | None = None
+     parent_session_id: str | None = None       # 主 Agent 为 None
+     depth: int = 0                             # 主=0，子=1，孙=2
+     agent_path: str = "main"                  # v2.1：由 room 在 spawn 时拼接，例如 "main/ag-x/ag-y"；UI 直接用它渲染树
+     persona: str = ""                          # P0 只是展示名；绑定 persona 目录是 P1

+ class ApprovalRule(BaseModel):                 # R-b，只由 orbit-control 签发
+     rule_id: str
+     tool_name: str
+     argument_pattern: dict[str, str] = Field(default_factory=dict)  # 按参数名匹配，路径先规范化（§8）；默认 = 当前参数的精确值
+     any_arguments: bool = False                # True 才表示任意参数；必须由用户二次确认（§8）
+     scope: Literal["room", "persona"] = "room"
+     scope_id: str                              # roomId 或 personaId

+ class PermissionSnapshot(BaseModel):           # R-a，每次由 control 发起的 turn 都随附
+     preset: PermissionPreset = "workspace-write"
+     rules: list[ApprovalRule] = Field(default_factory=list)
+     revision: int = 0                          # control 侧单调递增，用于审计
+     issued_at: str = ""

+ class QuestionChoice(BaseModel):
+     id: str
+     label: str

+ class QuestionAsk(BaseModel):                  # R3
+     question_id: str                           # v2："q-" + ask_id(room_id, agent_id, turn_id, call_id)，见 §2.2
+     text: str
+     choices: list[QuestionChoice] = Field(default_factory=list)
+     allow_custom: bool = True
+     agent: AgentRef = Field(default_factory=AgentRef)

+ class QuestionAnswer(BaseModel):
+     question_id: str
+     choice_ids: list[str] = Field(default_factory=list)
+     text: str = ""                             # 自由文本（「其他…」）

+ class TodoItem(BaseModel):                     # R4
+     id: str
+     content: str
+     status: Literal["pending", "in_progress", "completed", "cancelled"] = "pending"

+ class DelegationLimits(BaseModel):             # 第 6 节
+     max_depth: int = 2                         # 主 Agent 之下的层数（main→child→grandchild）
+     max_children_per_parent: int = 4           # 每个父 Agent 同时 running 的子 Agent 数
+     max_active_agents: int = 8                 # 整个 room 同时活跃的子 Agent 数（不含 main）

# ---- 修改 ----
  class ApprovalAsk(BaseModel):
      approval_request_id: str                   # ~ v2："apr-" + ask_id(room_id, agent_id, turn_id, call_id)，见 §2.2（原为 "apr-{call_id}"，runtime.py:267）
+     turn_id: str = ""                          # 发起这次停车的 turn
      tool_name: str
      call_id: str | None = None
      reason: str | None = None
+     agent: AgentRef = Field(default_factory=AgentRef)
+     arguments: dict[str, str] = Field(default_factory=dict)   # 只用于展示：先脱敏再截断（§7），不会因为疑似密钥让整张卡失败
+     arguments_truncated: bool = False
+     risk: ToolRisk = "write"
+     allow_always: bool = False                 # v2：只有 risk=="sensitive" 且不是 always_ask 时为 True；destructive 和扣费类永远为 False

  class ExternalCall(BaseModel):
      tool_name: str; call_id: str; arguments: dict[str, str] = {}
+     question: QuestionAsk | None = None        # tool_name == "ask_user" 时有值 → 停车，不执行

  class OpenSessionInput(BaseModel):
      room_id: str; turn_id: str; permission_preset: PermissionPreset = "workspace-write"
+     agent: AgentRef = Field(default_factory=AgentRef)
+     permission: PermissionSnapshot | None = None   # None → 旧行为（只用 preset）
+     model: str = ""                            # 只传模型名；密钥永远不进来（第 8 节）

  class RunTurnInput(BaseModel):
      room_id: str; session_id: str; turn_id: str; message: str; state_version: int
+     permission: PermissionSnapshot | None = None
+     answer: QuestionAnswer | None = None       # 保留字段；R3 走 deliverToolResult，见 §3

  class ResolveApprovalInput(BaseModel):
      ...
~     outcome: Literal["allowed-once", "allowed-always", "rejected"]
+     permission: PermissionSnapshot | None = None
+     rule_id: str | None = None                 # allowed-always 时 control 新建的规则 id
+     call_id: str                               # v2.1：只作用于这一个调用；其他 ASKING 调用保持 ASKING（§2.2）

  class DeliverToolResultInput(BaseModel):
      ...
+     permission: PermissionSnapshot | None = None
+     error_code: ToolErrorCode | None = None
+     results: list[ToolResultItem] = Field(default_factory=list)  # v2.1：一次可以回填多个结果；非空时忽略单值字段
+     idempotency_key: str = ""                 # v2.1：= idem(room_id, session_id, call_id)，见 §2.3

+ class ToolResultItem(BaseModel):               # v2.1
+     tool_name: str; call_id: str; output: str = ""; output_ref: str = ""
+     metadata: dict[str, str] = Field(default_factory=dict)
+     result_state: Literal["success", "error", "denied", "interrupted"] = "success"
+     error_code: ToolErrorCode | None = None

  class SteerInput(BaseModel):
      ...
+     permission: PermissionSnapshot | None = None

~ TurnStatus = Literal["continue", "needs_approval", "needs_external", "completed", "failed"]   # C28：新增 failed（models.py:12）
+ ModelMode = Literal["mock", "real"]
+ ModelErrorCode = Literal["timeout", "auth", "rate_limited", "provider_error", "config"]   # C28
+ TurnErrorCode = Literal["timeout", "auth", "rate_limited", "provider_error", "config", "state_unreadable"]   # C34：TurnResult/TurnFailure/OrbitEvent 的 errorCode 用这个类型；ModelErrorCode 只描述模型调用本身

  class TurnResult(BaseModel):
      status: TurnStatus; session_id; state_version; text
+     model_mode: ModelMode = "mock"             # C28：前端的 mock 标记
+     model_name: str = ""
+     retryable: bool | None = None
~     approval: ApprovalAsk | None = None        # v2.1：已弃用，等于 approvals[0]，留给旧版 control
~     external: ExternalCall | None = None       # v2.1：已弃用，等于 externals[0]
+     approvals: list[ApprovalAsk] = Field(default_factory=list)   # v2.1：完整的待审批集合
+     externals: list[ExternalCall] = Field(default_factory=list)  # v2.1：完整的待执行 external 集合
+     usage_summary: dict[str, int] = Field(default_factory=dict)  # v2.1：本轮 token 汇总（明细走 usage 事件）
+     todos: list[TodoItem] | None = None        # 本轮最后一次 todo_write 的完整列表；None = 本轮没写
+     error_code: ToolErrorCode | ModelErrorCode | None = None   # C28：status=="failed" 时取 ModelErrorCode 的值

  class RoomWorkflowInput(BaseModel):            # camelCase 别名沿用
      room_id; permission_preset; kind
~     max_fanout: int = 4                        # 语义改为「每个父 Agent 同时 running 的子 Agent 数」（原为累计数）
~     max_depth: int = 2                         # 语义改为「主 Agent 之下的层数」：允许 new_depth <= max_depth
+     max_active_agents: int = Field(default=8, alias="maxActiveAgents")
+     model: str = ""
+     persona_id: str = Field(default="", alias="personaId")

~ class RoomCommand(BaseModel):
~     kind: Literal["open", "message", "approve", "abort", "steer", "answer"]
~     outcome: Literal["allowed-once", "allowed-always", "rejected"] = "allowed-once"
+     answer: QuestionAnswer | None = None
+     permission: PermissionSnapshot | None = None

  class RoomSnapshot(BaseModel):
      ...
+     pending_approvals: list[ApprovalAsk] = Field(default_factory=list)  # 主 Agent 和所有子 Agent 的
+     pending_questions: list[QuestionAsk] = Field(default_factory=list)  # v2：主 Agent 和子 Agent 的问题都在这里（Q6）
+     agents: list[AgentRef] = Field(default_factory=list)                # 当前活跃的子 Agent
+     limits: DelegationLimits = Field(default_factory=DelegationLimits)

  class AgentRunInput(BaseModel):
      room_id; prompt; permission_preset; depth; max_depth
+     agent: AgentRef                            # 必填，由 room 分配
+     room_workflow_id: str                      # 子 Agent 只能 signal 这个 workflow
+     permission: PermissionSnapshot | None = None   # 只是初值；运行中随 room 刷新（§6.1）
+     model: str = ""

+ class PermissionChangedRequest(BaseModel):     # v2：control 在规则新建或撤销时发给 room 的 Update
+     permission: PermissionSnapshot

+ class RunTurnRequest(BaseModel):               # runTurn Update 的参数（原为 dict[str,str]，workflows.py:200）
+     model_config = ConfigDict(populate_by_name=True)
+     turn_id: str = Field(alias="turnId"); message: str = ""
+     permission: PermissionSnapshot | None = None
+ class DecideRequest(BaseModel):                # decide Update（原为 dict，:220）
+     turn_id: str = Field(alias="turnId"); approval_request_id: str = Field(alias="approvalRequestId")
+     decision: Literal["allow", "reject", "allow-always"] = "allow"
+     rule_id: str | None = Field(default=None, alias="ruleId")
+     resume_turn_id: str = Field(default="", alias="resumeTurnId")
+     permission: PermissionSnapshot | None = None
+ class AnswerRequest(BaseModel):                # answer Update（新增）
+     turn_id: str = Field(alias="turnId"); answer: QuestionAnswer
+     permission: PermissionSnapshot | None = None
```

所有新模型加进 `contract_models()`（`models.py:242-273`），重新导出 `schema/*.json`。Update 参数从 `dict[str,str]` 换成模型后，线上 JSON 依然兼容：Go 端传的是同名 camelCase 键（`orch/client.go:130-133,150-155`）。

### 2.0 停车 id 的生成规则（v2，S-M1）

- `ask_id(room_id, agent_id, turn_id, call_id) = sha256("{room_id}|{agent_id}|{turn_id}|{call_id}").hexdigest()[:20]`。审批用 `apr-` 前缀，问题用 `q-` 前缀。由 worker 在停车时生成。activity 重试会命中同一个 turn_id 的缓存结果（`runtime.py:395-403`），所以重试得到的 id 相同，不需要额外状态。
- **前提（新增约束）**：同一个 session 内 `turn_id` 必须唯一。control 每次请求都生成随机 `tn_…`（`app.go:333,423`），这一点已经满足；workflow 内部派生的 turn_id 也要带上单调的步骤号（`{turn}:x{n}`，`workflows.py:338` 已经是这样）。AgentRunWorkflow 的循环不能再用固定的 `{wf}:turn`（`workflows.py:117`），改成 `{wf}:t{n}`。worker 发现同一个 turn_id 被用于不同的输入时，拒绝并报 `ValueError("turn_id reused")`（idempotency 缓存命中时比对输入的哈希）。
- 过期 id：room workflow 只接受当前在 `pending_approvals` / `pending_questions` 里的 id；control 在 decide/answer 之前先 query 校验（§6.2），不在列表里的返回 409 `APPROVAL_NOT_PENDING` / `QUESTION_NOT_PENDING`。
- 验收 **S-ID-1**：mock 在两个 turn 里用相同的 `call_id` 调 `gated_echo`。turn-1 的审批被 reject，turn-2 重新停车；这时对 turn-1 的 approvalId 执行 decide(allow)，返回 409；turn-2 的 gated_echo 执行 0 次（没有它的 `tool.result`），它的审批仍然是 pending。两个 approvalRequestId 不同。

### 2.1 OrbitEvent（事件信封）

```python
  class OrbitEvent(BaseModel):
+     model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)   # 修 §1.1
~     type: Literal[..., "question.asked", "question.answered", "todo.updated",
~                   "approval.resolved", "agent.spawn_rejected", "assistant.delta", "turn.failed"]   # v2.1：由 v2 的 approval.decided 更名而来
      event_id; occurred_at; session_id; room_id; job_id; text; runtime; runtime_version; permission_preset
+     turn_id: str = ""
+     agent_id: str = "main"                     # R1：所有 agent 维度事件都必须带
+     parent_agent_id: str | None = None
+     parent_session_id: str | None = None
+     depth: int = 0
+     persona: str = ""
+     tool_name: str = ""; call_id: str = ""
+     approval_request_id: str = ""
+     risk: ToolRisk | None = None
+     error_code: ToolErrorCode | None = None
+     status: str = ""                           # session.status / agent.finished 的 reason 等
+     question: QuestionAsk | None = None
+     answer: QuestionAnswer | None = None
+     todos: list[TodoItem] | None = None
+     todo_revision: int = 0
+     limit: str = ""; limit_max: int = 0; limit_current: int = 0   # agent.spawn_rejected
+     agent_path: str = "main"                  # v2.1
+     model_mode: ModelMode; model_name: str = ""   # C28/C33：所有由 worker 发出的事件都带；Literal["mock","real"]，没有空串默认值
+     tool_state: Literal["success","error","denied","interrupted"] | None = None  # v2.1：tool.result 用
+     truncated: bool = False                    # C33：tool.result 的 text 超过 4KB 时截断，并设为 true；完整输出以后走产物通道（§16）
+     args_preview: str = ""                    # v2.1/C33：tool.call 用；先脱敏，再截断到 ≤256 字符（§7）
+     outcome: str = ""; decided_by: str = ""   # v2.1：approval.resolved 用
+     block_id: str = ""; seq: int = 0; delta: str = ""; activity_attempt: int = 1   # v2.1：assistant.delta 用；这里的 seq 是 block 内分片序号，不是 §18.4 的任务事件 seq
+     model: str = ""; input_tokens: int = 0; output_tokens: int = 0
+     cache_input_tokens: int = 0; cache_creation_input_tokens: int = 0; latency_ms: int = 0   # v2.1：usage 用（来自 ModelCallEndEvent）
+     failure: TurnFailure | None = None         # C28：turn.failed 用（对齐 #3 `3ec4204`）

+ class TurnFailure(BaseModel):                  # C28；JSON：{turnId, agentId, errorCode, retryable, message}；C33：只有这 5 个字段
+     turn_id: str; agent_id: str
+     error_code: ModelErrorCode; retryable: bool
+     message: str                               # 必须和 §10.2 固定文案表中的一项完全一致
```
`HttpEventIngest` 改成 `model_dump(mode="json", by_alias=True, exclude_none=True)`。`text` 不再存工具名。

**C33（对齐 orbit-runtime#4，A1）**：
- `tool.result.text` 最多 **4KB**，超出部分截断，并设置 `truncated: true`；完整输出以后通过产物通道提供（§16）。`argsPreview` 先脱敏，再截断到 **≤256 字符**。
- control 对**校验通过**的 worker 事件**原样透传**，不改写，也不丢字段，包括 `assistant.delta`、`turn.failed`、`usage` 等。`assistant.delta` **不占用** 500 条活动历史的名额（`app.go:31,684-687`），只走 SSE。这一条在 **Last-Event-ID 的 control PR** 里交付（§16.3）。

**v2.1 事件补充（C18、C20）**：
- `assistant.delta {turnId, blockId, seq, delta, activityAttempt}`：来自 `TextBlockDeltaEvent`，每 100ms 或每 200 字合并一次后发出。control **只通过 SSE 转发，不写入 activity 或 audit**（否则会挤掉 500 条上限）。最终文本仍然由 `assistant.message` 落库。activity 重试时 `activityAttempt` 递增，UI 收到更大的 attempt 时清空这个 turn 的草稿。
- `usage {agentId, agentPath, model, inputTokens, outputTokens, cacheInputTokens, cacheCreationInputTokens, latencyMs}`：每个 `ModelCallEndEvent` 发一次。
- `approval.resolved {approvalRequestId, callId, agentId, agentPath, outcome: allowed-once|allowed-always|rejected|cancelled, decidedBy, ruleId?}`：由 control 在 decide 成功后发出（v2 叫 `approval.decided`）。
- 所有 agent 维度的事件都带 `agentPath`。

control 侧的变更：`workerEventTypes`（`app.go:565-574`）加入 5 个新类型；`ActivityEvent`（`app.go:87-107`，spec 在 `openapi.yaml:761-821`）新增 `agentId, parentAgentId, parentSessionId, depth, persona, risk, errorCode, questionId, data`（object，放 question/answer/todos/limit），spec 的 `type` 枚举同步扩充。

---

### 2.2 并行停车（v2.1，C16/C22/C24）

- **来源真相**：每次 `_drive` 结束后，worker 用 `agent.state.get_awaiting_tool_calls(agent.name)` 生成**完整的** `approvals[]`（ASKING）和 `externals[]`（SUBMITTED），不再从流事件里取第一个或最后一个。只要列表不为空，status 就不能是 `continue`：有 ASKING 调用时为 `needs_approval`，否则为 `needs_external`。只有列表为空、并且 `finished_reason==COMPLETED` 时才是 `completed`。
- **一个调用一张卡，逐个决定**：`resolveApproval` 只带一个 `approval_request_id` 和 `call_id`，worker 发出的 `UserConfirmResultEvent.confirm_results` **只包含这一个调用**，其他 ASKING 调用保持 ASKING。**P0 没有"全部允许"**，API 也不接受批量决定。worker 在同一个 turn 内遇到重复的 `call_id`（同一次模型响应里出现两个相同 id）时，拒绝这个 turn（`ValueError("duplicate call_id")`），防止两张卡共用一个 id。
- **部分回填**：批准一个或回填一部分 external 之后，如果还有待办调用，worker 返回同样的列表形式（剩余部分），并为剩余调用**重新发出** `approval.asked` / `tool.call`。id 是确定性的，control 按 id upsert，不会重复出卡。room 保持 `awaiting_approval` / `awaiting_external`，直到列表清空。
- **external 批量执行**：workflow 对 `externals[]` 用 `asyncio.gather`（确定性）并行执行，然后**一次** `deliverToolResult(results=[…])` 回填，或者按完成顺序逐个回填（每次都遵守上一条规则）。同时有 approvals 和 externals 时，先执行 externals，再停下来等审批。
- **需要验证**（A2 的测试义务）：AgentScope 2.0.8 能否接受只含部分调用的 `confirm_results`，并让其余调用保持 ASKING。如果不能，worker 在 blob 里暂存已作出的决定，等所有 ASKING 调用都有决定后再一次性提交。但 S-Ra-10 要求批准后立即执行，所以这时需要在 Orbit 包装层单独执行被批准的调用，并把结果注入。
- 验收：
  - **S-PP-1**：mock 在一次响应里发出两个 gated 调用（call_id 不同）。先批准一个，它立刻执行，另一个仍然 pending，room 保持 `awaiting_approval`；再拒绝另一个，它执行 0 次并得到 `APPROVAL_REJECTED`，这时 room 才回到 `running`。整个过程中被批准的调用只执行一次。
  - **S-PP-2**：一次响应里发出两个 `gateway_lookup`，两次 `gatewayExecute` 都执行，结果一次回填；status 序列里没有出现过 `continue`。
  - **S-PP-3**（故障注入）：两个 external 只回填一个，TurnResult 是 `needs_external`，`externals` 里剩一项，对应的 `tool.call` 被重新发出，room 保持 `awaiting_external`；补上第二个结果后才继续。

### 2.3 执行粒度、幂等与长 room（v2.1，C21）

- **每个 turn 一个 activity**，跑到停车或结束为止，这是默认方案。**只有**有外部副作用、需要审批、长时间运行或跨 agent 的工具才做成 external tool，每个都成为 workflow 里单独的 activity 或子工作流。只读、快速、可以重复执行的工具留在 activity 内。风险级为 `write` 的工作区工具留在 activity 内，但必须对工作区幂等，因为 activity 重试可能让它重复执行。
- **幂等键**：`idem(room_id, session_id, call_id) = sha256(...)[:24]`。gateway 和所有有副作用的 external activity 都要把它传给下游，并在下游去重。call_id 保存在 AgentState 里，重试时不会变。继续保留 `turn_id:activity` 结果缓存和 `state_version` 校验。
- **检查点**：AgentState 只存在 blob store（Postgres + Fernet）里。Temporal payload 里只放 `session_id + state_version` 以及小摘要，**永远不把 AgentState 或完整 context 作为 activity 的输入或返回值**。大的工具输出（超过 16 KB）写入 blob，payload 里用 `output_ref`。activity 内按 `ModelCallEndEvent` 做子版本检查点（`v.n`），用来降低重试成本；这是可选项，P0 不强制。
- **continue-as-new**：room 在空闲时（没有进行中的 handler，`workflow.all_handlers_finished()` 为真）检查 `workflow.info().is_continue_as_new_suggested()`，或者按 turn 数阈值（例如 200）判断，然后 `continue_as_new(RoomCarryOver{room_id, session_id, state_version, preset, permission, limits, agents 登记表, pending_approvals, pending_questions, child_workflow_ids, decided_approvals})`。有子 Agent 在运行时**推迟** continue-as-new（P0 的简化做法）；之后改成通过 external handle 重新关联子 Agent。（C34：`decided_approvals` 见 §2.4）
- **task queue**：`orbit`（workflow）、`orbit-agent`（LLM activity，按模型并发数配置 `max_concurrent_activities`）、`orbit-sandbox`（沙箱或编码类，例如 Cloud Agent）、`orbit-gateway`（**只有**它能解析凭证，已经存在：`orbit_worker/main.py` 里的 `{queue}-gateway`）。队列名通过 `RoomWorkflowInput` 或环境变量配置，默认值兼容现有部署：没有配置时 `orbit-agent` 退回 `orbit`。
- **禁止**：在一个 activity 里起多个 Agent 互相对话来模拟团队；使用 `agentscope.app` 的 Team 或调度器（C19）；在 activity 里阻塞等待人工审批。

### 2.4 decide 投递幂等（C34，orbit-control#16 复审 F2）

**问题**：control 在 `Orch.Decide` 或 `resolveApproval` 出错时调 `ReopenApproval`。如果错误是超时，决定可能已经送达，重开后再 decide 一次，就会带着新的 turn id 再投递一次，工具可能执行两次。

**规则**：
1. **投递方式**：decide 一律用 Temporal Workflow Update 投递，`update_id = approvalRequestId`。同一个 workflow run 里，Temporal 按 update_id 去重。
2. **turn id 确定**：续跑的 turn id 由 approval id 推导：`resumeTurnId = "t-" + sha256("decide:" + approvalRequestId)[:24]`。重试时不生成新的 turn id。
3. **重复投递返回第一次的结果**：room workflow 维护 `decided_approvals: dict[approvalRequestId, DecideOutcome]`。handler 先查这张表，命中就直接返回第一次的 `DecideOutcome`，不报错，也不重新执行。`DecideOutcome` 只放小摘要（decision、agentId、resumeTurnId、turn 摘要），不放 AgentState。
4. **随 continue-as-new 带过去，有上限**：`decided_approvals` 放进 `RoomCarryOver`（§2.3）。只保留仍在 `pending_approvals` 生命周期内、或者决定后还没到 `ORBIT_DECIDED_APPROVAL_TTL_S`（默认 86400 秒）的条目，过期条目在 continue-as-new 时丢弃。另有硬上限 `MAX_DECIDED_APPROVALS = 1024`，写成常量，不能用环境变量改。插入第 1025 个未过期条目时，workflow 以 `ApplicationError("DECIDED_APPROVALS_LIMIT", non_retryable=True)` 失败，并发出 `session.status` 报警事件。**不能**悄悄淘汰旧条目。
5. **超时用同一个 updateId 重试**：control 遇到超时、`Unavailable`、`DeadlineExceeded`，或者不知道有没有送达的情况，用同一个 update_id 重试（有界退避，总时长不超过 30 秒）。审批保持 `decided`，**不调** `ReopenApproval`。重试用完后，审批仍保持 `decided`，并返回 202 `{deliveryState:"unknown"}`，让前端靠事件流确认。
6. **重开的前提**：只有 Temporal 明确返回“没送达”，才允许重开。“没送达”只包括两种：Update 被 validator 拒绝，或者 workflow 从来没有收到这个 update_id（`NotFound`，并且 history 里没有这个 update_id）。重开必须走带条件的状态转换：`UPDATE approvals SET status='pending', decision=NULL, decided_at=NULL WHERE id=$1 AND status IN ('allowed','rejected') AND delivery_state='not_delivered'`，影响 0 行就不重开。approvals 表新增 `delivery_state TEXT NOT NULL DEFAULT 'pending'`，取值 `pending|delivering|delivered|unknown|not_delivered`。“已送达但超时”和“workflow 已不存在”同时出现时，`delivery_state` 是 `unknown`，审批**保持** `decided`。
7. **重试和重开并发时只生效一次**：同一个审批的重试和重开都先抢 `delivery_state` 的行锁（`SELECT … FOR UPDATE`），用条件 UPDATE 完成状态转换。最终状态唯一，worker 最多收到一次。
8. **子 Agent**：room 把 decide 转成 signal 给子工作流的 `resolve` 前，先查 `decided_approvals`。子工作流的 `resolve` 也按 approvalRequestId 去重，重复的 signal 忽略并记 warning。

**验收**（E2E，真实 Temporal 加 orbit-worker；用例编号由 Sentinel 在本 PR 复审时确认）：
- **S-ID-2 重复投递**：同一个 approvalRequestId 连续 decide 两次（绕过 control 的 409，直接对 workflow 发两次 Update，update_id 相同）。两次返回的 `DecideOutcome` 完全相同；worker 端副作用计数为 1；房间事件流里这个 resumeTurnId 的 `turn.started` 只出现 1 次。
- **S-ID-3 已送达但超时**：在 control 和 Temporal 之间注入一次“Update 已被 workflow 接受，但 control 收到超时”。control 用同一个 update_id 重试；审批保持 `decided`，没有调用 `ReopenApproval`；worker 只收到一次。
- **S-ID-4 确定没送达**：让 workflow 不存在（从未启动这个 room），decide 失败。审批回到 `pending`，`delivery_state=not_delivered`，再 decide 可以成功。
- **S-ID-5 已送达但超时，之后 workflow 不存在**：先注入 S-ID-3 的超时，再让 workflow 终止。审批保持 `decided`，`delivery_state=unknown`，不重开。
- **S-ID-6 重试和重开并发**：同一个审批并发发起 4 次同 update_id 重试和 4 次重开。最终 `status` 和 `delivery_state` 唯一，worker 最多收到 1 次，没有 500。
- **S-ID-7 continue-as-new 之后重复投递**：decide 一次后强制 continue-as-new，再用同一个 update_id 投递。返回第一次的 `DecideOutcome`，worker 仍然只处理一次。
- **S-ID-8 上限**：`decided_approvals` 里已有 1024 个未过期条目，再投第 1025 个。workflow 以 `DECIDED_APPROVALS_LIMIT` 失败，产生报警事件；失败前的 history 和 carry-over 里 1024 个 id 一个不少。


---

## 3. R3 向用户提问（追问卡）

**机制**：新增外部工具 `ask_user`（`_ExternalTool` 子类），参数为 `{text, choices?: json string, allowCustom?}`。worker 看到 `RequireExternalExecutionEvent` 且工具名是 `ask_user` 时，把 `ExternalCall.question` 填好，返回 `needs_external`。RoomWorkflow 在 `_after_turn` 里**不执行这个调用，而是停车**：状态设为 `awaiting_external`，把问题追加到 `pending_questions`，外部工具循环就此结束。这就是把"等人"推广出去：复用 `awaiting_external` 和 `deliverToolResult`，不新增 TurnStatus。答案通过 `deliverToolResult(tool_name="ask_user", output=<answer json>)` 交回，Agent 在同一个 reply 里继续，下一次模型调用就能看到答案（满足 P0 成功标准 3）。不把答案塞进"下一条用户消息"，是为了保持工具调用和结果成对，AgentScope 的 awaiting tool call 也要靠结果才能解除（`runtime.py:348-368`）。

| 层 | 变更 |
|---|---|
| 事件 | `question.asked {question, agentId, parentSessionId, callId}`、`question.answered {questionId, answer}`；abort 时发 `question.answered {errorCode: QUESTION_CANCELLED}` |
| Temporal | 新 Update `answer(AnswerRequest)`。validator：状态必须是 `awaiting_external`，并且 `question_id` 和 pending 的一致，否则拒绝（ApplicationError `QUESTION_NOT_PENDING`）。handler 调 `deliverToolResult` 后走 `_after_turn`，返回 `_turn_payload`（和 `decide` 同构）。`getRoomView` 增加 `pendingQuestionIds`（数组）。patch id：`orbit-room-ask-user`（停车改变了命令序列）。**v2（Q6 已裁定）**：P0 允许子 Agent 调 `ask_user`，按 §4.3 通过 `childRequest(kind=question)` 上浮，进入 `pending_questions`，但不改变主 Agent 的 status；answer Update 按 question_id 找到对应 Agent，子 Agent 的问题由 room signal 子工作流的 `answer`。validator 改成"question_id 在 pending_questions 里"。 |
| 为什么用 Update 不用 signal | 需求原文写的是 "Temporal signal"。现有 `runTurn`、`decide` 都是 Update（signal 加同步返回），control 靠返回的 `texts` 落库消息（`app.go:354-363,373`）。如果 answer 用 signal，就拿不到这些文字。所以选 Update；如果坚持用 signal，就必须先让 control 从 ingest 的 `assistant.message` 落库（见 §13 待决 1）。 |
| control HTTP | `POST /v1/rooms/{roomId}/questions/{questionId}/answer`，请求 `{choiceIds?: string[], text?: string}`（至少一个非空；`allowCustom=false` 时 `text` 必须为空；choiceIds 必须属于 choices）。响应 200 `{room: Room, question: Question, approval: Approval|null}`。错误：404 `NOT_FOUND`；409 `QUESTION_NOT_PENDING`（control 先 query room 的 `pending_questions` 校验，同 §6.2）；400 `BAD_REQUEST`。Room 增加 `pendingQuestions: Question[]`（v1 是单个对象）。`Question = {id, roomId, agentId, parentSessionId, text, choices[{id,label}], allowCustom, status: pending|answered|cancelled, answer?, createdAt}`。 |
| control 状态 | `RoomState` 增加 `awaiting_external`（**spec 和 web 都要改**）。这个状态下 `POST /messages` 返回 409 `ROOM_AWAITING_INPUT`，不会把消息当成答案。 |

验收：
- S-R3-1：mock 输入 "ask me"（需要给 mock 加脚本）后，room.state 为 `awaiting_external`，`pendingQuestions[0].id` 非空；SSE 在 2 秒内收到 `question.asked`，带 `agentId="main"`。
- S-R3-2：用 `text="其他：蓝色"` 调 answer，得到 200；activity 里依次出现 `question.answered` 和 `assistant.message`；worker 侧最后一条 `ToolResultBlock(name="ask_user")` 的 output 含 "蓝色"。
- S-R3-3：同一个 questionId 再 answer 一次返回 409；`allowCustom=false` 时带 text 返回 400。
- S-R3-4：挂起状态下 `POST /messages` 返回 409 `ROOM_AWAITING_INPUT`。
- S-R3-5：挂起时 abort，得到 `question.answered{errorCode:QUESTION_CANCELLED}`，room 变为 `closed`。
- S-R3-6（v2）：子 Agent 调 `ask_user`，`pendingQuestions` 里出现一项，`agentId=<child>`、`parentSessionId=<主 session>`，主 Agent 的 room.state 不变；answer 之后子 Agent 继续执行，它的 `ToolResultBlock(ask_user)` 含答案。

---

## 4. R1 子 Agent 树（P0）

### 4.1 身份与事件
- 每个 Agent 有一个 `AgentRef`。主 Agent：`agent_id="main"`，`parent_*=None`，`depth=0`。子 Agent 的 id 由 room 生成：`ag-{workflow.uuid4().hex[:12]}`（确定性），Temporal 子工作流 id 为 `room:<roomId>/agent/<agentId>`，替换 `workflows.py:393` 基于 call_id 的写法。
- worker 在一个会话里发出的**所有**事件（`session.status, assistant.message, tool.call, tool.result, approval.asked, question.asked, todo.updated, usage, agent.started, agent.finished`）都带 `agentId/parentAgentId/parentSessionId/depth/persona`，来源是 `OpenSessionInput.agent`，要写进 `SessionBlob`，之后每个 activity 都从 blob 读。
- `agent.started`：`text` 不再放 session_id，改为子 Agent 的 prompt 摘要（≤200 字）。`agent.finished` 带 `status=<AgentFinishReason>`。
- 补齐 activity 内工具的 `tool.call/tool.result`：`_drive` 遇到每个 `ToolCallBlock`/`ToolResultBlock` 都发事件。今天只有外部工具才有 `tool.call`（`runtime.py:325-326`），P0 标准 1 要求"工具调用实时出现"。

### 4.2 `GET /v1/rooms/{roomId}/agents`（spec 已有，代码没注册）
响应 200：
```json
{ "items": [ { "agentId": "main", "sessionId": "…", "parentAgentId": null, "parentSessionId": null,
    "depth": 0, "persona": "", "state": "running", "summary": "", "startedAt": "…",
    "finishedAt": null, "finishReason": null, "pendingApprovalCount": 0, "childCount": 2 } ] }
```
`state ∈ running|awaiting_approval|awaiting_external|finished`，`finishReason ∈ AgentFinishReason`。数据来源：control 在 `Ingest` 时维护一张**独立的** per-room agent 投影表，按 `agent.started/finished/approval.asked/approval.resolved/question.*` 增量更新。不能每次从 activity 重算，因为 activity 每个 room 只保留 500 条（`app.go:31,685-687`）。返回扁平列表加父指针，树由客户端组装。room 不存在时返回 404。

### 4.3 执行模型（路径 a 的强化版：扁平 Temporal 树 + 逻辑父子）
- **只用 Temporal 子工作流实现委派，禁止使用 `agentscope.app` 的 Team 或调度器**（v2.1，C19）。它的模板允许 worker 权限比 leader 宽，而且会引入第二套持久化和调度。
- **所有**子 Agent 和孙 Agent 都是 RoomWorkflow 的**直接** Temporal 子工作流，逻辑树靠 `parent_agent_id` 表达。原因：(1) 第 9 条要求 room 级的活跃数由 room workflow 持有，Python SDK 的 workflow 不能对别的 workflow 调 Update，只能 signal 或 cancel；扁平结构下所有计数和启动都在一个地方完成。(2) abort 时只需要取消一层。
- `AgentRunWorkflow` 从"只跑一轮"改成完整循环（patch id：`orbit-agent-run-loop`）：`runTurn` 之后按 status 分支：
  - `needs_external` 且工具是 `gateway_*`：子 Agent 自己执行 `gatewayExecute`（风险已经在 worker 闸门里判过）。
  - 工具是 `agent_spawn/agent_wait/team_dissolve/agent_send`，或者 `needs_approval`，或者 `ask_user`：通过 `get_external_workflow_handle(room_workflow_id).signal("childRequest", ChildRequest{agent, kind, payload})` 交给 room，然后 `wait_condition` 等 room 回 `childResponse` signal，再 `deliverToolResult`（或者等 `resolveApproval`）。
  - `completed`：返回文本并 close。
- `agent_wait` 改成只等**调用者自己的**直接子 Agent。`agent_send` 投递到子 Agent 的 `incoming`，由子 Agent 在下一轮作为用户消息消费，不再是空操作。

### 4.4 验收
- S-R1-1：mock "spawn two" 场景（`tests/test_room_workflow.py:128-153`）下，`GET /agents` 返回 3 项；两个子 Agent 的 `parentSessionId` 等于主 sessionId，`depth=1`，`persona="worker"`（agent_spawn 参数里的值）。
- S-R1-2：room 内所有 worker 来源的 activity 都有非空 `agentId`；子 Agent 的每条事件都有 `parentSessionId`。
- S-R1-3：完成后两个子 Agent 都是 `state=finished, finishReason=completed`，并且各有一条 `agent.finished`。
- S-R1-4：同一个 room 连续两轮都 spawn（mock 的 call id 相同），不会因为子工作流 id 冲突而失败。
- S-R1-5：不存在的 room 调 `/agents` 返回 404；spec 里这个路径的 `x-unimplemented` 改为 false。

---

## 5. R4 待办（todo.updated）

- worker 内置工具 `todo_write(todos: TodoItem[])`，在 activity 内执行，`is_read_only=True`，不需要审批。**每次调用都传完整列表**，worker 整体替换 blob 里的 `todos[agent_id]`，`todo_revision += 1`，发出 `todo.updated {agentId, todos: <完整列表>, todoRevision}`。`TurnResult.todos` 带上本轮最终列表。
- 语义：幂等替换。消费方只保留 `todoRevision` 最大的那一份（按 agentId 区分），乱序或重复的事件没有影响。空列表 `[]` 表示清空，这和"没有事件"（不渲染待办浮层，显示空状态）不同。
- control：`GET /v1/rooms/{roomId}/todos` 返回 200 `{items: [{agentId, revision, todos: TodoItem[], updatedAt}]}`，数据来自 Ingest 投影。
- 验收：S-R4-1：连续两次 todo_write（3 项，然后 2 项）后，SSE 收到两条 `todo.updated`，第二条恰好 2 项；`GET /todos` 返回 2 项，revision=2。S-R4-2：同一事件重放两次，`/todos` 结果不变。S-R4-3：没调过 todo_write 的 room，`/todos` 返回 `{items: []}`。

---

## 6. 子 Agent 的权限、审批、中止与委派上限（P0，第 6–9 条）

### 6.1 子 Agent 权限不超过父 Agent（第 6 条）
- 子 Agent 的 `AgentRunInput.permission` 只是**初值**，取自 room 当前的快照。**v2（S-M2）**：子 Agent 在运行期间始终跟随 room 当前的快照：
  1. room 保存 `_permission`，也就是最近一次 control Update 带来的快照，包括新增的 `permissionChanged` Update（control 在规则新建或撤销时发送，§8）。每次更新后，room 向所有活跃子 Agent signal `permission(snapshot)`。
  2. 子 Agent 和 room 的每次 `childRequest`/`childResponse` 交互都带上 room 当前快照，子 Agent 以收到的最新 `revision` 为准（旧 revision 忽略）。
  3. 子 Agent 的每个 activity 在开始时使用当时已知的最新快照。已经在运行的 activity 不会中途换快照；"下一次调用"指的是新快照送达之后开始的第一个 activity 里发起的调用。停车和外部工具都会结束当前 activity，所以实际延迟最多一个 activity。
  4. **交集（v2.1，C19）**：子 Agent 的有效快照 = `intersect(父 Agent 的有效快照, 任务（room）当前快照, spawn 时请求的模板)`。preset 取最严的（read-only < workspace-write < danger-full-access）；allow 规则取交集（请求模板只能从父级已有的规则里挑）；deny 规则（如果以后引入）取并集。每次收到新快照时重新计算。结果只含规则数据，不含凭证。`agent_spawn` 可以带 `permissionPreset`，但只能**收窄**：顺序为 `read-only < workspace-write < danger-full-access`，超过父级就 clamp 到父级，并在 `agent.started.status` 里记 `permission_clamped`。rules 只继承，不能新增；子 Agent 不会自己产生 `allowed-always`，只有 control 能签发规则。
- worker 闸门对子会话同样生效：destructive 工具仍然停车；rejected 的调用不会执行（§7）。
- 验收：S-6-1：read-only 的 room 里 spawn 时要求 `danger-full-access`，子 Agent 实际拿到的快照 preset 是 `read-only`，它调写工具时得到 `PERMISSION_DENIED`，没有停车，gateway activity 执行 0 次。S-6-2：workspace-write 的 room 里，子 Agent 调 destructive 工具时停车，出现 `approval.asked{agentId=<child>}`。**S-6-3（v2）**：对 sensitive 工具 `gated_echo` 做 allow-always（scope=room），子 Agent 在运行中调用它，不停车；`DELETE /v1/approval-rules/{id}` 之后，子 Agent 下一次调 gated_echo 会发出 `approval.asked{agentId=<child>}`，工具在决定之前执行 0 次；Temporal history 里能看到 room 向子 Agent 发了 `permission` signal，revision 递增。

### 6.2 子 Agent 的审批在同一个任务台上显示（第 7 条）
- 子 Agent 的 `needs_approval` 通过 `childRequest(kind=approval)` 上浮到 room。room 把它放进 `pending_approvals`，**不改变**主 Agent 的 status。今天 `_approval` 只能存一个（`workflows.py:148,473`），改成一个列表。
- worker 发出 `approval.asked`，带 `agentId/parentSessionId/approvalRequestId/risk`。control 在 **Ingest** 时按 `approvalRequestId` upsert Approval（今天只在 runTurn 返回时创建，`app.go:375-395`，子 Agent 的审批走不到那里）。
- **ingest 鉴权（v2，S-4）**：只要 `TEMPORAL_ADDRESS` 有值，control 启动时就要求 `ORBIT_INTERNAL_TOKEN` 非空，否则拒绝启动；`Authorized` 不再对 loopback 放行（放行只保留给没有 Temporal 的本地开发模式）。worker 配了 `ORBIT_EVENT_INGEST_URL` 却没有 token 时拒绝启动。token 只从环境变量读，不进 payload 和日志。
- **decide 前校验（v2，S-4）**：control 收到 decide 后，先 query room workflow 的 `pendingApprovals`（新 query，返回 id、agentId、turnId）。不在列表里就返回 409 `APPROVAL_NOT_PENDING`，不会调 Update。因为 `approval.asked` 由 activity 发出，可能比 workflow 状态更早到达，所以 query 在 2 秒内每 200ms 重试一次，之后才返回 409。伪造的 ingest 事件最多只能生成一张点不动的卡片，不会产生任何执行。
- 解决审批只有一条路：`POST /v1/approvals/{id}/decide` → control → room 的 `decide` Update。room 按 `approval_request_id` 找到对应的 Agent：主 Agent 走原路径；子 Agent 则由 room signal 子工作流的 `resolve`，Update 立刻返回 `{decision, agentId, turn: null}`。子 Agent 没有 decide 或 answer 的公共入口，control 也没有任何 API 能直接指向子工作流 id。子 Agent 的 `resolve` 只接受它自己正在等的 `approval_request_id`，其他的都忽略并记 warning。注意：拥有 Temporal namespace 权限的人仍然可以直接 signal，这属于内部信任边界。
- Approval（control 与 spec）新增：`agentId, parentAgentId, parentSessionId, turnId, callId, arguments, argumentsTruncated, risk, allowAlways, ruleId?`。**v2**：`status ∈ pending|allowed|rejected|cancelled`（原来是 `pending|decided`），保留 `decision`。非 pending 的审批再 decide 一律返回 409。
- 验收：S-7-1：子 Agent 停车后，`GET /v1/approvals` 里有一条 pending，`agentId` 是子 Agent 的，`roomId` 就是本 room；主 Agent 的 room.state 不受影响。S-7-2：decide(allow) 后，子 Agent 继续执行，出现 `approval.resolved` 和子 Agent 的 `tool.result`。S-7-3：直接对子工作流 signal 一个陌生的 `approval_request_id`，子 Agent 不会继续执行，工具执行 0 次。**S-7-4（v2）**：不带 token 或带错误 token POST `/internal/events` 伪造 `approval.asked`，返回 401；带正确 token 但 id 不在 workflow pending 列表里时，卡片可能出现，但 decide 返回 409，Temporal 里没有 `decide` Update。

### 6.3 父 Agent 中止后级联停止（第 8 条）
**要达到的结果**（和机制无关）：父 Agent（主 Agent 或任意子 Agent）被中止后，它的**所有后代 Agent 都停止，并且不再发起任何工具调用**。它们的 pending 审批和问题作废，每个后代 Agent 都有一条终结事件。

两种实现路径：
- **(a) Temporal 子工作流，也就是当前代码用的路径（`workflows.py:394`）**：
  1. 所有子 Agent 启动时显式设置 `parent_close_policy=REQUEST_CANCEL`（room 被 terminate 或超时时也会请求取消子 Agent），room `_abort` 对每个活跃子 Agent `cancel()`，并在有界时间内（例如 30 秒）等待它们结束。
  2. **activity 必须发 heartbeat（v2，F2）**：`runTurn/resolveApproval/deliverToolResult/steer/answer` 这些 activity 一开始就启动一个**后台 asyncio 任务**，每 `ORBIT_HEARTBEAT_INTERVAL_S`（默认 10 秒）调一次 `activity.heartbeat()`，**和模型有没有输出无关**。模型驱动（`reply_stream` 消费）放在另一个任务里。调度时设 `heartbeat_timeout=30s`、`cancellation_type=WAIT_CANCELLATION_COMPLETED`。`_agent()` 显式设置 `ReActConfig(interruption_raise_cancelled_error=True)`（v2.1，C17）。AgentScope 默认会吞掉 `CancelledError`，把取消变成正常完成；设置这一项后，Temporal 的取消才会作为 `CancelledError` 抛出。收到取消（`CancelledError`）时：先取消模型任务（让底层 HTTP 请求中止），再取消 heartbeat 任务，然后发送 `UserInterruptEvent`，保存状态，把 blob 标成 `aborting` 后退出。activity 正常结束时也要取消 heartbeat 任务。只在 `reply_stream` 事件上发 heartbeat 是不够的：模型调用可能长时间没有输出。不发 heartbeat，取消信号就到不了正在运行的 activity（§1.3）。
  3. `AgentRunWorkflow` 捕获取消后，在不可取消的范围里执行 `closeSession(reason=aborted)`，发出 `agent.finished{status:aborted}`。
  4. **worker 兜底闸门**：blob 为 `closed/aborting` 的会话，任何工具调用一律返回 `SESSION_ABORTED`，这样可以覆盖取消的竞态窗口。
- **(b) activity 内的 AgentScope 子 Agent（当前没有用）**：如果将来改成在一个 activity 内部起子 Agent，那么这个 activity 必须发 heartbeat；收到取消时由它负责停掉自己启动的所有后代（逐个 interrupt 并等待）后再返回。room 级计数仍然必须通过 workflow 状态维护（见 6.4），不能只存在 activity 内存里。
- 结果状态和事件：每个后代 Agent 发 `agent.finished{status:"aborted"}` 和 `session.status "closed"`；pending 的 approval 和 question 变成 `cancelled`（`approval.resolved{status:cancelled}` 和 `question.answered{errorCode:QUESTION_CANCELLED}`）。room 变成 `closed`，`/agents` 里没有 `state=running` 的项。只中止某个子 Agent（`team_dissolve`）时，reason 为 `dissolved`，同样级联到它的后代。
- 验收：S-8-1：主 Agent 有 2 个子 Agent、1 个孙 Agent，其中孙 Agent 正在执行一个 mock 慢工具（sleep 10 秒，逐秒发 heartbeat）。abort 之后 5 秒内，4 个 Agent 都有 `agent.finished{aborted}`；慢工具的副作用文件没有生成；Temporal 里这 3 个子工作流都是 Canceled，不是 Running。S-8-2：abort 60 秒后，worker 日志里这些 session 没有新的 `tool.call`。S-8-3：abort 时 pending 的子 Agent 审批变成 `cancelled`，对它 decide 返回 409。**S-8-4（v2）**：mock 模型单次调用阻塞 45 秒，期间不产生任何输出（heartbeat_timeout=30s）。activity 正常完成，没有 heartbeat timeout，也没有重试（history 里 attempt=1）。**S-8-5（v2，v2.1 加强）**：在这 45 秒阻塞中 abort，activity 在 heartbeat 间隔加 5 秒内结束，mock 记录到模型请求被取消，之后没有 `tool.call`；**Temporal history 里该 activity 的结果是 `ActivityTaskCanceled`，不是 `ActivityTaskCompleted`**；blob 里保存了 interrupted 状态。

### 6.4 委派上限（第 9 条，已定）
- 默认值：`max_depth=2`（main→child→grandchild）、`max_children_per_parent=4`（同时 running）、`max_active_agents=8`（整个 room 活跃的子孙 Agent 总数）。硬上限由 workflow 常量兜底（已有 `_HARD_DEPTH=4`、`_HARD_FANOUT=8`，`workflows.py:46-47`，另加 `_HARD_ACTIVE=16`），control 下发的值会被 clamp 到硬上限以内。
- 计数**只存在于 RoomWorkflow 状态里**：`_active: dict[agent_id, AgentRef]` 和 `_running_children: dict[parent_agent_id, int]`。子 Agent 结束时（handle 完成或取消），room 减计数。孙 Agent 的 spawn 也经过 room（§4.3），所以计数不会分散到别的 workflow 或 activity。
- 检查顺序：depth → per-parent → room active。超限时**不排队**：room 用 `result_state="error"`、`output='{"code":"DELEGATION_LIMIT_EXCEEDED","limit":"max_active_agents","max":8,"current":8}'` 和 `error_code=DELEGATION_LIMIT_EXCEEDED` 交回调用方；worker 在 deliverToolResult 时发 `agent.spawn_rejected{agentId=<调用方>, limit, limitMax, limitCurrent}` 和 `tool.result{errorCode}`。
- 可配置：`POST /v1/rooms` 可以带 `delegation: {maxDepth?, maxChildrenPerParent?, maxActiveAgents?}`，control 做校验（1..硬上限），然后通过 `RoomWorkflowInput` 的 `maxDepth/maxFanout/maxActiveAgents` 下发（camelCase 别名，Go 端补上）。Room 响应里带 `delegation`。
- 语义变化（**BREAKING，只影响 workflow 内部**）：`max_fanout` 从"累计数"改成"并发数"，`max_depth` 的判断从 `depth+1 >= max_depth` 改成 `new_depth <= max_depth`。放在 patch `orbit-room-delegation-v2` 后面，旧的执行保持原逻辑。
- 验收：S-9-1：mock 要求 spawn 5 个且不 wait，第 5 次得到 tool.result `errorCode=DELEGATION_LIMIT_EXCEEDED, limit=max_children_per_parent`，并有对应的 `agent.spawn_rejected`；Temporal 里只有 4 个子工作流。S-9-2：孙 Agent 再 spawn（depth 3），得到 `limit=max_depth`。S-9-3：4 个子 Agent 各 spawn 2 个孙 Agent，第 9 个活跃 Agent 被拒，`limit=max_active_agents`。S-9-4：一个子 Agent 结束后再 spawn，可以成功（证明计的是并发数）。S-9-5：创建 room 时 `delegation.maxDepth=9`，返回 400。

---

## 7. R-a 权限快照与风险闸门

- **策略和已记住的审批只存在 orbit-control。** control 每次发起 `runTurn/decide/answer/steer` Update 时都现算一份 `PermissionSnapshot`（room preset 加上匹配本 room 或本 persona 的有效规则），放进 Update 参数。workflow 只把它**透传**给本轮所有 activity（包括 `_after_turn` 里的 deliverToolResult 循环），并继承给子 Agent，不做任何策略判断。worker 在**每个** activity 开始时用快照覆盖 `agent.state.permission_context`，写回 blob 前把 rules 剥掉（blob 里只留 preset），所以撤销的规则不会残留。快照为 None 时退回今天的行为（只看 blob 里的 preset），和旧版 control 兼容。
- **风险分级（v2：默认拒绝，S-M3）**：每个工具**必须显式**声明 `orbit_risk: ToolRisk` 和 `always_ask: bool`，没有推断，也没有默认值。
  - 注册时：worker 在 `_drive` 注册工具的地方（今天在 `runtime.py:248-258`）检查，缺声明就抛异常，这个 activity 失败。
  - 运行时（v2.1 拆分，C25）：
    - **内置工具**（代码里注册的）没有声明：注册失败，运行时一律 `PERMISSION_DENIED`，由 CI 检查兜底。
    - **动态 MCP 工具**（运行时从 connector 注册，P1）没有标注：按 **destructive + always_ask** 处理，任何 preset 下都会停车（read-only 下仍然是 `PERMISSION_DENIED`），不提供"总是允许"。connector 配置里可以显式标注，把级别调低。
    - **任何人都不能放宽默认拒绝检查**：CI 测试和闸门判断不允许配置开关或 allowlist 旁路；修改它们需要 Sentinel 评审。
  - CI：新增测试，枚举 `orbit_tools()`、`gated_echo`、`ask_user`、`todo_write`，以及跑一次 `_drive` 之后 toolkit 里的全部工具，逐个断言显式声明了 `orbit_risk`（检查类属性本身，不是继承来的默认值）。缺声明时 CI 失败。
  - 各级含义：`read` 只读；`write` 工作区内可回退的写；`sensitive` 有外部副作用但不是破坏性的（发消息、gateway 写查询等），会停车，**可以**记住；`destructive` 是不可逆的（删除、覆盖、转账），会停车，**永远不能**记住。
  - 现有工具：`gateway_charge` = destructive + always_ask；`gated_echo` = **sensitive**（v1 是 destructive，改动是为了让 allow-always 有测试对象）；`gateway_lookup` = read；`agent_spawn/agent_send/agent_wait/team_dissolve` = write；`ask_user`、`todo_write` = read。
  - 闸门同时覆盖外部工具：外部工具今天一律 ALLOW（`tools.py:17-26`），改成先过闸门，再发 `RequireExternalExecution`。

| preset \ 风险 | read | write | sensitive | destructive | always_ask（任意级别） | 内置工具未声明 |
|---|---|---|---|---|---|---|
| read-only | 允许 | `PERMISSION_DENIED`（不停车） | `PERMISSION_DENIED` | `PERMISSION_DENIED` | `PERMISSION_DENIED` | `PERMISSION_DENIED` |
| workspace-write | 允许 | 允许 | 停车；有匹配规则则允许 | 停车（不能记住） | 停车（不能记住） | `PERMISSION_DENIED` |
| danger-full-access | 允许 | 允许 | 允许 | 允许 | **停车**（不能记住） | `PERMISSION_DENIED` |

  动态 MCP 工具未标注时，按 destructive + always_ask 那一列处理。

  规则只能把 sensitive 的"停车"变成"允许"，永远不能越过"拒绝"，也不适用于 destructive 和 always_ask。
- **always_ask 在 BYPASS 下也必须停车（v2，S-6，原 Q2）**：Orbit 闸门是一层包装，在 AgentScope 的权限判断**之前**执行，不依赖 `PermissionMode`。即使 danger-full-access 映射成 `PermissionMode.BYPASS`（`runtime.py:59`），always_ask 工具也一定发出 `RequireUserConfirm` 和 `approval.asked`。如果测试发现 AgentScope 在 BYPASS 下跳过了工具的 ASK，就由包装层直接产生停车结果，不交给 AgentScope 判断。
- **审批卡参数（v2，S-7）**：worker 生成 `ApprovalAsk.arguments` 时做两步。(1) 脱敏：键名命中 `_SECRET_KEYS`（`secrets.py`）、值以 `sk-` 开头、值形如 `Bearer …`、或者是长度 ≥32 的高熵串，都替换成 `[REDACTED]`。和 `reject_secret_values` 不同，这里只替换，不抛异常，卡片照常生成。(2) 截断：单个值超过 512 字符、总长超过 4 KB 时截断，加上 `…(truncated)`，并设置 `arguments_truncated=true`。执行用的是原始参数，展示用的是处理后的副本。前端只按**纯文本**渲染（不解析 Markdown 或 HTML，不自动转链接），并标注"以下为 Agent 提供的参数"，防止提示注入。
- **拒绝的调用永远不会执行（v2 产品规则 N1）**：拒绝 = 工具不执行 + 任务**不关闭** + 明确告诉 Agent"用户拒绝了"，Agent 继续这一轮；审批卡变成 `rejected`，不可再点（再 decide 返回 409）；"停止"是独立的 `POST /abort`，和拒绝无关。`resolveApproval(rejected)` 后，worker 保证这个 call 的结果是 `ToolResultBlock(state=ERROR, output='{"code":"APPROVAL_REJECTED","message":"用户拒绝了这次工具调用，请不要重试同样的调用，换一种方式继续或询问用户。"}')`。外部工具不会产生 `needs_external`，activity 内工具的函数体不会被调用。worker 发 `tool.result{errorCode:APPROVAL_REJECTED}` 和 `approval.resolved{status:rejected}`，Agent 继续这一轮，room 回到 `running`。**BREAKING（control 行为）**：control 不再因为 reject 关闭 room（删除 `app.go:428-438` 里的 closed 分支），spec `openapi.yaml:353-356,899` 的描述同步修改。
- 新增 `approval.resolved {approvalRequestId, agentId, status: allowed-once|allowed-always|rejected|cancelled, ruleId?}`，由 control 在 decide 成功后发出。
- 验收：S-Ra-1：workspace-write 下 "charge"，decide(reject) 后，gateway activity 执行次数为 0（Temporal history 里没有 `gatewayExecute`），出现 `tool.result.errorCode=APPROVAL_REJECTED`，room.state 为 `running`（不是 closed）。S-Ra-2：read-only 下 "gated"，直接得到 `PERMISSION_DENIED`，没有 approval 记录。S-Ra-3：每个 runTurn activity 的输入里 `permission.preset` 等于 room preset，`revision` 单调递增（查 Temporal history）。S-Ra-4：任何 workflow history 或事件里都搜不到规则以外的策略数据或密钥（配合 §10 的检查）。**S-Ra-5（v2，属于 PR-A2）**：danger-full-access 的 room 里 "charge"，发出 `approval.asked{toolName:gateway_charge}`，room 进入 `awaiting_approval`，决定之前 `gatewayExecute` 执行 0 次。**S-Ra-6（v2）**：给某个工具去掉 `orbit_risk` 后跑 CI，风险声明测试失败；运行时调用这个未声明的工具，得到 `PERMISSION_DENIED`。**S-Ra-7（v2）**：参数里有 `{"token":"sk-abc…"}` 时，卡片照常生成，`arguments.token=="[REDACTED]"`；有 10 KB 参数时 `argumentsTruncated=true`，卡片参数总长不超过 4 KB。**S-Ra-9（v2.1）**：mock 一次发出两个 gated_echo（call_id 不同），得到两张审批卡，两个 approvalRequestId 不同。**S-Ra-10（v2.1）**：只批准第一张，只有第一个调用执行（1 条 `tool.result`），第二张仍然 pending，room 保持 `awaiting_approval`。**S-Ra-11（v2.1）**：一张允许、一张拒绝，被允许的执行一次，被拒绝的执行 0 次，并得到 `APPROVAL_REJECTED`。**S-Ra-12（v2.1）**：mock 在同一次响应里发出两个相同 call_id 的调用，worker 拒绝这个 turn（报错，工具执行 0 次）。**S-Ra-13（v2.1）**：注册一个没有标注的动态 MCP 工具（测试桩），danger-full-access 下调用会停车，卡片 `allowAlways=false`。**S-Ra-8（v2）**：reject 之后审批卡 `status=rejected`，再 decide 返回 409，room.state 为 `running`，Agent 的下一条 `assistant.message` 存在。

## 8. R-b 已记住的审批（allowed-always）

- **作用域选择**：默认 `scope=room`（只在本任务有效，影响范围最小，适合主要用户群，也就是业务用户，在一次任务里不被反复打断）。如果 room 绑定了 persona，可以选 `scope=persona`（同一个助理下的新任务也生效，给重复性工作用）。**不提供**用户全局或项目级作用域：项目属于 P1，全局规则风险太大。以后加 `scope=project` 时是纯新增枚举值。
- **适用范围（v2，N2）**：只有 `risk=sensitive` 并且不是 always_ask 的工具才提供"总是允许"（`allowAlways=true`）。destructive 和扣费类（always_ask）**完全不提供**这个选项，decide 时带 allow-always 返回 400 `ALLOW_ALWAYS_NOT_PERMITTED`。
- **规则内容（v2，S-5）**：`tool_name` + `argument_pattern`。
  - 默认：用这次调用的**精确参数**生成模式，每个值都做 glob 转义。
  - 任意参数（`any_arguments=true`，`argument_pattern={}`）必须由用户在确认卡上做**二次选择**（"对这个工具的所有参数都允许"），请求里显式带 `rule.anyArguments:true`。
  - 也可以提交自定义模式（高级用法，P0 UI 可以不做）。
- **匹配规则（v2，S-5）**：
  - 工具的 `input_schema` 里标了 `format: "path"` 的参数，匹配前先规范化：转成 POSIX，按工作区根目录解析相对路径，`normpath` 消除 `.`、`..` 和 `//`；worker 侧再做 `realpath`，把符号链接解析成真实路径。结果不在工作区内的路径**永远不匹配**，也就是会重新询问。模式本身也按同样的方法规范化。
  - 匹配使用分段 glob：`*` 不跨越 `/`，跨目录必须写 `**`。非路径参数用精确匹配或 fnmatch。
  - 所有匹配都在 worker 闸门里完成，control 只负责保存和下发规则。
- HTTP：
  - `POST /v1/approvals/{id}/decide` 的请求改为 `{decision: "allow"|"reject"|"allow-always", rule?: {argumentPattern?: object, anyArguments?: boolean, scope?: "room"|"persona"}}`。不传 `rule` 时，默认使用当前参数的精确模式。control 先保存规则（内存加 `approval-rules/<id>.json`；P0 接受重启后丢失，见 §13），再调 decide Update（`outcome=allowed-always, ruleId`），然后向 room 发 `permissionChanged` Update；本次调用放行，相当于 allowed-once。响应是 Approval（带 `ruleId`）。
  - `GET /v1/approval-rules?roomId=&personaId=` 返回 200 `{items: ApprovalRule[]}`（camelCase，外加 `createdAt, createdFromApprovalId`）。
  - `DELETE /v1/approval-rules/{ruleId}` 返回 200 `{revoked: true, effectiveFrom: "next-turn"}`；不存在时 404。**v2.1（C23，S-Rb-8）**：同时删除 `approval-rules/<id>.json`；文件删除失败时返回 500，不返回 200，避免以后实现启动加载时规则"复活"。撤销成功后，前端必须提示"从下一轮生效"（C26）。
- **撤销从下一轮生效**：已经发出的快照在当前 activity 内继续有效。control 在 DELETE 成功后立即向 room 发 `permissionChanged` Update，room 更新 `_permission`，并 signal 给所有活跃子 Agent（§6.1）。主 Agent 和子 Agent 在下一个 activity 开始时都使用新快照。room 已经关闭或不存在时，control 忽略 Update 返回的错误，DELETE 仍然返回 200。
- 验收：S-Rb-1：对 gated_echo 执行 allow-always 后，下一条消息再触发 gated_echo，不停车，直接出现 `tool.result`。S-Rb-2：DELETE 这条规则，再下一条消息触发时重新出现 `approval.asked`（对应 P0 成功标准 2）。S-Rb-3：对 gateway_charge 执行 allow-always 返回 400。S-Rb-4：scope=room 的规则在另一个 room 里无效。**S-Rb-5（v2）**：默认 allow-always（不传 rule）只放行完全相同的参数，参数变化时重新询问。**S-Rb-6（v2）**：path 参数的精确规则是 `docs/a.txt` 时，调用 `docs/../secret.txt` 或指向工作区外的符号链接，都会重新询问。**S-Rb-8（v2.1，High）**：撤销一条规则 → 重启 control → `GET /v1/approval-rules` 里没有它，`.orbit-data/approval-rules/<id>.json` 不存在 → 下一次 gated_echo 重新询问。**S-Rb-7（v2）**：对 destructive 工具（例如给 mock 加的 `delete_file`）带 allow-always 返回 400，审批卡上 `allowAlways=false`。

---

## 9. 建房间的健壮性

- **`Idempotency-Key` 请求头**（可选，≤128 字符）：control 保存 `key → {roomId, requestHash}`（内存加 FileStore `idempotency/<sha256(key)>.json`，TTL 24 小时）。**v2（S-Q4）**：control 启动时必须**重新加载**未过期的 idempotency 记录，以及它们指向的 `rooms/<id>.json`（今天 `persistRoom` 已经在写这个文件，`catalog.go:274-277`，但从来不读）。这样重启后用同一个 key 重放，拿到的 room 依然存在，不会是 404。同一个 key 加同样的 body，返回原来的 room（200，响应头 `Idempotent-Replayed: true`；即使原 room 是 `failed` 也原样返回）。同一个 key 但 body 不同，返回 409 `IDEMPOTENCY_KEY_REUSED`。并发的相同 key 按 key 串行处理。CORS 的 allow-headers 加上 `idempotency-key`（`handler.go:43`）。Temporal 侧：workflow id 就是 `room:<roomId>`，重放时 `ExecuteWorkflow` 如果返回 `WorkflowExecutionAlreadyStarted`，视为成功并继续轮询 view。
  - C32 rev2：如果 key 对应的任务已经软删除，重放时返回和"任务不存在"相同的 404，不返回原来的 task_id（§18.7a）。
- **失败状态**：新增 `RoomState = "failed"`（终态，只在 control 里出现；workflow 的 `RoomStatus` 不加），Room 增加 `failure?: {code: "WORKFLOW_START_FAILED"|"SESSION_TIMEOUT", message}`。触发条件：`StartRoom` 返回错误，或 30 秒轮询超时（`orch/client.go:98-110`）。超时时还要 best-effort 调用 `TerminateWorkflow(room:<id>)`，防止 session 稍后出现，变成孤儿会话。failed room 要落盘（调 `persistRoom`），列表里能看到，`/messages` 对它返回 409 `ROOM_FAILED`。
  - **为什么用新状态而不是另加一个 status 字段**：今天失败被记成 `closed`，web 会显示成"已完成"（`orbit-web/src/model.ts:55`），把失败说成了成功。`failed` 是 room 生命周期里真实存在的终态，用 enum 表达最直接，客户端的 switch 能强制处理。如果另加字段，所有读 `state` 的地方都得记得再查一次这个字段。
- **502 带上 roomId**：`ErrorBody` 增加可选的 `roomId`（spec 里 `additionalProperties:false`，所以必须把字段写进 schema，`openapi.yaml:616-633`）。`POST /v1/rooms` 失败时返回 `502 {code:"WORKER_ERROR", message, roomId}`，这时 room 已经是 `failed`。body 校验失败仍然是 400，不带 roomId，也不会创建 room。
- 验收：S-RC-1：同一个 Idempotency-Key 连发两次，只产生一个 roomId，Temporal 里只有一个 `room:<id>`。S-RC-2：同一个 key、不同 body，返回 409。S-RC-3：停掉 orch（或者用一个不存在的 task queue）后建房间，返回 502，body.roomId 非空；`GET /v1/rooms/{roomId}` 返回 `state=failed`，带 failure.code；30 秒后 Temporal 里没有这个 room 的 Running 工作流。S-RC-4：浏览器预检 OPTIONS 放行 `Idempotency-Key`。**S-RC-5（v2）**：建房间后重启 control，用同一个 key 重放，返回同一个 roomId（200，带 `Idempotent-Replayed: true`），`GET /v1/rooms/{roomId}` 返回 200。

## 10. 模型配置与模型失败（C28 更新，来自 orbit-runtime#3）

### 10.1 环境变量（只在 worker 里读）
| 变量 | 说明 |
|---|---|
| `ORBIT_MODEL_MODE` | `mock`（默认）或 `real` |
| `ORBIT_MODEL_BASE_URL` | real 模式必填；OpenAI 兼容 endpoint |
| `ORBIT_MODEL_API_KEY` | real 模式必填；**只从 worker 环境变量读取**，永远不进 Temporal payload 和 history、事件、日志、AgentState |
| `ORBIT_MODEL_NAME` | real 模式必填；默认模型名 |
| `ORBIT_MODEL_TIMEOUT_SECONDS` | 可选，默认 60 |

- real 模式下，上面三个必填变量任一缺失或无效（例如 URL 解析失败、超时值不是正数）时，worker **启动即退出**（非 0），错误信息**只包含变量名**，不包含变量值。
- mock 模式启动时打印 WARNING 日志 `chat model: mock`。
- orch 和 control 不持有模型密钥。契约里只传**模型名**：`RoomWorkflowInput.model`、`OpenSessionInput.model`、`AgentRunInput.model`，默认空，表示使用 `ORBIT_MODEL_NAME`。`reject_secret_values`（`orbit_worker/secrets.py`）已经覆盖 blob，这次扩展到所有外发事件，并在 activity 输入进入时断言。
- **mock 标记**：`TurnResult`、所有 worker 事件，以及 `runTurn`/`decide`/`answer` 这些 Update 的返回里都带 `modelMode`（`mock|real`）和 `modelName`，前端据此显示 mock 标记（P0 成功标准 5）。control 把它们投影到 `Room.runtime.modelMode/modelName`。

### 10.2 模型失败
- **`TurnStatus` 增加 `failed`**。模型调用失败时，本轮返回 `status="failed"`：**state 保持在原来的 `state_version`**（失败的 turn 不写 blob），**不回退到 mock**。room workflow 收到 `failed` 后回到失败前的状态：一般是 `running`；如果失败发生在 approve 或 deliverToolResult 之后的续跑中，就保持原来的停车状态，见下面的已知限制。
- **新事件 `turn.failed`**（对齐 orbit-runtime#3 head `3ec4204`）：字段包在 `failure` 对象里，`{type:"turn.failed", …信封字段, failure:{turnId, agentId, errorCode, retryable, message}}`。同时再发一条人类可读的 `session.status`，`text="turn failed: …"`（内容就是固定文案，只用于活动时间线；前端判断时不能依赖它）。TurnResult 同样带 `errorCode` 和 `retryable`。
  - **agentId 现状**：#3 里 `failure.agentId` 目前填的是 Orbit **session id**。A1 会改成契约里的 agent id（主 Agent 为 `"main"`），并加上 `agentPath`。在那之前，消费方不要把它当作 agent id 使用。
- **固定文案（Nexus）**：`failure.message` 必须**完全等于**下表中的一项：

| errorCode | message（完全一致） | 按钮 |
|---|---|---|
| `timeout` | 模型响应超时，这一轮没跑完。 | 重试 |
| `rate_limited` | 模型当前请求太多，请稍后再试。 | 重试 |
| `provider_error` | 模型服务暂时出错，这一轮没跑完。 | 重试 |
| `auth` | 模型配置有问题，请联系管理员。 | 无 |
| `config` | 模型配置有问题，请联系管理员。 | 无 |
| `state_unreadable` | 这个任务的会话记录无法读取，请新建任务继续。 | 无（C34；文案待 Nexus 确认） |


| errorCode | 触发条件 | retryable |
|---|---|---|
| `timeout` | 超过 `ORBIT_MODEL_TIMEOUT_SECONDS` | true |
| `auth` | HTTP 401/403 | **false** |
| `rate_limited` | HTTP 429 | true |
| `provider_error` | HTTP 5xx 或连接错误 | true |
| `config` | HTTP 400/404（模型名、路径错误等） | **false** |

- 前端**只读取** `errorCode`、`retryable`、`turnId`、`agentId` 这几个字段，**不解析** `message` 文本。
- **`failure.message` 只能是上面固定文案表中的一项**（由 worker 按 errorCode 选择），**永远不透传 provider 的原始响应或异常文本**，也不能包含密钥、host、URL 或请求体。S-RM-1 会断言 message 属于预定义文案集合。
- **P0 中 Temporal 不自动重试模型错误**：这一轮里工具可能已经执行过，重试会重复副作用。所以模型错误在 worker 里转换成 `status=failed` 正常返回，不抛异常，不会触发 activity 的 RetryPolicy。
- **P1**：只针对 `rate_limited` 和 `timeout`，并且**本轮还没有执行任何工具**时，允许在 worker 内做有上限的退避重试。
- **已知限制（交给 A2 的幂等键处理，§2.3）**：(1) approve 之后模型失败，用户重新批准时，工具会再执行一次；(2) `deliverToolResult` 失败时，room 停在 `awaiting_external`。A2 用基于 call_id 的幂等键（下游去重）解决第 (1) 条，并为第 (2) 条提供"重新投递"路径（同一个 call_id 和幂等键）。
- **已知后续事项**：
  - **S-RM-2**：Qwen 在触发内容安全和上下文超长时都返回 HTTP 400，目前都被映射成 `config`（不可重试，提示联系管理员），这个提示不准确。P1 会新增单独的 errorCode（例如 `content_filtered`、`context_overflow`），同时补充固定文案。
  - **S-RM-3**：不是 `ModelRequestError` 的解析异常（例如响应格式错误）目前会变成 activity 失败，被 RetryPolicy 重试 3 次，违反"P0 不自动重试模型错误"。A1 把它们映射成 `provider_error`（返回 `status=failed`），并补上测试。
- **"重新批准"的上线条件**：A2 的幂等键合并并且 S-Ra-10 签字之前，前端不提供"重新批准"，只提示"这一步没完成，请停止后重新发起"，并提供"停止"按钮（§14）。
- **上线顺序**：PR-B 让 control 认识 `failed` 和 `turn.failed` 之前，**real 模式只用于 dev 环境**。

### 10.3 其他
- **`state_unreadable`（C34，orbit-runtime#6）**：worker 读不出会话保存的 AgentState 时使用，包括生产环境里的明文 blob，以及当前 `ORBIT_STATE_KEY` 解不开的 blob。`openSession` 和 `runTurn` 遇到它时，本轮 `status=failed`、`errorCode=state_unreadable`、`retryable=false`，不改 blob。`closeSession` 和 `abort` 遇到它时按**幂等成功**处理，让用户始终能停止和关闭任务。只有 `StateUnreadableError` 走这条路径；数据库连接和认证错误照常抛出，交给 Temporal 重试。blob 解密成功但模型校验失败（`ValidationError`）时，也算 `state_unreadable`，但要单独记 ERROR 日志，和密钥问题区分开。P0 不做密钥轮换，密钥丢失会让所有已有会话变成 `state_unreadable`。orbit-web 从生成的类型导入这个值，不能手写字符串。
- 顺带修正：`grantEnv` 明文目前在直连 worker 模式下会进 HTTP payload（`app.go:250-252`），和"secrets 不进 payload"相冲突。这条路径对当前 runtime 已经失效（§1.9），建议 PR-B 直接删掉。Temporal 模式下 grant 下发只传 `grantId` 引用，放到 P1。

### 10.4 验收
- S-M-1：用一个假 key（形如 `sk-test…`）启动 real 模式，跑完一轮后，用 `temporal workflow show` 导出 history，对它、control 的 `.orbit-data/audit/*.jsonl` 和各容器日志执行 `grep -r "sk-test"`，都没有结果。
- S-M-2：mock 模式下，SSE 事件和 Update 返回都带 `modelMode=mock`；worker 启动日志里有 WARNING `chat model: mock`。
- S-M-3（C28）：real 模式下去掉 `ORBIT_MODEL_API_KEY`，worker 非 0 退出，输出里有变量名，没有任何变量值。（不变）
- **S-SU-1（C34）**：用密钥 A 写入会话，换成密钥 B 重启 worker 后发一轮消息，得到 `turn.failed{errorCode:state_unreadable, retryable:false}`，文案和固定文案表一致，blob 不变。**S-SU-2**：同样的场景下调 `closeSession` 和 `abort`，都返回成功，重复调用也成功，room 变为 `closed`。**S-SU-3**：打桩让数据库连接失败，activity 抛错并被 Temporal 重试，不会变成 `state_unreadable`。
- **S-M-4（C33，流式脱敏）**：
  - 一个密钥被拆在相邻的两个 `assistant.delta` 分片里，发出去的事件里它被替换成 `[REDACTED]`；脱敏只做替换，**永远不抛异常**，也不会中断这一轮。
  - 一段很长的纯中文文本（没有空格），在 block 结束之前至少发出 **2 条** delta（不能因为等空格或分词边界而一直攒着）。
  - 一个夹在中文里的密钥被拆在两个分片里，同样被替换成 `[REDACTED]`，前后的中文保持完整。
- **S-RM-1（C28，orbit-runtime#3 离线测试）**：用桩服务分别模拟 401、429、500 和超时。每种情况都断言：`status=failed`，errorCode 分别为 `auth/rate_limited/provider_error/timeout`，retryable 正确；blob 的 `state_version` 和内容不变；错误、事件和 DEBUG 日志里都不出现 key 和 host；`turn.failed.failure.message` 和固定文案表中的某一项完全一致，不包含 provider 原文；还有一条 `session.status "turn failed: …"`；`MockChatModel` 没有被调用。

## 11. spec 漂移修正（以 spec 为准，`orbit-control/docs/openapi.yaml`）

| 漂移 | 处理 |
|---|---|
| `/v1/mcp-connectors` GET/POST 只在代码里有（`handler.go:254-274`） | 写进 spec：`McpConnector{id,name,command,args[],envRefs[],createdAt}`，POST 请求 `{name,command,args?,envRefs?}`（`catalog.go:20-27,126-147`） |
| `POST /v1/grants` 只在代码里有（`handler.go:275-295`） | 写进 spec：请求 `{env: {NAME: value}, ttlSeconds?}`，响应 `GrantPublic{id,envNames,expiresAt}`，说明"值永不回显"。**v2.1（C27）**：标为 internal，必须带 `Authorization: Bearer <ORBIT_INTERNAL_TOKEN>`，否则返回 401；Caddy 公网入口不转发这个接口 |
| `/v1/cloud-agents` 标着 unimplemented（`openapi.yaml:45-46,492-525`） | GET/POST 改成 `x-unimplemented:false`，注明"POST 只登记 `queued`，还不会启动 CloudAgentJob"（`catalog.go:197-230`）；`/{agentId}` 和 `/cancel` 保持 unimplemented（代码里没有） |
| persona 字段：spec 叫 `mcpServerIds`（`openapi.yaml:909`），代码叫 `mcpConnectorIds`（`handler.go:241`、`catalog.go:16`） | 统一为 `mcpConnectorIds`；`Persona` 响应 schema 补全；`POST /personas` 的 summary 去掉 "(unimplemented)"，响应从 501 改成 200（`openapi.yaml:385-410`） |
| `CreateRoomRequest` 缺 `grantId`（代码接受，`handler.go:100`） | 补上，同时加入 `delegation` |
| `RoomState` 缺新状态 | 加入 `awaiting_external, failed`（§3、§9） |
| `decideApproval` 描述写着 reject→abort | 改成 §7 的语义；`DecideApprovalRequest.decision` 加入 `allow-always` 和 `rule` |
| `createRoom` 502 引用的是 NotImplemented 示例 | 改成 `WorkerError`（ErrorBody+roomId） |
| info.description 写着"Control calls orbit-worker HTTP activities directly" | 改成"只经由 RoomWorkflow（Temporal）"；直连模式标 deprecated |
| 新接口 | `/v1/rooms/{id}/agents`（实现）、`/todos`、`/questions/{qid}/answer`、`/v1/approval-rules`（GET）、`/v1/approval-rules/{ruleId}`（DELETE） |

---

## 12. 拆分后的 PR（v2：runtime 拆成 3 个 + control 1 个，F1）

**前置**：orbit-runtime 上正在进行的真模型 PR（orbit-runtime#3，会改 `runtime.py`，引入 `ORBIT_MODEL_*`、`failed` 和 `turn.failed`，见 §10）**先合并**，A1 在它之上 rebase。PR-B 要处理 `TurnStatus=failed` 和 `turn.failed`（投影到 Room 和失败卡），并把 `modelMode/modelName` 投影到 `Room.runtime`；PR-B 上线之前 real 模式只用于 dev（C28）。

合并顺序：真模型 PR → **A1 → A2 → A3**（orbit-runtime，依次合并，每一个都向后兼容，都可以单独部署）→ **PR-B**（orbit-control）。

**PR-A1：事件修复（解锁 `/events` 实时流）**
- `orbit_contracts`：`OrbitEvent` 改 camelCase 别名，并加上 §2.1 的字段；加入 `AgentRef`、`ToolErrorCode`；重新导出 schema。
- `orbit_worker`：`HttpEventIngest` 用 `by_alias` 修复；**所有事件带 `agentId`/`agentPath`**（A1 里都是 `"main"`，parent 为空）；`toolName` 不再写进 `text`；activity 内工具也发 `tool.call` 和 `tool.result`（带 `toolState/argsPreview`）；v2.1：从 `ModelCallEndEvent` 发 `usage`，发 `assistant.delta`（C18、C20）。
- 兼容性：旧版 control 能直接用（它本来就读 camelCase）；新增的事件类型在 A1 里还不会发出。
- 验收：S-R1-2（只看 main 部分）；P0 标准 1"工具调用 2 秒内出现"在现有 control 上就能验证。

**PR-A2：权限、审批、追问、待办（单 Agent）**
- 模型：`PermissionSnapshot/ApprovalRule/QuestionAsk/TodoItem`，`RunTurnRequest/DecideRequest/AnswerRequest/PermissionChangedRequest`，`ResolveApprovalInput.outcome` 加 `allowed-always`。
- worker（v2.1）：**并行停车列表化**、逐个决定、部分回填后继续停车、拒绝重复 call_id（§2.2）；external 的 `results[]` 批量回填；gateway 幂等键（§2.3）。
- worker：快照覆盖；Orbit 风险闸门（默认拒绝，外加 BYPASS 下 always_ask 仍然停车）；`orbit_risk` 声明和 CI 测试；`APPROVAL_REJECTED` 保证和拒绝文案；§2.0 的 id 规则和 turn_id 复用检测；参数脱敏和截断；路径规范化匹配；`ask_user`、`todo_write`。
- orch：Update 参数改成模型；`answer`、`permissionChanged` Update；`pendingApprovals`、`pendingQuestions` query；patch `orbit-room-ask-user`。
- mock 脚本：ask、todo、repeat-call-id、slow-tool、delete_file（destructive）。
- 兼容性：快照为 None 时退回 preset；旧版 control 不调新 Update，也能照常运行。
- 验收：S-ID-1、S-PP-1..3、S-R3-1..5、S-R4-*、S-Ra-1..13、S-Rb-1..8（其中需要 control 的部分在 PR-B 之后验证）。

**PR-A3：子 Agent（P0 委派）**
- `AgentRunWorkflow` 完整循环（patch `orbit-agent-run-loop`）；扁平子工作流加逻辑树；`childRequest/childResponse/resolve/answer/permission` signal；子 Agent 的审批、追问、快照刷新；委派上限（patch `orbit-room-delegation-v2`）和 `agent.spawn_rejected`；`parent_close_policy=REQUEST_CANCEL`；后台 heartbeat（10 秒）和取消，**显式设置 `ReActConfig(interruption_raise_cancelled_error=True)`**；子 Agent 权限取交集；`SESSION_ABORTED` 闸门；级联中止。
- 验收：S-R1-*、S-R3-6、S-6-*、S-7-1..3、S-8-*、S-9-*。

**后续 A4（建议，不阻塞 P0）**：task queue 拆分（`orbit-agent`、`orbit-sandbox`）、continue-as-new、activity 内子版本检查点（§2.3）；SOP（§15，P1）。

**PR-B：orbit-control（A1–A3 部署之后）**
1. spec 改动（§11）先行，作为验收依据。
2. `Ingest` 接受新事件类型；`ActivityEvent` 扩展；agent、todo、question、approval 投影。有 Temporal 时 token 必填，不再对 loopback 放行。
3. decide/answer 之前先 query 校验（409）；Approval.status 新枚举；reject 不关闭 room；allow-always（默认精确参数、anyArguments、destructive 返回 400）。
4. 新接口：`/agents`、`/todos`、`/questions/{qid}/answer`、`/v1/approval-rules`（GET/DELETE），外加 `permissionChanged` 下发。
5. 快照的计算和下发（runTurn、decide、answer、steer、permissionChanged）。
6. Idempotency-Key（持久化，启动时重新加载 idempotency 记录和对应的 room）、`failed` 状态、502 带 roomId、CORS；`delegation` 的校验和下发。
7. 删除直连 worker 的路径（至少删掉 grantEnv 明文）。
- v2.1：`DELETE /v1/approval-rules` 同时删除文件（S-Rb-8）；`/v1/grants` 需要 internal token（C27）；`approval.resolved` 事件；`assistant.delta` 只走 SSE，不入库。
- 验收：S-7-4、S-Rb-8、S-RC-*、S-M-*、**S-G-1（v2.1）：不带 internal token POST `/v1/grants` 返回 401，带正确 token 返回 200**，以及 A2、A3 里依赖 control 的那部分。

orbit-infra 只需要更新 submodule 指针；部署时确认 `ORBIT_INTERNAL_TOKEN` 已经配好（compose 和 prod env）。

## 13. 待决问题（v2 更新）

已裁定：
- ~~Q2~~ → 硬要求，见 §7（S-6）。
- ~~Q4~~ → P0 接受规则在重启后丢失；Idempotency key 必须持久化并在启动时重新加载，指向的 room 也要一起加载（§9，S-Q4）。
- ~~Q6~~ → P0 允许子 Agent 调 `ask_user`，卡片显示发起的子 Agent（§3，S-Q6）。
- ~~`/v1/grants` 是否 internal~~ → internal，需要 token，否则 401（C27）。
- ~~sensitive/destructive 谁定、MCP 怎么办~~ → 内置工具在代码里声明，缺声明时 CI 失败；MCP 工具未标注时按 destructive + always_ask（C25）。

仍然待决：
1. R3 的答案用 Update（本草案）还是 signal？如果用 signal，control 需要改成从 ingest 的 `assistant.message` 落库消息。
2. `ConfirmResult(confirmed=False)` 之后，AgentScope 生成的拒绝结果文本能不能控制？如果不能，worker 在 `_drive` 里改写成 §7 规定的 `APPROVAL_REJECTED` 文案。
4. `todo_write` 放在 activity 内由 worker 维护（本草案的选择），还是作为外部工具由 workflow 持有？
5. ~~（v2 新增）`sensitive` 和 `destructive` 的分级由谁定？~~（v2.1 已裁定，见上）P0 内置工具在代码里声明；MCP 工具（P1）需要在 connector 配置里声明，没有声明就按默认拒绝处理。需要产品确认这对 MCP 接入的体验影响。
6. （v2 新增）规则在重启后丢失时，UI 的"已记住的审批"页也会变空。需不需要给用户一个提示？还是 PR-B 顺手在启动时加载规则（成本很低，因为已经落盘）？
8. （v2.1 新增）AgentScope 能否接受部分 `confirm_results`（§2.2）；不能的话，需要在包装层单独执行被批准的调用，实现复杂度上升。
9. （v2.1 新增）调研建议 spawn 时**冻结**子 Agent 快照（AS §6.4），本草案按 Sentinel M2 改成"随 room 刷新，并且每次重新取交集"。两者冲突的地方已经按 M2 处理，请调研作者确认没有遗漏的风险。
7. （v2 新增）控制面 decide 前的 query 最多重试 2 秒。如果 worker 到 workflow 的延迟更大，会误报 409。需要在 PR-B 压测里确认这个阈值。

## 14. orbit-web 兼容与迁移

- **模型失败卡（C28）**：只根据 `turn.failed` 或 TurnResult 里的 `errorCode/retryable` 渲染，不解析错误文本。
  - 文案以 §10.2 的固定文案表为准（前端可以直接显示 `failure.message`，或者按 errorCode 查同一张表）。`auth`、`config`："模型配置有问题，请联系管理员。"，**没有**按钮；`timeout`、`rate_limited`、`provider_error`：显示对应文案和"重试"。
  - 其他（retryable=true）："重试"按钮重新发送最后一条用户消息，**使用新的 turnId**。
  - room 处于 `awaiting_approval` 时，按钮是"重新批准"，不是"重试"。
  - **上线规则（C28）**：A2 的幂等键合并并且 S-Ra-10 签字**之前**，前端**不显示"重新批准"**（重新批准可能让工具执行两次，见 §10.2 已知限制）。卡在这个状态的 turn 显示"这一步没完成，请停止后重新发起"，只有一个"停止"按钮（调用 `/abort`）。A2 合并之后再启用"重新批准"。
  - 页面始终根据 `modelMode` 显示 mock 或 real 标记。
- `RoomState`（`orbit-web/src/model.ts:3`）要加 `awaiting_external | failed`，`stateLabel` 补上"等你回答"和"启动失败"。在 web 上线之前，未知状态显示成通用的"进行中"，不要落到"已完成"。
- `roomCreateAlert`（`src/lib/rooms.ts:109-115`）：body 里有 `roomId` 时直接跳到那个任务，显示失败卡；没有时保留现在的文案。建房间请求带上 `Idempotency-Key`（每次点击生成一个 uuid，超时重试时复用），这样 45 秒超时后重试不会再开一个房间。
- `decideApproval(…, "allow"|"reject")` 保持兼容；新增 `"allow-always"`，只在 `allowAlways=true` 时显示。默认按精确参数记住；"所有参数都允许"要做成二次选择（`anyArguments:true`）。**reject 之后 room 保持 running**：卡片变成"已拒绝"，不可再点，继续显示 Agent 的后续回复；"停止"按钮仍然走 `/abort`，和拒绝无关。
- Approval.status 从 `decided` 改成 `allowed|rejected|cancelled`。web 目前只判断 `=== "pending"`（`src/store.ts:272`），所以不受影响；显示已决定的卡片时要按新值渲染。409 `APPROVAL_NOT_PENDING` 要显示成"这张卡已失效"。
- **每个调用一张审批卡，逐个决定；P0 不做"全部允许"按钮**（C22）。已决定的卡在同一 turn 其他卡未决时显示"已允许，等待其他确认"。撤销"总是允许"成功后提示**"从下一轮生效"**（C26）。
- `assistant.delta` 用来显示流式草稿，`activityAttempt` 变大时清空草稿；`approval.decided` 已更名为 `approval.resolved`。
- 审批卡参数和追问卡文字**只按纯文本渲染**（不用 Markdown 或 HTML，不自动转链接），显示 `[REDACTED]` 和截断标记。
- 追问卡：`pendingQuestions` 是数组，卡片上显示发起的 Agent（子 Agent 显示 persona 或 agentId）。
- `ActivityEvent` 只加可选字段，旧代码忽略即可。子 Agent 委派卡和子线程面板用 `agentId/parentSessionId` 加 `GET /agents`；待办浮层用 `todo.updated` 或 `GET /todos`；追问卡用 `question.asked` 和 answer 接口。
- 审批列表里会出现子 Agent 的审批（带 `agentId`），任务台要标出它来自哪个子 Agent。
- persona 表单字段固定为 `mcpConnectorIds`。
- 术语：UI 一律叫「任务」，代码和 API 保持 `room`（room = 任务）。侧栏的「项目（含任务组）」在 P0 继续置灰，标「未接入」，**没有接口**；资料库（团队空间、我的文档）同样置灰。本草案不给它们提供接口，也不增加 `taskGroup` 或 `space` 字段。多 Agent 不设单独的入口或房间类型：在当前任务里用 @ 拉入子 Agent。

## 15. SOP 与计划事件（P1，场景 5，不在 A1–A3）

- 产品定位：SOP 属于 P1（Nexus：场景 5）。本节只占位，P0 不实现，也不进 A1–A3。
- 方向（取自 AS §6.5）：SOP = 版本化模板（数据，按 `sop_id@version` 固定），由 Temporal 的 `SopWorkflow` 解释执行，每一步一个 `runStep` activity（带 structured_schema、工具白名单、预算）→ 可选 verifier → 可选审批 → 下一步。模板变更不需要 `workflow.patched`，解释器逻辑变更才需要。
- 预留事件（P1 定稿）：`plan.updated {agentPath, tasks:[{id, subject, state, owner, blockedBy}], version}`（全量快照，来自 AgentState.tasks_context）；`sop.step.started|completed|failed {sopId, version, stepId, attempt, outputRef}`。
- 和 R4 的关系：P0 的 `todo.updated`（`todo_write`）保持不变；P1 如果引入 AgentScope Task 工具和 `plan.updated`，再决定是否合并这两个事件。

## 16. 产物事件与读取接口 (C30)

字段参考 SourceWeft packages/contracts (Apache-2.0)，仅借鉴设计，未复制代码。

### 16.1 事件：`artifact.created` / `artifact.updated`
- **只在提交后发出**：runtime 把一个产物版本**成功持久化**（committed）之后，才为这个版本发**一次**事件。第一个版本发 `artifact.created`，之后的版本发 `artifact.updated`。持久化失败时**不发**产物事件，失败通过 `tool.result`（`toolState=error`）或 `turn.failed` 表达。`tool.call`/`tool.result` 只表示进度，不代表产物已经存在。
- **事件不带内容**，只带元数据。内容只能通过 §16.2 的读取接口获取。
- 载荷字段（camelCase；和 §2.1 的信封字段并列，`type` 取上面两个值之一）：

| 字段 | 类型 | 规则 |
|---|---|---|
| `eventId` | string | **确定性生成**：`artifact:{turnId}:{artifactId}:{version}`。消费方按它去重（activity 重试重发同一事件时 eventId 不变） |
| `artifactId` | string | 在任务内唯一，版本之间不变 |
| `version` | int ≥ 1 | 同一个 artifact 内**严格递增**。ingest 幂等规则（C30a）：eventId **和** contentDigest 都与已存的相同 → 200，**不做任何事**（不入库，不广播）；同一 version 但 digest 不同，或者 version 低于当前最新版本 → 409 `artifact_version_conflict`（不投影，不转发） |
| `parentVersion` | int \| null | 上一版本；首版为 null |
| `title` | string | 显示名；前端按纯文本渲染 |
| `mimeType` | string | **由服务端按内容嗅探决定**，永远不采用模型或工具声明的值 |
| `sizeBytes` | int | 持久化后的真实字节数 |
| `contentDigest` | string | `sha256:<hex>` |
| `previewable` | bool | 由服务端决定：超过单个产物的大小上限，或者类型不可预览时为 false |
| `taskId` | string | 等于 roomId（room = 任务，C29） |
| `turnId` | string | 产生这个版本的 turn |
| `sourceToolCallId` | string \| null | 产生它的工具调用；不是由工具产生的就是 null |
| `agentId` | string | 同 A1（主 Agent 为 `"main"`） |
| `agentPath` | string | 格式同 A1（例如 `main/ag-x`） |

- §2.1 的 `OrbitEvent.type` 增加 `artifact.created`、`artifact.updated`，并加入上表中的可选字段；control 的 `workerEventTypes`（`app.go:565-574`）同步增加。产物事件写进 activity（它们是持久记录，不像 `assistant.delta` 只走 SSE）。

### 16.2 读取接口（orbit-control）
- `GET /v1/artifacts/{artifactId}` 返回 200 `{artifactId, taskId, title, latestVersion, versions:[{version, parentVersion, mimeType, sizeBytes, contentDigest, previewable, turnId, sourceToolCallId, agentId, agentPath, createdAt}]}`。
- `GET /v1/artifacts/{artifactId}/versions/{version}` 返回 200，响应体是内容字节，`Content-Type` 等于嗅探得到的 `mimeType`，外加 `ETag: "<contentDigest>"`。
- **需要鉴权**：依赖 §17（C31）。现状：control 目前没有用户鉴权，也没有租户或任务成员的概念（`openapi.yaml` 的 `security: []`），所以在 §17 落地之前，这两个接口只能在 dev 环境开放。
- **安全要求（Sentinel，必须有测试）**：
  1. 分两类，**永远不返回 403**（C30a，Sentinel 已确认）：
     - **未登录**：返回 **401**。鉴权中间件在**任何数据库查询之前**拒绝请求，所以不管 artifact 是否存在、属于哪个租户，状态码和响应体**字节完全相同**。
     - **已登录，但无权访问或对象不存在**：跨租户、不是任务成员（P0 指不是创建者）、artifact 不存在、version 不存在，这四种情况返回**同一个 404**（`NOT_FOUND`，响应体字节完全相同）。
  2. 所有响应都带 `X-Content-Type-Options: nosniff`。
  3. HTML、SVG 以及其他可以执行脚本的类型（按嗅探结果判断，例如 `text/html`、`image/svg+xml`、`application/xhtml+xml`、`text/xml`、`application/javascript`），返回 `Content-Disposition: attachment` 和 `Content-Security-Policy: sandbox; default-src 'none'`。
  4. **预览路径（P0 决定，C30a）**：
     - 前端用**带鉴权的读取接口** fetch 内容。`fetch` 不受 `Content-Disposition: attachment` 影响，所以能拿到内容。
     - 可以执行脚本的类型，渲染进 `<iframe sandbox="allow-scripts" referrerpolicy="no-referrer" srcdoc=…>`，**不带** `allow-same-origin`。这样 frame 的 origin 是 opaque 的，读不到控制台的 cookie、DOM 或 storage。
     - 前端插入的 meta CSP 必须是 srcdoc 的**第一个节点**，内容为 `default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data: blob:; font-src data:; connect-src 'none'; form-action 'none'`（最低要求是前四项）。
     - `sandbox` 属性**只能是** `allow-scripts`，**不能包含** `allow-same-origin`、`allow-popups`、`allow-top-navigation`（包括它的各种变体）或 `allow-forms`。
     - 前端通过 **DOM 属性**赋值（`iframe.srcdoc = html`），**绝不**用字符串拼出包含 iframe 的 HTML 标记。
     - 只在 `previewable=true` 时预览。直接在浏览器里打开读取 URL 仍然是下载（attachment）。
     - **独立的预览 origin 加 inline 接口推迟到 P1**，替换原来"必须独立 origin"的要求。
     - 格式处理：docx 转成的 HTML 必须先过 **DOMPurify**，再放进 srcdoc；PDF 用**最新版 pdf.js**，并设置 `isEvalSupported: false`；xlsx **不能用** npm 上的 `xlsx@0.18.5`，放在 **Web Worker** 里解析，并限制大小。
  5. 单个产物有大小上限，可以通过 `ORBIT_ARTIFACT_MAX_BYTES` 配置（worker 和 control 读同一个值）。超过上限时事件**照常发出**，但 `previewable=false`。
  6. 子 Agent 产生的产物，访问权限不超过父任务：权限按 `taskId` 判断，和 agentPath 无关。这一条在 **S-9** 场景里测试（例如在 S-9-1 的子 Agent 产物上，用非任务成员访问得到 404）。
- 验收：
  - S-AR-1：一次成功写入，只收到 1 条 `artifact.created`，eventId 符合 `artifact:{turnId}:{artifactId}:1`。activity 重试后用相同的 eventId 和 digest 重发，control 返回 **200**，不做任何事：activity、audit 和 SSE 里都只有 1 条。
  - S-AR-2：写入失败时没有产物事件，只有 `tool.result` 错误或 `turn.failed`。
  - S-AR-3：同一个 artifact 依次 ingest：v1（200，存储并广播）→ v2（200，存储并广播）→ v2、digest 相同（**200，不做任何事**）→ v2、digest 不同（**409 `artifact_version_conflict`**）→ v1（**409 `artifact_version_conflict`**）。最终只有 v1、v2 两个版本，共 2 条事件。
  - S-AR-4：工具声明 `text/plain`，实际内容是 HTML，事件和接口里的 mimeType 都是 `text/html`，下载时是 attachment 加 CSP sandbox。
  - **S-AR-5a**：未登录分别请求一个存在的 artifact 和一个不存在的 artifact（元数据接口和版本接口都测），都返回 401，响应体**字节完全相同**；数据库查询计数为 0（用 mock store 或查询日志断言）。
  - **S-AR-5b**：已登录时跨租户、不是创建者、artifact 不存在、version 不存在，这四种情况都返回 404，响应体字节完全相同。
  - S-AR-6：所有响应都有 nosniff 头。
  - S-AR-7：超过 `ORBIT_ARTIFACT_MAX_BYTES` 时事件照常发出，`previewable=false`。
  - S-AR-8（重写）：预览 iframe 的 `sandbox` 属性**恰好是** `allow-scripts`，没有 `allow-same-origin`。预览里的脚本读 `document.cookie` 为空或抛错，访问 `parent.document` 抛 SecurityError，访问 `localStorage` 抛错；在预览里 `fetch(任意 URL)` 被 CSP 拦截（会有 securitypolicyviolation 事件）。sandbox 属性里没有 `allow-popups`、`allow-top-navigation*`、`allow-forms`、`allow-same-origin`。产物脚本调用 `window.open()` 返回 null，也不会出现新窗口；执行 `top.location = …` 会抛错，控制台的 URL 不变。srcdoc 的**第一个节点**是那条 meta CSP，其中至少包含最低要求的四项。代码审查和单元测试确认 srcdoc 通过 `iframe.srcdoc =` 赋值，没有拼接 iframe 的 HTML。`previewable=false` 的产物不渲染 iframe。

### 16.3 实现拆分
- **runtime PR（事件）**：放在 A1 合并之后（依赖 camelCase 信封和 `agentId/agentPath`），负责嗅探、digest、大小判断、持久化后发事件。
- **control PR（接口加鉴权测试）**：ingest 版本校验、产物投影、两个读取接口、安全头，以及 S-AR-5..8；依赖鉴权落地。
- **SSE 鉴权**：`/v1/rooms/{id}/events` 适用同样的规则。未登录时在**建立流之前**直接返回 401（不写 `text/event-stream` 头，也不先打开流再关闭）；已登录但没有权限时同样在建流之前返回 404。
- **Last-Event-ID 的 control PR 还要交付（C33）**：原样透传校验通过的 worker 事件（包括 `assistant.delta`、`turn.failed`、`usage`）；`assistant.delta` 不计入 500 条活动历史。
- **依赖**：control 的 `/events` SSE 必须支持 `Last-Event-ID` 续传（游标语义见 §18.4：每个任务自己的 `seq`）（断线重连之后补发错过的事件，包括产物事件）。现状：SSE 只推送新事件，不支持续传（`handler.go:176-202`）。
- 上线顺序（C31）：control 产物读取 PR 放在 auth PR（§17）之后。
- 聊天流 UI 不受影响，可以先做；**产物面板**要等这两个 PR 都合并后再接入。

## 17. 登录鉴权与任务归属 (C31)

状态：**草案**（P0，Nexus 决定；未签字）。

### 17.1 现状
- control 没有用户鉴权：spec 是 `security: []`（`openapi.yaml:31`），CORS 是 `Access-Control-Allow-Origin: *`（`handler.go:41`），所有 room 对所有调用方可见（`ListRooms`，`app.go:271-280`）。健康检查路径是 `/health`（`handler.go:88`），不是 `/healthz`。本节沿用 `/health`，如果要改名，放进 spec 漂移修正里处理。
- `/internal/*` 已经有 `ORBIT_INTERNAL_TOKEN`（C15），保持不变。

### 17.2 登录（control 作为 BFF）
- 走 **OIDC Authorization Code + PKCE**，对接企业 IdP。接口：
  - `GET /auth/login`：生成 state、nonce 和 PKCE verifier（`code_challenge_method=S256`，三者都放在服务端的临时存储里，并和登录前的临时 cookie 绑定，10 分钟过期），302 跳转到 IdP。
  - `GET /auth/callback`：**必须**依次校验 **state**（缺失或不匹配就失败）、用 code 加 **PKCE verifier** 换 token、**校验 id_token 的签名以及 `iss/aud/exp/nonce`**（JWKS 从 issuer 发现，nonce 缺失或不匹配就失败）。任何一步失败都不建立 session，返回登录失败页。成功后**重新生成 session id**：丢弃登录前的任何 session 或临时 id，签发新的随机 id，防止 session 固定攻击。然后 302 回到控制台。
  - **登录后回跳**：`/auth/login` 可带 `returnTo`，只接受以单个 `/` 开头的同源相对路径（拒绝 `//`、`/\`、带协议或 host 的值，以及含控制字符的值），并存进登录前的临时状态，callback 只从那里取。不合规一律回到 `/`，防止开放重定向。
  - `POST /auth/logout`：删除服务端 session，清掉 cookie，返回 204。
  - `GET /v1/me`：返回 200 `{userId, tenantId, displayName, email}`；未登录返回 401。
- 用户身份键 = `(iss, sub)`，映射成内部 `userId`（第一次登录时创建）。email 或 displayName 只用于展示，不作为身份键。
- 环境变量：
  - `ORBIT_AUTH_MODE=oidc|local`。`local` 使用 dev 账号，**prod 拒绝**：`orbit-infra/scripts/prod-up.sh` 在 `ORBIT_AUTH_MODE` 不是 `oidc` 时直接退出；control 在 prod 配置下也拒绝以 local 模式启动。
  - `ORBIT_OIDC_ISSUER`、`ORBIT_OIDC_CLIENT_ID`、`ORBIT_OIDC_CLIENT_SECRET`、`ORBIT_OIDC_REDIRECT_URL`，在 prod 的 compose 里都用 `${VAR:?}`，缺失时启动失败。client secret 只存在 control 的环境变量里，不进日志和响应。

### 17.3 Session
- 服务端 session 存在 **Postgres** 里。cookie `orbit_session` 只放一个不透明的随机 id（≥128 bit），属性为 **`HttpOnly`、`Secure`、`SameSite=Lax`**、`Path=/`。只有 `ORBIT_AUTH_MODE=local` 的本地 dev 模式可以不设 `Secure`（因为本地是 http），其他模式一律必须设置。
- 登录成功后必须**重新生成 session id**（见 17.2）。
- 空闲超时 **12 小时**，绝对超时 **7 天**。logout 时**销毁服务端 session**（删除记录），旧 cookie 立即失效。
- **localStorage 里不存任何 token**；前端永远看不到 id token 或 access token。
- SSE（`/v1/rooms/{id}/events`）使用同一个 cookie；鉴权在建立流之前完成：未登录返回 401，无权访问返回 404，都不会打开流。因此 CORS 不能再用 `*`：改成只允许配置好的控制台 origin，并设置 `Access-Control-Allow-Credentials: true`。推荐和控制台同源部署，由 Caddy 反代。

### 17.4 CSRF
- **检查顺序固定**：先做登录校验（未登录返回 401），再做 CSRF 校验（返回 403），最后才做授权和查库（返回 404）。未登录的请求在所有路径上都得到同一个 401，与 S-AUTH-1 一致。
- 所有会改变状态的 `/v1` 请求（POST/PUT/PATCH/DELETE）都必须满足两个条件：`Origin` 属于允许的 origin，**并且**带 `X-Orbit-Request: 1` 头。任何一项不满足都返回 **403** `CSRF_REJECTED`。`/auth/callback` 靠 state 校验，不适用这条规则。

### 17.5 数据模型
- 所有持久化实体（task/room、turn、event、artifact、artifact version、session）都带 **`tenantId`**；task 另带 **`createdBy`**（userId）。
- P0 每个部署只有一个默认租户（`ORBIT_DEFAULT_TENANT`），但 schema 从一开始就是多租户的，P1 不需要迁移。
- `tenantId`、`createdBy` **只取自 session 和任务记录**：新建任务时从 session 取；之后的实体从所属任务取。**请求体**里的 `tenantId`/`createdBy`，以及 runtime 事件里的同名字段，一律忽略，不报错，也不采用。runtime/worker 永远看不到用户 session。worker → control 的内部接口继续使用 `ORBIT_INTERNAL_TOKEN`。

### 17.6 授权（P0）
- 用户能访问某个任务，以及它的全部事件、审批、问题、待办、agents 和产物，**当且仅当** `task.tenantId == session.tenantId` **并且** `task.createdBy == session.userId`。否则返回 **404**，响应体和"不存在"完全相同。
- `GET /v1/rooms` 只列出满足上面条件的任务。`POST /v1/approvals/{id}/decide`、answer、`/v1/approval-rules` 等接口，都先解析出所属任务，再套用同一条规则。
- **未登录**：`/v1/me` 以及所有其他 `/v1` 接口（任务、事件、SSE、产物等）一律返回 **401** `UNAUTHENTICATED`（**Sentinel 已确认**，取代 C30 原来的"未登录返回 404"）。鉴权中间件在**任何数据库查询之前**拒绝请求，状态码和响应体字节在所有路径上都相同，所以不泄露存在性信息。SSE 在建立流之前拒绝。
- P1（这里只列出，不展开）：任务成员、项目成员和角色、租户开通和管理后台、服务账号。

### 17.7 OpenAPI
- 在 `components.securitySchemes` 里新增 `cookieAuth`（`type: apiKey, in: cookie, name: orbit_session`），所有 `/v1/*` 路径都要求它（替换根级的 `security: []`）。`/internal/*` 使用内部 token scheme（`bearerAuth`）。`/health` 和 `/auth/*` 公开（`security: []`）。
- 401 和 403 响应改成真实会发出的响应，不再是 W1 的"保留形状"（`openapi.yaml:21-22`）。

### 17.8 验收 S-AUTH
- S-AUTH-1：未登录访问任务列表和详情、消息、事件 SSE、产物元数据和内容、审批，一律返回 401，响应体字节相同，数据库查询数为 0。SSE 返回普通的 401 JSON，没有 `Content-Type: text/event-stream`，也不会先建立流再断开。
- S-AUTH-2：用户 B 访问用户 A 的任务、事件、产物、审批，返回 404，响应体和不存在的 id 相同。
- S-AUTH-3：测试里预置两个租户，跨租户访问返回 404。
- S-AUTH-4：prod 配置加 `ORBIT_AUTH_MODE=local` 时，`prod-up.sh` 非 0 退出，control 拒绝启动；缺少任一 OIDC 变量时同样失败。
- S-AUTH-5：session 空闲超过 12 小时或者从创建起超过 7 天后，请求返回 401（用可注入的时钟测试）。
- S-AUTH-6：logout 之后用旧 cookie 请求返回 401，服务端 session 记录已经删除。
- S-AUTH-7：状态变更请求缺少 `X-Orbit-Request` 或 Origin 不对，返回 403；两者都正确时通过。
- S-AUTH-8：runtime 事件里伪造 `tenantId` 或 `createdBy`，control 忽略，投影结果以任务上盖的章为准。
- S-AUTH-9：`Set-Cookie` 包含 `HttpOnly; SameSite=Lax; Path=/`，prod 下包含 `Secure`；响应和前端存储里都没有 token。
- S-AUTH-10：id_token 的 iss、aud、nonce 不对或者已经过期，callback 失败，不建立 session。
- S-AUTH-11：登录前后 session id 不同（先拿一个登录前的 id，登录后的 `orbit_session` 和它不同，旧 id 不能用）。
- S-AUTH-12：callback 缺少 state，或者 state 不匹配，登录失败；nonce 缺失或不匹配，登录失败；PKCE verifier 不匹配，换 token 失败，登录失败。三种情况都不建立 session。
- S-AUTH-13：`POST /v1/rooms` 的请求体里带 `tenantId: "other"`、`createdBy: "u-evil"`，创建出的任务 tenantId 和 createdBy 仍然来自 session。其他接口带这两个字段时同样被忽略。
- S-AUTH-14：非 local 模式下 cookie 一定带 `Secure`；local 模式下可以不带，但仍然有 HttpOnly 和 SameSite=Lax。
- S-AUTH-15：`returnTo` 为 `https://evil.example`、`//evil.example`、`/\evil.example`、`javascript:...` 时，登录成功后一律跳到 `/`；为 `/rooms/abc` 时跳回原页面。
- S-AUTH-16：未登录并且缺少 CSRF 头的 POST 请求，返回 401，不返回 403。

### 17.9 上线顺序
- **auth PR**（orbit-control 和 orbit-infra 的 prod-up 检查）：在 A1 之后，**和 A2 并行**，并且在**存储地基 PR（§18）之后**，因为 session、users 和 oidc_login_state 都依赖它。
- control 的**产物读取 PR**（§16.3）在 auth PR 之后。
- PR-B 里所有新 `/v1` 接口都按本节的授权规则实现，auth 没合并之前只在 dev 环境开放。
- orbit-web：登录跳转、401 时回到 `/auth/login`、所有变更请求带 `X-Orbit-Request: 1`、fetch 使用 `credentials: "include"`。

## 18. control 持久化存储 (C32，草案)

状态：**草案**（已按 Sentinel 评审修订，见 C32 rev；Sentinel 同意了方向，**尚未签字**）。这是地基升级，需要 **Celestial 和 Sentinel** 签字。

### 18.1 现状（orbit-control main `846d27e`，已用 `gh` 核对，main 没有新提交）
- 所有状态都在 `App` 结构体的内存 map 里（`internal/app/app.go:117-133`），包括 `Rooms`、`Messages`、`Approvals`、`Activity`、`SessionRoom`、`Personas`、`McpConnectors`、`CloudAgents`、`grants`、`sequences`、`subs`。
- `FileStore`（`internal/store/file.go`）只写不读（C15、§1.7）：`rooms/<id>.json`、`audit/<roomId>.jsonl`、`personas/`、`mcp/`、`cloud-agents/`、`grants/`（只有公开视图）。数据目录是 `ORBIT_DATA_DIR`，compose 里是 `/var/lib/orbit`，挂在 `control-data` 卷上（`orbit-infra/compose.yaml:70-72`）。
- 真实字段：
  - `Room{id, kind, title, state, permissionPreset, runtime{kernel, protocol, isolation}, sessionId, createdAt}`（`app.go:53-62`）
  - `Message{id, roomId, role, type, text, createdAt}`（`:64-71`）
  - `Approval{id, roomId, sessionId, approvalRequestId, toolName, reason, status, decision, createdAt}`（`:73-83`）
  - `ActivityEvent{id, sequence, type, roomId, sessionId, turnId, source, runtime, protocol, role, text, toolName, callId, approvalId, approvalRequestId, reason, status, permissionPreset, occurredAt}`（`:87-107`）；**sequence 已经是每个 room 单独计数**（`a.sequences[roomID]++`，`:651`），只是重启后清零
  - `Persona`、`McpConnector`、`CloudAgentJob`（`catalog.go:12-52`）
- 事件类型白名单在 `workerEventTypes`（`app.go:565-574`）。**没有 Turn 结构**：turn id 是临时生成的 `tn_…`（`app.go:333,423`），不会保存。
- ID 都是带前缀的随机字符串（`rm_`、`msg_`、`ap_`、`ev_`、`persona_`、`mcp_`、`caj_`），所以表里的 ID 列一律用 `TEXT`。
- 现有数据**不迁移**：内存和 FileStore 里的数据只在 dev 环境存在，本来就是临时的，这一点明确不做迁移。

### 18.2 数据库与迁移
- 复用 compose 里的 `postgres` 服务（`postgres:16-alpine`，`compose.yaml:11-21`），**单独建库 `orbit_control`**，不和 Temporal 或 worker 的 `temporal` 库混用。
- 连接串 `ORBIT_CONTROL_DB_URL`，prod compose 里写成 `${ORBIT_CONTROL_DB_URL:?}`。**使用的是非 owner 的应用角色**（`orbit_app`：不是表的 owner，**没有 BYPASSRLS**）。迁移用单独的 owner 角色，通过 `ORBIT_CONTROL_MIGRATE_DB_URL` 连接。
- **迁移工具选 goose**：一句话理由是它可以作为库嵌入 Go 二进制（`embed.FS`），启动时直接执行，每个 SQL 文件里同时写 up 和 down，不需要额外的 CLI 或镜像。
- 规则：
  - 每个迁移**都有 up 和 down**。
  - 在空库上执行 up，和在已经迁移过的库上执行 up，**都成功**（依靠 goose 的版本表保证幂等）。
  - CI 跑 **up → down → up**。
  - prod 策略是**只向前**：down 只用于 CI 和 dev，不在 prod 回滚。
  - 启动时是否执行迁移由 `ORBIT_CONTROL_MIGRATE_ON_START=1` 控制（默认关闭，prod 由部署脚本显式打开）。
- **内存存储只用于测试**：prod 配置（`ORBIT_ENV=prod` 或 `ORBIT_AUTH_MODE=oidc`）下缺少 `ORBIT_CONTROL_DB_URL` 时，control **拒绝启动**，**永远不会悄悄退回到内存存储**。

### 18.3 表结构
凡是标了 **[T]** 的表，都有 `tenant_id TEXT NOT NULL REFERENCES tenants(id)`，并启用 RLS（见 18.5）。时间一律用 `TIMESTAMPTZ`。

| 表 | 列（类型） | 主键 / 唯一约束 / 索引 / 外键 |
|---|---|---|
| `tenants` | `id TEXT`, `name TEXT`, `created_at` | PK(id)；启动时从 `ORBIT_DEFAULT_TENANT` 写入默认租户（upsert） |
| `users` [T] | `id TEXT`, `tenant_id`, `iss TEXT`, `sub TEXT`, `display_name TEXT`, `email TEXT`, `created_at`, `last_login_at` | PK(id)；UNIQUE(iss, sub)；INDEX(tenant_id) |
| `sessions` | `id_hash BYTEA`（sha256(session id)，**不保存原始 id**）, `user_id TEXT`, `tenant_id TEXT`, `created_at`, `last_seen_at`, `expires_at`（绝对过期时间） | PK(id_hash)；FK user_id→users；INDEX(expires_at)（用于清理）。**登录前阶段的表，不启用 RLS**，理由见 18.5 |
| `oidc_login_state` | `state TEXT`, `nonce TEXT`, `pkce_verifier TEXT`, `return_to TEXT`, `pre_session_hash BYTEA`, `expires_at` | PK(state)；INDEX(expires_at)；一次性使用（callback 时 `DELETE … RETURNING`）；登录前阶段，不启用 RLS |
| `rooms`（= 任务）[T] | `id TEXT`, `tenant_id`, `created_by TEXT`, `kind TEXT`, `title TEXT`, `state TEXT`, `permission_preset TEXT`, `runtime JSONB`, `session_id TEXT`, `persona_id TEXT`, `delegation JSONB`, `failure JSONB`, `last_event_seq BIGINT NOT NULL DEFAULT 0`, `created_at`, `updated_at`, **`deleted_at TIMESTAMPTZ NULL`**, **`deleted_by TEXT NULL`**（C32 rev2，软删除） | PK(id)；FK created_by→users；INDEX(tenant_id, created_by, created_at DESC) WHERE deleted_at IS NULL；CHECK state IN (idle, running, awaiting_approval, awaiting_external, closed, failed) |
| `turns` [T] | `id TEXT`, `tenant_id`, `task_id TEXT`, `kind TEXT`（message\|decide\|answer\|steer）, `status TEXT`, `error_code TEXT`, `model_mode TEXT`, `model_name TEXT`, `created_at`, `finished_at` | PK(id)；FK task_id→rooms；INDEX(task_id, created_at) |
| `events` [T] | `id BIGSERIAL`（**只作内部主键**）, `tenant_id`, `task_id TEXT`, `seq BIGINT NOT NULL`, `event_uid TEXT`（信封里的 eventId，产物事件的 eventId 是确定性的）, `turn_id TEXT`, `type TEXT`, `source TEXT`, `agent_id TEXT`, `agent_path TEXT`, `payload JSONB`, `created_at` | PK(id)；**UNIQUE(task_id, seq)**；UNIQUE(task_id, event_uid)；INDEX(task_id, seq)；FK task_id→rooms。**`assistant.delta` 不入库** |
| `messages` [T] | `id`, `tenant_id`, `task_id`, `role`, `type`, `text`, `created_at` | PK(id)；INDEX(task_id, created_at)。**需求里没有这张表，但现有代码有**：消息和事件是分开存的（`app.go:64-71,311-313`） |
| `approvals` [T] | `id`, `tenant_id`, `task_id`, `approval_request_id`, `call_id`, `turn_id`, `agent_id`, `agent_path`, `tool_name`, `reason`, `arguments JSONB`（已脱敏）, `risk`, `allow_always BOOL`, `status`, `decision`, `rule_id`, `created_at`, `decided_at` | PK(id)；UNIQUE(task_id, approval_request_id)；INDEX(task_id, status) |
| `approval_rules` [T] | `id`, `tenant_id`, `tool_name`, `argument_pattern JSONB`, `any_arguments BOOL`, `scope`, `scope_id`, `room_id TEXT NULL`（C32 rev2：只存在数据库里，scope=room 时等于 scope_id，否则是 NULL）, `created_from_approval_id`, `created_at` | PK(id)；INDEX(tenant_id, scope, scope_id)；CHECK ((scope='room') = (room_id IS NOT NULL))；FK room_id→rooms ON DELETE RESTRICT。DELETE 就是删行（这也满足了 S-Rb-8 的精神） |
| `idempotency_keys` [T] | `key_hash BYTEA`, `tenant_id`, `created_by TEXT`, `request_hash BYTEA`, `task_id`, `created_at`, `expires_at` | **PK(tenant_id, created_by, key_hash)**（C32 rev：不同用户使用相同的 key 互不影响，§9）；FK task_id→rooms |
| `artifacts` [T] | `id TEXT`, `tenant_id`, `task_id`, `title`, `latest_version INT NOT NULL`, `created_at`, `updated_at` | PK(id)；FK task_id→rooms；INDEX(task_id) |
| `artifact_versions` [T] | `artifact_id TEXT`, `tenant_id`, `version INT`, `parent_version INT NULL`, `mime_type`, `size_bytes BIGINT`, `content_digest TEXT`, `previewable BOOL`, `storage_ref TEXT`, `turn_id`, `source_tool_call_id`, `agent_id`, `agent_path`, `created_at` | PK(artifact_id, version)，也就是 UNIQUE(artifact_id, version)；FK artifact_id→artifacts；CHECK version ≥ 1 |
| `personas`、`mcp_connectors`、`cloud_agent_jobs` [T] | 按 `catalog.go:12-52` 的现有字段 | PK(id)；INDEX(tenant_id) |

- **需求里的 `unique(artifact_id, content_digest, version)` 不单独建**：(artifact_id, version) 已经唯一，这个三列约束永远成立，属于冗余索引。幂等判断放在事务里：锁住 `artifacts` 行，然后 `INSERT … ON CONFLICT (artifact_id, version) DO NOTHING`。冲突时比较已存的 `content_digest`：相同 → 200 并且不做任何事；不同，或者 `version ≤ latest_version`（而且不是这种"相同 digest 重发"的情况）→ 409 `artifact_version_conflict`（§16.1）。
- **外键删除行为（C32 rev2）**：所有指向 `rooms` 的子表外键（`turns`、`events`、`messages`、`approvals`、`idempotency_keys`、`artifacts`，以及以后任何按任务划分的表）一律是 **`ON DELETE RESTRICT`**；`artifact_versions → artifacts` 也是 RESTRICT；作用域是 room 的 `approval_rules` 通过 `room_id` 引用 rooms，同样是 ON DELETE RESTRICT。P0 **没有物理删除的路径**。
- **grants 不入库**：grant 里有密钥明文，P0 继续只放在内存里（TTL 15 分钟）。持久化 grant 属于 P1，需要加密。
- **agents、todos、questions 的投影不建表**：按任务从 `events` 重放计算（每个任务的事件数量有限），P1 再考虑物化。

### 18.4 事件游标（Sentinel 决定）
- **不用全局 BIGSERIAL 做 SSE 游标**：序列的分配顺序不等于提交顺序，并发事务可能乱序提交，按 `id > X` 重放会**永久漏掉**较小的那条。
- **每个任务单独计数**。每次写事件都在**同一个事务**里完成：
  1. `SELECT set_config('app.tenant_id', $1, true)`（见 18.5，禁止写成 `SET LOCAL`）
  2. `UPDATE rooms SET last_event_seq = last_event_seq + 1 WHERE tenant_id=$1 AND id=$2 RETURNING last_event_seq`（行锁让同一任务的写入串行化）
  3. `INSERT INTO events(…, seq) VALUES (…, <上一步返回的值>)`
  4. COMMIT
  事务回滚时计数也一起回滚，所以**每个任务的 seq 连续，没有空洞**。
- SSE 的 `id:` 字段和 `Last-Event-ID` 都用**这个任务的 `seq`**。它在任务内单调递增，满足 Sentinel 第 2 条。`events.id` 只作内部主键。
- 这一条**取代**之前"全局序列"的说法，只针对 Postgres 实现。单独的内存版 Last-Event-ID PR 可以继续用进程内计数，本 PR 再切换成每任务 seq，重放和重置行为不变。
- 重放和前端**永远不能把空洞当成丢失**：用每任务 seq 不会有空洞，但代码不能因为看到空洞就重置。
- 实时推送：P0 仍然是**单实例内存 fanout**（`subs`，`app.go:132,615-633`）。事务提交**之后**再广播。多实例用 LISTEN/NOTIFY 属于 P1。
- **重放和实时流的交接顺序（C32 rev，HIGH 1）**。如果先查库再订阅，查询和订阅之间提交的事件会丢，所以必须按下面的顺序：
  1. **先订阅**实时推送，把收到的事件放进这个连接的缓冲区（有上限，比如 10000 条；超过上限就断开连接，让客户端用 Last-Event-ID 重连，不能静默丢弃）。
  2. **再查库**：取 `seq > Last-Event-ID` 的事件，按 seq 升序发送，记下发出去的最大 seq，记为 `maxReplayed`。没有 Last-Event-ID 时 `maxReplayed` 取 0，只在需要补历史时才查库。
  3. **冲刷缓冲区**：按 seq 升序发送，**丢掉 `seq ≤ maxReplayed` 的事件**；之后实时事件照常发送，仍然丢掉 `seq ≤` 已发送最大值的事件。
  - 内存存储接口（测试用，也包括单独的内存版 Last-Event-ID PR）**必须实现同样的顺序**：`Subscribe` 先于 `ListSince`，接口签名里就体现这个顺序（例如 `SubscribeAndReplay(taskID, lastSeq) (<-chan Event, cancel)` 在内部完成三步），调用方不能自己拼装。

### 18.5 租户隔离（两层）
- **查询层**：每个 repository 方法都以 `tenantId` 作为参数，SQL 里都带 `tenant_id = $n`。越权访问仍然返回 §16 规定的同一个 404。
- **RLS 兜底**：所有 [T] 表执行 `ENABLE ROW LEVEL SECURITY` 和 `FORCE ROW LEVEL SECURITY`，policy 为 `USING (tenant_id = current_setting('app.tenant_id', true)) WITH CHECK (同上)`（`FOR ALL`；`rooms` 例外，按命令分开写，见下面的软删除可见性）。这里用 `missing_ok=true`：没有设置时返回 NULL，于是**一行都匹配不到**，满足"没有 SET LOCAL 的查询返回 0 行"。如果写成不带第二个参数的 `current_setting('app.tenant_id')`，没设置时会直接报错。应用角色不是 owner，也没有 BYPASSRLS；迁移用 owner 角色执行。
- **软删除的可见性（C32 rev2）**：
  - `rooms` 的应用角色 policy **按命令分开写**（Sentinel H1：UPDATE policy 如果没有单独的 WITH CHECK，Postgres 会拿 USING 当检查条件，`SET deleted_at=now()` 就会报 `new row violates row-level security policy`）。令 T = `tenant_id = current_setting('app.tenant_id', true)`：`FOR SELECT USING (T AND deleted_at IS NULL)`；`FOR UPDATE USING (T AND deleted_at IS NULL) WITH CHECK (T)`；`FOR INSERT WITH CHECK (T)`；**应用角色没有 DELETE policy，也没有 DELETE 权限**（C32 rev3：`REVOKE DELETE ON rooms FROM orbit_app`，物理删除直接报 `42501 permission denied`，不依赖"没有 policy 所以影响 0 行"这种静默失败）。软删除的 UPDATE **不能带 `RETURNING`**：更新后的新行在 SELECT policy 下看不到，RETURNING 会报错。用受影响行数判断：0 → 404。§18.4 里递增 seq 的 `UPDATE … RETURNING last_event_seq` 没问题，因为它只改未删除的行，新行在 SELECT policy 下仍然可见。
  - **软删除的实现方式（签字时补充）**：Postgres 文档写明，UPDATE 的 WHERE 或 RETURNING 引用了表的列时，要对旧行和新行都做读取检查，也就是新行也要满足 SELECT policy。因此即使不带 `RETURNING`，`UPDATE rooms SET deleted_at=now() WHERE id=$1` 也会因为新行不满足 `deleted_at IS NULL` 而报错。P0 的软删除**统一用** `SECURITY DEFINER` 函数 `orbit_soft_delete_room(p_id text, p_user text) RETURNS integer`：函数属于一个不能登录（NOLOGIN）、带 BYPASSRLS 的专用角色，固定 `SET search_path = pg_catalog, public`；函数内部自己校验 `tenant_id = current_setting('app.tenant_id', true) AND created_by = p_user AND deleted_at IS NULL`，然后写入 `deleted_at=now(), deleted_by=p_user`，返回受影响的行数（0 表示 404）；应用角色对它只有 EXECUTE 权限，函数由迁移创建。上面的 `FOR UPDATE` policy 仍然保留，给其他 UPDATE 使用。S-DB-11 的静态检查把这个函数列进 allowlist，S-DB-13 (i) 以这个函数作为判定对象。
  - 带 `task_id` 的子表（`events`、`messages`、`turns`、`artifacts`、`approvals`、`idempotency_keys`，以及以后任何按任务划分的表）：policy 在租户条件之外再加 `AND EXISTS (SELECT 1 FROM rooms r WHERE r.id = <child>.task_id AND r.deleted_at IS NULL)`，**USING 和 WITH CHECK 都加**（这是有意的：已删除任务的子表行不能再写入或修改；子表不会被软删除 UPDATE 改动，所以没有 H1 的问题）。`artifact_versions` 没有 task_id，改成 `EXISTS (SELECT 1 FROM artifacts a WHERE a.id = artifact_versions.artifact_id)`；artifacts 自己的 policy 已经包含 room 条件，所以会连带生效。
  - 选 EXISTS，不在子表里冗余一个 deleted_at（Celestial 的决定）：只有一个真相来源，删除只需要改一行。`rooms(id)` 的主键索引足以支撑这个子查询；子查询本身也受 rooms 的 RLS 约束（租户条件加未删除条件），这正是我们要的效果。
  - `approval_rules`：policy 在租户条件之外再加 `AND (room_id IS NULL OR EXISTS (SELECT 1 FROM rooms r WHERE r.id = approval_rules.room_id AND r.deleted_at IS NULL))`。作用域是 persona 的规则不受影响；作用域是 room 的规则在任务删除后既读不到，也不会被匹配应用（快照是从能读到的规则计算出来的，§8）。
  - `cloud_agent_jobs`：现有结构里没有 task_id（`catalog.go:43-52`），P0 只按租户隔离；以后关联到任务时，套用同样的 EXISTS policy。
  - 这是兜底：repository 层的查询照样显式带 `deleted_at IS NULL`；即使漏写，RLS 也会拦住。
- **设置租户 GUC 的唯一写法（C32 rev，HIGH 2）**：在请求的事务里执行 `SELECT set_config('app.tenant_id', $1, true)`，tenantId 通过绑定参数传入，第三个参数 `true` 表示只在本事务内有效（LOCAL），事务结束后自动失效。`SET LOCAL` 不接受绑定参数，只能靠字符串拼接，会有 SQL 注入风险，所以**禁止**。**同样禁止**会话级的 `SET app.tenant_id`（连接放回连接池后会带到别的请求里），以及 `set_config(…, false)`。没有事务的查询不能访问 [T] 表（repository 层强制每次都开事务）。
- **登录前阶段的表**（`sessions`、`oidc_login_state`）不启用 RLS：session 查找发生在知道租户之前，oidc 状态本身不属于任何租户。应用角色对这两张表只有 SELECT/INSERT/UPDATE/DELETE 权限，并且只能通过 `internal/store/auth` 包里的少数方法访问。这些方法写在静态检查的 allowlist 里，除此之外不允许任何方法跳过 tenant 参数。`users` 表启用 RLS：callback 时先在事务里用 `set_config('app.tenant_id', 默认租户, true)` 设置租户（P0），再按 (iss, sub) 查询。

### 18.6 Session 与日志
- 只保存 `sha256(session id)`；cookie 里是原始 id。
- **P0 不保存 IdP 的 refresh token 或 access token**：服务端 session 已经够用，id_token 校验后就丢弃。这样更简单，也不需要加密。P1 如果需要 refresh token，用 `ORBIT_STATE_KEY` 一类的密钥加密后保存。
- 日志和错误响应里**不能出现**任何 token、cookie 值或 DB URL（连接失败时只记录 host 占位符和错误码）。

### 18.7 产物内容存储
- **P0：本地卷，按内容寻址**，路径是 `/var/lib/orbit/artifacts/{tenant}/{sha256}`（沿用 control 现有的 `control-data` 卷）。`storage_ref` 存的是相对路径。写入流程是先写临时文件，fsync，再 rename，并在写入前后校验 sha256。
- 选这个方案的理由：
  - 同样内容天然去重，重复写入是幂等的，和 digest 幂等规则一致；
  - 不让 bytea 撑大表和 WAL，数据库备份保持轻量；
  - key 的格式和以后的对象存储（S3/OSS，P1）一样，迁移时只是换后端。
- 大小上限是 `ORBIT_ARTIFACT_MAX_BYTES`。超过上限时内容不保存、不能预览，但元数据和事件照常（§16）。
- **内容怎么到 control**：worker 和 control 是两个容器，不共享卷。P0 由 worker 调用 `POST /internal/artifact-blobs?taskId=…`（使用内部 token，body 是内容，`X-Content-Digest` 头带 sha256）。control 写卷后返回 `storage_ref`，**之后**才发产物事件，这也满足"提交之后才发事件"。这个接口是本 PR 新增的，属于 §16 runtime 事件 PR 的依赖。
- **这个接口的边界（C32 rev，MEDIUM 3）**：
  1. **租户和任务由 control 决定**：根据 `taskId` 查库得出 `tenant_id`（任务不存在就返回 404）。worker 送来的任何 tenant 字段或头都**不采用**。
  2. **存储路径只用 control 自己算出的 sha256**，永远不用 `X-Content-Digest` 头或任何文件名。这个头只用来**比对**：和 control 算出的值不一致就返回 422，并删除临时文件；头缺失或格式不对就返回 400。
  3. **流式写入**：边读边计数、边算 hash，写进**同一目录**下的临时文件。字节数一超过 `ORBIT_ARTIFACT_MAX_BYTES` 就**立即返回 413**，停止读取并删除临时文件。**绝不**把整个 body 读进内存。完成后先 fsync，再原子 rename 到 `{tenant}/{sha256}`；目标已经存在时直接删除临时文件（内容寻址，天然幂等）。
  4. **只绑定在内部监听地址上**（`ORBIT_CONTROL_INTERNAL_ADDR`，例如 `:8081`），Caddy 公网入口不转发。**现状不符**：control 目前只有一个监听端口（`cmd/orbit-control/main.go`），`/internal/events` 和公开 API 挂在同一个 mux 上（`handler.go:218`）。本 PR 把所有 `/internal/*` 都迁到内部监听地址上，公开监听地址上的这些路径返回 404。
- 保留期：P0 **永久保存**事件和产物；P1 制定保留策略（按租户配置的 TTL、归档和清理）。

### 18.7a 任务删除：软删除（C32 rev2，取代 C32 rev 的硬删除）
- **现状**：契约之前没有定义删除语义；control 没有 `DELETE /v1/rooms/{id}`（`handler.go` 没有这个路由）；orbit-web 的"删除"（`removeMatter`，`src/store.ts:155-161`，main `1426e44`）目前只在前端隐藏（`hiddenIds`），不调后端。
- **P0 决定：软删除**（Nexus 的产品决定，Sentinel 同意）。字段、RLS 和外键见 18.3 和 18.5；子表外键一律 `ON DELETE RESTRICT`，P0 没有物理删除的路径。
- **`DELETE /v1/rooms/{roomId}`**：
  - 只有创建者可以删除。判断顺序按 §17：未登录 → 401；CSRF 校验失败 → 403；不是创建者、任务不存在或已经删除 → 404。
  - 执行顺序：先向 room workflow 发 abort，按 §6.3 级联停止所有 Agent；然后在一个事务里执行 `UPDATE rooms SET deleted_at = now(), deleted_by = <session.userId> WHERE tenant_id = … AND id = … AND deleted_at IS NULL`（**不带 RETURNING**，见 18.5），受影响 1 行 → **204**；0 行（重复删除，或者任务不存在）→ **404**。
  - **abort 失败或超时（M1-3）**：软删除照常执行，返回 204；同时记录 warning 并告警（带 task id，不带任何密钥）。用户必须始终能删掉任务。之后迟到的 worker 事件按下一条处理。
  - **删除后迟到的 worker 事件（M1-2，abort 是异步的）**：递增 seq 的 `UPDATE rooms … RETURNING last_event_seq` 返回 0 行，或者子表 INSERT 没通过 EXISTS WITH CHECK，这两种情况 `/internal/events` 都回滚并返回 **404**：事件丢弃，seq 不递增，不返回 500。worker 把 404 当作**不可重试**，直接丢弃。
  - `deleted_at` 和 `deleted_by` **只由服务端**根据 session 设置；请求体里带的这两个字段一律忽略，不采用。
- **删除时断开实时连接**：control 关闭这个任务所有正在进行的 SSE 连接。之后带着这个任务的 `Last-Event-ID` 重连返回 **404**，**不会补发任何事件**。
- **Idempotency-Key 重放**：如果重放的建房间请求对应的任务已经删除，**不能返回**这个已删除任务的 task_id，而是返回和“任务不存在”相同的 404（§9）。实现上（M1-1）：父任务删除后，这行 `idempotency_keys` 对应用角色不可见，查不到，于是走 INSERT，但主键唯一性不受 RLS 影响，会撞上主键冲突。规定：**主键冲突并且这一行不可见 → 404**（响应体和任务不存在时相同），绝不返回 500，也绝不新建任务。
- **恢复和彻底清除**：P0 **没有 API**。P1 做管理后台（单独的管理员角色），届时再决定保留期和 blob GC。P0 **从不删除 blob 文件**（按内容寻址的文件可能被多个任务共用）。
- 前端：删除确认对话框和文案沿用 Nexus 之前的规范，前端接上这个接口之后才算完成。在那之前继续只做前端隐藏。

### 18.8 验收 S-DB
- S-DB-1：静态测试（go/analysis 或 AST 扫描）检查 `internal/store` 的每个 repository 方法都有 `tenantID` 参数，并且 SQL 里包含 `tenant_id`；只有 allowlist 里的 auth 方法可以例外，allowlist 有改动时 CI 要求 Sentinel 评审。
- S-DB-2：用应用角色连接、没有设置 `app.tenant_id` 时，查询任意 [T] 表都返回 0 行；设置为 A 租户（`set_config(…, true)`）时读不到 B 租户的数据；应用角色没有 BYPASSRLS，也不是 owner（查 `pg_roles`/`pg_class` 断言）。
- S-DB-3：创建任务、事件和产物后重启 control，数据都还在，`last_event_seq` 继续递增。
- S-DB-4：`sessions` 表里只有 32 字节的 hash，找不到原始 cookie 值；数据库里没有任何 IdP token 列或 token 值。
- S-DB-5：同一个 artifact 依次插入 v1、v2、v2（相同 digest）、v2（不同 digest）、v1，得到 200、200、200（不做任何事）、409、409，最终只有两行版本。
- S-DB-6：CI 在空库上执行 up；在已迁移的库上再执行一次 up，没有变化且成功；up → down → up 成功。
- S-DB-7（并发）：N（例如 32）个并发写入者往同一个任务写 1000 条事件，同时有一个读者在中途用 `Last-Event-ID` 断开再重连重放。断言 seq 从 1 连续到 1000，读者收到的事件不漏、不重，按 seq 严格递增。**C32 rev**：通过测试钩子，**在查库和订阅之间**、以及**在订阅和查库之间**各注入一次写入，两种情况都断言不漏、不重。Postgres 存储和内存存储都要跑这个测试。
- S-DB-8：prod 配置下不设 `ORBIT_CONTROL_DB_URL`，control 非 0 退出，不会退回内存存储。
- S-DB-9：抓取日志，在 DB 连接失败、session 错误、OIDC 错误这些路径上，日志和错误响应里都没有 cookie 值、token、DB URL 或密码。
- S-DB-10：`assistant.delta` 不入库（写一轮流式回复后，`events` 里没有这种类型）。
- **S-DB-11**（C32 rev）：(a) 静态检查（CI 用 grep 或 lint）拒绝以下写法：`SET app.tenant_id`、`SET LOCAL app.tenant_id`（不管有没有拼接）、任何通过字符串拼接构造的 SET 语句，以及第三个参数是 `false` 的 `set_config('app.tenant_id', …)`；(b) 运行时测试：连接池大小设为 1，请求 A 设置租户后结束，请求 B 复用同一个连接，`current_setting('app.tenant_id', true)` 为空，查询 [T] 表返回 0 行。
- **S-DB-12**（C32 rev）：`/internal/artifact-blobs`：(a) 请求里带伪造的 tenant，control 忽略，文件落在任务真正所属租户的目录下；(b) `X-Content-Digest` 和内容不一致返回 422，没有留下文件；头里写 `../x` 或文件名也不会影响路径；(c) body 大小是上限加 1 时返回 413，control 的 RSS 峰值远小于 body 大小，目录里没有残留的临时文件；(d) 公开监听地址上请求 `/internal/artifact-blobs` 和 `/internal/events` 返回 404，只有内部监听地址可以访问。
- **S-DB-13**（C32 rev2，软删除）：删除一个带事件、消息、turn、审批、产物的任务后：
  - (a) 任务、消息、事件、agents、todos、审批、产物元数据和内容接口都返回 404；`GET /v1/rooms` 不再列出它；
  - (b) 用应用角色直接写 SQL，**完全不带任何 deleted_at 条件**，查 `rooms` 读不到它；绕开 rooms、直接按 task_id 查 `events`、`messages`、`artifacts`、`artifact_versions`、`approvals`，同样返回 0 行；
  - (c) 删除之前已经连上的 SSE 被断开；用这个任务的 Last-Event-ID 重连返回 404，没有补发；
  - (d) 用建这个任务时的 Idempotency-Key 重放创建请求，返回 404，响应里不出现原来的 task_id；
  - (e) 请求体里带 `deleted_by: "u-evil"`，被忽略；
  - (f) 用 owner 角色查询，`deleted_at` 和 `deleted_by`（等于 session 用户）都正确记录，可以用于审计；
  - (g) 再删除一次返回 404；子表外键是 RESTRICT：用 owner 角色试图物理删除 rooms 行时失败；另一个任务引用的同一个 blob 文件仍然存在，可以正常读取；
  - (h) 删除之前在这个任务里创建了一条 scope=room 的 allow-always 规则：删除后 `GET /v1/approval-rules` 不列出它；应用角色直接查 `approval_rules WHERE room_id=<该任务>` 返回 0 行；同一租户的其他任务调用同一工具时照常停车，不会用上这条规则；
  - (i) 通过应用角色（开着 RLS）软删除返回 204，不报 `new row violates row-level security policy`；用 owner 角色查询，`deleted_at` 和 `deleted_by` 都已设置；应用角色执行 `DELETE FROM rooms` 报 SQLSTATE `42501`（permission denied），行数不变（C32 rev3）；`orbit_app` 以外的角色调用 `orbit_soft_delete_room` 报 `42501`（函数 EXECUTE 已从 PUBLIC 收回）；
  - (j) 删除后，worker 对这个任务 `POST /internal/events` 返回 404，`last_event_seq` 不变，没有 500；用原来的 Idempotency-Key 重放创建请求返回 404，任务总数不变；
  - (k) 让 abort 超时或失败（打桩 Temporal），DELETE 仍然返回 204，任务被软删除，日志里有带 task id 的 warning，不带任何密钥。

### 18.9 顺序
- 这个"**存储地基 PR**"放在 **A1 之后**、**和 A2 并行**、**在 auth PR 之前**（§17.9）。
- 它是地基升级，需要 **Celestial 和 Sentinel** 签字。
- PR-B 里的新表（approvals、rules、idempotency 等）都建在这套存储上；§16 的 control 产物 PR 依赖这里的 `artifacts` 表和 blob 接口。

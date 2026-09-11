# Codex 模型请求字段说明

本文梳理当前 Codex 源码中，一次模型请求从本地会话进入模型提供方时会携带哪些字段、这些字段如何生成，以及哪些字段在安装/启动/线程/turn 生命周期中相对固定或持续变化。

## 范围

- 主要覆盖普通 Responses API 推理请求。
- 同时覆盖 WebSocket `response.create`、Responses Lite、compact、memory 等近似请求中的公共字段。
- 本文不解释 OpenAI Responses API 的完整公开协议；内容以当前仓库实现为准。
- 模型请求中**没有 OS PID**，也没有专门的“Codex CLI 进程唯一 ID”。

## 请求构造路径

一次普通对话请求大致经过：

1. UI/输入层产生用户问题。
2. app-server 将输入路由到 core thread。
3. core 创建或复用 turn，生成 `TurnContext`。
4. `TurnContext` 通过 `TurnMetadataState` 保存会话/线程/turn 元数据。
5. `Session::responses_metadata()` 生成 `CodexResponsesMetadata`。
6. `ModelClient` 构造 `ResponsesApiRequest`。
7. 传输层把 `ResponsesApiRequest` 序列化为 JSON 或 WebSocket `response.create`，并追加请求头。

主要源码入口：

- 最终请求对象：`codex-rs/codex-api/src/common.rs`
- 请求构造：`codex-rs/core/src/client.rs`
- turn metadata：`codex-rs/core/src/turn_metadata.rs`
- metadata 投影：`codex-rs/core/src/responses_metadata.rs`
- 上下文窗口：`codex-rs/core/src/session/mod.rs`
- 会话/线程 ID：`codex-rs/core/src/session/session.rs`
- 安装 ID：`codex-rs/core/src/installation_id.rs`
- 传输头：`codex-rs/codex-api/src/requests/headers.rs`
- rollout trace：`codex-rs/rollout-trace/src/inference.rs`

## 最终请求主体

`ResponsesApiRequest` 的主要字段：

| 字段 | 含义 |
|---|---|
| `model` | 当前模型 slug |
| `instructions` | 系统/base instructions |
| `input` | 发送给模型的历史上下文和当前输入 |
| `tools` | 当前模型可见工具定义 |
| `tool_choice` | 工具选择策略 |
| `parallel_tool_calls` | 是否允许并行工具调用 |
| `reasoning` | reasoning effort/summary/context |
| `store` | 是否保存请求 |
| `stream` | 是否流式 |
| `stream_options` | reasoning summary 等流式选项 |
| `include` | 要求返回的字段 |
| `service_tier` | 服务等级 |
| `prompt_cache_key` | prompt cache key |
| `text` | verbosity 和结构化输出 schema |
| `client_metadata` | Codex 自有 metadata |
| `access_programs` | cyber access program |
| `previous_response_id` | WebSocket 增量续传时使用 |
| `generate` | WebSocket prewarm 时使用 |

## 字段层级

可以按以下层级理解稳定性：

```text
installation/process
  └─ session/thread
       └─ context window
            └─ turn
                 └─ one upstream request/attempt
```

## 安装或启动后基本固定的字段

| 字段 | 说明 | 稳定范围 |
|---|---|---|
| `installation_id` | 安装在 `CODEX_HOME/installation_id`，UUIDv4 | 首次生成后固定 |
| `originator` | 客户端产品标识 | 进程内固定 |
| `User-Agent` | 客户端名、版本、系统/架构 | 进程启动后基本固定 |
| 默认模型 provider | provider/base URL/认证配置 | 通常运行内固定 |
| `x-codex-installation-id` | installation_id 的兼容投影 | 同 installation |

注意：`installation_id` 不是进程级 ID。多个 Codex CLI 进程共用同一个 `CODEX_HOME` 时，`installation_id` 相同。

## 会话/线程层字段

| 字段 | 说明 | 稳定范围 |
|---|---|---|
| `session_id` | root thread 会话 ID | 新根线程创建时生成；恢复时沿用；子代理继承 root 的 session_id |
| `thread_id` | 当前 Codex thread ID | 新线程创建时生成；恢复时沿用 |
| `parent_thread_id` | 父线程 ID | 子代理创建后固定 |
| `forked_from_thread_id` | fork 来源线程 | fork 后固定 |
| `forked_from_ordinal_exclusive` | fork 历史边界 | fork 后固定 |
| `parent_turn_id` | 父任务 turn | 子任务创建后固定 |
| `root_turn_id` | 根任务 turn | 普通任务固定为自身；子任务继承根任务 |
| `agent_name` | agent 路径 | 线程/agent 创建后固定 |
| `subagent_header` | 子代理类型 header | 子代理创建后固定 |
| `subagent_kind` | 子代理分类 | 子代理创建后固定 |
| `thread_source` | thread 来源 | 创建后固定 |
| `cwd`/workspace/environment | 工作目录和环境 | 线程内通常稳定，可设置切换 |
| `prompt_cache_key` | 通常等于 `session_id` | 同一会话内稳定 |

## 每次 turn/请求会变化的字段

| 字段 | 说明 |
|---|---|
| `turn_id` | 每次提交 turn 生成 UUIDv7 |
| `turn_trigger` | turn 来源，普通 UI 输入通常为空 |
| `turn_started_at_unix_ms` | 当前 turn 开始时间 |
| `request_kind` | turn/prewarm/compaction/memory |
| `input` | 当前模型可见上下文和用户问题 |
| `tools` | 随配置、模型、插件、MCP、步骤变化 |
| `instructions` | 通常会重新生成，但内容在未改配置时接近稳定 |
| `model` | 当前模型 |
| `reasoning` | effort/summary/context |
| `service_tier` | 当前或 turn 覆盖后的服务等级 |
| `text` | verbosity 与 output schema |
| `include` | 当前固定包含 `reasoning.encrypted_content` |
| `access_programs` | 当前账号/请求解析结果 |

## 上下文窗口字段

| 字段 | 说明 | 稳定范围 |
|---|---|---|
| `window_id` | 当前上下文窗口 ID | 同一窗口内稳定 |
| `window_number` | 窗口序号 | compact 后变化 |
| `context_window_id` | context window UUID | 新建/compact 窗口后变化 |
| `workspaces` | 当前工作区 Git 状态 | 每个 turn 刷新，仓库状态变化时改变 |
| `tool_namespaces_info` | 模型可见工具 namespace | 随模型/工具集变化 |
| `history_ingest_requested` | 历史 ingestion 开关 | 由 feature/config 决定，通常稳定 |
| `extra` | 自定义扩展 metadata | 随来源配置变化 |

`window_id` 不是每次问题都换，而是在上下文压缩、rollback、新建 context window 时变化。

## 单次网络调用/追踪字段

| 字段 | 说明 |
|---|---|
| `x-codex-turn-state` | 服务端返回后用于 sticky routing；同 turn 内后续请求发送 |
| `x-codex-inference-call-id` | 每次 inference attempt 的本地 UUID；rollout trace 开启时作为 header 附加 |
| `traceparent` / `tracestate` | W3C trace context；WebSocket metadata 可能携带 |
| `x-codex-ws-stream-request-start-ms` | WebSocket 请求发送时间戳 |
| `upstream_request_id` | 上游返回的请求 ID，不属于请求体 |

`x-codex-inference-call-id` 只在 rollout trace 开启时生成并附加，普通未开启 trace 的运行可能没有该字段。

## 请求头里还会出现的内容

| Header | 含义 |
|---|---|
| `session-id` | `session_id` 的 HTTP 投影 |
| `thread-id` | `thread_id` 的 HTTP 投影 |
| `x-client-request-id` | 当前 thread id |
| `originator` | 客户端产品 |
| `User-Agent` | 客户端/版本/系统 |
| `x-codex-turn-state` | turn sticky token |
| `x-openai-subagent` | 子代理标识 |
| `x-codex-parent-thread-id` | 父线程 ID |
| `x-codex-window-id` | 当前窗口 ID |
| `x-codex-turn-metadata` | 完整 Codex metadata JSON 的兼容投影 |
| `x-codex-beta-features` | beta feature 标记 |
| `x-codex-routing-hint` | 路由提示 |
| `x-oai-attestation` | attestation header |
| `x-codex-inference-call-id` | rollout trace 开启时附加 |
| `Accept` | 流式 SSE 请求头 |
| `x-openai-internal-codex-responses-lite` | Responses Lite 标记 |

## CodexResponsesMetadata 中的字段

`CodexResponsesMetadata` 是 metadata 的核心结构，主要字段如下：

- `installation_id`
- `session_id`
- `thread_id`
- `agent_name`
- `turn_id`
- `routing_hint`
- `window_id`
- `window_number`
- `context_window_id`
- `request_kind`
- `forked_from_thread_id`
- `forked_from_ordinal_exclusive`
- `parent_thread_id`
- `parent_turn_id`
- `root_turn_id`
- `subagent_header`
- `subagent_kind`
- `thread_source`
- `turn_trigger`
- `sandbox`
- `sandbox_mode`
- `auto_review_enabled`
- `node_repl_auto_review_required`
- `node_repl_disabled`
- `workspaces`
- `tool_namespaces_info`
- `turn_started_at_unix_ms`
- `history_ingest_requested`
- `extra`

这些字段中：

- `installation_id` 属于安装层，基本固定。
- `session_id/thread_id` 属于会话/线程层，创建后固定、恢复后保持。
- `turn_id`、`turn_started_at_unix_ms`、`request_kind` 等属于单次 turn。
- `window_id/window_number/context_window_id` 属于上下文窗口层。
- `workspaces/tool_namespaces_info/extra` 等会随当前工作区、工具、配置和调用方 metadata 变化。
- `sandbox/sandbox_mode/auto_review_enabled` 等权限相关字段通常在当前权限配置内稳定，但权限配置切换后会变化。

## 明文与密文字段

普通请求字段在本地结构中是明文字符串/JSON，包括用户输入、历史消息、工具定义、参数、metadata、窗口信息、模型配置等。

字段级密文出现在以下类型中：

- `Reasoning.encrypted_content`
- `FunctionCall.encrypted_function_args`
- `Compaction.encrypted_content`
- `ContextCompaction.encrypted_content`
- `AgentMessageInputContent::EncryptedContent`
- `FunctionCallOutputContentItem::EncryptedContent`
- 部分 agent 通信 payload 使用 `InterAgentCommunication.encrypted_content`
- 工具 JSON Schema 可通过 `encrypted: true` 标记加密参数

Codex 对这些内容不做本地解密，主要作为不透明字符串保存、传递和持久化。

## 总结

- 安装后基本固定：`installation_id`、`originator`、`User-Agent`、客户端 provider 配置。
- 线程/会话创建后基本固定：`session_id`、`thread_id`、parent/fork 关系、agent 路径。
- 窗口级变化：`window_id`、`window_number`、`context_window_id`。
- 每次 turn/请求变化：`turn_id`、`input`、`model`、`reasoning`、`service_tier`、`tools`、时间戳、追踪 ID。
- 模型请求不包含 OS PID，也没有 Codex CLI 进程专用固定 ID。

## responses-api-proxy 改造方案与保留约束

### 目标链路

```text
POST /v1/responses
  -> responses-api-proxy
  -> app-server turn/start（内部 raw Responses 分支）
  -> Codex Core/AuthManager
  -> ChatGPT Codex backend
```

代理不直接把 ChatGPT OAuth access token 当作 OpenAI Platform API Key 使用。
OAuth 读取、刷新、provider endpoint 和最终 Authorization 均由 app-server/Core
负责。

### 身份字段替换

代理为每个请求分配一个空闲线程，并只替换以下逻辑字段：

- `installation_id`
- `session_id`
- `thread_id`
- `root_turn_id`
- `parent_turn_id`

线程没有有效 turn lineage 时，删除旧的 `root_turn_id` 和 `parent_turn_id`；不伪造
新的值。上述字段在 `client_metadata` 及其 `x-codex-turn-metadata` JSON 投影中保持
一致，并由 Codex 继续生成对应的 HTTP headers。

### 其他字段原样保留

`model`、`input`、`instructions`、`tools`、`tool_choice`、`reasoning`、`store`、
`stream`、`stream_options`、`include`、`service_tier`、`prompt_cache_key`、`text`、
`metadata`、`previous_response_id`、未知字段及其嵌套值都必须从代理输入完整带到
Codex raw Responses 请求。不能把请求降级为只有 `input` 的 `turn/start` 文本输入，
也不能通过普通 turn Prompt 重建来替代原请求。

由传输层管理、因而不属于“原样保留”的字段只有：`Authorization`、`Host`、
`Content-Length`、连接管理字段，以及由 Codex/AuthManager 产生的认证和兼容性 headers。

### 实现边界

标准 app-server `turn/start` 会把输入转换成 Codex 内部 Prompt，因此不能直接满足
原样透传约束。实现应增加显式的内部 raw Responses 分支：仍由 `turn/start` 建立
thread/turn 生命周期和 session identity，但把完整 JSON body 交给 Codex raw Responses
transport；普通文本 `turn/start` 行为不改变。

### 验收条件

1. 代理单元测试对改写前后的 JSON 做深度比较，除五类身份字段外完全相等。
2. app-server/Core 测试使用受控上游捕获真实出站 body，确认非身份字段保持不变。
3. 受控上游请求确认包含替换后的五类身份字段及对应 headers。
4. 五个 session 并发占用时第六个请求等待；成功、错误、客户端断开都会释放 session。
5. 使用 `auth.json` 中的 Codex OAuth 账号成功访问 Codex backend，不再出现
   `api.responses.write` API-key scope 错误。

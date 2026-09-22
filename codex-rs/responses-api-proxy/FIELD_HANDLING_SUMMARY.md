# Codex 代理字段处理汇总

更新日期：2026-09-22。本文描述当前字段处理规则；线上验收记录与具体镜像版本单独保存。

## 一、总体原则

请求链路：客户端 → HTTP 入口 → Responses 代理 → Codex app-server → 上游模型。

**普通业务请求头默认保留；指定身份字段稳定替换；上游凭证和传输字段由服务器管理。**
代理与 app-server 共用 `codex-http-client` 的 `raw_responses_headers` 规则，不再分别维护允许头名称的白名单。
字段仍留在原来的 header 或 JSON 位置；不存在的 metadata 字段不新增，已有 null 保留。
除第五节明确列出的 SDK 兼容规则外，JSON 内容保持语义一致；不保证空白、键顺序或 HTTP 头名称大小写逐字节一致。

## 二、身份与工作区替换

仅处理指定请求头、正文 `client_metadata` 及其 `x-codex-turn-metadata` JSON 字符串；不遍历消息、工具参数或任意嵌套对象。

| 字段 | 处理规则 |
| --- | --- |
| `installation_id` / `x-codex-installation-id` | 使用 app-server 的安装身份。 |
| `session_id` / `thread_id` | 使用当前绑定的服务端会话、线程身份。 |
| `window_id` / `x-codex-window-id` | 使用服务端窗口身份。 |
| `context_window_id` | 稳定映射，保留与 window ID 的区分。 |
| `turn_id` / `root_turn_id` / `parent_turn_id` | 在共同命名空间稳定映射；相同原值得到相同结果，保留父子轮次关系。 |
| `parent_thread_id` / `x-codex-parent-thread-id` | 使用持久记录的父线程映射或服务端父身份；无法解析时拒绝请求。 |
| `workspaces` 目录键 | 映射到绑定资料的目录，附加由原路径确定的稳定后缀；数量不变。 |
| workspace 的 `associated_remote_urls` | 替换已有非 null URL，保留 origin、upstream 等 remote 名称。 |
| workspace 的 `latest_git_commit_hash` / `has_changes` | 替换为同一份资料中的提交哈希与修改状态。 |

五套工作区资料保存在 `CODEX_METADATA_PROFILES` 指向的持久化文件中，部署使用 `/data/metadata-profiles.json`。
服务端 thread 的稳定哈希选择一套；多个 workspace 一对一映射。线程别名也持久保存，供父子关联恢复。
不得为已有绑定直接改换资料内容或清空别名。未配置资料文件时 workspace 保留原值。
五套资料与会话容量是两个概念：当前部署使用持久会话模式，容量为 32，并非五个临时会话轮换。

## 三、普通业务请求头

通过名称、值、重复和大小检查后，除下一节明确列出的例外，请求头原样保留其值，包括未知业务头。

| 字段示例 | 处理规则 |
| --- | --- |
| `x-openai-internal-codex-responses-lite` | 保留；不得因未列入白名单而丢失。 |
| `x-codex-beta-features` / `OpenAI-Beta` | 保留功能与协议标记。 |
| `x-codex-turn-state` | 原样传递；不修改、不全局缓存、不跨会话共享。 |
| `x-codex-inference-call-id` | 已有值保留；缺失时 app-server 为本次请求生成 UUID。 |
| `traceparent` / `tracestate` | 保留已有追踪值。 |
| `Accept` / `Content-Type` / 其他业务头 | 保留；缺失时允许传输层提供默认值。 |

保留模式标记后，上游的模型与请求格式约束仍然生效，代理不会删除标记或改写业务正文来规避错误。
本次部署实测：`gpt-5.5` 配合 lite=true 返回上游 400“不支持该模式”；`gpt-6-astra` 的有效 Lite 请求返回 200。
Lite 验收请求使用 `reasoning.context=all_turns`、`parallel_tool_calls=false`，工具声明放在 input 的 additional_tools 项中。

边界限制：至多 128 个入站头，每个名称至多 256 字节，每个值至多 8192 字节，名称和值合计至多 64 KiB。
大小写不同也视为相同名称；重复名称、非法头值或超限输入会被拒绝，不静默截断。JSON-RPC 中无法表达重复的同名键，应在 HTTP 入口拒绝。
迁移镜像的 Nginx 启用 `underscores_in_headers`，避免合法的下划线业务头在进入代理之前被静默丢弃。

## 四、关键身份、凭证及传输例外

| 字段 | 处理规则与原因 |
| --- | --- |
| `Authorization` | 客户端值仅用于 Worker 入口鉴权；移除后由 Codex 上游认证配置提供真实凭证。 |
| `Proxy-Authorization` / `api-key` / `x-api-key` | 不转发客户端凭证；上游认证配置按需提供。 |
| `ChatGPT-Account-Id` / `OpenAI-Organization` / `OpenAI-Project` | 不让客户端值覆盖服务器选定账号或上游提供方配置。 |
| `originator` | raw Responses 出站固定写入 `Codex Desktop`，覆盖客户端、线程来源及提供方默认值。 |
| `User-Agent` | raw Responses 出站固定写入 `Codex Desktop/0.155.0-alpha.9.2 (Mac OS 13.5.0; arm64) unknown (Codex Desktop; 26.915.31945)`，不随宿主系统或客户端来值变化。 |
| `session-id` / `thread-id` / `x-client-request-id` | 去掉客户端值，由 Codex transport 写入服务端会话/线程身份。下划线形式的身份字段按第二节替换。 |
| `x-oai-attestation` | 移除原客户端签名；它与原客户端身份绑定，不能在改写身份后原样沿用。当前 raw 链路不生成新的 attestation，不伪造签名。 |
| `Cookie` / `Cookie2` / `Set-Cookie` | 不转发客户端 cookie 或以请求头夹带的 Set-Cookie；服务端自身 cookie store 仍由真实 transport 管理。 |
| `x-codex-queue-*` | 内部调度信封，只用于 Worker，不发送给上游。 |
| `Forwarded` / `Via` / `X-Real-IP` / `X-Forwarded-*` | 不泄漏入口或中间代理的来源信息。 |
| `CF-*` / `CDN-Loop` / `True-Client-IP` / `Fastly-Client-IP` / `X-Envoy-External-Address` | 移除 CDN 路由、入口凭证及代理来源地址，避免泄漏凭证或触发 CDN 循环检查。 |
| `Origin` / `Referer` / `Sec-Fetch-*` / `Sec-CH-UA*` | 浏览器对入口站点的上下文不带入上游；SDK 的 `x-stainless-*` 与业务追踪头仍保留。 |
| `Host` / `Content-Length` / `Content-Encoding` / `Accept-Encoding` / `Expect` | 由实际目标、重新序列化的 JSON 及服务端传输能力决定，不沿用入站长度或压缩状态。 |
| `Connection` / `Keep-Alive` / `Proxy-Connection` / `Proxy-Authenticate` / `TE` / `Trailer` / `Transfer-Encoding` / `Upgrade` | 逐跳字段不跨代理转发；Connection 点名的其他字段也移除。 |

丢弃客户端凭证并不保证每个字段都会在出站出现：取决于服务器当前认证、账号及提供方配置。
`x-oai-attestation` 的缺失是明确例外；普通业务头不得使用这一理由被笼统过滤。

## 五、正文及响应边界

`model`、`input`、`instructions`、工具定义/结果、sandbox 设置、`previous_response_id`、普通 `metadata` 和其他非指定身份位置保持不变。
代理不执行工具、不追加 prompt、不重建历史、不运行 Agent loop。

HTTP Worker 自动适配普通 SDK 的三个参数：

| 参数 | 服务端处理 |
| --- | --- |
| `store` | 缺省补 `false`；已有值（包括 null）保留，由上游校验。 |
| `stream` | 上游固定为 `true`。客户端为 `true` 时原样返回 SSE；缺省、null 或 `false` 时收集终态事件并返回普通 Response JSON。非法类型返回 400。 |
| `max_output_tokens` | 上游不支持，发送前移除。**客户端给出的输出上限不会执行**；正整数会附带响应头 `x-codex-ignored-parameters: max_output_tokens`、`x-codex-output-token-limit: not-enforced`。null 移除且不提示；非法类型或非正数返回 400。 |

普通 SDK 最小请求只需 `model` 和列表形式的 `input`。
不补入可选的 instructions、tools、reasoning、text 或 Lite 模式标记。
会话/线程身份与缺省 inference-call ID 由既有 transport 机制提供。
这些适配仅在 Worker 侧发生；Sub2API 严格透传到 Worker 的正文仍逐字节不变。

响应头保留路由状态、重试提示、媒体类型和已允许的额度信息；不转发上游 Set-Cookie。
流式调用保留原始 SSE；非流式调用保留终态 Response 的真实用量与 completed/failed/incomplete 状态。
Codex 终态可能不带 output，此时按 output_index 排序收集完整的 output_item.done，保留消息、工具调用与 reasoning。
单行、单个 SSE 事件及累计 output 限制为 4 MiB，最多 4096 个输出项；completed 缺失完整输出项、缺少终态、损坏或截断的流返回 HTTP 502，不返回伪造的成功结果。
上游 HTTP 错误保留状态码和正文。每个请求独立处理路由状态；工具续接由客户端携带历史与对应 call_id。

## 六、验证要求

- 共享策略：普通/未知业务头保留，凭证与逐跳字段排除，大小写重复与长度限制有效。
- 代理 HTTP → RPC：业务头不丢，队列信封与客户端身份凭证不泄漏。
- app-server RPC → 模拟上游：业务头真实到达 HTTP 接收端，服务端 session/thread/请求 ID 取代客户端值，正文保持既定规则。
- 线上实际发送边界：对照同一请求的客户端与上游记录，逐个比较应保留字段；不能仅凭 HTTP 200 判定头完整。
- metadata 回归：header-only、body-only、两处同时存在、工具续接、父子关联以及持久绑定。

管理页捕获的是应用 HTTP 发送边界，不是完整网络抓包。Host、Content-Length 等可能在此边界之后由 HTTP 客户端添加，不能仅因快照缺失就判定未发送。
固定的 User-Agent 和 originator 在 raw HTTP 发送前显式写入，因此管理页的上游快照也应包含这两个最终值。这是该代理约定的客户端标识，不代表实际 Linux 宿主的操作系统或二进制版本。

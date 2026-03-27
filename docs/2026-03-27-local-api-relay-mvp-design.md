# Local API Relay MVP Design

**Goal**

在工作区根目录交付一个可本地运行的 API 中转服务：应用侧只替换 base URL 和本地 key；服务端负责把本地 key 映射成上游 key，并尽量原样透传请求/响应。

**Chosen Approach**

- 选用 Python 3.12 标准库实现单文件 `ThreadingHTTPServer`，避免引入额外依赖和环境安装成本。
- 每个应用通过配置文件拿到独立 `local_key`、`app_id`、`upstream_base_url`、`upstream_api_key`。
- 请求侧仅改鉴权：本地 `Authorization: Bearer <local_key>` 或 `X-API-Key` 被替换为上游 key；路径、方法、查询串、请求体、绝大多数头部保持不变。
- 使用 SQLite 持久化请求事件，管理端点直接从事件表聚合出每应用 usage 和 idle 判断。

**Runtime Surface**

- 业务透传：除 `/_relay/*` 外的所有路径都按原路径转发到上游。
- 管理端点：
  - `GET /_relay/health`：最小健康信息
  - `GET /_relay/stats`：总量 + 每应用请求统计
  - `GET /_relay/idle`：最小“是否空转”判断结果
- 管理端点可用 `X-Relay-Admin-Key` 保护；留空则不校验。

**Data Model**

`request_log` 单表记录：

- `started_at` / `finished_at`
- `app_id`
- `method` / `path`
- `status_code`
- `forwarded`
- `request_bytes` / `response_bytes`
- `upstream_ms`
- `error_kind`
- `prompt_tokens` / `completion_tokens` / `total_tokens`

非流式 JSON 响应若含上游 `usage` 字段，则抽取 token 统计；否则仅保留请求级计数与字节数。

**Idle Rule**

MVP 先只做一条最小规则：

- 在 `idle_window_seconds` 窗口内，只要存在至少一条“已成功转发且响应码为 2xx”的请求，系统判定为 `idle=false`
- 否则判定为 `idle=true`

返回体同时给出窗口内 `requests` / `forwarded` / `successes` / `last_request_at` / `last_success_at`，便于后续升级规则而不改接口形态。

**Out of Scope**

- 动态热加载配置
- 多租户限流、配额、熔断
- 完整 SSE / chunked 请求侧代理专项压测
- 更复杂的“空转”业务语义判断

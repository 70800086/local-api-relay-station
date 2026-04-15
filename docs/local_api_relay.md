# 本地 API Relay

这版是最终最简结构。

顶层正式接口现在有五个 key：

- `server`
- `local`
- `upstreams`
- `order`
- `pricing`（可选，但只要要看 estimated cost，建议显式配置）

目标很直接：

- `local` 只描述本地客户端和它们的 `local_key`
- `upstreams` 只描述真实上游和上游 key
- `order` 决定 relay 对所有代理请求按什么顺序尝试上游
- `pricing` 提供 relay 自身 stats 的 estimated cost 口径，不再依赖 tasks-service 额外适配器
- 请求里的 `model` 原样转发，relay 不再声明“某模型走哪个源”或“某源支持哪些模型”

## 文件

- 服务脚本：`local_api_relay.py`
- 配置样例：`local_api_relay.example.json`
- 实际配置：`local_api_relay.json`
- 测试：`tests/test_local_api_relay.py`
- 默认 SQLite：`state/local_api_relay.sqlite3`

## 配置结构

先复制样例：

```bash
cp local_api_relay.example.json local_api_relay.json
```

### `server`

- `listen_host`
- `listen_port`
- `admin_key`
- `database_path`
- `idle_window_seconds`
- `request_timeout_seconds`

### `local`

只放本地客户端：

```json
{
  "local": {
    "clients": [
      {
        "client_id": "openclaw",
        "local_key": "local-openclaw-key"
      },
      {
        "client_id": "codex",
        "local_key": "local-codex-key"
      }
    ]
  }
}
```

说明：

- `client_id` 仅用于统计
- `local_key` 是本地客户端实际拿来访问 relay 的 key
- 这里不再出现 route、policy、model group 之类概念

### `upstreams`

上游注册表按 upstream id 建对象：

```json
{
  "upstreams": {
    "shenfeng": {
      "base_url": "http://23.144.68.54:8080/v1",
      "api_key": "sk-your-upstream-key",
      "enabled": true,
      "transport": {
        "timeout_seconds": 120
      }
    },
    "codexFor": {
      "base_url": "https://api-vip.codex-for.me/v1",
      "api_key": "clp-your-upstream-key",
      "enabled": true,
      "transport": {
        "timeout_seconds": 120
      }
    }
  }
}
```

字段含义：

- `base_url`
- `api_key`
- `enabled`
- `transport`
  - 目前只使用 `timeout_seconds`

### `order`

全局 upstream 顺序：

```json
{
  "order": ["shenfeng", "codexFor", "codetab"]
}
```

relay 会严格按这个顺序尝试。

### `pricing`

可选，但如果希望 `/_relay/stats` 自带 `estimated_cost`，就需要配置。

```json
{
  "pricing": {
    "currency": "USD",
    "upstreams": {
      "shenfeng": {
        "input_per_million_tokens": 0.5,
        "output_per_million_tokens": 1.5,
        "models": {
          "gpt-5-mini": {
            "input_per_million_tokens": 0.25,
            "output_per_million_tokens": 1.0
          }
        }
      },
      "codexFor": {
        "input_per_million_tokens": 0.6,
        "output_per_million_tokens": 1.8
      }
    }
  }
}
```

规则：

- `currency` 是 relay stats 里 `estimated_cost.currency` 的统一货币单位
- `upstreams.<id>` 对应一个 upstream 的默认单价
- `upstreams.<id>.models.<model>` 可以覆盖某个模型在这个 upstream 下的单价
- 如果缺少 pricing、缺少匹配规则，或者只有 `total_tokens` 没有 `prompt_tokens/completion_tokens`，该请求不会计入 `estimated_cost`

## 请求行为

proxy 请求的最小决策顺序固定如下：

1. 用本地 key 匹配 `local.clients[].local_key`
2. 解析 request body JSON
3. 保留 body 里的 `model` 原样不改
4. 先以 `order` 作为基础成本顺序生成候选 upstream
5. 对每个 upstream：
   - 必须 `enabled=true`
   - 最近失败过的 upstream 会被临时降权，恢复窗口过后自动回到基础顺序前排
   - 已经被 breaker 打开的 upstream 会先跳过，等 cooldown 结束后再用 half-open probe 试探恢复
   - 用同一个 body 原样转发
   - 只要该 upstream 返回错误响应或请求异常，就继续尝试下一个 upstream
   - 上游成功就立即返回
6. 全部 upstream 失败后返回聚合错误，列出每个 upstream 的失败摘要

不会再做的事情：

- 不再有 `model_groups`
- 不再有 upstream `models`
- 不再做 model id 改写
- 不再按 client 定制上游顺序

## 400 / 401 语义

- `401`：本地 key 无效
- `400`：request body 不是合法 JSON
- `400`：request body 缺少 `model`
- `502`：所有 upstream 都失败，relay 返回聚合错误

## 动态优先级与恢复

- `order` 仍然是默认顺序，通常表达成本优先级
- 任何上游失败都会触发一次临时降权，避免同一个坏源持续挡在前面
- 降权只持续一个短恢复窗口，窗口过后该 upstream 会自动回到 `order` 对应的位置，重新争取前排
- 真正的硬故障（例如超时、连接失败、5xx、429）还会继续累加 breaker；达到阈值后暂时摘出，cooldown 结束后再 probe

这样做的主要权衡：

- 优点：池里只要还有活路，请求更容易被救回来；而且便宜 upstream 恢复后会自动重新抢回前排
- 代价：恢复窗口结束后的第一批请求可能会再次探测到刚恢复不久的上游，换来一次额外 fallback

## 聚合错误输出

当所有 upstream 都失败时，relay 返回：

- HTTP `502`
- `error.type = "upstream_fallback_exhausted"`
- `error.attempts[]`：按实际尝试顺序列出 `upstream_id`、`error_kind`、可用时的 `status_code` / `message`

## 单请求 fallback trace

每次代理请求都会返回一个响应头：

- `X-Relay-Request-Trace-Id`

这个 trace id 会同时写进 `request_log_v4`，并附带最小排障字段：

- `request_trace_id`
- `attempt_index`
- `terminal_outcome`

同一次原始请求的所有 attempt 会共享同一个 `request_trace_id`，所以 operator 可以按一次请求稳定还原完整 attempt 链，而不是只能看散落的原始日志行。

## 启动

```bash
PYTHONDONTWRITEBYTECODE=1 python3 local_api_relay.py --config local_api_relay.json
```

默认监听：

- `http://127.0.0.1:8787`

## 自动热重载

relay 会自动轮询 `local_api_relay.json`。

- 轮询间隔默认是 1 秒
- 文件内容连续两次观察一致后才会真正尝试 reload，避免编辑器写入过程中的半成品配置被误吃进去
- `upstreams`、`order`、`local.clients`、`idle_window_seconds`、`request_timeout_seconds` 这类运行期配置会直接切到新 runtime
- `database_path` 变化会新建 SQLite 连接，后续请求写入新库，旧请求继续收尾到旧库
- `listen_host` / `listen_port` 变化会拉起新的 listener 并切走流量，旧端口随后退役

reload 失败时不会把当前可用 relay 打挂。典型失败场景包括：

- JSON 非法
- schema 校验失败
- `order` 引用了不存在的 upstream
- 新数据库打不开
- 新 host/port 绑定失败

发生这些错误时，relay 会继续使用旧配置服务，并把最近一次 reload 错误写到 `/_relay/health`。

## 客户端接入

客户端只需要改两项：

- `base URL` 改成 relay
- `API key` 改成自己的 `local_key`

支持的本地鉴权入口：

- `Authorization: Bearer <local_key>`
- `X-API-Key: <local_key>`
- `Api-Key: <local_key>`

## 当前仓库内的实际配置

`local_api_relay.json` 当前已经切到：

- 本地客户端：`codex`、`openclaw`
- upstreams：`shenfeng`、`codexFor`、`codetab`
- `order`：`["shenfeng", "codexFor", "codetab"]`

其中：

- `shenfeng` 当前地址是 `http://23.144.68.54:8080/v1`

## 管理端点

如果配置了 `admin_key`，访问 `/_relay/*` 时必须带：

```http
X-Relay-Admin-Key: <admin_key>
```

### `/_relay/health`

返回：

- `server`
- `local`
- `upstreams`
- `order`
- `breaker`
- `upstream_status`
- `reload`

其中：

- `order` 仍是配置里的原始顺序。
- `breaker.upstreams` 是每个 upstream 的断路器原始快照。
- `upstream_status` 是给 operator 排障看的 live 视图，至少包含：
  - `configured_order`
  - `effective_order`
  - `upstreams[]`

`upstream_status.upstreams[]` 至少包含：

- `upstream_id`
- `enabled`
- `configured_index`
- `effective_index`
- `breaker_state`
- `routing_state`（`healthy` / `degraded` / `cooldown` / `probing` / `disabled`）
- `priority_penalty`
- `cooldown_active`
- `degraded_active`
- `cooldown_until`
- `degraded_until`
- `recovery_at`
- `last_failure`

其中 `last_failure` 会带最近一次让 upstream 进入降权/熔断路径的 `error_kind`、`status_code`（如有）、`reason` 与时间戳，便于直接确认为什么被降权、何时恢复以及当前有效排序是否符合预期。

其中 `reload` 至少包含：

- `config_path`
- `config_version`
- `reload_enabled`
- `reload_poll_interval_seconds`
- `last_reload_at`
- `last_reload_status`
- `last_reload_error`
- `last_successful_reload_at`
- `draining_runtimes`

### `/_relay/stats`

至少返回：

- `totals`
- `clients`
- `upstreams`
- `models`
- `usage_policy`
- `usage_coverage`

其中 `models` 统计的是请求体里原始传入的 model id，而不是 relay 改写后的模型名。

其中每个 `usage` 现在至少包含：

- `responses`
- `prompt_tokens`
- `completion_tokens`
- `total_tokens`
- `cached_tokens`
- `estimated_cost`

其中：

- `estimated_cost` 是 relay 基于 `pricing` 与请求日志 token 字段推导出的估算值
- `usage_policy` 会明确说明 token / estimated cost 的来源和缺失场景
- `usage_coverage` 会明确当前 stats 口径里，多少请求具备 prompt/completion/total token，多少请求真正进入了 estimated cost

### `/_relay/request-traces?request_trace_id=<trace_id>`

按单请求返回 fallback trace，至少包含：

- `request_trace_id`
- `configured_order`
- `effective_order`
- `order_pool[]`
- `planned_attempt_count`
- `attempt_count`
- `fallback_triggered`
- `traversed_all_planned_attempts`
- `terminal_outcome`
- `terminal_attempt_index`
- `terminal_upstream_id`
- `attempts[]`

其中：

- `order_pool[]` 会标出这次请求在当时的 effective order 里，哪些 upstream 被 `attempted`、哪些被 `skipped`
- `order_pool[].skip_reason` 目前最少会区分 `disabled` 和 `breaker_open`
- `traversed_all_planned_attempts=true` 表示这次请求已经把当时所有可尝试 upstream 都走完了；如果同时 `terminal_outcome=fallback_success`，说明它是在最后一跳才救回来
- `attempts[]` 按 `attempt_index` 顺序列出每一跳的 `upstream_id`、`status_code`、`error_kind`、`upstream_ms` 和该请求的最终 `terminal_outcome`

### `/_relay/idle`

基于最近请求时间和 in-flight 请求判断是否空闲。

## Operator CLI

relay 自己提供正式 operator 入口，不再需要 tasks-service 额外包一层：

### 启动服务

```bash
python3 local_api_relay.py --config local_api_relay.json
```

也支持显式写成：

```bash
python3 local_api_relay.py serve --config local_api_relay.json
```

### Probe 上游

```bash
python3 local_api_relay.py probe --config local_api_relay.json
python3 local_api_relay.py probe --config local_api_relay.json --upstream-id shenfeng
python3 local_api_relay.py probe --config local_api_relay.json --path /models --timeout-seconds 5
```

### 查 credits

```bash
python3 local_api_relay.py credits --config local_api_relay.json
python3 local_api_relay.py credits --config local_api_relay.json --upstream-id codexFor
```

目前会优先尝试：

- `/api/user/self`
- `/api/token/self`

### 跑 real-request test

```bash
python3 local_api_relay.py test --config local_api_relay.json
python3 local_api_relay.py test --config local_api_relay.json --upstream-id shenfeng
```

行为：

- 先打 `/models`
- 自动挑一个可用文本模型
- `gpt-5/gpt-4.1/o1/o3/o4` 族优先走 `/responses`
- 其他模型优先走 `/chat/completions`
- 如果遇到兼容性报错，会在少量已知场景下自动重试（例如 `responses -> chat/completions`，或 `chat/completions` 补 `stream=true`）

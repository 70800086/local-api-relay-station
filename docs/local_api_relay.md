# 本地 API Relay

这版是最终结构，不再兼容昨天那套 `clients / routes / policies` 配置。

顶层只有四个 key：

- `server`
- `local`
- `upstreams`
- `model_groups`

目标很直接：

- `local` 只描述本地客户端和它们的 `local_key`
- `upstreams` 只描述真实上游和上游 key
- `model_groups` 决定某个 canonical model 要按什么顺序尝试哪些上游

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
- 这里不再出现 route、policy、upstream 之类的概念

### `upstreams`

上游注册表按 upstream id 建对象：

```json
{
  "upstreams": {
    "primary-openai": {
      "base_url": "https://api.openai.com/v1",
      "api_key": "sk-primary",
      "enabled": true,
      "transport": {
        "timeout_seconds": 120
      },
      "models": {
        "gpt-5.4": "gpt-5.4",
        "gpt-4o-mini": "gpt-4o-mini"
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
- `models`
  - `canonical_model -> upstream_model`

### `model_groups`

`canonical model -> ordered upstream ids`

```json
{
  "model_groups": {
    "gpt-5.4": ["primary-openai", "backup-openai"],
    "gpt-4o-mini": ["backup-openai"]
  }
}
```

relay 会严格按这个顺序尝试。

## 请求行为

proxy 请求的最小决策顺序固定如下：

1. 用本地 key 匹配 `local.clients[].local_key`
2. 从 request body 解析 canonical `model`
3. 查 `model_groups[model]`
4. 按顺序遍历 upstream id
5. 对每个 upstream：
   - 必须 `enabled=true`
   - 必须在 `upstreams[upstream_id].models` 里支持该 canonical model
   - 转发前把 body 里的 `model` 改写成 upstream model
   - 请求失败时只有这三类会继续尝试下一个 upstream：
     - 网络错误
     - 超时
     - upstream 5xx

不会再做的事情：

- 不再有 `policy`
- 不再有 `default_route`
- 不再有 `fallback_routes`
- 不再有 `X-Relay-Route`
- 不再按 client 定制上游顺序

## 400 / 401 语义

- `401`：本地 key 无效
- `400`：request body 没有 canonical `model`
- `400`：该 model 没有 `model_groups[model]`
- `400`：有 group，但没有任何启用中的 upstream 支持该 model

换句话说，这版 relay 默认只接受“body 里带 canonical model 的 API 请求”。

## 模型改写

例如：

```json
{
  "upstreams": {
    "azure-prod": {
      "base_url": "https://example.openai.azure.com/openai/deployments/prod",
      "api_key": "azure-key",
      "enabled": true,
      "models": {
        "gpt-4o-mini": "gpt-4o-mini-prod"
      }
    }
  }
}
```

客户端发：

```json
{"model":"gpt-4o-mini"}
```

relay 发到该 upstream 时会改成：

```json
{"model":"gpt-4o-mini-prod"}
```

客户端永远只看到 canonical model。

## 启动

```bash
PYTHONDONTWRITEBYTECODE=1 python3 local_api_relay.py --config local_api_relay.json
```

默认监听：

- `http://127.0.0.1:8787`

## 客户端接入

客户端只需要改两项：

- `base URL` 改成 relay
- `API key` 改成自己的 `local_key`

支持的本地鉴权入口：

- `Authorization: Bearer <local_key>`
- `X-API-Key: <local_key>`
- `Api-Key: <local_key>`

## 当前仓库内的实际配置

`local_api_relay.json` 已切到：

- 本地客户端：`codex`、`openclaw`
- upstreams：`shenfeng`、`vip-primary`
- `model_groups`：当前统一把已登记模型按 `["shenfeng", "vip-primary"]` 顺序尝试

其中：

- `shenfeng` 仍使用当前有效地址 `http://23.144.68.54:8080/v1`
- 这表示在不引入 client-specific 行为的前提下，所有本地客户端共享同一套模型分组顺序

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
- `model_groups`

用途是确认当前加载的最终 schema 与运行状态。

### `/_relay/stats`

至少返回：

- `totals`
- `clients`
- `upstreams`
- `models`

当前统计仍然是“attempt 级”而不是“最终请求级”：

- 如果某次请求先打第一个 upstream 拿到 503，再打第二个 upstream 成功
- stats 会记成两条 attempt

字段至少包括：

- `requests`
- `successes`
- `failures`
- `status_codes`
- `usage`
- `latency_ms`
- `last_request_at` / `last_success_at`
- `in_flight_requests`

补充：

- `totals.local_rejects` 统计无效本地 key
- `models[].upstream_models` 展示某个 canonical model 实际命中过哪些 upstream model

### `/_relay/idle`

规则保持简单：

- 窗口内无请求：`idle=true`
- 有请求处理中：`reason=requests_in_flight`
- 窗口内有成功请求：`reason=recent_successful_requests`
- 窗口内只有失败请求：`reason=recent_failed_requests`
- 窗口内只有本地 key 被拒请求：`reason=recent_rejected_requests`

## 与 watchdog 的关系

`task_activity.py` 目前只依赖 `/_relay/stats` 里的：

- `clients[].client_id`
- `clients[].last_request_at`
- `clients[].last_success_at`

这三个字段在最终 schema 下继续保留，所以 `task_activity.py` / `task_watchdog.py` 不需要跟着这次重构改名。

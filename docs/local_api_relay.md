# 本地 API Relay

这版是最终最简结构。

顶层只有四个 key：

- `server`
- `local`
- `upstreams`
- `order`

目标很直接：

- `local` 只描述本地客户端和它们的 `local_key`
- `upstreams` 只描述真实上游和上游 key
- `order` 决定 relay 对所有代理请求按什么顺序尝试上游
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

## 请求行为

proxy 请求的最小决策顺序固定如下：

1. 用本地 key 匹配 `local.clients[].local_key`
2. 解析 request body JSON
3. 保留 body 里的 `model` 原样不改
4. 按 `order` 顺序遍历 upstream id
5. 对每个 upstream：
   - 必须 `enabled=true`
   - 用同一个 body 原样转发
   - 上游成功就立即返回
   - 网络错误 / 超时 / 5xx / 明显不支持模型这类上游 4xx 时继续尝试下一个
6. 全部 upstream 失败后返回最后一个有意义的错误

不会再做的事情：

- 不再有 `model_groups`
- 不再有 upstream `models`
- 不再做 model id 改写
- 不再按 client 定制上游顺序

## 400 / 401 语义

- `401`：本地 key 无效
- `400`：request body 不是合法 JSON
- `400`：request body 缺少 `model`
- `502/5xx`：所有 upstream 都失败

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

### `/_relay/stats`

至少返回：

- `totals`
- `clients`
- `upstreams`
- `models`

其中 `models` 统计的是请求体里原始传入的 model id，而不是 relay 改写后的模型名。

### `/_relay/idle`

基于最近请求时间和 in-flight 请求判断是否空闲。

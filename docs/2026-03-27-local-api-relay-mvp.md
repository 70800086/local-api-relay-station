# Local API Relay MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local API relay MVP that maps per-app local keys to upstream keys, proxies requests with minimal rewriting, records usage, and exposes a minimal idle decision.

**Architecture:** A single-file Python HTTP relay handles auth translation and upstream forwarding, while SQLite stores request events for aggregated stats and idle decisions. Black-box unittest coverage drives the MVP from the process boundary instead of testing internal helpers only.

**Tech Stack:** Python 3.12 stdlib (`http.server`, `http.client`, `sqlite3`, `unittest`)

---

### Task 1: Write black-box failing tests

**Files:**
- Create: `tests/test_local_api_relay.py`
- Create: `local_api_relay.py`
- Test: `tests/test_local_api_relay.py`

- [x] **Step 1: Write the failing test**

```python
def test_proxy_rewrites_local_key_and_records_usage(self) -> None:
    with RelayProcess(config_path, admin_key="relay-admin"):
        status, headers, payload = make_request(...)
        self.assertEqual(status, 201)
        self.assertEqual(headers.get("x-upstream-trace"), "stub-1")
```

- [x] **Step 2: Run test to verify it fails**

Run: `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v tests.test_local_api_relay`
Expected: FAIL because `local_api_relay.py` does not exist yet

- [x] **Step 3: Write minimal implementation**

```python
class RelayRequestHandler(BaseHTTPRequestHandler):
    def _handle_proxy(self) -> None:
        app = self.relay_config.apps_by_key.get(local_key or "")
        forwarded_headers = _build_upstream_headers(self.headers, app.upstream_api_key)
        connection.request(self.command, upstream_path, body=request_body, headers=forwarded_headers)
```

- [x] **Step 4: Run test to verify it passes**

Run: `PYTHONDONTWRITEBYTECODE=1 python3 -W error::ResourceWarning -m unittest -v tests.test_local_api_relay`
Expected: PASS without warnings

### Task 2: Persist usage and expose admin endpoints

**Files:**
- Modify: `local_api_relay.py`
- Test: `tests/test_local_api_relay.py`

- [x] **Step 1: Store request events in SQLite**

```python
CREATE TABLE IF NOT EXISTS request_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    app_id TEXT,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    forwarded INTEGER NOT NULL
)
```

- [x] **Step 2: Add stats and idle endpoints**

```python
if self.path == "/_relay/stats":
    self._send_json(200, self.usage_store.stats_summary())
if self.path == "/_relay/idle":
    self._send_json(200, self.usage_store.idle_status(self.relay_config.idle_window_seconds))
```

- [x] **Step 3: Re-run focused tests**

Run: `PYTHONDONTWRITEBYTECODE=1 python3 -W error::ResourceWarning -m unittest -v tests.test_local_api_relay`
Expected: PASS

### Task 3: Ship config sample and record task completion

**Files:**
- Create: `local_api_relay.example.json`
- Create: `docs/superpowers/specs/2026-03-27-local-api-relay-mvp-design.md`
- Create: `docs/superpowers/plans/2026-03-27-local-api-relay-mvp.md`
- Modify: `TASKS.md`

- [x] **Step 1: Add runnable sample config**

```json
{
  "listen_host": "127.0.0.1",
  "listen_port": 8787,
  "apps": [{"app_id": "chatbox", "local_key": "local-chatbox-key"}]
}
```

- [x] **Step 2: Document MVP boundaries**

```markdown
MVP idle rule: if there is no successful forwarded 2xx request within the configured window, report idle=true.
```

- [x] **Step 3: Update task tracker**

Run: `sed -n '1,24p' TASKS.md`
Expected: task `6a` shows `DONE` and `100%`

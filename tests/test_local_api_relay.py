from __future__ import annotations

import contextlib
import http.client
import json
import socket
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import local_api_relay as relay


DEFAULT_UPSTREAM_HEADERS = {
    "Content-Type": "application/json",
    "X-Upstream-Trace": "stub-1",
}
DEFAULT_UPSTREAM_BODY = json.dumps(
    {
        "id": "resp_123",
        "object": "chat.completion",
        "usage": {
            "prompt_tokens": 3,
            "completion_tokens": 5,
            "total_tokens": 8,
        },
        "choices": [{"message": {"role": "assistant", "content": "ok"}}],
    }
).encode("utf-8")


def find_free_port() -> int:
    try:
        with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.bind(("127.0.0.1", 0))
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return int(sock.getsockname()[1])
    except PermissionError as exc:
        raise unittest.SkipTest("loopback sockets unavailable in this environment") from exc


def make_request(
    port: int,
    method: str,
    path: str,
    *,
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
) -> tuple[int, dict[str, str], bytes]:
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    connection.request(method, path, body=body, headers=headers or {})
    response = connection.getresponse()
    payload = response.read()
    response_headers = {key.lower(): value for key, value in response.getheaders()}
    connection.close()
    return response.status, response_headers, payload


def wait_for(predicate: Any, timeout: float = 2.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError("condition was not met before timeout")


def make_config_payload(
    *,
    listen_port: int,
    database_path: str,
    upstream_base_urls: dict[str, str] | None = None,
) -> dict[str, Any]:
    upstream_base_urls = upstream_base_urls or {
        "primary": "https://primary.example.com/v1",
        "backup": "https://backup.example.com/v1",
    }
    return {
        "server": {
            "listen_host": "127.0.0.1",
            "listen_port": listen_port,
            "admin_key": "relay-admin",
            "database_path": database_path,
            "idle_window_seconds": 300,
            "request_timeout_seconds": 120,
        },
        "local": {
            "clients": [
                {
                    "client_id": "chat-client",
                    "local_key": "local-chat-key",
                }
            ]
        },
        "upstreams": {
            "primary": {
                "base_url": upstream_base_urls["primary"],
                "api_key": "upstream-primary-key",
                "enabled": True,
                "transport": {"timeout_seconds": 120},
            },
            "backup": {
                "base_url": upstream_base_urls["backup"],
                "api_key": "upstream-backup-key",
                "enabled": True,
                "transport": {"timeout_seconds": 120},
            },
        },
        "order": ["primary", "backup"],
    }


class RecordingUpstreamHandler(BaseHTTPRequestHandler):
    requests: list[dict[str, object]] = []
    response_plan: list[dict[str, object]] = []
    response_plan_lock = threading.Lock()

    def do_POST(self) -> None:
        self._record_and_reply()

    def do_GET(self) -> None:
        self._record_and_reply()

    def log_message(self, format: str, *args: object) -> None:
        return

    @classmethod
    def reset(cls) -> None:
        cls.requests = []
        cls.response_plan = []

    @classmethod
    def queue_responses(cls, *responses: dict[str, object]) -> None:
        with cls.response_plan_lock:
            cls.response_plan.extend(responses)

    def _record_and_reply(self) -> None:
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length)
        self.__class__.requests.append(
            {
                "method": self.command,
                "path": self.path,
                "headers": {key.lower(): value for key, value in self.headers.items()},
                "body": body,
            }
        )

        response_status = 201
        response_headers = dict(DEFAULT_UPSTREAM_HEADERS)
        response_body = DEFAULT_UPSTREAM_BODY
        with self.__class__.response_plan_lock:
            if self.__class__.response_plan:
                response_plan_item = self.__class__.response_plan.pop(0)
                response_status = int(response_plan_item.get("status", response_status))
                response_headers = dict(response_plan_item.get("headers", response_headers))
                response_body = response_plan_item.get("body", response_body)

        self.send_response(response_status)
        for key, value in response_headers.items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        self.wfile.write(response_body)


class UpstreamServer:
    def __init__(self) -> None:
        RecordingUpstreamHandler.reset()
        self.port = find_free_port()
        try:
            self.server = ThreadingHTTPServer(("127.0.0.1", self.port), RecordingUpstreamHandler)
        except PermissionError as exc:
            raise unittest.SkipTest("loopback sockets unavailable in this environment") from exc
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> "UpstreamServer":
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class RelayServer:
    def __init__(self, config_path: Path) -> None:
        self.config = relay.RelayConfig.load(config_path)
        self.usage_store = relay.UsageStore(self.config.database_path)
        runtime = relay.RelayRuntime(
            config=self.config,
            usage_store=self.usage_store,
            config_version=relay._hash_config_bytes(config_path.read_bytes()),
            close_usage_store_when_drained=False,
        )
        self.runtime_manager = relay.RelayRuntimeManager(
            config_path=config_path,
            active_runtime=runtime,
            reload_poll_interval_seconds=1.0,
        )
        try:
            self.server = relay.RelayHTTPServer(
                (self.config.listen_host, self.config.listen_port),
                relay.RelayRequestHandler,
                self.runtime_manager,
            )
        except PermissionError as exc:
            self.usage_store.close()
            raise unittest.SkipTest("loopback sockets unavailable in this environment") from exc
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> "RelayServer":
        self.thread.start()
        deadline = time.time() + 2
        while time.time() < deadline:
            try:
                make_request(
                    self.config.listen_port,
                    "GET",
                    "/_relay/health",
                    headers={"X-Relay-Admin-Key": self.config.admin_key},
                )
                return self
            except OSError:
                time.sleep(0.05)
        raise AssertionError("relay did not become ready in time")

    def __exit__(self, exc_type, exc, tb) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.usage_store.close()


class RelayServiceHarness:
    def __init__(self, config_path: Path, reload_poll_interval_seconds: float = 0.05) -> None:
        self.service = relay.RelayService(config_path, reload_poll_interval_seconds=reload_poll_interval_seconds)
        self.config = relay.RelayConfig.load(config_path)
        self.port = self.config.listen_port
        self.admin_key = self.config.admin_key
        self.thread = threading.Thread(target=self.service.serve_forever, daemon=True)

    def __enter__(self) -> "RelayServiceHarness":
        self.thread.start()
        deadline = time.time() + 2
        while time.time() < deadline:
            try:
                make_request(
                    self.port,
                    "GET",
                    "/_relay/health",
                    headers={"X-Relay-Admin-Key": self.admin_key},
                )
                return self
            except OSError:
                time.sleep(0.05)
        raise AssertionError("relay service did not become ready in time")

    def __exit__(self, exc_type, exc, tb) -> None:
        self.service.shutdown()
        self.thread.join(timeout=2)


class RelayRuntimeManagerTests(unittest.TestCase):
    def test_runtime_manager_tracks_runtime_lease_and_reload_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "relay-config.json"
            payload = make_config_payload(
                listen_port=find_free_port(),
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

            config = relay.RelayConfig.load(config_path)
            store = relay.UsageStore(Path(tmp_dir) / "relay.sqlite3")
            try:
                runtime = relay.RelayRuntime(
                    config=config,
                    usage_store=store,
                    config_version=relay._hash_config_bytes(config_path.read_bytes()),
                    close_usage_store_when_drained=False,
                )
                manager = relay.RelayRuntimeManager(
                    config_path=config_path,
                    active_runtime=runtime,
                    reload_poll_interval_seconds=1.0,
                )

                lease = manager.acquire_runtime()
                self.assertEqual(lease.config.order, ["primary", "backup"])
                self.assertEqual(runtime.active_requests, 1)
                self.assertEqual(manager.reload_state_snapshot()["last_reload_status"], "never")
                self.assertEqual(manager.reload_state_snapshot()["draining_runtimes"], 0)
                lease.close()
                self.assertEqual(runtime.active_requests, 0)
            finally:
                store.close()


class RelayPlanningTests(unittest.TestCase):
    def write_config(self, tmp_dir: str, payload: dict[str, Any]) -> Path:
        config_path = Path(tmp_dir) / "relay-config.json"
        config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return config_path

    def test_relay_config_loads_final_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            payload = make_config_payload(
                listen_port=8787,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            config = relay.RelayConfig.load(self.write_config(tmp_dir, payload))

        self.assertEqual(config.listen_host, "127.0.0.1")
        self.assertEqual(config.listen_port, 8787)
        self.assertEqual(config.local_clients_by_key["local-chat-key"].client_id, "chat-client")
        self.assertEqual(config.upstreams_by_id["primary"].api_key, "upstream-primary-key")
        self.assertEqual(config.order, ["primary", "backup"])

    def test_plan_request_attempts_uses_global_order_and_preserves_request_body(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            payload = make_config_payload(
                listen_port=8787,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            config = relay.RelayConfig.load(self.write_config(tmp_dir, payload))

        request_body = b'{"model":"gpt-test","messages":[]}'
        plan = relay.plan_request_attempts(
            config,
            headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
            request_body=request_body,
        )

        self.assertIsNone(plan.error_status)
        self.assertEqual(plan.client.client_id, "chat-client")
        self.assertEqual(plan.requested_model, "gpt-test")
        self.assertEqual([attempt.upstream.upstream_id for attempt in plan.attempts], ["primary", "backup"])
        self.assertTrue(all(attempt.request_body == request_body for attempt in plan.attempts))

    def test_plan_request_attempts_skips_disabled_upstreams_and_allows_missing_model(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            payload = make_config_payload(
                listen_port=8787,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            payload["upstreams"]["primary"]["enabled"] = False
            config = relay.RelayConfig.load(self.write_config(tmp_dir, payload))

        plan = relay.plan_request_attempts(
            config,
            headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
            request_body=b'{"messages":[]}',
        )

        self.assertIsNone(plan.error_status)
        self.assertIsNone(plan.requested_model)
        self.assertEqual([attempt.upstream.upstream_id for attempt in plan.attempts], ["backup"])

    def test_plan_request_attempts_rejects_invalid_local_key_and_missing_enabled_upstreams(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            payload = make_config_payload(
                listen_port=8787,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            config = relay.RelayConfig.load(self.write_config(tmp_dir, payload))

            disabled_payload = make_config_payload(
                listen_port=8787,
                database_path=str(Path(tmp_dir) / "relay-2.sqlite3"),
            )
            disabled_payload["upstreams"]["primary"]["enabled"] = False
            disabled_payload["upstreams"]["backup"]["enabled"] = False
            disabled_config = relay.RelayConfig.load(self.write_config(tmp_dir, disabled_payload))

        invalid_key_plan = relay.plan_request_attempts(
            config,
            headers={"Authorization": "Bearer wrong-key", "Content-Type": "application/json"},
            request_body=b'{"model":"gpt-test"}',
        )
        missing_upstreams_plan = relay.plan_request_attempts(
            disabled_config,
            headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
            request_body=b'{"model":"gpt-test"}',
        )

        self.assertEqual(invalid_key_plan.error_status, 401)
        self.assertEqual(invalid_key_plan.error_kind, "invalid_local_key")
        self.assertEqual(invalid_key_plan.attempts, [])

        self.assertEqual(missing_upstreams_plan.error_status, 502)
        self.assertEqual(missing_upstreams_plan.error_kind, "no_enabled_upstreams")
        self.assertEqual(missing_upstreams_plan.attempts, [])

    def test_should_retry_with_fallback_only_for_5xx_and_obvious_model_mismatch_4xx(self) -> None:
        self.assertTrue(relay._should_retry_with_fallback(500, "", b"", "gpt-test"))
        self.assertTrue(
            relay._should_retry_with_fallback(
                400,
                "application/json",
                b'{"error":{"message":"Model gpt-test is not supported here"}}',
                "gpt-test",
            )
        )
        self.assertTrue(
            relay._should_retry_with_fallback(
                404,
                "application/json",
                b'{"error":{"message":"No such model: gpt-test"}}',
                "gpt-test",
            )
        )
        self.assertFalse(
            relay._should_retry_with_fallback(
                403,
                "application/json",
                b'{"error":{"message":"Your request was blocked."}}',
                "gpt-test",
            )
        )
        self.assertFalse(
            relay._should_retry_with_fallback(
                400,
                "application/json",
                b'{"error":{"message":"bad request"}}',
                "gpt-test",
            )
        )


class RelayStoreTests(unittest.TestCase):
    def test_usage_store_aggregates_totals_clients_upstreams_and_models(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            store = relay.UsageStore(Path(tmp_dir) / "relay.sqlite3")
            try:
                start = datetime(2026, 3, 28, 6, 0, tzinfo=timezone.utc)
                store.record_request(
                    started_at=start,
                    finished_at=start,
                    client_id="chat-client",
                    upstream_id="primary",
                    requested_model="gpt-test",
                    method="POST",
                    path="/v1/chat/completions",
                    status_code=201,
                    forwarded=True,
                    request_bytes=120,
                    response_bytes=240,
                    upstream_ms=18,
                    error_kind=None,
                    prompt_tokens=3,
                    completion_tokens=5,
                    total_tokens=8,
                )
                store.record_request(
                    started_at=start,
                    finished_at=start,
                    client_id="chat-client",
                    upstream_id="backup",
                    requested_model="gpt-test",
                    method="POST",
                    path="/v1/chat/completions",
                    status_code=503,
                    forwarded=True,
                    request_bytes=110,
                    response_bytes=90,
                    upstream_ms=25,
                    error_kind="upstream_status_fallback",
                    prompt_tokens=None,
                    completion_tokens=None,
                    total_tokens=None,
                )
                store.record_request(
                    started_at=start,
                    finished_at=start,
                    client_id=None,
                    upstream_id=None,
                    requested_model="gpt-test",
                    method="POST",
                    path="/v1/chat/completions",
                    status_code=401,
                    forwarded=False,
                    request_bytes=100,
                    response_bytes=60,
                    upstream_ms=2,
                    error_kind="invalid_local_key",
                    prompt_tokens=None,
                    completion_tokens=None,
                    total_tokens=None,
                )

                stats = store.stats_summary()
            finally:
                store.close()

        self.assertEqual(stats["totals"]["requests"], 3)
        self.assertEqual(stats["totals"]["successes"], 1)
        self.assertEqual(stats["totals"]["failures"], 2)
        self.assertEqual(stats["totals"]["local_rejects"], 1)
        self.assertEqual(stats["totals"]["status_codes"], {"201": 1, "401": 1, "503": 1})
        self.assertEqual(stats["totals"]["usage"]["responses"], 1)
        self.assertEqual(stats["clients"][0]["client_id"], "chat-client")
        self.assertEqual(stats["clients"][0]["requests"], 2)
        upstream_stats = {item["upstream_id"]: item for item in stats["upstreams"]}
        self.assertEqual(upstream_stats["primary"]["successes"], 1)
        self.assertEqual(upstream_stats["backup"]["failures"], 1)
        model_stats = {item["model"]: item for item in stats["models"]}
        self.assertEqual(model_stats["gpt-test"]["requests"], 2)
        self.assertNotIn("upstream_models", model_stats["gpt-test"])


class RelayServerTests(unittest.TestCase):
    def test_health_endpoint_reports_reload_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "relay-config.json"
            payload = make_config_payload(
                listen_port=find_free_port(),
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

            with RelayServer(config_path):
                status, _, payload_bytes = make_request(
                    payload["server"]["listen_port"],
                    "GET",
                    "/_relay/health",
                    headers={"X-Relay-Admin-Key": payload["server"]["admin_key"]},
                )

        self.assertEqual(status, 200)
        health = json.loads(payload_bytes)
        self.assertEqual(health["reload"]["last_reload_status"], "never")
        self.assertTrue(health["reload"]["reload_enabled"])
        self.assertEqual(health["reload"]["config_path"], str(config_path))
        self.assertEqual(health["reload"]["draining_runtimes"], 0)


class RelayHotReloadTests(unittest.TestCase):
    def write_config(self, path: Path, payload: dict[str, Any]) -> None:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def test_runtime_only_reload_updates_order_and_local_keys(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            primary_upstream = UpstreamServer()
            backup_upstream = UpstreamServer()
            with primary_upstream, backup_upstream:
                port = find_free_port()
                config_path = Path(tmp_dir) / "relay-config.json"
                payload = make_config_payload(
                    listen_port=port,
                    database_path=str(Path(tmp_dir) / "relay.sqlite3"),
                    upstream_base_urls={
                        "primary": f"http://127.0.0.1:{primary_upstream.port}/v1",
                        "backup": f"http://127.0.0.1:{backup_upstream.port}/v1",
                    },
                )
                self.write_config(config_path, payload)

                with RelayServiceHarness(config_path):
                    RecordingUpstreamHandler.queue_responses({"status": 201})
                    status, _, _ = make_request(
                        port,
                        "POST",
                        "/v1/chat/completions",
                        headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
                        body=b'{"model":"gpt-test","messages":[]}',
                    )
                    self.assertEqual(status, 201)
                    self.assertEqual(
                        RecordingUpstreamHandler.requests[0]["headers"]["authorization"],
                        "Bearer upstream-primary-key",
                    )

                    payload["order"] = ["backup", "primary"]
                    payload["local"]["clients"].append({"client_id": "new-client", "local_key": "local-new-key"})
                    self.write_config(config_path, payload)

                    def reloaded() -> bool:
                        health_status, _, health_payload = make_request(
                            port,
                            "GET",
                            "/_relay/health",
                            headers={"X-Relay-Admin-Key": payload["server"]["admin_key"]},
                        )
                        health = json.loads(health_payload)
                        return health_status == 200 and health["order"] == ["backup", "primary"]

                    wait_for(reloaded)

                    RecordingUpstreamHandler.requests.clear()
                    RecordingUpstreamHandler.queue_responses({"status": 201})
                    status, _, _ = make_request(
                        port,
                        "POST",
                        "/v1/chat/completions",
                        headers={"Authorization": "Bearer local-new-key", "Content-Type": "application/json"},
                        body=b'{"model":"gpt-test","messages":[]}',
                    )
                    self.assertEqual(status, 201)
                    self.assertEqual(
                        RecordingUpstreamHandler.requests[0]["headers"]["authorization"],
                        "Bearer upstream-backup-key",
                    )

    def test_invalid_reload_keeps_old_runtime_and_reports_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            port = find_free_port()
            config_path = Path(tmp_dir) / "relay-config.json"
            payload = make_config_payload(
                listen_port=port,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            self.write_config(config_path, payload)

            with RelayServiceHarness(config_path):
                config_path.write_text("{bad json", encoding="utf-8")

                def reload_failed() -> bool:
                    status, _, response = make_request(
                        port,
                        "GET",
                        "/_relay/health",
                        headers={"X-Relay-Admin-Key": payload["server"]["admin_key"]},
                    )
                    health = json.loads(response)
                    return status == 200 and health["reload"]["last_reload_status"] == "error"

                wait_for(reload_failed)

                status, _, _ = make_request(
                    port,
                    "GET",
                    "/_relay/health",
                    headers={"X-Relay-Admin-Key": payload["server"]["admin_key"]},
                )
                self.assertEqual(status, 200)


class LocalApiRelayIntegrationTests(unittest.TestCase):
    def write_config(self, tmp_dir: str, payload: dict[str, Any]) -> Path:
        config_path = Path(tmp_dir) / "relay-config.json"
        config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return config_path

    def test_admin_endpoints_and_fallback_flow_match_final_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir, UpstreamServer() as upstream:
            relay_port = find_free_port()
            payload = make_config_payload(
                listen_port=relay_port,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
                upstream_base_urls={
                    "primary": f"http://127.0.0.1:{upstream.port}/primary",
                    "backup": f"http://127.0.0.1:{upstream.port}/backup",
                },
            )
            config_path = self.write_config(tmp_dir, payload)
            RecordingUpstreamHandler.queue_responses(
                {
                    "status": 400,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Upstream-Trace": "primary-unsupported",
                    },
                    "body": b'{"error":{"message":"Model gpt-test is not supported here"}}',
                },
                {
                    "status": 201,
                    "headers": dict(DEFAULT_UPSTREAM_HEADERS),
                    "body": DEFAULT_UPSTREAM_BODY,
                },
            )

            with RelayServer(config_path):
                health_status, _, health_payload = make_request(
                    relay_port,
                    "GET",
                    "/_relay/health",
                    headers={"X-Relay-Admin-Key": "relay-admin"},
                )
                self.assertEqual(health_status, 200)
                health = json.loads(health_payload)
                self.assertEqual(health["status"], "ok")
                self.assertEqual(health["local"]["clients"], ["chat-client"])
                self.assertEqual(health["order"], ["primary", "backup"])
                self.assertEqual(
                    [item["upstream_id"] for item in health["upstreams"]],
                    ["backup", "primary"],
                )

                idle_status, _, idle_payload = make_request(
                    relay_port,
                    "GET",
                    "/_relay/idle",
                    headers={"X-Relay-Admin-Key": "relay-admin"},
                )
                self.assertEqual(idle_status, 200)
                idle = json.loads(idle_payload)
                self.assertTrue(idle["idle"])
                self.assertEqual(idle["reason"], "no_recent_requests")

                request_body = b'{"model":"gpt-test","messages":[]}'
                status, headers, payload = make_request(
                    relay_port,
                    "POST",
                    "/v1/chat/completions",
                    headers={
                        "Authorization": "Bearer local-chat-key",
                        "Content-Type": "application/json",
                    },
                    body=request_body,
                )
                self.assertEqual(status, 201)
                self.assertEqual(headers.get("x-upstream-trace"), "stub-1")
                self.assertEqual(payload, DEFAULT_UPSTREAM_BODY)
                self.assertEqual(len(RecordingUpstreamHandler.requests), 2)
                self.assertEqual(RecordingUpstreamHandler.requests[0]["path"], "/primary/v1/chat/completions")
                self.assertEqual(RecordingUpstreamHandler.requests[1]["path"], "/backup/v1/chat/completions")
                self.assertEqual(
                    RecordingUpstreamHandler.requests[0]["headers"]["authorization"],
                    "Bearer upstream-primary-key",
                )
                self.assertEqual(
                    RecordingUpstreamHandler.requests[1]["headers"]["authorization"],
                    "Bearer upstream-backup-key",
                )
                self.assertEqual(RecordingUpstreamHandler.requests[0]["body"], request_body)
                self.assertEqual(RecordingUpstreamHandler.requests[1]["body"], request_body)

                stats_status, _, stats_payload = make_request(
                    relay_port,
                    "GET",
                    "/_relay/stats",
                    headers={"X-Relay-Admin-Key": "relay-admin"},
                )
                self.assertEqual(stats_status, 200)
                stats = json.loads(stats_payload)
                self.assertEqual(stats["totals"]["requests"], 2)
                self.assertEqual(stats["totals"]["successes"], 1)
                self.assertEqual(stats["totals"]["failures"], 1)
                upstream_stats = {item["upstream_id"]: item for item in stats["upstreams"]}
                self.assertEqual(upstream_stats["primary"]["status_codes"], {"400": 1})
                self.assertEqual(upstream_stats["backup"]["status_codes"], {"201": 1})
                self.assertEqual(stats["clients"][0]["client_id"], "chat-client")
                self.assertEqual(stats["models"][0]["model"], "gpt-test")

                idle_status, _, idle_payload = make_request(
                    relay_port,
                    "GET",
                    "/_relay/idle",
                    headers={"X-Relay-Admin-Key": "relay-admin"},
                )
                self.assertEqual(idle_status, 200)
                idle = json.loads(idle_payload)
                self.assertFalse(idle["idle"])
                self.assertEqual(idle["reason"], "recent_successful_requests")

    def test_last_meaningful_error_is_returned_when_later_upstreams_only_hit_gateway_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir, UpstreamServer() as upstream:
            relay_port = find_free_port()
            unreachable_port = find_free_port()
            payload = make_config_payload(
                listen_port=relay_port,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
                upstream_base_urls={
                    "primary": f"http://127.0.0.1:{upstream.port}/primary",
                    "backup": f"http://127.0.0.1:{unreachable_port}/backup",
                },
            )
            config_path = self.write_config(tmp_dir, payload)
            RecordingUpstreamHandler.queue_responses(
                {
                    "status": 404,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Upstream-Trace": "primary-no-model",
                    },
                    "body": b'{"error":{"message":"No such model: gpt-test"}}',
                }
            )

            with RelayServer(config_path):
                status, headers, payload = make_request(
                    relay_port,
                    "POST",
                    "/v1/chat/completions",
                    headers={
                        "Authorization": "Bearer local-chat-key",
                        "Content-Type": "application/json",
                    },
                    body=b'{"model":"gpt-test","messages":[]}',
                )
                self.assertEqual(status, 404)
                self.assertEqual(headers.get("x-upstream-trace"), "primary-no-model")
                self.assertEqual(
                    json.loads(payload)["error"]["message"],
                    "No such model: gpt-test",
                )
                self.assertEqual(len(RecordingUpstreamHandler.requests), 1)

                stats_status, _, stats_payload = make_request(
                    relay_port,
                    "GET",
                    "/_relay/stats",
                    headers={"X-Relay-Admin-Key": "relay-admin"},
                )
                self.assertEqual(stats_status, 200)
                stats = json.loads(stats_payload)
                self.assertEqual(stats["totals"]["requests"], 2)
                self.assertEqual(stats["totals"]["successes"], 0)
                self.assertEqual(stats["totals"]["failures"], 2)

    def test_admin_endpoints_require_admin_key_and_invalid_local_key_is_counted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir, UpstreamServer() as upstream:
            relay_port = find_free_port()
            payload = make_config_payload(
                listen_port=relay_port,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
                upstream_base_urls={
                    "primary": f"http://127.0.0.1:{upstream.port}/primary",
                    "backup": f"http://127.0.0.1:{upstream.port}/backup",
                },
            )
            config_path = self.write_config(tmp_dir, payload)

            with RelayServer(config_path):
                forbidden_status, _, forbidden_payload = make_request(relay_port, "GET", "/_relay/stats")
                self.assertEqual(forbidden_status, 403)
                self.assertEqual(json.loads(forbidden_payload)["error"]["message"], "forbidden")

                rejected_status, _, rejected_payload = make_request(
                    relay_port,
                    "POST",
                    "/v1/chat/completions",
                    headers={"Authorization": "Bearer wrong-key", "Content-Type": "application/json"},
                    body=b'{"model":"gpt-test"}',
                )
                self.assertEqual(rejected_status, 401)
                self.assertEqual(
                    json.loads(rejected_payload)["error"]["message"],
                    "invalid local api key",
                )
                self.assertEqual(RecordingUpstreamHandler.requests, [])

                stats_status, _, stats_payload = make_request(
                    relay_port,
                    "GET",
                    "/_relay/stats",
                    headers={"X-Relay-Admin-Key": "relay-admin"},
                )
                self.assertEqual(stats_status, 200)
                stats = json.loads(stats_payload)
                self.assertEqual(stats["totals"]["requests"], 1)
                self.assertEqual(stats["totals"]["local_rejects"], 1)
                self.assertEqual(stats["clients"], [])
                self.assertEqual(stats["upstreams"], [])
                self.assertEqual(stats["models"], [])


if __name__ == "__main__":
    unittest.main()

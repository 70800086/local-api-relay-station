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
                "models": {
                    "gpt-test": "primary-gpt-test",
                    "gpt-shared": "primary-gpt-shared",
                },
            },
            "backup": {
                "base_url": upstream_base_urls["backup"],
                "api_key": "upstream-backup-key",
                "enabled": True,
                "transport": {"timeout_seconds": 120},
                "models": {
                    "gpt-test": "backup-gpt-test",
                    "gpt-alt": "backup-gpt-alt",
                    "gpt-shared": "backup-gpt-shared",
                },
            },
        },
        "model_groups": {
            "gpt-test": ["primary", "backup"],
            "gpt-alt": ["backup"],
            "gpt-shared": ["primary", "backup"],
        },
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
        try:
            self.server = relay.RelayHTTPServer(
                (self.config.listen_host, self.config.listen_port),
                relay.RelayRequestHandler,
                self.config,
                self.usage_store,
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
        self.assertEqual(config.model_groups["gpt-test"], ["primary", "backup"])

    def test_plan_request_attempts_uses_model_group_order(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            payload = make_config_payload(
                listen_port=8787,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            config = relay.RelayConfig.load(self.write_config(tmp_dir, payload))

        plan = relay.plan_request_attempts(
            config,
            headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
            request_body=b'{"model":"gpt-test","messages":[]}',
        )

        self.assertIsNone(plan.error_status)
        self.assertEqual(plan.client.client_id, "chat-client")
        self.assertEqual(plan.canonical_model, "gpt-test")
        self.assertEqual([attempt.upstream.upstream_id for attempt in plan.attempts], ["primary", "backup"])
        self.assertEqual(plan.attempts[0].upstream_model, "primary-gpt-test")
        self.assertEqual(
            json.loads(plan.attempts[1].request_body.decode("utf-8"))["model"],
            "backup-gpt-test",
        )

    def test_plan_request_attempts_skips_disabled_and_unsupported_upstreams(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            payload = make_config_payload(
                listen_port=8787,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            payload["model_groups"]["gpt-alt"] = ["primary", "backup"]
            payload["upstreams"]["primary"]["enabled"] = False
            config = relay.RelayConfig.load(self.write_config(tmp_dir, payload))

        plan = relay.plan_request_attempts(
            config,
            headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
            request_body=b'{"model":"gpt-alt"}',
        )

        self.assertIsNone(plan.error_status)
        self.assertEqual([attempt.upstream.upstream_id for attempt in plan.attempts], ["backup"])
        self.assertEqual(plan.attempts[0].upstream_model, "backup-gpt-alt")

    def test_plan_request_attempts_rejects_missing_model_group_and_missing_support(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            payload = make_config_payload(
                listen_port=8787,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            missing_group_config = relay.RelayConfig.load(self.write_config(tmp_dir, payload))

            payload_without_support = make_config_payload(
                listen_port=8787,
                database_path=str(Path(tmp_dir) / "relay-2.sqlite3"),
            )
            payload_without_support["upstreams"]["primary"]["enabled"] = False
            payload_without_support["upstreams"]["backup"]["models"].pop("gpt-test")
            unsupported_config = relay.RelayConfig.load(
                self.write_config(tmp_dir, payload_without_support)
            )

        missing_group_plan = relay.plan_request_attempts(
            missing_group_config,
            headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
            request_body=b'{"model":"gpt-missing"}',
        )
        unsupported_plan = relay.plan_request_attempts(
            unsupported_config,
            headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
            request_body=b'{"model":"gpt-test"}',
        )
        missing_model_plan = relay.plan_request_attempts(
            unsupported_config,
            headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
            request_body=b'{"messages":[]}',
        )

        self.assertEqual(missing_group_plan.error_status, 400)
        self.assertEqual(missing_group_plan.error_kind, "model_not_configured")
        self.assertEqual(missing_group_plan.attempts, [])

        self.assertEqual(unsupported_plan.error_status, 400)
        self.assertEqual(unsupported_plan.error_kind, "model_not_supported")
        self.assertEqual(unsupported_plan.attempts, [])

        self.assertEqual(missing_model_plan.error_status, 400)
        self.assertEqual(missing_model_plan.error_kind, "missing_model")
        self.assertEqual(missing_model_plan.attempts, [])

    def test_should_retry_with_fallback_only_for_upstream_5xx(self) -> None:
        self.assertTrue(relay._should_retry_with_fallback(500))
        self.assertTrue(relay._should_retry_with_fallback(503))
        self.assertFalse(relay._should_retry_with_fallback(429))
        self.assertFalse(relay._should_retry_with_fallback(400))


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
                    canonical_model="gpt-test",
                    upstream_model="primary-gpt-test",
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
                    canonical_model="gpt-test",
                    upstream_model="backup-gpt-test",
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
                    canonical_model="gpt-test",
                    upstream_model=None,
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
        self.assertEqual(
            model_stats["gpt-test"]["upstream_models"],
            {"backup-gpt-test": 1, "primary-gpt-test": 1},
        )


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
                    "status": 503,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Upstream-Trace": "primary-503",
                    },
                    "body": b'{"error":{"message":"primary down"}}',
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
                self.assertEqual(health["model_groups"]["gpt-test"], ["primary", "backup"])
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
                self.assertEqual(
                    json.loads(RecordingUpstreamHandler.requests[1]["body"].decode("utf-8"))["model"],
                    "backup-gpt-test",
                )

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
                self.assertEqual(upstream_stats["primary"]["status_codes"], {"503": 1})
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
                if stats["models"]:
                    self.assertEqual([item["model"] for item in stats["models"]], ["gpt-test"])
                    self.assertEqual(stats["models"][0]["requests"], 0)
                    self.assertEqual(stats["models"][0]["successes"], 0)
                    self.assertEqual(stats["models"][0]["failures"], 0)
                else:
                    self.assertEqual(stats["models"], [])


if __name__ == "__main__":
    unittest.main()

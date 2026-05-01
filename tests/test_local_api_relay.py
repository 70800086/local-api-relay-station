from __future__ import annotations

import contextlib
import http.client
import json
import sqlite3
import socket
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
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

DEFAULT_RESPONSES_BODY = json.dumps(
    {
        "id": "resp_456",
        "object": "response",
        "usage": {
            "input_tokens": 7,
            "input_tokens_details": {"cached_tokens": 4},
            "output_tokens": 2,
            "total_tokens": 9,
        },
        "output": [
            {
                "type": "message",
                "content": [{"type": "output_text", "text": "ok"}],
            }
        ],
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
    release_response = threading.Event()
    response_started = threading.Event()

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
        cls.release_response = threading.Event()
        cls.response_started = threading.Event()

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
                wait_for_release = bool(response_plan_item.get("wait_for_release", False))
            else:
                wait_for_release = False

        if wait_for_release:
            self.__class__.response_started.set()
            self.__class__.release_response.wait(timeout=5)

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
            circuit_breaker=relay.CircuitBreakerManager([upstream.upstream_id for upstream in self.config.upstreams]),
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
                    circuit_breaker=relay.CircuitBreakerManager([upstream.upstream_id for upstream in config.upstreams]),
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

    def test_plan_request_attempts_preserves_responses_body_without_auto_prompt_cache_key(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            payload = make_config_payload(
                listen_port=8787,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            config = relay.RelayConfig.load(self.write_config(tmp_dir, payload))

        first_request_body = b'{"model":"gpt-5-mini","input":"Reply with: pong","max_output_tokens":16}'
        second_request_body = b'{\n  "max_output_tokens": 16,\n  "input": "Reply with: pong",\n  "model": "gpt-5-mini"\n}'
        first_plan = relay.plan_request_attempts(
            config,
            headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
            request_body=first_request_body,
            request_path="/v1/responses",
        )
        second_plan = relay.plan_request_attempts(
            config,
            headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
            request_body=second_request_body,
            request_path="/responses",
        )

        self.assertEqual(first_plan.error_status, None)
        self.assertEqual(second_plan.error_status, None)
        self.assertEqual(first_plan.attempts[0].request_body, first_request_body)
        self.assertEqual(second_plan.attempts[0].request_body, second_request_body)
        self.assertNotIn("prompt_cache_key", json.loads(first_plan.attempts[0].request_body))
        self.assertNotIn("prompt_cache_key", json.loads(second_plan.attempts[0].request_body))

    def test_plan_request_attempts_preserves_client_prompt_cache_key_for_responses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            payload = make_config_payload(
                listen_port=8787,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            config = relay.RelayConfig.load(self.write_config(tmp_dir, payload))

        request_body = b'{"model":"gpt-5-mini","input":"Reply with: pong","prompt_cache_key":"client-key","max_output_tokens":16}'
        plan = relay.plan_request_attempts(
            config,
            headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
            request_body=request_body,
            request_path="/v1/responses",
        )

        self.assertIsNone(plan.error_status)
        self.assertEqual(plan.attempts[0].request_body, request_body)

    def test_request_observability_kwargs_extracts_prompt_cache_key_stream_reasoning_and_canonical_hash(self) -> None:
        first = relay._request_observability_kwargs(
            b'{"model":"gpt-5-mini","input":"Reply with: pong","stream":true,"reasoning":{"effort":"high"},"prompt_cache_key":"client-key","max_output_tokens":16}'
        )
        second = relay._request_observability_kwargs(
            b'{\n  "max_output_tokens": 16,\n  "prompt_cache_key": "client-key",\n  "reasoning": {"effort": "high"},\n  "stream": true,\n  "input": "Reply with: pong",\n  "model": "gpt-5-mini"\n}'
        )

        self.assertEqual(first["prompt_cache_key"], "client-key")
        self.assertEqual(first["canonical_body_sha256"], second["canonical_body_sha256"])
        self.assertTrue(first["stream"])
        self.assertEqual(first["reasoning_effort"], "high")

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

    def test_should_retry_with_fallback_for_any_upstream_error_status(self) -> None:
        self.assertFalse(relay._should_retry_with_fallback(201, "", b"", "gpt-test"))
        self.assertTrue(relay._should_retry_with_fallback(400, "", b"", "gpt-test"))
        self.assertTrue(relay._should_retry_with_fallback(401, "", b"", "gpt-test"))
        self.assertTrue(relay._should_retry_with_fallback(403, "", b"", "gpt-test"))
        self.assertTrue(relay._should_retry_with_fallback(404, "", b"", "gpt-test"))
        self.assertTrue(relay._should_retry_with_fallback(429, "", b"", "gpt-test"))
        self.assertTrue(relay._should_retry_with_fallback(500, "", b"", "gpt-test"))

    def test_build_upstream_target_does_not_duplicate_existing_base_path_prefix(self) -> None:
        self.assertEqual(
            relay._build_upstream_target("https://api.example.com/v1", "/chat/completions"),
            "https://api.example.com/v1/chat/completions",
        )
        self.assertEqual(
            relay._build_upstream_target("https://api.example.com/v1", "/v1/chat/completions"),
            "https://api.example.com/v1/chat/completions",
        )
        self.assertEqual(
            relay._build_upstream_target("https://api.example.com/primary", "/v1/chat/completions"),
            "https://api.example.com/primary/v1/chat/completions",
        )

    def test_plan_request_attempts_skips_open_circuit_upstream(self) -> None:
        payload = make_config_payload(listen_port=find_free_port(), database_path=":memory:")
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "relay-config.json"
            config_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            config = relay.RelayConfig.load(config_path)
        breaker = relay.CircuitBreakerManager(["primary", "backup"])
        breaker.record_failure("primary", was_half_open=False)
        breaker.record_failure("primary", was_half_open=False)
        breaker.record_failure("primary", was_half_open=False)

        plan = relay.plan_request_attempts(
            config,
            headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
            request_body=b'{"model":"gpt-test"}',
            circuit_breaker=breaker,
        )

        self.assertEqual([attempt.upstream.upstream_id for attempt in plan.attempts], ["backup"])

    def test_plan_request_attempts_uses_dynamic_priority_and_restores_base_order_after_recovery(self) -> None:
        payload = make_config_payload(listen_port=find_free_port(), database_path=":memory:")
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "relay-config.json"
            config_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            config = relay.RelayConfig.load(config_path)

        breaker = relay.CircuitBreakerManager(
            ["primary", "backup"],
            config=relay.CircuitBreakerConfig(
                failure_threshold=3,
                cooldown_seconds=30,
                recovery_window_seconds=0.2,
            ),
        )
        breaker.record_soft_failure("primary", was_half_open=False)

        degraded_plan = relay.plan_request_attempts(
            config,
            headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
            request_body=b'{"model":"gpt-test"}',
            circuit_breaker=breaker,
        )
        self.assertEqual([attempt.upstream.upstream_id for attempt in degraded_plan.attempts], ["backup", "primary"])

        def recovered() -> bool:
            plan = relay.plan_request_attempts(
                config,
                headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
                request_body=b'{"model":"gpt-test"}',
                circuit_breaker=breaker,
            )
            return [attempt.upstream.upstream_id for attempt in plan.attempts] == ["primary", "backup"]

        wait_for(recovered, timeout=1.0)

    def test_plan_request_attempts_does_not_repeat_same_upstream_in_one_request(self) -> None:
        config = relay.RelayConfig(
            listen_host="127.0.0.1",
            listen_port=8787,
            admin_key="relay-admin",
            database_path=Path(":memory:"),
            idle_window_seconds=300,
            request_timeout_seconds=120,
            local_clients=[relay.LocalClientConfig(client_id="chat-client", local_key="local-chat-key")],
            upstreams=[
                relay.UpstreamConfig(
                    upstream_id="primary",
                    base_url="https://primary.example.com/v1",
                    api_key="upstream-primary-key",
                    enabled=True,
                    transport={"timeout_seconds": 120},
                ),
                relay.UpstreamConfig(
                    upstream_id="backup",
                    base_url="https://backup.example.com/v1",
                    api_key="upstream-backup-key",
                    enabled=True,
                    transport={"timeout_seconds": 120},
                ),
            ],
            order=["primary", "backup", "primary"],
        )

        plan = relay.plan_request_attempts(
            config,
            headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
            request_body=b'{"model":"gpt-test","messages":[]}',
        )

        self.assertEqual([attempt.upstream.upstream_id for attempt in plan.attempts], ["primary", "backup"])

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
                    cached_tokens=2,
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
                    cached_tokens=None,
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
                    cached_tokens=None,
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
        self.assertEqual(stats["totals"]["usage"]["cached_tokens"], 2)
        self.assertEqual(stats["clients"][0]["client_id"], "chat-client")
        self.assertEqual(stats["clients"][0]["requests"], 2)
        upstream_stats = {item["upstream_id"]: item for item in stats["upstreams"]}
        self.assertEqual(upstream_stats["primary"]["successes"], 1)
        self.assertEqual(upstream_stats["primary"]["usage"]["cached_tokens"], 2)
        self.assertEqual(upstream_stats["backup"]["failures"], 1)
        model_stats = {item["model"]: item for item in stats["models"]}
        self.assertEqual(model_stats["gpt-test"]["requests"], 2)
        self.assertEqual(model_stats["gpt-test"]["usage"]["cached_tokens"], 2)
        self.assertNotIn("upstream_models", model_stats["gpt-test"])

    def test_extract_usage_metrics_supports_responses_json_and_sse(self) -> None:
        json_metrics = relay._extract_usage_metrics(
            "application/json",
            DEFAULT_RESPONSES_BODY,
        )
        self.assertEqual(json_metrics, (7, 2, 9, 4))

        sse_body = (
            b'data: {"type":"response.output_text.delta","delta":"OK"}\n\n'
            b'data: {"type":"response.completed","response":{"usage":{"input_tokens":7,"input_tokens_details":{"cached_tokens":4},"output_tokens":2,"total_tokens":9}}}\n\n'
            b"data: [DONE]\n\n"
        )
        sse_metrics = relay._extract_usage_metrics(
            "text/event-stream",
            sse_body,
        )
        self.assertEqual(sse_metrics, (7, 2, 9, 4))


class RelayServerTests(unittest.TestCase):
    def test_pricing_endpoint_returns_builtin_catalog_entry_with_source(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            relay_port = find_free_port()
            payload = make_config_payload(
                listen_port=relay_port,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            config_path = Path(tmp_dir) / "relay-config.json"
            config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

            with RelayServer(config_path):
                status, _, payload_bytes = make_request(
                    relay_port,
                    "GET",
                    "/_relay/pricing?model=claude-sonnet-4-6",
                    headers={"X-Relay-Admin-Key": "relay-admin"},
                )

        self.assertEqual(status, 200)
        pricing = json.loads(payload_bytes)
        self.assertEqual(pricing["currency"], "USD")
        self.assertEqual(pricing["provider"]["id"], "anthropic")
        self.assertEqual(pricing["provider"]["label"], "Anthropic")
        self.assertEqual(pricing["model"]["id"], "claude-sonnet-4-6")
        self.assertEqual(pricing["pricing"]["input_per_million_tokens"], 3.0)
        self.assertEqual(pricing["pricing"]["cached_input_per_million_tokens"], 0.3)
        self.assertEqual(pricing["pricing"]["output_per_million_tokens"], 15.0)
        self.assertIn("docs.anthropic.com", pricing["pricing"]["source"]["url"])

    def test_upstream_costs_endpoint_returns_windowed_usage_and_cost_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            relay_port = find_free_port()
            payload = make_config_payload(
                listen_port=relay_port,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            payload["pricing"] = {
                "currency": "USD",
                "upstreams": {
                    "primary": {
                        "input_per_million_tokens": 1.0,
                        "output_per_million_tokens": 2.0,
                    },
                    "backup": {
                        "input_per_million_tokens": 0.5,
                        "output_per_million_tokens": 1.5,
                    },
                },
            }
            config_path = Path(tmp_dir) / "relay-config.json"
            config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

            seeded_store = relay.UsageStore(Path(tmp_dir) / "relay.sqlite3")
            try:
                window_start = datetime(2026, 4, 15, 0, 0, tzinfo=timezone.utc)
                window_end = window_start + timedelta(hours=1)
                seeded_store.record_request(
                    started_at=window_start,
                    finished_at=window_start + timedelta(minutes=10),
                    client_id="chat-client",
                    upstream_id="primary",
                    requested_model="gpt-test",
                    method="POST",
                    path="/v1/chat/completions",
                    status_code=200,
                    forwarded=True,
                    request_bytes=120,
                    response_bytes=240,
                    upstream_ms=15,
                    error_kind=None,
                    prompt_tokens=1000,
                    completion_tokens=500,
                    total_tokens=1500,
                )
                seeded_store.record_request(
                    started_at=window_start,
                    finished_at=window_start + timedelta(minutes=20),
                    client_id="chat-client",
                    upstream_id="primary",
                    requested_model="gpt-test",
                    method="POST",
                    path="/v1/chat/completions",
                    status_code=502,
                    forwarded=True,
                    request_bytes=120,
                    response_bytes=240,
                    upstream_ms=17,
                    error_kind="upstream_status_fallback",
                    prompt_tokens=200,
                    completion_tokens=100,
                    total_tokens=300,
                )
                seeded_store.record_request(
                    started_at=window_start,
                    finished_at=window_start + timedelta(minutes=40),
                    client_id="chat-client",
                    upstream_id="backup",
                    requested_model="gpt-test",
                    method="POST",
                    path="/v1/chat/completions",
                    status_code=201,
                    forwarded=True,
                    request_bytes=120,
                    response_bytes=240,
                    upstream_ms=19,
                    error_kind=None,
                    prompt_tokens=800,
                    completion_tokens=400,
                    total_tokens=1200,
                )
                seeded_store.record_request(
                    started_at=window_start,
                    finished_at=window_end,
                    client_id="chat-client",
                    upstream_id="backup",
                    requested_model="gpt-test",
                    method="POST",
                    path="/v1/chat/completions",
                    status_code=200,
                    forwarded=True,
                    request_bytes=120,
                    response_bytes=240,
                    upstream_ms=21,
                    error_kind=None,
                    prompt_tokens=999,
                    completion_tokens=999,
                    total_tokens=1998,
                )
            finally:
                seeded_store.close()

            with RelayServer(config_path):
                status, _, payload_bytes = make_request(
                    relay_port,
                    "GET",
                    "/_relay/upstream-costs?start=2026-04-15T00:00:00%2B00:00&end=2026-04-15T01:00:00%2B00:00",
                    headers={"X-Relay-Admin-Key": "relay-admin"},
                )

        self.assertEqual(status, 200)
        summary = json.loads(payload_bytes)
        self.assertEqual(summary["time_range"]["field"], "finished_at")
        self.assertEqual(summary["totals"]["requests"], 3)
        self.assertEqual(summary["totals"]["successes"], 2)
        self.assertEqual(summary["totals"]["failures"], 1)
        upstreams = {item["upstream_id"]: item for item in summary["upstreams"]}
        self.assertEqual(upstreams["primary"]["status_codes"], {"200": 1, "502": 1})
        self.assertEqual(upstreams["backup"]["status_codes"], {"201": 1})
        self.assertEqual(
            upstreams["backup"]["usage"]["estimated_cost"],
            {
                "currency": "USD",
                "estimated_requests": 1,
                "input": 0.0004,
                "cached_input": 0.0,
                "output": 0.0006,
                "total": 0.001,
            },
        )

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
        self.assertEqual(
            [item["upstream_id"] for item in health["breaker"]["upstreams"]],
            ["backup", "primary"],
        )

    def test_build_upstream_status_view_reports_cooldown_reason_and_effective_order(self) -> None:
        payload = make_config_payload(listen_port=find_free_port(), database_path=":memory:")
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "relay-config.json"
            config_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            config = relay.RelayConfig.load(config_path)

        breaker = relay.CircuitBreakerManager(["primary", "backup"])
        breaker.record_failure(
            "primary",
            was_half_open=False,
            error_kind="upstream_status",
            status_code=500,
            reason="primary failure 1",
        )
        breaker.record_failure(
            "primary",
            was_half_open=False,
            error_kind="upstream_status",
            status_code=502,
            reason="primary failure 2",
        )
        breaker.record_failure(
            "primary",
            was_half_open=False,
            error_kind="upstream_request_failed",
            reason="connect timed out",
        )

        status_view = relay.build_upstream_status_view(config, breaker)

        self.assertEqual(status_view["configured_order"], ["primary", "backup"])
        self.assertEqual(status_view["effective_order"], ["backup", "primary"])
        upstreams = {item["upstream_id"]: item for item in status_view["upstreams"]}
        primary = upstreams["primary"]
        self.assertTrue(primary["enabled"])
        self.assertEqual(primary["routing_state"], "cooldown")
        self.assertTrue(primary["cooldown_active"])
        self.assertFalse(primary["degraded_active"])
        self.assertEqual(primary["priority_penalty"], 3)
        self.assertEqual(primary["effective_index"], 1)
        self.assertEqual(primary["recovery_at"], primary["cooldown_until"])
        self.assertEqual(
            primary["last_failure"],
            {
                "at": primary["last_failure"]["at"],
                "error_kind": "upstream_request_failed",
                "reason": "connect timed out",
            },
        )
        self.assertIsNotNone(primary["last_failure"]["at"])

    def test_circuit_breaker_opens_after_repeated_failures(self) -> None:
        breaker = relay.CircuitBreakerManager(["primary"])
        breaker.record_failure("primary", was_half_open=False)
        breaker.record_failure("primary", was_half_open=False)
        breaker.record_failure("primary", was_half_open=False)
        snapshot = breaker.snapshot()
        self.assertEqual(snapshot[0]["upstream_id"], "primary")
        self.assertEqual(snapshot[0]["state"], "open")
        self.assertEqual(snapshot[0]["consecutive_failures"], 3)
        self.assertIsNotNone(snapshot[0]["cooldown_until"])

    def test_circuit_breaker_temporarily_downweights_failed_upstream_and_restores_order(self) -> None:
        breaker = relay.CircuitBreakerManager(
            ["primary", "backup"],
            config=relay.CircuitBreakerConfig(
                failure_threshold=3,
                cooldown_seconds=30,
                recovery_window_seconds=0.2,
            ),
        )

        self.assertEqual(breaker.prioritized_upstream_ids(["primary", "backup"]), ["primary", "backup"])

        breaker.record_failure("primary", was_half_open=False)

        self.assertEqual(breaker.prioritized_upstream_ids(["primary", "backup"]), ["backup", "primary"])
        snapshot = {item["upstream_id"]: item for item in breaker.snapshot()}
        self.assertEqual(snapshot["primary"]["priority_tier"], "degraded")
        self.assertIsNotNone(snapshot["primary"]["degraded_until"])

        wait_for(
            lambda: breaker.prioritized_upstream_ids(["primary", "backup"]) == ["primary", "backup"],
            timeout=1.0,
        )


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

    def test_database_path_reload_switches_new_requests_to_new_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            with UpstreamServer() as upstream:
                port = find_free_port()
                first_db = Path(tmp_dir) / "relay.sqlite3"
                second_db = Path(tmp_dir) / "relay-next.sqlite3"
                config_path = Path(tmp_dir) / "relay-config.json"
                payload = make_config_payload(
                    listen_port=port,
                    database_path=str(first_db),
                    upstream_base_urls={
                        "primary": f"http://127.0.0.1:{upstream.port}/v1",
                        "backup": f"http://127.0.0.1:{upstream.port}/v1",
                    },
                )
                self.write_config(config_path, payload)

                with RelayServiceHarness(config_path):
                    RecordingUpstreamHandler.queue_responses({"status": 201}, {"status": 201})
                    first_status, _, _ = make_request(
                        port,
                        "POST",
                        "/v1/chat/completions",
                        headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
                        body=b'{"model":"gpt-test","messages":[]}',
                    )
                    self.assertEqual(first_status, 201)

                    payload["server"]["database_path"] = str(second_db)
                    self.write_config(config_path, payload)

                    def switched() -> bool:
                        status, _, response = make_request(
                            port,
                            "GET",
                            "/_relay/health",
                            headers={"X-Relay-Admin-Key": payload["server"]["admin_key"]},
                        )
                        health = json.loads(response)
                        return status == 200 and health["server"]["database_path"] == str(second_db)

                    wait_for(switched)

                    second_status, _, _ = make_request(
                        port,
                        "POST",
                        "/v1/chat/completions",
                        headers={"Authorization": "Bearer local-chat-key", "Content-Type": "application/json"},
                        body=b'{"model":"gpt-test","messages":[]}',
                    )
                    self.assertEqual(second_status, 201)

            with sqlite3.connect(first_db) as first_conn:
                first_count = first_conn.execute(f"SELECT COUNT(*) FROM {relay.REQUEST_LOG_TABLE}").fetchone()[0]
            with sqlite3.connect(second_db) as second_conn:
                second_count = second_conn.execute(f"SELECT COUNT(*) FROM {relay.REQUEST_LOG_TABLE}").fetchone()[0]

        self.assertEqual(first_count, 1)
        self.assertEqual(second_count, 1)

    def test_listener_reload_moves_service_to_new_port(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            first_port = find_free_port()
            second_port = find_free_port()
            config_path = Path(tmp_dir) / "relay-config.json"
            payload = make_config_payload(
                listen_port=first_port,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            self.write_config(config_path, payload)

            with RelayServiceHarness(config_path):
                payload["server"]["listen_port"] = second_port
                self.write_config(config_path, payload)

                def moved() -> bool:
                    try:
                        status, _, response = make_request(
                            second_port,
                            "GET",
                            "/_relay/health",
                            headers={"X-Relay-Admin-Key": payload["server"]["admin_key"]},
                        )
                    except OSError:
                        return False
                    health = json.loads(response)
                    return status == 200 and health["server"]["listen_port"] == second_port

                wait_for(moved)
                with self.assertRaises(OSError):
                    make_request(
                        first_port,
                        "GET",
                        "/_relay/health",
                        headers={"X-Relay-Admin-Key": payload["server"]["admin_key"]},
                    )

    def test_listener_reload_bind_failure_keeps_previous_port_alive(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            first_port = find_free_port()
            blocked_port = find_free_port()
            config_path = Path(tmp_dir) / "relay-config.json"
            payload = make_config_payload(
                listen_port=first_port,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
            )
            self.write_config(config_path, payload)

            blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            blocker.bind(("127.0.0.1", blocked_port))
            blocker.listen(1)
            try:
                with RelayServiceHarness(config_path):
                    payload["server"]["listen_port"] = blocked_port
                    self.write_config(config_path, payload)

                    def failed() -> bool:
                        status, _, response = make_request(
                            first_port,
                            "GET",
                            "/_relay/health",
                            headers={"X-Relay-Admin-Key": payload["server"]["admin_key"]},
                        )
                        health = json.loads(response)
                        return status == 200 and health["reload"]["last_reload_status"] == "error"

                    wait_for(failed)
            finally:
                blocker.close()

    def test_shutdown_waits_for_in_flight_request_before_closing_active_usage_store(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir, UpstreamServer() as upstream:
            port = find_free_port()
            config_path = Path(tmp_dir) / "relay-config.json"
            payload = make_config_payload(
                listen_port=port,
                database_path=str(Path(tmp_dir) / "relay.sqlite3"),
                upstream_base_urls={
                    "primary": f"http://127.0.0.1:{upstream.port}/v1",
                    "backup": f"http://127.0.0.1:{upstream.port}/v1",
                },
            )
            self.write_config(config_path, payload)
            service = relay.RelayService(config_path, reload_poll_interval_seconds=0.05)
            service_thread = threading.Thread(target=service.serve_forever, daemon=True)
            service_thread.start()
            original_server_close = service.listener.server.server_close
            original_server_shutdown = service.listener.server.shutdown
            try:
                wait_for(
                    lambda: make_request(
                        port,
                        "GET",
                        "/_relay/health",
                        headers={"X-Relay-Admin-Key": payload["server"]["admin_key"]},
                    )[0]
                    == 200
                )

                active_store = service.runtime_manager.active_runtime().usage_store
                store_closed = threading.Event()
                original_close = active_store.close

                def tracked_close() -> None:
                    store_closed.set()
                    original_close()

                active_store.close = tracked_close  # type: ignore[method-assign]
                server_shutdown_completed = threading.Event()

                def tracked_server_shutdown() -> None:
                    original_server_shutdown()
                    server_shutdown_completed.set()

                service.listener.server.shutdown = tracked_server_shutdown  # type: ignore[method-assign]
                service.listener.server.server_close = lambda: None  # type: ignore[method-assign]

                RecordingUpstreamHandler.queue_responses({"status": 201, "wait_for_release": True})
                request_result: dict[str, tuple[int, dict[str, str], bytes]] = {}

                def perform_request() -> None:
                    request_result["response"] = make_request(
                        port,
                        "POST",
                        "/v1/chat/completions",
                        headers={
                            "Authorization": "Bearer local-chat-key",
                            "Content-Type": "application/json",
                        },
                        body=b'{"model":"gpt-test","messages":[]}',
                    )

                request_thread = threading.Thread(target=perform_request, daemon=True)
                request_thread.start()
                wait_for(lambda: RecordingUpstreamHandler.response_started.is_set())

                service.shutdown()
                wait_for(lambda: server_shutdown_completed.is_set())
                time.sleep(0.05)
                self.assertFalse(
                    store_closed.is_set(),
                    "shutdown closed the active usage store before the in-flight request finished",
                )
                self.assertTrue(
                    request_thread.is_alive(),
                    "request unexpectedly finished before the upstream gate was released",
                )

                RecordingUpstreamHandler.release_response.set()
                request_thread.join(timeout=2)
                self.assertFalse(request_thread.is_alive(), "in-flight request did not complete after release")
                service_thread.join(timeout=2)
                self.assertFalse(service_thread.is_alive(), "relay service did not stop after request drain")
                self.assertTrue(store_closed.is_set(), "active usage store was never closed during shutdown")

                status, _, body = request_result["response"]
                self.assertEqual(status, 201)
                self.assertEqual(body, DEFAULT_UPSTREAM_BODY)
            finally:
                RecordingUpstreamHandler.release_response.set()
                service.listener.server.shutdown = original_server_shutdown  # type: ignore[method-assign]
                service.listener.server.server_close = original_server_close  # type: ignore[method-assign]
                service.shutdown()
                service_thread.join(timeout=2)
                original_server_close()


class LocalApiRelayIntegrationTests(unittest.TestCase):
    def write_config(self, tmp_dir: str, payload: dict[str, Any]) -> Path:
        config_path = Path(tmp_dir) / "relay-config.json"
        config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return config_path

    def test_responses_requests_preserve_original_body_without_auto_prompt_cache_key(self) -> None:
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
                    "status": 200,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Upstream-Trace": "stub-1",
                    },
                    "body": DEFAULT_RESPONSES_BODY,
                },
                {
                    "status": 200,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Upstream-Trace": "stub-2",
                    },
                    "body": DEFAULT_RESPONSES_BODY,
                },
            )

            first_request_body = b'{"model":"gpt-5-mini","input":"Reply with: pong","max_output_tokens":16}'
            second_request_body = b'{\n  "max_output_tokens": 16,\n  "input": "Reply with: pong",\n  "model": "gpt-5-mini"\n}'
            with RelayServer(config_path):
                first_status, _, first_response = make_request(
                    relay_port,
                    "POST",
                    "/v1/responses",
                    headers={
                        "Authorization": "Bearer local-chat-key",
                        "Content-Type": "application/json",
                    },
                    body=first_request_body,
                )
                second_status, _, second_response = make_request(
                    relay_port,
                    "POST",
                    "/v1/responses",
                    headers={
                        "Authorization": "Bearer local-chat-key",
                        "Content-Type": "application/json",
                    },
                    body=second_request_body,
                )

        self.assertEqual(first_status, 200)
        self.assertEqual(second_status, 200)
        self.assertEqual(first_response, DEFAULT_RESPONSES_BODY)
        self.assertEqual(second_response, DEFAULT_RESPONSES_BODY)
        self.assertEqual(len(RecordingUpstreamHandler.requests), 2)
        self.assertEqual(RecordingUpstreamHandler.requests[0]["path"], "/primary/v1/responses")
        self.assertEqual(RecordingUpstreamHandler.requests[1]["path"], "/primary/v1/responses")
        self.assertEqual(RecordingUpstreamHandler.requests[0]["body"], first_request_body)
        self.assertEqual(RecordingUpstreamHandler.requests[1]["body"], second_request_body)
        self.assertNotIn("prompt_cache_key", json.loads(RecordingUpstreamHandler.requests[0]["body"]))
        self.assertNotIn("prompt_cache_key", json.loads(RecordingUpstreamHandler.requests[1]["body"]))

    def test_responses_requests_preserve_client_prompt_cache_key_when_present(self) -> None:
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
                    "status": 200,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Upstream-Trace": "client-key-preserved",
                    },
                    "body": DEFAULT_RESPONSES_BODY,
                }
            )

            request_body = b'{"model":"gpt-5-mini","input":"Reply with: pong","prompt_cache_key":"client-key","max_output_tokens":16}'
            with RelayServer(config_path):
                status, _, response_body = make_request(
                    relay_port,
                    "POST",
                    "/v1/responses",
                    headers={
                        "Authorization": "Bearer local-chat-key",
                        "Content-Type": "application/json",
                    },
                    body=request_body,
                )

        self.assertEqual(status, 200)
        self.assertEqual(response_body, DEFAULT_RESPONSES_BODY)
        self.assertEqual(len(RecordingUpstreamHandler.requests), 1)
        self.assertEqual(RecordingUpstreamHandler.requests[0]["path"], "/primary/v1/responses")
        self.assertEqual(RecordingUpstreamHandler.requests[0]["body"], request_body)

    def test_request_trace_endpoint_includes_request_observability_fields_for_responses(self) -> None:
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
                    "status": 200,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Upstream-Trace": "responses-observability",
                    },
                    "body": DEFAULT_RESPONSES_BODY,
                }
            )

            with RelayServer(config_path):
                status, headers, response_body = make_request(
                    relay_port,
                    "POST",
                    "/v1/responses",
                    headers={
                        "Authorization": "Bearer local-chat-key",
                        "Content-Type": "application/json",
                    },
                    body=b'{"model":"gpt-5-mini","input":"Reply with: pong","max_output_tokens":16,"stream":true,"reasoning":{"effort":"high"},"prompt_cache_key":"client-key"}',
                )
                self.assertEqual(status, 200)
                self.assertEqual(response_body, DEFAULT_RESPONSES_BODY)
                request_trace_id = headers.get("x-relay-request-trace-id")
                self.assertTrue(request_trace_id)

                trace_status, _, trace_payload = make_request(
                    relay_port,
                    "GET",
                    f"/_relay/request-traces?request_trace_id={request_trace_id}",
                    headers={"X-Relay-Admin-Key": "relay-admin"},
                )
                self.assertEqual(trace_status, 200)
                trace = json.loads(trace_payload)

        self.assertEqual(trace["attempts"][0]["prompt_cache_key"], "client-key")
        self.assertTrue(trace["attempts"][0]["stream"])
        self.assertEqual(trace["attempts"][0]["reasoning_effort"], "high")
        self.assertTrue(trace["attempts"][0]["canonical_body_sha256"])

    def test_request_trace_endpoint_reconstructs_fallback_attempt_chain(self) -> None:
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
                    "status": 404,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Upstream-Trace": "primary-no-model",
                    },
                    "body": b'{"error":{"message":"No such model: gpt-test"}}',
                },
                {
                    "status": 201,
                    "headers": dict(DEFAULT_UPSTREAM_HEADERS),
                    "body": DEFAULT_UPSTREAM_BODY,
                },
            )

            with RelayServer(config_path):
                status, headers, response_body = make_request(
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
                self.assertEqual(response_body, DEFAULT_UPSTREAM_BODY)
                request_trace_id = headers.get("x-relay-request-trace-id")
                self.assertTrue(request_trace_id)

                trace_status, _, trace_payload = make_request(
                    relay_port,
                    "GET",
                    f"/_relay/request-traces?request_trace_id={request_trace_id}",
                    headers={"X-Relay-Admin-Key": "relay-admin"},
                )
                self.assertEqual(trace_status, 200)
                trace = json.loads(trace_payload)

                self.assertEqual(trace["request_trace_id"], request_trace_id)
                self.assertEqual(trace["configured_order"], ["primary", "backup"])
                self.assertEqual(trace["effective_order"], ["primary", "backup"])
                self.assertEqual(trace["terminal_outcome"], "fallback_success")
                self.assertTrue(trace["fallback_triggered"])
                self.assertEqual(trace["planned_attempt_count"], 2)
                self.assertEqual(trace["attempt_count"], 2)
                self.assertTrue(trace["traversed_all_planned_attempts"])
                self.assertEqual(trace["terminal_attempt_index"], 1)
                self.assertEqual(trace["terminal_upstream_id"], "backup")
                self.assertEqual(
                    [
                        (
                            item["pool_index"],
                            item["upstream_id"],
                            item["state"],
                            item.get("skip_reason"),
                            item.get("attempt_index"),
                        )
                        for item in trace["order_pool"]
                    ],
                    [
                        (0, "primary", "attempted", None, 0),
                        (1, "backup", "attempted", None, 1),
                    ],
                )
                self.assertEqual(
                    [
                        (
                            item["attempt_index"],
                            item["upstream_id"],
                            item["status_code"],
                            item["error_kind"],
                            item["terminal_outcome"],
                        )
                        for item in trace["attempts"]
                    ],
                    [
                        (0, "primary", 404, "upstream_status_fallback", "fallback_success"),
                        (1, "backup", 201, None, "fallback_success"),
                    ],
                )

    def test_request_trace_endpoint_marks_skipped_upstreams(self) -> None:
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
                    "status": 201,
                    "headers": dict(DEFAULT_UPSTREAM_HEADERS),
                    "body": DEFAULT_UPSTREAM_BODY,
                }
            )

            with RelayServer(config_path) as relay_server:
                breaker = relay_server.runtime_manager.active_runtime().circuit_breaker
                for _ in range(3):
                    breaker.record_failure(
                        "primary",
                        was_half_open=False,
                        error_kind="upstream_request_failed",
                        reason="forced-open-for-test",
                    )

                status, headers, response_body = make_request(
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
                self.assertEqual(response_body, DEFAULT_UPSTREAM_BODY)
                request_trace_id = headers.get("x-relay-request-trace-id")
                self.assertTrue(request_trace_id)

                trace_status, _, trace_payload = make_request(
                    relay_port,
                    "GET",
                    f"/_relay/request-traces?request_trace_id={request_trace_id}",
                    headers={"X-Relay-Admin-Key": "relay-admin"},
                )
                self.assertEqual(trace_status, 200)
                trace = json.loads(trace_payload)

                self.assertEqual(trace["configured_order"], ["primary", "backup"])
                self.assertEqual(trace["effective_order"], ["backup", "primary"])
                self.assertEqual(trace["terminal_outcome"], "success")
                self.assertFalse(trace["fallback_triggered"])
                self.assertEqual(trace["planned_attempt_count"], 1)
                self.assertEqual(trace["attempt_count"], 1)
                self.assertTrue(trace["traversed_all_planned_attempts"])
                self.assertEqual(trace["terminal_attempt_index"], 0)
                self.assertEqual(trace["terminal_upstream_id"], "backup")
                self.assertEqual(
                    [
                        (
                            item["pool_index"],
                            item["upstream_id"],
                            item["state"],
                            item.get("skip_reason"),
                            item.get("attempt_index"),
                        )
                        for item in trace["order_pool"]
                    ],
                    [
                        (0, "backup", "attempted", None, 0),
                        (1, "primary", "skipped", "breaker_open", None),
                    ],
                )
                self.assertEqual(
                    [
                        (
                            item["attempt_index"],
                            item["upstream_id"],
                            item["status_code"],
                            item["error_kind"],
                            item["terminal_outcome"],
                        )
                        for item in trace["attempts"]
                    ],
                    [
                        (0, "backup", 201, None, "success"),
                    ],
                )

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
                    "status": 404,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Upstream-Trace": "primary-no-model",
                    },
                    "body": b'{"error":{"message":"No such model: gpt-test"}}',
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
                self.assertEqual(upstream_stats["primary"]["status_codes"], {"404": 1})
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

    def test_health_endpoint_exposes_live_upstream_status_after_fallback_failure(self) -> None:
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
                    "status": 429,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Upstream-Trace": "primary-rate-limit-429",
                    },
                    "body": b'{"error":{"message":"rate limit exceeded"}}',
                },
                {
                    "status": 201,
                    "headers": dict(DEFAULT_UPSTREAM_HEADERS),
                    "body": DEFAULT_UPSTREAM_BODY,
                },
            )

            with RelayServer(config_path):
                status, _, response_body = make_request(
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
                self.assertEqual(response_body, DEFAULT_UPSTREAM_BODY)

                health_status, _, health_payload = make_request(
                    relay_port,
                    "GET",
                    "/_relay/health",
                    headers={"X-Relay-Admin-Key": "relay-admin"},
                )
                self.assertEqual(health_status, 200)
                health = json.loads(health_payload)
                self.assertIn("upstream_status", health)
                self.assertEqual(health["upstream_status"]["configured_order"], ["primary", "backup"])
                self.assertEqual(health["upstream_status"]["effective_order"], ["backup", "primary"])
                upstreams = {item["upstream_id"]: item for item in health["upstream_status"]["upstreams"]}
                self.assertEqual(upstreams["backup"]["effective_index"], 0)
                self.assertEqual(upstreams["primary"]["effective_index"], 1)
                self.assertEqual(upstreams["primary"]["routing_state"], "degraded")
                self.assertEqual(upstreams["primary"]["priority_penalty"], 1)
                self.assertEqual(
                    upstreams["primary"]["last_failure"],
                    {
                        "at": upstreams["primary"]["last_failure"]["at"],
                        "error_kind": "upstream_status",
                        "status_code": 429,
                        "reason": "rate limit exceeded",
                    },
                )
                self.assertIsNotNone(upstreams["primary"]["recovery_at"])

    def test_non_routable_400_still_falls_back_to_backup(self) -> None:
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
                        "X-Upstream-Trace": "primary-bad-request",
                    },
                    "body": b'{"error":{"message":"bad request"}}',
                },
                {
                    "status": 201,
                    "headers": dict(DEFAULT_UPSTREAM_HEADERS),
                    "body": DEFAULT_UPSTREAM_BODY,
                },
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
                self.assertEqual(status, 201)
                self.assertEqual(headers.get("x-upstream-trace"), "stub-1")
                self.assertEqual(payload, DEFAULT_UPSTREAM_BODY)
                self.assertEqual(len(RecordingUpstreamHandler.requests), 2)
                self.assertEqual(RecordingUpstreamHandler.requests[0]["path"], "/primary/v1/chat/completions")
                self.assertEqual(RecordingUpstreamHandler.requests[1]["path"], "/backup/v1/chat/completions")

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

    def test_upstream_quota_403_rate_limit_429_and_model_404_fallback_to_backup(self) -> None:
        scenarios = [
            (
                "quota_403",
                {
                    "status": 403,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Upstream-Trace": "primary-quota-403",
                    },
                    "body": b'{"error":{"message":"403 \\u60a8\\u7684\\u5957\\u9910\\u5df2\\u7ecf\\u5230\\u671f\\u6216\\u8005\\u989d\\u5ea6\\u7528\\u5b8c"}}',
                },
            ),
            (
                "rate_limit_429",
                {
                    "status": 429,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Upstream-Trace": "primary-rate-limit-429",
                    },
                    "body": b'{"error":{"message":"rate limit exceeded"}}',
                },
            ),
            (
                "model_404",
                {
                    "status": 404,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Upstream-Trace": "primary-no-model",
                    },
                    "body": b'{"error":{"message":"No such model: gpt-test"}}',
                },
            ),
        ]

        for scenario_name, first_response in scenarios:
            with self.subTest(scenario=scenario_name):
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
                        first_response,
                        {
                            "status": 201,
                            "headers": dict(DEFAULT_UPSTREAM_HEADERS),
                            "body": DEFAULT_UPSTREAM_BODY,
                        },
                    )

                    with RelayServer(config_path):
                        status, headers, response_body = make_request(
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
                        self.assertEqual(response_body, DEFAULT_UPSTREAM_BODY)
                        self.assertEqual(len(RecordingUpstreamHandler.requests), 2)
                        self.assertEqual(RecordingUpstreamHandler.requests[0]["path"], "/primary/v1/chat/completions")
                        self.assertEqual(RecordingUpstreamHandler.requests[1]["path"], "/backup/v1/chat/completions")

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
                        self.assertEqual(upstream_stats["backup"]["status_codes"], {"201": 1})
                        self.assertEqual(upstream_stats["primary"]["failures"], 1)

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
                self.assertEqual(status, 502)
                self.assertEqual(headers.get("content-type"), "application/json; charset=utf-8")
                exhausted = json.loads(payload)
                self.assertEqual(exhausted["error"]["type"], "upstream_fallback_exhausted")
                self.assertEqual(exhausted["error"]["message"], "all upstream attempts failed")
                self.assertEqual(
                    [item["upstream_id"] for item in exhausted["error"]["attempts"]],
                    ["primary", "backup"],
                )
                self.assertEqual(exhausted["error"]["attempts"][0]["status_code"], 404)
                self.assertEqual(exhausted["error"]["attempts"][0]["message"], "No such model: gpt-test")
                self.assertEqual(exhausted["error"]["attempts"][1]["error_kind"], "upstream_request_failed")
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


class RelayResponsesCompatibilityTests(unittest.TestCase):
    def test_responses_request_to_chat_completions_body(self) -> None:
        translated = relay._responses_request_to_chat_completions_body(
            json.dumps(
                {
                    "model": "deepseek-v4-pro",
                    "instructions": "be brief",
                    "input": [
                        {
                            "role": "user",
                            "content": [
                                {"type": "input_text", "text": "hello"},
                                {"type": "input_text", "text": "world"},
                            ],
                        }
                    ],
                    "max_output_tokens": 32,
                    "stream": True,
                    "temperature": 0.2,
                }
            ).encode("utf-8")
        )
        self.assertIsNotNone(translated)
        payload = json.loads(translated or b"{}")
        self.assertEqual(payload["model"], "deepseek-v4-pro")
        self.assertEqual(payload["messages"][0], {"role": "system", "content": "be brief"})
        self.assertEqual(payload["messages"][1], {"role": "user", "content": "hello\nworld"})
        self.assertEqual(payload["max_tokens"], 32)
        self.assertTrue(payload["stream"])
        self.assertEqual(payload["temperature"], 0.2)

    def test_chat_completions_request_to_responses_body(self) -> None:
        translated = relay._chat_completions_request_to_responses_body(
            json.dumps(
                {
                    "model": "deepseek-v4-pro",
                    "messages": [
                        {"role": "system", "content": "be brief"},
                        {
                            "role": "user",
                            "content": [
                                {"type": "text", "text": "hello"},
                                {"type": "text", "text": "world"},
                            ],
                        },
                    ],
                    "max_tokens": 24,
                    "stream": True,
                    "temperature": 0.3,
                }
            ).encode("utf-8")
        )
        self.assertIsNotNone(translated)
        payload = json.loads(translated or b"{}")
        self.assertEqual(payload["model"], "deepseek-v4-pro")
        self.assertEqual(payload["instructions"], "be brief")
        self.assertEqual(
            payload["input"],
            [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "hello"},
                        {"type": "input_text", "text": "world"},
                    ],
                }
            ],
        )
        self.assertEqual(payload["max_output_tokens"], 24)
        self.assertTrue(payload["stream"])
        self.assertEqual(payload["temperature"], 0.3)

    def test_chat_completions_stream_translates_to_responses_events(self) -> None:
        state = relay._new_responses_compat_stream_state("trace_123")
        first_events = relay._chat_completions_sse_to_responses_events(
            'data: {"id":"chatcmpl_1","model":"deepseek-v4-pro","choices":[{"delta":{"content":"pong"},"finish_reason":null}]}',
            state,
        )
        final_events = relay._finalize_responses_compat_events(state)
        encoded = b"".join(relay._responses_sse_event_bytes(event) for event in [*first_events, *final_events])
        text = encoded.decode("utf-8")
        self.assertIn('"type":"response.output_text.delta"', text)
        self.assertIn('"delta":"pong"', text)
        self.assertIn('"type":"response.completed"', text)
        self.assertIn('"output_text":"pong"', text)
        self.assertTrue(text.rstrip().endswith("data: [DONE]"))

    def test_responses_stream_translates_to_chat_completions_events(self) -> None:
        state = relay._new_chat_completions_compat_stream_state("trace_456")
        first_events = relay._responses_sse_to_chat_completions_events(
            'data: {"type":"response.output_text.delta","delta":"pong"}',
            state,
        )
        final_events = relay._responses_sse_to_chat_completions_events(
            'data: {"type":"response.completed","response":{"id":"resp_1","model":"deepseek-v4-pro","output_text":"pong","usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}}',
            state,
        )
        encoded = b"".join(relay._chat_completions_sse_event_bytes(event) for event in [*first_events, *final_events])
        text = encoded.decode("utf-8")
        self.assertIn('"object":"chat.completion.chunk"', text)
        self.assertIn('"content":"pong"', text)
        self.assertIn('"finish_reason":"stop"', text)
        self.assertTrue(text.rstrip().endswith("data: [DONE]"))

    def test_proxy_retries_responses_on_same_upstream_with_chat_completions_compat(self) -> None:
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
            config_path = Path(tmp_dir) / "relay-config.json"
            config_path.write_text(json.dumps(payload), encoding="utf-8")
            RecordingUpstreamHandler.queue_responses(
                {
                    "status": 404,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Upstream-Trace": "primary-no-responses",
                    },
                    "body": b'{"error":{"message":"responses endpoint not supported; use /chat/completions"}}',
                },
                {
                    "status": 200,
                    "headers": {
                        "Content-Type": "text/event-stream",
                        "X-Upstream-Trace": "primary-chat-compat",
                    },
                    "body": (
                        b'data: {"id":"chatcmpl_1","object":"chat.completion.chunk","model":"deepseek-v4-pro","choices":[{"index":0,"delta":{"content":"pong"},"finish_reason":null}]}'
                        b'\n\n'
                        b'data: {"id":"chatcmpl_1","object":"chat.completion.chunk","model":"deepseek-v4-pro","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}'
                        b'\n\n'
                        b'data: [DONE]\n\n'
                    ),
                },
            )

            with RelayServer(config_path):
                status, headers, response_body = make_request(
                    relay_port,
                    "POST",
                    "/v1/responses",
                    headers={
                        "Authorization": "Bearer local-chat-key",
                        "Content-Type": "application/json",
                    },
                    body=json.dumps(
                        {
                            "model": "deepseek-v4-pro",
                            "input": "Reply with: pong",
                            "max_output_tokens": 16,
                            "stream": True,
                        }
                    ).encode("utf-8"),
                )

        self.assertEqual(status, 200)
        self.assertIn("text/event-stream", headers.get("content-type", ""))
        text = response_body.decode("utf-8")
        self.assertIn('"type":"response.output_text.delta"', text)
        self.assertIn('"delta":"pong"', text)
        self.assertIn('"type":"response.completed"', text)
        self.assertEqual(len(RecordingUpstreamHandler.requests), 2)
        self.assertEqual(RecordingUpstreamHandler.requests[0]["path"], "/primary/v1/responses")
        self.assertEqual(RecordingUpstreamHandler.requests[1]["path"], "/primary/v1/chat/completions")
        retry_payload = json.loads(RecordingUpstreamHandler.requests[1]["body"])
        self.assertEqual(retry_payload["model"], "deepseek-v4-pro")
        self.assertEqual(retry_payload["messages"], [{"role": "user", "content": "Reply with: pong"}])
        self.assertEqual(retry_payload["max_tokens"], 16)
        self.assertTrue(retry_payload["stream"])

    def test_proxy_retries_chat_completions_on_same_upstream_with_responses_compat(self) -> None:
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
            config_path = Path(tmp_dir) / "relay-config.json"
            config_path.write_text(json.dumps(payload), encoding="utf-8")
            RecordingUpstreamHandler.queue_responses(
                {
                    "status": 404,
                    "headers": {
                        "Content-Type": "application/json",
                        "X-Upstream-Trace": "primary-no-chat",
                    },
                    "body": b'{"error":{"message":"chat/completions endpoint not supported; use /responses"}}',
                },
                {
                    "status": 200,
                    "headers": {
                        "Content-Type": "text/event-stream",
                        "X-Upstream-Trace": "primary-responses-compat",
                    },
                    "body": (
                        b'data: {"type":"response.output_text.delta","delta":"pong"}\n\n'
                        b'data: {"type":"response.completed","response":{"id":"resp_1","model":"deepseek-v4-pro","output_text":"pong","usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}}\n\n'
                        b'data: [DONE]\n\n'
                    ),
                },
            )

            with RelayServer(config_path):
                status, headers, response_body = make_request(
                    relay_port,
                    "POST",
                    "/v1/chat/completions",
                    headers={
                        "Authorization": "Bearer local-chat-key",
                        "Content-Type": "application/json",
                    },
                    body=json.dumps(
                        {
                            "model": "deepseek-v4-pro",
                            "messages": [{"role": "user", "content": "Reply with: pong"}],
                            "max_tokens": 16,
                            "stream": True,
                        }
                    ).encode("utf-8"),
                )

        self.assertEqual(status, 200)
        self.assertIn("text/event-stream", headers.get("content-type", ""))
        text = response_body.decode("utf-8")
        self.assertIn('"object":"chat.completion.chunk"', text)
        self.assertIn('"content":"pong"', text)
        self.assertIn('"finish_reason":"stop"', text)
        self.assertEqual(len(RecordingUpstreamHandler.requests), 2)
        self.assertEqual(RecordingUpstreamHandler.requests[0]["path"], "/primary/v1/chat/completions")
        self.assertEqual(RecordingUpstreamHandler.requests[1]["path"], "/primary/v1/responses")
        retry_payload = json.loads(RecordingUpstreamHandler.requests[1]["body"])
        self.assertEqual(retry_payload["model"], "deepseek-v4-pro")
        self.assertEqual(
            retry_payload["input"],
            [{"role": "user", "content": [{"type": "input_text", "text": "Reply with: pong"}]}],
        )
        self.assertEqual(retry_payload["max_output_tokens"], 16)
        self.assertTrue(retry_payload["stream"])


class RelayDefaultModelTests(unittest.TestCase):
    def test_resolve_upstream_model_no_default_model_returns_unchanged(self) -> None:
        upstream = relay.UpstreamConfig(
            upstream_id="test-upstream",
            base_url="https://example.com/v1",
            api_key="sk-test",
            enabled=True,
            transport={},
            default_model=None,
        )
        body = b'{"model":"default","messages":[]}'
        result = relay._resolve_upstream_model(body, upstream)
        self.assertEqual(result, body)

    def test_resolve_upstream_model_substitutes_default_when_configured(self) -> None:
        upstream = relay.UpstreamConfig(
            upstream_id="test-upstream",
            base_url="https://example.com/v1",
            api_key="sk-test",
            enabled=True,
            transport={},
            default_model="gpt-5.4",
        )
        body = b'{"model":"default","messages":[{"role":"user","content":"hi"}]}'
        result = relay._resolve_upstream_model(body, upstream)
        payload = json.loads(result)
        self.assertEqual(payload["model"], "gpt-5.4")
        self.assertEqual(payload["messages"], [{"role": "user", "content": "hi"}])

    def test_resolve_upstream_model_does_not_substitute_non_default_model(self) -> None:
        upstream = relay.UpstreamConfig(
            upstream_id="test-upstream",
            base_url="https://example.com/v1",
            api_key="sk-test",
            enabled=True,
            transport={},
            default_model="gpt-5.4",
        )
        body = b'{"model":"deepseek-v4-pro","messages":[]}'
        result = relay._resolve_upstream_model(body, upstream)
        payload = json.loads(result)
        self.assertEqual(payload["model"], "deepseek-v4-pro")

    def test_resolve_upstream_model_substitutes_default_reasoning_effort_when_configured(self) -> None:
        upstream = relay.UpstreamConfig(
            upstream_id="test-upstream",
            base_url="https://example.com/v1",
            api_key="sk-test",
            enabled=True,
            transport={},
            default_model="gpt-5.4",
            default_reasoning_effort="high",
        )
        body = b'{"model":"default","input":"Reply with: pong","reasoning":{"effort":"default"},"max_output_tokens":16}'
        result = relay._resolve_upstream_model(body, upstream)
        payload = json.loads(result)
        self.assertEqual(payload["model"], "gpt-5.4")
        self.assertEqual(payload["reasoning"], {"effort": "high"})

    def test_resolve_upstream_model_leaves_non_default_reasoning_effort_unchanged(self) -> None:
        upstream = relay.UpstreamConfig(
            upstream_id="test-upstream",
            base_url="https://example.com/v1",
            api_key="sk-test",
            enabled=True,
            transport={},
            default_model="gpt-5.4",
            default_reasoning_effort="high",
        )
        body = b'{"model":"gpt-5.4","input":"Reply with: pong","reasoning":{"effort":"low"},"max_output_tokens":16}'
        result = relay._resolve_upstream_model(body, upstream)
        payload = json.loads(result)
        self.assertEqual(payload["model"], "gpt-5.4")
        self.assertEqual(payload["reasoning"], {"effort": "low"})

    def test_resolve_upstream_model_handles_missing_model_field(self) -> None:
        upstream = relay.UpstreamConfig(
            upstream_id="test-upstream",
            base_url="https://example.com/v1",
            api_key="sk-test",
            enabled=True,
            transport={},
            default_model="gpt-5.4",
        )
        body = b'{"messages":[]}'
        result = relay._resolve_upstream_model(body, upstream)
        self.assertEqual(result, body)

    def test_resolve_upstream_model_handles_invalid_json(self) -> None:
        upstream = relay.UpstreamConfig(
            upstream_id="test-upstream",
            base_url="https://example.com/v1",
            api_key="sk-test",
            enabled=True,
            transport={},
            default_model="gpt-5.4",
        )
        body = b'not-valid-json'
        result = relay._resolve_upstream_model(body, upstream)
        self.assertEqual(result, body)

    def test_config_parses_default_model_from_upstream(self) -> None:
        config_json = json.dumps({
            "server": {},
            "local": {"clients": []},
            "upstreams": {
                "provider-a": {
                    "base_url": "https://a.example.com/v1",
                    "api_key": "sk-a",
                    "default_model": "deepseek-v4-pro",
                    "default_reasoning_effort": "high",
                },
                "provider-b": {
                    "base_url": "https://b.example.com/v1",
                    "api_key": "sk-b",
                },
            },
            "order": ["provider-a", "provider-b"],
        })
        config_path = Path(tempfile.mkdtemp()) / "relay-config.json"
        config_path.write_text(config_json)
        try:
            config = relay.RelayConfig.load(str(config_path))
            upstream_a = config.upstreams_by_id["provider-a"]
            upstream_b = config.upstreams_by_id["provider-b"]
            self.assertEqual(upstream_a.default_model, "deepseek-v4-pro")
            self.assertEqual(upstream_a.default_reasoning_effort, "high")
            self.assertIsNone(upstream_b.default_model)
            self.assertIsNone(upstream_b.default_reasoning_effort)
        finally:
            config_path.unlink(missing_ok=True)

    def test_config_merges_upstream_defaults_recursively(self) -> None:
        config_json = json.dumps({
            "server": {},
            "local": {"clients": []},
            "upstream_defaults": {
                "enabled": True,
                "default_reasoning_effort": "xhigh",
                "transport": {
                    "timeout_seconds": 90,
                },
            },
            "upstreams": {
                "provider-a": {
                    "base_url": "https://a.example.com/v1",
                    "api_key": "sk-a",
                    "default_model": "gpt-5.4",
                },
                "provider-b": {
                    "base_url": "https://b.example.com/v1",
                    "api_key": "sk-b",
                    "enabled": False,
                    "transport": {
                        "timeout_seconds": 30,
                    },
                },
            },
            "order": ["provider-a", "provider-b"],
        })
        config_path = Path(tempfile.mkdtemp()) / "relay-config.json"
        config_path.write_text(config_json)
        try:
            config = relay.RelayConfig.load(str(config_path))
            upstream_a = config.upstreams_by_id["provider-a"]
            upstream_b = config.upstreams_by_id["provider-b"]
            self.assertTrue(upstream_a.enabled)
            self.assertEqual(upstream_a.default_model, "gpt-5.4")
            self.assertEqual(upstream_a.default_reasoning_effort, "xhigh")
            self.assertEqual(upstream_a.transport.get("timeout_seconds"), 90)
            self.assertFalse(upstream_b.enabled)
            self.assertEqual(upstream_b.default_reasoning_effort, "xhigh")
            self.assertEqual(upstream_b.transport.get("timeout_seconds"), 30)
        finally:
            config_path.unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()

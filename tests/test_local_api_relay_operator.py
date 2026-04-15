from __future__ import annotations

import contextlib
import json
import socket
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
from pathlib import Path

import local_api_relay as relay


def find_free_port() -> int:
    try:
        with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.bind(("127.0.0.1", 0))
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return int(sock.getsockname()[1])
    except PermissionError as exc:
        raise unittest.SkipTest("loopback sockets unavailable in this environment") from exc


def write_config(
    config_path: Path,
    *,
    upstreams: dict[str, dict[str, object]],
    order: list[str],
    pricing: dict[str, object] | None = None,
) -> Path:
    payload = {
        "server": {
            "listen_host": "127.0.0.1",
            "listen_port": find_free_port(),
            "admin_key": "relay-admin",
            "database_path": str(config_path.parent / "relay.sqlite3"),
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
        "upstreams": upstreams,
        "order": order,
    }
    if pricing is not None:
        payload["pricing"] = pricing
    config_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return config_path


class ProbeHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = b'{"data":[]}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


class CreditsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/primary/api/user/self":
            body = json.dumps(
                {
                    "success": True,
                    "data": {
                        "balance": 5.5,
                        "quota": 10.0,
                        "used_quota": 4.5,
                        "remain_quota": 5.5,
                        "currency": "USD",
                    },
                }
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        return


class RealRequestHandler(BaseHTTPRequestHandler):
    requests: list[tuple[str, str, bytes]] = []

    @classmethod
    def reset(cls) -> None:
        cls.requests = []

    def do_GET(self) -> None:
        self.__class__.requests.append((self.command, self.path, b""))
        if self.path == "/primary/v1/models":
            body = json.dumps({"data": [{"id": "gpt-5-mini"}]}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length)
        self.__class__.requests.append((self.command, self.path, body))
        if self.path == "/primary/v1/responses":
            payload = json.dumps(
                {
                    "id": "resp_123",
                    "output_text": "pong",
                    "usage": {
                        "input_tokens": 2,
                        "output_tokens": 3,
                        "total_tokens": 5,
                    },
                }
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        return


class HttpServerHarness:
    def __init__(self, handler: type[BaseHTTPRequestHandler]) -> None:
        self.port = find_free_port()
        try:
            self.server = ThreadingHTTPServer(("127.0.0.1", self.port), handler)
        except PermissionError as exc:
            raise unittest.SkipTest("loopback sockets unavailable in this environment") from exc
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> "HttpServerHarness":
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class RelayOperatorTests(unittest.TestCase):
    def test_stats_summary_includes_estimated_cost_usage_policy_and_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            config_path = write_config(
                Path(tmp_dir) / "relay-config.json",
                upstreams={
                    "primary": {
                        "base_url": "https://primary.example.com/v1",
                        "api_key": "primary-key",
                        "enabled": True,
                        "transport": {"timeout_seconds": 120},
                    },
                    "backup": {
                        "base_url": "https://backup.example.com/v1",
                        "api_key": "backup-key",
                        "enabled": True,
                        "transport": {"timeout_seconds": 120},
                    },
                },
                order=["primary", "backup"],
                pricing={
                    "currency": "USD",
                    "upstreams": {
                        "primary": {
                            "input_per_million_tokens": 1.0,
                            "output_per_million_tokens": 2.0,
                            "models": {
                                "gpt-test": {
                                    "input_per_million_tokens": 3.0,
                                    "output_per_million_tokens": 4.0,
                                }
                            },
                        }
                    },
                },
            )
            config = relay.RelayConfig.load(config_path)
            store = relay.UsageStore(Path(tmp_dir) / "relay.sqlite3")
            try:
                started_at = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
                store.record_request(
                    started_at=started_at,
                    finished_at=started_at,
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
                store.record_request(
                    started_at=started_at,
                    finished_at=started_at,
                    client_id="chat-client",
                    upstream_id="primary",
                    requested_model="gpt-lite",
                    method="POST",
                    path="/v1/chat/completions",
                    status_code=200,
                    forwarded=True,
                    request_bytes=120,
                    response_bytes=240,
                    upstream_ms=16,
                    error_kind=None,
                    prompt_tokens=2000,
                    completion_tokens=1000,
                    total_tokens=3000,
                )
                store.record_request(
                    started_at=started_at,
                    finished_at=started_at,
                    client_id="chat-client",
                    upstream_id="backup",
                    requested_model="gpt-test",
                    method="POST",
                    path="/v1/chat/completions",
                    status_code=200,
                    forwarded=True,
                    request_bytes=120,
                    response_bytes=240,
                    upstream_ms=17,
                    error_kind=None,
                    prompt_tokens=800,
                    completion_tokens=400,
                    total_tokens=1200,
                )
                store.record_request(
                    started_at=started_at,
                    finished_at=started_at,
                    client_id="chat-client",
                    upstream_id="primary",
                    requested_model="gpt-test",
                    method="POST",
                    path="/v1/chat/completions",
                    status_code=200,
                    forwarded=True,
                    request_bytes=120,
                    response_bytes=240,
                    upstream_ms=18,
                    error_kind=None,
                    prompt_tokens=None,
                    completion_tokens=None,
                    total_tokens=200,
                )

                stats = store.stats_summary(config=config)
            finally:
                store.close()

        self.assertEqual(
            stats["totals"]["usage"]["estimated_cost"],
            {
                "currency": "USD",
                "estimated_requests": 2,
                "input": 0.005,
                "output": 0.004,
                "total": 0.009,
            },
        )
        self.assertEqual(
            stats["usage_coverage"],
            {
                "window_requests": 4,
                "prompt_tokens": {
                    "recorded_requests": 3,
                    "missing_requests": 1,
                },
                "completion_tokens": {
                    "recorded_requests": 3,
                    "missing_requests": 1,
                },
                "total_tokens": {
                    "recorded_requests": 4,
                    "missing_requests": 0,
                },
                "estimated_cost": {
                    "estimated_requests": 2,
                    "missing_requests": 2,
                    "missing_breakdown": {
                        "missing_pricing_config": 1,
                        "missing_pricing_rule": 0,
                        "missing_prompt_and_completion_tokens": 1,
                    },
                },
            },
        )
        self.assertEqual(stats["usage_policy"]["estimated_cost"]["estimated"], True)
        upstreams = {item["upstream_id"]: item for item in stats["upstreams"]}
        self.assertEqual(
            upstreams["primary"]["usage"]["estimated_cost"],
            {
                "currency": "USD",
                "estimated_requests": 2,
                "input": 0.005,
                "output": 0.004,
                "total": 0.009,
            },
        )
        self.assertEqual(
            upstreams["backup"]["usage"]["estimated_cost"],
            {
                "currency": "USD",
                "estimated_requests": 0,
                "input": 0.0,
                "output": 0.0,
                "total": 0.0,
            },
        )
        models = {item["model"]: item for item in stats["models"]}
        self.assertEqual(
            models["gpt-test"]["usage"]["estimated_cost"],
            {
                "currency": "USD",
                "estimated_requests": 1,
                "input": 0.003,
                "output": 0.002,
                "total": 0.005,
            },
        )

    def test_cli_probe_outputs_json_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir, HttpServerHarness(ProbeHandler) as server:
            config_path = write_config(
                Path(tmp_dir) / "relay-config.json",
                upstreams={
                    "primary": {
                        "base_url": f"http://127.0.0.1:{server.port}/primary/v1",
                        "api_key": "primary-key",
                        "enabled": True,
                        "transport": {"timeout_seconds": 7},
                    }
                },
                order=["primary"],
            )
            stdout = StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = relay.main(["probe", "--config", str(config_path)])

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["summary"], {"total": 1, "successes": 1, "failures": 0})
        self.assertEqual(payload["upstreams"][0]["upstream_id"], "primary")
        self.assertTrue(payload["upstreams"][0]["success"])
        self.assertEqual(payload["upstreams"][0]["status_code"], 200)

    def test_cli_credits_outputs_json_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir, HttpServerHarness(CreditsHandler) as server:
            config_path = write_config(
                Path(tmp_dir) / "relay-config.json",
                upstreams={
                    "primary": {
                        "base_url": f"http://127.0.0.1:{server.port}/primary/v1",
                        "api_key": "primary-key",
                        "enabled": True,
                        "transport": {"timeout_seconds": 7},
                    },
                    "unsupported": {
                        "base_url": f"http://127.0.0.1:{server.port}/unsupported/v1",
                        "api_key": "unsupported-key",
                        "enabled": True,
                        "transport": {"timeout_seconds": 7},
                    },
                },
                order=["primary", "unsupported"],
            )
            stdout = StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = relay.main(["credits", "--config", str(config_path)])

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(
            payload["summary"],
            {
                "total": 2,
                "supported": 1,
                "unsupported": 1,
                "failures": 0,
                "disabled": 0,
            },
        )
        self.assertEqual(payload["upstreams"][0]["upstream_id"], "primary")
        self.assertEqual(payload["upstreams"][0]["status"], "supported")
        self.assertEqual(payload["upstreams"][0]["credits"]["remaining"], 5.5)
        self.assertEqual(payload["upstreams"][1]["status"], "unsupported")

    def test_cli_test_outputs_json_summary(self) -> None:
        RealRequestHandler.reset()
        with tempfile.TemporaryDirectory() as tmp_dir, HttpServerHarness(RealRequestHandler) as server:
            config_path = write_config(
                Path(tmp_dir) / "relay-config.json",
                upstreams={
                    "primary": {
                        "base_url": f"http://127.0.0.1:{server.port}/primary/v1",
                        "api_key": "primary-key",
                        "enabled": True,
                        "transport": {"timeout_seconds": 7},
                    }
                },
                order=["primary"],
            )
            stdout = StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = relay.main(["test", "--config", str(config_path)])

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(
            payload["summary"],
            {
                "total": 1,
                "models_successes": 1,
                "request_successes": 1,
                "failures": 0,
            },
        )
        self.assertEqual(payload["upstreams"][0]["models"]["selected_model"], "gpt-5-mini")
        self.assertEqual(payload["upstreams"][0]["request"]["mode"], "responses")
        self.assertEqual(payload["upstreams"][0]["request"]["reply_preview"], "pong")
        self.assertEqual(
            payload["upstreams"][0]["request"]["usage"],
            {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5},
        )
        self.assertEqual(
            RealRequestHandler.requests,
            [
                ("GET", "/primary/v1/models", b""),
                ("POST", "/primary/v1/responses", b'{"model": "gpt-5-mini", "input": "Reply with: pong", "max_output_tokens": 16}'),
            ],
        )

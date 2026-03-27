from __future__ import annotations

import contextlib
import http.client
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = ROOT / "local_api_relay.py"


def find_free_port() -> int:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return int(sock.getsockname()[1])


def make_request(
    port: int,
    method: str,
    path: str,
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


class RecordingUpstreamHandler(BaseHTTPRequestHandler):
    requests: list[dict[str, object]] = []
    response_status = 201
    response_headers = {
        "Content-Type": "application/json",
        "X-Upstream-Trace": "stub-1",
    }
    response_body = json.dumps(
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

    def do_POST(self) -> None:
        self._record_and_reply()

    def do_GET(self) -> None:
        self._record_and_reply()

    def log_message(self, format: str, *args: object) -> None:
        return

    @classmethod
    def reset(cls) -> None:
        cls.requests = []

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
        self.send_response(self.__class__.response_status)
        for key, value in self.__class__.response_headers.items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(self.__class__.response_body)))
        self.end_headers()
        self.wfile.write(self.__class__.response_body)


class UpstreamServer:
    def __init__(self) -> None:
        RecordingUpstreamHandler.reset()
        self.port = find_free_port()
        self.server = ThreadingHTTPServer(("127.0.0.1", self.port), RecordingUpstreamHandler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self) -> "UpstreamServer":
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class RelayProcess:
    def __init__(self, config_path: Path, admin_key: str) -> None:
        self.config_path = config_path
        self.admin_key = admin_key
        self.process: subprocess.Popen[str] | None = None
        self.port = json.loads(config_path.read_text(encoding="utf-8"))["listen_port"]

    def __enter__(self) -> "RelayProcess":
        self.process = subprocess.Popen(
            [sys.executable, "-u", str(SCRIPT_PATH), "--config", str(self.config_path)],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        )
        self._wait_until_ready()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.process is None:
            return
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.process.stdout is not None:
            self.process.stdout.close()
        if self.process.stderr is not None:
            self.process.stderr.close()

    def _wait_until_ready(self) -> None:
        deadline = time.time() + 5
        while time.time() < deadline:
            if self.process is None:
                raise AssertionError("relay process was not started")
            if self.process.poll() is not None:
                _, stderr = self.process.communicate(timeout=1)
                raise AssertionError(f"relay exited early: {stderr.strip()}")
            try:
                status, _, _ = make_request(
                    self.port,
                    "GET",
                    "/_relay/health",
                    headers={"X-Relay-Admin-Key": self.admin_key},
                )
            except OSError:
                time.sleep(0.1)
                continue
            if status == 200:
                return
            time.sleep(0.1)
        raise AssertionError("relay did not become ready in time")


class LocalApiRelayTests(unittest.TestCase):
    def write_config(self, tmp_dir: str, upstream_port: int, relay_port: int) -> Path:
        config_path = Path(tmp_dir) / "relay-config.json"
        config_path.write_text(
            json.dumps(
                {
                    "listen_host": "127.0.0.1",
                    "listen_port": relay_port,
                    "admin_key": "relay-admin",
                    "database_path": str(Path(tmp_dir) / "relay.sqlite3"),
                    "idle_window_seconds": 300,
                    "apps": [
                        {
                            "app_id": "chat-app",
                            "local_key": "local-chat-key",
                            "upstream_base_url": f"http://127.0.0.1:{upstream_port}",
                            "upstream_api_key": "upstream-real-key",
                        }
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        return config_path

    def test_proxy_rewrites_local_key_and_records_usage(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir, UpstreamServer() as upstream:
            relay_port = find_free_port()
            config_path = self.write_config(tmp_dir, upstream.port, relay_port)
            request_body = json.dumps(
                {"model": "gpt-test", "messages": [{"role": "user", "content": "ping"}]}
            ).encode("utf-8")

            with RelayProcess(config_path, admin_key="relay-admin"):
                status, headers, payload = make_request(
                    relay_port,
                    "POST",
                    "/v1/chat/completions?stream=false",
                    headers={
                        "Authorization": "Bearer local-chat-key",
                        "Content-Type": "application/json",
                    },
                    body=request_body,
                )

                self.assertEqual(status, 201)
                self.assertEqual(headers.get("x-upstream-trace"), "stub-1")
                self.assertEqual(payload, RecordingUpstreamHandler.response_body)
                self.assertEqual(len(RecordingUpstreamHandler.requests), 1)
                upstream_request = RecordingUpstreamHandler.requests[0]
                self.assertEqual(upstream_request["path"], "/v1/chat/completions?stream=false")
                self.assertEqual(upstream_request["body"], request_body)
                self.assertEqual(
                    upstream_request["headers"]["authorization"],
                    "Bearer upstream-real-key",
                )

                stats_status, _, stats_payload = make_request(
                    relay_port,
                    "GET",
                    "/_relay/stats",
                    headers={"X-Relay-Admin-Key": "relay-admin"},
                )
                self.assertEqual(stats_status, 200)
                stats = json.loads(stats_payload)
                self.assertEqual(stats["totals"]["requests"], 1)
                self.assertEqual(stats["totals"]["local_rejects"], 0)
                self.assertEqual(len(stats["apps"]), 1)
                self.assertEqual(stats["apps"][0]["app_id"], "chat-app")
                self.assertEqual(stats["apps"][0]["requests"], 1)
                self.assertEqual(stats["apps"][0]["successes"], 1)
                self.assertEqual(stats["apps"][0]["total_tokens"], 8)

    def test_invalid_local_key_is_rejected_and_counted(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir, UpstreamServer() as upstream:
            relay_port = find_free_port()
            config_path = self.write_config(tmp_dir, upstream.port, relay_port)

            with RelayProcess(config_path, admin_key="relay-admin"):
                status, _, payload = make_request(
                    relay_port,
                    "POST",
                    "/v1/chat/completions",
                    headers={"Authorization": "Bearer wrong-key", "Content-Type": "application/json"},
                    body=b'{"model":"gpt-test"}',
                )

                self.assertEqual(status, 401)
                error_payload = json.loads(payload)
                self.assertEqual(error_payload["error"]["message"], "invalid local api key")
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
                self.assertEqual(stats["apps"], [])

    def test_idle_endpoint_flips_after_successful_forward(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir, UpstreamServer() as upstream:
            relay_port = find_free_port()
            config_path = self.write_config(tmp_dir, upstream.port, relay_port)

            with RelayProcess(config_path, admin_key="relay-admin"):
                idle_status, _, idle_payload = make_request(
                    relay_port,
                    "GET",
                    "/_relay/idle",
                    headers={"X-Relay-Admin-Key": "relay-admin"},
                )
                self.assertEqual(idle_status, 200)
                idle = json.loads(idle_payload)
                self.assertTrue(idle["idle"])
                self.assertEqual(idle["reason"], "no_successful_requests_in_window")

                status, _, _ = make_request(
                    relay_port,
                    "POST",
                    "/v1/chat/completions",
                    headers={
                        "Authorization": "Bearer local-chat-key",
                        "Content-Type": "application/json",
                    },
                    body=b'{"model":"gpt-test"}',
                )
                self.assertEqual(status, 201)

                idle_status, _, idle_payload = make_request(
                    relay_port,
                    "GET",
                    "/_relay/idle",
                    headers={"X-Relay-Admin-Key": "relay-admin"},
                )
                self.assertEqual(idle_status, 200)
                idle = json.loads(idle_payload)
                self.assertFalse(idle["idle"])
                self.assertEqual(idle["reason"], "recent_successful_request")


if __name__ == "__main__":
    unittest.main()

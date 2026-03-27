from __future__ import annotations

import argparse
import http.client
import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import SplitResult, urlsplit, urlunsplit


HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
CAPTURE_BODY_LIMIT = 1_000_000


@dataclass(frozen=True)
class AppConfig:
    app_id: str
    local_key: str
    upstream_base_url: str
    upstream_api_key: str


@dataclass(frozen=True)
class RelayConfig:
    listen_host: str
    listen_port: int
    admin_key: str
    database_path: Path
    idle_window_seconds: int
    request_timeout_seconds: int
    apps: list[AppConfig]

    @property
    def apps_by_key(self) -> dict[str, AppConfig]:
        return {app.local_key: app for app in self.apps}

    @classmethod
    def load(cls, path: str | Path) -> "RelayConfig":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        apps_payload = payload.get("apps") or []
        apps = [
            AppConfig(
                app_id=str(item["app_id"]),
                local_key=str(item["local_key"]),
                upstream_base_url=str(item["upstream_base_url"]),
                upstream_api_key=str(item["upstream_api_key"]),
            )
            for item in apps_payload
        ]
        local_keys = [app.local_key for app in apps]
        if len(local_keys) != len(set(local_keys)):
            raise ValueError("duplicate local_key entries are not allowed")
        database_path = Path(payload.get("database_path") or "state/local_api_relay.sqlite3")
        return cls(
            listen_host=str(payload.get("listen_host") or "127.0.0.1"),
            listen_port=int(payload.get("listen_port") or 8787),
            admin_key=str(payload.get("admin_key") or ""),
            database_path=database_path,
            idle_window_seconds=int(payload.get("idle_window_seconds") or 300),
            request_timeout_seconds=int(payload.get("request_timeout_seconds") or 120),
            apps=apps,
        )


class UsageStore:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(database_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        with self.lock:
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS request_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    app_id TEXT,
                    method TEXT NOT NULL,
                    path TEXT NOT NULL,
                    status_code INTEGER NOT NULL,
                    forwarded INTEGER NOT NULL,
                    request_bytes INTEGER NOT NULL,
                    response_bytes INTEGER NOT NULL,
                    upstream_ms INTEGER NOT NULL,
                    error_kind TEXT,
                    prompt_tokens INTEGER,
                    completion_tokens INTEGER,
                    total_tokens INTEGER
                )
                """
            )
            self.connection.commit()

    def close(self) -> None:
        with self.lock:
            self.connection.close()

    def record_request(
        self,
        *,
        started_at: datetime,
        finished_at: datetime,
        app_id: str | None,
        method: str,
        path: str,
        status_code: int,
        forwarded: bool,
        request_bytes: int,
        response_bytes: int,
        upstream_ms: int,
        error_kind: str | None,
        prompt_tokens: int | None,
        completion_tokens: int | None,
        total_tokens: int | None,
    ) -> None:
        with self.lock:
            self.connection.execute(
                """
                INSERT INTO request_log (
                    started_at,
                    finished_at,
                    app_id,
                    method,
                    path,
                    status_code,
                    forwarded,
                    request_bytes,
                    response_bytes,
                    upstream_ms,
                    error_kind,
                    prompt_tokens,
                    completion_tokens,
                    total_tokens
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _serialize_datetime(started_at),
                    _serialize_datetime(finished_at),
                    app_id,
                    method,
                    path,
                    status_code,
                    1 if forwarded else 0,
                    request_bytes,
                    response_bytes,
                    upstream_ms,
                    error_kind,
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                ),
            )
            self.connection.commit()

    def stats_summary(self) -> dict[str, Any]:
        with self.lock:
            totals_row = self.connection.execute(
                """
                SELECT
                    COUNT(*) AS requests,
                    COALESCE(SUM(CASE WHEN forwarded = 1 THEN 1 ELSE 0 END), 0) AS forwarded,
                    COALESCE(SUM(CASE WHEN status_code BETWEEN 200 AND 299 THEN 1 ELSE 0 END), 0) AS successes,
                    COALESCE(SUM(CASE WHEN error_kind = 'invalid_local_key' THEN 1 ELSE 0 END), 0) AS local_rejects
                FROM request_log
                """
            ).fetchone()
            app_rows = self.connection.execute(
                """
                SELECT
                    app_id,
                    COUNT(*) AS requests,
                    COALESCE(SUM(CASE WHEN forwarded = 1 THEN 1 ELSE 0 END), 0) AS forwarded,
                    COALESCE(SUM(CASE WHEN status_code BETWEEN 200 AND 299 THEN 1 ELSE 0 END), 0) AS successes,
                    COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
                    COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
                    COALESCE(SUM(total_tokens), 0) AS total_tokens,
                    MAX(finished_at) AS last_request_at
                FROM request_log
                WHERE app_id IS NOT NULL
                GROUP BY app_id
                ORDER BY app_id ASC
                """
            ).fetchall()
        return {
            "totals": {
                "requests": int(totals_row["requests"]),
                "forwarded": int(totals_row["forwarded"]),
                "successes": int(totals_row["successes"]),
                "local_rejects": int(totals_row["local_rejects"]),
            },
            "apps": [
                {
                    "app_id": str(row["app_id"]),
                    "requests": int(row["requests"]),
                    "forwarded": int(row["forwarded"]),
                    "successes": int(row["successes"]),
                    "prompt_tokens": int(row["prompt_tokens"]),
                    "completion_tokens": int(row["completion_tokens"]),
                    "total_tokens": int(row["total_tokens"]),
                    "last_request_at": row["last_request_at"],
                }
                for row in app_rows
            ],
        }

    def idle_status(self, window_seconds: int) -> dict[str, Any]:
        cutoff = _serialize_datetime(datetime.fromtimestamp(time.time() - window_seconds, tz=timezone.utc))
        with self.lock:
            row = self.connection.execute(
                """
                SELECT
                    COUNT(*) AS requests,
                    COALESCE(SUM(CASE WHEN forwarded = 1 THEN 1 ELSE 0 END), 0) AS forwarded,
                    COALESCE(SUM(CASE WHEN forwarded = 1 AND status_code BETWEEN 200 AND 299 THEN 1 ELSE 0 END), 0) AS successes,
                    MAX(finished_at) AS last_request_at,
                    MAX(CASE WHEN forwarded = 1 AND status_code BETWEEN 200 AND 299 THEN finished_at END) AS last_success_at
                FROM request_log
                WHERE finished_at >= ?
                """,
                (cutoff,),
            ).fetchone()
        successes = int(row["successes"])
        if successes > 0:
            idle = False
            reason = "recent_successful_request"
        else:
            idle = True
            reason = "no_successful_requests_in_window"
        return {
            "idle": idle,
            "reason": reason,
            "window_seconds": window_seconds,
            "recent": {
                "requests": int(row["requests"]),
                "forwarded": int(row["forwarded"]),
                "successes": successes,
                "last_request_at": row["last_request_at"],
                "last_success_at": row["last_success_at"],
            },
        }


class RelayHTTPServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], handler_class: type[BaseHTTPRequestHandler], config: RelayConfig, usage_store: UsageStore) -> None:
        super().__init__(server_address, handler_class)
        self.relay_config = config
        self.usage_store = usage_store


class RelayRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        self._handle()

    def do_POST(self) -> None:
        self._handle()

    def do_PUT(self) -> None:
        self._handle()

    def do_PATCH(self) -> None:
        self._handle()

    def do_DELETE(self) -> None:
        self._handle()

    def do_OPTIONS(self) -> None:
        self._handle()

    def do_HEAD(self) -> None:
        self._handle()

    def log_message(self, format: str, *args: object) -> None:
        return

    @property
    def relay_config(self) -> RelayConfig:
        return self.server.relay_config  # type: ignore[attr-defined]

    @property
    def usage_store(self) -> UsageStore:
        return self.server.usage_store  # type: ignore[attr-defined]

    def _handle(self) -> None:
        if self.path.startswith("/_relay/"):
            self._handle_admin()
            return
        self._handle_proxy()

    def _handle_admin(self) -> None:
        if self.relay_config.admin_key and self.headers.get("X-Relay-Admin-Key", "") != self.relay_config.admin_key:
            self._send_json(403, {"error": {"message": "forbidden"}})
            return
        if self.path == "/_relay/health":
            self._send_json(
                200,
                {
                    "status": "ok",
                    "listen_host": self.relay_config.listen_host,
                    "listen_port": self.relay_config.listen_port,
                    "database_path": str(self.relay_config.database_path),
                    "apps": [app.app_id for app in self.relay_config.apps],
                },
            )
            return
        if self.path == "/_relay/stats":
            self._send_json(200, self.usage_store.stats_summary())
            return
        if self.path == "/_relay/idle":
            self._send_json(200, self.usage_store.idle_status(self.relay_config.idle_window_seconds))
            return
        self._send_json(404, {"error": {"message": "admin endpoint not found"}})

    def _handle_proxy(self) -> None:
        started_at = datetime.now(timezone.utc)
        request_body = self._read_request_body()
        local_key = _extract_local_key(self.headers)
        app = self.relay_config.apps_by_key.get(local_key or "")
        if app is None:
            payload = {"error": {"message": "invalid local api key"}}
            response_bytes = self._send_json(401, payload)
            finished_at = datetime.now(timezone.utc)
            self.usage_store.record_request(
                started_at=started_at,
                finished_at=finished_at,
                app_id=None,
                method=self.command,
                path=self.path,
                status_code=401,
                forwarded=False,
                request_bytes=len(request_body),
                response_bytes=response_bytes,
                upstream_ms=_duration_ms(started_at, finished_at),
                error_kind="invalid_local_key",
                prompt_tokens=None,
                completion_tokens=None,
                total_tokens=None,
            )
            return
        target = _build_upstream_target(app.upstream_base_url, self.path)
        parsed_target = urlsplit(target)
        connection: http.client.HTTPConnection | http.client.HTTPSConnection
        connection_class = http.client.HTTPSConnection if parsed_target.scheme == "https" else http.client.HTTPConnection
        connection = connection_class(
            parsed_target.hostname,
            parsed_target.port,
            timeout=self.relay_config.request_timeout_seconds,
        )
        forwarded_headers = _build_upstream_headers(self.headers, app.upstream_api_key)
        upstream_path = urlunsplit(("", "", parsed_target.path or "/", parsed_target.query, ""))
        prompt_tokens = None
        completion_tokens = None
        total_tokens = None
        response_bytes = 0
        response_status = 502
        error_kind: str | None = None
        finished_at = started_at
        try:
            connection.request(self.command, upstream_path, body=request_body, headers=forwarded_headers)
            response = connection.getresponse()
            response_status = int(response.status)
            response_bytes, captured_body = self._relay_upstream_response(response)
            finished_at = datetime.now(timezone.utc)
            prompt_tokens, completion_tokens, total_tokens = _extract_usage_metrics(
                response.getheader("Content-Type", ""),
                captured_body,
            )
        except Exception:
            error_kind = "upstream_request_failed"
            payload = {"error": {"message": "upstream request failed"}}
            response_bytes = self._send_json(502, payload)
            finished_at = datetime.now(timezone.utc)
            response_status = 502
        finally:
            connection.close()
            self.usage_store.record_request(
                started_at=started_at,
                finished_at=finished_at,
                app_id=app.app_id,
                method=self.command,
                path=self.path,
                status_code=response_status,
                forwarded=error_kind is None,
                request_bytes=len(request_body),
                response_bytes=response_bytes,
                upstream_ms=_duration_ms(started_at, finished_at),
                error_kind=error_kind,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=total_tokens,
            )

    def _relay_upstream_response(self, response: http.client.HTTPResponse) -> tuple[int, bytes]:
        upstream_headers = response.getheaders()
        content_length_header = response.getheader("Content-Length")
        has_known_length = content_length_header is not None
        expects_no_body = self.command == "HEAD" or response.status in {204, 304} or 100 <= response.status < 200
        use_chunked = not expects_no_body and not has_known_length
        self.send_response(response.status, response.reason)
        for key, value in upstream_headers:
            normalized_key = key.lower()
            if normalized_key in HOP_BY_HOP_HEADERS:
                continue
            if use_chunked and normalized_key == "content-length":
                continue
            self.send_header(key, value)
        if use_chunked:
            self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        if expects_no_body:
            return 0, b""
        captured = bytearray()
        total_bytes = 0
        while True:
            chunk = response.read(64 * 1024)
            if not chunk:
                break
            total_bytes += len(chunk)
            if len(captured) + len(chunk) <= CAPTURE_BODY_LIMIT:
                captured.extend(chunk)
            if use_chunked:
                self.wfile.write(f"{len(chunk):X}\r\n".encode("ascii"))
                self.wfile.write(chunk)
                self.wfile.write(b"\r\n")
            else:
                self.wfile.write(chunk)
            self.wfile.flush()
        if use_chunked:
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        return total_bytes, bytes(captured)

    def _read_request_body(self) -> bytes:
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length <= 0:
            return b""
        return self.rfile.read(content_length)

    def _send_json(self, status_code: int, payload: dict[str, Any]) -> int:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)
            self.wfile.flush()
        return len(body)


def _duration_ms(started_at: datetime, finished_at: datetime) -> int:
    return max(0, int((finished_at - started_at).total_seconds() * 1000))


def _serialize_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _extract_local_key(headers: Any) -> str | None:
    authorization = headers.get("Authorization")
    if authorization and authorization.lower().startswith("bearer "):
        return authorization.split(" ", 1)[1].strip()
    for header_name in ("X-API-Key", "Api-Key"):
        header_value = headers.get(header_name)
        if header_value:
            return header_value.strip()
    return None


def _build_upstream_target(upstream_base_url: str, incoming_path: str) -> str:
    parsed_base = urlsplit(upstream_base_url)
    parsed_incoming = urlsplit(incoming_path)
    base_path = parsed_base.path.rstrip("/")
    incoming_only_path = parsed_incoming.path or "/"
    combined_path = f"{base_path}{incoming_only_path}" if base_path else incoming_only_path
    combined_query = "&".join(part for part in [parsed_base.query, parsed_incoming.query] if part)
    target = SplitResult(
        scheme=parsed_base.scheme,
        netloc=parsed_base.netloc,
        path=combined_path or "/",
        query=combined_query,
        fragment="",
    )
    return urlunsplit(target)


def _build_upstream_headers(headers: Any, upstream_api_key: str) -> dict[str, str]:
    forwarded = {
        key: value
        for key, value in headers.items()
        if key.lower() not in HOP_BY_HOP_HEADERS and key.lower() not in {"host", "content-length", "authorization", "x-api-key", "api-key"}
    }
    if headers.get("Authorization"):
        forwarded["Authorization"] = f"Bearer {upstream_api_key}"
    elif headers.get("X-API-Key"):
        forwarded["X-API-Key"] = upstream_api_key
    elif headers.get("Api-Key"):
        forwarded["Api-Key"] = upstream_api_key
    else:
        forwarded["Authorization"] = f"Bearer {upstream_api_key}"
    return forwarded


def _extract_usage_metrics(content_type: str, body: bytes) -> tuple[int | None, int | None, int | None]:
    if not body or "application/json" not in content_type.lower():
        return None, None, None
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return None, None, None
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return None, None, None
    prompt_tokens = _coerce_optional_int(usage.get("prompt_tokens"))
    completion_tokens = _coerce_optional_int(usage.get("completion_tokens"))
    total_tokens = _coerce_optional_int(usage.get("total_tokens"))
    return prompt_tokens, completion_tokens, total_tokens


def _coerce_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local API relay MVP")
    parser.add_argument("--config", required=True, help="Path to relay config JSON")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = RelayConfig.load(args.config)
    usage_store = UsageStore(config.database_path)
    server = RelayHTTPServer((config.listen_host, config.listen_port), RelayRequestHandler, config, usage_store)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        usage_store.close()


if __name__ == "__main__":
    main()

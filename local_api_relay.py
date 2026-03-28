from __future__ import annotations

import argparse
import http.client
import json
import select
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import SplitResult, urlsplit, urlunsplit
from zoneinfo import ZoneInfo


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
REQUEST_LOG_TABLE = "request_log_v4"
LOCAL_TIMEZONE = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class LocalClientConfig:
    client_id: str
    local_key: str


@dataclass(frozen=True)
class UpstreamConfig:
    upstream_id: str
    base_url: str
    api_key: str
    enabled: bool
    transport: dict[str, Any]

    def timeout_seconds(self, default: int) -> int:
        try:
            return int(self.transport.get("timeout_seconds") or default)
        except (TypeError, ValueError):
            return default


@dataclass(frozen=True)
class PlannedUpstreamRequest:
    upstream: UpstreamConfig
    request_body: bytes


@dataclass(frozen=True)
class RequestPlan:
    client: LocalClientConfig | None
    requested_model: str | None
    attempts: list[PlannedUpstreamRequest]
    error_status: int | None = None
    error_kind: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class StoredUpstreamError:
    status_code: int
    reason: str
    headers: list[tuple[str, str]]
    body: bytes


@dataclass(frozen=True)
class RelayConfig:
    listen_host: str
    listen_port: int
    admin_key: str
    database_path: Path
    idle_window_seconds: int
    request_timeout_seconds: int
    local_clients: list[LocalClientConfig]
    upstreams: list[UpstreamConfig]
    order: list[str]

    @property
    def local_clients_by_key(self) -> dict[str, LocalClientConfig]:
        return {client.local_key: client for client in self.local_clients}

    @property
    def upstreams_by_id(self) -> dict[str, UpstreamConfig]:
        return {upstream.upstream_id: upstream for upstream in self.upstreams}

    @classmethod
    def load(cls, path: str | Path) -> "RelayConfig":
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("relay config must be a JSON object")

        server_payload = _as_dict(payload.get("server"), "server")
        local_payload = _as_dict(payload.get("local"), "local")
        upstreams_payload = _as_dict(payload.get("upstreams"), "upstreams")
        order_payload = _as_list(payload.get("order"), "order")

        local_clients = [
            LocalClientConfig(
                client_id=str(item["client_id"]),
                local_key=str(item["local_key"]),
            )
            for item in (_as_dict(item, "local.clients[]") for item in _as_list(local_payload.get("clients"), "local.clients"))
        ]
        _ensure_unique([client.client_id for client in local_clients], "client_id")
        _ensure_unique([client.local_key for client in local_clients], "local_key")

        upstreams: list[UpstreamConfig] = []
        for upstream_id, raw_upstream in sorted(upstreams_payload.items()):
            upstream_payload = _as_dict(raw_upstream, f"upstreams.{upstream_id}")
            upstreams.append(
                UpstreamConfig(
                    upstream_id=str(upstream_id),
                    base_url=str(upstream_payload["base_url"]),
                    api_key=str(upstream_payload["api_key"]),
                    enabled=_as_bool(upstream_payload.get("enabled", True)),
                    transport=_coerce_dict(upstream_payload.get("transport")),
                )
            )
        _ensure_unique([upstream.upstream_id for upstream in upstreams], "upstream_id")

        upstreams_by_id = {upstream.upstream_id: upstream for upstream in upstreams}
        order = [str(item) for item in order_payload]
        _ensure_unique(order, "order")
        for upstream_id in order:
            if upstream_id not in upstreams_by_id:
                raise ValueError(f"order references unknown upstream {upstream_id}")

        database_path = Path(server_payload.get("database_path") or "state/local_api_relay.sqlite3")
        return cls(
            listen_host=str(server_payload.get("listen_host") or "127.0.0.1"),
            listen_port=int(server_payload.get("listen_port") or 8787),
            admin_key=str(server_payload.get("admin_key") or ""),
            database_path=database_path,
            idle_window_seconds=int(server_payload.get("idle_window_seconds") or 300),
            request_timeout_seconds=int(server_payload.get("request_timeout_seconds") or 120),
            local_clients=local_clients,
            upstreams=upstreams,
            order=order,
        )


class UsageStore:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(database_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        self.active_requests = 0
        self.active_requests_by_client: dict[str, int] = {}
        self.active_requests_by_upstream: dict[str, int] = {}
        self.active_requests_by_model: dict[str, int] = {}
        with self.lock:
            self.connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {REQUEST_LOG_TABLE} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT NOT NULL,
                    client_id TEXT,
                    upstream_id TEXT,
                    requested_model TEXT,
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
            self.connection.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{REQUEST_LOG_TABLE}_finished_at ON {REQUEST_LOG_TABLE}(finished_at)"
            )
            self.connection.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{REQUEST_LOG_TABLE}_client_finished_at ON {REQUEST_LOG_TABLE}(client_id, finished_at)"
            )
            self.connection.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{REQUEST_LOG_TABLE}_upstream_finished_at ON {REQUEST_LOG_TABLE}(upstream_id, finished_at)"
            )
            self.connection.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{REQUEST_LOG_TABLE}_model_finished_at ON {REQUEST_LOG_TABLE}(requested_model, finished_at)"
            )
            self.connection.commit()

    def close(self) -> None:
        with self.lock:
            self.connection.close()

    def request_started(
        self,
        client_id: str | None,
        upstream_id: str | None,
        requested_model: str | None,
    ) -> None:
        with self.lock:
            self.active_requests += 1
            _increment_counter(self.active_requests_by_client, client_id)
            _increment_counter(self.active_requests_by_upstream, upstream_id)
            _increment_counter(self.active_requests_by_model, requested_model)

    def request_finished(
        self,
        client_id: str | None,
        upstream_id: str | None,
        requested_model: str | None,
    ) -> None:
        with self.lock:
            self.active_requests = max(0, self.active_requests - 1)
            _decrement_counter(self.active_requests_by_client, client_id)
            _decrement_counter(self.active_requests_by_upstream, upstream_id)
            _decrement_counter(self.active_requests_by_model, requested_model)

    def record_request(
        self,
        *,
        started_at: datetime,
        finished_at: datetime,
        client_id: str | None,
        upstream_id: str | None,
        requested_model: str | None,
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
                f"""
                INSERT INTO {REQUEST_LOG_TABLE} (
                    started_at,
                    finished_at,
                    client_id,
                    upstream_id,
                    requested_model,
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _serialize_datetime(started_at),
                    _serialize_datetime(finished_at),
                    client_id,
                    upstream_id,
                    requested_model,
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
            totals_row = self.connection.execute(_summary_query()).fetchone()
            totals_status_rows = self.connection.execute(
                f"""
                SELECT status_code, COUNT(*) AS count
                FROM {REQUEST_LOG_TABLE}
                GROUP BY status_code
                ORDER BY status_code ASC
                """
            ).fetchall()
            client_rows = self.connection.execute(
                _dimension_summary_query("client_id", "client_id IS NOT NULL", "client_id")
            ).fetchall()
            client_status_rows = self.connection.execute(
                _dimension_status_query("client_id", "client_id IS NOT NULL", "client_id")
            ).fetchall()
            upstream_rows = self.connection.execute(
                _dimension_summary_query("upstream_id", "upstream_id IS NOT NULL", "upstream_id")
            ).fetchall()
            upstream_status_rows = self.connection.execute(
                _dimension_status_query("upstream_id", "upstream_id IS NOT NULL", "upstream_id")
            ).fetchall()
            model_rows = self.connection.execute(
                _dimension_summary_query(
                    "requested_model",
                    "requested_model IS NOT NULL AND client_id IS NOT NULL",
                    "requested_model",
                )
            ).fetchall()
            model_status_rows = self.connection.execute(
                _dimension_status_query(
                    "requested_model",
                    "requested_model IS NOT NULL AND client_id IS NOT NULL",
                    "requested_model",
                )
            ).fetchall()
            active_requests = self.active_requests
            active_requests_by_client = dict(self.active_requests_by_client)
            active_requests_by_upstream = dict(self.active_requests_by_upstream)
            active_requests_by_model = dict(self.active_requests_by_model)

        return {
            "totals": _build_summary_payload(
                totals_row,
                status_codes=_status_code_counts(totals_status_rows),
                in_flight_requests=active_requests,
                include_local_rejects=True,
            ),
            "clients": _build_dimension_summaries(
                rows=client_rows,
                status_rows=client_status_rows,
                label_key="client_id",
                active_counts=active_requests_by_client,
            ),
            "upstreams": _build_dimension_summaries(
                rows=upstream_rows,
                status_rows=upstream_status_rows,
                label_key="upstream_id",
                active_counts=active_requests_by_upstream,
            ),
            "models": _build_dimension_summaries(
                rows=model_rows,
                status_rows=model_status_rows,
                label_key="model",
                active_counts=active_requests_by_model,
            ),
        }

    def idle_status(self, window_seconds: int) -> dict[str, Any]:
        cutoff = _serialize_datetime(datetime.fromtimestamp(time.time() - window_seconds, tz=timezone.utc))
        with self.lock:
            row = self.connection.execute(
                f"""
                SELECT
                    COUNT(*) AS requests,
                    COALESCE(SUM(CASE WHEN forwarded = 1 THEN 1 ELSE 0 END), 0) AS forwarded,
                    COALESCE(SUM(CASE WHEN forwarded = 1 AND status_code BETWEEN 200 AND 299 THEN 1 ELSE 0 END), 0) AS successes,
                    COALESCE(SUM(CASE WHEN error_kind = 'invalid_local_key' THEN 1 ELSE 0 END), 0) AS local_rejects,
                    AVG(upstream_ms) AS avg_upstream_ms,
                    MIN(upstream_ms) AS min_upstream_ms,
                    MAX(upstream_ms) AS max_upstream_ms,
                    MAX(finished_at) AS last_request_at,
                    MAX(CASE WHEN forwarded = 1 AND status_code BETWEEN 200 AND 299 THEN finished_at END) AS last_success_at
                FROM {REQUEST_LOG_TABLE}
                WHERE finished_at >= ?
                """,
                (cutoff,),
            ).fetchone()
            status_rows = self.connection.execute(
                f"""
                SELECT status_code, COUNT(*) AS count
                FROM {REQUEST_LOG_TABLE}
                WHERE finished_at >= ?
                GROUP BY status_code
                ORDER BY status_code ASC
                """,
                (cutoff,),
            ).fetchall()
            active_requests = self.active_requests

        recent_requests = int(row["requests"])
        successes = int(row["successes"])
        failures = max(0, recent_requests - successes)
        local_rejects = int(row["local_rejects"])
        if active_requests > 0:
            idle = False
            state = "active"
            reason = "requests_in_flight"
        elif recent_requests == 0:
            idle = True
            state = "idle"
            reason = "no_recent_requests"
        elif successes > 0:
            idle = False
            state = "active"
            reason = "recent_successful_requests"
        elif local_rejects == failures and failures > 0:
            idle = False
            state = "rejected"
            reason = "recent_rejected_requests"
        elif failures > 0:
            idle = False
            state = "degraded"
            reason = "recent_failed_requests"
        else:
            idle = False
            state = "active"
            reason = "recent_request_activity"
        return {
            "idle": idle,
            "state": state,
            "reason": reason,
            "window_seconds": window_seconds,
            "recent": {
                "requests": recent_requests,
                "forwarded": int(row["forwarded"]),
                "successes": successes,
                "failures": failures,
                "local_rejects": local_rejects,
                "status_codes": _status_code_counts(status_rows),
                "latency_ms": _latency_summary(
                    recent_requests,
                    row["avg_upstream_ms"],
                    row["min_upstream_ms"],
                    row["max_upstream_ms"],
                ),
                "in_flight_requests": active_requests,
                "last_request_at": row["last_request_at"],
                "last_request_at_local": _to_local_isoformat(row["last_request_at"]),
                "last_success_at": row["last_success_at"],
                "last_success_at_local": _to_local_isoformat(row["last_success_at"]),
            },
        }


class RelayHTTPServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        config: RelayConfig,
        usage_store: UsageStore,
    ) -> None:
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
                    "server": {
                        "listen_host": self.relay_config.listen_host,
                        "listen_port": self.relay_config.listen_port,
                        "database_path": str(self.relay_config.database_path),
                        "idle_window_seconds": self.relay_config.idle_window_seconds,
                        "request_timeout_seconds": self.relay_config.request_timeout_seconds,
                    },
                    "local": {
                        "clients": sorted(client.client_id for client in self.relay_config.local_clients),
                    },
                    "upstreams": [
                        {
                            "upstream_id": upstream.upstream_id,
                            "base_url": upstream.base_url,
                            "enabled": upstream.enabled,
                            "transport": upstream.transport,
                        }
                        for upstream in sorted(self.relay_config.upstreams, key=lambda item: item.upstream_id)
                    ],
                    "order": list(self.relay_config.order),
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
        original_request_body = self._read_request_body()
        plan = plan_request_attempts(self.relay_config, self.headers, original_request_body)
        client_id = plan.client.client_id if plan.client is not None else None
        initial_upstream_id = plan.attempts[0].upstream.upstream_id if plan.attempts else None
        self.usage_store.request_started(client_id, initial_upstream_id, plan.requested_model)
        try:
            if plan.error_status is not None:
                started_at = datetime.now(timezone.utc)
                response_bytes = self._send_json(
                    plan.error_status,
                    {"error": {"message": plan.error_message or "relay request rejected"}},
                )
                finished_at = datetime.now(timezone.utc)
                self.usage_store.record_request(
                    started_at=started_at,
                    finished_at=finished_at,
                    client_id=client_id,
                    upstream_id=None,
                    requested_model=plan.requested_model,
                    method=self.command,
                    path=self.path,
                    status_code=plan.error_status,
                    forwarded=False,
                    request_bytes=len(original_request_body),
                    response_bytes=response_bytes,
                    upstream_ms=_duration_ms(started_at, finished_at),
                    error_kind=plan.error_kind,
                    prompt_tokens=None,
                    completion_tokens=None,
                    total_tokens=None,
                )
                return

            last_meaningful_error: StoredUpstreamError | None = None
            for index, attempt in enumerate(plan.attempts):
                attempt_started_at = datetime.now(timezone.utc)
                parsed_target = urlsplit(_build_upstream_target(attempt.upstream.base_url, self.path))
                upstream_path = urlunsplit(("", "", parsed_target.path or "/", parsed_target.query, ""))
                connection_class = (
                    http.client.HTTPSConnection
                    if parsed_target.scheme == "https"
                    else http.client.HTTPConnection
                )
                connection: http.client.HTTPConnection | http.client.HTTPSConnection | None = None
                try:
                    connection = connection_class(
                        parsed_target.hostname,
                        parsed_target.port,
                        timeout=attempt.upstream.timeout_seconds(self.relay_config.request_timeout_seconds),
                    )
                    forwarded_headers = _build_upstream_headers(self.headers, attempt.upstream.api_key)
                    connection.request(
                        self.command,
                        upstream_path,
                        body=attempt.request_body,
                        headers=forwarded_headers,
                    )
                    response = connection.getresponse()

                    if response.status < 400 or _is_upgrade_response(self.headers, response):
                        if _is_upgrade_response(self.headers, response):
                            response_bytes = self._relay_upstream_upgrade(connection, response)
                            captured_body = b""
                        else:
                            response_bytes, captured_body = self._relay_upstream_response(response)
                        finished_at = datetime.now(timezone.utc)
                        prompt_tokens, completion_tokens, total_tokens = _extract_usage_metrics(
                            response.getheader("Content-Type", ""),
                            captured_body,
                        )
                        self.usage_store.record_request(
                            started_at=attempt_started_at,
                            finished_at=finished_at,
                            client_id=client_id,
                            upstream_id=attempt.upstream.upstream_id,
                            requested_model=plan.requested_model,
                            method=self.command,
                            path=self.path,
                            status_code=int(response.status),
                            forwarded=True,
                            request_bytes=len(attempt.request_body),
                            response_bytes=response_bytes,
                            upstream_ms=_duration_ms(attempt_started_at, finished_at),
                            error_kind=None,
                            prompt_tokens=prompt_tokens,
                            completion_tokens=completion_tokens,
                            total_tokens=total_tokens,
                        )
                        return

                    response_headers = response.getheaders()
                    response_bytes, captured_body = _read_full_response_body(response)
                    finished_at = datetime.now(timezone.utc)
                    prompt_tokens, completion_tokens, total_tokens = _extract_usage_metrics(
                        response.getheader("Content-Type", ""),
                        captured_body,
                    )
                    should_retry = _should_retry_with_fallback(
                        int(response.status),
                        response.getheader("Content-Type", ""),
                        captured_body,
                        plan.requested_model,
                    )
                    can_retry = should_retry and index + 1 < len(plan.attempts)
                    self.usage_store.record_request(
                        started_at=attempt_started_at,
                        finished_at=finished_at,
                        client_id=client_id,
                        upstream_id=attempt.upstream.upstream_id,
                        requested_model=plan.requested_model,
                        method=self.command,
                        path=self.path,
                        status_code=int(response.status),
                        forwarded=True,
                        request_bytes=len(attempt.request_body),
                        response_bytes=response_bytes,
                        upstream_ms=_duration_ms(attempt_started_at, finished_at),
                        error_kind="upstream_status_fallback" if can_retry else None,
                        prompt_tokens=prompt_tokens,
                        completion_tokens=completion_tokens,
                        total_tokens=total_tokens,
                    )
                    if can_retry:
                        last_meaningful_error = StoredUpstreamError(
                            status_code=int(response.status),
                            reason=response.reason,
                            headers=response_headers,
                            body=captured_body,
                        )
                        continue
                    self._relay_buffered_response(
                        int(response.status),
                        response.reason,
                        response_headers,
                        captured_body,
                    )
                    return
                except (OSError, TimeoutError, http.client.HTTPException):
                    finished_at = datetime.now(timezone.utc)
                    self.usage_store.record_request(
                        started_at=attempt_started_at,
                        finished_at=finished_at,
                        client_id=client_id,
                        upstream_id=attempt.upstream.upstream_id,
                        requested_model=plan.requested_model,
                        method=self.command,
                        path=self.path,
                        status_code=502,
                        forwarded=False,
                        request_bytes=len(attempt.request_body),
                        response_bytes=0,
                        upstream_ms=_duration_ms(attempt_started_at, finished_at),
                        error_kind="upstream_request_failed",
                        prompt_tokens=None,
                        completion_tokens=None,
                        total_tokens=None,
                    )
                    if index + 1 < len(plan.attempts):
                        continue
                    if last_meaningful_error is not None:
                        self._relay_buffered_response(
                            last_meaningful_error.status_code,
                            last_meaningful_error.reason,
                            last_meaningful_error.headers,
                            last_meaningful_error.body,
                        )
                        return
                    self._send_json(502, {"error": {"message": "upstream request failed"}})
                    return
                finally:
                    if connection is not None:
                        connection.close()

            if last_meaningful_error is not None:
                self._relay_buffered_response(
                    last_meaningful_error.status_code,
                    last_meaningful_error.reason,
                    last_meaningful_error.headers,
                    last_meaningful_error.body,
                )
                return
            self._send_json(502, {"error": {"message": "upstream request failed"}})
        finally:
            self.usage_store.request_finished(client_id, initial_upstream_id, plan.requested_model)

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

    def _relay_upstream_upgrade(
        self,
        connection: http.client.HTTPConnection | http.client.HTTPSConnection,
        response: http.client.HTTPResponse,
    ) -> int:
        upstream_headers = response.getheaders()
        self.send_response(response.status, response.reason)
        for key, value in upstream_headers:
            self.send_header(key, value)
        self.end_headers()
        self.wfile.flush()
        upstream_socket = connection.sock
        if upstream_socket is None:
            raise RuntimeError("upstream socket missing for upgrade relay")
        client_socket = self.connection
        upstream_socket.setblocking(False)
        client_socket.setblocking(False)
        total_bytes = 0
        sockets = [client_socket, upstream_socket]
        while True:
            readable, _, exceptional = select.select(
                sockets,
                [],
                sockets,
                self.relay_config.request_timeout_seconds,
            )
            if exceptional or not readable:
                break
            for source in readable:
                try:
                    chunk = source.recv(64 * 1024)
                except BlockingIOError:
                    continue
                if not chunk:
                    return total_bytes
                target_socket = upstream_socket if source is client_socket else client_socket
                target_socket.sendall(chunk)
                if source is upstream_socket:
                    total_bytes += len(chunk)
        return total_bytes

    def _relay_buffered_response(
        self,
        status_code: int,
        reason: str,
        headers: list[tuple[str, str]],
        body: bytes,
    ) -> int:
        expects_no_body = self.command == "HEAD" or status_code in {204, 304} or 100 <= status_code < 200
        self.send_response(status_code, reason)
        for key, value in headers:
            lowered = key.lower()
            if lowered in HOP_BY_HOP_HEADERS or lowered == "content-length":
                continue
            self.send_header(key, value)
        self.send_header("Content-Length", "0" if expects_no_body else str(len(body)))
        self.end_headers()
        if not expects_no_body:
            self.wfile.write(body)
            self.wfile.flush()
        return 0 if expects_no_body else len(body)

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


def plan_request_attempts(config: RelayConfig, headers: Any, request_body: bytes) -> RequestPlan:
    local_key = _extract_local_key(headers)
    client = config.local_clients_by_key.get(local_key or "")
    _, requested_model = _parse_request_body(request_body)
    if client is None:
        return RequestPlan(
            client=None,
            requested_model=requested_model,
            attempts=[],
            error_status=401,
            error_kind="invalid_local_key",
            error_message="invalid local api key",
        )

    attempts: list[PlannedUpstreamRequest] = []
    seen_upstream_ids: set[str] = set()
    for upstream_id in config.order:
        if not upstream_id or upstream_id in seen_upstream_ids:
            continue
        seen_upstream_ids.add(upstream_id)
        upstream = config.upstreams_by_id[upstream_id]
        if not upstream.enabled:
            continue
        attempts.append(PlannedUpstreamRequest(upstream=upstream, request_body=request_body))

    if attempts:
        return RequestPlan(
            client=client,
            requested_model=requested_model,
            attempts=attempts,
        )

    return RequestPlan(
        client=client,
        requested_model=requested_model,
        attempts=[],
        error_status=502,
        error_kind="no_enabled_upstreams",
        error_message="no enabled upstreams configured",
    )


def _should_retry_with_fallback(
    status_code: int,
    content_type: str = "",
    response_body: bytes = b"",
    requested_model: str | None = None,
) -> bool:
    if status_code >= 500:
        return True
    if status_code < 400 or status_code >= 500:
        return False
    message = _extract_error_message(content_type, response_body).lower()
    if not message:
        return False
    requested_model_lower = (requested_model or "").strip().lower()
    model_hint = "model" in message or (requested_model_lower and requested_model_lower in message)
    if not model_hint:
        return False
    markers = (
        "unsupported model",
        "not supported",
        "no such model",
        "unknown model",
        "model not found",
        "does not exist",
        "not exist",
        "invalid model",
        "unrecognized model",
        "model is unavailable",
        "model unavailable",
        "not available",
    )
    return any(marker in message for marker in markers)


def _parse_request_body(request_body: bytes) -> tuple[dict[str, Any] | None, str | None]:
    if not request_body:
        return None, None
    try:
        payload = json.loads(request_body)
    except json.JSONDecodeError:
        return None, None
    if not isinstance(payload, dict):
        return None, None
    model = payload.get("model")
    if not isinstance(model, str) or not model.strip():
        return payload, None
    return payload, model.strip()


def _extract_error_message(content_type: str, body: bytes) -> str:
    snippet = body[:CAPTURE_BODY_LIMIT]
    if not snippet:
        return ""
    try:
        text = snippet.decode("utf-8", errors="replace")
    except Exception:
        text = ""
    if "json" not in content_type.lower():
        return text
    try:
        payload = json.loads(snippet)
    except json.JSONDecodeError:
        return text
    if isinstance(payload, dict):
        error = payload.get("error")
        if isinstance(error, dict):
            message = error.get("message")
            if isinstance(message, str):
                return message
        message = payload.get("message")
        if isinstance(message, str):
            return message
    return text


def _duration_ms(started_at: datetime, finished_at: datetime) -> int:
    return max(0, int((finished_at - started_at).total_seconds() * 1000))


def _serialize_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _to_local_isoformat(value: str | None) -> str | None:
    if not value:
        return None
    return datetime.fromisoformat(value).astimezone(LOCAL_TIMEZONE).isoformat(timespec="seconds")


def _extract_local_key(headers: Any) -> str | None:
    authorization = headers.get("Authorization")
    if authorization and authorization.lower().startswith("bearer "):
        return authorization.split(" ", 1)[1].strip()
    for header_name in ("X-API-Key", "Api-Key"):
        header_value = headers.get(header_name)
        if header_value:
            return header_value.strip()
    return None


def _is_upgrade_response(headers: Any, response: http.client.HTTPResponse) -> bool:
    connection_header = headers.get("Connection", "")
    upgrade_header = headers.get("Upgrade", "")
    response_connection = response.getheader("Connection", "")
    response_upgrade = response.getheader("Upgrade", "")
    return (
        response.status == 101
        and "upgrade" in connection_header.lower()
        and bool(upgrade_header)
        and "upgrade" in response_connection.lower()
        and bool(response_upgrade)
    )


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
    is_upgrade = "upgrade" in headers.get("Connection", "").lower() and bool(headers.get("Upgrade"))
    forwarded: dict[str, str] = {}
    for key, value in headers.items():
        lowered = key.lower()
        if lowered in {"host", "content-length", "authorization", "x-api-key", "api-key"}:
            continue
        if lowered in HOP_BY_HOP_HEADERS and not (is_upgrade and lowered in {"connection", "upgrade"}):
            continue
        forwarded[key] = value
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


def _latency_summary(
    requests: int,
    avg_upstream_ms: Any,
    min_upstream_ms: Any,
    max_upstream_ms: Any,
) -> dict[str, int | None]:
    if requests <= 0 or avg_upstream_ms is None or min_upstream_ms is None or max_upstream_ms is None:
        return {"avg": None, "min": None, "max": None}
    return {
        "avg": int(float(avg_upstream_ms) + 0.5),
        "min": int(min_upstream_ms),
        "max": int(max_upstream_ms),
    }


def _status_code_counts(rows: list[sqlite3.Row]) -> dict[str, int]:
    return {str(int(row["status_code"])): int(row["count"]) for row in rows}


def _summary_query() -> str:
    return f"""
        SELECT
            COUNT(*) AS requests,
            COALESCE(SUM(CASE WHEN forwarded = 1 THEN 1 ELSE 0 END), 0) AS forwarded,
            COALESCE(SUM(CASE WHEN status_code BETWEEN 200 AND 299 THEN 1 ELSE 0 END), 0) AS successes,
            COALESCE(SUM(CASE WHEN error_kind = 'invalid_local_key' THEN 1 ELSE 0 END), 0) AS local_rejects,
            COALESCE(SUM(CASE WHEN prompt_tokens IS NOT NULL OR completion_tokens IS NOT NULL OR total_tokens IS NOT NULL THEN 1 ELSE 0 END), 0) AS usage_responses,
            COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
            COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
            COALESCE(SUM(total_tokens), 0) AS total_tokens,
            AVG(upstream_ms) AS avg_upstream_ms,
            MIN(upstream_ms) AS min_upstream_ms,
            MAX(upstream_ms) AS max_upstream_ms,
            MAX(finished_at) AS last_request_at,
            MAX(CASE WHEN status_code BETWEEN 200 AND 299 THEN finished_at END) AS last_success_at
        FROM {REQUEST_LOG_TABLE}
    """


def _dimension_summary_query(column: str, where_clause: str, order_column: str) -> str:
    return f"""
        SELECT
            {column} AS dimension_id,
            COUNT(*) AS requests,
            COALESCE(SUM(CASE WHEN forwarded = 1 THEN 1 ELSE 0 END), 0) AS forwarded,
            COALESCE(SUM(CASE WHEN status_code BETWEEN 200 AND 299 THEN 1 ELSE 0 END), 0) AS successes,
            COALESCE(SUM(CASE WHEN prompt_tokens IS NOT NULL OR completion_tokens IS NOT NULL OR total_tokens IS NOT NULL THEN 1 ELSE 0 END), 0) AS usage_responses,
            COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
            COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
            COALESCE(SUM(total_tokens), 0) AS total_tokens,
            AVG(upstream_ms) AS avg_upstream_ms,
            MIN(upstream_ms) AS min_upstream_ms,
            MAX(upstream_ms) AS max_upstream_ms,
            MAX(finished_at) AS last_request_at,
            MAX(CASE WHEN status_code BETWEEN 200 AND 299 THEN finished_at END) AS last_success_at
        FROM {REQUEST_LOG_TABLE}
        WHERE {where_clause}
        GROUP BY {column}
        ORDER BY {order_column} ASC
    """


def _dimension_status_query(column: str, where_clause: str, order_column: str) -> str:
    return f"""
        SELECT {column} AS dimension_id, status_code, COUNT(*) AS count
        FROM {REQUEST_LOG_TABLE}
        WHERE {where_clause}
        GROUP BY {column}, status_code
        ORDER BY {order_column} ASC, status_code ASC
    """


def _build_dimension_summaries(
    *,
    rows: list[sqlite3.Row],
    status_rows: list[sqlite3.Row],
    label_key: str,
    active_counts: dict[str, int],
) -> list[dict[str, Any]]:
    status_codes_by_id: dict[str, dict[str, int]] = {}
    for row in status_rows:
        dimension_id = str(row["dimension_id"])
        status_codes_by_id.setdefault(dimension_id, {})[str(int(row["status_code"]))] = int(row["count"])

    summaries_by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        dimension_id = str(row["dimension_id"])
        summary = _build_summary_payload(
            row,
            status_codes=status_codes_by_id.get(dimension_id, {}),
            in_flight_requests=active_counts.get(dimension_id, 0),
        )
        summary[label_key] = dimension_id
        summaries_by_id[dimension_id] = summary

    for dimension_id, in_flight_requests in active_counts.items():
        if dimension_id in summaries_by_id:
            continue
        summary = _empty_summary_payload(in_flight_requests)
        summary[label_key] = dimension_id
        summaries_by_id[dimension_id] = summary

    return [summaries_by_id[dimension_id] for dimension_id in sorted(summaries_by_id)]


def _build_summary_payload(
    row: sqlite3.Row,
    *,
    status_codes: dict[str, int],
    in_flight_requests: int,
    include_local_rejects: bool = False,
) -> dict[str, Any]:
    requests = int(row["requests"])
    successes = int(row["successes"])
    payload = {
        "requests": requests,
        "forwarded": int(row["forwarded"]),
        "successes": successes,
        "failures": max(0, requests - successes),
        "status_codes": status_codes,
        "usage": {
            "responses": int(row["usage_responses"]),
            "prompt_tokens": int(row["prompt_tokens"]),
            "completion_tokens": int(row["completion_tokens"]),
            "total_tokens": int(row["total_tokens"]),
        },
        "latency_ms": _latency_summary(
            requests,
            row["avg_upstream_ms"],
            row["min_upstream_ms"],
            row["max_upstream_ms"],
        ),
        "in_flight_requests": in_flight_requests,
        "last_request_at": row["last_request_at"],
        "last_request_at_local": _to_local_isoformat(row["last_request_at"]),
        "last_success_at": row["last_success_at"],
        "last_success_at_local": _to_local_isoformat(row["last_success_at"]),
    }
    if include_local_rejects:
        payload["local_rejects"] = int(row["local_rejects"])
    return payload


def _empty_summary_payload(in_flight_requests: int) -> dict[str, Any]:
    return {
        "requests": 0,
        "forwarded": 0,
        "successes": 0,
        "failures": 0,
        "status_codes": {},
        "usage": {
            "responses": 0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        },
        "latency_ms": _latency_summary(0, None, None, None),
        "in_flight_requests": in_flight_requests,
        "last_request_at": None,
        "last_request_at_local": None,
        "last_success_at": None,
        "last_success_at_local": None,
    }


def _read_full_response_body(response: http.client.HTTPResponse) -> tuple[int, bytes]:
    chunks: list[bytes] = []
    total_bytes = 0
    while True:
        chunk = response.read(64 * 1024)
        if not chunk:
            return total_bytes, b"".join(chunks)
        total_bytes += len(chunk)
        chunks.append(chunk)


def _as_dict(value: Any, label: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def _as_list(value: Any, label: str) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{label} must be a list")
    return value


def _coerce_dict(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("expected object")
    return value


def _as_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() not in {"", "0", "false", "no", "off"}
    return bool(value)


def _ensure_unique(values: list[str], label: str) -> None:
    if len(values) != len(set(values)):
        raise ValueError(f"duplicate {label} entries are not allowed")


def _increment_counter(counter: dict[str, int], key: str | None) -> None:
    if key is None:
        return
    counter[key] = counter.get(key, 0) + 1


def _decrement_counter(counter: dict[str, int], key: str | None) -> None:
    if key is None:
        return
    current = counter.get(key, 0)
    if current <= 1:
        counter.pop(key, None)
        return
    counter[key] = current - 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local API relay")
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

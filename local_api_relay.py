from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import select
import sqlite3
import socket
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import SplitResult, parse_qs, urlsplit, urlunsplit
from urllib.request import Request, urlopen
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
DEFAULT_COST_CURRENCY = "USD"
TOKENS_PER_MILLION = Decimal("1000000")
COST_PRECISION = Decimal("0.00000001")
DEFAULT_PROBE_METHOD = "GET"
DEFAULT_PROBE_PATH = "/models"
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_MAX_PARALLEL_PROBES = 8
UNSUPPORTED_ENDPOINT_STATUS_CODES = {404, 405, 501}
DEFAULT_MODELS_PATH = "/models"
DEFAULT_RESPONSES_PATH = "/responses"
DEFAULT_CHAT_COMPLETIONS_PATH = "/chat/completions"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 10.0
DEFAULT_PREVIEW_MODEL_LIMIT = 5
DEFAULT_REPLY_PREVIEW_CHARS = 160
DEFAULT_RESPONSES_INPUT = "Reply with: pong"
DEFAULT_CHAT_MESSAGE = "Reply with: pong"
RESPONSES_MODEL_PREFIXES = ("gpt-5", "gpt-4.1", "o1", "o3", "o4")
STREAM_REQUIRED_ERROR_MARKERS = (
    "stream must be true",
    "stream=true",
    "stream is required",
    "streaming is required",
)
RESPONSES_FALLBACK_ERROR_MARKERS = (
    "responses endpoint not supported",
    "responses api is not supported",
    "responses are not supported",
    "unsupported endpoint",
    "chat/completions",
    "chat completions",
)
NON_TEXT_MODEL_MARKERS = (
    "embedding",
    "moderation",
    "whisper",
    "tts",
    "audio",
    "image",
    "rerank",
)
TEXT_MODEL_HINTS = (
    "gpt",
    "claude",
    "gemini",
    "deepseek",
    "qwen",
    "llama",
    "mistral",
    "command",
    "glm",
    "yi",
    "kimi",
    "doubao",
    "o1",
    "o3",
    "o4",
)


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
class TokenPrice:
    input_per_million_tokens: Decimal
    output_per_million_tokens: Decimal


@dataclass(frozen=True)
class UpstreamPricing:
    default_price: TokenPrice | None
    model_prices: dict[str, TokenPrice]


@dataclass(frozen=True)
class PricingCatalog:
    currency: str
    upstreams: dict[str, UpstreamPricing]


@dataclass(frozen=True)
class PlannedUpstreamRequest:
    upstream: UpstreamConfig
    request_body: bytes
    breaker_permitted: bool = True


@dataclass(frozen=True)
class RequestPlan:
    client: LocalClientConfig | None
    requested_model: str | None
    attempts: list[PlannedUpstreamRequest]
    error_status: int | None = None
    error_kind: str | None = None
    error_message: str | None = None
    configured_order: list[str] = field(default_factory=list)
    effective_order: list[str] = field(default_factory=list)
    order_pool: list[dict[str, Any]] = field(default_factory=list)

    def routing_snapshot(self) -> dict[str, Any]:
        return {
            "configured_order": list(self.configured_order),
            "effective_order": list(self.effective_order),
            "order_pool": [dict(item) for item in self.order_pool],
        }


@dataclass(frozen=True)
class UpstreamAttemptFailure:
    upstream_id: str
    error_kind: str
    status_code: int | None = None
    reason: str | None = None
    message: str | None = None

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "upstream_id": self.upstream_id,
            "error_kind": self.error_kind,
        }
        if self.status_code is not None:
            payload["status_code"] = self.status_code
        if self.reason:
            payload["reason"] = self.reason
        if self.message:
            payload["message"] = self.message
        return payload


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
    pricing: PricingCatalog | None = None

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
        pricing = _load_pricing_catalog(
            payload.get("pricing"),
            upstreams_payload=upstreams_payload,
        )
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
            pricing=pricing,
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
                    request_trace_id TEXT,
                    attempt_index INTEGER,
                    requested_model TEXT,
                    method TEXT NOT NULL,
                    path TEXT NOT NULL,
                    status_code INTEGER NOT NULL,
                    forwarded INTEGER NOT NULL,
                    request_bytes INTEGER NOT NULL,
                    response_bytes INTEGER NOT NULL,
                    upstream_ms INTEGER NOT NULL,
                    error_kind TEXT,
                    terminal_outcome TEXT,
                    routing_snapshot_json TEXT,
                    prompt_tokens INTEGER,
                    completion_tokens INTEGER,
                    total_tokens INTEGER,
                    cached_tokens INTEGER
                )
                """
            )
            self._ensure_request_log_columns()
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
            self.connection.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{REQUEST_LOG_TABLE}_trace_attempt ON {REQUEST_LOG_TABLE}(request_trace_id, attempt_index)"
            )
            self.connection.commit()

    def close(self) -> None:
        with self.lock:
            self.connection.close()

    def _ensure_request_log_columns(self) -> None:
        existing_columns = {
            str(row["name"])
            for row in self.connection.execute(f"PRAGMA table_info({REQUEST_LOG_TABLE})").fetchall()
        }
        expected_columns = {
            "request_trace_id": "TEXT",
            "attempt_index": "INTEGER",
            "terminal_outcome": "TEXT",
            "routing_snapshot_json": "TEXT",
            "cached_tokens": "INTEGER",
        }
        for column_name, column_type in expected_columns.items():
            if column_name in existing_columns:
                continue
            self.connection.execute(
                f"ALTER TABLE {REQUEST_LOG_TABLE} ADD COLUMN {column_name} {column_type}"
            )

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
        request_trace_id: str | None = None,
        attempt_index: int | None = None,
        terminal_outcome: str | None = None,
        routing_snapshot_json: str | None = None,
        cached_tokens: int | None = None,
    ) -> None:
        with self.lock:
            self.connection.execute(
                f"""
                INSERT INTO {REQUEST_LOG_TABLE} (
                    started_at,
                    finished_at,
                    client_id,
                    upstream_id,
                    request_trace_id,
                    attempt_index,
                    requested_model,
                    method,
                    path,
                    status_code,
                    forwarded,
                    request_bytes,
                    response_bytes,
                    upstream_ms,
                    error_kind,
                    terminal_outcome,
                    routing_snapshot_json,
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    cached_tokens
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _serialize_datetime(started_at),
                    _serialize_datetime(finished_at),
                    client_id,
                    upstream_id,
                    request_trace_id,
                    attempt_index,
                    requested_model,
                    method,
                    path,
                    status_code,
                    1 if forwarded else 0,
                    request_bytes,
                    response_bytes,
                    upstream_ms,
                    error_kind,
                    terminal_outcome,
                    routing_snapshot_json,
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    cached_tokens,
                ),
            )
            self.connection.commit()

    def finalize_request_trace(self, request_trace_id: str | None, terminal_outcome: str | None) -> None:
        if not request_trace_id or not terminal_outcome:
            return
        with self.lock:
            self.connection.execute(
                f"""
                UPDATE {REQUEST_LOG_TABLE}
                SET terminal_outcome = ?
                WHERE request_trace_id = ?
                """,
                (terminal_outcome, request_trace_id),
            )
            self.connection.commit()

    def request_trace(self, request_trace_id: str) -> dict[str, Any] | None:
        with self.lock:
            rows = self.connection.execute(
                f"""
                SELECT
                    id,
                    started_at,
                    finished_at,
                    client_id,
                    upstream_id,
                    request_trace_id,
                    attempt_index,
                    requested_model,
                    method,
                    path,
                    status_code,
                    forwarded,
                    request_bytes,
                    response_bytes,
                    upstream_ms,
                    error_kind,
                    terminal_outcome,
                    routing_snapshot_json
                FROM {REQUEST_LOG_TABLE}
                WHERE request_trace_id = ?
                ORDER BY
                    CASE WHEN attempt_index IS NULL THEN 1 ELSE 0 END ASC,
                    attempt_index ASC,
                    id ASC
                """,
                (request_trace_id,),
            ).fetchall()

        if not rows:
            return None

        first_row = rows[0]
        routing_snapshot = _parse_json_object(first_row["routing_snapshot_json"])
        configured_order = _string_list(routing_snapshot.get("configured_order"))
        effective_order = _string_list(routing_snapshot.get("effective_order"))
        order_pool = routing_snapshot.get("order_pool")
        if not isinstance(order_pool, list):
            order_pool = []
        normalized_order_pool: list[dict[str, Any]] = []
        for item in order_pool:
            if not isinstance(item, dict):
                continue
            normalized_item = {
                "pool_index": _coerce_optional_int(item.get("pool_index")),
                "upstream_id": str(item.get("upstream_id") or ""),
                "state": str(item.get("state") or ""),
            }
            if item.get("skip_reason"):
                normalized_item["skip_reason"] = str(item["skip_reason"])
            if item.get("attempt_index") is not None:
                normalized_item["attempt_index"] = _coerce_optional_int(item.get("attempt_index"))
            normalized_order_pool.append(normalized_item)
        normalized_order_pool.sort(
            key=lambda item: (
                item["pool_index"] if item["pool_index"] is not None else len(normalized_order_pool),
                item["upstream_id"],
            )
        )

        attempts: list[dict[str, Any]] = []
        started_candidates: list[str] = []
        finished_candidates: list[str] = []
        terminal_outcome = None
        for row in rows:
            started_candidates.append(str(row["started_at"]))
            finished_candidates.append(str(row["finished_at"]))
            if row["terminal_outcome"] and terminal_outcome is None:
                terminal_outcome = str(row["terminal_outcome"])
        for row in rows:
            if row["attempt_index"] is None:
                continue
            attempts.append(
                {
                    "attempt_index": int(row["attempt_index"]),
                    "upstream_id": row["upstream_id"],
                    "started_at": row["started_at"],
                    "started_at_local": _to_local_isoformat(row["started_at"]),
                    "finished_at": row["finished_at"],
                    "finished_at_local": _to_local_isoformat(row["finished_at"]),
                    "status_code": int(row["status_code"]),
                    "forwarded": bool(row["forwarded"]),
                    "request_bytes": int(row["request_bytes"]),
                    "response_bytes": int(row["response_bytes"]),
                    "upstream_ms": int(row["upstream_ms"]),
                    "error_kind": row["error_kind"],
                    "terminal_outcome": row["terminal_outcome"] or terminal_outcome,
                }
            )

        attempts.sort(key=lambda item: item["attempt_index"])
        planned_attempt_count = sum(1 for item in normalized_order_pool if item.get("state") == "attempted")
        attempt_count = len(attempts)
        terminal_row = attempts[-1] if attempts else None
        fallback_triggered = attempt_count > 1
        traversed_all_planned_attempts = attempt_count == planned_attempt_count
        terminal_status_code = (
            terminal_row["status_code"]
            if terminal_row is not None
            else int(first_row["status_code"])
        )
        terminal_error_kind = (
            terminal_row["error_kind"]
            if terminal_row is not None
            else first_row["error_kind"]
        )

        return {
            "request_trace_id": request_trace_id,
            "client_id": first_row["client_id"],
            "requested_model": first_row["requested_model"],
            "method": first_row["method"],
            "path": first_row["path"],
            "started_at": min(started_candidates) if started_candidates else None,
            "started_at_local": _to_local_isoformat(min(started_candidates) if started_candidates else None),
            "finished_at": max(finished_candidates) if finished_candidates else None,
            "finished_at_local": _to_local_isoformat(max(finished_candidates) if finished_candidates else None),
            "configured_order": configured_order,
            "effective_order": effective_order,
            "order_pool": normalized_order_pool,
            "planned_attempt_count": planned_attempt_count,
            "attempt_count": attempt_count,
            "fallback_triggered": fallback_triggered,
            "traversed_all_planned_attempts": traversed_all_planned_attempts,
            "terminal_outcome": terminal_outcome,
            "terminal_status_code": terminal_status_code,
            "terminal_error_kind": terminal_error_kind,
            "terminal_attempt_index": terminal_row["attempt_index"] if terminal_row is not None else None,
            "terminal_upstream_id": terminal_row["upstream_id"] if terminal_row is not None else None,
            "attempts": attempts,
        }

    def stats_summary(self, *, config: RelayConfig | None = None) -> dict[str, Any]:
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
            usage_rows = self.connection.execute(
                f"""
                SELECT
                    client_id,
                    upstream_id,
                    requested_model,
                    prompt_tokens,
                    completion_tokens,
                    total_tokens
                FROM {REQUEST_LOG_TABLE}
                WHERE upstream_id IS NOT NULL
                """
            ).fetchall()
            active_requests = self.active_requests
            active_requests_by_client = dict(self.active_requests_by_client)
            active_requests_by_upstream = dict(self.active_requests_by_upstream)
            active_requests_by_model = dict(self.active_requests_by_model)

        payload = {
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
        return _augment_stats_payload(
            payload,
            usage_rows=usage_rows,
            pricing=config.pricing if config is not None else None,
        )

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


@dataclass
class CircuitBreakerState:
    upstream_id: str
    state: str = "closed"
    consecutive_failures: int = 0
    priority_penalty: int = 0
    opened_at: str | None = None
    cooldown_until: str | None = None
    degraded_until: str | None = None
    last_failure_at: str | None = None
    last_failure_error_kind: str | None = None
    last_failure_status_code: int | None = None
    last_failure_reason: str | None = None
    last_success_at: str | None = None
    half_open_probe_in_flight: bool = False

    def priority_tier(self, now: datetime) -> str:
        if self.state == "open":
            cooldown_until = _parse_iso_datetime(self.cooldown_until)
            if cooldown_until is not None and now < cooldown_until:
                return "open"
        degraded_until = _parse_iso_datetime(self.degraded_until)
        if degraded_until is not None and now < degraded_until:
            return "degraded"
        if self.state == "half_open":
            return "probing"
        return "healthy"

    def recovery_at(self, now: datetime) -> str | None:
        cooldown_until = _parse_iso_datetime(self.cooldown_until)
        if cooldown_until is not None and now < cooldown_until:
            return self.cooldown_until
        degraded_until = _parse_iso_datetime(self.degraded_until)
        if degraded_until is not None and now < degraded_until:
            return self.degraded_until
        return None

    def routing_state(self, now: datetime) -> str:
        tier = self.priority_tier(now)
        return {"open": "cooldown"}.get(tier, tier)

    def as_dict(self, now: datetime) -> dict[str, Any]:
        return {
            "upstream_id": self.upstream_id,
            "state": self.state,
            "consecutive_failures": self.consecutive_failures,
            "priority_penalty": self.priority_penalty,
            "priority_tier": self.priority_tier(now),
            "routing_state": self.routing_state(now),
            "opened_at": self.opened_at,
            "cooldown_until": self.cooldown_until,
            "degraded_until": self.degraded_until,
            "recovery_at": self.recovery_at(now),
            "last_failure_at": self.last_failure_at,
            "last_failure_error_kind": self.last_failure_error_kind,
            "last_failure_status_code": self.last_failure_status_code,
            "last_failure_reason": self.last_failure_reason,
            "last_success_at": self.last_success_at,
            "half_open_probe_in_flight": self.half_open_probe_in_flight,
        }


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 3
    cooldown_seconds: int = 30
    recovery_window_seconds: float = 15.0
    max_recovery_window_seconds: float = 60.0


@dataclass
class CircuitBreakerProbeLease:
    manager: "CircuitBreakerManager"
    upstream_id: str
    half_open: bool = False
    _closed: bool = False

    def mark_success(self) -> None:
        if self._closed:
            return
        self.manager.record_success(self.upstream_id, was_half_open=self.half_open)
        self._closed = True

    def mark_failure(
        self,
        *,
        error_kind: str | None = None,
        status_code: int | None = None,
        reason: str | None = None,
    ) -> None:
        if self._closed:
            return
        self.manager.record_failure(
            self.upstream_id,
            was_half_open=self.half_open,
            error_kind=error_kind,
            status_code=status_code,
            reason=reason,
        )
        self._closed = True

    def mark_soft_failure(
        self,
        *,
        error_kind: str | None = None,
        status_code: int | None = None,
        reason: str | None = None,
    ) -> None:
        if self._closed:
            return
        self.manager.record_soft_failure(
            self.upstream_id,
            was_half_open=self.half_open,
            error_kind=error_kind,
            status_code=status_code,
            reason=reason,
        )
        self._closed = True

    def close(self) -> None:
        if self._closed:
            return
        self.manager.release_probe(self.upstream_id, was_half_open=self.half_open)
        self._closed = True


class CircuitBreakerManager:
    def __init__(self, upstream_ids: list[str], config: CircuitBreakerConfig | None = None) -> None:
        self._lock = threading.Lock()
        self._config = config or CircuitBreakerConfig()
        self._states = {upstream_id: CircuitBreakerState(upstream_id=upstream_id) for upstream_id in upstream_ids}

    def sync_upstreams(self, upstream_ids: list[str]) -> None:
        with self._lock:
            existing = set(self._states)
            for upstream_id in upstream_ids:
                if upstream_id not in self._states:
                    self._states[upstream_id] = CircuitBreakerState(upstream_id=upstream_id)
            for upstream_id in existing - set(upstream_ids):
                self._states.pop(upstream_id, None)

    def permits_request(self, upstream_id: str) -> bool:
        now = datetime.now(timezone.utc)
        with self._lock:
            state = self._states.setdefault(upstream_id, CircuitBreakerState(upstream_id=upstream_id))
            if state.state != "open":
                return True
            cooldown_until = _parse_iso_datetime(state.cooldown_until)
            return cooldown_until is None or now >= cooldown_until

    def prioritized_upstream_ids(self, ordered_upstream_ids: list[str]) -> list[str]:
        now = datetime.now(timezone.utc)
        with self._lock:
            seen: set[str] = set()
            ranked: list[tuple[tuple[int, int, int], str]] = []
            for index, upstream_id in enumerate(ordered_upstream_ids):
                if upstream_id in seen:
                    continue
                seen.add(upstream_id)
                state = self._states.setdefault(upstream_id, CircuitBreakerState(upstream_id=upstream_id))
                tier = state.priority_tier(now)
                tier_rank = {"healthy": 0, "probing": 0, "degraded": 1, "open": 2}.get(tier, 1)
                penalty = state.priority_penalty if tier == "degraded" else 0
                ranked.append(((tier_rank, penalty, index), upstream_id))
        ranked.sort(key=lambda item: item[0])
        return [upstream_id for _, upstream_id in ranked]

    def acquire(self, upstream_id: str) -> CircuitBreakerProbeLease | None:
        now = datetime.now(timezone.utc)
        with self._lock:
            state = self._states.setdefault(upstream_id, CircuitBreakerState(upstream_id=upstream_id))
            if state.state == "open":
                cooldown_until = _parse_iso_datetime(state.cooldown_until)
                if cooldown_until is not None and now < cooldown_until:
                    return None
                if state.half_open_probe_in_flight:
                    return None
                state.state = "half_open"
                state.half_open_probe_in_flight = True
                return CircuitBreakerProbeLease(manager=self, upstream_id=upstream_id, half_open=True)
            if state.state == "half_open" and state.half_open_probe_in_flight:
                return None
            return CircuitBreakerProbeLease(manager=self, upstream_id=upstream_id, half_open=False)

    def record_success(self, upstream_id: str, *, was_half_open: bool) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            state = self._states.setdefault(upstream_id, CircuitBreakerState(upstream_id=upstream_id))
            state.consecutive_failures = 0
            state.priority_penalty = 0
            state.state = "closed"
            state.opened_at = None
            state.cooldown_until = None
            state.degraded_until = None
            state.last_success_at = now.isoformat()
            state.half_open_probe_in_flight = False

    def record_failure(
        self,
        upstream_id: str,
        *,
        was_half_open: bool,
        error_kind: str | None = None,
        status_code: int | None = None,
        reason: str | None = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            state = self._states.setdefault(upstream_id, CircuitBreakerState(upstream_id=upstream_id))
            state.half_open_probe_in_flight = False
            state.last_failure_at = now.isoformat()
            state.last_failure_error_kind = error_kind
            state.last_failure_status_code = status_code
            state.last_failure_reason = reason
            state.priority_penalty = self._next_priority_penalty(state, now)
            state.degraded_until = self._priority_recovery_deadline(now, state.priority_penalty)
            if was_half_open:
                state.consecutive_failures = self._config.failure_threshold
            else:
                state.consecutive_failures += 1
            if state.consecutive_failures >= self._config.failure_threshold:
                state.state = "open"
                state.opened_at = now.isoformat()
                state.cooldown_until = (now + timedelta(seconds=max(self._config.cooldown_seconds, 1))).isoformat()
                state.degraded_until = state.cooldown_until
            else:
                state.state = "closed"

    def record_soft_failure(
        self,
        upstream_id: str,
        *,
        was_half_open: bool,
        error_kind: str | None = None,
        status_code: int | None = None,
        reason: str | None = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            state = self._states.setdefault(upstream_id, CircuitBreakerState(upstream_id=upstream_id))
            state.half_open_probe_in_flight = False
            state.last_failure_at = now.isoformat()
            state.last_failure_error_kind = error_kind
            state.last_failure_status_code = status_code
            state.last_failure_reason = reason
            state.priority_penalty = self._next_priority_penalty(state, now)
            state.degraded_until = self._priority_recovery_deadline(now, state.priority_penalty)
            state.consecutive_failures = 0
            state.state = "closed"
            state.opened_at = None
            state.cooldown_until = None

    def release_probe(self, upstream_id: str, *, was_half_open: bool) -> None:
        if not was_half_open:
            return
        with self._lock:
            state = self._states.setdefault(upstream_id, CircuitBreakerState(upstream_id=upstream_id))
            if state.state == "half_open":
                state.half_open_probe_in_flight = False
                state.state = "open"

    def snapshot(self) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc)
        with self._lock:
            return [self._states[upstream_id].as_dict(now) for upstream_id in sorted(self._states)]

    def _next_priority_penalty(self, state: CircuitBreakerState, now: datetime) -> int:
        degraded_until = _parse_iso_datetime(state.degraded_until)
        active_penalty = state.priority_penalty if degraded_until is not None and now < degraded_until else 0
        return max(1, min(active_penalty + 1, self._config.failure_threshold))

    def _priority_recovery_deadline(self, now: datetime, penalty: int) -> str | None:
        base_window = max(float(self._config.recovery_window_seconds), 0.0)
        if base_window <= 0:
            return None
        capped_seconds = min(
            max(base_window * max(penalty, 1), base_window),
            max(float(self._config.max_recovery_window_seconds), base_window),
        )
        return (now + timedelta(seconds=capped_seconds)).isoformat()


@dataclass
class RelayRuntime:
    config: RelayConfig
    usage_store: UsageStore
    config_version: str
    close_usage_store_when_drained: bool
    circuit_breaker: CircuitBreakerManager
    active_requests: int = 0


def build_upstream_status_view(config: RelayConfig, circuit_breaker: CircuitBreakerManager) -> dict[str, Any]:
    breaker_snapshot = {item["upstream_id"]: item for item in circuit_breaker.snapshot()}
    upstreams_by_id = config.upstreams_by_id
    configured_order = list(config.order)
    effective_order = [
        upstream_id
        for upstream_id in circuit_breaker.prioritized_upstream_ids(config.order)
        if upstreams_by_id[upstream_id].enabled
    ]
    configured_index = {upstream_id: index for index, upstream_id in enumerate(configured_order)}
    effective_index = {upstream_id: index for index, upstream_id in enumerate(effective_order)}
    upstreams: list[dict[str, Any]] = []
    for upstream in config.upstreams:
        breaker_state = breaker_snapshot.get(upstream.upstream_id, {})
        last_failure: dict[str, Any] | None = None
        if breaker_state.get("last_failure_at"):
            last_failure = {
                "at": breaker_state["last_failure_at"],
                "error_kind": breaker_state.get("last_failure_error_kind"),
                "reason": breaker_state.get("last_failure_reason"),
            }
            if breaker_state.get("last_failure_status_code") is not None:
                last_failure["status_code"] = breaker_state["last_failure_status_code"]
            last_failure = {key: value for key, value in last_failure.items() if value is not None}
        routing_state = "disabled" if not upstream.enabled else breaker_state.get("routing_state", "healthy")
        cooldown_active = upstream.enabled and routing_state == "cooldown"
        degraded_active = upstream.enabled and routing_state == "degraded"
        upstreams.append(
            {
                "upstream_id": upstream.upstream_id,
                "enabled": upstream.enabled,
                "configured_index": configured_index.get(upstream.upstream_id),
                "effective_index": effective_index.get(upstream.upstream_id) if upstream.enabled else None,
                "breaker_state": breaker_state.get("state", "closed"),
                "routing_state": routing_state,
                "consecutive_failures": breaker_state.get("consecutive_failures", 0),
                "priority_penalty": breaker_state.get("priority_penalty", 0),
                "cooldown_active": cooldown_active,
                "degraded_active": degraded_active,
                "opened_at": breaker_state.get("opened_at"),
                "cooldown_until": breaker_state.get("cooldown_until"),
                "degraded_until": breaker_state.get("degraded_until"),
                "recovery_at": breaker_state.get("recovery_at"),
                "last_failure": last_failure,
                "last_success_at": breaker_state.get("last_success_at"),
                "half_open_probe_in_flight": breaker_state.get("half_open_probe_in_flight", False),
            }
        )
    upstreams.sort(
        key=lambda item: (
            0 if item["enabled"] and item["effective_index"] is not None else 1,
            item["effective_index"]
            if item["effective_index"] is not None
            else (item["configured_index"] if item["configured_index"] is not None else len(configured_order)),
            item["configured_index"] if item["configured_index"] is not None else len(configured_order),
            item["upstream_id"],
        )
    )
    return {
        "configured_order": configured_order,
        "effective_order": effective_order,
        "upstreams": upstreams,
    }


@dataclass
class ReloadState:
    config_path: Path
    config_version: str
    reload_enabled: bool
    reload_poll_interval_seconds: float
    last_reload_at: str | None = None
    last_reload_status: str = "never"
    last_reload_error: str | None = None
    last_successful_reload_at: str | None = None

    def as_dict(self, *, draining_runtimes: int) -> dict[str, Any]:
        return {
            "config_path": str(self.config_path),
            "config_version": self.config_version,
            "reload_enabled": self.reload_enabled,
            "reload_poll_interval_seconds": self.reload_poll_interval_seconds,
            "last_reload_at": self.last_reload_at,
            "last_reload_status": self.last_reload_status,
            "last_reload_error": self.last_reload_error,
            "last_successful_reload_at": self.last_successful_reload_at,
            "draining_runtimes": draining_runtimes,
        }


class RuntimeLease:
    def __init__(self, manager: "RelayRuntimeManager", runtime: RelayRuntime) -> None:
        self._manager = manager
        self._runtime = runtime
        self._closed = False

    @property
    def config(self) -> RelayConfig:
        return self._runtime.config

    @property
    def usage_store(self) -> UsageStore:
        return self._runtime.usage_store

    @property
    def config_version(self) -> str:
        return self._runtime.config_version

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._manager.release_runtime(self._runtime)


class RelayRuntimeManager:
    def __init__(
        self,
        *,
        config_path: Path,
        active_runtime: RelayRuntime,
        reload_poll_interval_seconds: float,
    ) -> None:
        self._lock = threading.Lock()
        self._active_runtime = active_runtime
        self._draining_runtimes: list[RelayRuntime] = []
        self._reload_state = ReloadState(
            config_path=config_path,
            config_version=active_runtime.config_version,
            reload_enabled=True,
            reload_poll_interval_seconds=reload_poll_interval_seconds,
        )

    def acquire_runtime(self) -> RuntimeLease:
        with self._lock:
            self._active_runtime.active_requests += 1
            return RuntimeLease(self, self._active_runtime)

    def release_runtime(self, runtime: RelayRuntime) -> None:
        stores_to_close: list[UsageStore] = []
        with self._lock:
            runtime.active_requests = max(0, runtime.active_requests - 1)
            survivors: list[RelayRuntime] = []
            for retired in self._draining_runtimes:
                if retired.active_requests == 0:
                    if retired.close_usage_store_when_drained:
                        stores_to_close.append(retired.usage_store)
                    continue
                survivors.append(retired)
            self._draining_runtimes = survivors
        for store in stores_to_close:
            store.close()

    def reload_state_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return self._reload_state.as_dict(draining_runtimes=len(self._draining_runtimes))

    def active_runtime(self) -> RelayRuntime:
        with self._lock:
            return self._active_runtime

    def wait_for_runtime_drained(self, runtime: RelayRuntime, poll_interval_seconds: float = 0.01) -> None:
        while True:
            with self._lock:
                if runtime.active_requests == 0:
                    return
            time.sleep(poll_interval_seconds)

    def swap_runtime(self, runtime: RelayRuntime, *, close_retired_usage_store: bool = False) -> None:
        stores_to_close: list[UsageStore] = []
        with self._lock:
            retired = self._active_runtime
            retired.close_usage_store_when_drained = retired.close_usage_store_when_drained or close_retired_usage_store
            self._active_runtime = runtime
            self._reload_state.config_version = runtime.config_version
            self._reload_state.last_reload_at = datetime.now(timezone.utc).isoformat()
            self._reload_state.last_reload_status = "success"
            self._reload_state.last_reload_error = None
            self._reload_state.last_successful_reload_at = self._reload_state.last_reload_at
            if retired is not runtime:
                if retired.active_requests == 0:
                    if retired.close_usage_store_when_drained:
                        stores_to_close.append(retired.usage_store)
                else:
                    self._draining_runtimes.append(retired)
        for store in stores_to_close:
            store.close()

    def record_reload_error(self, message: str) -> None:
        with self._lock:
            self._reload_state.last_reload_at = datetime.now(timezone.utc).isoformat()
            self._reload_state.last_reload_status = "error"
            self._reload_state.last_reload_error = message


class RelayHTTPServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        runtime_manager: RelayRuntimeManager,
    ) -> None:
        super().__init__(server_address, handler_class)
        self.runtime_manager = runtime_manager


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
    def runtime_manager(self) -> RelayRuntimeManager:
        return self.server.runtime_manager  # type: ignore[attr-defined]

    def _handle(self) -> None:
        runtime = self.runtime_manager.acquire_runtime()
        try:
            if self.path.startswith("/_relay/"):
                self._handle_admin(runtime)
                return
            self._handle_proxy(runtime)
        finally:
            runtime.close()

    def _handle_admin(self, runtime: RuntimeLease) -> None:
        parsed_path = urlsplit(self.path)
        if runtime.config.admin_key and self.headers.get("X-Relay-Admin-Key", "") != runtime.config.admin_key:
            self._send_json(403, {"error": {"message": "forbidden"}})
            return
        if parsed_path.path == "/_relay/health":
            self._send_json(
                200,
                {
                    "status": "ok",
                    "server": {
                        "listen_host": runtime.config.listen_host,
                        "listen_port": runtime.config.listen_port,
                        "database_path": str(runtime.config.database_path),
                        "idle_window_seconds": runtime.config.idle_window_seconds,
                        "request_timeout_seconds": runtime.config.request_timeout_seconds,
                    },
                    "local": {
                        "clients": sorted(client.client_id for client in runtime.config.local_clients),
                    },
                    "upstreams": [
                        {
                            "upstream_id": upstream.upstream_id,
                            "base_url": upstream.base_url,
                            "enabled": upstream.enabled,
                            "transport": upstream.transport,
                        }
                        for upstream in sorted(runtime.config.upstreams, key=lambda item: item.upstream_id)
                    ],
                    "order": list(runtime.config.order),
                    "breaker": {
                        "upstreams": runtime._runtime.circuit_breaker.snapshot(),
                    },
                    "upstream_status": build_upstream_status_view(runtime.config, runtime._runtime.circuit_breaker),
                    "reload": self.runtime_manager.reload_state_snapshot(),
                },
            )
            return
        if parsed_path.path == "/_relay/stats":
            self._send_json(200, runtime.usage_store.stats_summary(config=runtime.config))
            return
        if parsed_path.path == "/_relay/idle":
            self._send_json(200, runtime.usage_store.idle_status(runtime.config.idle_window_seconds))
            return
        if parsed_path.path == "/_relay/request-traces":
            request_trace_id = (parse_qs(parsed_path.query).get("request_trace_id") or [""])[0].strip()
            if not request_trace_id:
                self._send_json(400, {"error": {"message": "request_trace_id is required"}})
                return
            trace = runtime.usage_store.request_trace(request_trace_id)
            if trace is None:
                self._send_json(404, {"error": {"message": "request trace not found"}})
                return
            self._send_json(200, trace)
            return
        self._send_json(404, {"error": {"message": "admin endpoint not found"}})

    def _handle_proxy(self, runtime: RuntimeLease) -> None:
        request_trace_id = _new_request_trace_id()
        self._current_request_trace_id = request_trace_id
        original_request_body = self._read_request_body()
        plan = plan_request_attempts(
            runtime.config,
            self.headers,
            original_request_body,
            runtime._runtime.circuit_breaker,
        )
        routing_snapshot_json = _serialize_json(plan.routing_snapshot())
        client_id = plan.client.client_id if plan.client is not None else None
        initial_upstream_id = plan.attempts[0].upstream.upstream_id if plan.attempts else None
        track_request_state = plan.error_status is None
        if track_request_state:
            runtime.usage_store.request_started(client_id, initial_upstream_id, plan.requested_model)
        terminal_outcome: str | None = None
        try:
            if plan.error_status is not None:
                started_at = datetime.now(timezone.utc)
                response_bytes = self._send_json(
                    plan.error_status,
                    {"error": {"message": plan.error_message or "relay request rejected"}},
                )
                finished_at = datetime.now(timezone.utc)
                runtime.usage_store.record_request(
                    started_at=started_at,
                    finished_at=finished_at,
                    client_id=client_id,
                    upstream_id=None,
                    request_trace_id=request_trace_id,
                    attempt_index=None,
                    requested_model=plan.requested_model,
                    method=self.command,
                    path=self.path,
                    status_code=plan.error_status,
                    forwarded=False,
                    request_bytes=len(original_request_body),
                    response_bytes=response_bytes,
                    upstream_ms=_duration_ms(started_at, finished_at),
                    error_kind=plan.error_kind,
                    terminal_outcome=plan.error_kind,
                    routing_snapshot_json=routing_snapshot_json,
                    prompt_tokens=None,
                    completion_tokens=None,
                    total_tokens=None,
                    cached_tokens=None,
                )
                terminal_outcome = plan.error_kind
                return

            attempt_failures: list[UpstreamAttemptFailure] = []
            for index, attempt in enumerate(plan.attempts):
                attempt_upstream_id = attempt.upstream.upstream_id
                breaker_lease = runtime._runtime.circuit_breaker.acquire(attempt.upstream.upstream_id)
                if breaker_lease is None:
                    attempt_started_at = datetime.now(timezone.utc)
                    response_bytes = 0
                    if index + 1 >= len(plan.attempts):
                        response_bytes = self._send_json(503, {"error": {"message": "all upstreams temporarily unavailable"}})
                        terminal_outcome = "all_upstreams_temporarily_unavailable"
                    finished_at = datetime.now(timezone.utc)
                    runtime.usage_store.record_request(
                        started_at=attempt_started_at,
                        finished_at=finished_at,
                        client_id=client_id,
                        upstream_id=attempt_upstream_id,
                        request_trace_id=request_trace_id,
                        attempt_index=index,
                        requested_model=plan.requested_model,
                        method=self.command,
                        path=self.path,
                        status_code=503,
                        forwarded=False,
                        request_bytes=len(attempt.request_body),
                        response_bytes=response_bytes,
                        upstream_ms=_duration_ms(attempt_started_at, finished_at),
                        error_kind="upstream_temporarily_unavailable",
                        terminal_outcome=terminal_outcome,
                        routing_snapshot_json=routing_snapshot_json,
                        prompt_tokens=None,
                        completion_tokens=None,
                        total_tokens=None,
                        cached_tokens=None,
                    )
                    if index + 1 < len(plan.attempts):
                        continue
                    runtime.usage_store.finalize_request_trace(request_trace_id, terminal_outcome)
                    return
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
                        timeout=attempt.upstream.timeout_seconds(runtime.config.request_timeout_seconds),
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
                            response_bytes = self._relay_upstream_upgrade(
                                connection,
                                response,
                                runtime.config.request_timeout_seconds,
                            )
                            captured_body = b""
                        else:
                            response_bytes, captured_body = self._relay_upstream_response(response)
                        finished_at = datetime.now(timezone.utc)
                        terminal_outcome = "success" if index == 0 else "fallback_success"
                        prompt_tokens, completion_tokens, total_tokens, cached_tokens = _extract_usage_metrics(
                            response.getheader("Content-Type", ""),
                            captured_body,
                        )
                        runtime.usage_store.record_request(
                            started_at=attempt_started_at,
                            finished_at=finished_at,
                            client_id=client_id,
                            upstream_id=attempt.upstream.upstream_id,
                            request_trace_id=request_trace_id,
                            attempt_index=index,
                            requested_model=plan.requested_model,
                            method=self.command,
                            path=self.path,
                            status_code=int(response.status),
                            forwarded=True,
                            request_bytes=len(attempt.request_body),
                            response_bytes=response_bytes,
                            upstream_ms=_duration_ms(attempt_started_at, finished_at),
                            error_kind=None,
                            terminal_outcome=terminal_outcome,
                            routing_snapshot_json=routing_snapshot_json,
                            prompt_tokens=prompt_tokens,
                            completion_tokens=completion_tokens,
                            total_tokens=total_tokens,
                            cached_tokens=cached_tokens,
                        )
                        breaker_lease.mark_success()
                        runtime.usage_store.finalize_request_trace(request_trace_id, terminal_outcome)
                        return

                    response_bytes, captured_body = _read_full_response_body(response)
                    finished_at = datetime.now(timezone.utc)
                    prompt_tokens, completion_tokens, total_tokens, cached_tokens = _extract_usage_metrics(
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
                    failure_reason = _extract_error_message(response.getheader("Content-Type", ""), captured_body) or response.reason
                    attempt_failures.append(
                        UpstreamAttemptFailure(
                            upstream_id=attempt.upstream.upstream_id,
                            error_kind="upstream_status",
                            status_code=int(response.status),
                            reason=response.reason,
                            message=failure_reason,
                        )
                    )
                    runtime.usage_store.record_request(
                        started_at=attempt_started_at,
                        finished_at=finished_at,
                        client_id=client_id,
                        upstream_id=attempt.upstream.upstream_id,
                        request_trace_id=request_trace_id,
                        attempt_index=index,
                        requested_model=plan.requested_model,
                        method=self.command,
                        path=self.path,
                        status_code=int(response.status),
                        forwarded=True,
                        request_bytes=len(attempt.request_body),
                        response_bytes=response_bytes,
                        upstream_ms=_duration_ms(attempt_started_at, finished_at),
                        error_kind="upstream_status_fallback" if can_retry else "upstream_status_exhausted",
                        terminal_outcome=None if can_retry else "fallback_exhausted",
                        routing_snapshot_json=routing_snapshot_json,
                        prompt_tokens=prompt_tokens,
                        completion_tokens=completion_tokens,
                        total_tokens=total_tokens,
                        cached_tokens=cached_tokens,
                    )
                    if _should_trip_circuit_breaker(int(response.status)):
                        breaker_lease.mark_failure(
                            error_kind="upstream_status",
                            status_code=int(response.status),
                            reason=failure_reason,
                        )
                    else:
                        breaker_lease.mark_soft_failure(
                            error_kind="upstream_status",
                            status_code=int(response.status),
                            reason=failure_reason,
                        )
                    if can_retry:
                        continue
                    break
                except (OSError, TimeoutError, http.client.HTTPException) as exc:
                    finished_at = datetime.now(timezone.utc)
                    runtime.usage_store.record_request(
                        started_at=attempt_started_at,
                        finished_at=finished_at,
                        client_id=client_id,
                        upstream_id=attempt.upstream.upstream_id,
                        request_trace_id=request_trace_id,
                        attempt_index=index,
                        requested_model=plan.requested_model,
                        method=self.command,
                        path=self.path,
                        status_code=502,
                        forwarded=False,
                        request_bytes=len(attempt.request_body),
                        response_bytes=0,
                        upstream_ms=_duration_ms(attempt_started_at, finished_at),
                        error_kind="upstream_request_failed",
                        terminal_outcome=None if index + 1 < len(plan.attempts) else "fallback_exhausted",
                        routing_snapshot_json=routing_snapshot_json,
                        prompt_tokens=None,
                        completion_tokens=None,
                        total_tokens=None,
                        cached_tokens=None,
                    )
                    attempt_failures.append(
                        UpstreamAttemptFailure(
                            upstream_id=attempt.upstream.upstream_id,
                            error_kind="upstream_request_failed",
                            message=str(exc) or "upstream request failed",
                        )
                    )
                    breaker_lease.mark_failure(
                        error_kind="upstream_request_failed",
                        reason=str(exc) or "upstream request failed",
                    )
                    if index + 1 < len(plan.attempts):
                        continue
                    break
                finally:
                    if connection is not None:
                        connection.close()

            if attempt_failures:
                terminal_outcome = "fallback_exhausted"
                runtime.usage_store.finalize_request_trace(request_trace_id, terminal_outcome)
                self._send_exhausted_attempts(attempt_failures)
                return
            terminal_outcome = "upstream_request_failed"
            runtime.usage_store.finalize_request_trace(request_trace_id, terminal_outcome)
            self._send_json(502, {"error": {"message": "upstream request failed"}})
        finally:
            if track_request_state:
                runtime.usage_store.request_finished(client_id, initial_upstream_id, plan.requested_model)
            runtime.usage_store.finalize_request_trace(request_trace_id, terminal_outcome)

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
        self._send_request_trace_header()
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
        request_timeout_seconds: int,
    ) -> int:
        upstream_headers = response.getheaders()
        self.send_response(response.status, response.reason)
        for key, value in upstream_headers:
            self.send_header(key, value)
        self._send_request_trace_header()
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
                request_timeout_seconds,
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
        self._send_request_trace_header()
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
        self._send_request_trace_header()
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)
            self.wfile.flush()
        return len(body)

    def _send_exhausted_attempts(self, failures: list[UpstreamAttemptFailure]) -> int:
        return self._send_json(
            502,
            {
                "error": {
                    "type": "upstream_fallback_exhausted",
                    "message": "all upstream attempts failed",
                    "attempts": [failure.as_dict() for failure in failures],
                }
            },
        )

    def _send_request_trace_header(self) -> None:
        request_trace_id = getattr(self, "_current_request_trace_id", None)
        if request_trace_id:
            self.send_header("X-Relay-Request-Trace-Id", request_trace_id)


class RelayListener:
    def __init__(self, config: RelayConfig, runtime_manager: RelayRuntimeManager) -> None:
        self.server = RelayHTTPServer((config.listen_host, config.listen_port), RelayRequestHandler, runtime_manager)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class RelayService:
    def __init__(self, config_path: str | Path, reload_poll_interval_seconds: float = 1.0) -> None:
        self.config_path = Path(config_path)
        self.reload_poll_interval_seconds = reload_poll_interval_seconds
        config = RelayConfig.load(self.config_path)
        config_bytes = self.config_path.read_bytes()
        usage_store = UsageStore(config.database_path)
        active_runtime = RelayRuntime(
            config=config,
            usage_store=usage_store,
            config_version=_hash_config_bytes(config_bytes),
            close_usage_store_when_drained=False,
            circuit_breaker=CircuitBreakerManager([upstream.upstream_id for upstream in config.upstreams]),
        )
        self.runtime_manager = RelayRuntimeManager(
            config_path=self.config_path,
            active_runtime=active_runtime,
            reload_poll_interval_seconds=reload_poll_interval_seconds,
        )
        self.listener = RelayListener(config, self.runtime_manager)
        self._stop_event = threading.Event()
        self._watcher_thread = threading.Thread(target=self._watch_config_loop, daemon=True)
        self._active_signature = (self.config_path.stat().st_mtime_ns, active_runtime.config_version)
        self._pending_signature: tuple[int, str] | None = None

    def serve_forever(self) -> None:
        self.listener.start()
        self._watcher_thread.start()
        try:
            while not self._stop_event.wait(0.1):
                pass
        finally:
            self.listener.stop()
            active_runtime = self.runtime_manager.active_runtime()
            self.runtime_manager.wait_for_runtime_drained(active_runtime)
            active_runtime.usage_store.close()

    def shutdown(self) -> None:
        self._stop_event.set()
        if self._watcher_thread.is_alive():
            self._watcher_thread.join(timeout=2)

    def _watch_config_loop(self) -> None:
        while not self._stop_event.wait(self.reload_poll_interval_seconds):
            self._maybe_reload()

    def _reload_database_runtime(self, new_config: RelayConfig, config_version: str) -> None:
        new_store = UsageStore(new_config.database_path)
        new_runtime = RelayRuntime(
            config=new_config,
            usage_store=new_store,
            config_version=config_version,
            close_usage_store_when_drained=True,
            circuit_breaker=CircuitBreakerManager([upstream.upstream_id for upstream in new_config.upstreams]),
        )
        self.runtime_manager.swap_runtime(new_runtime, close_retired_usage_store=True)

    def _reload_listener_runtime(self, new_config: RelayConfig, config_version: str) -> None:
        current_listener = self.listener
        current_runtime = self.runtime_manager.active_runtime()
        new_store = (
            UsageStore(new_config.database_path)
            if Path(new_config.database_path) != Path(current_runtime.config.database_path)
            else current_runtime.usage_store
        )
        close_retired_usage_store = new_store is not current_runtime.usage_store
        new_runtime = RelayRuntime(
            config=new_config,
            usage_store=new_store,
            config_version=config_version,
            close_usage_store_when_drained=close_retired_usage_store,
            circuit_breaker=current_runtime.circuit_breaker,
        )
        new_runtime.circuit_breaker.sync_upstreams([upstream.upstream_id for upstream in new_config.upstreams])
        try:
            new_listener = RelayListener(new_config, self.runtime_manager)
        except Exception:
            if close_retired_usage_store:
                new_store.close()
            raise
        try:
            new_listener.start()
        except Exception:
            if close_retired_usage_store:
                new_store.close()
            raise
        self.listener = new_listener
        self.runtime_manager.swap_runtime(
            new_runtime,
            close_retired_usage_store=close_retired_usage_store,
        )
        current_listener.stop()

    def _maybe_reload(self) -> None:
        try:
            raw_bytes = self.config_path.read_bytes()
            signature = (self.config_path.stat().st_mtime_ns, _hash_config_bytes(raw_bytes))
            if signature == self._active_signature:
                self._pending_signature = None
                return
            if signature != self._pending_signature:
                self._pending_signature = signature
                return
            self._pending_signature = None
            new_config = RelayConfig.load(self.config_path)
            current_runtime = self.runtime_manager.active_runtime()
            current_config = current_runtime.config
            if (
                new_config.listen_host != current_config.listen_host
                or new_config.listen_port != current_config.listen_port
            ):
                self._reload_listener_runtime(new_config, signature[1])
                self._active_signature = signature
                return
            if Path(new_config.database_path) != Path(current_config.database_path):
                self._reload_database_runtime(new_config, signature[1])
                self._active_signature = signature
                return
            if (
                new_config.listen_host == current_config.listen_host
                and new_config.listen_port == current_config.listen_port
                and Path(new_config.database_path) == Path(current_config.database_path)
            ):
                self.runtime_manager.swap_runtime(
                    RelayRuntime(
                        config=new_config,
                        usage_store=current_runtime.usage_store,
                        config_version=signature[1],
                        close_usage_store_when_drained=False,
                        circuit_breaker=current_runtime.circuit_breaker,
                    )
                )
                self.runtime_manager.active_runtime().circuit_breaker.sync_upstreams(
                    [upstream.upstream_id for upstream in new_config.upstreams]
                )
                self._active_signature = signature
                return
            raise RuntimeError("runtime-only reload path received infrastructure change")
        except Exception as exc:
            self.runtime_manager.record_reload_error(str(exc))


def plan_request_attempts(
    config: RelayConfig,
    headers: Any,
    request_body: bytes,
    circuit_breaker: CircuitBreakerManager | None = None,
) -> RequestPlan:
    local_key = _extract_local_key(headers)
    client = config.local_clients_by_key.get(local_key or "")
    _, requested_model = _parse_request_body(request_body)
    configured_order = list(config.order)
    ordered_upstream_ids = (
        circuit_breaker.prioritized_upstream_ids(config.order)
        if circuit_breaker is not None
        else list(config.order)
    )
    if client is None:
        return RequestPlan(
            client=None,
            requested_model=requested_model,
            attempts=[],
            error_status=401,
            error_kind="invalid_local_key",
            error_message="invalid local api key",
            configured_order=configured_order,
            effective_order=list(ordered_upstream_ids),
        )

    attempts: list[PlannedUpstreamRequest] = []
    order_pool: list[dict[str, Any]] = []
    skipped_breaker_upstream_ids: list[str] = []
    seen_upstream_ids: set[str] = set()
    effective_order: list[str] = []
    for pool_index, upstream_id in enumerate(ordered_upstream_ids):
        if not upstream_id or upstream_id in seen_upstream_ids:
            continue
        seen_upstream_ids.add(upstream_id)
        effective_order.append(upstream_id)
        upstream = config.upstreams_by_id[upstream_id]
        if not upstream.enabled:
            order_pool.append(
                {
                    "pool_index": pool_index,
                    "upstream_id": upstream_id,
                    "state": "skipped",
                    "skip_reason": "disabled",
                }
            )
            continue
        if circuit_breaker is not None and not circuit_breaker.permits_request(upstream_id):
            skipped_breaker_upstream_ids.append(upstream_id)
            order_pool.append(
                {
                    "pool_index": pool_index,
                    "upstream_id": upstream_id,
                    "state": "skipped",
                    "skip_reason": "breaker_open",
                }
            )
            continue
        attempt_index = len(attempts)
        attempts.append(
            PlannedUpstreamRequest(
                upstream=upstream,
                request_body=request_body,
            )
        )
        order_pool.append(
            {
                "pool_index": pool_index,
                "upstream_id": upstream_id,
                "state": "attempted",
                "attempt_index": attempt_index,
            }
        )

    if attempts:
        return RequestPlan(
            client=client,
            requested_model=requested_model,
            attempts=attempts,
            configured_order=configured_order,
            effective_order=effective_order,
            order_pool=order_pool,
        )

    if skipped_breaker_upstream_ids:
        return RequestPlan(
            client=client,
            requested_model=requested_model,
            attempts=[],
            error_status=503,
            error_kind="all_upstreams_open",
            error_message="all upstreams temporarily unavailable",
            configured_order=configured_order,
            effective_order=effective_order,
            order_pool=order_pool,
        )

    return RequestPlan(
        client=client,
        requested_model=requested_model,
        attempts=[],
        error_status=502,
        error_kind="no_enabled_upstreams",
        error_message="no enabled upstreams configured",
        configured_order=configured_order,
        effective_order=effective_order,
        order_pool=order_pool,
    )


def _should_retry_with_fallback(
    status_code: int,
    content_type: str = "",
    response_body: bytes = b"",
    requested_model: str | None = None,
) -> bool:
    del content_type, response_body, requested_model
    return status_code >= 400


def _should_trip_circuit_breaker(status_code: int) -> bool:
    return status_code >= 500 or status_code in {408, 409, 425, 429}


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


def _new_request_trace_id() -> str:
    return uuid.uuid4().hex


def _serialize_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _parse_json_object(value: Any) -> dict[str, Any]:
    if not value or not isinstance(value, str):
        return {}
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, str)]


def _duration_ms(started_at: datetime, finished_at: datetime) -> int:
    return max(0, int((finished_at - started_at).total_seconds() * 1000))


def _serialize_datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def _to_local_isoformat(value: str | None) -> str | None:
    if not value:
        return None
    return datetime.fromisoformat(value).astimezone(LOCAL_TIMEZONE).isoformat(timespec="seconds")


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


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
    if base_path and (
        incoming_only_path == base_path or incoming_only_path.startswith(f"{base_path}/")
    ):
        combined_path = incoming_only_path
    else:
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


def _extract_usage_metrics(content_type: str, body: bytes) -> tuple[int | None, int | None, int | None, int | None]:
    usage = _extract_usage_payload(content_type, body)
    if not isinstance(usage, dict):
        return None, None, None, None
    prompt_tokens = _coerce_optional_int(usage.get("prompt_tokens"))
    if prompt_tokens is None:
        prompt_tokens = _coerce_optional_int(usage.get("input_tokens"))
    completion_tokens = _coerce_optional_int(usage.get("completion_tokens"))
    if completion_tokens is None:
        completion_tokens = _coerce_optional_int(usage.get("output_tokens"))
    total_tokens = _coerce_optional_int(usage.get("total_tokens"))
    cached_tokens = _extract_cached_tokens(usage)
    return prompt_tokens, completion_tokens, total_tokens, cached_tokens


def _extract_usage_payload(content_type: str, body: bytes) -> dict[str, Any] | None:
    if not body:
        return None
    normalized = content_type.lower()
    if "application/json" in normalized:
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            return None
        usage = payload.get("usage")
        return usage if isinstance(usage, dict) else None
    if "text/event-stream" not in normalized:
        return None

    latest_usage: dict[str, Any] | None = None
    text = body.decode("utf-8", errors="replace")
    for line in text.splitlines():
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if not data or data == "[DONE]":
            continue
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            continue
        usage = payload.get("usage")
        if isinstance(usage, dict):
            latest_usage = usage
            continue
        response_payload = payload.get("response")
        if isinstance(response_payload, dict):
            nested_usage = response_payload.get("usage")
            if isinstance(nested_usage, dict):
                latest_usage = nested_usage
    return latest_usage


def _extract_cached_tokens(usage: dict[str, Any]) -> int | None:
    for details_key in ("prompt_tokens_details", "input_tokens_details"):
        details = usage.get(details_key)
        if not isinstance(details, dict):
            continue
        cached_tokens = _coerce_optional_int(details.get("cached_tokens"))
        if cached_tokens is not None:
            return cached_tokens
    return None


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
            COALESCE(SUM(CASE WHEN prompt_tokens IS NOT NULL OR completion_tokens IS NOT NULL OR total_tokens IS NOT NULL OR cached_tokens IS NOT NULL THEN 1 ELSE 0 END), 0) AS usage_responses,
            COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
            COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
            COALESCE(SUM(total_tokens), 0) AS total_tokens,
            COALESCE(SUM(cached_tokens), 0) AS cached_tokens,
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
            COALESCE(SUM(CASE WHEN prompt_tokens IS NOT NULL OR completion_tokens IS NOT NULL OR total_tokens IS NOT NULL OR cached_tokens IS NOT NULL THEN 1 ELSE 0 END), 0) AS usage_responses,
            COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
            COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
            COALESCE(SUM(total_tokens), 0) AS total_tokens,
            COALESCE(SUM(cached_tokens), 0) AS cached_tokens,
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
            "cached_tokens": int(row["cached_tokens"]),
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
            "cached_tokens": 0,
        },
        "latency_ms": _latency_summary(0, None, None, None),
        "in_flight_requests": in_flight_requests,
        "last_request_at": None,
        "last_request_at_local": None,
        "last_success_at": None,
        "last_success_at_local": None,
    }


def _augment_stats_payload(
    payload: dict[str, Any],
    *,
    usage_rows: list[sqlite3.Row],
    pricing: PricingCatalog | None,
) -> dict[str, Any]:
    totals_bucket = _new_usage_enrichment_bucket()
    client_buckets: dict[str, dict[str, Any]] = {
        str(item["client_id"]): _new_usage_enrichment_bucket()
        for item in payload.get("clients", [])
        if item.get("client_id")
    }
    upstream_buckets: dict[str, dict[str, Any]] = {
        str(item["upstream_id"]): _new_usage_enrichment_bucket()
        for item in payload.get("upstreams", [])
        if item.get("upstream_id")
    }
    model_buckets: dict[str, dict[str, Any]] = {
        str(item["model"]): _new_usage_enrichment_bucket()
        for item in payload.get("models", [])
        if item.get("model")
    }

    for row in usage_rows:
        _accumulate_usage_enrichment(totals_bucket, row, pricing=pricing)

        client_id = str(row["client_id"] or "").strip()
        if client_id and client_id in client_buckets:
            _accumulate_usage_enrichment(client_buckets[client_id], row, pricing=pricing)

        upstream_id = str(row["upstream_id"] or "").strip()
        if upstream_id and upstream_id in upstream_buckets:
            _accumulate_usage_enrichment(upstream_buckets[upstream_id], row, pricing=pricing)

        requested_model = str(row["requested_model"] or "").strip()
        if requested_model and client_id and requested_model in model_buckets:
            _accumulate_usage_enrichment(model_buckets[requested_model], row, pricing=pricing)

    currency = pricing.currency if pricing is not None else DEFAULT_COST_CURRENCY
    payload["totals"]["usage"]["estimated_cost"] = _finalize_estimated_cost(totals_bucket, currency=currency)
    for item in payload.get("clients", []):
        bucket = client_buckets.get(str(item.get("client_id") or ""))
        item["usage"]["estimated_cost"] = _finalize_estimated_cost(bucket, currency=currency)
    for item in payload.get("upstreams", []):
        bucket = upstream_buckets.get(str(item.get("upstream_id") or ""))
        item["usage"]["estimated_cost"] = _finalize_estimated_cost(bucket, currency=currency)
    for item in payload.get("models", []):
        bucket = model_buckets.get(str(item.get("model") or ""))
        item["usage"]["estimated_cost"] = _finalize_estimated_cost(bucket, currency=currency)

    payload["usage_policy"] = _usage_policy()
    payload["usage_coverage"] = _finalize_usage_coverage(totals_bucket)
    return payload


def _new_usage_enrichment_bucket() -> dict[str, Any]:
    return {
        "estimated_requests": 0,
        "_estimated_input": Decimal("0"),
        "_estimated_output": Decimal("0"),
        "_requests": 0,
        "_prompt_token_requests": 0,
        "_completion_token_requests": 0,
        "_total_token_requests": 0,
        "_missing_estimated_pricing_config": 0,
        "_missing_estimated_pricing_rule": 0,
        "_missing_estimated_prompt_and_completion_tokens": 0,
    }


def _accumulate_usage_enrichment(
    bucket: dict[str, Any],
    row: sqlite3.Row,
    *,
    pricing: PricingCatalog | None,
) -> None:
    prompt_tokens = _coerce_optional_int(row["prompt_tokens"])
    completion_tokens = _coerce_optional_int(row["completion_tokens"])
    total_tokens = _coerce_optional_int(row["total_tokens"])
    bucket["_requests"] += 1
    if prompt_tokens is not None:
        bucket["_prompt_token_requests"] += 1
    if completion_tokens is not None:
        bucket["_completion_token_requests"] += 1
    if total_tokens is not None:
        bucket["_total_token_requests"] += 1

    estimated_cost, missing_reason = _estimate_row_cost(
        pricing=pricing,
        upstream_id=row["upstream_id"],
        requested_model=row["requested_model"],
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
    )
    if estimated_cost is None:
        if missing_reason == "missing_pricing_config":
            bucket["_missing_estimated_pricing_config"] += 1
        elif missing_reason == "missing_pricing_rule":
            bucket["_missing_estimated_pricing_rule"] += 1
        elif missing_reason == "missing_prompt_and_completion_tokens":
            bucket["_missing_estimated_prompt_and_completion_tokens"] += 1
        return

    bucket["estimated_requests"] += 1
    bucket["_estimated_input"] += estimated_cost["input"]
    bucket["_estimated_output"] += estimated_cost["output"]


def _estimate_row_cost(
    *,
    pricing: PricingCatalog | None,
    upstream_id: Any,
    requested_model: Any,
    prompt_tokens: int | None,
    completion_tokens: int | None,
) -> tuple[dict[str, Decimal] | None, str | None]:
    if pricing is None:
        return None, "missing_pricing_config"
    upstream_pricing = pricing.upstreams.get(str(upstream_id or ""))
    if upstream_pricing is None:
        return None, "missing_pricing_config"
    price = _resolve_token_price(upstream_pricing, requested_model=requested_model)
    if price is None:
        return None, "missing_pricing_rule"
    if prompt_tokens is None and completion_tokens is None:
        return None, "missing_prompt_and_completion_tokens"
    return (
        {
            "input": (Decimal(prompt_tokens or 0) * price.input_per_million_tokens) / TOKENS_PER_MILLION,
            "output": (Decimal(completion_tokens or 0) * price.output_per_million_tokens) / TOKENS_PER_MILLION,
        },
        None,
    )


def _resolve_token_price(upstream_pricing: UpstreamPricing, *, requested_model: Any) -> TokenPrice | None:
    normalized_model = str(requested_model or "").strip().lower()
    if normalized_model and normalized_model in upstream_pricing.model_prices:
        return upstream_pricing.model_prices[normalized_model]
    return upstream_pricing.default_price


def _finalize_estimated_cost(bucket: dict[str, Any] | None, *, currency: str) -> dict[str, Any]:
    if bucket is None:
        bucket = _new_usage_enrichment_bucket()
    estimated_input = Decimal(bucket["_estimated_input"])
    estimated_output = Decimal(bucket["_estimated_output"])
    return {
        "currency": currency,
        "estimated_requests": int(bucket["estimated_requests"]),
        "input": _decimal_to_float(estimated_input),
        "output": _decimal_to_float(estimated_output),
        "total": _decimal_to_float(estimated_input + estimated_output),
    }


def _finalize_usage_coverage(bucket: dict[str, Any]) -> dict[str, Any]:
    request_count = int(bucket["_requests"])
    estimated_requests = int(bucket["estimated_requests"])
    return {
        "window_requests": request_count,
        "prompt_tokens": {
            "recorded_requests": int(bucket["_prompt_token_requests"]),
            "missing_requests": request_count - int(bucket["_prompt_token_requests"]),
        },
        "completion_tokens": {
            "recorded_requests": int(bucket["_completion_token_requests"]),
            "missing_requests": request_count - int(bucket["_completion_token_requests"]),
        },
        "total_tokens": {
            "recorded_requests": int(bucket["_total_token_requests"]),
            "missing_requests": request_count - int(bucket["_total_token_requests"]),
        },
        "estimated_cost": {
            "estimated_requests": estimated_requests,
            "missing_requests": request_count - estimated_requests,
            "missing_breakdown": {
                "missing_pricing_config": int(bucket["_missing_estimated_pricing_config"]),
                "missing_pricing_rule": int(bucket["_missing_estimated_pricing_rule"]),
                "missing_prompt_and_completion_tokens": int(
                    bucket["_missing_estimated_prompt_and_completion_tokens"]
                ),
            },
        },
    }


def _usage_policy() -> dict[str, Any]:
    return {
        "token_usage": {
            "prompt_tokens": "request_log_v4.prompt_tokens",
            "completion_tokens": "request_log_v4.completion_tokens",
            "total_tokens": "request_log_v4.total_tokens",
            "cached_tokens": "request_log_v4.cached_tokens",
        },
        "estimated_cost": {
            "label": "estimated_cost",
            "estimated": True,
            "source": "relay pricing config x request_log_v4.prompt_tokens/completion_tokens",
        },
        "missing_scenarios": [
            "Rows with NULL token columns do not contribute to token usage aggregates.",
            "Rows without pricing config or a matching pricing rule do not contribute to estimated_cost.",
            "Rows without prompt_tokens and completion_tokens cannot produce estimated_cost, even if total_tokens is present.",
        ],
    }


def _decimal_to_float(value: Decimal) -> float:
    quantized = value.quantize(COST_PRECISION, rounding=ROUND_HALF_UP)
    return float(quantized.normalize())


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


def _load_pricing_catalog(
    raw_pricing: Any,
    *,
    upstreams_payload: dict[str, Any],
) -> PricingCatalog | None:
    if raw_pricing is not None:
        pricing_payload = _as_dict(raw_pricing, "pricing")
        upstream_pricing_payload = _as_dict(pricing_payload.get("upstreams"), "pricing.upstreams")
        upstreams: dict[str, UpstreamPricing] = {}
        for upstream_id, raw_upstream in upstream_pricing_payload.items():
            upstreams[str(upstream_id)] = _load_upstream_pricing(
                _as_dict(raw_upstream, f"pricing.upstreams.{upstream_id}")
            )
        return PricingCatalog(
            currency=str(pricing_payload.get("currency") or DEFAULT_COST_CURRENCY),
            upstreams=upstreams,
        )

    inline_upstreams: dict[str, UpstreamPricing] = {}
    for upstream_id, raw_upstream in upstreams_payload.items():
        upstream_payload = _as_dict(raw_upstream, f"upstreams.{upstream_id}")
        raw_inline_pricing = upstream_payload.get("pricing")
        if raw_inline_pricing is None:
            continue
        inline_upstreams[str(upstream_id)] = _load_upstream_pricing(
            _as_dict(raw_inline_pricing, f"upstreams.{upstream_id}.pricing")
        )
    if not inline_upstreams:
        return None
    return PricingCatalog(
        currency=DEFAULT_COST_CURRENCY,
        upstreams=inline_upstreams,
    )


def _load_upstream_pricing(payload: dict[str, Any]) -> UpstreamPricing:
    default_price = None
    if "input_per_million_tokens" in payload or "output_per_million_tokens" in payload:
        default_price = _load_token_price(payload, "pricing default")
    model_prices: dict[str, TokenPrice] = {}
    models_payload = _coerce_dict(payload.get("models"))
    for model_id, raw_model in models_payload.items():
        model_prices[str(model_id).strip().lower()] = _load_token_price(
            _as_dict(raw_model, f"pricing.models.{model_id}"),
            f"pricing.models.{model_id}",
        )
    return UpstreamPricing(default_price=default_price, model_prices=model_prices)


def _load_token_price(payload: dict[str, Any], label: str) -> TokenPrice:
    return TokenPrice(
        input_per_million_tokens=_coerce_decimal(payload.get("input_per_million_tokens"), f"{label}.input_per_million_tokens"),
        output_per_million_tokens=_coerce_decimal(payload.get("output_per_million_tokens"), f"{label}.output_per_million_tokens"),
    )


def _coerce_decimal(value: Any, label: str) -> Decimal:
    if value is None:
        raise ValueError(f"{label} is required")
    try:
        decimal_value = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{label} must be a number") from exc
    if decimal_value < 0:
        raise ValueError(f"{label} must be >= 0")
    return decimal_value


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


def _hash_config_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def probe_upstreams(
    config_path: str | Path,
    *,
    upstream_ids: list[str] | None = None,
    probe_path: str = DEFAULT_PROBE_PATH,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    resolved_config_path = Path(config_path).expanduser().resolve()
    config = RelayConfig.load(resolved_config_path)
    targets = _select_operator_upstreams(config, upstream_ids=upstream_ids)
    results = _parallel_map(
        targets,
        lambda upstream: _probe_upstream(upstream, probe_path=probe_path, timeout_seconds=timeout_seconds),
    )
    successes = sum(1 for item in results if bool(item["success"]))
    return {
        "config_path": str(resolved_config_path),
        "probe": {
            "method": DEFAULT_PROBE_METHOD,
            "path": _normalize_probe_path(probe_path),
        },
        "selected_upstream_ids": [target.upstream_id for target in targets],
        "summary": {
            "total": len(results),
            "successes": successes,
            "failures": len(results) - successes,
        },
        "upstreams": results,
    }


def query_upstream_credits(
    config_path: str | Path,
    *,
    upstream_ids: list[str] | None = None,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    resolved_config_path = Path(config_path).expanduser().resolve()
    config = RelayConfig.load(resolved_config_path)
    targets = _select_operator_upstreams(config, upstream_ids=upstream_ids)
    results = _parallel_map(
        targets,
        lambda upstream: _query_upstream_credits(upstream, timeout_seconds=timeout_seconds),
    )
    return {
        "config_path": str(resolved_config_path),
        "selected_upstream_ids": [target.upstream_id for target in targets],
        "summary": {
            "total": len(results),
            "supported": sum(1 for item in results if str(item["status"]) == "supported"),
            "unsupported": sum(1 for item in results if str(item["status"]) == "unsupported"),
            "failures": sum(1 for item in results if str(item["status"]) == "request_failed"),
            "disabled": sum(1 for item in results if str(item["status"]) == "disabled"),
        },
        "upstreams": results,
    }


def exercise_upstreams(
    config_path: str | Path,
    *,
    upstream_ids: list[str] | None = None,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    resolved_config_path = Path(config_path).expanduser().resolve()
    config = RelayConfig.load(resolved_config_path)
    targets = _select_operator_upstreams(config, upstream_ids=upstream_ids)
    results = _parallel_map(
        targets,
        lambda upstream: _exercise_upstream(upstream, timeout_seconds=timeout_seconds),
    )
    return {
        "config_path": str(resolved_config_path),
        "selected_upstream_ids": [target.upstream_id for target in targets],
        "summary": {
            "total": len(results),
            "models_successes": sum(1 for item in results if bool(item["models"]["success"])),
            "request_successes": sum(1 for item in results if bool(item["request"]["success"])),
            "failures": sum(1 for item in results if not bool(item["success"])),
        },
        "upstreams": results,
    }


def _select_operator_upstreams(
    config: RelayConfig,
    *,
    upstream_ids: list[str] | None,
) -> list[UpstreamConfig]:
    upstreams_by_id = config.upstreams_by_id
    if upstream_ids:
        selected: list[UpstreamConfig] = []
        seen_ids: set[str] = set()
        for raw_upstream_id in upstream_ids:
            upstream_id = str(raw_upstream_id)
            if upstream_id in seen_ids:
                continue
            upstream = upstreams_by_id.get(upstream_id)
            if upstream is None:
                raise ValueError(f"unknown upstream_id: {upstream_id}")
            selected.append(upstream)
            seen_ids.add(upstream_id)
        return selected

    ordered_upstreams: list[UpstreamConfig] = []
    seen_ids: set[str] = set()
    for upstream_id in config.order:
        upstream = upstreams_by_id.get(upstream_id)
        if upstream is None or upstream_id in seen_ids:
            continue
        ordered_upstreams.append(upstream)
        seen_ids.add(upstream_id)
    for upstream_id in sorted(upstreams_by_id):
        if upstream_id in seen_ids:
            continue
        ordered_upstreams.append(upstreams_by_id[upstream_id])
    return [upstream for upstream in ordered_upstreams if upstream.enabled]


def _parallel_map(
    upstreams: list[UpstreamConfig],
    func: Any,
) -> list[dict[str, Any]]:
    if not upstreams:
        return []
    if len(upstreams) == 1:
        return [func(upstreams[0])]
    max_workers = min(len(upstreams), DEFAULT_MAX_PARALLEL_PROBES)
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="relay-operator") as executor:
        futures = [executor.submit(func, upstream) for upstream in upstreams]
        return [future.result() for future in futures]


def _probe_upstream(
    upstream: UpstreamConfig,
    *,
    probe_path: str,
    timeout_seconds: float | None,
) -> dict[str, Any]:
    if not upstream.enabled:
        return {
            "upstream_id": upstream.upstream_id,
            "base_url": upstream.base_url,
            "enabled": False,
            "success": False,
            "status_code": None,
            "error_kind": "disabled",
            "error": "upstream is disabled in relay config",
            "latency_ms": 0,
        }

    request = Request(
        _build_probe_url(upstream.base_url, probe_path),
        method=DEFAULT_PROBE_METHOD,
        headers={
            "Authorization": f"Bearer {upstream.api_key}",
            "Accept": "application/json",
            "User-Agent": "local-api-relay-probe/1",
        },
    )
    started_at = time.perf_counter()
    try:
        with urlopen(request, timeout=_coerce_timeout(timeout_seconds, upstream.timeout_seconds(DEFAULT_TIMEOUT_SECONDS))) as response:
            response.read()
            return {
                "upstream_id": upstream.upstream_id,
                "base_url": upstream.base_url,
                "enabled": True,
                "success": True,
                "status_code": int(getattr(response, "status", response.getcode())),
                "error_kind": None,
                "error": None,
                "latency_ms": _operator_duration_ms(started_at),
            }
    except HTTPError as exc:
        return {
            "upstream_id": upstream.upstream_id,
            "base_url": upstream.base_url,
            "enabled": True,
            "success": False,
            "status_code": int(exc.code),
            "error_kind": _http_error_kind(int(exc.code)),
            "error": f"{int(exc.code)} {exc.reason}",
            "latency_ms": _operator_duration_ms(started_at),
        }
    except URLError as exc:
        return {
            "upstream_id": upstream.upstream_id,
            "base_url": upstream.base_url,
            "enabled": True,
            "success": False,
            "status_code": None,
            "error_kind": _network_error_kind(exc.reason),
            "error": _stringify_error(exc.reason),
            "latency_ms": _operator_duration_ms(started_at),
        }
    except OSError as exc:
        return {
            "upstream_id": upstream.upstream_id,
            "base_url": upstream.base_url,
            "enabled": True,
            "success": False,
            "status_code": None,
            "error_kind": _network_error_kind(exc),
            "error": _stringify_error(exc),
            "latency_ms": _operator_duration_ms(started_at),
        }


def _query_upstream_credits(
    upstream: UpstreamConfig,
    *,
    timeout_seconds: float | None,
) -> dict[str, Any]:
    if not upstream.enabled:
        return _credits_result(
            upstream,
            status="disabled",
            supported=False,
            source=None,
            status_code=None,
            error_kind="disabled",
            error="upstream is disabled in relay config",
            latency_ms=0,
        )

    started_at = time.perf_counter()
    effective_timeout = _coerce_timeout(timeout_seconds, upstream.timeout_seconds(DEFAULT_TIMEOUT_SECONDS))
    for profile_id, profile_path in (
        ("newapi_user_self", "/api/user/self"),
        ("newapi_token_self", "/api/token/self"),
    ):
        for endpoint_url in _candidate_endpoint_urls(upstream.base_url, profile_path):
            request = Request(
                endpoint_url,
                method="GET",
                headers={
                    "Authorization": f"Bearer {upstream.api_key}",
                    "Accept": "application/json",
                    "User-Agent": "local-api-relay-credits/1",
                },
            )
            try:
                with urlopen(request, timeout=effective_timeout) as response:
                    body = response.read()
                    status_code = int(getattr(response, "status", response.getcode()))
            except HTTPError as exc:
                status_code = int(exc.code)
                if status_code in UNSUPPORTED_ENDPOINT_STATUS_CODES:
                    continue
                return _credits_result(
                    upstream,
                    status="request_failed",
                    supported=False,
                    source={"profile_id": profile_id, "path": profile_path},
                    status_code=status_code,
                    error_kind=_http_error_kind(status_code),
                    error=f"{status_code} {exc.reason}",
                    latency_ms=_operator_duration_ms(started_at),
                )
            except URLError as exc:
                return _credits_result(
                    upstream,
                    status="request_failed",
                    supported=False,
                    source={"profile_id": profile_id, "path": profile_path},
                    status_code=None,
                    error_kind=_network_error_kind(exc.reason),
                    error=_stringify_error(exc.reason),
                    latency_ms=_operator_duration_ms(started_at),
                )
            except OSError as exc:
                return _credits_result(
                    upstream,
                    status="request_failed",
                    supported=False,
                    source={"profile_id": profile_id, "path": profile_path},
                    status_code=None,
                    error_kind=_network_error_kind(exc),
                    error=_stringify_error(exc),
                    latency_ms=_operator_duration_ms(started_at),
                )

            payload = _load_json_payload(body)
            if _payload_reports_error(payload):
                return _credits_result(
                    upstream,
                    status="request_failed",
                    supported=False,
                    source={"profile_id": profile_id, "path": profile_path},
                    status_code=status_code,
                    error_kind="credits_query_rejected",
                    error=_payload_error_message(payload),
                    latency_ms=_operator_duration_ms(started_at),
                )
            credits = _extract_credits(payload)
            if credits is None:
                continue
            return _credits_result(
                upstream,
                status="supported",
                supported=True,
                source={"profile_id": profile_id, "path": profile_path},
                status_code=status_code,
                error_kind=None,
                error=None,
                latency_ms=_operator_duration_ms(started_at),
                credits=credits,
            )

    return _credits_result(
        upstream,
        status="unsupported",
        supported=False,
        source=None,
        status_code=None,
        error_kind="unsupported",
        error="no supported credits endpoint matched this upstream",
        latency_ms=_operator_duration_ms(started_at),
    )


def _credits_result(
    upstream: UpstreamConfig,
    *,
    status: str,
    supported: bool,
    source: dict[str, str] | None,
    status_code: int | None,
    error_kind: str | None,
    error: str | None,
    latency_ms: int,
    credits: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "upstream_id": upstream.upstream_id,
        "base_url": upstream.base_url,
        "enabled": upstream.enabled,
        "status": status,
        "supported": supported,
        "source": source,
        "status_code": status_code,
        "error_kind": error_kind,
        "error": error,
        "latency_ms": latency_ms,
        "credits": credits or _empty_credits(),
    }


def _exercise_upstream(
    upstream: UpstreamConfig,
    *,
    timeout_seconds: float | None,
) -> dict[str, Any]:
    if not upstream.enabled:
        return {
            "upstream_id": upstream.upstream_id,
            "base_url": upstream.base_url,
            "enabled": False,
            "success": False,
            "latency_ms": 0,
            "models": {
                "success": False,
                "status_code": None,
                "error_kind": "disabled",
                "error": "upstream is disabled in relay config",
                "latency_ms": 0,
                "count": None,
                "preview": [],
                "selected_model": None,
            },
            "request": {
                "attempted": False,
                "success": False,
                "status_code": None,
                "error_kind": "disabled",
                "error": "upstream is disabled in relay config",
                "latency_ms": 0,
                "mode": None,
                "path": None,
                "model": None,
                "reply_preview": None,
                "usage": None,
                "attempts": 0,
                "compatibility_retry": None,
            },
        }

    models_result = _fetch_models(upstream, timeout_seconds=timeout_seconds)
    request_result = _skipped_request_result("models_failed")
    if models_result["success"] and models_result["selected_model"]:
        request_result = _perform_real_request(
            upstream,
            model=str(models_result["selected_model"]),
            timeout_seconds=timeout_seconds,
        )
    return {
        "upstream_id": upstream.upstream_id,
        "base_url": upstream.base_url,
        "enabled": upstream.enabled,
        "success": bool(models_result["success"]) and bool(request_result["success"]),
        "latency_ms": int(models_result["latency_ms"]) + int(request_result["latency_ms"]),
        "models": models_result,
        "request": request_result,
    }


def _fetch_models(
    upstream: UpstreamConfig,
    *,
    timeout_seconds: float | None,
) -> dict[str, Any]:
    request = Request(
        _build_probe_url(upstream.base_url, DEFAULT_MODELS_PATH),
        method="GET",
        headers=_operator_headers(upstream.api_key, content_type=False),
    )
    started_at = time.perf_counter()
    try:
        with urlopen(request, timeout=_coerce_timeout(timeout_seconds, upstream.timeout_seconds(DEFAULT_REQUEST_TIMEOUT_SECONDS))) as response:
            body = response.read()
            status_code = int(getattr(response, "status", response.getcode()))
    except HTTPError as exc:
        return {
            "success": False,
            "status_code": int(exc.code),
            "error_kind": _http_error_kind(int(exc.code)),
            "error": _http_error_message(exc),
            "latency_ms": _operator_duration_ms(started_at),
            "count": None,
            "preview": [],
            "selected_model": None,
        }
    except URLError as exc:
        return {
            "success": False,
            "status_code": None,
            "error_kind": _network_error_kind(exc.reason),
            "error": _stringify_error(exc.reason),
            "latency_ms": _operator_duration_ms(started_at),
            "count": None,
            "preview": [],
            "selected_model": None,
        }
    except OSError as exc:
        return {
            "success": False,
            "status_code": None,
            "error_kind": _network_error_kind(exc),
            "error": _stringify_error(exc),
            "latency_ms": _operator_duration_ms(started_at),
            "count": None,
            "preview": [],
            "selected_model": None,
        }

    payload = _load_json_payload(body)
    model_ids = _extract_model_ids(payload)
    selected_model = _select_model(model_ids)
    if not selected_model:
        return {
            "success": False,
            "status_code": status_code,
            "error_kind": "no_models",
            "error": "models response did not include a usable model id",
            "latency_ms": _operator_duration_ms(started_at),
            "count": len(model_ids),
            "preview": model_ids[:DEFAULT_PREVIEW_MODEL_LIMIT],
            "selected_model": None,
        }
    return {
        "success": True,
        "status_code": status_code,
        "error_kind": None,
        "error": None,
        "latency_ms": _operator_duration_ms(started_at),
        "count": len(model_ids),
        "preview": model_ids[:DEFAULT_PREVIEW_MODEL_LIMIT],
        "selected_model": selected_model,
    }


def _perform_real_request(
    upstream: UpstreamConfig,
    *,
    model: str,
    timeout_seconds: float | None,
) -> dict[str, Any]:
    initial_mode = _request_mode_for_model(model)
    attempts = [
        _perform_real_request_attempt(
            upstream,
            model=model,
            timeout_seconds=timeout_seconds,
            mode=initial_mode,
            path=_request_path_for_mode(initial_mode),
            body=_request_body_for_mode(model=model, mode=initial_mode),
        )
    ]
    compatibility_retry = _compatibility_retry_plan(attempts[0], model=model)
    if compatibility_retry is not None:
        attempts.append(
            _perform_real_request_attempt(
                upstream,
                model=model,
                timeout_seconds=timeout_seconds,
                mode=str(compatibility_retry["mode"]),
                path=str(compatibility_retry["path"]),
                body=_coerce_dict(compatibility_retry["body"]),
            )
        )
    final_result = dict(attempts[-1])
    final_result["latency_ms"] = sum(int(item.get("latency_ms") or 0) for item in attempts)
    final_result["attempts"] = len(attempts)
    if compatibility_retry is None:
        final_result["compatibility_retry"] = None
    else:
        final_result["compatibility_retry"] = {
            "applied": True,
            "kind": str(compatibility_retry["kind"]),
            "initial_mode": attempts[0].get("mode"),
            "initial_path": attempts[0].get("path"),
            "final_mode": final_result.get("mode"),
            "final_path": final_result.get("path"),
        }
    return final_result


def _perform_real_request_attempt(
    upstream: UpstreamConfig,
    *,
    model: str,
    timeout_seconds: float | None,
    mode: str,
    path: str,
    body: dict[str, Any],
) -> dict[str, Any]:
    request = Request(
        _build_probe_url(upstream.base_url, path),
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers=_operator_headers(upstream.api_key, content_type=True),
    )
    started_at = time.perf_counter()
    try:
        with urlopen(request, timeout=_coerce_timeout(timeout_seconds, upstream.timeout_seconds(DEFAULT_REQUEST_TIMEOUT_SECONDS))) as response:
            payload = response.read()
            status_code = int(getattr(response, "status", response.getcode()))
    except HTTPError as exc:
        return {
            "attempted": True,
            "success": False,
            "status_code": int(exc.code),
            "error_kind": _http_error_kind(int(exc.code)),
            "error": _http_error_message(exc),
            "latency_ms": _operator_duration_ms(started_at),
            "mode": mode,
            "path": path,
            "model": model,
            "reply_preview": None,
            "usage": None,
        }
    except URLError as exc:
        return {
            "attempted": True,
            "success": False,
            "status_code": None,
            "error_kind": _network_error_kind(exc.reason),
            "error": _stringify_error(exc.reason),
            "latency_ms": _operator_duration_ms(started_at),
            "mode": mode,
            "path": path,
            "model": model,
            "reply_preview": None,
            "usage": None,
        }
    except OSError as exc:
        return {
            "attempted": True,
            "success": False,
            "status_code": None,
            "error_kind": _network_error_kind(exc),
            "error": _stringify_error(exc),
            "latency_ms": _operator_duration_ms(started_at),
            "mode": mode,
            "path": path,
            "model": model,
            "reply_preview": None,
            "usage": None,
        }

    parsed_payload = _parse_response_payload(payload)
    if parsed_payload is None:
        preview = payload.decode("utf-8", errors="replace")
        return {
            "attempted": True,
            "success": False,
            "status_code": status_code,
            "error_kind": "invalid_response_payload",
            "error": "response body was neither JSON nor supported SSE data",
            "latency_ms": _operator_duration_ms(started_at),
            "mode": mode,
            "path": path,
            "model": model,
            "reply_preview": _truncate_text(preview),
            "usage": None,
        }

    return {
        "attempted": True,
        "success": True,
        "status_code": status_code,
        "error_kind": None,
        "error": None,
        "latency_ms": _operator_duration_ms(started_at),
        "mode": mode,
        "path": path,
        "model": model,
        "reply_preview": _extract_reply_preview(parsed_payload),
        "usage": _extract_operator_usage(parsed_payload),
    }


def _compatibility_retry_plan(attempt: dict[str, Any], *, model: str) -> dict[str, Any] | None:
    if bool(attempt.get("success")):
        return None
    mode = str(attempt.get("mode") or "")
    status_code = _coerce_optional_int(attempt.get("status_code"))
    error_text = str(attempt.get("error") or "").lower()
    if mode == "chat.completions" and _requires_stream_retry(status_code=status_code, error_text=error_text):
        body = _request_body_for_mode(model=model, mode=mode)
        body["stream"] = True
        return {
            "kind": "stream_required",
            "mode": mode,
            "path": _request_path_for_mode(mode),
            "body": body,
        }
    if mode == "responses" and _requires_chat_fallback(status_code=status_code, error_text=error_text):
        fallback_mode = "chat.completions"
        return {
            "kind": "responses_to_chat_completions",
            "mode": fallback_mode,
            "path": _request_path_for_mode(fallback_mode),
            "body": _request_body_for_mode(model=model, mode=fallback_mode),
        }
    return None


def _requires_stream_retry(*, status_code: int | None, error_text: str) -> bool:
    return status_code == 400 and any(marker in error_text for marker in STREAM_REQUIRED_ERROR_MARKERS)


def _requires_chat_fallback(*, status_code: int | None, error_text: str) -> bool:
    if status_code in UNSUPPORTED_ENDPOINT_STATUS_CODES:
        return True
    if status_code not in {400, 422}:
        return False
    return any(marker in error_text for marker in RESPONSES_FALLBACK_ERROR_MARKERS)


def _skipped_request_result(error_kind: str) -> dict[str, Any]:
    return {
        "attempted": False,
        "success": False,
        "status_code": None,
        "error_kind": error_kind,
        "error": "request skipped because models stage did not produce a usable model",
        "latency_ms": 0,
        "mode": None,
        "path": None,
        "model": None,
        "reply_preview": None,
        "usage": None,
        "attempts": 0,
        "compatibility_retry": None,
    }


def _operator_headers(api_key: str, *, content_type: bool) -> dict[str, str]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "User-Agent": "local-api-relay-operator/1",
    }
    if content_type:
        headers["Content-Type"] = "application/json"
    return headers


def _build_probe_url(base_url: str, path: str) -> str:
    parsed = urlsplit(base_url)
    combined_path = parsed.path.rstrip("/") + _normalize_probe_path(path)
    return urlunsplit((parsed.scheme, parsed.netloc, combined_path, "", ""))


def _normalize_probe_path(path: str) -> str:
    normalized = str(path or DEFAULT_PROBE_PATH).strip()
    if not normalized.startswith("/"):
        normalized = f"/{normalized}"
    return normalized


def _candidate_endpoint_urls(base_url: str, endpoint_path: str) -> list[str]:
    parsed = urlsplit(base_url)
    base_path = parsed.path.rstrip("/")
    prefixes: list[str] = []
    if base_path.endswith("/v1"):
        prefixes.append(base_path[:-3])
    elif base_path:
        prefixes.append(base_path)
    prefixes.append("")

    urls: list[str] = []
    seen_paths: set[str] = set()
    for prefix in prefixes:
        path = f"{prefix.rstrip('/')}{endpoint_path}"
        if not path.startswith("/"):
            path = f"/{path}"
        if path in seen_paths:
            continue
        seen_paths.add(path)
        urls.append(urlunsplit((parsed.scheme, parsed.netloc, path, "", "")))
    return urls


def _load_json_payload(body: bytes) -> Any:
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def _payload_reports_error(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("success") is False:
        return True
    return isinstance(payload.get("error"), dict)


def _payload_error_message(payload: Any) -> str:
    if not isinstance(payload, dict):
        return "credits endpoint rejected request"
    error_payload = payload.get("error")
    if isinstance(error_payload, dict):
        message = str(error_payload.get("message") or "").strip()
        if message:
            return message
    message = str(payload.get("message") or "").strip()
    if message:
        return message
    return "credits endpoint rejected request"


def _extract_credits(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    source = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if not isinstance(source, dict):
        return None
    balance = _pick_number(source, "balance", "amount", "balance_amount", "remaining_balance")
    quota = _pick_number(source, "quota", "credit_limit", "limit", "total_quota", "grant_quota")
    used = _pick_number(source, "used_quota", "used", "used_amount", "consumed_quota")
    remaining = _pick_number(
        source,
        "remaining",
        "remaining_quota",
        "remain_quota",
        "available_quota",
        "available_amount",
    )
    currency = _pick_string(source, "currency", "quota_currency", "balance_currency")
    unit = _pick_string(source, "unit", "quota_unit")
    if all(value is None for value in (balance, quota, used, remaining)):
        return None
    if unit is None:
        if any(value is not None for value in (quota, used, remaining)):
            unit = "quota"
        elif balance is not None:
            unit = "currency"
    return {
        "balance": balance,
        "quota": quota,
        "used": used,
        "remaining": remaining,
        "currency": currency,
        "unit": unit,
    }


def _pick_number(source: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key not in source:
            continue
        value = source.get(key)
        if value is None or isinstance(value, bool):
            continue
        try:
            return float(Decimal(str(value).strip()))
        except (InvalidOperation, ValueError):
            continue
    return None


def _pick_string(source: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = str(source.get(key) or "").strip()
        if value:
            return value
    return None


def _empty_credits() -> dict[str, Any]:
    return {
        "balance": None,
        "quota": None,
        "used": None,
        "remaining": None,
        "currency": None,
        "unit": None,
    }


def _parse_response_payload(payload: bytes) -> Any | None:
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _parse_sse_payload(payload)


def _parse_sse_payload(payload: bytes) -> dict[str, Any] | None:
    try:
        decoded = payload.decode("utf-8")
    except UnicodeDecodeError:
        return None
    text_parts: list[str] = []
    usage: dict[str, int] | None = None
    saw_event = False
    for raw_line in decoded.splitlines():
        line = raw_line.strip()
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if not data:
            continue
        if data == "[DONE]":
            saw_event = True
            continue
        try:
            item = json.loads(data)
        except json.JSONDecodeError:
            continue
        saw_event = True
        preview = _extract_reply_preview(item)
        if preview:
            text_parts.append(preview)
            usage = _extract_operator_usage(item) or usage
            continue
        delta_text = _extract_stream_delta_text(item)
        if delta_text:
            text_parts.append(delta_text)
        usage = _extract_operator_usage(item) or usage
    if not saw_event:
        return None
    payload_dict: dict[str, Any] = {}
    combined_text = "".join(text_parts)
    if combined_text.strip():
        payload_dict["output_text"] = combined_text
    if usage is not None:
        payload_dict["usage"] = usage
    return payload_dict or None


def _extract_stream_delta_text(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    choices = payload.get("choices")
    if not isinstance(choices, list):
        return None
    parts: list[str] = []
    for choice in choices:
        if not isinstance(choice, dict):
            continue
        delta = choice.get("delta")
        if not isinstance(delta, dict):
            continue
        content = delta.get("content")
        if isinstance(content, str) and content:
            parts.append(content)
            continue
        if isinstance(content, list):
            for item in content:
                if not isinstance(item, dict):
                    continue
                text = item.get("text")
                if isinstance(text, str) and text:
                    parts.append(text)
    return "".join(parts) if parts else None


def _extract_model_ids(payload: Any) -> list[str]:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [str(item.get("id")).strip() for item in data if isinstance(item, dict) and str(item.get("id") or "").strip()]
    if isinstance(payload, list):
        return [str(item.get("id")).strip() for item in payload if isinstance(item, dict) and str(item.get("id") or "").strip()]
    return []


def _select_model(model_ids: list[str]) -> str | None:
    if not model_ids:
        return None
    for model_id in model_ids:
        normalized = model_id.lower()
        if any(marker in normalized for marker in NON_TEXT_MODEL_MARKERS):
            continue
        if any(hint in normalized for hint in TEXT_MODEL_HINTS):
            return model_id
    for model_id in model_ids:
        normalized = model_id.lower()
        if any(marker in normalized for marker in NON_TEXT_MODEL_MARKERS):
            continue
        return model_id
    return model_ids[0]


def _request_mode_for_model(model: str) -> str:
    normalized = model.lower()
    if normalized.startswith(RESPONSES_MODEL_PREFIXES):
        return "responses"
    return "chat.completions"


def _request_path_for_mode(mode: str) -> str:
    if mode == "responses":
        return DEFAULT_RESPONSES_PATH
    return DEFAULT_CHAT_COMPLETIONS_PATH


def _request_body_for_mode(*, model: str, mode: str) -> dict[str, Any]:
    if mode == "responses":
        return {
            "model": model,
            "input": DEFAULT_RESPONSES_INPUT,
            "max_output_tokens": 16,
        }
    return {
        "model": model,
        "messages": [{"role": "user", "content": DEFAULT_CHAT_MESSAGE}],
        "max_tokens": 16,
    }


def _extract_reply_preview(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return _truncate_text(output_text.strip())
    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        preview = _extract_message_content(choices[0].get("message") if isinstance(choices[0], dict) else None)
        if preview:
            return _truncate_text(preview)
    output = payload.get("output")
    if isinstance(output, list):
        for item in output:
            preview = _extract_message_content(item)
            if preview:
                return _truncate_text(preview)
    return None


def _extract_message_content(message: Any) -> str | None:
    if not isinstance(message, dict):
        return None
    content = message.get("content")
    if isinstance(content, str) and content.strip():
        return content.strip()
    if not isinstance(content, list):
        return None
    parts: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if isinstance(text, str) and text.strip():
            parts.append(text.strip())
    return "\n".join(parts) if parts else None


def _extract_operator_usage(payload: Any) -> dict[str, int] | None:
    if not isinstance(payload, dict):
        return None
    usage = payload.get("usage")
    if not isinstance(usage, dict):
        return None
    prompt_tokens = _coerce_optional_int(usage.get("prompt_tokens"))
    if prompt_tokens is None:
        prompt_tokens = _coerce_optional_int(usage.get("input_tokens"))
    completion_tokens = _coerce_optional_int(usage.get("completion_tokens"))
    if completion_tokens is None:
        completion_tokens = _coerce_optional_int(usage.get("output_tokens"))
    total_tokens = _coerce_optional_int(usage.get("total_tokens"))
    if prompt_tokens is None and completion_tokens is None and total_tokens is None:
        return None
    return {
        "prompt_tokens": prompt_tokens or 0,
        "completion_tokens": completion_tokens or 0,
        "total_tokens": total_tokens or 0,
    }


def _truncate_text(text: str) -> str:
    stripped = text.strip()
    if len(stripped) <= DEFAULT_REPLY_PREVIEW_CHARS:
        return stripped
    return f"{stripped[:DEFAULT_REPLY_PREVIEW_CHARS - 1]}…"


def _coerce_timeout(value: Any, fallback: float) -> float:
    try:
        timeout = float(value)
    except (TypeError, ValueError):
        return fallback
    if timeout <= 0:
        return fallback
    return timeout


def _http_error_kind(status_code: int) -> str:
    if status_code == 401:
        return "http_401"
    if status_code == 403:
        return "http_403"
    if status_code == 429:
        return "http_429"
    if 500 <= status_code <= 599:
        return "http_5xx"
    return "http_error"


def _network_error_kind(error: Any) -> str:
    if isinstance(error, (socket.timeout, TimeoutError)):
        return "timeout"
    if isinstance(error, ConnectionRefusedError):
        return "connection_refused"
    if isinstance(error, socket.gaierror):
        return "dns_error"
    return "network_error"


def _http_error_message(exc: HTTPError) -> str:
    body = b""
    if exc.fp is not None:
        body = exc.read()
    extracted = _extract_error_message(str(exc.headers.get("Content-Type", "")) if exc.headers else "", body)
    return extracted or f"{int(exc.code)} {exc.reason}"


def _operator_duration_ms(started_at: float) -> int:
    return int((time.perf_counter() - started_at) * 1000)


def _stringify_error(error: Any) -> str:
    message = str(error)
    return message or error.__class__.__name__


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    raw_args = list(sys.argv[1:] if argv is None else argv)
    known_commands = {"serve", "probe", "credits", "test"}
    if not raw_args or raw_args[0] not in known_commands:
        raw_args = ["serve", *raw_args]

    parser = argparse.ArgumentParser(description="Local API relay")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve", help="Run relay server")
    serve_parser.add_argument("--config", required=True, help="Path to relay config JSON")

    probe_parser = subparsers.add_parser("probe", help="Probe upstream /models endpoints")
    probe_parser.add_argument("--config", required=True, help="Path to relay config JSON")
    probe_parser.add_argument("--upstream-id", dest="upstream_ids", action="append", default=None)
    probe_parser.add_argument("--path", default=DEFAULT_PROBE_PATH)
    probe_parser.add_argument("--timeout-seconds", type=float, default=None)

    credits_parser = subparsers.add_parser("credits", help="Query upstream credits endpoints")
    credits_parser.add_argument("--config", required=True, help="Path to relay config JSON")
    credits_parser.add_argument("--upstream-id", dest="upstream_ids", action="append", default=None)
    credits_parser.add_argument("--timeout-seconds", type=float, default=None)

    test_parser = subparsers.add_parser("test", help="Run upstream real-request tests")
    test_parser.add_argument("--config", required=True, help="Path to relay config JSON")
    test_parser.add_argument("--upstream-id", dest="upstream_ids", action="append", default=None)
    test_parser.add_argument("--timeout-seconds", type=float, default=None)

    return parser.parse_args(raw_args)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command == "serve":
        service = RelayService(args.config)
        try:
            service.serve_forever()
        except KeyboardInterrupt:
            service.shutdown()
        return 0
    if args.command == "probe":
        payload = probe_upstreams(
            args.config,
            upstream_ids=args.upstream_ids,
            probe_path=args.path,
            timeout_seconds=args.timeout_seconds,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 0
    if args.command == "credits":
        payload = query_upstream_credits(
            args.config,
            upstream_ids=args.upstream_ids,
            timeout_seconds=args.timeout_seconds,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 0
    if args.command == "test":
        payload = exercise_upstreams(
            args.config,
            upstream_ids=args.upstream_ids,
            timeout_seconds=args.timeout_seconds,
        )
        print(json.dumps(payload, ensure_ascii=False))
        return 0
    raise ValueError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())

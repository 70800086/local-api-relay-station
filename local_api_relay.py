from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import select
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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
    breaker_permitted: bool = True


@dataclass(frozen=True)
class RequestPlan:
    client: LocalClientConfig | None
    requested_model: str | None
    attempts: list[PlannedUpstreamRequest]
    error_status: int | None = None
    error_kind: str | None = None
    error_message: str | None = None


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
        if runtime.config.admin_key and self.headers.get("X-Relay-Admin-Key", "") != runtime.config.admin_key:
            self._send_json(403, {"error": {"message": "forbidden"}})
            return
        if self.path == "/_relay/health":
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
        if self.path == "/_relay/stats":
            self._send_json(200, runtime.usage_store.stats_summary())
            return
        if self.path == "/_relay/idle":
            self._send_json(200, runtime.usage_store.idle_status(runtime.config.idle_window_seconds))
            return
        self._send_json(404, {"error": {"message": "admin endpoint not found"}})

    def _handle_proxy(self, runtime: RuntimeLease) -> None:
        original_request_body = self._read_request_body()
        plan = plan_request_attempts(
            runtime.config,
            self.headers,
            original_request_body,
            runtime._runtime.circuit_breaker,
        )
        client_id = plan.client.client_id if plan.client is not None else None
        initial_upstream_id = plan.attempts[0].upstream.upstream_id if plan.attempts else None
        runtime.usage_store.request_started(client_id, initial_upstream_id, plan.requested_model)
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

            attempt_failures: list[UpstreamAttemptFailure] = []
            for index, attempt in enumerate(plan.attempts):
                breaker_lease = runtime._runtime.circuit_breaker.acquire(attempt.upstream.upstream_id)
                if breaker_lease is None:
                    if index + 1 < len(plan.attempts):
                        continue
                    self._send_json(503, {"error": {"message": "all upstreams temporarily unavailable"}})
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
                        prompt_tokens, completion_tokens, total_tokens = _extract_usage_metrics(
                            response.getheader("Content-Type", ""),
                            captured_body,
                        )
                        runtime.usage_store.record_request(
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
                        breaker_lease.mark_success()
                        return

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
                        requested_model=plan.requested_model,
                        method=self.command,
                        path=self.path,
                        status_code=int(response.status),
                        forwarded=True,
                        request_bytes=len(attempt.request_body),
                        response_bytes=response_bytes,
                        upstream_ms=_duration_ms(attempt_started_at, finished_at),
                        error_kind="upstream_status_fallback" if can_retry else "upstream_status_exhausted",
                        prompt_tokens=prompt_tokens,
                        completion_tokens=completion_tokens,
                        total_tokens=total_tokens,
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
                self._send_exhausted_attempts(attempt_failures)
                return
            self._send_json(502, {"error": {"message": "upstream request failed"}})
        finally:
            runtime.usage_store.request_finished(client_id, initial_upstream_id, plan.requested_model)

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
        request_timeout_seconds: int,
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
            self.runtime_manager.active_runtime().usage_store.close()

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
    skipped_breaker_upstream_ids: list[str] = []
    seen_upstream_ids: set[str] = set()
    ordered_upstream_ids = (
        circuit_breaker.prioritized_upstream_ids(config.order)
        if circuit_breaker is not None
        else list(config.order)
    )
    for upstream_id in ordered_upstream_ids:
        if not upstream_id or upstream_id in seen_upstream_ids:
            continue
        seen_upstream_ids.add(upstream_id)
        upstream = config.upstreams_by_id[upstream_id]
        if not upstream.enabled:
            continue
        breaker_permitted = True
        if circuit_breaker is not None and not circuit_breaker.permits_request(upstream_id):
            skipped_breaker_upstream_ids.append(upstream_id)
            breaker_permitted = False
        attempts.append(
            PlannedUpstreamRequest(
                upstream=upstream,
                request_body=request_body,
                breaker_permitted=breaker_permitted,
            )
        )

    attempts = [attempt for attempt in attempts if attempt.breaker_permitted]

    if attempts:
        return RequestPlan(
            client=client,
            requested_model=requested_model,
            attempts=attempts,
        )

    if skipped_breaker_upstream_ids:
        return RequestPlan(
            client=client,
            requested_model=requested_model,
            attempts=[],
            error_status=503,
            error_kind="all_upstreams_open",
            error_message="all upstreams temporarily unavailable",
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


def _hash_config_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local API relay")
    parser.add_argument("--config", required=True, help="Path to relay config JSON")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    service = RelayService(args.config)
    try:
        service.serve_forever()
    except KeyboardInterrupt:
        service.shutdown()


if __name__ == "__main__":
    main()

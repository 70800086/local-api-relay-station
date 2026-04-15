from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


TASK_BOARD_HEARTBEAT_WINDOW = timedelta(minutes=15)
TASK_BOARD_PROGRESS_WINDOW = timedelta(minutes=60)
RELAY_REQUEST_ACTIVITY_WINDOW = timedelta(minutes=15)
RELAY_SUCCESS_ACTIVITY_WINDOW = timedelta(minutes=30)
SIGNAL_CHANGE_FIELDS = {"next_action", "blocker", "done_criteria"}


class TaskLike(Protocol):
    status: str
    heartbeat_at: datetime | None
    last_progress_at: datetime | None
    next_action: str
    recent_changes: set[str]


@dataclass(frozen=True)
class RelayClientActivity:
    client_id: str
    last_request_at: datetime | None
    last_success_at: datetime | None


@dataclass(frozen=True)
class ActivityAssessment:
    is_active: bool
    is_stale: bool
    task_board_active: bool
    relay_active: bool
    reason: str


def assess_task_activity(
    task: TaskLike,
    now: datetime,
    *,
    relay_activity: RelayClientActivity | None = None,
    changed_fields: set[str] | None = None,
) -> ActivityAssessment:
    task_board_reason = _task_board_activity_reason(task, now, changed_fields=changed_fields)
    if task_board_reason is not None:
        return ActivityAssessment(
            is_active=True,
            is_stale=False,
            task_board_active=True,
            relay_active=False,
            reason=task_board_reason,
        )

    relay_reason = _relay_activity_reason(relay_activity, now, task=task)
    if relay_reason is not None:
        return ActivityAssessment(
            is_active=True,
            is_stale=False,
            task_board_active=False,
            relay_active=True,
            reason=relay_reason,
        )

    if task.status != "DOING":
        return ActivityAssessment(
            is_active=False,
            is_stale=False,
            task_board_active=False,
            relay_active=False,
            reason="not_doing",
        )

    if task.last_progress_at is None:
        return ActivityAssessment(
            is_active=False,
            is_stale=True,
            task_board_active=False,
            relay_active=False,
            reason="missing_progress",
        )

    if now - task.last_progress_at > TASK_BOARD_PROGRESS_WINDOW:
        return ActivityAssessment(
            is_active=False,
            is_stale=True,
            task_board_active=False,
            relay_active=False,
            reason="stale_progress",
        )

    if next_action_too_large(task.next_action):
        return ActivityAssessment(
            is_active=False,
            is_stale=True,
            task_board_active=False,
            relay_active=False,
            reason="next_action_too_large",
        )

    return ActivityAssessment(
        is_active=False,
        is_stale=False,
        task_board_active=False,
        relay_active=False,
        reason="waiting_for_progress",
    )


def relay_client_activity_from_stats_payload(
    payload: dict[str, Any],
    *,
    client_id: str = "openclaw",
) -> RelayClientActivity | None:
    clients = payload.get("clients")
    if not isinstance(clients, list):
        return None

    for client_payload in clients:
        if str(client_payload.get("client_id", "")) != client_id:
            continue
        return RelayClientActivity(
            client_id=client_id,
            last_request_at=_maybe_parse_iso_datetime(client_payload.get("last_request_at")),
            last_success_at=_maybe_parse_iso_datetime(client_payload.get("last_success_at")),
        )

    return None


def load_local_relay_activity(
    config_path: str | Path | None = None,
    *,
    client_id: str = "openclaw",
    timeout_seconds: float = 1.5,
) -> RelayClientActivity | None:
    if config_path is None:
        config_path = resolve_local_relay_config_path()

    config = _read_relay_config(config_path)
    if config is None:
        return None

    server = config.get("server")
    if not isinstance(server, dict):
        return None

    admin_key = str(server.get("admin_key") or "").strip()
    if not admin_key:
        return None

    host = str(server.get("listen_host") or "127.0.0.1")
    try:
        port = int(server.get("listen_port") or 8787)
    except (TypeError, ValueError):
        return None

    stats_url = f"http://{host}:{port}/_relay/stats"
    return fetch_relay_client_activity(
        stats_url,
        admin_key=admin_key,
        client_id=client_id,
        timeout_seconds=timeout_seconds,
    )


def resolve_local_relay_config_path(workspace_root: str | Path | None = None) -> Path:
    root = Path(workspace_root) if workspace_root is not None else Path(__file__).resolve().parent
    relay_subdir_candidate = root / "relay" / "local_api_relay.json"
    legacy_root_candidate = root / "local_api_relay.json"
    if relay_subdir_candidate.exists():
        return relay_subdir_candidate
    if legacy_root_candidate.exists():
        return legacy_root_candidate
    return relay_subdir_candidate


def fetch_relay_client_activity(
    stats_url: str,
    *,
    admin_key: str,
    client_id: str = "openclaw",
    timeout_seconds: float = 1.5,
) -> RelayClientActivity | None:
    request = Request(stats_url, headers={"X-Relay-Admin-Key": admin_key})
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = json.load(response)
    except (HTTPError, URLError, OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    return relay_client_activity_from_stats_payload(payload, client_id=client_id)


def next_action_too_large(next_action: str) -> bool:
    if not next_action.strip():
        return True
    markers = ("，", "；", ";", "并", "然后", "再", "以及", "、")
    return any(marker in next_action for marker in markers)


def _task_board_activity_reason(
    task: TaskLike,
    now: datetime,
    *,
    changed_fields: set[str] | None,
) -> str | None:
    if task.status != "DOING" or task.heartbeat_at is None:
        return None

    if now - task.heartbeat_at > TASK_BOARD_HEARTBEAT_WINDOW:
        return None

    observed_changes = changed_fields or task.recent_changes
    if task.last_progress_at is not None and now - task.last_progress_at <= TASK_BOARD_PROGRESS_WINDOW:
        return "task_board_recent_progress"

    if SIGNAL_CHANGE_FIELDS & set(observed_changes):
        return "task_board_recent_change"

    return None


def _relay_activity_reason(
    activity: RelayClientActivity | None,
    now: datetime,
    *,
    task: TaskLike | None = None,
) -> str | None:
    if activity is None:
        return None

    if task is not None:
        if task.heartbeat_at is None:
            return None
        if now - task.heartbeat_at > TASK_BOARD_HEARTBEAT_WINDOW:
            return None

    if activity.last_request_at is not None and now - activity.last_request_at <= RELAY_REQUEST_ACTIVITY_WINDOW:
        return "relay_recent_request"

    if activity.last_success_at is not None and now - activity.last_success_at <= RELAY_SUCCESS_ACTIVITY_WINDOW:
        return "relay_recent_success"

    return None


def _read_relay_config(path: str | Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _maybe_parse_iso_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed

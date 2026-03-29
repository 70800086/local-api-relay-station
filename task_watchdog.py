from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from task_activity import RelayClientActivity, load_local_relay_activity

TASKS_SERVICE_SRC = Path(__file__).resolve().parent / "tasks-service" / "src"
if TASKS_SERVICE_SRC.exists() and str(TASKS_SERVICE_SRC) not in sys.path:
    sys.path.insert(0, str(TASKS_SERVICE_SRC))

from tasks_service.models import ActivitySignal
from tasks_service.models import Task as TS_Task
from tasks_service.models import TaskSnapshot as TS_TaskSnapshot
from tasks_service.service import TasksService
from tasks_service.policy import TaskPolicy
from tasks_service.store.json_store import JsonTaskStore as TS_JsonTaskStore
from tasks_service.store.markdown_view import MarkdownTaskView as TS_MarkdownTaskView
from tasks_service.sync import detect_snapshot_conflicts as ts_detect_snapshot_conflicts
from tasks_service.sync import load_authoritative_snapshot as ts_load_authoritative_snapshot
from tasks_service.sync import resolve_conflicts as ts_resolve_conflicts
from tasks_service.sync import sync_snapshot_if_needed as ts_sync_snapshot_if_needed

NO_REPLY = "NO_REPLY"
RESUME_TASK = "RESUME_TASK"
NOTIFY_ONLY = "NOTIFY_ONLY"
NOTIFY = NOTIFY_ONLY


@dataclass
class TaskRecord:
    task_id: str
    status: str
    progress: int
    last_progress_at: datetime | None
    heartbeat_at: datetime | None
    blocker: str
    next_action: str
    done_criteria: str
    recent_changes: set[str] = field(default_factory=set, repr=False, compare=False)

    def clone(self) -> "TaskRecord":
        return TaskRecord(
            task_id=self.task_id,
            status=self.status,
            progress=self.progress,
            last_progress_at=self.last_progress_at,
            heartbeat_at=self.heartbeat_at,
            blocker=self.blocker,
            next_action=self.next_action,
            done_criteria=self.done_criteria,
            recent_changes=set(self.recent_changes),
        )

    def sync_key(self) -> tuple[Any, ...]:
        return (
            self.task_id,
            self.status,
            self.progress,
            self.blocker,
            self.next_action,
            self.done_criteria,
        )


@dataclass
class TaskStateSnapshot:
    updated_at: datetime
    active_task_id: str
    tasks: list[TaskRecord]
    source: str
    needs_resync: bool = False
    baseline_signature: tuple[Any, ...] | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.baseline_signature is None:
            self.baseline_signature = self.sync_signature()

    def clone(self) -> "TaskStateSnapshot":
        return TaskStateSnapshot(
            updated_at=self.updated_at,
            active_task_id=self.active_task_id,
            tasks=[task.clone() for task in self.tasks],
            source=self.source,
            needs_resync=self.needs_resync,
            baseline_signature=self.baseline_signature,
        )

    def sync_signature(self) -> tuple[Any, ...]:
        return (
            self.active_task_id,
            tuple(task.sync_key() for task in self.tasks),
        )


@dataclass
class TickDecision:
    action: str
    reason: str
    changed: bool
    notify_text: str
    updated_snapshot: TaskStateSnapshot


@dataclass
class Conflict:
    task_id: str
    field: str
    json_value: Any
    md_value: Any


@dataclass
class WriteResult:
    written: bool
    reason: str
    path: str


class TaskStateLoadError(Exception):
    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


class _StaticTaskStore:
    def __init__(self, snapshot: TS_TaskSnapshot) -> None:
        self.snapshot = snapshot

    def load_snapshot(self) -> TS_TaskSnapshot:
        return self.snapshot

    def save_snapshot(self, snapshot: TS_TaskSnapshot) -> None:
        self.snapshot = snapshot


def _to_ts_snapshot(snapshot: TaskStateSnapshot) -> TS_TaskSnapshot:
    return TS_TaskSnapshot(
        updated_at=snapshot.updated_at,
        active_task_id=snapshot.active_task_id,
        tasks=[
            TS_Task(
                task_id=task.task_id,
                status=task.status,
                progress=task.progress,
                blocker=task.blocker,
                next_action=task.next_action,
                done_criteria=task.done_criteria,
                last_progress_at=task.last_progress_at,
                heartbeat_at=task.heartbeat_at,
                recent_changes=set(task.recent_changes),
            )
            for task in snapshot.tasks
        ],
        source=snapshot.source,
        needs_resync=snapshot.needs_resync,
        baseline_signature=snapshot.baseline_signature,
    )


def _from_ts_snapshot(snapshot: TS_TaskSnapshot) -> TaskStateSnapshot:
    return TaskStateSnapshot(
        updated_at=snapshot.updated_at,
        active_task_id=snapshot.active_task_id,
        tasks=[
            TaskRecord(
                task_id=task.task_id,
                status=task.status,
                progress=task.progress,
                last_progress_at=task.last_progress_at,
                heartbeat_at=task.heartbeat_at,
                blocker=task.blocker,
                next_action=task.next_action,
                done_criteria=task.done_criteria,
                recent_changes=set(task.recent_changes),
            )
            for task in snapshot.tasks
        ],
        source=snapshot.source,
        needs_resync=snapshot.needs_resync,
        baseline_signature=snapshot.baseline_signature,
    )


def _relay_activity_to_signals(relay_activity: RelayClientActivity | None) -> list[ActivitySignal]:
    if relay_activity is None:
        return []
    signals: list[ActivitySignal] = []
    if relay_activity.last_request_at is not None:
        signals.append(ActivitySignal(source="relay", kind="recent_request", last_seen_at=relay_activity.last_request_at))
    if relay_activity.last_success_at is not None:
        signals.append(ActivitySignal(source="relay", kind="recent_success", last_seen_at=relay_activity.last_success_at))
    return signals


def read_tasks_json(path: str | Path) -> TaskStateSnapshot:
    try:
        return _from_ts_snapshot(TS_JsonTaskStore(path).load_snapshot())
    except Exception as exc:
        kind = getattr(exc, "kind", "load_error")
        raise TaskStateLoadError(kind, str(exc)) from exc


def read_tasks_md(path: str | Path) -> TaskStateSnapshot:
    try:
        return _from_ts_snapshot(TS_MarkdownTaskView(path).read_snapshot())
    except Exception as exc:
        kind = getattr(exc, "kind", "load_error")
        raise TaskStateLoadError(kind, str(exc)) from exc


def load_authoritative_snapshot(
    now: datetime,
    json_path: str | Path,
    md_path: str | Path,
) -> TaskStateSnapshot:
    try:
        return _from_ts_snapshot(ts_load_authoritative_snapshot(now, json_path, md_path))
    except Exception as exc:
        kind = getattr(exc, "kind", "load_error")
        raise TaskStateLoadError(kind, str(exc)) from exc


def detect_snapshot_conflicts(
    json_snapshot: TaskStateSnapshot,
    md_snapshot: TaskStateSnapshot,
) -> list[Conflict]:
    return [
        Conflict(task_id=item.task_id, field=item.field, json_value=item.json_value, md_value=item.md_value)
        for item in ts_detect_snapshot_conflicts(_to_ts_snapshot(json_snapshot), _to_ts_snapshot(md_snapshot))
    ]


def resolve_conflicts(
    json_snapshot: TaskStateSnapshot,
    md_snapshot: TaskStateSnapshot,
) -> TaskStateSnapshot:
    return _from_ts_snapshot(ts_resolve_conflicts(_to_ts_snapshot(json_snapshot), _to_ts_snapshot(md_snapshot)))


def find_active_task(snapshot: TaskStateSnapshot) -> TaskRecord | None:
    if not snapshot.active_task_id:
        return None
    for task in snapshot.tasks:
        if task.task_id == snapshot.active_task_id:
            return task
    return None


def select_next_runnable_task(snapshot: TaskStateSnapshot) -> TaskRecord | None:
    for task in snapshot.tasks:
        if task.status == "TODO":
            return task
    return None


def shrink_next_action(task: TaskRecord) -> TaskRecord:
    service = TasksService(_StaticTaskStore(_to_ts_snapshot(TaskStateSnapshot(datetime.now(task.last_progress_at.tzinfo if task.last_progress_at else ZoneInfo('Asia/Shanghai')), task.task_id, [task.clone()], 'compat'))), TaskPolicy())
    shrunk = service.shrink_next_action(task)
    return shrunk


def decide_watchdog_tick(
    snapshot: TaskStateSnapshot,
    now: datetime,
    relay_activity: RelayClientActivity | None = None,
) -> TickDecision:
    signals = _relay_activity_to_signals(relay_activity)
    service = TasksService(_StaticTaskStore(_to_ts_snapshot(snapshot)), TaskPolicy())
    ts_decision = service.decide_tick(now, signals=signals)
    updated_snapshot = _from_ts_snapshot(ts_decision.snapshot)
    notify_text = ts_decision.notify_text
    if ts_decision.action == NOTIFY_ONLY and ts_decision.reason == "blocked":
        active_task = find_active_task(updated_snapshot)
        if active_task is not None:
            notify_text = (
                f"{active_task.task_id} -> {active_task.status}（DOING / {active_task.progress}%）："
                f"下一步：{active_task.next_action}。完成标准：{active_task.done_criteria}"
            )
    return TickDecision(
        action=ts_decision.action,
        reason=ts_decision.reason,
        changed=ts_decision.changed,
        notify_text=notify_text,
        updated_snapshot=updated_snapshot,
    )


def sync_snapshot_if_needed(snapshot: TaskStateSnapshot, json_path: str | Path) -> WriteResult:
    result = ts_sync_snapshot_if_needed(_to_ts_snapshot(snapshot), json_path)
    return WriteResult(written=result.written, reason=result.reason, path=result.path)


def render_notify_text(decision: TickDecision) -> str:
    if decision.action == NO_REPLY:
        return ""
    return decision.notify_text


def run_watchdog_tick(
    now: datetime,
    json_path: str | Path = Path("state") / "tasks.json",
    md_path: str | Path = Path("TASKS.md"),
    relay_activity: RelayClientActivity | None = None,
    load_relay: bool = True,
) -> TickDecision:
    snapshot = load_authoritative_snapshot(now, json_path, md_path)
    if relay_activity is None and load_relay:
        relay_activity = load_local_relay_activity()
    decision = decide_watchdog_tick(snapshot, now, relay_activity=relay_activity)
    sync_snapshot_if_needed(decision.updated_snapshot, json_path)
    return decision


def _parse_now(value: str | None) -> datetime:
    if value:
        return datetime.fromisoformat(value)
    return datetime.now(ZoneInfo("Asia/Shanghai"))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one structured TASKS watchdog tick.")
    parser.add_argument("--tasks-json", default=str(Path("state") / "tasks.json"))
    parser.add_argument("--tasks-md", default="TASKS.md")
    parser.add_argument("--now", help="ISO datetime override, for tests or dry runs")
    args = parser.parse_args()

    decision = run_watchdog_tick(
        now=_parse_now(args.now),
        json_path=args.tasks_json,
        md_path=args.tasks_md,
    )
    print(
        json.dumps(
            {
                "action": decision.action,
                "reason": decision.reason,
                "changed": decision.changed,
                "notify_text": render_notify_text(decision),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

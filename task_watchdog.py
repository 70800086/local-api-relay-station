from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from task_activity import RelayClientActivity, assess_task_activity, load_local_relay_activity


VALID_STATUSES = {"TODO", "DOING", "BLOCKED", "DONE"}
STRUCTURAL_FIELDS = ("status", "progress", "blocker", "next_action", "done_criteria")
CONFLICT_FIELDS = ("status", "progress", "next_action", "done_criteria")
NO_REPLY = "NO_REPLY"
RESUME_TASK = "RESUME_TASK"
NOTIFY_ONLY = "NOTIFY_ONLY"
NOTIFY = NOTIFY_ONLY


class TaskStateLoadError(Exception):
    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


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

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status,
            "progress": self.progress,
            "last_progress_at": _serialize_datetime(self.last_progress_at),
            "heartbeat_at": _serialize_datetime(self.heartbeat_at),
            "blocker": self.blocker,
            "next_action": self.next_action,
            "done_criteria": self.done_criteria,
        }

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


def read_tasks_json(path: str | Path) -> TaskStateSnapshot:
    json_path = Path(path)
    if not json_path.exists():
        raise TaskStateLoadError("missing_file", f"Missing task state file: {json_path}")

    try:
        payload = json.loads(json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise TaskStateLoadError("invalid_json", str(exc)) from exc

    for field_name in ("updated_at", "active_task_id", "tasks"):
        if field_name not in payload:
            raise TaskStateLoadError("schema_error", f"Missing top-level field: {field_name}")

    tasks_payload = payload["tasks"]
    if not isinstance(tasks_payload, list):
        raise TaskStateLoadError("schema_error", "tasks must be a list")

    tasks: list[TaskRecord] = []
    for item in tasks_payload:
        task = _task_from_json(item)
        tasks.append(task)

    return TaskStateSnapshot(
        updated_at=_parse_iso_datetime(payload["updated_at"]),
        active_task_id=str(payload.get("active_task_id", "")),
        tasks=tasks,
        source="tasks_json",
    )


def read_tasks_md(path: str | Path) -> TaskStateSnapshot:
    tasks_path = Path(path)
    if not tasks_path.exists():
        raise TaskStateLoadError("missing_file", f"Missing tasks markdown file: {tasks_path}")

    text = tasks_path.read_text(encoding="utf-8")
    lines = text.splitlines()
    updated_at = _parse_tasks_md_updated_at(lines, tasks_path)
    active_task_id = _parse_active_task_id(text)
    tasks = _parse_active_tasks(lines, updated_at)

    if not active_task_id:
        active_task_id = next((task.task_id for task in tasks if task.status == "DOING"), "")

    return TaskStateSnapshot(
        updated_at=updated_at,
        active_task_id=active_task_id,
        tasks=tasks,
        source="tasks_md",
    )


def load_authoritative_snapshot(
    now: datetime,
    json_path: str | Path,
    md_path: str | Path,
) -> TaskStateSnapshot:
    try:
        json_snapshot = read_tasks_json(json_path)
    except TaskStateLoadError:
        md_snapshot = read_tasks_md(md_path)
        md_snapshot.needs_resync = True
        return md_snapshot

    if now - json_snapshot.updated_at > timedelta(minutes=30):
        md_snapshot = read_tasks_md(md_path)
        md_snapshot.needs_resync = True
        return md_snapshot

    try:
        md_snapshot = read_tasks_md(md_path)
    except TaskStateLoadError:
        return json_snapshot

    conflicts = detect_snapshot_conflicts(json_snapshot, md_snapshot)
    if conflicts:
        return resolve_conflicts(json_snapshot, md_snapshot)

    return json_snapshot


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


def detect_snapshot_conflicts(
    json_snapshot: TaskStateSnapshot,
    md_snapshot: TaskStateSnapshot,
) -> list[Conflict]:
    conflicts: list[Conflict] = []

    if json_snapshot.active_task_id != md_snapshot.active_task_id:
        conflicts.append(
            Conflict(
                task_id="__snapshot__",
                field="active_task_id",
                json_value=json_snapshot.active_task_id,
                md_value=md_snapshot.active_task_id,
            )
        )

    json_map = {task.task_id: task for task in json_snapshot.tasks}
    md_map = {task.task_id: task for task in md_snapshot.tasks}
    for task_id in sorted(set(json_map) | set(md_map)):
        if task_id not in json_map or task_id not in md_map:
            conflicts.append(
                Conflict(
                    task_id=task_id,
                    field="task_presence",
                    json_value=task_id in json_map,
                    md_value=task_id in md_map,
                )
            )
            continue
        json_task = json_map[task_id]
        md_task = md_map[task_id]
        for field_name in CONFLICT_FIELDS:
            if getattr(json_task, field_name) != getattr(md_task, field_name):
                conflicts.append(
                    Conflict(
                        task_id=task_id,
                        field=field_name,
                        json_value=getattr(json_task, field_name),
                        md_value=getattr(md_task, field_name),
                    )
                )

    return conflicts


def resolve_conflicts(
    json_snapshot: TaskStateSnapshot,
    md_snapshot: TaskStateSnapshot,
) -> TaskStateSnapshot:
    del json_snapshot
    resolved = md_snapshot.clone()
    resolved.source = "tasks_md"
    resolved.needs_resync = True
    return resolved


def shrink_next_action(task: TaskRecord) -> TaskRecord:
    updated = task.clone()
    candidate = _shrink_text(updated.next_action)
    if candidate and candidate != updated.next_action:
        updated.next_action = candidate
        updated.recent_changes.add("next_action")
    return updated


def decide_watchdog_tick(
    snapshot: TaskStateSnapshot,
    now: datetime,
    relay_activity: RelayClientActivity | None = None,
) -> TickDecision:
    updated_snapshot = snapshot.clone()
    active_task = find_active_task(updated_snapshot)

    if active_task:
        assessment = assess_task_activity(active_task, now, relay_activity=relay_activity)
        if assessment.is_active:
            return TickDecision(
                action=NO_REPLY,
                reason="doing",
                changed=False,
                notify_text="",
                updated_snapshot=updated_snapshot,
            )

        if not assessment.is_stale:
            active_task.heartbeat_at = now
            return TickDecision(
                action=NO_REPLY,
                reason="waiting",
                changed=False,
                notify_text="",
                updated_snapshot=updated_snapshot,
            )

        if active_task.blocker.strip():
            active_task.status = "BLOCKED"
            updated_snapshot.updated_at = now
            notify_text = _render_task_status_change(active_task, previous_status="DOING")
            return TickDecision(
                action=NOTIFY_ONLY,
                reason="blocked",
                changed=True,
                notify_text=notify_text,
                updated_snapshot=updated_snapshot,
            )

        shrunk_task = shrink_next_action(active_task)
        if shrunk_task.next_action != active_task.next_action:
            active_task.next_action = shrunk_task.next_action
            active_task.last_progress_at = now
            active_task.heartbeat_at = now
            active_task.recent_changes = set(shrunk_task.recent_changes)
            updated_snapshot.updated_at = now
            return TickDecision(
                action=NO_REPLY,
                reason="stale",
                changed=True,
                notify_text="",
                updated_snapshot=updated_snapshot,
            )

        active_task.heartbeat_at = now
        updated_snapshot.updated_at = now
        notify_text = _render_resume_task_text(active_task)
        return TickDecision(
            action=RESUME_TASK,
            reason="resume_task",
            changed=True,
            notify_text=notify_text,
            updated_snapshot=updated_snapshot,
        )

    next_task = select_next_runnable_task(updated_snapshot)
    if next_task is not None:
        previous_status = next_task.status
        next_task.status = "DOING"
        next_task.heartbeat_at = now
        next_task.last_progress_at = now
        updated_snapshot.active_task_id = next_task.task_id
        updated_snapshot.updated_at = now
        return TickDecision(
            action=RESUME_TASK,
            reason="switch_task",
            changed=True,
            notify_text=_render_resume_task_text(next_task, previous_status=previous_status),
            updated_snapshot=updated_snapshot,
        )

    return TickDecision(
        action=NO_REPLY,
        reason="no_runnable_task",
        changed=False,
        notify_text="",
        updated_snapshot=updated_snapshot,
    )


def write_tasks_json(snapshot: TaskStateSnapshot, path: str | Path) -> WriteResult:
    json_path = Path(path)
    json_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "updated_at": _serialize_datetime(snapshot.updated_at),
        "active_task_id": snapshot.active_task_id,
        "tasks": [task.to_json_dict() for task in snapshot.tasks],
    }
    content = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"

    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=json_path.parent,
        delete=False,
    ) as handle:
        handle.write(content)
        temp_name = handle.name

    os.replace(temp_name, json_path)
    return WriteResult(written=True, reason="written", path=str(json_path))


def sync_snapshot_if_needed(snapshot: TaskStateSnapshot, json_path: str | Path) -> WriteResult:
    current_signature = snapshot.sync_signature()
    if not snapshot.needs_resync and current_signature == snapshot.baseline_signature:
        return WriteResult(written=False, reason="no_structural_change", path=str(json_path))

    snapshot.updated_at = datetime.now(snapshot.updated_at.tzinfo or ZoneInfo("Asia/Shanghai"))
    result = write_tasks_json(snapshot, json_path)
    snapshot.baseline_signature = snapshot.sync_signature()
    snapshot.needs_resync = False
    return result


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


def _task_from_json(payload: dict[str, Any]) -> TaskRecord:
    for field_name in ("task_id", "status", "progress"):
        if field_name not in payload:
            raise TaskStateLoadError("schema_error", f"Missing task field: {field_name}")

    status = str(payload["status"])
    if status not in VALID_STATUSES:
        raise TaskStateLoadError("schema_error", f"Invalid status: {status}")

    progress = int(payload["progress"])
    if not 0 <= progress <= 100:
        raise TaskStateLoadError("schema_error", f"Invalid progress: {progress}")

    return TaskRecord(
        task_id=str(payload["task_id"]),
        status=status,
        progress=progress,
        last_progress_at=_maybe_parse_iso_datetime(payload.get("last_progress_at")),
        heartbeat_at=_maybe_parse_iso_datetime(payload.get("heartbeat_at")),
        blocker=str(payload.get("blocker", "")),
        next_action=str(payload.get("next_action", "")),
        done_criteria=str(payload.get("done_criteria", "")),
    )


def _parse_tasks_md_updated_at(lines: list[str], path: Path) -> datetime:
    for line in lines:
        match = re.match(
            r"^Last updated:\s*(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s+([A-Za-z_\-/+]+)\s*$",
            line.strip(),
        )
        if match:
            naive = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M")
            return naive.replace(tzinfo=ZoneInfo(match.group(2)))

    return datetime.fromtimestamp(path.stat().st_mtime, tz=ZoneInfo("Asia/Shanghai"))


def _parse_active_task_id(text: str) -> str:
    match = re.search(r"^- 当前主任务：`([^`]+)`", text, flags=re.MULTILINE)
    if match:
        return match.group(1)
    return ""


def _parse_active_tasks(lines: list[str], updated_at: datetime) -> list[TaskRecord]:
    in_active_tasks = False
    current_task_id: str | None = None
    current_fields: dict[str, str] = {}
    tasks: list[TaskRecord] = []

    for line in lines:
        stripped = line.strip()
        if stripped == "## Active Tasks":
            in_active_tasks = True
            continue
        if in_active_tasks and stripped.startswith("## ") and stripped != "## Active Tasks":
            break
        if not in_active_tasks:
            continue

        task_match = re.match(r"^###\s+([^\.\s]+)\.", stripped)
        if task_match:
            if current_task_id is not None:
                tasks.append(_task_from_md_fields(current_task_id, current_fields, updated_at))
            current_task_id = task_match.group(1)
            current_fields = {}
            continue

        if current_task_id is None or not stripped.startswith("-"):
            continue

        if stripped.startswith("- Status:"):
            current_fields["status"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- Progress:"):
            current_fields["progress"] = stripped.split(":", 1)[1].strip().rstrip("%")
        elif stripped.startswith("- Last progress:"):
            current_fields["last_progress_at"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- 下一步动作："):
            current_fields["next_action"] = stripped.split("：", 1)[1].strip()
        elif stripped.startswith("- 完成标准："):
            current_fields["done_criteria"] = stripped.split("：", 1)[1].strip()
        elif stripped.startswith("- Blocker:"):
            current_fields["blocker"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("- 阻塞"):
            current_fields["blocker"] = stripped.split("：", 1)[1].strip() if "：" in stripped else stripped.split(":", 1)[1].strip()

    if current_task_id is not None:
        tasks.append(_task_from_md_fields(current_task_id, current_fields, updated_at))

    return tasks


def _task_from_md_fields(task_id: str, fields: dict[str, str], updated_at: datetime) -> TaskRecord:
    status = fields.get("status", "TODO")
    if status not in VALID_STATUSES:
        raise TaskStateLoadError("schema_error", f"Invalid status in TASKS.md: {status}")

    progress = int(fields.get("progress", "0"))
    if not 0 <= progress <= 100:
        raise TaskStateLoadError("schema_error", f"Invalid progress in TASKS.md: {progress}")

    last_progress_at = _parse_last_progress(fields.get("last_progress_at"), updated_at.tzinfo or ZoneInfo("Asia/Shanghai"))
    heartbeat_at = last_progress_at or updated_at
    return TaskRecord(
        task_id=task_id,
        status=status,
        progress=progress,
        last_progress_at=last_progress_at,
        heartbeat_at=heartbeat_at,
        blocker=fields.get("blocker", ""),
        next_action=fields.get("next_action", ""),
        done_criteria=fields.get("done_criteria", ""),
    )


def _parse_last_progress(value: str | None, tzinfo: ZoneInfo) -> datetime | None:
    if not value:
        return None
    match = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})", value)
    if not match:
        return None
    naive = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M")
    return naive.replace(tzinfo=tzinfo)


def _parse_iso_datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError as exc:
        raise TaskStateLoadError("schema_error", f"Invalid datetime: {value}") from exc


def _maybe_parse_iso_datetime(value: str | None) -> datetime | None:
    if value in (None, ""):
        return None
    return _parse_iso_datetime(str(value))


def _serialize_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat(timespec="seconds")


def _shrink_text(text: str) -> str:
    candidate = text.strip()
    for delimiter in ("，", "；", ";", "。"):
        if delimiter in candidate:
            first = candidate.split(delimiter, 1)[0].strip()
            if first:
                return first
    for delimiter in ("并", "然后", "再", "以及", "、"):
        if delimiter in candidate:
            first = candidate.split(delimiter, 1)[0].strip()
            if first:
                return first
    return candidate


def _render_task_status_change(task: TaskRecord, previous_status: str) -> str:
    progress_text = f"{task.progress}%"
    return (
        f"{task.task_id} -> {task.status}（{previous_status} / {progress_text}）："
        f"下一步：{task.next_action}。完成标准：{task.done_criteria}"
    )


def _render_resume_task_text(task: TaskRecord, previous_status: str | None = None) -> str:
    status_note = f"（{previous_status} -> {task.status}）" if previous_status else f"（{task.status}）"
    return (
        f"恢复任务 {task.task_id}{status_note}："
        f"优先继续下一步：{task.next_action}。完成标准：{task.done_criteria}"
    )


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
    message = render_notify_text(decision)
    print(message or NO_REPLY)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

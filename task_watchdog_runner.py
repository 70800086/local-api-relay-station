from __future__ import annotations

import argparse
import json
import shlex
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from task_watchdog import (
    NO_REPLY,
    NOTIFY_ONLY,
    RESUME_TASK,
    TickDecision,
    _parse_now,
    find_active_task,
    run_watchdog_tick,
)


@dataclass(frozen=True)
class EventDispatchConfig:
    url: str | None
    token: str | None
    mode: str = "now"
    timeout_ms: int = 30_000
    expect_final: bool = False


@dataclass(frozen=True)
class CodexResumeConfig:
    enabled: bool = False
    command: str = "codex exec --full-auto"
    workdir: str | None = None
    timeout_seconds: int = 1800
    cooldown_minutes: int = 15
    state_path: str | None = None


@dataclass(frozen=True)
class RunnerResult:
    decision_action: str
    decision_reason: str
    decision_changed: bool
    notify_text: str
    event_dispatched: bool
    dispatched_command: list[str] | None
    codex_resumed: bool = False
    codex_command: list[str] | None = None
    codex_skipped: bool = False
    codex_skip_reason: str | None = None
    dispatch_error: str | None = None
    codex_error: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        return asdict(self)


def load_gateway_event_config(config_path: str | Path = Path("/root/.openclaw/openclaw.json")) -> EventDispatchConfig:
    try:
        payload = json.loads(Path(config_path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return EventDispatchConfig(url=None, token=None)

    gateway = payload.get("gateway") or {}
    auth = gateway.get("auth") or {}
    bind = gateway.get("bind") or "loopback"
    host = "127.0.0.1" if bind == "loopback" else str(bind)
    try:
        port = int(gateway.get("port") or 10376)
    except (TypeError, ValueError):
        port = 10376

    return EventDispatchConfig(
        url=f"ws://{host}:{port}",
        token=str(auth.get("token") or "").strip() or None,
    )


def build_system_event_text(decision: TickDecision) -> str:
    active_task = find_active_task(decision.updated_snapshot)
    lines = [
        "watchdog-triggered system check",
        f"action: {decision.action}",
        f"reason: {decision.reason}",
        f"changed: {str(decision.changed).lower()}",
        f"notify_text: {decision.notify_text or NO_REPLY}",
    ]
    if active_task is not None:
        lines.extend(
            [
                f"active_task_id: {active_task.task_id}",
                f"active_task_status: {active_task.status}",
                f"active_task_progress: {active_task.progress}",
                f"next_action: {active_task.next_action or '-'}",
                f"done_criteria: {active_task.done_criteria or '-'}",
                f"blocker: {active_task.blocker or '-'}",
            ]
        )
        if decision.action == RESUME_TASK:
            lines.extend(
                [
                    "resume_mode: continue_current_task",
                    "resume_instruction: 立即按 next_action 恢复执行；若需要编码代理，优先恢复/拉起对应 Codex 工作流。",
                ]
            )
    return "\n".join(lines)


def build_system_event_command(
    event_text: str,
    *,
    config: EventDispatchConfig,
) -> list[str]:
    command = [
        "openclaw",
        "system",
        "event",
        "--mode",
        config.mode,
        "--timeout",
        str(config.timeout_ms),
        "--text",
        event_text,
    ]
    if config.expect_final:
        command.append("--expect-final")
    if config.url:
        command.extend(["--url", config.url])
    if config.token:
        command.extend(["--token", config.token])
    return command


def dispatch_system_event(
    decision: TickDecision,
    *,
    config: EventDispatchConfig,
    run_fn: Any = subprocess.run,
) -> list[str]:
    event_text = build_system_event_text(decision)
    command = build_system_event_command(event_text, config=config)
    run_fn(command, check=True, text=True, capture_output=True)
    return command


def build_codex_resume_prompt(decision: TickDecision) -> str:
    active_task = find_active_task(decision.updated_snapshot)
    if active_task is None:
        raise ValueError("Cannot resume Codex without an active task")

    return (
        "你是被 watchdog 恢复拉起的 Codex 执行器。\n"
        f"当前任务: {active_task.task_id}\n"
        f"任务状态: {active_task.status}\n"
        f"当前进度: {active_task.progress}%\n"
        f"恢复原因: {decision.reason}\n"
        f"下一步动作: {active_task.next_action or '-'}\n"
        f"完成标准: {active_task.done_criteria or '-'}\n"
        f"阻塞项: {active_task.blocker or '-'}\n\n"
        "要求:\n"
        "1. 先围绕当前任务继续推进，不要切到别的任务。\n"
        "2. 先阅读必要文件，再做最小可执行推进。\n"
        "3. 如果能直接完成一个明确子步骤，就直接动手。\n"
        "4. 如果确实缺上下文，先从当前仓库内自行定位，不要先停下来提问。\n"
        "5. 完成后输出你做了什么、还剩什么。\n"
    )


def build_codex_resume_command(decision: TickDecision, *, config: CodexResumeConfig) -> list[str]:
    prompt = build_codex_resume_prompt(decision)
    command = shlex.split(config.command)
    command.append(prompt)
    return command


def _default_codex_state_path(config: CodexResumeConfig) -> Path:
    base = Path(config.workdir or ".")
    return base / "state" / "watchdog_codex_resume.json"


def load_codex_resume_state(path: str | Path) -> dict[str, Any]:
    state_path = Path(path)
    if not state_path.exists():
        return {"last_runs": {}}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"last_runs": {}}
    if not isinstance(payload, dict):
        return {"last_runs": {}}
    last_runs = payload.get("last_runs")
    if not isinstance(last_runs, dict):
        payload["last_runs"] = {}
    return payload


def save_codex_resume_state(path: str | Path, state: dict[str, Any]) -> None:
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def should_skip_codex_resume(
    decision: TickDecision,
    *,
    config: CodexResumeConfig,
    now: datetime,
) -> tuple[bool, str | None]:
    active_task = find_active_task(decision.updated_snapshot)
    if active_task is None:
        return True, "missing_active_task"

    state_path = Path(config.state_path) if config.state_path else _default_codex_state_path(config)
    state = load_codex_resume_state(state_path)
    task_state = (state.get("last_runs") or {}).get(active_task.task_id)
    if not isinstance(task_state, dict):
        return False, None

    last_started_at = task_state.get("started_at")
    if not last_started_at:
        return False, None

    try:
        started_at = datetime.fromisoformat(str(last_started_at))
    except ValueError:
        return False, None

    cooldown = timedelta(minutes=max(config.cooldown_minutes, 0))
    if now - started_at < cooldown:
        return True, f"cooldown_active:{active_task.task_id}"

    return False, None


def mark_codex_resume_started(decision: TickDecision, *, config: CodexResumeConfig, now: datetime) -> None:
    active_task = find_active_task(decision.updated_snapshot)
    if active_task is None:
        return
    state_path = Path(config.state_path) if config.state_path else _default_codex_state_path(config)
    state = load_codex_resume_state(state_path)
    last_runs = state.setdefault("last_runs", {})
    last_runs[active_task.task_id] = {
        "started_at": now.isoformat(timespec="seconds"),
        "reason": decision.reason,
        "next_action": active_task.next_action,
    }
    save_codex_resume_state(state_path, state)


def resume_with_codex(
    decision: TickDecision,
    *,
    config: CodexResumeConfig,
    run_fn: Any = subprocess.run,
) -> list[str]:
    command = build_codex_resume_command(decision, config=config)
    run_fn(
        command,
        check=True,
        text=True,
        capture_output=True,
        cwd=config.workdir or None,
        timeout=config.timeout_seconds,
    )
    return command


def run_timer_watchdog(
    *,
    now: datetime,
    tasks_json: str | Path = Path("state") / "tasks.json",
    tasks_md: str | Path = Path("TASKS.md"),
    config_path: str | Path = Path("/root/.openclaw/openclaw.json"),
    dispatch_events: bool = True,
    relay_activity: Any = ...,
    load_relay: bool = True,
    run_fn: Any = subprocess.run,
    codex_config: CodexResumeConfig | None = None,
) -> RunnerResult:
    watchdog_kwargs = {"now": now, "json_path": tasks_json, "md_path": tasks_md}
    if relay_activity is not ...:
        watchdog_kwargs["relay_activity"] = relay_activity
    watchdog_kwargs["load_relay"] = load_relay
    decision = run_watchdog_tick(**watchdog_kwargs)
    dispatched_command = None
    event_dispatched = False
    dispatch_error = None
    codex_command = None
    codex_resumed = False
    codex_skipped = False
    codex_skip_reason = None
    codex_error = None

    if dispatch_events and decision.action in {RESUME_TASK, NOTIFY_ONLY}:
        try:
            dispatched_command = dispatch_system_event(
                decision,
                config=load_gateway_event_config(config_path),
                run_fn=run_fn,
            )
            event_dispatched = True
        except subprocess.CalledProcessError as exc:
            dispatched_command = list(exc.cmd) if isinstance(exc.cmd, (list, tuple)) else None
            stderr = (exc.stderr or "").strip()
            stdout = (exc.stdout or "").strip()
            detail = stderr or stdout or str(exc)
            dispatch_error = f"system event dispatch failed: {detail}"

    codex_config = codex_config or CodexResumeConfig()
    if decision.action == RESUME_TASK and codex_config.enabled:
        skip, skip_reason = should_skip_codex_resume(decision, config=codex_config, now=now)
        if skip:
            codex_skipped = True
            codex_skip_reason = skip_reason
        else:
            try:
                codex_command = resume_with_codex(decision, config=codex_config, run_fn=run_fn)
                codex_resumed = True
                mark_codex_resume_started(decision, config=codex_config, now=now)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired, ValueError) as exc:
                if isinstance(exc, subprocess.CalledProcessError):
                    codex_command = list(exc.cmd) if isinstance(exc.cmd, (list, tuple)) else None
                    stderr = (exc.stderr or "").strip()
                    stdout = (exc.stdout or "").strip()
                    detail = stderr or stdout or str(exc)
                else:
                    detail = str(exc)
                codex_error = f"codex resume failed: {detail}"

    return RunnerResult(
        decision_action=decision.action,
        decision_reason=decision.reason,
        decision_changed=decision.changed,
        notify_text=decision.notify_text,
        event_dispatched=event_dispatched,
        dispatched_command=dispatched_command,
        codex_resumed=codex_resumed,
        codex_command=codex_command,
        codex_skipped=codex_skipped,
        codex_skip_reason=codex_skip_reason,
        dispatch_error=dispatch_error,
        codex_error=codex_error,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="System-timer watchdog runner that bridges into OpenClaw system events.")
    parser.add_argument("--tasks-json", default=str(Path("state") / "tasks.json"))
    parser.add_argument("--tasks-md", default="TASKS.md")
    parser.add_argument("--openclaw-config", default="/root/.openclaw/openclaw.json")
    parser.add_argument("--now", help="ISO datetime override, for tests or dry runs")
    parser.add_argument("--dry-run", action="store_true", help="Compute decision but do not dispatch system event")
    parser.add_argument("--no-relay", action="store_true", help="Disable relay activity loading for this run")
    parser.add_argument("--json", action="store_true", help="Print structured result as JSON")
    parser.add_argument("--resume-codex", action="store_true", help="When decision is RESUME_TASK, also run local codex exec")
    parser.add_argument("--codex-command", default="codex exec --full-auto", help="Base Codex command used for resume execution")
    parser.add_argument("--codex-workdir", default=".", help="Working directory for Codex resume execution")
    parser.add_argument("--codex-timeout", type=int, default=1800, help="Timeout seconds for Codex resume execution")
    parser.add_argument("--codex-cooldown-minutes", type=int, default=15, help="Cooldown window for the same task before Codex can be resumed again")
    parser.add_argument("--codex-state-path", default="", help="Optional path for Codex resume cooldown state")
    args = parser.parse_args()

    result = run_timer_watchdog(
        now=_parse_now(args.now),
        tasks_json=args.tasks_json,
        tasks_md=args.tasks_md,
        config_path=args.openclaw_config,
        dispatch_events=not args.dry_run,
        relay_activity=None if args.no_relay else ...,
        codex_config=CodexResumeConfig(
            enabled=args.resume_codex,
            command=args.codex_command,
            workdir=args.codex_workdir,
            timeout_seconds=args.codex_timeout,
            cooldown_minutes=args.codex_cooldown_minutes,
            state_path=args.codex_state_path or None,
        ),
    )
    if args.json:
        print(json.dumps(result.to_json_dict(), ensure_ascii=False, indent=2))
    else:
        print(result.notify_text or NO_REPLY)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

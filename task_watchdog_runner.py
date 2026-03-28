from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from task_watchdog import NO_REPLY, TickDecision, _parse_now, run_watchdog_tick


@dataclass(frozen=True)
class EventDispatchConfig:
    url: str | None
    token: str | None
    mode: str = "now"
    timeout_ms: int = 30_000
    expect_final: bool = False


@dataclass(frozen=True)
class RunnerResult:
    decision_action: str
    decision_reason: str
    decision_changed: bool
    notify_text: str
    event_dispatched: bool
    dispatched_command: list[str] | None
    dispatch_error: str | None = None

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
    return (
        "watchdog-triggered system check\n"
        f"reason: {decision.reason}\n"
        f"changed: {str(decision.changed).lower()}\n"
        f"notify_text: {decision.notify_text or NO_REPLY}"
    )


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
) -> RunnerResult:
    watchdog_kwargs = {"now": now, "json_path": tasks_json, "md_path": tasks_md}
    if relay_activity is not ...:
        watchdog_kwargs["relay_activity"] = relay_activity
    watchdog_kwargs["load_relay"] = load_relay
    decision = run_watchdog_tick(**watchdog_kwargs)
    dispatched_command = None
    event_dispatched = False
    dispatch_error = None
    if dispatch_events and decision.action != NO_REPLY:
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

    return RunnerResult(
        decision_action=decision.action,
        decision_reason=decision.reason,
        decision_changed=decision.changed,
        notify_text=decision.notify_text,
        event_dispatched=event_dispatched,
        dispatched_command=dispatched_command,
        dispatch_error=dispatch_error,
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
    args = parser.parse_args()

    result = run_timer_watchdog(
        now=_parse_now(args.now),
        tasks_json=args.tasks_json,
        tasks_md=args.tasks_md,
        config_path=args.openclaw_config,
        dispatch_events=not args.dry_run,
        relay_activity=None if args.no_relay else ...,
    )
    if args.json:
        print(json.dumps(result.to_json_dict(), ensure_ascii=False, indent=2))
    else:
        print(result.notify_text or NO_REPLY)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

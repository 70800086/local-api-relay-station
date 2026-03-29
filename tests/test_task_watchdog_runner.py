from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

from task_watchdog_runner import (
    CodexResumeConfig,
    build_codex_resume_command,
    build_codex_resume_prompt,
    build_system_event_command,
    load_codex_resume_state,
    load_gateway_event_config,
    run_timer_watchdog,
)
from task_watchdog import RESUME_TASK, TaskRecord, TaskStateSnapshot, TickDecision


class TaskWatchdogRunnerTests(unittest.TestCase):
    def test_load_gateway_event_config_reads_local_gateway_token(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            config_path = Path(tmp_dir) / "openclaw.json"
            config_path.write_text(
                json.dumps(
                    {
                        "gateway": {
                            "port": 10376,
                            "bind": "loopback",
                            "auth": {"mode": "token", "token": "abc123"},
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            config = load_gateway_event_config(config_path)

            self.assertEqual(config.url, "ws://127.0.0.1:10376")
            self.assertEqual(config.token, "abc123")

    def test_build_system_event_command_includes_gateway_credentials(self) -> None:
        config = load_gateway_event_config.__annotations__
        del config
        from task_watchdog_runner import EventDispatchConfig

        command = build_system_event_command(
            "watchdog-triggered system check",
            config=EventDispatchConfig(url="ws://127.0.0.1:10376", token="abc123"),
        )

        self.assertIn("openclaw", command[0])
        self.assertIn("system", command)
        self.assertIn("event", command)
        self.assertIn("--url", command)
        self.assertIn("ws://127.0.0.1:10376", command)
        self.assertIn("--token", command)
        self.assertIn("abc123", command)

    def test_build_codex_resume_prompt_contains_task_context(self) -> None:
        decision = TickDecision(
            action=RESUME_TASK,
            reason="resume_task",
            changed=True,
            notify_text="恢复任务 1v",
            updated_snapshot=TaskStateSnapshot(
                updated_at=__import__("datetime").datetime.fromisoformat("2026-03-29T10:00:00+08:00"),
                active_task_id="1v",
                tasks=[
                    TaskRecord(
                        task_id="1v",
                        status="DOING",
                        progress=5,
                        last_progress_at=None,
                        heartbeat_at=None,
                        blocker="",
                        next_action="阅读 task_watchdog_runner.py 并接入 Codex 恢复路径",
                        done_criteria="watchdog 能触发真实恢复执行",
                    )
                ],
                source="tasks_json",
            ),
        )

        prompt = build_codex_resume_prompt(decision)

        self.assertIn("当前任务: 1v", prompt)
        self.assertIn("下一步动作: 阅读 task_watchdog_runner.py 并接入 Codex 恢复路径", prompt)
        self.assertIn("完成标准: watchdog 能触发真实恢复执行", prompt)

    def test_build_codex_resume_command_appends_prompt(self) -> None:
        decision = TickDecision(
            action=RESUME_TASK,
            reason="resume_task",
            changed=True,
            notify_text="恢复任务 1v",
            updated_snapshot=TaskStateSnapshot(
                updated_at=__import__("datetime").datetime.fromisoformat("2026-03-29T10:00:00+08:00"),
                active_task_id="1v",
                tasks=[
                    TaskRecord(
                        task_id="1v",
                        status="DOING",
                        progress=5,
                        last_progress_at=None,
                        heartbeat_at=None,
                        blocker="",
                        next_action="接入 Codex 恢复路径",
                        done_criteria="watchdog 能触发真实恢复执行",
                    )
                ],
                source="tasks_json",
            ),
        )

        command = build_codex_resume_command(
            decision,
            config=CodexResumeConfig(enabled=True, command="codex exec --full-auto", workdir="/tmp"),
        )

        self.assertEqual(command[:3], ["codex", "exec", "--full-auto"])
        self.assertIn("当前任务: 1v", command[-1])

    def test_run_timer_watchdog_dispatches_system_event_when_watchdog_wants_notify(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            tasks_json = tmp / "tasks.json"
            tasks_json.write_text(
                json.dumps(
                    {
                        "updated_at": "2026-03-28T12:29:00+08:00",
                        "active_task_id": "6h",
                        "tasks": [
                            {
                                "task_id": "6h",
                                "status": "DOING",
                                "progress": 5,
                                "last_progress_at": "2026-03-28T10:00:00+08:00",
                                "heartbeat_at": "2026-03-28T10:00:00+08:00",
                                "blocker": "等待桥接入口落地",
                                "next_action": "继续推进",
                                "done_criteria": "timer 可触发 watchdog",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            tasks_md = tmp / "TASKS.md"
            tasks_md.write_text(
                "Last updated: 2026-03-28 12:29 Asia/Shanghai\n\n## Active Tasks\n\n### 6h. 实现系统定时器巡检并通过 OpenClaw 统一提醒\n- Status: DOING\n- Progress: 5%\n- 阻塞项：等待桥接入口落地\n- 下一步动作：实现 systemd timer 与 runner\n- 完成标准：timer 可触发 watchdog\n",
                encoding="utf-8",
            )
            config_path = tmp / "openclaw.json"
            config_path.write_text(
                json.dumps(
                    {
                        "gateway": {
                            "port": 10376,
                            "bind": "loopback",
                            "auth": {"mode": "token", "token": "abc123"},
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            run_fn = Mock()
            result = run_timer_watchdog(
                now=__import__("datetime").datetime.fromisoformat("2026-03-28T12:30:00+08:00"),
                tasks_json=tasks_json,
                tasks_md=tasks_md,
                config_path=config_path,
                dispatch_events=True,
                load_relay=False,
                run_fn=run_fn,
            )

            self.assertTrue(result.event_dispatched)
            self.assertEqual(result.decision_action, "NOTIFY_ONLY")
            run_fn.assert_called_once()
            called_command = run_fn.call_args.args[0]
            self.assertIn("openclaw", called_command[0])
            self.assertIn("system", called_command)
            self.assertIn("event", called_command)

    def test_run_timer_watchdog_records_dispatch_error_without_crashing(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            tasks_json = tmp / "tasks.json"
            tasks_json.write_text(
                json.dumps(
                    {
                        "updated_at": "2026-03-28T12:29:00+08:00",
                        "active_task_id": "6h",
                        "tasks": [
                            {
                                "task_id": "6h",
                                "status": "DOING",
                                "progress": 5,
                                "last_progress_at": "2026-03-28T10:00:00+08:00",
                                "heartbeat_at": "2026-03-28T10:00:00+08:00",
                                "blocker": "等待桥接入口落地",
                                "next_action": "继续推进",
                                "done_criteria": "timer 可触发 watchdog",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            tasks_md = tmp / "TASKS.md"
            tasks_md.write_text(
                "Last updated: 2026-03-28 12:29 Asia/Shanghai\n\n## Active Tasks\n\n### 6h. 实现系统定时器巡检并通过 OpenClaw 统一提醒\n- Status: DOING\n- Progress: 5%\n- 阻塞项：等待桥接入口落地\n- 下一步动作：实现 systemd timer 与 runner\n- 完成标准：timer 可触发 watchdog\n",
                encoding="utf-8",
            )
            config_path = tmp / "openclaw.json"
            config_path.write_text(
                json.dumps(
                    {
                        "gateway": {
                            "port": 10376,
                            "bind": "loopback",
                            "auth": {"mode": "token", "token": "abc123"},
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            run_fn = Mock(
                side_effect=subprocess.CalledProcessError(
                    1,
                    ["openclaw", "system", "event"],
                    stderr="boom",
                )
            )
            result = run_timer_watchdog(
                now=__import__("datetime").datetime.fromisoformat("2026-03-28T12:30:00+08:00"),
                tasks_json=tasks_json,
                tasks_md=tasks_md,
                config_path=config_path,
                dispatch_events=True,
                load_relay=False,
                run_fn=run_fn,
            )

            self.assertFalse(result.event_dispatched)
            self.assertEqual(result.decision_action, "NOTIFY_ONLY")
            self.assertIn("dispatch failed", result.dispatch_error)
            run_fn.assert_called_once()

    def test_run_timer_watchdog_dispatches_system_event_for_resume_task(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            tasks_json = tmp / "tasks.json"
            tasks_json.write_text(
                json.dumps(
                    {
                        "updated_at": "2026-03-28T12:29:00+08:00",
                        "active_task_id": "6h",
                        "tasks": [
                            {
                                "task_id": "6h",
                                "status": "DOING",
                                "progress": 5,
                                "last_progress_at": "2026-03-28T10:00:00+08:00",
                                "heartbeat_at": "2026-03-28T10:00:00+08:00",
                                "blocker": "",
                                "next_action": "继续推进",
                                "done_criteria": "timer 可触发 watchdog",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            tasks_md = tmp / "TASKS.md"
            tasks_md.write_text(
                "Last updated: 2026-03-28 12:29 Asia/Shanghai\n\n## Active Tasks\n\n### 6h. 实现系统定时器巡检并通过 OpenClaw 统一提醒\n- Status: DOING\n- Progress: 5%\n- 下一步动作：继续推进\n- 完成标准：timer 可触发 watchdog\n",
                encoding="utf-8",
            )
            run_fn = Mock()
            result = run_timer_watchdog(
                now=__import__("datetime").datetime.fromisoformat("2026-03-28T12:30:00+08:00"),
                tasks_json=tasks_json,
                tasks_md=tasks_md,
                dispatch_events=True,
                load_relay=False,
                run_fn=run_fn,
            )

            self.assertTrue(result.event_dispatched)
            self.assertEqual(result.decision_action, "RESUME_TASK")
            called_command = run_fn.call_args.args[0]
            self.assertIn("--text", called_command)
            event_text = called_command[called_command.index("--text") + 1]
            self.assertIn("action: RESUME_TASK", event_text)
            self.assertIn("active_task_id: 6h", event_text)
            self.assertIn("resume_mode: continue_current_task", event_text)

    def test_run_timer_watchdog_can_resume_with_codex_and_persist_state(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            state_path = tmp / "state" / "watchdog_codex_resume.json"
            tasks_json = tmp / "tasks.json"
            tasks_json.write_text(
                json.dumps(
                    {
                        "updated_at": "2026-03-28T12:29:00+08:00",
                        "active_task_id": "6h",
                        "tasks": [
                            {
                                "task_id": "6h",
                                "status": "DOING",
                                "progress": 5,
                                "last_progress_at": "2026-03-28T10:00:00+08:00",
                                "heartbeat_at": "2026-03-28T10:00:00+08:00",
                                "blocker": "",
                                "next_action": "继续推进 watchdog 恢复路径",
                                "done_criteria": "timer 可触发真实恢复执行",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            tasks_md = tmp / "TASKS.md"
            tasks_md.write_text(
                "Last updated: 2026-03-28 12:29 Asia/Shanghai\n\n## Active Tasks\n\n### 6h. 实现系统定时器巡检并通过 OpenClaw 统一提醒\n- Status: DOING\n- Progress: 5%\n- 下一步动作：继续推进 watchdog 恢复路径\n- 完成标准：timer 可触发真实恢复执行\n",
                encoding="utf-8",
            )
            run_fn = Mock()
            result = run_timer_watchdog(
                now=__import__("datetime").datetime.fromisoformat("2026-03-28T12:30:00+08:00"),
                tasks_json=tasks_json,
                tasks_md=tasks_md,
                dispatch_events=False,
                load_relay=False,
                run_fn=run_fn,
                codex_config=CodexResumeConfig(
                    enabled=True,
                    command="codex exec --full-auto",
                    workdir=str(tmp),
                    state_path=str(state_path),
                    cooldown_minutes=15,
                ),
            )

            self.assertTrue(result.codex_resumed)
            self.assertFalse(result.codex_skipped)
            self.assertIsNotNone(result.codex_command)
            self.assertEqual(result.codex_command[:3], ["codex", "exec", "--full-auto"])
            self.assertIn("当前任务: 6h", result.codex_command[-1])
            saved = load_codex_resume_state(state_path)
            self.assertIn("6h", saved["last_runs"])
            run_fn.assert_called_once()

    def test_run_timer_watchdog_skips_codex_resume_during_cooldown(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            state_path = tmp / "state" / "watchdog_codex_resume.json"
            state_path.parent.mkdir(parents=True, exist_ok=True)
            state_path.write_text(
                json.dumps(
                    {
                        "last_runs": {
                            "6h": {
                                "started_at": "2026-03-28T12:25:00+08:00",
                                "reason": "resume_task",
                                "next_action": "继续推进 watchdog 恢复路径",
                            }
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ) + "\n",
                encoding="utf-8",
            )
            tasks_json = tmp / "tasks.json"
            tasks_json.write_text(
                json.dumps(
                    {
                        "updated_at": "2026-03-28T12:29:00+08:00",
                        "active_task_id": "6h",
                        "tasks": [
                            {
                                "task_id": "6h",
                                "status": "DOING",
                                "progress": 5,
                                "last_progress_at": "2026-03-28T10:00:00+08:00",
                                "heartbeat_at": "2026-03-28T10:00:00+08:00",
                                "blocker": "",
                                "next_action": "继续推进 watchdog 恢复路径",
                                "done_criteria": "timer 可触发真实恢复执行",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            tasks_md = tmp / "TASKS.md"
            tasks_md.write_text(
                "Last updated: 2026-03-28 12:29 Asia/Shanghai\n\n## Active Tasks\n\n### 6h. 实现系统定时器巡检并通过 OpenClaw 统一提醒\n- Status: DOING\n- Progress: 5%\n- 下一步动作：继续推进 watchdog 恢复路径\n- 完成标准：timer 可触发真实恢复执行\n",
                encoding="utf-8",
            )
            run_fn = Mock()
            result = run_timer_watchdog(
                now=__import__("datetime").datetime.fromisoformat("2026-03-28T12:30:00+08:00"),
                tasks_json=tasks_json,
                tasks_md=tasks_md,
                dispatch_events=False,
                load_relay=False,
                run_fn=run_fn,
                codex_config=CodexResumeConfig(
                    enabled=True,
                    command="codex exec --full-auto",
                    workdir=str(tmp),
                    state_path=str(state_path),
                    cooldown_minutes=15,
                ),
            )

            self.assertFalse(result.codex_resumed)
            self.assertTrue(result.codex_skipped)
            self.assertEqual(result.codex_skip_reason, "cooldown_active:6h")
            run_fn.assert_not_called()

    def test_run_timer_watchdog_skips_dispatch_when_no_reply(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            tasks_json = tmp / "tasks.json"
            tasks_json.write_text(
                json.dumps(
                    {
                        "updated_at": "2026-03-28T12:00:00+08:00",
                        "active_task_id": "6h",
                        "tasks": [
                            {
                                "task_id": "6h",
                                "status": "DOING",
                                "progress": 50,
                                "last_progress_at": "2026-03-28T12:20:00+08:00",
                                "heartbeat_at": "2026-03-28T12:20:00+08:00",
                                "blocker": "",
                                "next_action": "继续实现 timer",
                                "done_criteria": "timer 可触发 watchdog",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            tasks_md = tmp / "TASKS.md"
            tasks_md.write_text(
                "Last updated: 2026-03-28 12:20 Asia/Shanghai\n\n## Active Tasks\n\n### 6h. 实现系统定时器巡检并通过 OpenClaw 统一提醒\n- Status: DOING\n- Progress: 50%\n- 下一步动作：继续实现 timer\n- 完成标准：timer 可触发 watchdog\n",
                encoding="utf-8",
            )
            run_fn = Mock()
            result = run_timer_watchdog(
                now=__import__("datetime").datetime.fromisoformat("2026-03-28T12:25:00+08:00"),
                tasks_json=tasks_json,
                tasks_md=tasks_md,
                dispatch_events=True,
                run_fn=run_fn,
            )

            self.assertFalse(result.event_dispatched)
            self.assertEqual(result.decision_action, "NO_REPLY")
            run_fn.assert_not_called()


if __name__ == "__main__":
    unittest.main()

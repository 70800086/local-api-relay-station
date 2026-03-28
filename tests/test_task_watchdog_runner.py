from __future__ import annotations

import json
import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

from task_watchdog_runner import (
    build_system_event_command,
    build_system_event_text,
    load_gateway_event_config,
    run_timer_watchdog,
)


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
        config = load_gateway_event_config.__annotations__  # satisfy lint-like tools
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
                "Last updated: 2026-03-28 12:29 Asia/Shanghai\n\n## Active Tasks\n\n### 6h. 实现系统定时器巡检并通过 OpenClaw 统一提醒\n- Status: DOING\n- Progress: 5%\n- 下一步动作：实现 systemd timer 与 runner\n- 完成标准：timer 可触发 watchdog\n",
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
            self.assertEqual(result.decision_action, "NOTIFY")
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
                "Last updated: 2026-03-28 12:29 Asia/Shanghai\n\n## Active Tasks\n\n### 6h. 实现系统定时器巡检并通过 OpenClaw 统一提醒\n- Status: DOING\n- Progress: 5%\n- 下一步动作：实现 systemd timer 与 runner\n- 完成标准：timer 可触发 watchdog\n",
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
            self.assertEqual(result.decision_action, "NOTIFY")
            self.assertIn("dispatch failed", result.dispatch_error)
            run_fn.assert_called_once()

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

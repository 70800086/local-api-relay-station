import json
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

from task_watchdog import (
    TaskRecord,
    TaskStateSnapshot,
    decide_watchdog_tick,
    find_active_task,
    load_authoritative_snapshot,
    read_tasks_json,
    render_notify_text,
    sync_snapshot_if_needed,
)
from task_activity import RelayClientActivity


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 3, 27, 9, 56, tzinfo=TZ)

TASKS_MD = """# TASKS.md

Last updated: 2026-03-27 08:40 Asia/Shanghai

## Active Tasks

### 3b-9. 为 validation 层补对照实验与效果汇总输出
- Status: BLOCKED
- Progress: 30%
- Priority: P2
- Type: build
- Last progress: 2026-03-27 04:00 已确认旧任务过大
- 下一步动作：等待拆分为更小可执行子任务后恢复
- 完成标准：至少能输出一组可复核的对照实验结果与指标汇总

### 4a. 将任务推进判断从“模型读 `TASKS.md`”升级为“结构化状态 + 规则判定"
- Status: DOING
- Progress: 90%
- Priority: P0
- Type: ops
- Last progress: 2026-03-27 08:40 已完成接口草图收口
- 下一步动作：把读取、判定、回退、同步写回 helper 落成代码与测试，并补齐仅 heartbeat 不强制写盘约束
- 完成标准：形成一版可执行的结构化状态方案草案，足以指导后续实现巡检规则优先判定

### 5a. 升级 OpenClaw 到最新稳定版本
- Status: TODO
- Progress: 0%
- Priority: P1
- Type: ops
- Last progress: 2026-03-27 00:46 待确认版本信息
- 下一步动作：先检查当前 OpenClaw 版本、可升级目标版本及本地安装来源，再制定最小风险升级步骤与验证项
- 完成标准：OpenClaw 已升级完成，关键能力验证通过，且如有配置迁移/兼容变化已记录

### 5b. 升级微信插件到最新稳定版本
- Status: TODO
- Progress: 0%
- Priority: P1
- Type: ops
- Last progress: 2026-03-27 00:46 待确认插件版本信息
- 下一步动作：先检查当前微信插件版本、来源、可升级目标与兼容关系，再制定最小风险升级步骤与验证项
- 完成标准：微信插件已升级完成，消息收发与投递验证通过，且如有配置变化已记录

## Current Snapshot

- 当前主任务：`4a` 将任务推进判断从“模型读 `TASKS.md`”升级为“结构化状态 + 规则判定"
- 当前可执行任务：把 `4a` 的 helper 与规则判定链路落成最小可执行代码
- 当前阻塞项：`3b-9` 已因粒度过大暂时 BLOCKED
- 当前最有价值的下一步：先把结构化状态读写和规则判定 helper 代码化
"""


def write_tasks_fixture(base: Path) -> Path:
    tasks_path = base / "TASKS.md"
    tasks_path.write_text(TASKS_MD, encoding="utf-8")
    return tasks_path


class TaskWatchdogTests(unittest.TestCase):
    def test_load_authoritative_snapshot_falls_back_to_md_when_json_missing(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            tasks_path = write_tasks_fixture(tmp)

            snapshot = load_authoritative_snapshot(
                NOW,
                tmp / "state" / "tasks.json",
                tasks_path,
            )

            self.assertEqual(snapshot.source, "tasks_md")
            self.assertTrue(snapshot.needs_resync)
            self.assertEqual(snapshot.active_task_id, "4a")
            self.assertEqual([task.task_id for task in snapshot.tasks], ["3b-9", "4a", "5a", "5b"])

    def test_load_authoritative_snapshot_prefers_md_when_json_conflicts(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            state_dir = tmp / "state"
            state_dir.mkdir()
            tasks_path = write_tasks_fixture(tmp)
            json_path = state_dir / "tasks.json"
            json_path.write_text(
                json.dumps(
                    {
                        "updated_at": "2026-03-27T08:40:00+08:00",
                        "active_task_id": "4a",
                        "tasks": [
                            {
                                "task_id": "4a",
                                "status": "DOING",
                                "progress": 75,
                                "last_progress_at": "2026-03-27T08:40:00+08:00",
                                "heartbeat_at": "2026-03-27T08:45:00+08:00",
                                "blocker": "",
                                "next_action": "旧动作",
                                "done_criteria": "旧完成标准",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            snapshot = load_authoritative_snapshot(NOW, json_path, tasks_path)

            active_task = find_active_task(snapshot)
            self.assertEqual(snapshot.source, "tasks_md")
            self.assertTrue(snapshot.needs_resync)
            self.assertIsNotNone(active_task)
            self.assertEqual(active_task.progress, 90)
            self.assertIn("helper", active_task.next_action)

    def test_decide_watchdog_tick_returns_no_reply_for_fresh_doing_task(self) -> None:
        snapshot = TaskStateSnapshot(
            updated_at=NOW - timedelta(minutes=5),
            active_task_id="4a",
            tasks=[
                TaskRecord(
                    task_id="4a",
                    status="DOING",
                    progress=90,
                    last_progress_at=NOW - timedelta(minutes=30),
                    heartbeat_at=NOW - timedelta(minutes=5),
                    blocker="",
                    next_action="补齐测试",
                    done_criteria="规则可执行",
                )
            ],
            source="tasks_json",
        )

        decision = decide_watchdog_tick(snapshot, NOW)

        self.assertEqual(decision.action, "NO_REPLY")
        self.assertEqual(decision.reason, "doing")
        self.assertFalse(decision.changed)
        self.assertEqual(render_notify_text(decision), "")

    def test_decide_watchdog_tick_marks_blocked_when_stale_task_has_blocker(self) -> None:
        snapshot = TaskStateSnapshot(
            updated_at=NOW - timedelta(minutes=5),
            active_task_id="4a",
            tasks=[
                TaskRecord(
                    task_id="4a",
                    status="DOING",
                    progress=90,
                    last_progress_at=NOW - timedelta(hours=2),
                    heartbeat_at=NOW - timedelta(minutes=5),
                    blocker="等待外部环境修复",
                    next_action="继续推进",
                    done_criteria="规则可执行",
                )
            ],
            source="tasks_json",
        )

        decision = decide_watchdog_tick(
            snapshot,
            NOW,
            relay_activity=RelayClientActivity(
                client_id="openclaw",
                last_request_at=NOW - timedelta(hours=2),
                last_success_at=NOW - timedelta(hours=2),
            ),
        )
        updated_task = find_active_task(decision.updated_snapshot)

        self.assertEqual(decision.action, "NOTIFY")
        self.assertEqual(decision.reason, "blocked")
        self.assertTrue(decision.changed)
        self.assertEqual(updated_task.status, "BLOCKED")
        self.assertIn("4a -> BLOCKED", render_notify_text(decision))

    def test_decide_watchdog_tick_returns_no_reply_when_only_relay_is_active(self) -> None:
        snapshot = TaskStateSnapshot(
            updated_at=NOW - timedelta(minutes=5),
            active_task_id="4a",
            tasks=[
                TaskRecord(
                    task_id="4a",
                    status="DOING",
                    progress=90,
                    last_progress_at=NOW - timedelta(hours=2),
                    heartbeat_at=NOW - timedelta(minutes=20),
                    blocker="",
                    next_action="继续推进",
                    done_criteria="规则可执行",
                )
            ],
            source="tasks_json",
        )

        decision = decide_watchdog_tick(
            snapshot,
            NOW,
            relay_activity=RelayClientActivity(
                client_id="openclaw",
                last_request_at=NOW - timedelta(minutes=3),
                last_success_at=NOW - timedelta(minutes=8),
            ),
        )

        self.assertEqual(decision.action, "NO_REPLY")
        self.assertEqual(decision.reason, "doing")
        self.assertFalse(decision.changed)
        self.assertEqual(render_notify_text(decision), "")

    def test_decide_watchdog_tick_switches_first_todo_in_original_order(self) -> None:
        snapshot = TaskStateSnapshot(
            updated_at=NOW - timedelta(minutes=5),
            active_task_id="",
            tasks=[
                TaskRecord(
                    task_id="3b-9",
                    status="BLOCKED",
                    progress=30,
                    last_progress_at=NOW - timedelta(hours=5),
                    heartbeat_at=NOW - timedelta(hours=1),
                    blocker="等待拆分",
                    next_action="等待拆分",
                    done_criteria="输出对照实验",
                ),
                TaskRecord(
                    task_id="5a",
                    status="TODO",
                    progress=0,
                    last_progress_at=NOW - timedelta(hours=9),
                    heartbeat_at=NOW - timedelta(hours=1),
                    blocker="",
                    next_action="检查当前 OpenClaw 版本",
                    done_criteria="升级验证通过",
                ),
                TaskRecord(
                    task_id="5b",
                    status="TODO",
                    progress=0,
                    last_progress_at=NOW - timedelta(hours=9),
                    heartbeat_at=NOW - timedelta(hours=1),
                    blocker="",
                    next_action="检查微信插件版本",
                    done_criteria="插件升级验证通过",
                ),
            ],
            source="tasks_json",
        )

        decision = decide_watchdog_tick(snapshot, NOW)
        updated_task = find_active_task(decision.updated_snapshot)

        self.assertEqual(decision.action, "NOTIFY")
        self.assertEqual(decision.reason, "switch_task")
        self.assertEqual(decision.updated_snapshot.active_task_id, "5a")
        self.assertEqual(updated_task.status, "DOING")
        self.assertEqual(updated_task.progress, 0)
        self.assertIn("5a -> DOING", render_notify_text(decision))

    def test_sync_snapshot_if_needed_skips_write_for_heartbeat_only_change(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            json_path = tmp / "tasks.json"
            json_path.write_text(
                json.dumps(
                    {
                        "updated_at": "2026-03-27T08:40:00+08:00",
                        "active_task_id": "4a",
                        "tasks": [
                            {
                                "task_id": "4a",
                                "status": "DOING",
                                "progress": 90,
                                "last_progress_at": "2026-03-27T08:40:00+08:00",
                                "heartbeat_at": "2026-03-27T08:40:00+08:00",
                                "blocker": "",
                                "next_action": "补齐测试",
                                "done_criteria": "规则可执行",
                            }
                        ],
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            original_text = json_path.read_text(encoding="utf-8")

            snapshot = read_tasks_json(json_path)
            active_task = find_active_task(snapshot)
            active_task.heartbeat_at = NOW

            result = sync_snapshot_if_needed(snapshot, json_path)

            self.assertFalse(result.written)
            self.assertEqual(json_path.read_text(encoding="utf-8"), original_text)


if __name__ == "__main__":
    unittest.main()

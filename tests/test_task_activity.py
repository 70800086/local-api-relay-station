import unittest
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

from task_activity import (
    assess_task_activity,
    relay_client_activity_from_stats_payload,
    resolve_local_relay_config_path,
)
from task_watchdog import TaskRecord


TZ = ZoneInfo("Asia/Shanghai")
NOW = datetime(2026, 3, 28, 11, 30, tzinfo=TZ)


def make_task(
    *,
    last_progress_at: datetime | None,
    heartbeat_at: datetime | None,
    next_action: str = "继续推进",
) -> TaskRecord:
    return TaskRecord(
        task_id="6f",
        status="DOING",
        progress=20,
        last_progress_at=last_progress_at,
        heartbeat_at=heartbeat_at,
        blocker="",
        next_action=next_action,
        done_criteria="完成拆分并接上 relay",
    )


class TaskActivityTests(unittest.TestCase):
    def test_resolve_local_relay_config_path_prefers_relay_subdir_file(self) -> None:
        with TemporaryDirectory() as workspace_root:
            root = Path(workspace_root)
            legacy_path = root / "local_api_relay.json"
            relay_path = root / "relay" / "local_api_relay.json"
            relay_path.parent.mkdir(parents=True, exist_ok=True)
            legacy_path.write_text("{}", encoding="utf-8")
            relay_path.write_text("{}", encoding="utf-8")

            resolved = resolve_local_relay_config_path(root)

            self.assertEqual(resolved, relay_path)

    def test_resolve_local_relay_config_path_falls_back_to_legacy_root_file(self) -> None:
        with TemporaryDirectory() as workspace_root:
            root = Path(workspace_root)
            legacy_path = root / "local_api_relay.json"
            legacy_path.write_text("{}", encoding="utf-8")

            resolved = resolve_local_relay_config_path(root)

            self.assertEqual(resolved, legacy_path)

    def test_assess_task_activity_detects_task_board_only_activity(self) -> None:
        assessment = assess_task_activity(
            make_task(
                last_progress_at=NOW - timedelta(minutes=20),
                heartbeat_at=NOW - timedelta(minutes=5),
            ),
            NOW,
        )

        self.assertTrue(assessment.is_active)
        self.assertTrue(assessment.task_board_active)
        self.assertFalse(assessment.relay_active)
        self.assertFalse(assessment.is_stale)
        self.assertEqual(assessment.reason, "task_board_recent_progress")

    def test_assess_task_activity_detects_relay_only_activity_when_task_board_is_recent(self) -> None:
        relay_activity = relay_client_activity_from_stats_payload(
            {
                "clients": [
                    {
                        "client_id": "openclaw",
                        "last_request_at": (NOW - timedelta(minutes=3)).isoformat(),
                        "last_success_at": (NOW - timedelta(minutes=8)).isoformat(),
                    }
                ]
            },
            client_id="openclaw",
        )

        assessment = assess_task_activity(
            make_task(
                last_progress_at=NOW - timedelta(hours=2),
                heartbeat_at=NOW - timedelta(minutes=5),
            ),
            NOW,
            relay_activity=relay_activity,
        )

        self.assertTrue(assessment.is_active)
        self.assertFalse(assessment.task_board_active)
        self.assertTrue(assessment.relay_active)
        self.assertFalse(assessment.is_stale)
        self.assertEqual(assessment.reason, "relay_recent_request")

    def test_assess_task_activity_does_not_let_relay_activity_hide_stale_task_board(self) -> None:
        relay_activity = relay_client_activity_from_stats_payload(
            {
                "clients": [
                    {
                        "client_id": "openclaw",
                        "last_request_at": (NOW - timedelta(minutes=3)).isoformat(),
                        "last_success_at": (NOW - timedelta(minutes=8)).isoformat(),
                    }
                ]
            },
            client_id="openclaw",
        )

        assessment = assess_task_activity(
            make_task(
                last_progress_at=NOW - timedelta(hours=2),
                heartbeat_at=NOW - timedelta(minutes=20),
            ),
            NOW,
            relay_activity=relay_activity,
        )

        self.assertFalse(assessment.is_active)
        self.assertFalse(assessment.task_board_active)
        self.assertFalse(assessment.relay_active)
        self.assertTrue(assessment.is_stale)
        self.assertEqual(assessment.reason, "stale_progress")

    def test_assess_task_activity_marks_stale_when_task_board_and_relay_are_idle(self) -> None:
        relay_activity = relay_client_activity_from_stats_payload(
            {
                "clients": [
                    {
                        "client_id": "openclaw",
                        "last_request_at": (NOW - timedelta(hours=2)).isoformat(),
                        "last_success_at": (NOW - timedelta(hours=2)).isoformat(),
                    }
                ]
            },
            client_id="openclaw",
        )

        assessment = assess_task_activity(
            make_task(
                last_progress_at=NOW - timedelta(hours=2),
                heartbeat_at=NOW - timedelta(minutes=20),
            ),
            NOW,
            relay_activity=relay_activity,
        )

        self.assertFalse(assessment.is_active)
        self.assertFalse(assessment.task_board_active)
        self.assertFalse(assessment.relay_active)
        self.assertTrue(assessment.is_stale)
        self.assertEqual(assessment.reason, "stale_progress")


if __name__ == "__main__":
    unittest.main()

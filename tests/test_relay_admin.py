from __future__ import annotations

import os
import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "deploy" / "bin" / "relay-admin"


class RelayAdminScriptTests(unittest.TestCase):
    def run_script(self, command: str, *, fake_systemctl_dir: Path) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env["PATH"] = f"{fake_systemctl_dir}:{env.get('PATH', '')}"
        return subprocess.run(
            ["bash", str(SCRIPT_PATH), command],
            cwd=REPO_ROOT,
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )

    def write_fake_systemctl(self, sandbox: Path) -> Path:
        calls_path = sandbox / "systemctl.calls"
        script_path = sandbox / "systemctl"
        script_path.write_text(
            "\n".join(
                [
                    "#!/usr/bin/env bash",
                    "set -euo pipefail",
                    f"printf '%s\\n' \"$*\" >> {calls_path}",
                    "if [ \"$#\" -ge 4 ] && [ \"$1\" = \"--user\" ] && [ \"$4\" = \"status\" ]; then",
                    "  printf 'relay active\\n'",
                    "fi",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        script_path.chmod(0o755)
        return calls_path

    def test_status_calls_user_systemctl_for_local_api_relay_service(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            sandbox = Path(tmp_dir)
            calls_path = self.write_fake_systemctl(sandbox)

            result = self.run_script("status", fake_systemctl_dir=sandbox)

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                calls_path.read_text(encoding="utf-8").splitlines(),
                ["--user --no-pager --full status local-api-relay.service"],
            )

    def test_restart_calls_restart_then_status_for_local_api_relay_service(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            sandbox = Path(tmp_dir)
            calls_path = self.write_fake_systemctl(sandbox)

            result = self.run_script("restart", fake_systemctl_dir=sandbox)

            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                calls_path.read_text(encoding="utf-8").splitlines(),
                [
                    "--user restart local-api-relay.service",
                    "--user --no-pager --full status local-api-relay.service",
                ],
            )

    def test_unknown_command_is_rejected_without_touching_systemctl(self) -> None:
        with TemporaryDirectory() as tmp_dir:
            sandbox = Path(tmp_dir)
            calls_path = self.write_fake_systemctl(sandbox)

            result = self.run_script("reload", fake_systemctl_dir=sandbox)

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Usage:", result.stderr)
            self.assertFalse(calls_path.exists() and calls_path.read_text(encoding="utf-8").strip())


if __name__ == "__main__":
    unittest.main()

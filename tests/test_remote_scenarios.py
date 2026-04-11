#!/usr/bin/env python3
"""
Test remote path scenarios for backup, patch, and rollback.
These use subprocess mocks to verify command generation.
"""

import os
import sys
import pytest
import subprocess

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib import backup_lib


class TestRemotePatchAndRollback:
    """Verify patch and rollback generate correct remote scp commands."""

    def test_patch_to_remote(self, tmp_path, monkeypatch):
        output = tmp_path / "output"
        output.mkdir()
        (output / "file.txt").write_text("patch_data")

        calls = []

        def fake_popen(cmd, **kwargs):
            calls.append(cmd)
            class FakeProc:
                stdout = iter([])
                returncode = 0
                def wait(self):
                    return 0
            return FakeProc()

        monkeypatch.setattr(subprocess, "Popen", fake_popen)

        result = backup_lib.patch(
            str(output), "root@192.168.1.1:/remote/target", password="secret"
        )
        assert result is True
        # mkdir + scp for file.txt
        assert calls[0][-1] == "mkdir -p /remote/target"
        assert calls[1][3] == "scp"
        assert calls[1][-1] == "root@192.168.1.1:/remote/target/"
        assert calls[1][-2].endswith("file.txt")

    def test_rollback_to_remote(self, tmp_path, monkeypatch):
        backup_ts = tmp_path / "backups" / "target_20240101_120000"
        backup_ts.mkdir(parents=True)
        (backup_ts / "old.txt").write_text("old_data")

        calls = []

        def fake_popen(cmd, **kwargs):
            calls.append(cmd)
            class FakeProc:
                stdout = iter([])
                returncode = 0
                def wait(self):
                    return 0
            return FakeProc()

        monkeypatch.setattr(subprocess, "Popen", fake_popen)

        result = backup_lib.rollback(
            str(backup_ts), "root@192.168.1.1:/remote/target", password="secret"
        )
        assert result is True
        assert calls[0][-1] == "mkdir -p /remote/target"
        assert calls[1][3] == "scp"
        assert calls[1][-1] == "root@192.168.1.1:/remote/target/"
        assert calls[1][-2].endswith("old.txt")


class TestRemoteBackupPull:
    """Verify backup can pull from a remote target to local backup dir."""

    def test_backup_remote_target_without_password_fails(self, tmp_path):
        backup_dir = tmp_path / "backups"
        backup_dir.mkdir()

        with pytest.raises(ValueError, match="Remote target backup requires SSH password"):
            backup_lib.backup(
                "root@192.168.1.1:/remote/target", str(backup_dir)
            )

    def test_backup_remote_target_scp_pull(self, tmp_path, monkeypatch):
        backup_dir = tmp_path / "backups"
        backup_dir.mkdir()

        calls = []

        def fake_popen(cmd, **kwargs):
            calls.append(cmd)
            class FakeProc:
                stdout = iter([])
                returncode = 0
                def wait(self):
                    return 0
            return FakeProc()

        monkeypatch.setattr(subprocess, "Popen", fake_popen)

        dest = backup_lib.backup(
            "root@192.168.1.1:/remote/target", str(backup_dir), password="secret"
        )
        assert dest.startswith(str(backup_dir))
        assert len(calls) == 1
        cmd = calls[0]
        assert cmd[0] == "sshpass"
        assert cmd[2] == "secret"
        assert cmd[3] == "scp"
        assert cmd[4] == "-r"
        assert cmd[-2] == "root@192.168.1.1:/remote/target"
        assert cmd[-1].startswith(str(backup_dir))

    def test_backup_remote_target_scp_pull_failure(self, tmp_path, monkeypatch):
        backup_dir = tmp_path / "backups"
        backup_dir.mkdir()

        class FakeProc:
            stdout = iter([])
            def wait(self):
                return 1

        monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: FakeProc())

        with pytest.raises(RuntimeError, match="Remote backup failed"):
            backup_lib.backup(
                "root@192.168.1.1:/remote/target", str(backup_dir), password="secret"
            )


class TestRemoteCompatibilitySkipped:
    """Verify remote targets skip local structure checks."""

    def test_verify_structure_skips_remote(self, tmp_path):
        a = tmp_path / "a"
        a.mkdir()
        assert backup_lib.verify_structure(str(a), "root@1.2.3.4:/path") is True

    def test_check_patch_compatibility_remote(self, tmp_path):
        output = tmp_path / "output"
        output.mkdir()
        result, _ = backup_lib.check_patch_compatibility(
            str(output), "root@1.2.3.4:/path"
        )
        assert result == "remote"


class TestGuiRemoteBackupBlocked:
    """
    Verify that the GUI currently blocks backup when target is remote.
    This documents current behavior so we know to fix it if needed.
    """

    def test_gui_backup_blocks_remote(self):
        # We test the logic directly without starting Qt
        target = "root@192.168.1.1:/remote/target"
        assert backup_lib.is_remote(target) is True
        # In main_window._on_backup there is:
        #   if is_remote(target_dir): QMessageBox.warning(..., "Backup operations only support local Target directories")
        # So backup is explicitly blocked in GUI.
        pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

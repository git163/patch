#!/usr/bin/env python3
"""Tests for lib.backup_lib"""

import os
import sys
import pytest
import tempfile
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.backup_lib import (
    is_remote,
    parse_remote,
    list_backups,
    backup,
    patch,
    rollback,
    verify_structure,
)


class TestRemotePath:
    def test_is_remote_true(self):
        assert is_remote("root@192.168.1.1:/path") is True
        assert is_remote("user@host:/a/b") is True
        assert is_remote("deploy@10.0.0.1:/opt/app") is True

    def test_is_remote_false(self):
        assert is_remote("/local/path") is False
        assert is_remote("./relative/path") is False
        assert is_remote("") is False
        assert is_remote("not_remote") is False
        assert is_remote("user@host") is False  # missing colon path

    def test_parse_remote(self):
        assert parse_remote("root@192.168.1.1:/path") == ("root@192.168.1.1", "/path")
        assert parse_remote("user@host:/a/b/c") == ("user@host", "/a/b/c")

    def test_parse_remote_invalid(self):
        with pytest.raises(ValueError):
            parse_remote("/local/path")


class TestListBackups:
    def test_list_backups_empty(self, tmp_path):
        assert list_backups(str(tmp_path)) == []

    def test_list_backups_with_dirs(self, tmp_path):
        # Create valid backup dirs
        d1 = tmp_path / "target_20240101_120000"
        d2 = tmp_path / "target_20240102_120000"
        d3 = tmp_path / "not_a_backup"
        d1.mkdir()
        d2.mkdir()
        d3.mkdir()

        backups = list_backups(str(tmp_path))
        assert len(backups) == 2
        # Sorted reverse, newest first
        assert os.path.basename(backups[0]) == "target_20240102_120000"
        assert os.path.basename(backups[1]) == "target_20240101_120000"


class TestBackup:
    def test_backup_creates_timestamped_dir(self, tmp_path):
        target = tmp_path / "target"
        backup_dir = tmp_path / "backups"
        target.mkdir()
        (target / "file.txt").write_text("hello")

        logs = []
        dest = backup(str(target), str(backup_dir), logger=logs.append)

        assert os.path.isdir(dest)
        assert os.path.isfile(os.path.join(dest, "file.txt"))
        assert dest.startswith(str(backup_dir))


class TestPatch:
    def test_patch_local(self, tmp_path):
        output = tmp_path / "output"
        target = tmp_path / "target"
        output.mkdir()
        target.mkdir()
        (output / "new_file.txt").write_text("new content")
        (target / "old_file.txt").write_text("old content")

        logs = []
        patch(str(output), str(target), logger=logs.append)

        assert os.path.isfile(str(target / "new_file.txt"))
        assert os.path.isfile(str(target / "old_file.txt"))
        assert (target / "new_file.txt").read_text() == "new content"

    def test_patch_overwrites_existing(self, tmp_path):
        output = tmp_path / "output"
        target = tmp_path / "target"
        output.mkdir()
        target.mkdir()
        (output / "file.txt").write_text("new")
        (target / "file.txt").write_text("old")

        patch(str(output), str(target))
        assert (target / "file.txt").read_text() == "new"


class TestRollback:
    def test_rollback_local(self, tmp_path):
        backup_ts = tmp_path / "backups" / "target_20240101_120000"
        target = tmp_path / "target"
        backup_ts.mkdir(parents=True)
        target.mkdir()
        (backup_ts / "backup_file.txt").write_text("backup data")
        (target / "current_file.txt").write_text("current data")

        logs = []
        rollback(str(backup_ts), str(target), logger=logs.append)

        assert os.path.isfile(str(target / "backup_file.txt"))
        assert os.path.isfile(str(target / "current_file.txt"))
        assert (target / "backup_file.txt").read_text() == "backup data"


class TestVerifyStructure:
    def test_identical_structure(self, tmp_path):
        a = tmp_path / "a"
        b = tmp_path / "b"
        a.mkdir()
        b.mkdir()
        (a / "file.txt").write_text("a")
        (b / "file.txt").write_text("b")

        assert verify_structure(str(a), str(b)) is True

    def test_missing_target_dir(self, tmp_path):
        a = tmp_path / "a"
        a.mkdir()
        b = tmp_path / "b"

        assert verify_structure(str(a), str(b)) is True

    def test_source_missing(self, tmp_path):
        a = tmp_path / "a"  # doesn't exist
        b = tmp_path / "b"
        b.mkdir()

        assert verify_structure(str(a), str(b)) is False

    def test_remote_target_skips_check(self, tmp_path):
        a = tmp_path / "a"
        a.mkdir()
        assert verify_structure(str(a), "root@1.2.3.4:/path") is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

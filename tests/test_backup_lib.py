#!/usr/bin/env python3
"""Tests for lib.backup_lib"""

import os
import sys
import time
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
    check_patch_compatibility,
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
        assert is_remote("@host:/path") is False  # missing user

    def test_parse_remote(self):
        assert parse_remote("root@192.168.1.1:/path") == ("root@192.168.1.1", "/path")
        assert parse_remote("user@host:/a/b/c") == ("user@host", "/a/b/c")

    def test_parse_remote_invalid(self):
        with pytest.raises(ValueError):
            parse_remote("/local/path")
        with pytest.raises(ValueError):
            parse_remote("root@192.168.1.1")


class TestTildeExpansion:
    def test_backup_with_tilde(self, tmp_path, monkeypatch):
        home = str(tmp_path)
        monkeypatch.setenv("HOME", home)
        target = tmp_path / "target"
        backup_dir = tmp_path / "backups"
        target.mkdir()
        backup_dir.mkdir()
        (target / "file.txt").write_text("hello")

        from lib.backup_lib import _expand_path
        assert _expand_path("~/backups") == os.path.join(home, "backups")

        dest = backup("~/target", "~/backups")
        assert os.path.isdir(dest)
        assert os.path.isfile(os.path.join(dest, "file.txt"))

    def test_list_backups_with_tilde(self, tmp_path, monkeypatch):
        home = str(tmp_path)
        monkeypatch.setenv("HOME", home)
        backup_dir = tmp_path / "backups"
        backup_dir.mkdir()
        d1 = backup_dir / "app_20240101_120000"
        d1.mkdir()

        backups = list_backups("~/backups")
        assert len(backups) == 1

    def test_patch_with_tilde(self, tmp_path, monkeypatch):
        home = str(tmp_path)
        monkeypatch.setenv("HOME", home)
        output = tmp_path / "output"
        target = tmp_path / "target"
        output.mkdir()
        target.mkdir()
        (output / "f.txt").write_text("data")

        patch("~/output", "~/target")
        assert os.path.isfile(str(tmp_path / "target" / "f.txt"))


class TestListBackups:
    def test_list_backups_empty(self, tmp_path):
        assert list_backups(str(tmp_path)) == []

    def test_list_backups_with_dirs(self, tmp_path):
        d1 = tmp_path / "target_20240101_120000"
        d2 = tmp_path / "target_20240102_120000"
        d3 = tmp_path / "not_a_backup"
        d1.mkdir()
        d2.mkdir()
        d3.mkdir()

        backups = list_backups(str(tmp_path))
        assert len(backups) == 2
        assert os.path.basename(backups[0]) == "target_20240102_120000"
        assert os.path.basename(backups[1]) == "target_20240101_120000"

    def test_list_backups_ignores_files(self, tmp_path):
        (tmp_path / "target_20240101_120000").write_text("not a dir")
        assert list_backups(str(tmp_path)) == []


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

    def test_backup_multiple_times(self, tmp_path):
        target = tmp_path / "target"
        backup_dir = tmp_path / "backups"
        target.mkdir()
        (target / "f.txt").write_text("v1")

        dest1 = backup(str(target), str(backup_dir))
        (target / "f.txt").write_text("v2")
        time.sleep(1)
        dest2 = backup(str(target), str(backup_dir))

        assert dest1 != dest2
        assert os.path.basename(dest1) != os.path.basename(dest2)
        backups = list_backups(str(backup_dir))
        assert len(backups) == 2

    def test_backup_missing_target(self, tmp_path):
        with pytest.raises(ValueError):
            backup(str(tmp_path / "missing"), str(tmp_path / "backups"))


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

    def test_patch_merges_subdirectories(self, tmp_path):
        output = tmp_path / "output"
        target = tmp_path / "target"
        output.mkdir()
        target.mkdir()

        (output / "a").mkdir()
        (output / "a" / "f1.txt").write_text("1")

        (target / "a").mkdir()
        (target / "a" / "f2.txt").write_text("2")
        (target / "b").mkdir()
        (target / "b" / "f3.txt").write_text("3")

        patch(str(output), str(target))
        assert (target / "a" / "f1.txt").read_text() == "1"
        assert (target / "a" / "f2.txt").read_text() == "2"
        assert (target / "b" / "f3.txt").read_text() == "3"

    def test_patch_missing_output(self, tmp_path):
        with pytest.raises(ValueError):
            patch(str(tmp_path / "missing"), str(tmp_path / "target"))


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

    def test_rollback_missing_backup(self, tmp_path):
        with pytest.raises(ValueError):
            rollback(str(tmp_path / "missing"), str(tmp_path / "target"))


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
        a = tmp_path / "a"
        b = tmp_path / "b"
        b.mkdir()

        assert verify_structure(str(a), str(b)) is False

    def test_remote_target_skips_check(self, tmp_path):
        a = tmp_path / "a"
        a.mkdir()
        assert verify_structure(str(a), "root@1.2.3.4:/path") is True

    def test_hidden_files_ignored(self, tmp_path):
        a = tmp_path / "a"
        b = tmp_path / "b"
        a.mkdir()
        b.mkdir()
        (a / ".hidden").write_text("h")
        (b / "file.txt").write_text("b")

        # Hidden files are ignored, so no overlap -> False
        assert verify_structure(str(a), str(b)) is False


class TestCheckPatchCompatibility:
    def test_match(self, tmp_path):
        output = tmp_path / "output"
        target = tmp_path / "target"
        output.mkdir()
        target.mkdir()
        (output / "f.txt").write_text("1")
        (target / "f.txt").write_text("2")

        result, details = check_patch_compatibility(str(output), str(target))
        assert result == "match"
        assert details["only_output"] == []
        assert details["mismatch"] == []

    def test_partial(self, tmp_path):
        output = tmp_path / "output"
        target = tmp_path / "target"
        output.mkdir()
        target.mkdir()
        (output / "common.txt").write_text("1")
        (output / "only_out.txt").write_text("2")
        (target / "common.txt").write_text("3")
        (target / "only_tgt.txt").write_text("4")

        result, details = check_patch_compatibility(str(output), str(target))
        assert result == "partial"
        assert "only_out.txt" in details["only_output"]
        assert "only_tgt.txt" in details["only_target"]

    def test_none(self, tmp_path):
        output = tmp_path / "output"
        target = tmp_path / "target"
        output.mkdir()
        target.mkdir()
        (output / "a.txt").write_text("1")
        (target / "b.txt").write_text("2")

        result, details = check_patch_compatibility(str(output), str(target))
        assert result == "none"
        assert details["only_output"] == ["a.txt"]
        assert details["only_target"] == ["b.txt"]

    def test_empty_target(self, tmp_path):
        output = tmp_path / "output"
        target = tmp_path / "target"
        output.mkdir()
        (output / "a.txt").write_text("1")

        result, _ = check_patch_compatibility(str(output), str(target))
        assert result == "empty_target"

    def test_remote(self, tmp_path):
        output = tmp_path / "output"
        output.mkdir()
        result, _ = check_patch_compatibility(str(output), "root@1.2.3.4:/path")
        assert result == "remote"

    def test_mismatch_type(self, tmp_path):
        output = tmp_path / "output"
        target = tmp_path / "target"
        output.mkdir()
        target.mkdir()
        (output / "item").write_text("file")
        (target / "item").mkdir()

        result, details = check_patch_compatibility(str(output), str(target))
        assert result == "partial"
        assert len(details["mismatch_info"]) == 1
        assert details["mismatch_info"][0]["name"] == "item"
        assert details["mismatch_info"][0]["output_type"] == "文件"
        assert details["mismatch_info"][0]["target_type"] == "目录"


class TestFullWorkflow:
    def test_backup_patch_rollback_chain(self, tmp_path):
        """Simulate full workflow: backup target, patch output, rollback."""
        target = tmp_path / "target"
        output = tmp_path / "output"
        backup_dir = tmp_path / "backups"

        target.mkdir()
        output.mkdir()

        # Initial target state
        (target / "keep.txt").write_text("keep")
        (target / "replace.txt").write_text("old")

        # Step 1: backup target
        dest = backup(str(target), str(backup_dir))
        assert os.path.isfile(os.path.join(dest, "keep.txt"))

        # Step 2: patch output (output has replace.txt and new_file.txt)
        (output / "replace.txt").write_text("new")
        (output / "new_file.txt").write_text("new")
        patch(str(output), str(target))

        assert (target / "replace.txt").read_text() == "new"
        assert (target / "new_file.txt").read_text() == "new"
        assert (target / "keep.txt").read_text() == "keep"

        # Step 3: rollback to backup
        rollback(dest, str(target))
        assert (target / "keep.txt").read_text() == "keep"
        assert (target / "replace.txt").read_text() == "old"
        # rollback merges, so new_file.txt should still exist
        assert (target / "new_file.txt").read_text() == "new"

    def test_hidden_files_not_counted_in_compatibility(self, tmp_path):
        output = tmp_path / "output"
        target = tmp_path / "target"
        output.mkdir()
        target.mkdir()
        (output / ".DS_Store").write_text("hidden")
        (output / "real.txt").write_text("1")
        (target / "real.txt").write_text("2")

        result, details = check_patch_compatibility(str(output), str(target))
        assert result == "match"
        assert ".DS_Store" not in details.get("only_output", [])


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

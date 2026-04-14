"""
Tests for lib.backup_lib patch, rollback, backup, and compatibility functions.
Covers both local and remote targets with target directory validation.
"""

import os
import re
import shutil
import tempfile
from unittest.mock import patch as mock_patch, MagicMock, call
import pytest

from lib.backup_lib import (
    patch,
    rollback,
    backup,
    find_overlapping_paths,
    backup_overlapping_files,
    check_patch_compatibility,
    is_remote,
    parse_remote,
    list_backups,
)


# =============================================================================
# Helpers
# =============================================================================

def _write_file(path, content="test"):
    os.makedirs(os.path.dirname(path), exist_ok=True) if os.path.dirname(path) else None
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def _read_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _dir_files(root):
    """Return sorted list of relative file paths under root."""
    files = []
    for dirpath, _dirnames, filenames in os.walk(root):
        rel_dir = os.path.relpath(dirpath, root)
        if rel_dir == ".":
            rel_dir = ""
        for name in filenames:
            files.append(os.path.join(rel_dir, name) if rel_dir else name)
    return sorted(files)


def _collect_commands(mock_run_cmd):
    """Return list of command lists passed to mock_run_cmd."""
    return [c[0][0] for c in mock_run_cmd.call_args_list]


class _FakeSFTPItem:
    def __init__(self, filename: str, is_dir: bool = False):
        self.filename = filename
        self.st_mode = __import__("stat").S_IFDIR if is_dir else __import__("stat").S_IFREG


def _mock_paramiko(monkeypatch, exec_results=None):
    """Mock paramiko.SSHClient for remote operation tests.

    exec_results: list of (exit_status, stdout_lines, stderr_text).
    Returns (fake_client, fake_sftp).
    """
    exec_results = list(exec_results) if exec_results is not None else [(0, [], "")]
    result_iter = iter(exec_results)

    class FakeChannel:
        def __init__(self, status):
            self._status = status
        def recv_exit_status(self):
            return self._status

    class FakeStdout:
        def __init__(self, lines, status):
            self.channel = FakeChannel(status)
            self._data = "\n".join(lines).encode("utf-8") if lines else b""
        def read(self):
            return self._data

    class FakeStderr:
        def __init__(self, text):
            self._data = text.encode("utf-8")
        def read(self):
            return self._data

    class FakeSFTP:
        def __init__(self):
            self.put_calls = []
            self.get_calls = []
            self.mkdir_calls = []
            self.listdir_attr_calls = []
            self._remote_tree = {}
        def put(self, local, remote):
            self.put_calls.append((str(local), str(remote)))
        def get(self, remote, local):
            self.get_calls.append((str(remote), str(local)))
            # Touch local file so backup appears non-empty
            os.makedirs(os.path.dirname(local), exist_ok=True)
            with open(local, "w", encoding="utf-8") as f:
                f.write("mock")
        def mkdir(self, path):
            self.mkdir_calls.append(path)
        def listdir_attr(self, path):
            self.listdir_attr_calls.append(path)
            return self._remote_tree.get(path, [])
        def close(self):
            pass

    fake_sftp = FakeSFTP()

    class FakeClient:
        def __init__(self):
            self.exec_calls = []
            self.closed = False
        def set_missing_host_key_policy(self, policy):
            pass
        def connect(self, **kwargs):
            pass
        def exec_command(self, cmd):
            self.exec_calls.append(cmd)
            try:
                status, lines, err = next(result_iter)
            except StopIteration:
                status, lines, err = 0, [], ""
            return None, FakeStdout(lines, status), FakeStderr(err)
        def open_sftp(self):
            return fake_sftp
        def close(self):
            self.closed = True

    fake_client = FakeClient()
    import paramiko
    monkeypatch.setattr(paramiko, "SSHClient", lambda: fake_client)
    return fake_client, fake_sftp


# =============================================================================
# Local Target Tests
# =============================================================================

class TestLocalPatch:
    """Validate patch behavior for local target directories."""

    def test_patch_copies_files_to_existing_target(self, tmp_path):
        output_dir = tmp_path / "output"
        target_dir = tmp_path / "target"
        output_dir.mkdir()
        target_dir.mkdir()

        _write_file(output_dir / "a.txt", "output_a")
        _write_file(output_dir / "sub" / "b.txt", "output_b")

        patch(str(output_dir), str(target_dir))

        assert (target_dir / "a.txt").exists()
        assert _read_file(target_dir / "a.txt") == "output_a"
        assert (target_dir / "sub" / "b.txt").exists()
        assert _read_file(target_dir / "sub" / "b.txt") == "output_b"

    def test_patch_creates_target_if_missing(self, tmp_path):
        output_dir = tmp_path / "output"
        target_dir = tmp_path / "target" / "nested"
        output_dir.mkdir()

        _write_file(output_dir / "file.txt", "content")

        patch(str(output_dir), str(target_dir))

        assert target_dir.exists()
        assert (target_dir / "file.txt").exists()

    def test_patch_overwrites_existing_files(self, tmp_path):
        output_dir = tmp_path / "output"
        target_dir = tmp_path / "target"
        output_dir.mkdir()
        target_dir.mkdir()

        _write_file(output_dir / "file.txt", "new_content")
        _write_file(target_dir / "file.txt", "old_content")

        patch(str(output_dir), str(target_dir))

        assert _read_file(target_dir / "file.txt") == "new_content"

    def test_patch_preserves_non_overlapping_target_files(self, tmp_path):
        output_dir = tmp_path / "output"
        target_dir = tmp_path / "target"
        output_dir.mkdir()
        target_dir.mkdir()

        _write_file(output_dir / "new.txt", "new")
        _write_file(target_dir / "old.txt", "old")

        patch(str(output_dir), str(target_dir))

        assert (target_dir / "new.txt").exists()
        assert (target_dir / "old.txt").exists()
        assert _read_file(target_dir / "old.txt") == "old"


class TestLocalRollback:
    """Validate rollback behavior for local target directories."""

    def test_rollback_restores_backup_to_target(self, tmp_path):
        backup_dir = tmp_path / "backup"
        target_dir = tmp_path / "target"
        backup_dir.mkdir()
        target_dir.mkdir()

        _write_file(backup_dir / "a.txt", "backup_a")
        _write_file(backup_dir / "sub" / "b.txt", "backup_b")
        _write_file(target_dir / "a.txt", "patched_a")

        rollback(str(backup_dir), str(target_dir))

        assert _read_file(target_dir / "a.txt") == "backup_a"
        assert (target_dir / "sub" / "b.txt").exists()
        assert _read_file(target_dir / "sub" / "b.txt") == "backup_b"

    def test_rollback_creates_target_if_missing(self, tmp_path):
        backup_dir = tmp_path / "backup"
        target_dir = tmp_path / "target" / "nested"
        backup_dir.mkdir()

        _write_file(backup_dir / "file.txt", "backup")

        rollback(str(backup_dir), str(target_dir))

        assert target_dir.exists()
        assert (target_dir / "file.txt").exists()

    def test_rollback_overwrites_target_and_preserves_extra_files(self, tmp_path):
        backup_dir = tmp_path / "backup"
        target_dir = tmp_path / "target"
        backup_dir.mkdir()
        target_dir.mkdir()

        _write_file(backup_dir / "shared.txt", "backup_shared")
        _write_file(target_dir / "shared.txt", "target_shared")
        _write_file(target_dir / "extra.txt", "target_extra")

        rollback(str(backup_dir), str(target_dir))

        assert _read_file(target_dir / "shared.txt") == "backup_shared"
        assert (target_dir / "extra.txt").exists()
        assert _read_file(target_dir / "extra.txt") == "target_extra"


class TestLocalPatchRollbackIntegration:
    """End-to-end: backup original, patch, rollback, validate target directory."""

    def test_full_backup_patch_rollback_cycle(self, tmp_path):
        output_dir = tmp_path / "output"
        backup_base = tmp_path / "backups"
        target_dir = tmp_path / "target"

        output_dir.mkdir()
        backup_base.mkdir()
        target_dir.mkdir()

        # Original target state
        _write_file(target_dir / "shared.txt", "original_shared")
        _write_file(target_dir / "original_only.txt", "original_only")

        # Output state to patch
        _write_file(output_dir / "shared.txt", "patched_shared")
        _write_file(output_dir / "new_only.txt", "new_only")

        # Step 1: backup original target
        backup_path = backup(str(target_dir), str(backup_base))
        assert backup_path is not None
        assert os.path.isdir(backup_path)

        # Step 2: patch
        patch(str(output_dir), str(target_dir))

        # Validate patched target
        assert _read_file(target_dir / "shared.txt") == "patched_shared"
        assert (target_dir / "new_only.txt").exists()
        assert _read_file(target_dir / "new_only.txt") == "new_only"
        assert (target_dir / "original_only.txt").exists()
        assert _read_file(target_dir / "original_only.txt") == "original_only"

        # Step 3: rollback to original backup
        rollback(backup_path, str(target_dir))

        # Validate rolled-back target (backup copied over, extra files remain)
        assert _read_file(target_dir / "shared.txt") == "original_shared"
        assert (target_dir / "original_only.txt").exists()
        assert _read_file(target_dir / "original_only.txt") == "original_only"
        assert (target_dir / "new_only.txt").exists()
        assert _read_file(target_dir / "new_only.txt") == "new_only"


class TestLocalCompatibilityAndOverlap:
    """Compatibility and overlap detection for local targets."""

    def test_check_patch_compatibility_match(self, tmp_path):
        output_dir = tmp_path / "output"
        target_dir = tmp_path / "target"
        output_dir.mkdir()
        target_dir.mkdir()

        _write_file(output_dir / "a.txt", "a")
        _write_file(target_dir / "a.txt", "a")

        result, details = check_patch_compatibility(str(output_dir), str(target_dir))
        assert result == "match"

    def test_check_patch_compatibility_partial(self, tmp_path):
        output_dir = tmp_path / "output"
        target_dir = tmp_path / "target"
        output_dir.mkdir()
        target_dir.mkdir()

        _write_file(output_dir / "a.txt", "a")
        _write_file(output_dir / "only_output.txt", "o")
        _write_file(target_dir / "a.txt", "a")
        _write_file(target_dir / "only_target.txt", "t")

        result, details = check_patch_compatibility(str(output_dir), str(target_dir))
        assert result == "partial"
        assert "only_output" in details
        assert "only_target" in details
        assert "only_output.txt" in details["only_output"]
        assert "only_target.txt" in details["only_target"]

    def test_check_patch_compatibility_empty_target(self, tmp_path):
        output_dir = tmp_path / "output"
        target_dir = tmp_path / "target"
        output_dir.mkdir()
        target_dir.mkdir()

        _write_file(output_dir / "a.txt", "a")

        result, _ = check_patch_compatibility(str(output_dir), str(target_dir))
        assert result == "empty_target"

    def test_find_overlapping_paths(self, tmp_path):
        output_dir = tmp_path / "output"
        target_dir = tmp_path / "target"
        output_dir.mkdir()
        target_dir.mkdir()

        _write_file(output_dir / "a.txt", "a")
        _write_file(output_dir / "sub" / "b.txt", "b")
        _write_file(target_dir / "a.txt", "a")
        _write_file(target_dir / "sub" / "b.txt", "b")
        _write_file(target_dir / "c.txt", "c")

        overlaps = find_overlapping_paths(str(output_dir), str(target_dir))
        assert sorted(overlaps) == ["a.txt", "sub", "sub/b.txt"]

    def test_backup_overlapping_files_copies_only_overlaps(self, tmp_path):
        output_dir = tmp_path / "output"
        target_dir = tmp_path / "target"
        backup_dir = tmp_path / "backup"
        output_dir.mkdir()
        target_dir.mkdir()

        _write_file(output_dir / "shared.txt", "o")
        _write_file(output_dir / "only_output.txt", "o")
        _write_file(target_dir / "shared.txt", "t")
        _write_file(target_dir / "only_target.txt", "t")

        result = backup_overlapping_files(str(output_dir), str(target_dir), str(backup_dir))
        assert result is not None
        assert os.path.isfile(os.path.join(result, "shared.txt"))
        assert _read_file(os.path.join(result, "shared.txt")) == "t"
        assert not os.path.exists(os.path.join(result, "only_target.txt"))
        assert not os.path.exists(os.path.join(result, "only_output.txt"))

    def test_backup_overlapping_files_keeps_bottom_most_items(self, tmp_path):
        """When a directory and its child both overlap, backup only the child."""
        output_dir = tmp_path / "output"
        target_dir = tmp_path / "target"
        backup_dir = tmp_path / "backup"
        output_dir.mkdir()
        target_dir.mkdir()

        # output and target both have sub/a.txt
        # target also has sub/b.txt which is NOT in output
        _write_file(output_dir / "sub" / "a.txt", "output_a")
        _write_file(target_dir / "sub" / "a.txt", "target_a")
        _write_file(target_dir / "sub" / "b.txt", "target_b")

        result = backup_overlapping_files(str(output_dir), str(target_dir), str(backup_dir))
        assert result is not None

        # Should back up the bottom-most overlapping item (sub/a.txt),
        # NOT the entire sub/ directory (which would incorrectly include sub/b.txt).
        assert os.path.isfile(os.path.join(result, "sub", "a.txt"))
        assert _read_file(os.path.join(result, "sub", "a.txt")) == "target_a"
        assert not os.path.exists(os.path.join(result, "sub", "b.txt"))


# =============================================================================
# Remote Target Tests (commands validated via mocking)
# =============================================================================

class TestRemotePatch:
    """Validate patch behavior for remote target directories."""

    def test_patch_remote_executes_ssh_and_sftp(self, monkeypatch, tmp_path):
        fake_client, fake_sftp = _mock_paramiko(monkeypatch)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _write_file(output_dir / "a.txt", "a")
        _write_file(output_dir / "sub" / "b.txt", "b")

        remote_target = "user@192.168.1.100:/remote/target"
        patch(str(output_dir), remote_target, password="secret")

        exec_cmds = fake_client.exec_calls
        assert any("mkdir -p" in c for c in exec_cmds)
        assert sum("rm -rf" in c for c in exec_cmds) == 2
        assert len(fake_sftp.put_calls) == 2  # a.txt and sub dir tree

    def test_patch_remote_target_validated_by_mkdir(self, monkeypatch, tmp_path):
        fake_client, _ = _mock_paramiko(monkeypatch)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _write_file(output_dir / "file.txt", "f")

        remote_target = "user@192.168.1.100:/remote/target"
        patch(str(output_dir), remote_target, password="secret")

        assert any("mkdir -p" in c for c in fake_client.exec_calls)
        assert any("/remote/target" in c for c in fake_client.exec_calls)


class TestRemoteRollback:
    """Validate rollback behavior for remote target directories."""

    def test_rollback_remote_executes_ssh_and_sftp(self, monkeypatch, tmp_path):
        fake_client, fake_sftp = _mock_paramiko(monkeypatch)
        backup_dir = tmp_path / "backup"
        backup_dir.mkdir()
        _write_file(backup_dir / "a.txt", "a")
        _write_file(backup_dir / "sub" / "b.txt", "b")

        remote_target = "user@192.168.1.100:/remote/target"
        rollback(str(backup_dir), remote_target, password="secret")

        assert sum("rm -rf" in c for c in fake_client.exec_calls) == 2
        assert len(fake_sftp.put_calls) == 2  # a.txt and sub dir tree


class TestRemoteBackup:
    """Validate backup behavior for remote target directories."""

    def test_backup_remote_executes_sftp_from_remote(self, monkeypatch, tmp_path):
        fake_client, fake_sftp = _mock_paramiko(monkeypatch)
        fake_sftp._remote_tree = {"/remote/target": [_FakeSFTPItem("file.txt")]}
        backup_base = tmp_path / "backups"
        backup_base.mkdir()

        remote_target = "user@192.168.1.100:/remote/target"
        result = backup(remote_target, str(backup_base), password="secret")

        assert "/remote/target" in fake_sftp.listdir_attr_calls
        assert len(fake_sftp.get_calls) == 1
        assert result is not None
        assert os.path.basename(os.path.dirname(result)) == "backups"


class TestRemoteCompatibilityAndOverlap:
    """Compatibility and overlap detection for remote targets."""

    @mock_patch("lib.backup_lib._run_ssh_cmd")
    def test_check_patch_compatibility_remote_match(self, mock_ssh):
        mock_ssh.side_effect = [
            (0, ["a.txt", "sub"]),  # ls -A1
            (0, ["file"]),          # a.txt type check
            (0, ["dir"]),           # sub type check
        ]
        output_dir = tempfile.mkdtemp()
        os.makedirs(os.path.join(output_dir, "sub"))
        _write_file(os.path.join(output_dir, "a.txt"), "a")

        remote_target = "user@192.168.1.100:/remote/target"
        result, details = check_patch_compatibility(output_dir, remote_target, password="secret")

        assert result == "match"
        assert mock_ssh.called

        shutil.rmtree(output_dir)

    @mock_patch("lib.backup_lib._run_ssh_cmd")
    def test_check_patch_compatibility_remote_partial(self, mock_ssh):
        # First call: ls -A1 returns items
        # Second+ calls: test -d for type checks
        mock_ssh.side_effect = [
            (0, ["a.txt", "b.txt"]),
            (0, ["file"]),
            (0, ["file"]),
        ]
        output_dir = tempfile.mkdtemp()
        _write_file(os.path.join(output_dir, "a.txt"), "a")
        _write_file(os.path.join(output_dir, "c.txt"), "c")

        remote_target = "user@192.168.1.100:/remote/target"
        result, details = check_patch_compatibility(output_dir, remote_target, password="secret")

        assert result == "partial"
        assert "only_output" in details
        assert "only_target" in details

        shutil.rmtree(output_dir)

    @mock_patch("lib.backup_lib._run_ssh_cmd")
    def test_find_overlapping_paths_remote(self, mock_ssh):
        mock_ssh.return_value = (0, ["a.txt", "sub/b.txt"])
        output_dir = tempfile.mkdtemp()
        _write_file(os.path.join(output_dir, "a.txt"), "a")
        _write_file(os.path.join(output_dir, "sub", "b.txt"), "b")
        _write_file(os.path.join(output_dir, "c.txt"), "c")

        remote_target = "user@192.168.1.100:/remote/target"
        overlaps = find_overlapping_paths(output_dir, remote_target, password="secret")

        assert "a.txt" in overlaps
        assert "sub/b.txt" in overlaps
        assert "c.txt" not in overlaps

        shutil.rmtree(output_dir)

    def test_backup_overlapping_files_remote_triggers_full_backup(self, monkeypatch, tmp_path):
        fake_client, fake_sftp = _mock_paramiko(
            monkeypatch, exec_results=[(0, [""], "")]  # test -d returns 0
        )
        fake_sftp._remote_tree = {"/remote/target": [_FakeSFTPItem("a.txt")]}

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        backup_dir = tmp_path / "backup"
        backup_dir.mkdir()
        _write_file(output_dir / "a.txt", "a")

        remote_target = "user@192.168.1.100:/remote/target"
        result = backup_overlapping_files(str(output_dir), remote_target, str(backup_dir), password="secret")

        # Remote target should trigger a full backup via SFTP
        assert result is not None
        assert any("test -d" in c for c in fake_client.exec_calls)
        assert len(fake_sftp.get_calls) == 1


# =============================================================================
# Utilities
# =============================================================================

class TestUtilities:
    def test_is_remote(self):
        assert is_remote("root@192.168.1.1:/path") is True
        assert is_remote("/local/path") is False
        assert is_remote("") is False

    def test_parse_remote(self):
        user_host, remote_path = parse_remote("root@192.168.1.1:/var/log")
        assert user_host == "root@192.168.1.1"
        assert remote_path == "/var/log"

    def test_list_backups(self, tmp_path):
        backup_dir = tmp_path / "backups"
        backup_dir.mkdir()
        (backup_dir / "target_20240101_120000").mkdir()
        (backup_dir / "target_20240102_120000").mkdir()
        (backup_dir / "not_a_backup").mkdir()

        backups = list_backups(str(backup_dir))
        assert len(backups) == 2
        assert all(re.search(r"_\d{8}_\d{6}$", os.path.basename(b)) for b in backups)

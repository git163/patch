"""
Core library for backup, patch, and rollback operations.
"""

import os
import shutil
import re
import subprocess
import shlex
from datetime import datetime


def _expand_path(path: str) -> str:
    """Expand ~ to home directory for local paths."""
    if is_remote(path):
        return path
    return os.path.expanduser(path)


def is_remote(path: str) -> bool:
    """Check if path is a remote path like root@192.168.1.100:/path"""
    if not path:
        return False
    return bool(re.match(r"^[^@\s]+@[^:]+:.+$", path))


def parse_remote(path: str):
    """Parse remote path into (user_host, remote_path)."""
    # root@192.168.1.100:/remote/path
    match = re.match(r"^([^@\s]+@[^:]+):(.+)$", path)
    if not match:
        raise ValueError(f"Invalid remote path: {path}")
    return match.group(1), match.group(2)


def list_backups(backup_dir: str) -> list:
    """List timestamped backup directories under backup_dir."""
    backup_dir = _expand_path(backup_dir)
    if not os.path.isdir(backup_dir):
        return []
    dirs = []
    for name in os.listdir(backup_dir):
        full = os.path.join(backup_dir, name)
        if os.path.isdir(full) and re.search(r"_\d{8}_\d{6}$", name):
            dirs.append(full)
    dirs.sort(reverse=True)
    return dirs


def _list_visible(root: str) -> list:
    """List non-hidden items under root recursively."""
    items = []
    for dirpath, dirnames, filenames in os.walk(root):
        # Filter hidden directories to prevent walking into them
        dirnames[:] = [d for d in dirnames if not d.startswith('.')]
        rel_dir = os.path.relpath(dirpath, root)
        for f in filenames:
            if f.startswith('.'):
                continue
            if rel_dir == '.':
                items.append(f)
            else:
                items.append(os.path.join(rel_dir, f))
    return sorted(items)


def check_patch_compatibility(output_dir: str, target_dir: str, password: str = "", logger=None):
    """
    Check compatibility between output_dir and target_dir for patching.

    Returns (result, details_dict):
      result:
      - "match"       : all top-level items overlap (target has all output items)
      - "partial"     : some items overlap, some differ
      - "none"        : no overlapping top-level items at all
      - "empty_target": target does not exist or is empty
      - "remote"      : target is remote (basic check performed remotely)

      details_dict may contain keys:
      - "only_output": items only in output (max 10)
      - "only_target": items only in target (max 10)
      - "mismatch"   : items present in both but type differs (file vs dir) (max 10)
    """
    output_dir = _expand_path(output_dir)
    if is_remote(target_dir):
        if logger:
            logger(f"Remote target, performing basic compatibility check: {target_dir}")
        try:
            target_items = {
                name for name in _list_remote_toplevel_items(target_dir, password, logger)
                if not name.startswith('.')
            }
        except Exception as e:
            if logger:
                logger(f"Failed to list remote target items: {e}")
            return "remote", {}

        output_items = {name for name in os.listdir(output_dir) if not name.startswith('.')}

        if not target_items:
            return "empty_target", {}

        if not output_items:
            return "match", {}

        overlap = output_items & target_items
        only_output = sorted(output_items - target_items)
        only_target = sorted(target_items - output_items)

        # Check type mismatches for overlapping names
        mismatch = []
        mismatch_info = []
        user_host_r, remote_dir_r = parse_remote(target_dir)
        for name in sorted(overlap):
            out_path = os.path.join(output_dir, name)
            out_is_dir = os.path.isdir(out_path)
            test_cmd = f"test -d {shlex.quote(os.path.join(remote_dir_r, name))} && echo dir || echo file"
            ret_r, out_r = _run_ssh_cmd(user_host_r, password, test_cmd, logger)
            tgt_is_dir = False
            if ret_r == 0 and out_r:
                tgt_is_dir = out_r[0].strip() == "dir"
            if out_is_dir != tgt_is_dir:
                mismatch.append(name)
                mismatch_info.append({
                    "name": name,
                    "output_type": "directory" if out_is_dir else "file",
                    "target_type": "directory" if tgt_is_dir else "file",
                })

        if logger:
            logger(f"Output items: {sorted(output_items)}")
            logger(f"Remote target items: {sorted(target_items)}")
            logger(f"Overlap: {sorted(overlap)}, Only in output: {only_output}")
            if mismatch:
                logger(f"Type mismatch: {mismatch}")

        details = {
            "only_output": only_output[:10],
            "only_target": only_target[:10],
            "mismatch": mismatch[:10],
            "mismatch_info": mismatch_info[:10],
        }

        if not overlap and not mismatch:
            return "none", details
        if only_output or mismatch:
            return "partial", details
        return "match", details

    target_dir = _expand_path(target_dir)
    if not os.path.isdir(target_dir):
        if logger:
            logger(f"Target directory does not exist, will create: {target_dir}")
        return "empty_target", {}

    output_items = {name for name in os.listdir(output_dir) if not name.startswith('.')}
    target_items = {name for name in os.listdir(target_dir) if not name.startswith('.')}

    if not target_items:
        return "empty_target", {}

    if not output_items:
        return "match", {}

    overlap = output_items & target_items
    only_output = sorted(output_items - target_items)
    only_target = sorted(target_items - output_items)

    # Check type mismatches for overlapping names
    mismatch = []
    mismatch_info = []
    for name in sorted(overlap):
        out_path = os.path.join(output_dir, name)
        tgt_path = os.path.join(target_dir, name)
        out_is_dir = os.path.isdir(out_path)
        tgt_is_dir = os.path.isdir(tgt_path)
        if out_is_dir != tgt_is_dir:
            mismatch.append(name)
            mismatch_info.append({
                "name": name,
                "output_type": "directory" if out_is_dir else "file",
                "target_type": "directory" if tgt_is_dir else "file",
            })

    if logger:
        logger(f"Output items: {sorted(output_items)}")
        logger(f"Target items: {sorted(target_items)}")
        logger(f"Overlap: {sorted(overlap)}, Only in output: {only_output}")
        if mismatch:
            logger(f"Type mismatch: {mismatch}")

    details = {
        "only_output": only_output[:10],
        "only_target": only_target[:10],
        "mismatch": mismatch[:10],
        "mismatch_info": mismatch_info[:10],
    }

    if not overlap and not mismatch:
        return "none", details
    if only_output or mismatch:
        return "partial", details
    return "match", details


def _run_cmd(cmd: list, logger=None) -> int:
    """Run a shell command and stream output to logger."""
    if logger:
        logger(f"Executing command: {' '.join(cmd)}")
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        for line in proc.stdout:
            line = line.rstrip()
            if logger:
                logger(line)
        proc.wait()
        return proc.returncode
    except Exception as e:
        if logger:
            logger(f"Command execution failed: {e}")
        return -1


def _run_ssh_cmd(user_host: str, password: str, remote_cmd: str, logger=None):
    """Run a remote command via sshpass/ssh and return (returncode, stdout_lines)."""
    cmd = [
        "sshpass", "-p", password,
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        user_host, remote_cmd
    ]
    if logger:
        logger(f"Executing ssh command on {user_host}: {remote_cmd}")
    try:
        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if proc.returncode != 0 and logger:
            logger(f"SSH stderr: {proc.stderr.strip()}")
        return proc.returncode, proc.stdout.splitlines()
    except Exception as e:
        if logger:
            logger(f"SSH command execution failed: {e}")
        return -1, []


def _list_remote_toplevel_items(remote_path: str, password: str, logger=None) -> list:
    """List top-level non-hidden items of a remote directory."""
    user_host, remote_dir = parse_remote(remote_path)
    remote_cmd = f"ls -A1 {shlex.quote(remote_dir)} 2>/dev/null || true"
    ret, lines = _run_ssh_cmd(user_host, password, remote_cmd, logger)
    if ret != 0:
        return []
    return [line.strip() for line in lines if line.strip() and not line.strip().startswith('.')]


def _list_remote_paths(remote_path: str, password: str, logger=None) -> list:
    """List all non-hidden relative paths under a remote directory."""
    user_host, remote_dir = parse_remote(remote_path)
    escaped_dir = re.escape(remote_dir)
    remote_cmd = (
        f"find {shlex.quote(remote_dir)} -not -path '*/\\.*' | "
        f"sed 's|^{escaped_dir}/||; /^$/d'"
    )
    ret, lines = _run_ssh_cmd(user_host, password, remote_cmd, logger)
    if ret != 0:
        return []
    return sorted([line.strip() for line in lines if line.strip()])


def _copy_dir_local(source_dir: str, dest_dir: str, logger=None) -> bool:
    """Local copy. If dest exists and is a directory, copy contents into it."""
    if os.path.exists(dest_dir) and os.path.isdir(dest_dir):
        # Copy contents instead of replacing the directory itself
        for item in os.listdir(source_dir):
            src_item = os.path.join(source_dir, item)
            dst_item = os.path.join(dest_dir, item)
            if os.path.isdir(src_item):
                shutil.copytree(src_item, dst_item, dirs_exist_ok=True)
            else:
                shutil.copy2(src_item, dst_item)
        if logger:
            logger(f"Local copy completed: {source_dir} -> {dest_dir}")
        return True
    else:
        shutil.copytree(source_dir, dest_dir, dirs_exist_ok=True)
        if logger:
            logger(f"Local copy completed: {source_dir} -> {dest_dir}")
        return True


def _copy_dir_remote(source_dir: str, remote_path: str, password: str, logger=None) -> bool:
    """Copy directory to remote via scp using sshpass."""
    user_host, remote_dir = parse_remote(remote_path)

    # Ensure remote parent directory exists
    mkdir_cmd = [
        "sshpass", "-p", password,
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        user_host, f"mkdir -p {shlex.quote(remote_dir)}"
    ]
    if logger:
        logger(f"Executing command: {' '.join(mkdir_cmd[:2])} *** {' '.join(mkdir_cmd[3:])}")
    ret = _run_cmd(mkdir_cmd, logger)
    if ret != 0:
        if logger:
            logger("Remote directory creation failed, continuing with copy attempt...")

    # scp -r source_dir/* user@host:/remote_dir/
    # Build file list inside source_dir and scp each to remote
    cmd_base = [
        "sshpass", "-p", password,
        "scp", "-r", "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
    ]

    for item in os.listdir(source_dir):
        src_item = os.path.join(source_dir, item)
        dest_uri = f"{user_host}:{remote_dir}/"

        # Remove remote counterpart first to avoid nested directory bug
        # (scp -r src_item user@host:/remote_dir/ copies into existing dir)
        rm_cmd = [
            "sshpass", "-p", password,
            "ssh", "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            user_host, f"rm -rf {shlex.quote(os.path.join(remote_dir, item))}"
        ]
        if logger:
            logger(f"Executing command: sshpass -p *** ssh ... rm -rf {shlex.quote(os.path.join(remote_dir, item))}")
        _run_cmd(rm_cmd, logger)

        cmd = cmd_base + [src_item, dest_uri]
        if logger:
            logger(f"Executing command: sshpass -p *** scp -r ... {src_item} {dest_uri}")
        ret = _run_cmd(cmd, logger)
        if ret != 0:
            if logger:
                logger(f"Copy failed: {src_item}")
            return False
    if logger:
        logger("Remote copy completed")
    return True


def _backup_from_remote(remote_path: str, backup_dir: str, password: str, logger=None) -> str:
    """Backup remote directory to local via scp using sshpass."""
    user_host, remote_dir = parse_remote(remote_path)
    basename = os.path.basename(os.path.normpath(remote_dir)) or "remote_backup"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_name = f"{basename}_{user_host}_{timestamp}"
    dest_path = os.path.join(backup_dir, dest_name)

    os.makedirs(backup_dir, exist_ok=True)

    cmd = [
        "sshpass", "-p", password,
        "scp", "-r", "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        f"{user_host}:{remote_dir}", dest_path,
    ]
    if logger:
        logger(f"Executing command: sshpass -p *** scp -r ... {user_host}:{remote_dir} {dest_path}")
    ret = _run_cmd(cmd, logger)
    if ret != 0:
        raise RuntimeError(f"Remote backup failed: {user_host}:{remote_dir} -> {dest_path}")
    if logger:
        logger(f"Remote backup completed: {user_host}:{remote_dir} -> {dest_path}")
    return dest_path


def backup(target_dir: str, backup_dir: str, password: str = "", logger=None) -> str:
    """Backup target_dir into backup_dir/target_basename_YYYYMMDD_HHMMSS/."""
    backup_dir = _expand_path(backup_dir)

    if is_remote(target_dir):
        if not password:
            raise ValueError("Remote target backup requires SSH password")
        return _backup_from_remote(target_dir, backup_dir, password, logger)

    target_dir = _expand_path(target_dir)
    if not os.path.isdir(target_dir):
        raise ValueError(f"Target directory does not exist: {target_dir}")

    basename = os.path.basename(os.path.normpath(target_dir))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_name = f"{basename}_{timestamp}"
    dest_path = os.path.join(backup_dir, dest_name)

    os.makedirs(backup_dir, exist_ok=True)

    # Use cp -r command for logging, but fallback to shutil.copytree
    cmd = ["cp", "-r", target_dir, dest_path]
    if logger:
        logger(f"Executing command: {' '.join(cmd)}")
    ret = _run_cmd(cmd, logger)
    if ret != 0:
        if logger:
            logger(f"cp command failed, falling back to shutil copy...")
        shutil.copytree(target_dir, dest_path)
        if logger:
            logger(f"Local copy completed: {target_dir} -> {dest_path}")

    return dest_path


def find_overlapping_paths(output_dir: str, target_dir: str, password: str = "", logger=None) -> list:
    """Return sorted list of relative paths that exist in both output and target."""
    output_dir = _expand_path(output_dir)
    if not os.path.isdir(output_dir):
        return []

    if is_remote(target_dir):
        remote_items = _list_remote_paths(target_dir, password, logger)
        output_items = _list_visible(output_dir)
        overlap_set = set(output_items) & set(remote_items)
        return sorted(overlap_set)

    target_dir = _expand_path(target_dir)
    if not os.path.isdir(target_dir):
        return []

    overlaps = []
    for dirpath, dirnames, filenames in os.walk(output_dir):
        rel_dir = os.path.relpath(dirpath, output_dir)
        if rel_dir == '.':
            rel_dir = ''
        for name in dirnames + filenames:
            rel_path = os.path.join(rel_dir, name) if rel_dir else name
            if os.path.lexists(os.path.join(target_dir, rel_path)):
                overlaps.append(rel_path)
    return sorted(overlaps)


def backup_overlapping_files(output_dir: str, target_dir: str, backup_dir: str, password: str = "", logger=None):
    """
    Backup files in target_dir that would be overwritten by output_dir.

    For local targets: backs up only overlapping files/dirs, preserving structure.
    For remote targets: performs a full backup of target_dir.

    Returns the backup directory path, or None if nothing to backup.
    """
    output_dir = _expand_path(output_dir)
    backup_dir = _expand_path(backup_dir)

    if is_remote(target_dir):
        if not password:
            raise ValueError("Remote target backup requires SSH password")
        if logger:
            logger("Remote target: performing full backup before patch")
        return backup(target_dir, backup_dir, password=password, logger=logger)

    target_dir = _expand_path(target_dir)
    if not os.path.isdir(target_dir) or not os.path.isdir(output_dir):
        return None

    overlaps = find_overlapping_paths(output_dir, target_dir)
    if not overlaps:
        if logger:
            logger("No overlapping files to backup")
        return None

    # Filter out paths whose parent directory is also in the overlap list
    overlap_set = set(overlaps)
    filtered = []
    for p in overlaps:
        parent = os.path.dirname(p)
        has_parent = False
        while parent:
            if parent in overlap_set:
                has_parent = True
                break
            parent = os.path.dirname(parent)
        if not has_parent:
            filtered.append(p)

    basename = os.path.basename(os.path.normpath(target_dir))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_name = f"{basename}_overwrite_{timestamp}"
    dest_path = os.path.join(backup_dir, dest_name)
    os.makedirs(dest_path, exist_ok=True)

    for rel_path in filtered:
        src = os.path.join(target_dir, rel_path)
        dst = os.path.join(dest_path, rel_path)
        dst_parent = os.path.dirname(dst)
        if dst_parent and not os.path.exists(dst_parent):
            os.makedirs(dst_parent, exist_ok=True)
        if os.path.isdir(src):
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dst)

    if logger:
        logger(f"Backed up {len(filtered)} overlapping item(s) to {dest_path}")
    return dest_path


def patch(output_dir: str, target_dir: str, password: str = "", logger=None) -> bool:
    """Patch: copy output_dir into target_dir."""
    output_dir = _expand_path(output_dir)
    if not os.path.isdir(output_dir):
        raise ValueError(f"Output directory does not exist: {output_dir}")

    if is_remote(target_dir):
        return _copy_dir_remote(output_dir, target_dir, password, logger)
    else:
        target_dir = _expand_path(target_dir)
        os.makedirs(target_dir, exist_ok=True)
        # For local copy, try cp -r first for logging visibility
        # cp -r output_dir/* target_dir/  doesn't work well with hidden files
        # Use rsync if available, otherwise cp -r then merge
        cmd = ["cp", "-r", output_dir + "/.", target_dir]
        if logger:
            logger(f"Executing command: cp -r {output_dir}/. {target_dir}")
        ret = _run_cmd(["cp", "-r", output_dir + "/.", target_dir], logger)
        if ret != 0:
            if logger:
                logger("cp command failed, falling back to shutil copy...")
            _copy_dir_local(output_dir, target_dir, logger)
        return True


def rollback(backup_timestamp_dir: str, target_dir: str, password: str = "", logger=None) -> bool:
    """Rollback: copy backup_timestamp_dir into target_dir."""
    backup_timestamp_dir = _expand_path(backup_timestamp_dir)
    if not os.path.isdir(backup_timestamp_dir):
        raise ValueError(f"Backup directory does not exist: {backup_timestamp_dir}")

    if is_remote(target_dir):
        return _copy_dir_remote(backup_timestamp_dir, target_dir, password, logger)
    else:
        target_dir = _expand_path(target_dir)
        os.makedirs(target_dir, exist_ok=True)
        if logger:
            logger(f"Executing command: cp -r {backup_timestamp_dir}/. {target_dir}")
        ret = _run_cmd(["cp", "-r", backup_timestamp_dir + "/.", target_dir], logger)
        if ret != 0:
            if logger:
                logger("cp command failed, falling back to shutil copy...")
            _copy_dir_local(backup_timestamp_dir, target_dir, logger)
        return True

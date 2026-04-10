"""
Core library for backup, patch, and rollback operations.
"""

import os
import shutil
import re
import subprocess
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


def verify_structure(source_dir: str, target_dir: str, logger=None) -> bool:
    """
    Verify that directory structures are compatible (ignoring hidden files).
    For remote, only basic checks on source side.
    """
    source_dir = _expand_path(source_dir)
    if not os.path.isdir(source_dir):
        if logger:
            logger(f"源目录不存在: {source_dir}")
        return False

    if is_remote(target_dir):
        # For remote targets, we only verify source exists and is non-empty
        if logger:
            logger(f"远程目标跳过本地结构校验: {target_dir}")
        return True

    target_dir = _expand_path(target_dir)
    if not os.path.isdir(target_dir):
        if logger:
            logger(f"目标目录不存在，将创建: {target_dir}")
        return True

    source_items = _list_visible(source_dir)
    target_items = _list_visible(target_dir)

    if logger:
        logger(f"源可见文件数: {len(source_items)}, 目标可见文件数: {len(target_items)}")

    # Consider compatible if at least one overlapping relative path exists
    # OR if target has no visible files (empty target is always compatible)
    if not target_items:
        return True

    overlap = set(source_items) & set(target_items)
    if overlap:
        return True

    if logger:
        logger("结构校验: 无可见文件交集")
    return False


def check_patch_compatibility(output_dir: str, target_dir: str, logger=None):
    """
    Check compatibility between output_dir and target_dir for patching.

    Returns (result, details_dict):
      result:
      - "match"       : all top-level items overlap (target has all output items)
      - "partial"     : some items overlap, some differ
      - "none"        : no overlapping top-level items at all
      - "empty_target": target does not exist or is empty
      - "remote"      : target is remote (skip local check)

      details_dict may contain keys:
      - "only_output": items only in output (max 10)
      - "only_target": items only in target (max 10)
      - "mismatch"   : items present in both but type differs (file vs dir) (max 10)
    """
    output_dir = _expand_path(output_dir)
    if is_remote(target_dir):
        if logger:
            logger(f"远程目标跳过兼容性检查: {target_dir}")
        return "remote", {}

    target_dir = _expand_path(target_dir)
    if not os.path.isdir(target_dir):
        if logger:
            logger(f"目标目录不存在，将创建: {target_dir}")
        return "empty_target", {}

    output_items = {name for name in os.listdir(output_dir) if not name.startswith('.')}
    target_items = {name for name in os.listdir(target_dir) if not name.startswith('.')}

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
                "output_type": "目录" if out_is_dir else "文件",
                "target_type": "目录" if tgt_is_dir else "文件",
            })

    if logger:
        logger(f"Output 项: {sorted(output_items)}")
        logger(f"Target 项: {sorted(target_items)}")
        logger(f"交集: {sorted(overlap)}, 仅 Output 有: {only_output}")
        if mismatch:
            logger(f"类型不一致: {mismatch}")

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
        logger(f"执行命令: {' '.join(cmd)}")
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
            logger(f"命令执行异常: {e}")
        return -1


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
            logger(f"本地复制完成: {source_dir} -> {dest_dir}")
        return True
    else:
        shutil.copytree(source_dir, dest_dir, dirs_exist_ok=True)
        if logger:
            logger(f"本地复制完成: {source_dir} -> {dest_dir}")
        return True


def _copy_dir_remote(source_dir: str, remote_path: str, password: str, logger=None) -> bool:
    """Copy directory to remote via scp using sshpass."""
    user_host, remote_dir = parse_remote(remote_path)

    # Ensure remote parent directory exists
    mkdir_cmd = [
        "sshpass", "-p", password,
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        user_host, f"mkdir -p {remote_dir}"
    ]
    if logger:
        logger(f"执行命令: {' '.join(mkdir_cmd[:2])} *** {' '.join(mkdir_cmd[3:])}")
    ret = _run_cmd(mkdir_cmd, logger)
    if ret != 0:
        if logger:
            logger("远程目录创建失败，继续尝试复制...")

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
        cmd = cmd_base + [src_item, dest_uri]
        if logger:
            logger(f"执行命令: sshpass -p *** scp -r ... {src_item} {dest_uri}")
        ret = _run_cmd(cmd, logger)
        if ret != 0:
            if logger:
                logger(f"复制失败: {src_item}")
            return False
    if logger:
        logger("远程复制完成")
    return True


def backup(target_dir: str, backup_dir: str, logger=None) -> str:
    """Backup target_dir into backup_dir/target_basename_YYYYMMDD_HHMMSS/."""
    target_dir = _expand_path(target_dir)
    backup_dir = _expand_path(backup_dir)
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
        logger(f"执行命令: {' '.join(cmd)}")
    ret = _run_cmd(cmd, logger)
    if ret != 0:
        if logger:
            logger(f"cp 命令失败，改用 shutil 复制...")
        shutil.copytree(target_dir, dest_path)
        if logger:
            logger(f"本地复制完成: {target_dir} -> {dest_path}")

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
            logger(f"执行命令: cp -r {output_dir}/. {target_dir}")
        ret = _run_cmd(["cp", "-r", output_dir + "/.", target_dir], logger)
        if ret != 0:
            if logger:
                logger("cp 命令失败，改用 shutil 复制...")
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
            logger(f"执行命令: cp -r {backup_timestamp_dir}/. {target_dir}")
        ret = _run_cmd(["cp", "-r", backup_timestamp_dir + "/.", target_dir], logger)
        if ret != 0:
            if logger:
                logger("cp 命令失败，改用 shutil 复制...")
            _copy_dir_local(backup_timestamp_dir, target_dir, logger)
        return True

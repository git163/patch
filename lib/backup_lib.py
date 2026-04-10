"""
Core library for backup, patch, and rollback operations.
"""

import os
import shutil
import re
import subprocess
from datetime import datetime


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
    if not os.path.isdir(backup_dir):
        return []
    dirs = []
    for name in os.listdir(backup_dir):
        full = os.path.join(backup_dir, name)
        if os.path.isdir(full) and re.search(r"_\d{8}_\d{6}$", name):
            dirs.append(full)
    dirs.sort(reverse=True)
    return dirs


def verify_structure(source_dir: str, target_dir: str, logger=None) -> bool:
    """
    Verify that directory structures are compatible.
    For local comparison use diff -rq.
    For remote, only basic checks on source side.
    """
    if not os.path.isdir(source_dir):
        if logger:
            logger(f"源目录不存在: {source_dir}")
        return False

    if is_remote(target_dir):
        # For remote targets, we only verify source exists and is non-empty
        if logger:
            logger(f"远程目标跳过本地结构校验: {target_dir}")
        return True

    if not os.path.isdir(target_dir):
        if logger:
            logger(f"目标目录不存在，将创建: {target_dir}")
        return True

    # Use diff -rq to compare structures (files presence only)
    cmd = ["diff", "-rq", source_dir, target_dir]
    if logger:
        logger(f"执行命令: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if logger:
            logger(result.stdout.strip())
        # diff returns 0 if identical, 1 if differ, >1 if error
        if result.returncode > 1:
            if logger:
                logger(f"结构校验出错，退出码: {result.returncode}")
            return False
        return True
    except Exception as e:
        if logger:
            logger(f"结构校验异常: {e}")
        return False


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
    if not os.path.isdir(output_dir):
        raise ValueError(f"Output directory does not exist: {output_dir}")

    if is_remote(target_dir):
        return _copy_dir_remote(output_dir, target_dir, password, logger)
    else:
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
    if not os.path.isdir(backup_timestamp_dir):
        raise ValueError(f"Backup directory does not exist: {backup_timestamp_dir}")

    if is_remote(target_dir):
        return _copy_dir_remote(backup_timestamp_dir, target_dir, password, logger)
    else:
        os.makedirs(target_dir, exist_ok=True)
        if logger:
            logger(f"执行命令: cp -r {backup_timestamp_dir}/. {target_dir}")
        ret = _run_cmd(["cp", "-r", backup_timestamp_dir + "/.", target_dir], logger)
        if ret != 0:
            if logger:
                logger("cp 命令失败，改用 shutil 复制...")
            _copy_dir_local(backup_timestamp_dir, target_dir, logger)
        return True

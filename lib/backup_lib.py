"""
Core library for backup, patch, and rollback operations.
"""

import os
import shutil
import re
import stat
import shlex
import tempfile
from datetime import datetime
from typing import Optional

import paramiko


def _expand_path(path: str) -> str:
    """Expand ~ to home directory for local paths."""
    if is_remote(path):
        return path
    return os.path.expanduser(path)


def _ignore_hidden(dir, contents):
    """Ignore hidden files and directories (e.g. .DS_Store)."""
    return [name for name in contents if name.startswith('.')]


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


def list_backups(backup_dir: str, backup_password: str = "", logger=None) -> list:
    """List timestamped backup directories under backup_dir."""
    backup_dir = _expand_path(backup_dir)
    if is_remote(backup_dir):
        if not backup_password:
            raise ValueError("Remote backup listing requires SSH password")
        return _list_remote_backups(backup_dir, backup_password, logger)
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


def check_patch_compatibility(output_dir: str, target_dir: str, output_password: str = "", target_password: str = "", logger=None):
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
    output_remote = is_remote(output_dir)
    target_remote = is_remote(target_dir)

    if output_remote and not output_password:
        raise ValueError("Remote output compatibility check requires SSH password")
    if target_remote and not target_password:
        raise ValueError("Remote target compatibility check requires SSH password")

    if output_remote and logger:
        logger(f"Remote output, performing compatibility check: {output_dir}")
    if target_remote and logger:
        logger(f"Remote target, performing compatibility check: {target_dir}")

    try:
        output_items = _get_toplevel_items(output_dir, output_password, logger)
    except Exception as e:
        if logger:
            logger(f"Failed to list output items: {e}")
        raise

    try:
        target_items = _get_toplevel_items(target_dir, target_password, logger)
    except Exception as e:
        if logger:
            logger(f"Failed to list target items: {e}")
        if target_remote:
            return "remote", {}
        if logger:
            logger(f"Target directory does not exist, will create: {target_dir}")
        return "empty_target", {}

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
        out_path = _join_remote(output_dir, name) if output_remote else os.path.join(output_dir, name)
        tgt_path = _join_remote(target_dir, name) if target_remote else os.path.join(target_dir, name)
        out_is_dir = _is_dir_path(out_path, output_password, logger)
        tgt_is_dir = _is_dir_path(tgt_path, target_password, logger)
        if out_is_dir != tgt_is_dir:
            mismatch.append(name)
            mismatch_info.append({
                "name": name,
                "output_type": "directory" if out_is_dir else "file",
                "target_type": "directory" if tgt_is_dir else "file",
            })

    if logger:
        logger(f"Output items: {sorted(output_items)}")
        tgt_label = "Remote target items" if target_remote else "Target items"
        logger(f"{tgt_label}: {sorted(target_items)}")
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


def _ssh_connect(user_host: str, password: str) -> paramiko.SSHClient:
    """Create and connect a paramiko SSHClient."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    if '@' in user_host:
        username, hostname = user_host.split('@', 1)
    else:
        username = None
        hostname = user_host
    client.connect(
        hostname=hostname,
        username=username,
        password=password,
        look_for_keys=False,
        allow_agent=False,
    )
    return client


def _run_ssh_cmd(user_host: str, password: str, remote_cmd: str, logger=None):
    """Run a remote command via paramiko and return (returncode, stdout_lines)."""
    if logger:
        logger(f"Executing ssh command on {user_host}: {remote_cmd}")
    try:
        client = _ssh_connect(user_host, password)
        try:
            _stdin, stdout, stderr = client.exec_command(remote_cmd)
            exit_status = stdout.channel.recv_exit_status()
            out_lines = stdout.read().decode("utf-8", errors="replace").splitlines()
            err_text = stderr.read().decode("utf-8", errors="replace").strip()
            if exit_status != 0 and logger and err_text:
                logger(f"SSH stderr: {err_text}")
            return exit_status, out_lines
        finally:
            client.close()
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


def _remove_if_empty(dir_path: str, logger=None) -> bool:
    """Remove directory if it exists and contains no files (even inside subdirs). Return True if removed."""
    if not os.path.isdir(dir_path):
        return False
    for _, _, filenames in os.walk(dir_path):
        if filenames:
            return False
    shutil.rmtree(dir_path)
    if logger:
        logger(f"Removed empty backup directory: {dir_path}")
    return True


def _get_toplevel_items(dir_path: str, password: str, logger=None) -> set:
    """Get top-level non-hidden items for local or remote directory."""
    if is_remote(dir_path):
        return {name for name in _list_remote_toplevel_items(dir_path, password, logger) if not name.startswith('.')}
    return {name for name in os.listdir(dir_path) if not name.startswith('.')}


def _is_dir_path(path: str, password: str, logger=None) -> bool:
    """Check if path is a directory (local or remote)."""
    if is_remote(path):
        user_host, remote_dir = parse_remote(path)
        test_cmd = f"test -d {shlex.quote(remote_dir)} && echo dir || echo file"
        ret, out = _run_ssh_cmd(user_host, password, test_cmd, logger)
        return ret == 0 and out and out[0].strip() == "dir"
    return os.path.isdir(path)


def _join_remote(remote_base: str, name: str) -> str:
    """Join a remote base path with a name."""
    user_host, remote_dir = parse_remote(remote_base)
    return f"{user_host}:{os.path.join(remote_dir, name).replace(chr(92), '/')}"


def _copy_dir_remote_to_local(remote_path: str, local_dir: str, password: str, logger=None) -> bool:
    """Download a remote directory to local via paramiko SFTP."""
    user_host, remote_dir = parse_remote(remote_path)
    os.makedirs(local_dir, exist_ok=True)
    client = _ssh_connect(user_host, password)
    try:
        sftp = client.open_sftp()
        try:
            _sftp_get_dir(sftp, remote_dir, local_dir, logger)
        finally:
            sftp.close()
    finally:
        client.close()
    if logger:
        logger(f"Remote download completed: {remote_path} -> {local_dir}")
    return True


def _copy_dir_remote_to_remote(source_remote: str, dest_remote: str, source_password: str = "", dest_password: str = "", logger=None) -> bool:
    """Copy directory from remote to remote via local temp."""
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        _copy_dir_remote_to_local(source_remote, tmpdir, source_password, logger)
        _copy_dir_remote(tmpdir, dest_remote, dest_password, logger)
    if logger:
        logger(f"Remote-to-remote copy completed: {source_remote} -> {dest_remote}")
    return True


def _backup_to_remote(target_dir: str, remote_path: str, password: str, logger=None) -> Optional[str]:
    """Backup a local directory to a remote backup directory."""
    user_host, remote_dir = parse_remote(remote_path)
    basename = os.path.basename(os.path.normpath(target_dir)) or "backup"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_name = f"{basename}_{timestamp}"
    dest_remote = f"{user_host}:{remote_dir}/{dest_name}"

    _copy_dir_remote(target_dir, dest_remote, password, logger)

    if logger:
        logger(f"Remote backup completed: {target_dir} -> {dest_remote}")
    return dest_remote


def _backup_remote_to_remote(target_dir: str, remote_path: str, target_password: str = "", backup_password: str = "", logger=None) -> Optional[str]:
    """Backup a remote directory to another remote backup directory."""
    target_user_host, target_remote_dir = parse_remote(target_dir)
    backup_user_host, backup_remote_dir = parse_remote(remote_path)
    basename = os.path.basename(os.path.normpath(target_remote_dir)) or "remote_backup"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_name = f"{basename}_{target_user_host}_{timestamp}"
    dest_remote = f"{backup_user_host}:{backup_remote_dir}/{dest_name}"

    _copy_dir_remote_to_remote(target_dir, dest_remote, target_password, backup_password, logger)

    if logger:
        logger(f"Remote-to-remote backup completed: {target_dir} -> {dest_remote}")
    return dest_remote


def _list_remote_backups(remote_path: str, password: str, logger=None) -> list:
    """List timestamped backup directories under a remote backup directory."""
    user_host, remote_dir = parse_remote(remote_path)
    remote_cmd = f"ls -1 {shlex.quote(remote_dir)} 2>/dev/null || true"
    ret, lines = _run_ssh_cmd(user_host, password, remote_cmd, logger)
    if ret != 0:
        return []
    backups = []
    for name in lines:
        name = name.strip()
        if re.search(r"_\d{8}_\d{6}$", name):
            backups.append(f"{user_host}:{remote_dir}/{name}")
    backups.sort(reverse=True)
    return backups


def _copy_dir_local(source_dir: str, dest_dir: str, logger=None) -> bool:
    """Local copy. If dest exists and is a directory, copy contents into it. Skips hidden files."""
    if os.path.exists(dest_dir) and os.path.isdir(dest_dir):
        # Copy contents instead of replacing the directory itself
        for item in os.listdir(source_dir):
            if item.startswith('.'):
                continue
            src_item = os.path.join(source_dir, item)
            dst_item = os.path.join(dest_dir, item)
            if os.path.isdir(src_item):
                shutil.copytree(src_item, dst_item, dirs_exist_ok=True, ignore=_ignore_hidden)
            else:
                shutil.copy2(src_item, dst_item)
        if logger:
            logger(f"Local copy completed: {source_dir} -> {dest_dir}")
        return True
    else:
        shutil.copytree(source_dir, dest_dir, dirs_exist_ok=True, ignore=_ignore_hidden)
        if logger:
            logger(f"Local copy completed: {source_dir} -> {dest_dir}")
        return True


def _sftp_put_dir(sftp, local_dir: str, remote_dir: str, logger=None):
    """Recursively upload a local directory to remote via SFTP."""
    for item in os.listdir(local_dir):
        if item.startswith('.'):
            continue
        local_path = os.path.join(local_dir, item)
        remote_path = remote_dir + '/' + item
        if os.path.isdir(local_path):
            try:
                sftp.mkdir(remote_path)
            except IOError:
                pass
            _sftp_put_dir(sftp, local_path, remote_path, logger)
        else:
            if logger:
                logger(f"Uploading {local_path} -> {remote_path}")
            sftp.put(local_path, remote_path)


def _sftp_get_dir(sftp, remote_dir: str, local_dir: str, logger=None):
    """Recursively download a remote directory to local via SFTP."""
    try:
        items = sftp.listdir_attr(remote_dir)
    except IOError:
        return
    for item in items:
        if item.filename.startswith('.'):
            continue
        remote_path = remote_dir + '/' + item.filename
        local_path = os.path.join(local_dir, item.filename)
        if stat.S_ISDIR(item.st_mode):
            os.makedirs(local_path, exist_ok=True)
            _sftp_get_dir(sftp, remote_path, local_path, logger)
        else:
            if logger:
                logger(f"Downloading {remote_path} -> {local_path}")
            sftp.get(remote_path, local_path)


def _copy_dir_remote(source_dir: str, remote_path: str, password: str, logger=None) -> bool:
    """Copy directory to remote via paramiko SFTP."""
    user_host, remote_dir = parse_remote(remote_path)

    client = _ssh_connect(user_host, password)
    try:
        _stdin, stdout, _stderr = client.exec_command(f"mkdir -p {shlex.quote(remote_dir)}")
        stdout.channel.recv_exit_status()

        sftp = client.open_sftp()
        try:
            for item in os.listdir(source_dir):
                if item.startswith('.'):
                    continue
                src_item = os.path.join(source_dir, item)
                remote_item = remote_dir + '/' + item

                _stdin, stdout, _stderr = client.exec_command(
                    f"rm -rf {shlex.quote(remote_item)}"
                )
                stdout.channel.recv_exit_status()

                if os.path.isdir(src_item):
                    try:
                        sftp.mkdir(remote_item)
                    except IOError:
                        pass
                    _sftp_put_dir(sftp, src_item, remote_item, logger)
                else:
                    if logger:
                        logger(f"Uploading {src_item} -> {remote_item}")
                    sftp.put(src_item, remote_item)
        finally:
            sftp.close()
    finally:
        client.close()

    if logger:
        logger("Remote copy completed")
    return True


def _backup_from_remote(remote_path: str, backup_dir: str, password: str, logger=None) -> Optional[str]:
    """Backup remote directory to local via paramiko SFTP."""
    user_host, remote_dir = parse_remote(remote_path)
    basename = os.path.basename(os.path.normpath(remote_dir)) or "remote_backup"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_name = f"{basename}_{user_host}_{timestamp}"
    dest_path = os.path.join(backup_dir, dest_name)

    os.makedirs(backup_dir, exist_ok=True)
    os.makedirs(dest_path, exist_ok=True)

    client = _ssh_connect(user_host, password)
    try:
        sftp = client.open_sftp()
        try:
            _sftp_get_dir(sftp, remote_dir, dest_path, logger)
        finally:
            sftp.close()
    finally:
        client.close()

    if logger:
        logger(f"Remote backup completed: {user_host}:{remote_dir} -> {dest_path}")
    if _remove_if_empty(dest_path, logger):
        return None
    return dest_path


def backup(target_dir: str, backup_dir: str, target_password: str = "", backup_password: str = "", logger=None) -> Optional[str]:
    """Backup target_dir into backup_dir/target_basename_YYYYMMDD_HHMMSS/."""
    target_remote = is_remote(target_dir)
    backup_remote = is_remote(backup_dir)

    if target_remote and not target_password:
        raise ValueError("Remote target backup requires SSH password")
    if backup_remote and not backup_password:
        raise ValueError("Remote backup directory requires SSH password")

    if target_remote and not backup_remote:
        return _backup_from_remote(target_dir, _expand_path(backup_dir), target_password, logger)

    if not target_remote and backup_remote:
        target_dir = _expand_path(target_dir)
        if not os.path.isdir(target_dir):
            raise ValueError(f"Target directory does not exist: {target_dir}")
        return _backup_to_remote(target_dir, backup_dir, backup_password, logger)

    if target_remote and backup_remote:
        return _backup_remote_to_remote(target_dir, backup_dir, target_password, backup_password, logger)

    # Both local
    target_dir = _expand_path(target_dir)
    backup_dir = _expand_path(backup_dir)
    if not os.path.isdir(target_dir):
        raise ValueError(f"Target directory does not exist: {target_dir}")

    basename = os.path.basename(os.path.normpath(target_dir))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_name = f"{basename}_{timestamp}"
    dest_path = os.path.join(backup_dir, dest_name)

    os.makedirs(backup_dir, exist_ok=True)

    # Copy target_dir to dest_path, skipping hidden files
    if logger:
        logger(f"Backing up {target_dir} -> {dest_path}")
    _copy_dir_local(target_dir, dest_path, logger)

    if _remove_if_empty(dest_path, logger):
        return None
    return dest_path


def find_overlapping_paths(output_dir: str, target_dir: str, output_password: str = "", target_password: str = "", logger=None) -> list:
    """Return sorted list of relative paths that exist in both output and target."""
    output_dir = _expand_path(output_dir)
    output_remote = is_remote(output_dir)
    target_remote = is_remote(target_dir)

    if output_remote and not output_password:
        raise ValueError("Remote output overlap detection requires SSH password")
    if target_remote and not target_password:
        raise ValueError("Remote target overlap detection requires SSH password")

    if output_remote or target_remote:
        output_items = _list_remote_paths(output_dir, output_password, logger) if output_remote else _list_visible(output_dir)
        target_items = _list_remote_paths(target_dir, target_password, logger) if target_remote else _list_visible(target_dir)
        overlap_set = set(output_items) & set(target_items)
        return sorted(overlap_set)

    target_dir = _expand_path(target_dir)
    if not os.path.isdir(target_dir) or not os.path.isdir(output_dir):
        return []

    overlaps = []
    for dirpath, dirnames, filenames in os.walk(output_dir):
        # Filter hidden directories to prevent walking into them
        dirnames[:] = [d for d in dirnames if not d.startswith('.')]
        rel_dir = os.path.relpath(dirpath, output_dir)
        if rel_dir == '.':
            rel_dir = ''
        for name in filenames:
            if name.startswith('.'):
                continue
            rel_path = os.path.join(rel_dir, name) if rel_dir else name
            if os.path.lexists(os.path.join(target_dir, rel_path)):
                overlaps.append(rel_path)
        for name in dirnames:
            rel_path = os.path.join(rel_dir, name) if rel_dir else name
            if os.path.lexists(os.path.join(target_dir, rel_path)):
                overlaps.append(rel_path)
    return sorted(overlaps)


def backup_overlapping_files(output_dir: str, target_dir: str, backup_dir: str, output_password: str = "", target_password: str = "", backup_password: str = "", logger=None):
    """
    Backup files in target_dir that would be overwritten by output_dir.

    For local targets: backs up only overlapping files/dirs, preserving structure.
    For remote targets: performs a full backup of target_dir.

    Returns the backup directory path, or None if nothing to backup.
    """
    output_dir = _expand_path(output_dir)
    backup_remote = is_remote(backup_dir)

    if is_remote(target_dir):
        if not target_password:
            raise ValueError("Remote target backup requires SSH password")
        if logger:
            logger("Remote target: performing full backup before patch")
        user_host, remote_dir = parse_remote(target_dir)
        ret, _ = _run_ssh_cmd(user_host, target_password, f"test -d {shlex.quote(remote_dir)}")
        if ret != 0:
            if logger:
                logger(f"Remote target directory does not exist, skipping backup: {target_dir}")
            return None
        return backup(target_dir, backup_dir, target_password=target_password, backup_password=backup_password, logger=logger)

    target_dir = _expand_path(target_dir)
    if not os.path.isdir(target_dir) or not os.path.isdir(output_dir):
        return None

    overlaps = find_overlapping_paths(output_dir, target_dir, output_password=output_password, target_password=target_password)
    if not overlaps:
        if logger:
            logger("No overlapping files to backup")
        return None

    # Keep only the bottom-most overlapping items.
    # If a directory and its child both overlap, we back up the child
    # (file or deepest directory) instead of the parent to avoid copying
    # non-overlapping siblings and to precisely target what will be overwritten.
    overlap_set = set(overlaps)
    filtered = []
    for p in overlaps:
        is_parent = False
        for other in overlap_set:
            if other != p and other.startswith(p + os.sep):
                is_parent = True
                break
        if not is_parent:
            logger and logger(f"Will backup overlapping item: {p}")
            filtered.append(p)

    basename = os.path.basename(os.path.normpath(target_dir))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_name = f"{basename}_overwrite_{timestamp}"
    if backup_remote:
        if not backup_password:
            raise ValueError("Remote backup directory requires SSH password")
        dest_path = os.path.join(tempfile.mkdtemp(), dest_name)
    else:
        dest_path = os.path.join(_expand_path(backup_dir), dest_name)
    os.makedirs(dest_path, exist_ok=True)

    for rel_path in filtered:
        src = os.path.join(target_dir, rel_path)
        dst = os.path.join(dest_path, rel_path)
        dst_parent = os.path.dirname(dst)
        if dst_parent and not os.path.exists(dst_parent):
            os.makedirs(dst_parent, exist_ok=True)
        if os.path.isdir(src):
            shutil.copytree(src, dst, dirs_exist_ok=True, ignore=_ignore_hidden)
        else:
            shutil.copy2(src, dst)

    if _remove_if_empty(dest_path, logger):
        if logger:
            logger("No files were actually backed up, removed empty directory")
        if backup_remote:
            shutil.rmtree(os.path.dirname(dest_path))
        return None

    if backup_remote:
        remote_dest = f"{parse_remote(backup_dir)[0]}:{parse_remote(backup_dir)[1]}/{dest_name}"
        _copy_dir_remote(dest_path, remote_dest, backup_password, logger)
        shutil.rmtree(os.path.dirname(dest_path))
        if logger:
            logger(f"Backed up {len(filtered)} overlapping item(s) to {remote_dest}")
        return remote_dest

    if logger:
        logger(f"Backed up {len(filtered)} overlapping item(s) to {dest_path}")
    return dest_path


def patch(output_dir: str, target_dir: str, output_password: str = "", target_password: str = "", logger=None) -> bool:
    """Patch: copy output_dir into target_dir."""
    output_dir = _expand_path(output_dir)
    output_remote = is_remote(output_dir)
    target_remote = is_remote(target_dir)

    if output_remote and not output_password:
        raise ValueError("Remote output patch requires SSH password")
    if target_remote and not target_password:
        raise ValueError("Remote target patch requires SSH password")

    if output_remote and target_remote:
        _copy_dir_remote_to_remote(output_dir, target_dir, output_password, target_password, logger)
        return True
    elif output_remote and not target_remote:
        target_dir = _expand_path(target_dir)
        os.makedirs(target_dir, exist_ok=True)
        if logger:
            logger(f"Patching remote {output_dir} -> {target_dir} (ignoring hidden files)")
        _copy_dir_remote_to_local(output_dir, target_dir, output_password, logger)
        return True
    elif not output_remote and target_remote:
        if not os.path.isdir(output_dir):
            raise ValueError(f"Output directory does not exist: {output_dir}")
        return _copy_dir_remote(output_dir, target_dir, target_password, logger)
    else:
        if not os.path.isdir(output_dir):
            raise ValueError(f"Output directory does not exist: {output_dir}")
        target_dir = _expand_path(target_dir)
        os.makedirs(target_dir, exist_ok=True)
        if logger:
            logger(f"Patching {output_dir} -> {target_dir} (ignoring hidden files)")
        _copy_dir_local(output_dir, target_dir, logger)
        return True


def rollback(backup_timestamp_dir: str, target_dir: str, backup_password: str = "", target_password: str = "", logger=None) -> bool:
    """Rollback: copy backup_timestamp_dir into target_dir."""
    backup_timestamp_dir = _expand_path(backup_timestamp_dir)
    backup_remote = is_remote(backup_timestamp_dir)
    target_remote = is_remote(target_dir)

    if backup_remote and not backup_password:
        raise ValueError("Remote backup rollback requires SSH password")
    if target_remote and not target_password:
        raise ValueError("Remote target rollback requires SSH password")

    if backup_remote and target_remote:
        _copy_dir_remote_to_remote(backup_timestamp_dir, target_dir, backup_password, target_password, logger)
        return True
    elif backup_remote and not target_remote:
        target_dir = _expand_path(target_dir)
        os.makedirs(target_dir, exist_ok=True)
        if logger:
            logger(f"Rolling back remote {backup_timestamp_dir} -> {target_dir} (ignoring hidden files)")
        _copy_dir_remote_to_local(backup_timestamp_dir, target_dir, backup_password, logger)
        return True
    elif not backup_remote and target_remote:
        if not os.path.isdir(backup_timestamp_dir):
            raise ValueError(f"Backup directory does not exist: {backup_timestamp_dir}")
        return _copy_dir_remote(backup_timestamp_dir, target_dir, target_password, logger)
    else:
        if not os.path.isdir(backup_timestamp_dir):
            raise ValueError(f"Backup directory does not exist: {backup_timestamp_dir}")
        target_dir = _expand_path(target_dir)
        os.makedirs(target_dir, exist_ok=True)
        if logger:
            logger(f"Rolling back {backup_timestamp_dir} -> {target_dir} (ignoring hidden files)")
        _copy_dir_local(backup_timestamp_dir, target_dir, logger)
        return True

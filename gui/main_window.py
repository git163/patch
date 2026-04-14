#!/usr/bin/env python3
"""PySide2/6 GUI for backup and patch tool."""

import json
import os
import shlex
import sys
from typing import Optional

# Add project root to sys.path so lib can be imported
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Try PySide2 first, fallback to PySide6
try:
    from PySide2.QtWidgets import (
        QApplication, QWidget, QVBoxLayout, QHBoxLayout,
        QLabel, QLineEdit, QPushButton, QTextEdit,
        QMessageBox, QInputDialog, QDialog, QFileDialog,
        QListWidget, QListWidgetItem, QStyle,
    )
    from PySide2.QtSvg import QSvgRenderer
    from PySide2.QtGui import QPixmap, QPainter
    from PySide2.QtCore import Qt, QThread, Signal
except ImportError:
    from PySide6.QtWidgets import (
        QApplication, QWidget, QVBoxLayout, QHBoxLayout,
        QLabel, QLineEdit, QPushButton, QTextEdit,
        QMessageBox, QInputDialog, QDialog, QFileDialog,
        QListWidget, QListWidgetItem, QStyle,
    )
    from PySide6.QtSvg import QSvgRenderer
    from PySide6.QtGui import QPixmap, QPainter
    from PySide6.QtCore import Qt, QThread, Signal

from lib.backup_lib import (
    patch, rollback, list_backups,
    is_remote, parse_remote, check_patch_compatibility,
    find_overlapping_paths, backup_overlapping_files,
)
from lib.password_manager import PasswordManager


DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "conf", "config.json"
)


class _ListRemoteThread(QThread):
    """Background thread to list remote directory via paramiko."""
    finished = Signal(bool, str, list)

    def __init__(self, password: str, user_host: str, remote_dir: str, parent=None):
        super().__init__(parent)
        self.password = password
        self.user_host = user_host
        self.remote_dir = remote_dir

    def run(self):
        try:
            import paramiko
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            username, hostname = self.user_host.split("@", 1)
            client.connect(
                hostname=hostname,
                username=username,
                password=self.password,
                look_for_keys=False,
                allow_agent=False,
            )
            try:
                _stdin, stdout, stderr = client.exec_command(
                    f"ls -F1 {shlex.quote(self.remote_dir)}"
                )
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    err = stderr.read().decode("utf-8", errors="replace").strip()
                    self.finished.emit(False, err, [])
                    return
                lines = []
                for line in stdout.read().decode("utf-8", errors="replace").splitlines():
                    line = line.strip()
                    if line and line not in (".", ".."):
                        lines.append(line)
                self.finished.emit(True, "", lines)
            finally:
                client.close()
        except Exception as e:
            self.finished.emit(False, str(e), [])


class _PreCheckThread(QThread):
    """Background thread for compatibility check and overlap detection."""
    log_msg = Signal(str)
    finished = Signal(object, object, list)

    def __init__(self, source_dir: str, target_dir: str, source_password: str = "", target_password: str = "", parent=None):
        super().__init__(parent)
        self.source_dir = source_dir
        self.target_dir = target_dir
        self.source_password = source_password
        self.target_password = target_password

    def run(self):
        try:
            def thread_log(msg: str):
                self.log_msg.emit(msg)
            compat, details = check_patch_compatibility(
                self.source_dir, self.target_dir,
                output_password=self.source_password, target_password=self.target_password, logger=thread_log
            )
            overlaps = find_overlapping_paths(
                self.source_dir, self.target_dir,
                output_password=self.source_password, target_password=self.target_password, logger=thread_log
            )
            self.finished.emit(compat, details, overlaps)
        except Exception as e:
            self.log_msg.emit(f"Pre-check failed: {e}")
            self.finished.emit(None, {}, [])


class _WorkerThread(QThread):
    """Generic background worker that emits log lines and a finished signal."""
    log_msg = Signal(str)
    finished = Signal(bool, str)

    def __init__(self, func, parent=None):
        super().__init__(parent)
        self.func = func

    def _thread_log(self, msg: str):
        self.log_msg.emit(msg)

    def run(self):
        try:
            self.func(logger=self._thread_log)
            self.finished.emit(True, "")
        except Exception as e:
            self.finished.emit(False, str(e))


class RemoteDirDialog(QDialog):
    """Simple remote directory browser via paramiko."""

    def __init__(self, parent, initial_path: str, password_manager: PasswordManager):
        super().__init__(parent)
        self.password_manager = password_manager
        self.current_password = ""
        self.selected_path = initial_path
        self._thread = None
        self._build_ui(initial_path)
        self._refresh()

    def _build_ui(self, initial_path):
        self.setWindowTitle("Remote Directory Browser")
        self.resize(600, 500)
        layout = QVBoxLayout(self)

        path_layout = QHBoxLayout()
        path_layout.addWidget(QLabel("Path:"))
        self.edit_path = QLineEdit(initial_path)
        path_layout.addWidget(self.edit_path)
        self.btn_up = QPushButton("Up")
        self.btn_refresh = QPushButton("Refresh")
        self.btn_up.clicked.connect(self._on_up)
        self.btn_refresh.clicked.connect(self._refresh)
        path_layout.addWidget(self.btn_up)
        path_layout.addWidget(self.btn_refresh)
        layout.addLayout(path_layout)

        self.list_widget = QListWidget()
        self.list_widget.itemDoubleClicked.connect(self._on_double_click)
        layout.addWidget(self.list_widget)

        self.status_label = QLabel("Loading...")
        self.status_label.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.status_label)

        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        self.btn_select = QPushButton("Select")
        self.btn_cancel = QPushButton("Cancel")
        self.btn_select.clicked.connect(self.accept)
        self.btn_cancel.clicked.connect(self.reject)
        btn_layout.addWidget(self.btn_select)
        btn_layout.addWidget(self.btn_cancel)
        layout.addLayout(btn_layout)

    def _set_loading(self, loading: bool):
        self.status_label.setVisible(loading)
        self.btn_up.setEnabled(not loading)
        self.btn_refresh.setEnabled(not loading)
        self.list_widget.setEnabled(not loading)
        self.btn_select.setEnabled(not loading)

    def _on_up(self):
        try:
            user_host, remote_dir = parse_remote(self.edit_path.text().strip())
        except Exception:
            return
        remote_dir = remote_dir.rstrip("/")
        parent = os.path.dirname(remote_dir)
        if not parent or parent == remote_dir:
            parent = remote_dir
        self.edit_path.setText(f"{user_host}:{parent}")
        self._refresh()

    def _refresh(self):
        path = self.edit_path.text().strip()
        try:
            user_host, remote_dir = parse_remote(path)
        except Exception:
            self.list_widget.clear()
            return

        password = self.password_manager.get_password_with_retry(user_host, self)
        if not password:
            return
        self.current_password = password

        self._set_loading(True)
        self.list_widget.clear()
        self._thread = _ListRemoteThread(self.current_password, user_host, remote_dir, self)
        self._thread.finished.connect(self._on_refresh_finished)
        self._thread.start()

    def _on_refresh_finished(self, ok: bool, error: str, lines: list):
        self._set_loading(False)
        if not ok:
            QMessageBox.warning(self, "Error", f"Failed to list directory:\n{error}")
            return

        for line in lines:
            # ls -F appends / for directories, etc.
            name = line.rstrip("*=@|>")
            item = QListWidgetItem(name)
            if line.endswith("/"):
                item.setIcon(self.style().standardIcon(QStyle.SP_DirIcon))
            else:
                item.setIcon(self.style().standardIcon(QStyle.SP_FileIcon))
            item.setData(Qt.UserRole, line)
            self.list_widget.addItem(item)

    def _on_double_click(self, item):
        raw = item.data(Qt.UserRole)
        if not raw or not raw.endswith("/"):
            return
        name = raw.rstrip("/")
        try:
            user_host, remote_dir = parse_remote(self.edit_path.text().strip())
        except Exception:
            return
        new_dir = os.path.join(remote_dir, name).replace("\\", "/")
        self.edit_path.setText(f"{user_host}:{new_dir}")
        self._refresh()

    def accept(self):
        self.selected_path = self.edit_path.text().strip()
        super().accept()

    def reject(self):
        if self._thread and self._thread.isRunning():
            self._thread.terminate()
            self._thread.wait()
        super().reject()

    @staticmethod
    def get_selected_path(parent, initial_path: str, password_manager: PasswordManager) -> str:
        dialog = RemoteDirDialog(parent, initial_path or "", password_manager)
        if dialog.exec() == QDialog.Accepted:
            return dialog.selected_path
        return initial_path


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Auto Backup and Patch Tool")
        self.resize(700, 600)
        self._worker = None
        self.password_manager = PasswordManager()
        self._build_ui()
        self._load_config(DEFAULT_CONFIG_PATH)

    def _build_ui(self):
        layout = QVBoxLayout()
        layout.setSpacing(12)
        layout.setContentsMargins(20, 20, 20, 20)

        # Save / Load / Exit buttons
        btn_layout = QHBoxLayout()
        self.btn_save = QPushButton("Save Params")
        self.btn_load = QPushButton("Load Params")
        self.btn_exit = QPushButton("Exit")
        self.btn_save.clicked.connect(self._on_save_params)
        self.btn_load.clicked.connect(self._on_load_params)
        self.btn_exit.clicked.connect(self._on_exit)
        btn_layout.addWidget(self.btn_save)
        btn_layout.addWidget(self.btn_load)
        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_exit)
        layout.addLayout(btn_layout)

        # Input fields
        self.edit_backup = QLineEdit()
        self.edit_output = QLineEdit()
        self.edit_target = QLineEdit()

        layout.addWidget(QLabel("Backup Dir:"))
        backup_layout = QHBoxLayout()
        backup_layout.addWidget(self.edit_backup)
        self.btn_browse_backup = QPushButton("...")
        self.btn_browse_backup.setFixedWidth(40)
        self.btn_browse_backup.clicked.connect(self._on_browse_backup)
        backup_layout.addWidget(self.btn_browse_backup)
        layout.addLayout(backup_layout)

        layout.addWidget(QLabel("Output Dir:"))
        output_layout = QHBoxLayout()
        output_layout.addWidget(self.edit_output)
        self.btn_browse_output = QPushButton("...")
        self.btn_browse_output.setFixedWidth(40)
        self.btn_browse_output.clicked.connect(self._on_browse_output)
        output_layout.addWidget(self.btn_browse_output)
        layout.addLayout(output_layout)

        layout.addWidget(QLabel("Target Dir (local path or user@ip:/path for remote):"))
        target_layout = QHBoxLayout()
        target_layout.addWidget(self.edit_target)
        self.btn_browse_target = QPushButton("...")
        self.btn_browse_target.setFixedWidth(40)
        self.btn_browse_target.clicked.connect(self._on_browse_target)
        target_layout.addWidget(self.btn_browse_target)
        layout.addLayout(target_layout)

        # Action buttons
        action_layout = QHBoxLayout()
        self.btn_patch = QPushButton("Patch")
        self.btn_rollback = QPushButton("Rollback")
        self.btn_patch.clicked.connect(self._on_patch)
        self.btn_rollback.clicked.connect(self._on_rollback)
        action_layout.addWidget(self.btn_patch)
        action_layout.addWidget(self.btn_rollback)
        layout.addLayout(action_layout)

        # Log window
        layout.addWidget(QLabel("Log Window:"))
        self.log_edit = QTextEdit()
        self.log_edit.setReadOnly(True)
        layout.addWidget(self.log_edit, stretch=1)

        self.setLayout(layout)

    def _log(self, msg: str):
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{timestamp}  {msg}"
        self.log_edit.append(line)

        # Append to log file under logs/
        log_dir = os.path.join(os.path.dirname(__file__), "..", "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, datetime.now().strftime("%Y-%m-%d") + ".log")
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    def _svg_pixmap(self, svg_path: str, size: int = 48) -> QPixmap:
        """Render an SVG file to a QPixmap."""
        renderer = QSvgRenderer(svg_path)
        pixmap = QPixmap(size, size)
        pixmap.fill(Qt.transparent)
        painter = QPainter(pixmap)
        renderer.render(painter)
        painter.end()
        return pixmap

    def _svg_icon_path(self, name: str) -> str:
        """Return absolute path to an SVG icon."""
        return os.path.join(os.path.dirname(__file__), "icons", f"{name}.svg")

    def _custom_msg_box(self, icon_name: str, title: str, text: str,
                        buttons=QMessageBox.Ok,
                        default_button=QMessageBox.NoButton) -> int:
        """Show a QMessageBox with a custom SVG icon."""
        msg = QMessageBox(self)
        msg.setWindowTitle(title)
        msg.setTextFormat(Qt.RichText)
        msg.setText(text)
        msg.setStandardButtons(buttons)
        msg.setDefaultButton(default_button)
        icon_path = self._svg_icon_path(icon_name)
        if os.path.exists(icon_path):
            msg.setIconPixmap(self._svg_pixmap(icon_path, size=96))
        return msg.exec() if hasattr(msg, "exec") else msg.exec()

    def _show_markdown_dialog(self, title: str, markdown_text: str) -> bool:
        """Show a dialog with markdown text and Yes/No buttons."""
        dialog = QDialog(self)
        dialog.setWindowTitle(title)
        dialog.setMinimumSize(700, 500)
        dialog.resize(800, 550)

        layout = QVBoxLayout(dialog)
        text_edit = QTextEdit()
        text_edit.setReadOnly(True)

        if hasattr(text_edit, "setMarkdown"):
            text_edit.setMarkdown(markdown_text)
        else:
            # Fallback: render markdown table as simple HTML table
            html = self._markdown_table_to_html(markdown_text)
            text_edit.setHtml(html)

        layout.addWidget(text_edit)

        btn_box = QHBoxLayout()
        btn_yes = QPushButton("Yes")
        btn_no = QPushButton("No")
        btn_box.addStretch()
        btn_box.addWidget(btn_no)
        btn_box.addWidget(btn_yes)
        layout.addLayout(btn_box)

        result = False

        def on_yes():
            nonlocal result
            result = True
            dialog.accept()

        def on_no():
            dialog.reject()

        btn_yes.clicked.connect(on_yes)
        btn_no.clicked.connect(on_no)

        dialog.exec() if hasattr(dialog, "exec") else dialog.exec()
        return result

    def _markdown_table_to_html(self, md: str) -> str:
        """Simple converter for markdown tables to HTML with centered table."""
        lines = md.strip().splitlines()
        in_table = False
        html_lines = [
            "<html>"
            "<body style='font-family: sans-serif; font-size: 14px; text-align: center;'>"
        ]
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("|") and stripped.endswith("|"):
                if not in_table:
                    html_lines.append(
                        "<table border='1' cellpadding='8' cellspacing='0' "
                        "style='border-collapse: collapse; margin: 0 auto;'>"
                    )
                    in_table = True
                cells = [c.strip() for c in stripped[1:-1].split("|")]
                # Skip separator lines
                if all(c.replace("-", "") == "" for c in cells):
                    continue
                html_lines.append("<tr>")
                for cell in cells:
                    html_lines.append(
                        f"<td style='border:1px solid #ccc; padding:6px 14px; text-align:left;'>"
                        f"{cell}</td>"
                    )
                html_lines.append("</tr>")
            else:
                if in_table:
                    html_lines.append("</table>")
                    in_table = False
                html_lines.append(f"<p style='margin: 10px 0;'>{stripped}</p>")
        if in_table:
            html_lines.append("</table>")
        html_lines.append("</body></html>")
        return "\n".join(html_lines)

    def _expand_path(self, path: str) -> str:
        if not path or is_remote(path):
            return path
        return os.path.expanduser(path)

    @staticmethod
    def _fmt_paths_html(title: str, items: list) -> str:
        """Format a list of label/path pairs as HTML for QMessageBox."""
        lines = [f"<h3 style='margin:4px 0;'>{title}</h3>", "<table cellspacing='6' cellpadding='2'>"]
        for label, path in items:
            lines.append(
                f"<tr><td align='right' valign='top'><b>{label}:</b></td>"
                f"<td valign='top'><code style='font-size:13px;'>{path}</code></td></tr>"
            )
        lines.append("</table>")
        return "\n".join(lines)

    def _get_password_for_path(self, path: str) -> Optional[str]:
        """Prompt for SSH password if path is remote. Returns empty string for local, None if user cancels."""
        if not is_remote(path):
            return ""
        user_host, _ = parse_remote(path)
        pwd = self.password_manager.get_password_with_retry(user_host, self)
        return pwd if pwd else None

    def _ensure_local_dir(self, dir_path: str, name: str) -> bool:
        """If local dir does not exist, prompt yes/no to create it."""
        real_path = self._expand_path(dir_path)
        if os.path.isdir(real_path):
            return True
        reply = self._custom_msg_box(
            "question", "Directory Does Not Exist",
            f"<b>{name} directory does not exist</b><br><br>"
            f"<code style='font-size:13px;'>{dir_path}</code><br><br>"
            f"Create?",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        if reply != QMessageBox.Yes:
            self._log(f"User cancelled creating {name} directory")
            return False
        try:
            os.makedirs(real_path, exist_ok=True)
            self._log(f"Created {name} directory: {real_path}")
            return True
        except Exception as e:
            self._log(f"Failed to create {name} directory: {e}")
            self._custom_msg_box("question", "Create Failed", str(e))
            return False

    def _check_output_exists(self, output_dir: str) -> bool:
        """Output is a source dir; if it does not exist, show warning."""
        if is_remote(output_dir):
            try:
                user_host, remote_dir = parse_remote(output_dir)
                password = self.password_manager.get_password_with_retry(user_host, self)
                if not password:
                    return False
                import paramiko
                client = paramiko.SSHClient()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                username, hostname = user_host.split("@", 1)
                client.connect(hostname=hostname, username=username, password=password, look_for_keys=False, allow_agent=False)
                sftp = client.open_sftp()
                try:
                    sftp.stat(remote_dir)
                finally:
                    sftp.close()
                client.close()
                return True
            except Exception as e:
                self._custom_msg_box(
                    "question", "Path Error",
                    f"<b>Remote output directory does not exist or is unreachable</b><br><br>"
                    f"<code style='font-size:13px;'>{output_dir}</code><br><br>{e}"
                )
                self._log(f"Remote output directory does not exist: {output_dir} ({e})")
                return False
        real_path = self._expand_path(output_dir)
        if os.path.isdir(real_path):
            return True
        self._custom_msg_box(
            "question", "Path Error",
            f"<b>Output directory does not exist</b><br><br>"
            f"<code style='font-size:13px;'>{output_dir}</code>"
        )
        self._log(f"Output directory does not exist: {real_path}")
        return False

    def _get_inputs(self):
        return {
            "backup": self.edit_backup.text().strip(),
            "output": self.edit_output.text().strip(),
            "target": self.edit_target.text().strip(),
        }

    def _set_inputs(self, data: dict):
        self.edit_backup.setText(data.get("backup", ""))
        self.edit_output.setText(data.get("output", ""))
        self.edit_target.setText(data.get("target", ""))

    def _load_config(self, path: str):
        self.password_manager.set_file_path(path)
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._set_inputs(data)
                self._log(f"Config loaded: {path}")
            except Exception as e:
                self._log(f"Failed to load config: {e}")
        else:
            self._log(f"Default config file does not exist: {path}")

    def _on_browse_backup(self):
        current = self.edit_backup.text().strip()
        if not current or not is_remote(current):
            start_dir = current or os.path.expanduser("~")
            path = QFileDialog.getExistingDirectory(self, "Select Backup Directory", start_dir)
            if path:
                self.edit_backup.setText(path)
        else:
            new_path = RemoteDirDialog.get_selected_path(self, current, self.password_manager)
            if new_path:
                self.edit_backup.setText(new_path)

    def _on_browse_output(self):
        current = self.edit_output.text().strip()
        if not current or not is_remote(current):
            start_dir = current or os.path.expanduser("~")
            path = QFileDialog.getExistingDirectory(self, "Select Output Directory", start_dir)
            if path:
                self.edit_output.setText(path)
        else:
            new_path = RemoteDirDialog.get_selected_path(self, current, self.password_manager)
            if new_path:
                self.edit_output.setText(new_path)

    def _on_browse_target(self):
        current = self.edit_target.text().strip()
        if not current or not is_remote(current):
            start_dir = current or os.path.expanduser("~")
            path = QFileDialog.getExistingDirectory(self, "Select Target Directory", start_dir)
            if path:
                self.edit_target.setText(path)
        else:
            new_path = RemoteDirDialog.get_selected_path(self, current, self.password_manager)
            if new_path:
                self.edit_target.setText(new_path)

    def _on_exit(self):
        self._log("Exiting program")
        QApplication.instance().quit()

    def _on_save_params(self):
        path, _ = QFileDialog.getSaveFileName(
            self, "Save Params", "", "JSON Files (*.json)"
        )
        if not path:
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            self.password_manager.set_file_path(path)
            data = dict(self._get_inputs())
            data["ssh_passwords"] = dict(self.password_manager._cache)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self._log(f"Params saved: {path}")
        except Exception as e:
            self._log(f"Failed to save params: {e}")

    def _on_load_params(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Load Params", "", "JSON Files (*.json)"
        )
        if not path:
            return
        try:
            self.password_manager.set_file_path(path)
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._set_inputs(data)
            self._log(f"Params loaded: {path}")
        except Exception as e:
            self._log(f"Failed to load params: {e}")

    def _set_busy(self, busy: bool):
        widgets = [
            self.btn_patch, self.btn_rollback,
            self.btn_save, self.btn_load,
            self.btn_browse_backup, self.btn_browse_output, self.btn_browse_target,
        ]
        for w in widgets:
            w.setEnabled(not busy)
        if busy:
            QApplication.setOverrideCursor(Qt.WaitCursor)
        else:
            QApplication.restoreOverrideCursor()

    def _start_worker(self, func, success_title: str, success_msg: str):
        self._set_busy(True)
        self._worker = _WorkerThread(func, self)
        self._worker.log_msg.connect(self._log)

        def on_finished(success, error):
            self._set_busy(False)
            if success:
                self._custom_msg_box("question", success_title, success_msg)
            else:
                self._log(f"{success_title} failed: {error}")
                self._custom_msg_box("question", f"{success_title} Failed", error)
            self._worker = None

        self._worker.finished.connect(on_finished)
        self._worker.start()

    def _on_patch(self):
        data = self._get_inputs()
        output_dir = data["output"]
        target_dir = data["target"]
        backup_dir = data["backup"]

        if not output_dir:
            self._custom_msg_box("question", "Input Error", "Output directory cannot be empty")
            return
        if not target_dir:
            self._custom_msg_box("question", "Input Error", "Target directory cannot be empty")
            return
        if not self._check_output_exists(output_dir):
            return

        output_pwd = self._get_password_for_path(output_dir)
        if output_pwd is None:
            return
        target_pwd = self._get_password_for_path(target_dir)
        if target_pwd is None:
            return
        backup_pwd = self._get_password_for_path(backup_dir)
        if backup_pwd is None:
            return

        if is_remote(target_dir) and not backup_dir:
            self._custom_msg_box("question", "Input Error", "Remote target requires Backup directory")
            return
        if not is_remote(target_dir):
            if not self._ensure_local_dir(target_dir, "Target"):
                return

        self._log("Starting compatibility check...")
        self._set_busy(True)
        pre_thread = _PreCheckThread(output_dir, target_dir, output_pwd, target_pwd, self)
        pre_thread.log_msg.connect(self._log)

        def on_precheck_finished(compat, details, overlaps):
            self._set_busy(False)
            self._continue_patch(output_dir, target_dir, output_pwd, target_pwd, backup_pwd, backup_dir, compat, details, overlaps)

        pre_thread.finished.connect(on_precheck_finished)
        pre_thread.start()

    def _continue_patch(self, output_dir, target_dir, output_pwd, target_pwd, backup_pwd, backup_dir, compatibility, details, overlapping_files):
        if compatibility is None:
            self._custom_msg_box("question", "Pre-check Failed", "Failed to perform compatibility check.")
            return
        if compatibility == "none":
            self._custom_msg_box(
                "question", "Patch Not Allowed",
                "Output and Target directories are completely different,<br>"
                "no common files or directories. Patching is forbidden to avoid overwriting the wrong directory."
            )
            self._log("Compatibility check failed: completely different, patching forbidden")
            return
        elif compatibility == "partial":
            only_output = details.get("only_output", [])
            only_target = details.get("only_target", [])
            mismatch_info = details.get("mismatch_info", [])

            lines = []
            lines.append("Output and Target directories are partially different. Comparison (showing first 10 items):")
            lines.append("")

            max_len = max(len(only_output), len(only_target), len(mismatch_info), 1)
            lines.append("| Output Side | Target Side |")
            lines.append("|-------------|-------------|")
            for i in range(min(max_len, 10)):
                left = only_output[i] if i < len(only_output) else ""
                right = only_target[i] if i < len(only_target) else ""
                lines.append(f"| {left} | {right} |")
            for m in mismatch_info[:10]:
                lines.append(f"| {m['name']} ({m['output_type']}) | {m['name']} ({m['target_type']}) |")
            if max_len > 10:
                lines.append(f"| ... {max_len - 10} more items not shown | |")
            lines.append("")
            lines.append("Continue patching?")
            md_text = "\n".join(lines)

            if not self._show_markdown_dialog("Partial Structure Mismatch", md_text):
                self._log("User cancelled patching")
                return

        if overlapping_files:
            self._log(f"Detected {len(overlapping_files)} overlapping file(s)/dir(s) that will be overwritten")

        md_lines = []
        md_lines.append(f"**Output:** `{output_dir}`")
        md_lines.append("")
        md_lines.append(f"**Target:** `{target_dir}`")
        md_lines.append("")
        if overlapping_files:
            target_full = self._expand_path(target_dir)
            md_lines.append(f"The following **{len(overlapping_files)}** file(s)/dir(s) will be overwritten:")
            md_lines.append("")
            md_lines.append("| # | Path |")
            md_lines.append("|---|------|")
            for idx, f in enumerate(overlapping_files[:20], start=1):
                full_path = os.path.join(target_full, f)
                md_lines.append(f"| {idx} | `{full_path}` |")
            if len(overlapping_files) > 20:
                md_lines.append(f"| ... | and {len(overlapping_files) - 20} more |")
            md_lines.append("")
        md_lines.append("Continue patching?")
        md_text = "\n".join(md_lines)

        if not self._show_markdown_dialog("Confirm Patch", md_text):
            self._log("User cancelled patching")
            return

        self._log("Starting patch...")

        def patch_worker(logger):
            if backup_dir:
                backup_path = backup_overlapping_files(
                    output_dir, target_dir, backup_dir,
                    output_password=output_pwd, target_password=target_pwd, backup_password=backup_pwd, logger=logger
                )
                if backup_path:
                    logger(f"Overwrite backup completed: {backup_path}")
            else:
                logger("Warning: Backup directory not set, skipping overwrite backup")
            patch(output_dir, target_dir, output_password=output_pwd, target_password=target_pwd, logger=logger)
            logger("Patch completed")

        self._start_worker(patch_worker, "Patch Successful", "Patch completed")

    def _on_rollback(self):
        data = self._get_inputs()
        backup_dir = data["backup"]
        target_dir = data["target"]

        if not backup_dir:
            self._custom_msg_box("question", "Input Error", "Backup directory cannot be empty")
            return
        if not target_dir:
            self._custom_msg_box("question", "Input Error", "Target directory cannot be empty")
            return

        backup_pwd = self._get_password_for_path(backup_dir)
        if backup_pwd is None:
            return
        target_pwd = self._get_password_for_path(target_dir)
        if target_pwd is None:
            return

        if is_remote(target_dir) and not backup_dir:
            self._custom_msg_box("question", "Input Error", "Remote target requires Backup directory")
            return
        if not is_remote(target_dir):
            if not self._ensure_local_dir(target_dir, "Target"):
                return

        try:
            backups = list_backups(backup_dir, backup_password=backup_pwd)
        except Exception as e:
            self._custom_msg_box("question", "Backup Error", str(e))
            return
        if not backups:
            self._custom_msg_box("question", "No Backups", "No timestamped backups in backup directory")
            return

        names = [os.path.basename(b) for b in backups]
        name, ok = QInputDialog.getItem(
            self, "Select Backup", "Please select a backup to rollback:", names, 0, False
        )
        if not ok or not name:
            self._log("User cancelled rollback")
            return

        if is_remote(backup_dir):
            user_host, remote_dir = parse_remote(backup_dir)
            selected_dir = f"{user_host}:{remote_dir}/{name}"
        else:
            selected_dir = os.path.join(backup_dir, name)

        self._log("Starting compatibility check...")
        self._set_busy(True)
        pre_thread = _PreCheckThread(selected_dir, target_dir, backup_pwd, target_pwd, self)
        pre_thread.log_msg.connect(self._log)

        def on_precheck_finished(compat, details, overlaps):
            self._set_busy(False)
            self._continue_rollback(selected_dir, target_dir, backup_pwd, target_pwd, compat, details, overlaps)

        pre_thread.finished.connect(on_precheck_finished)
        pre_thread.start()

    def _continue_rollback(self, selected_dir, target_dir, backup_pwd, target_pwd, compatibility, details, overlapping_files):
        if compatibility is None:
            self._custom_msg_box("question", "Pre-check Failed", "Failed to perform compatibility check.")
            return
        if compatibility == "none":
            self._custom_msg_box(
                "question", "Rollback Not Allowed",
                "Backup and Target directories are completely different,<br>"
                "no common files or directories. Rollback is forbidden."
            )
            self._log("Compatibility check failed: completely different, rollback forbidden")
            return
        elif compatibility == "partial":
            only_output = details.get("only_output", [])
            only_target = details.get("only_target", [])
            mismatch_info = details.get("mismatch_info", [])

            lines = []
            lines.append("Backup and Target directories are partially different. Comparison (showing first 10 items):")
            lines.append("")

            max_len = max(len(only_output), len(only_target), len(mismatch_info), 1)
            lines.append("| Backup Side | Target Side |")
            lines.append("|-------------|-------------|")
            for i in range(min(max_len, 10)):
                left = only_output[i] if i < len(only_output) else ""
                right = only_target[i] if i < len(only_target) else ""
                lines.append(f"| {left} | {right} |")
            for m in mismatch_info[:10]:
                lines.append(f"| {m['name']} ({m['output_type']}) | {m['name']} ({m['target_type']}) |")
            if max_len > 10:
                lines.append(f"| ... {max_len - 10} more items not shown | |")
            lines.append("")
            lines.append("Continue rollback?")
            md_text = "\n".join(lines)

            if not self._show_markdown_dialog("Partial Structure Mismatch", md_text):
                self._log("User cancelled rollback")
                return

        if overlapping_files:
            self._log(f"Detected {len(overlapping_files)} overlapping file(s)/dir(s) that will be overwritten")

        md_lines = []
        md_lines.append(f"**Backup:** `{selected_dir}`")
        md_lines.append("")
        md_lines.append(f"**Target:** `{target_dir}`")
        md_lines.append("")
        if overlapping_files:
            target_full = self._expand_path(target_dir)
            md_lines.append(f"The following **{len(overlapping_files)}** file(s)/dir(s) will be overwritten:")
            md_lines.append("")
            md_lines.append("| # | Path |")
            md_lines.append("|---|------|")
            for idx, f in enumerate(overlapping_files[:20], start=1):
                full_path = os.path.join(target_full, f)
                md_lines.append(f"| {idx} | `{full_path}` |")
            if len(overlapping_files) > 20:
                md_lines.append(f"| ... | and {len(overlapping_files) - 20} more |")
            md_lines.append("")
        md_lines.append("Continue rollback?")
        md_text = "\n".join(md_lines)

        if not self._show_markdown_dialog("Confirm Rollback", md_text):
            self._log("User cancelled rollback")
            return

        self._log("Starting rollback...")

        def rollback_worker(logger):
            rollback(selected_dir, target_dir, backup_password=backup_pwd, target_password=target_pwd, logger=logger)
            logger("Rollback completed")

        self._start_worker(rollback_worker, "Rollback Successful", "Rollback completed")


def main():
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec() if hasattr(app, "exec") else app.exec_())


if __name__ == "__main__":
    main()

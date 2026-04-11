#!/usr/bin/env python3
"""PySide2/6 GUI for backup and patch tool."""

import json
import os
import sys

# Add project root to sys.path so lib can be imported
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# Try PySide2 first, fallback to PySide6
try:
    from PySide2.QtWidgets import (
        QApplication, QWidget, QVBoxLayout, QHBoxLayout,
        QLabel, QLineEdit, QPushButton, QTextEdit,
        QMessageBox, QInputDialog, QDialog, QFileDialog
    )
    from PySide2.QtSvg import QSvgRenderer
    from PySide2.QtGui import QPixmap, QPainter
    from PySide2.QtCore import Qt
except ImportError:
    from PySide6.QtWidgets import (
        QApplication, QWidget, QVBoxLayout, QHBoxLayout,
        QLabel, QLineEdit, QPushButton, QTextEdit,
        QMessageBox, QInputDialog, QDialog, QFileDialog
    )
    from PySide6.QtSvg import QSvgRenderer
    from PySide6.QtGui import QPixmap, QPainter
    from PySide6.QtCore import Qt

from lib.backup_lib import (
    patch, rollback, list_backups,
    is_remote, check_patch_compatibility,
    find_overlapping_paths, backup_overlapping_files,
)


DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "conf", "config.json"
)


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Auto Backup and Patch Tool")
        self.resize(700, 600)
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
        layout.addWidget(self.edit_backup)

        layout.addWidget(QLabel("Output Dir:"))
        layout.addWidget(self.edit_output)

        layout.addWidget(QLabel("Target Dir (local path or user@ip:/path for remote):"))
        layout.addWidget(self.edit_target)

        layout.addWidget(QLabel("SSH Password (only for remote):"))
        pwd_layout = QHBoxLayout()
        self.edit_password = QLineEdit()
        self.edit_password.setEchoMode(QLineEdit.Password)
        self.btn_toggle_pwd = QPushButton("Show")
        self.btn_toggle_pwd.setFixedWidth(50)
        self.btn_toggle_pwd.clicked.connect(self._on_toggle_password)
        pwd_layout.addWidget(self.edit_password)
        pwd_layout.addWidget(self.btn_toggle_pwd)
        layout.addLayout(pwd_layout)

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

    def _on_toggle_password(self):
        if self.edit_password.echoMode() == QLineEdit.Password:
            self.edit_password.setEchoMode(QLineEdit.Normal)
            self.btn_toggle_pwd.setText("Hide")
        else:
            self.edit_password.setEchoMode(QLineEdit.Password)
            self.btn_toggle_pwd.setText("Show")

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
            "ssh_password": self.edit_password.text().strip(),
        }

    def _set_inputs(self, data: dict):
        self.edit_backup.setText(data.get("backup", ""))
        self.edit_output.setText(data.get("output", ""))
        self.edit_target.setText(data.get("target", ""))
        self.edit_password.setText(data.get("ssh_password", ""))

    def _load_config(self, path: str):
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
            with open(path, "w", encoding="utf-8") as f:
                json.dump(self._get_inputs(), f, ensure_ascii=False, indent=2)
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
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._set_inputs(data)
            self._log(f"Params loaded: {path}")
        except Exception as e:
            self._log(f"Failed to load params: {e}")

    def _on_patch(self):
        data = self._get_inputs()
        output_dir = data["output"]
        target_dir = data["target"]
        password = data["ssh_password"]
        backup_dir = data["backup"]

        if not output_dir:
            self._custom_msg_box("question", "Input Error", "Output directory cannot be empty")
            return
        if not target_dir:
            self._custom_msg_box("question", "Input Error", "Target directory cannot be empty")
            return
        if not self._check_output_exists(output_dir):
            return
        if is_remote(target_dir) and not password:
            self._custom_msg_box("question", "Input Error", "Remote target requires SSH password")
            return
        if is_remote(target_dir) and not backup_dir:
            self._custom_msg_box("question", "Input Error", "Remote target requires Backup directory")
            return
        if not is_remote(target_dir):
            if not self._ensure_local_dir(target_dir, "Target"):
                return

        self._log("Starting compatibility check...")
        compatibility, details = check_patch_compatibility(output_dir, target_dir, password=password, logger=self._log)
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

        # Detect overlapping files that will be overwritten
        overlapping_files = find_overlapping_paths(output_dir, target_dir, password=password, logger=self._log)
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
        try:
            if backup_dir:
                backup_path = backup_overlapping_files(
                    output_dir, target_dir, backup_dir,
                    password=password, logger=self._log
                )
                if backup_path:
                    self._log(f"Overwrite backup completed: {backup_path}")
            else:
                self._log("Warning: Backup directory not set, skipping overwrite backup")

            patch(output_dir, target_dir, password=password, logger=self._log)
            self._log("Patch completed")
            self._custom_msg_box("question", "Patch Successful", "Patch completed")
        except Exception as e:
            self._log(f"Patch failed: {e}")
            self._custom_msg_box("question", "Patch Failed", str(e))

    def _on_rollback(self):
        data = self._get_inputs()
        backup_dir = data["backup"]
        target_dir = data["target"]
        password = data["ssh_password"]

        if not backup_dir:
            self._custom_msg_box("question", "Input Error", "Backup directory cannot be empty")
            return
        if not target_dir:
            self._custom_msg_box("question", "Input Error", "Target directory cannot be empty")
            return
        if is_remote(target_dir) and not password:
            self._custom_msg_box("question", "Input Error", "Remote target requires SSH password")
            return
        if is_remote(target_dir) and not backup_dir:
            self._custom_msg_box("question", "Input Error", "Remote target requires Backup directory")
            return
        if not is_remote(target_dir):
            if not self._ensure_local_dir(target_dir, "Target"):
                return

        backups = list_backups(backup_dir)
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

        selected_dir = os.path.join(backup_dir, name)

        self._log("Starting compatibility check...")
        compatibility, details = check_patch_compatibility(selected_dir, target_dir, password=password, logger=self._log)
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

        # Detect overlapping files that will be overwritten during rollback
        overlapping_files = find_overlapping_paths(selected_dir, target_dir, password=password, logger=self._log)
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
        try:
            rollback(selected_dir, target_dir, password=password, logger=self._log)
            self._log("Rollback completed")
            self._custom_msg_box("question", "Rollback Successful", "Rollback completed")
        except Exception as e:
            self._log(f"Rollback failed: {e}")
            self._custom_msg_box("question", "Rollback Failed", str(e))


def main():
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec() if hasattr(app, "exec") else app.exec())


if __name__ == "__main__":
    main()

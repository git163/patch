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
        QMessageBox, QInputDialog, QDialog
    )
except ImportError:
    from PySide6.QtWidgets import (
        QApplication, QWidget, QVBoxLayout, QHBoxLayout,
        QLabel, QLineEdit, QPushButton, QTextEdit,
        QMessageBox, QInputDialog, QDialog
    )

from lib.backup_lib import (
    backup, patch, rollback, list_backups,
    is_remote, check_patch_compatibility
)


DEFAULT_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "conf", "config.json"
)


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("自动备份与打补丁工具")
        self.resize(700, 600)
        self._build_ui()
        self._load_config(DEFAULT_CONFIG_PATH)

    def _build_ui(self):
        layout = QVBoxLayout()
        layout.setSpacing(12)
        layout.setContentsMargins(20, 20, 20, 20)

        # Save / Load buttons
        btn_layout = QHBoxLayout()
        self.btn_save = QPushButton("保存参数")
        self.btn_load = QPushButton("加载参数")
        self.btn_save.clicked.connect(self._on_save_params)
        self.btn_load.clicked.connect(self._on_load_params)
        btn_layout.addWidget(self.btn_save)
        btn_layout.addWidget(self.btn_load)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        # Input fields
        self.edit_backup = QLineEdit()
        self.edit_output = QLineEdit()
        self.edit_target = QLineEdit()

        layout.addWidget(QLabel("Backup 目录:"))
        layout.addWidget(self.edit_backup)

        layout.addWidget(QLabel("Output 目录:"))
        layout.addWidget(self.edit_output)

        layout.addWidget(QLabel("Target 目录 (支持本地路径 或 用户名@ip:/path 远程路径):"))
        layout.addWidget(self.edit_target)

        layout.addWidget(QLabel("SSH 密码 (仅远程时需要):"))
        pwd_layout = QHBoxLayout()
        self.edit_password = QLineEdit()
        self.edit_password.setEchoMode(QLineEdit.Password)
        self.btn_toggle_pwd = QPushButton("显示")
        self.btn_toggle_pwd.setFixedWidth(50)
        self.btn_toggle_pwd.clicked.connect(self._on_toggle_password)
        pwd_layout.addWidget(self.edit_password)
        pwd_layout.addWidget(self.btn_toggle_pwd)
        layout.addLayout(pwd_layout)

        # Action buttons
        action_layout = QHBoxLayout()
        self.btn_backup = QPushButton("备 份")
        self.btn_patch = QPushButton("打 补 丁")
        self.btn_rollback = QPushButton("回 退 补 丁")
        self.btn_backup.clicked.connect(self._on_backup)
        self.btn_patch.clicked.connect(self._on_patch)
        self.btn_rollback.clicked.connect(self._on_rollback)
        action_layout.addWidget(self.btn_backup)
        action_layout.addWidget(self.btn_patch)
        action_layout.addWidget(self.btn_rollback)
        layout.addLayout(action_layout)

        # Log window
        layout.addWidget(QLabel("日志窗口:"))
        self.log_edit = QTextEdit()
        self.log_edit.setReadOnly(True)
        layout.addWidget(self.log_edit, stretch=1)

        self.setLayout(layout)

    def _log(self, msg: str):
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_edit.append(f"{timestamp}  {msg}")

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
        btn_yes = QPushButton("是")
        btn_no = QPushButton("否")
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

        dialog.exec() if hasattr(dialog, "exec") else dialog.exec_()
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
            self.btn_toggle_pwd.setText("隐藏")
        else:
            self.edit_password.setEchoMode(QLineEdit.Password)
            self.btn_toggle_pwd.setText("显示")

    def _expand_path(self, path: str) -> str:
        if not path or is_remote(path):
            return path
        return os.path.expanduser(path)

    def _ensure_local_dir(self, dir_path: str, name: str) -> bool:
        """If local dir does not exist, prompt yes/no to create it."""
        real_path = self._expand_path(dir_path)
        if os.path.isdir(real_path):
            return True
        reply = QMessageBox.question(
            self, "目录不存在",
            f"{name} 目录不存在:\n{dir_path}\n\n是否创建?",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply != QMessageBox.Yes:
            self._log(f"用户取消创建 {name} 目录")
            return False
        try:
            os.makedirs(real_path, exist_ok=True)
            self._log(f"已创建 {name} 目录: {real_path}")
            return True
        except Exception as e:
            self._log(f"创建 {name} 目录失败: {e}")
            QMessageBox.critical(self, "创建失败", str(e))
            return False

    def _check_output_exists(self, output_dir: str) -> bool:
        """Output is a source dir; if it does not exist, show warning."""
        real_path = self._expand_path(output_dir)
        if os.path.isdir(real_path):
            return True
        QMessageBox.warning(
            self, "路径错误",
            f"Output 目录不存在:\n{output_dir}"
        )
        self._log(f"Output 目录不存在: {real_path}")
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
                self._log(f"已加载配置: {path}")
            except Exception as e:
                self._log(f"加载配置失败: {e}")
        else:
            self._log(f"默认配置文件不存在: {path}")

    def _on_save_params(self):
        path = DEFAULT_CONFIG_PATH
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(self._get_inputs(), f, ensure_ascii=False, indent=2)
            self._log(f"参数已保存: {path}")
        except Exception as e:
            self._log(f"保存参数失败: {e}")

    def _on_load_params(self):
        path = DEFAULT_CONFIG_PATH
        if not os.path.exists(path):
            self._log(f"配置文件不存在: {path}")
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self._set_inputs(data)
            self._log(f"参数已加载: {path}")
        except Exception as e:
            self._log(f"加载参数失败: {e}")

    def _on_backup(self):
        data = self._get_inputs()
        backup_dir = data["backup"]
        target_dir = data["target"]

        if not backup_dir:
            QMessageBox.warning(self, "输入错误", "Backup 目录不能为空")
            return
        if not target_dir:
            QMessageBox.warning(self, "输入错误", "Target 目录不能为空")
            return
        if is_remote(target_dir):
            QMessageBox.warning(self, "不支持", "备份操作仅支持本地 Target 目录")
            return
        if not self._ensure_local_dir(target_dir, "Target"):
            return
        if not self._ensure_local_dir(backup_dir, "Backup"):
            return

        reply = QMessageBox.question(
            self, "确认备份",
            f"即将把 Target 备份到 Backup 目录:\n"
            f"Target: {target_dir}\n"
            f"Backup: {backup_dir}",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply != QMessageBox.Yes:
            self._log("用户取消备份")
            return

        self._log("开始备份...")
        try:
            dest = backup(target_dir, backup_dir, logger=self._log)
            self._log(f"备份完成: {dest}")
            QMessageBox.information(self, "备份成功", f"备份完成:\n{dest}")
        except Exception as e:
            self._log(f"备份失败: {e}")
            QMessageBox.critical(self, "备份失败", str(e))

    def _on_patch(self):
        data = self._get_inputs()
        output_dir = data["output"]
        target_dir = data["target"]
        password = data["ssh_password"]

        if not output_dir:
            QMessageBox.warning(self, "输入错误", "Output 目录不能为空")
            return
        if not target_dir:
            QMessageBox.warning(self, "输入错误", "Target 目录不能为空")
            return
        if not self._check_output_exists(output_dir):
            return
        if is_remote(target_dir) and not password:
            QMessageBox.warning(self, "输入错误", "远程 Target 需要 SSH 密码")
            return
        if not is_remote(target_dir):
            if not self._ensure_local_dir(target_dir, "Target"):
                return

        self._log("开始校验目录兼容性...")
        compatibility, details = check_patch_compatibility(output_dir, target_dir, logger=self._log)
        if compatibility == "none":
            QMessageBox.warning(
                self, "不允许打补丁",
                "Output 与 Target 目录完全不一致，\n"
                "没有任何共同的文件或目录，禁止打补丁以避免覆盖错误目录。"
            )
            self._log("兼容性检查不通过: 完全不一致，禁止打补丁")
            return
        elif compatibility == "partial":
            only_output = details.get("only_output", [])
            only_target = details.get("only_target", [])
            mismatch_info = details.get("mismatch_info", [])

            lines = []
            lines.append("Output 与 Target 目录部分不一致，以下为对比情况（最多显示前 10 项）：")
            lines.append("")

            max_len = max(len(only_output), len(only_target), len(mismatch_info), 1)
            lines.append("| Output 侧 | Target 侧 |")
            lines.append("|-----------|-----------|")
            for i in range(min(max_len, 10)):
                left = only_output[i] if i < len(only_output) else ""
                right = only_target[i] if i < len(only_target) else ""
                lines.append(f"| {left} | {right} |")
            for m in mismatch_info[:10]:
                lines.append(f"| {m['name']} ({m['output_type']}) | {m['name']} ({m['target_type']}) |")
            if max_len > 10:
                lines.append(f"| ... 还有 {max_len - 10} 项未显示 | |")
            lines.append("")
            lines.append("是否继续打补丁？")
            md_text = "\n".join(lines)

            if not self._show_markdown_dialog("结构部分不匹配", md_text):
                self._log("用户取消打补丁")
                return

        reply = QMessageBox.question(
            self, "确认打补丁",
            f"即将把 Output 补丁到 Target:\n"
            f"Output: {output_dir}\n"
            f"Target: {target_dir}",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply != QMessageBox.Yes:
            self._log("用户取消打补丁")
            return

        self._log("开始打补丁...")
        try:
            patch(output_dir, target_dir, password=password, logger=self._log)
            self._log("打补丁完成")
            QMessageBox.information(self, "打补丁成功", "打补丁完成")
        except Exception as e:
            self._log(f"打补丁失败: {e}")
            QMessageBox.critical(self, "打补丁失败", str(e))

    def _on_rollback(self):
        data = self._get_inputs()
        backup_dir = data["backup"]
        target_dir = data["target"]
        password = data["ssh_password"]

        if not backup_dir:
            QMessageBox.warning(self, "输入错误", "Backup 目录不能为空")
            return
        if not target_dir:
            QMessageBox.warning(self, "输入错误", "Target 目录不能为空")
            return
        if is_remote(target_dir) and not password:
            QMessageBox.warning(self, "输入错误", "远程 Target 需要 SSH 密码")
            return
        if not is_remote(target_dir):
            if not self._ensure_local_dir(target_dir, "Target"):
                return

        backups = list_backups(backup_dir)
        if not backups:
            QMessageBox.information(self, "无备份", "Backup 目录下没有时间戳备份")
            return

        names = [os.path.basename(b) for b in backups]
        name, ok = QInputDialog.getItem(
            self, "选择备份", "请选择要回退的备份:", names, 0, False
        )
        if not ok or not name:
            self._log("用户取消回退")
            return

        selected_dir = os.path.join(backup_dir, name)

        self._log("开始校验目录兼容性...")
        compatibility, details = check_patch_compatibility(selected_dir, target_dir, logger=self._log)
        if compatibility == "none":
            QMessageBox.warning(
                self, "不允许回退",
                "备份与 Target 目录完全不一致，\n"
                "没有任何共同的文件或目录，禁止回退以避免覆盖错误目录。"
            )
            self._log("兼容性检查不通过: 完全不一致，禁止回退")
            return
        elif compatibility == "partial":
            only_output = details.get("only_output", [])
            only_target = details.get("only_target", [])
            mismatch_info = details.get("mismatch_info", [])

            lines = []
            lines.append("备份与 Target 目录部分不一致，以下为对比情况（最多显示前 10 项）：")
            lines.append("")

            max_len = max(len(only_output), len(only_target), len(mismatch_info), 1)
            lines.append("| 备份侧 | Target 侧 |")
            lines.append("|---------|-----------|")
            for i in range(min(max_len, 10)):
                left = only_output[i] if i < len(only_output) else ""
                right = only_target[i] if i < len(only_target) else ""
                lines.append(f"| {left} | {right} |")
            for m in mismatch_info[:10]:
                lines.append(f"| {m['name']} ({m['output_type']}) | {m['name']} ({m['target_type']}) |")
            if max_len > 10:
                lines.append(f"| ... 还有 {max_len - 10} 项未显示 | |")
            lines.append("")
            lines.append("是否继续回退？")
            md_text = "\n".join(lines)

            if not self._show_markdown_dialog("结构部分不匹配", md_text):
                self._log("用户取消回退")
                return

        reply = QMessageBox.question(
            self, "确认回退",
            f"即将把备份回退到 Target:\n"
            f"备份: {selected_dir}\n"
            f"Target: {target_dir}",
            QMessageBox.Yes | QMessageBox.No
        )
        if reply != QMessageBox.Yes:
            self._log("用户取消回退")
            return

        self._log("开始回退补丁...")
        try:
            rollback(selected_dir, target_dir, password=password, logger=self._log)
            self._log("回退补丁完成")
            QMessageBox.information(self, "回退成功", "回退补丁完成")
        except Exception as e:
            self._log(f"回退失败: {e}")
            QMessageBox.critical(self, "回退失败", str(e))


def main():
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec() if hasattr(app, "exec") else app.exec_())


if __name__ == "__main__":
    main()

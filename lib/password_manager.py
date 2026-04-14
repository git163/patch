"""Password manager for SSH password caching with Qt dialog prompt."""

import json
import os

# Try PySide2 first, fallback to PySide6
try:
    from PySide2.QtWidgets import QInputDialog, QLineEdit
except ImportError:
    from PySide6.QtWidgets import QInputDialog, QLineEdit

import paramiko


DEFAULT_PASSWORD_FILE = os.path.join(
    os.path.dirname(__file__), "..", "conf", "config.json"
)


class PasswordManager:
    """Caches SSH passwords per user@host, prompts on-demand, and persists to JSON."""

    def __init__(self, file_path: str = None):
        self._file_path = file_path or os.path.abspath(DEFAULT_PASSWORD_FILE)
        self._cache = {}
        self.load()

    def load(self):
        """Load cached passwords from disk under the 'ssh_passwords' key."""
        if os.path.exists(self._file_path):
            try:
                with open(self._file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    self._cache = data.get("ssh_passwords", {})
                    if not isinstance(self._cache, dict):
                        self._cache = {}
                else:
                    self._cache = {}
            except Exception:
                self._cache = {}

    def save(self):
        """Persist cached passwords to disk under the 'ssh_passwords' key without touching other keys."""
        os.makedirs(os.path.dirname(self._file_path), exist_ok=True)
        data = {}
        if os.path.exists(self._file_path):
            try:
                with open(self._file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if not isinstance(data, dict):
                    data = {}
            except Exception:
                data = {}
        data["ssh_passwords"] = self._cache
        with open(self._file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def get_password(self, user_host: str, parent=None) -> str:
        """Return cached password or prompt user via QInputDialog."""
        if user_host in self._cache:
            return self._cache[user_host]

        text, ok = QInputDialog.getText(
            parent,
            "SSH Password Required",
            f"Password for {user_host}:",
            QLineEdit.Password,
        )
        if not ok:
            return ""

        self._cache[user_host] = text
        self.save()
        return text

    def invalidate(self, user_host: str):
        """Remove a cached password (e.g., after auth failure)."""
        if user_host in self._cache:
            del self._cache[user_host]
            self.save()

    def clear(self):
        """Clear all cached passwords and delete the file."""
        self._cache = {}
        if os.path.exists(self._file_path):
            try:
                os.remove(self._file_path)
            except Exception:
                pass

    def verify_password(self, user_host: str, password: str) -> bool:
        """Try to connect via paramiko to verify the password."""
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            if "@" in user_host:
                username, hostname = user_host.split("@", 1)
            else:
                username = None
                hostname = user_host
            client.connect(
                hostname=hostname,
                username=username,
                password=password,
                look_for_keys=False,
                allow_agent=False,
                timeout=10,
            )
            client.close()
            return True
        except paramiko.AuthenticationException:
            return False
        except Exception:
            # Network errors etc. are treated as "cannot verify" but not auth failure
            return False

    def get_password_with_retry(self, user_host: str, parent=None) -> str:
        """Get password and verify it; re-prompt on auth failure until correct or cancelled."""
        while True:
            pwd = self.get_password(user_host, parent)
            if not pwd:
                return ""
            if self.verify_password(user_host, pwd):
                return pwd
            self.invalidate(user_host)
            # Loop will prompt again because cache is now empty

"""Persistent memory for PiGenus.

Backed by data/state.json.  A simple key-value store that survives restarts.
"""

import json
import os
from typing import Any

# Resolve data/ relative to this file's parent directory (runtime/)
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
STATE_FILE = os.path.join(DATA_DIR, "state.json")


class Memory:
    """Simple key-value store that persists to state.json.

    On construction the existing file is loaded (if present), so all
    previously stored values are immediately available after a restart.
    """

    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self._data: dict = {}
        self.load()

    def load(self):
        """Load state from disk; start fresh when file is absent or corrupt."""
        if not os.path.exists(STATE_FILE):
            self._data = {}
            return
        try:
            with open(STATE_FILE, "r") as fh:
                self._data = json.load(fh)
        except json.JSONDecodeError:
            # Preserve the corrupted file for debugging, then start clean.
            corrupt_path = STATE_FILE + ".corrupt"
            try:
                if os.path.exists(corrupt_path):
                    os.remove(corrupt_path)
                os.replace(STATE_FILE, corrupt_path)
            except OSError:
                pass
            self._data = {}

    def save(self):
        """Persist current state to disk atomically (survives partial writes)."""
        tmp_path = STATE_FILE + ".tmp"
        with open(tmp_path, "w") as fh:
            json.dump(self._data, fh, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, STATE_FILE)

    def get(self, key: str, default: Any = None) -> Any:
        """Return value for *key*, or *default* if absent."""
        return self._data.get(key, default)

    def set(self, key: str, value: Any):
        """Store *value* for *key* and persist immediately."""
        self._data[key] = value
        self.save()

    def all(self) -> dict:
        """Return a copy of the entire state dictionary."""
        return dict(self._data)

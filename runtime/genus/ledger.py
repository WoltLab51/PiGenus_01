"""Task and agent ledgers for PiGenus.

Two append-only JSON ledgers:
    data/task_ledger.json   – records of completed / failed tasks
    data/agent_ledger.json  – records of agent activity (task pick-ups)
"""

import json
import os
from datetime import datetime, timezone

# Resolve data/ relative to this file's parent directory (runtime/)
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
TASK_LEDGER_FILE = os.path.join(DATA_DIR, "task_ledger.json")
AGENT_LEDGER_FILE = os.path.join(DATA_DIR, "agent_ledger.json")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Ledger:
    """Append-only JSON ledger stored on disk.

    Each entry is a dict; a "timestamp" key is added automatically when absent.
    """

    def __init__(self, path: str):
        os.makedirs(DATA_DIR, exist_ok=True)
        self._path = path
        self._entries: list = []
        self.load()

    def load(self):
        """Load existing entries; start fresh when file is absent or corrupt."""
        if not os.path.exists(self._path):
            self._entries = []
            return
        try:
            with open(self._path, "r") as fh:
                self._entries = json.load(fh)
        except json.JSONDecodeError:
            corrupt_path = self._path + ".corrupt"
            try:
                if os.path.exists(corrupt_path):
                    os.remove(corrupt_path)
                os.replace(self._path, corrupt_path)
            except OSError:
                pass
            self._entries = []

    def save(self):
        """Persist entries to disk atomically (survives partial writes)."""
        tmp_path = self._path + ".tmp"
        with open(tmp_path, "w") as fh:
            json.dump(self._entries, fh, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, self._path)

    def record(self, entry: dict):
        """Append *entry* (timestamped) to the ledger."""
        entry.setdefault("timestamp", _now())
        self._entries.append(entry)
        self.save()

    def entries(self) -> list:
        """Return a copy of all entries."""
        return list(self._entries)


def task_ledger() -> Ledger:
    """Return the shared task ledger."""
    return Ledger(TASK_LEDGER_FILE)


def agent_ledger() -> Ledger:
    """Return the shared agent ledger."""
    return Ledger(AGENT_LEDGER_FILE)

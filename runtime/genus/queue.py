"""Task queue for PiGenus.

FIFO queue persisted to data/queue.json.  All mutations are written to disk
immediately so no tasks are lost on an unexpected restart.
"""

import json
import os
import uuid
from typing import Optional

from .logger import get_logger

# Resolve data/ relative to this file's parent directory (runtime/)
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
QUEUE_FILE = os.path.join(DATA_DIR, "queue.json")


class TaskQueue:
    """FIFO queue with JSON persistence.

    Each task is a dict with the keys:
        id       – unique UUID string
        type     – task type string (e.g. "echo", "noop")
        payload  – arbitrary dict of task parameters
        status   – one of "pending", "processing", "done", "failed"
    """

    def __init__(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        self._queue: list = []
        self.load()

    def load(self):
        """Load queue from disk; start with an empty list when absent or corrupt.

        Any tasks left in ``"processing"`` status (from a previous crash
        between ``dequeue`` and ``mark_done/mark_failed``) are reset to
        ``"pending"`` so they are not lost.
        """
        if not os.path.exists(QUEUE_FILE):
            self._queue = []
            return
        try:
            with open(QUEUE_FILE, "r") as fh:
                self._queue = json.load(fh)
        except json.JSONDecodeError:
            corrupt_path = QUEUE_FILE + ".corrupt"
            try:
                if os.path.exists(corrupt_path):
                    os.remove(corrupt_path)
                os.replace(QUEUE_FILE, corrupt_path)
            except OSError:
                pass
            self._queue = []
            return

        # Reset interrupted tasks so they are retried on restart.
        recovered = 0
        for task in self._queue:
            if task.get("status") == "processing":
                task["status"] = "pending"
                recovered += 1
        if recovered:
            self.save()

    def save(self):
        """Persist current queue state to disk atomically (survives partial writes)."""
        tmp_path = QUEUE_FILE + ".tmp"
        with open(tmp_path, "w") as fh:
            json.dump(self._queue, fh, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, QUEUE_FILE)

    def enqueue(self, task_type: str, payload: Optional[dict] = None) -> dict:
        """Add a new task and return it."""
        task = {
            "id": str(uuid.uuid4()),
            "type": task_type,
            "payload": payload or {},
            "status": "pending",
        }
        self._queue.append(task)
        self.save()
        return task

    def dequeue(self) -> Optional[dict]:
        """Pop the next pending task (sets status to "processing")."""
        for task in self._queue:
            if task["status"] == "pending":
                task["status"] = "processing"
                self.save()
                return task
        return None

    def mark_done(self, task_id: str, result: Optional[dict] = None):
        """Mark a task as successfully completed."""
        for task in self._queue:
            if task["id"] == task_id:
                task["status"] = "done"
                task["result"] = result or {}
                break
        self.save()

    def mark_failed(self, task_id: str, reason: str = ""):
        """Mark a task as failed."""
        for task in self._queue:
            if task["id"] == task_id:
                task["status"] = "failed"
                task["reason"] = reason
                break
        self.save()

    def peek(self) -> Optional[dict]:
        """Return the next pending task without changing its status."""
        for task in self._queue:
            if task["status"] == "pending":
                return task
        return None

    def pending_count(self) -> int:
        """Return the number of tasks still waiting to be processed."""
        return sum(1 for t in self._queue if t["status"] == "pending")

    def unfinished_count(self) -> int:
        """Return the number of tasks not yet in a terminal state.

        Includes both ``"pending"`` and ``"processing"`` tasks so the
        orchestrator can detect whether work remains even after a restart
        that recovered interrupted tasks.
        """
        return sum(1 for t in self._queue if t["status"] in ("pending", "processing"))

    def load_from_json_file(self, path: str, whitelist=None) -> int:
        """Load tasks from an external JSON file and enqueue them.

        The file must contain a JSON array of task dicts.  Each dict must
        have at least a ``"type"`` key; ``"payload"`` is optional.

        Returns the number of tasks successfully enqueued.  Returns 0 (and
        logs a warning) if the file is missing, unreadable, or contains
        invalid JSON.  Individual invalid items, including task dicts that
        lack a ``"type"`` key, are skipped and logged as warnings.

        Parameters
        ----------
        whitelist:
            Optional TaskWhitelist instance.  When provided, task types not in
            the whitelist are logged and skipped rather than enqueued.
        """
        _log = get_logger()

        if not os.path.exists(path):
            _log.warning("load_from_json_file: file not found: %s", path)
            return 0

        try:
            with open(path, "r") as fh:
                data = json.load(fh)
        except (json.JSONDecodeError, OSError) as exc:
            _log.warning("load_from_json_file: cannot read %s: %s", path, exc)
            return 0

        if not isinstance(data, list):
            _log.warning("load_from_json_file: expected a JSON array in %s", path)
            return 0

        new_tasks = []
        for item in data:
            if not isinstance(item, dict):
                _log.warning("load_from_json_file: skipping non-dict item: %r", item)
                continue
            task_type = item.get("type")
            if not task_type:
                _log.warning("load_from_json_file: skipping task without 'type': %r", item)
                continue
            # Whitelist guard
            if whitelist is not None and not whitelist.is_allowed(str(task_type)):
                _log.warning(
                    "load_from_json_file: REJECTED type=%r — not in whitelist", task_type
                )
                continue
            payload = item.get("payload")
            new_tasks.append({
                "id": str(uuid.uuid4()),
                "type": str(task_type),
                "payload": payload if isinstance(payload, dict) else {},
                "status": "pending",
            })

        if new_tasks:
            self._queue.extend(new_tasks)
            self.save()

        return len(new_tasks)

    def __len__(self) -> int:
        return len(self._queue)

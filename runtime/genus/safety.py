"""Safety layer for PiGenus v0.4.

Provides a task type whitelist and kill-switch check used by the orchestrator
and queue to prevent unauthorised task types from entering the system.
"""

import os
from .logger import get_logger

logger = get_logger()

# Default allowed task types.
DEFAULT_WHITELIST = frozenset({"echo", "noop", "classify"})


class TaskWhitelist:
    """Enforces an allowed list of task types.

    Parameters
    ----------
    allowed:
        Iterable of permitted task type strings.  Defaults to DEFAULT_WHITELIST.
    """

    def __init__(self, allowed=None):
        self._allowed = frozenset(allowed) if allowed is not None else DEFAULT_WHITELIST

    @property
    def allowed(self) -> frozenset:
        return self._allowed

    def is_allowed(self, task_type: str) -> bool:
        """Return True if *task_type* is in the whitelist."""
        return task_type in self._allowed

    def check(self, task: dict) -> bool:
        """Return True and log acceptance, or False and log rejection.

        Never raises; always returns a bool.
        """
        try:
            task_type = task.get("type", "") if isinstance(task, dict) else ""
            task_id = task.get("id", "?") if isinstance(task, dict) else "?"
        except Exception:
            logger.warning("Safety | could not inspect task object: %r", task)
            return False

        if task_type in self._allowed:
            logger.debug("Safety | accepted task_id=%s type=%s", task_id, task_type)
            return True
        else:
            logger.warning(
                "Safety | REJECTED task_id=%s type=%r — not in whitelist %s",
                task_id, task_type, sorted(self._allowed),
            )
            return False


def check_kill_switch(data_dir: str) -> bool:
    """Return True if the STOP kill-switch file is present in *data_dir*.

    Never raises.
    """
    try:
        stop_path = os.path.join(data_dir, "STOP")
        return os.path.exists(stop_path)
    except Exception:
        return False


def consume_kill_switch(data_dir: str) -> bool:
    """Remove the STOP file if present.  Returns True if it was removed.

    Safe to call even when the file does not exist.
    """
    try:
        stop_path = os.path.join(data_dir, "STOP")
        if os.path.exists(stop_path):
            os.remove(stop_path)
            return True
    except OSError as exc:
        logger.warning("Safety | could not remove STOP file: %s", exc)
    return False

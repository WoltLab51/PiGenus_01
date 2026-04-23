"""BasicWorker agent for PiGenus.

Pulls the next pending task from the queue, processes it, and records the
outcome in the task and agent ledgers.

Supported task types:
    echo   – returns the message from payload back in the result
    noop   – does nothing; returns {"noop": True}
"""

from .queue import TaskQueue
from .ledger import Ledger
from .logger import get_logger

logger = get_logger()


class BasicWorker:
    """Minimal worker: handles 'echo' and 'noop' task types."""

    NAME = "basic_worker"

    def __init__(self, queue: TaskQueue, task_ledger: Ledger, agent_ledger: Ledger):
        self._queue = queue
        self._task_ledger = task_ledger
        self._agent_ledger = agent_ledger

    def run_once(self) -> bool:
        """Process one pending task.

        Returns True when a task was found and processed (regardless of
        success/failure), False when the queue was empty.
        """
        task = self._queue.dequeue()
        if task is None:
            return False

        logger.info(
            "Worker picked up task %s (type=%s)", task["id"], task["type"]
        )
        self._agent_ledger.record(
            {
                "event": "task_start",
                "agent": self.NAME,
                "task_id": task["id"],
                "task_type": task["type"],
            }
        )

        try:
            result = self._process(task)
            self._queue.mark_done(task["id"], result)
            self._task_ledger.record(
                {
                    "event": "task_done",
                    "task_id": task["id"],
                    "task_type": task["type"],
                    "result": result,
                }
            )
            logger.info("Task %s done: %s", task["id"], result)
        except Exception as exc:
            reason = str(exc)
            self._queue.mark_failed(task["id"], reason)
            self._task_ledger.record(
                {
                    "event": "task_failed",
                    "task_id": task["id"],
                    "task_type": task["type"],
                    "reason": reason,
                }
            )
            logger.error("Task %s failed: %s", task["id"], reason)

        return True

    def _process(self, task: dict) -> dict:
        """Dispatch to the appropriate handler; raise on unknown type."""
        task_type = task["type"]
        payload = task.get("payload", {})

        if task_type == "echo":
            message = payload.get("message", "")
            return {"echo": message}
        elif task_type == "noop":
            return {"noop": True}
        else:
            raise ValueError(f"Unknown task type: {task_type!r}")

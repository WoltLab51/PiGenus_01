"""Simple evaluator for PiGenus.

Reviews the task ledger and updates persistent run statistics in memory.
"""

from .ledger import Ledger
from .memory import Memory
from .logger import get_logger

logger = get_logger()


class Evaluator:
    """Reviews completed tasks and persists summary statistics."""

    def __init__(self, task_ledger: Ledger, memory: Memory):
        self._task_ledger = task_ledger
        self._memory = memory

    def evaluate(self) -> dict:
        """Count done/failed tasks, persist stats, and return them."""
        entries = self._task_ledger.entries()
        done = sum(1 for e in entries if e.get("event") == "task_done")
        failed = sum(1 for e in entries if e.get("event") == "task_failed")

        self._memory.set("tasks_done", done)
        self._memory.set("tasks_failed", failed)

        stats = {"tasks_done": done, "tasks_failed": failed}
        logger.info(
            "Evaluation: tasks_done=%d tasks_failed=%d", done, failed
        )
        return stats

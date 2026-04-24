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
        """Count done/failed tasks, compute scores, persist stats, and return them."""
        entries = self._task_ledger.entries()
        done = sum(1 for e in entries if e.get("event") == "task_done")
        failed = sum(1 for e in entries if e.get("event") == "task_failed")
        total = done + failed

        # success_score: fraction of tasks that succeeded
        success_score = done / total if total > 0 else 0.0

        # efficiency_score: based on duration_ms of done tasks; faster = higher score
        try:
            durations = [
                e["duration_ms"]
                for e in entries
                if e.get("event") == "task_done" and isinstance(e.get("duration_ms"), (int, float))
            ]
            if durations:
                avg_ms = sum(durations) / len(durations)
                # Normalise: 0 ms → 1.0, 10000 ms → ~0.0; cap between 0 and 1
                efficiency_score = max(0.0, min(1.0, 1.0 - avg_ms / 10000.0))
            else:
                efficiency_score = 0.5
        except Exception:
            efficiency_score = 0.5

        # stability_score: fraction of tasks that did NOT fail
        stability_score = 1.0 - (failed / total) if total > 0 else 0.0

        # resource_score: neutral placeholder (no external deps on Pi)
        resource_score = 0.5

        # learning_score: grows with experience, capped at 1.0
        learning_score = min(1.0, done / 10.0)

        self._memory.set("tasks_done", done)
        self._memory.set("tasks_failed", failed)
        self._memory.set("success_score", success_score)
        self._memory.set("efficiency_score", efficiency_score)
        self._memory.set("stability_score", stability_score)
        self._memory.set("resource_score", resource_score)
        self._memory.set("learning_score", learning_score)

        stats = {
            "tasks_done": done,
            "tasks_failed": failed,
            "success_score": success_score,
            "efficiency_score": efficiency_score,
            "stability_score": stability_score,
            "resource_score": resource_score,
            "learning_score": learning_score,
        }
        logger.info(
            "Evaluation: tasks_done=%d tasks_failed=%d "
            "success=%.2f efficiency=%.2f stability=%.2f resource=%.2f learning=%.2f",
            done, failed,
            success_score, efficiency_score, stability_score, resource_score, learning_score,
        )
        return stats

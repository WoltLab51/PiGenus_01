"""Orchestrator for PiGenus v0.2.

Wires up all components (memory, queue, worker, evaluator) and runs the
main tick-based loop.  On first run a sample task is seeded; on subsequent
runs persisted state and queue are reloaded transparently.

Usage:
    from genus.orchestrator import Orchestrator
    Orchestrator().run()
"""

import os
import time

from .memory import Memory
from .queue import TaskQueue
from . import queue as _queue_module
from .ledger import task_ledger, agent_ledger
from .worker import BasicWorker
from .evaluator import Evaluator
from .matcher import match
from .logger import get_logger

logger = get_logger()

# Default maximum ticks per run (0 = run indefinitely until queue empties)
DEFAULT_MAX_TICKS = 20


class Orchestrator:
    """Coordinates memory, queue, worker, and evaluator.

    Parameters
    ----------
    max_ticks:
        Maximum number of loop iterations before the orchestrator stops.
        Set to 0 to run until the queue is drained.
    tick_delay:
        Seconds to sleep between ticks (keeps CPU usage low on the Pi).
    """

    def __init__(self, max_ticks: int = DEFAULT_MAX_TICKS, tick_delay: float = 0.1):
        self.memory = Memory()
        self.queue = TaskQueue()
        self.tl = task_ledger()
        self.al = agent_ledger()
        self.worker = BasicWorker(self.queue, self.tl, self.al)
        self.evaluator = Evaluator(self.tl, self.memory)
        self.max_ticks = max_ticks
        self.tick_delay = tick_delay

    def _bootstrap(self):
        """Seed the queue with sample tasks on the very first run."""
        if not self.memory.get("bootstrapped"):
            logger.info("First run detected – seeding sample tasks.")
            self.queue.enqueue("echo", {"message": "Hello from PiGenus!"})
            self.queue.enqueue("noop")
            self.memory.set("bootstrapped", True)
        else:
            logger.info(
                "Restart detected – loaded persisted state "
                "(tasks_done=%s, tasks_failed=%s).",
                self.memory.get("tasks_done", 0),
                self.memory.get("tasks_failed", 0),
            )

        # Load any externally injected tasks from the data directory.
        # DATA_DIR is read at call time so the test harness can redirect it.
        ext_path = os.path.join(_queue_module.DATA_DIR, "external_queue.json")
        if os.path.exists(ext_path):
            injected = self.queue.load_from_json_file(ext_path)
            if injected:
                logger.info(
                    "Loaded %d task(s) from external queue file: %s",
                    injected, ext_path,
                )
                # Only rename after a confirmed successful load so the file
                # can be corrected and retried if it contained invalid JSON.
                processed_path = ext_path + ".processed"
                try:
                    os.replace(ext_path, processed_path)
                except OSError as exc:
                    logger.warning(
                        "Could not rename external queue file: %s", exc
                    )

    def run(self):
        """Run the main tick loop.

        Each tick the worker attempts to process one queued task.  The loop
        stops when the queue is empty or *max_ticks* is reached.  A final
        evaluation pass is always executed before returning.
        """
        logger.info("PiGenus v0.2 orchestrator starting.")
        self._bootstrap()

        tick = 0
        while self.max_ticks == 0 or tick < self.max_ticks:
            tick += 1
            pending = self.queue.pending_count()
            logger.info("Tick %d | queue pending=%d", tick, pending)

            next_task = self.queue.peek()
            if next_task:
                category, agent_name = match(next_task)
                logger.info(
                    "Match | task_id=%s type=%s -> category=%s agent=%s",
                    next_task.get("id", "?"), next_task.get("type", "?"),
                    category, agent_name,
                )

            processed = self.worker.run_once()

            if not processed and self.queue.unfinished_count() == 0:
                logger.info("Queue empty – running final evaluation.")
                break

            if self.tick_delay > 0:
                time.sleep(self.tick_delay)

        self.evaluator.evaluate()
        logger.info("Orchestrator finished.")

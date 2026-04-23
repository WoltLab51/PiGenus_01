"""Minimal unit tests for PiGenus v0.1.

Run with:
    python -m unittest tests/test_genus.py   (from runtime/)
    python runtime/tests/test_genus.py       (from repo root)
"""

import os
import sys
import tempfile
import unittest

# Make the 'genus' package importable from any working directory.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


# ---------------------------------------------------------------------------
# Helpers to redirect persistence files to a temporary directory so that
# tests do not pollute (or depend on) the real data/ directory.
# ---------------------------------------------------------------------------

_TMP_OBJ = tempfile.TemporaryDirectory(prefix="pigenus_test_")
_TMPDIR = _TMP_OBJ.name


def tearDownModule():
    """Remove the temporary directory after all tests have run."""
    _TMP_OBJ.cleanup()


def _patch_data_dir(module_name: str, tmpdir: str):
    """Override DATA_DIR and derived path constants in *module_name*."""
    import importlib
    mod = sys.modules.get(module_name) or importlib.import_module(module_name)
    mod.DATA_DIR = tmpdir
    if hasattr(mod, "STATE_FILE"):
        mod.STATE_FILE = os.path.join(tmpdir, "state.json")
    if hasattr(mod, "QUEUE_FILE"):
        mod.QUEUE_FILE = os.path.join(tmpdir, "queue.json")
    if hasattr(mod, "TASK_LEDGER_FILE"):
        mod.TASK_LEDGER_FILE = os.path.join(tmpdir, "task_ledger.json")
    if hasattr(mod, "AGENT_LEDGER_FILE"):
        mod.AGENT_LEDGER_FILE = os.path.join(tmpdir, "agent_ledger.json")


# Patch all modules before any test class is defined so that every
# instantiation uses the temporary directory.
for _mod in ("genus.memory", "genus.queue", "genus.ledger", "genus.logger"):
    _patch_data_dir(_mod, _TMPDIR)


# Now import the classes (after patching).
from genus.memory import Memory
from genus.queue import TaskQueue
from genus.ledger import Ledger
from genus.worker import BasicWorker
from genus.evaluator import Evaluator
from genus.orchestrator import Orchestrator


# ---------------------------------------------------------------------------
# Helper: remove a file if it exists (clean state between tests)
# ---------------------------------------------------------------------------

def _rm(*filenames):
    for name in filenames:
        path = os.path.join(_TMPDIR, name)
        if os.path.exists(path):
            os.remove(path)


# ---------------------------------------------------------------------------
# Memory tests
# ---------------------------------------------------------------------------

class TestMemory(unittest.TestCase):
    def setUp(self):
        _rm("state.json")

    def test_set_and_get(self):
        m = Memory()
        m.set("x", 99)
        self.assertEqual(m.get("x"), 99)

    def test_get_missing_returns_default(self):
        m = Memory()
        self.assertIsNone(m.get("missing"))
        self.assertEqual(m.get("missing", "fallback"), "fallback")

    def test_persistence_across_instances(self):
        m1 = Memory()
        m1.set("key", "value")
        m2 = Memory()  # new instance reads from disk
        self.assertEqual(m2.get("key"), "value")

    def test_all_returns_copy(self):
        m = Memory()
        m.set("a", 1)
        snapshot = m.all()
        snapshot["a"] = 999  # mutating the copy should not affect Memory
        self.assertEqual(m.get("a"), 1)


# ---------------------------------------------------------------------------
# TaskQueue tests
# ---------------------------------------------------------------------------

class TestTaskQueue(unittest.TestCase):
    def setUp(self):
        _rm("queue.json")

    def test_enqueue_returns_task(self):
        q = TaskQueue()
        task = q.enqueue("echo", {"message": "hi"})
        self.assertEqual(task["type"], "echo")
        self.assertEqual(task["status"], "pending")
        self.assertIn("id", task)

    def test_dequeue_sets_processing(self):
        q = TaskQueue()
        q.enqueue("noop")
        task = q.dequeue()
        self.assertIsNotNone(task)
        self.assertEqual(task["status"], "processing")

    def test_dequeue_empty_returns_none(self):
        q = TaskQueue()
        self.assertIsNone(q.dequeue())

    def test_pending_count(self):
        q = TaskQueue()
        q.enqueue("noop")
        q.enqueue("noop")
        self.assertEqual(q.pending_count(), 2)
        q.dequeue()
        self.assertEqual(q.pending_count(), 1)

    def test_mark_done(self):
        q = TaskQueue()
        t = q.enqueue("noop")
        q.dequeue()
        q.mark_done(t["id"], {"ok": True})
        self.assertEqual(q.pending_count(), 0)
        # Reload from disk and confirm status persisted
        q2 = TaskQueue()
        done = [x for x in q2._queue if x["id"] == t["id"]]
        self.assertEqual(done[0]["status"], "done")

    def test_mark_failed(self):
        q = TaskQueue()
        t = q.enqueue("bad")
        q.dequeue()
        q.mark_failed(t["id"], "oops")
        q2 = TaskQueue()
        failed = [x for x in q2._queue if x["id"] == t["id"]]
        self.assertEqual(failed[0]["status"], "failed")
        self.assertEqual(failed[0]["reason"], "oops")


# ---------------------------------------------------------------------------
# Ledger tests
# ---------------------------------------------------------------------------

class TestLedger(unittest.TestCase):
    def setUp(self):
        _rm("task_ledger.json")

    def test_record_and_retrieve(self):
        tl = Ledger(os.path.join(_TMPDIR, "task_ledger.json"))
        tl.record({"event": "task_done", "task_id": "abc"})
        entries = tl.entries()
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["event"], "task_done")
        self.assertIn("timestamp", entries[0])

    def test_persistence(self):
        tl1 = Ledger(os.path.join(_TMPDIR, "task_ledger.json"))
        tl1.record({"event": "task_done", "task_id": "x"})
        tl2 = Ledger(os.path.join(_TMPDIR, "task_ledger.json"))
        self.assertEqual(len(tl2.entries()), 1)


# ---------------------------------------------------------------------------
# BasicWorker tests
# ---------------------------------------------------------------------------

class TestBasicWorker(unittest.TestCase):
    def setUp(self):
        _rm("queue.json", "task_ledger.json", "agent_ledger.json")
        q = TaskQueue()
        tl = Ledger(os.path.join(_TMPDIR, "task_ledger.json"))
        al = Ledger(os.path.join(_TMPDIR, "agent_ledger.json"))
        self.worker = BasicWorker(q, tl, al)
        self.queue = q
        self.tl = tl

    def test_echo_task(self):
        self.queue.enqueue("echo", {"message": "hello"})
        ran = self.worker.run_once()
        self.assertTrue(ran)
        done = [e for e in self.tl.entries() if e["event"] == "task_done"]
        self.assertEqual(len(done), 1)
        self.assertEqual(done[0]["result"]["echo"], "hello")

    def test_noop_task(self):
        self.queue.enqueue("noop")
        ran = self.worker.run_once()
        self.assertTrue(ran)
        done = [e for e in self.tl.entries() if e["event"] == "task_done"]
        self.assertEqual(done[0]["result"]["noop"], True)

    def test_unknown_task_type_is_recorded_as_failed(self):
        self.queue.enqueue("unknown_xyz")
        ran = self.worker.run_once()
        self.assertTrue(ran)
        failed = [e for e in self.tl.entries() if e["event"] == "task_failed"]
        self.assertEqual(len(failed), 1)

    def test_empty_queue_returns_false(self):
        ran = self.worker.run_once()
        self.assertFalse(ran)


# ---------------------------------------------------------------------------
# Evaluator tests
# ---------------------------------------------------------------------------

class TestEvaluator(unittest.TestCase):
    def setUp(self):
        _rm("state.json", "task_ledger.json")

    def test_evaluate_counts(self):
        m = Memory()
        tl = Ledger(os.path.join(_TMPDIR, "task_ledger.json"))
        tl.record({"event": "task_done", "task_id": "1"})
        tl.record({"event": "task_done", "task_id": "2"})
        tl.record({"event": "task_failed", "task_id": "3"})
        ev = Evaluator(tl, m)
        stats = ev.evaluate()
        self.assertEqual(stats["tasks_done"], 2)
        self.assertEqual(stats["tasks_failed"], 1)
        # Stats should be persisted in memory
        m2 = Memory()
        self.assertEqual(m2.get("tasks_done"), 2)


# ---------------------------------------------------------------------------
# Orchestrator tests
# ---------------------------------------------------------------------------

class TestOrchestrator(unittest.TestCase):
    def setUp(self):
        _rm(
            "state.json",
            "queue.json",
            "task_ledger.json",
            "agent_ledger.json",
        )

    def _make_orchestrator(self, **kwargs):
        """Convenience: Orchestrator with no tick delay for fast tests."""
        return Orchestrator(tick_delay=0, **kwargs)

    def test_first_run_seeds_tasks_and_sets_bootstrapped(self):
        orc = self._make_orchestrator()
        orc.run()
        # Bootstrap flag must be persisted.
        m = Memory()
        self.assertTrue(m.get("bootstrapped"))

    def test_first_run_processes_seeded_tasks(self):
        orc = self._make_orchestrator()
        orc.run()
        # Both seeded tasks should be done.
        m = Memory()
        self.assertEqual(m.get("tasks_done"), 2)
        self.assertEqual(m.get("tasks_failed"), 0)

    def test_restart_skips_seeding(self):
        # First run seeds and processes.
        self._make_orchestrator().run()
        # Second run should NOT re-seed; queue stays empty.
        orc2 = self._make_orchestrator()
        orc2.run()
        q = TaskQueue()
        # All tasks remain in done state; no new pending tasks.
        self.assertEqual(q.pending_count(), 0)

    def test_evaluation_always_runs(self):
        """Evaluator must update stats even when queue was already empty."""
        # Pre-seed bootstrapped so the orchestrator skips seeding.
        m = Memory()
        m.set("bootstrapped", True)
        orc = self._make_orchestrator()
        orc.run()
        # Evaluator should have written stats (0 done, 0 failed on clean run).
        m2 = Memory()
        self.assertIsNotNone(m2.get("tasks_done"))

    def test_processing_tasks_recovered_on_restart(self):
        """Tasks stuck in 'processing' must be retried after a crash/restart."""
        q = TaskQueue()
        task = q.enqueue("echo", {"message": "crash-test"})
        # Simulate crash: mark as processing but never finish.
        q.dequeue()  # sets status="processing" and saves to disk

        # Reload queue (simulates restart).
        q2 = TaskQueue()
        # The task should have been reset to pending.
        stuck = [t for t in q2._queue if t["id"] == task["id"]]
        self.assertEqual(stuck[0]["status"], "pending")

    def test_unfinished_count_includes_processing(self):
        q = TaskQueue()
        q.enqueue("noop")
        q.enqueue("noop")
        q.dequeue()  # first task is now "processing"
        self.assertEqual(q.unfinished_count(), 2)
        self.assertEqual(q.pending_count(), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
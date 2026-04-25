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
from genus.problem_matrix import ProblemMatrix
from genus.agent_matrix import AgentMatrix
from genus.matcher import match
from genus.safety import TaskWhitelist, check_kill_switch, consume_kill_switch


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

    def test_set_in_and_get_section(self):
        m = Memory()
        m.set_in("semantic", "foo", 42)
        self.assertEqual(m.get_section("semantic")["foo"], 42)

    def test_backward_compat_flat_key(self):
        m = Memory()
        m.set("old_key", "val")
        m2 = Memory()
        self.assertEqual(m2.get("old_key"), "val")

    def test_migration_from_flat_format(self):
        import json as _json
        # Write a raw flat JSON file (old format without section keys).
        flat_path = os.path.join(_TMPDIR, "state.json")
        with open(flat_path, "w") as fh:
            _json.dump({"tasks_done": 3}, fh)
        m = Memory()
        self.assertEqual(m.get("tasks_done"), 3)

    def test_non_dict_json_is_treated_as_corrupt(self):
        import json as _json
        # Write valid JSON that is not a dict (e.g. a list).
        non_dict_path = os.path.join(_TMPDIR, "state.json")
        with open(non_dict_path, "w") as fh:
            _json.dump([1, 2, 3], fh)
        m = Memory()
        # Should start fresh without raising.
        self.assertEqual(m.all(), {})


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

    def test_peek_returns_first_pending(self):
        q = TaskQueue()
        t = q.enqueue("echo", {"message": "peek-me"})
        peeked = q.peek()
        self.assertIsNotNone(peeked)
        self.assertEqual(peeked["id"], t["id"])
        self.assertEqual(peeked["type"], "echo")

    def test_peek_does_not_mutate_status(self):
        q = TaskQueue()
        q.enqueue("noop")
        q.peek()
        # Status must still be pending after peek
        self.assertEqual(q.pending_count(), 1)

    def test_peek_empty_returns_none(self):
        q = TaskQueue()
        self.assertIsNone(q.peek())

    # ------------------------------------------------------------------
    # v0.2 load_from_json_file tests
    # ------------------------------------------------------------------

    def test_load_from_json_file_valid(self):
        import json as _json
        tasks = [
            {"type": "echo", "payload": {"message": "Hallo von extern!"}},
            {"type": "noop"},
        ]
        path = os.path.join(_TMPDIR, "ext_queue_valid.json")
        with open(path, "w") as fh:
            _json.dump(tasks, fh)
        q = TaskQueue()
        count = q.load_from_json_file(path)
        self.assertEqual(count, 2)
        self.assertEqual(q.pending_count(), 2)

    def test_load_from_json_file_missing_file(self):
        q = TaskQueue()
        count = q.load_from_json_file(os.path.join(_TMPDIR, "does_not_exist.json"))
        self.assertEqual(count, 0)

    def test_load_from_json_file_invalid_json(self):
        path = os.path.join(_TMPDIR, "ext_queue_bad.json")
        with open(path, "w") as fh:
            fh.write("this is not valid json {{{")
        q = TaskQueue()
        count = q.load_from_json_file(path)
        self.assertEqual(count, 0)

    def test_load_from_json_file_skips_task_without_type(self):
        import json as _json
        tasks = [
            {"type": "echo", "payload": {}},
            {"payload": {"orphan": True}},  # no "type" key – must be skipped
            {"type": "noop"},
        ]
        path = os.path.join(_TMPDIR, "ext_queue_partial.json")
        with open(path, "w") as fh:
            _json.dump(tasks, fh)
        q = TaskQueue()
        count = q.load_from_json_file(path)
        self.assertEqual(count, 2)
        self.assertEqual(q.pending_count(), 2)


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

    def test_enriched_entry_fields(self):
        _rm("queue.json", "task_ledger.json", "agent_ledger.json")
        q = TaskQueue()
        tl = Ledger(os.path.join(_TMPDIR, "task_ledger.json"))
        al = Ledger(os.path.join(_TMPDIR, "agent_ledger.json"))
        worker = BasicWorker(q, tl, al)
        q.enqueue("echo", {"message": "enrich-test"})
        worker.run_once()
        done = [e for e in tl.entries() if e.get("event") == "task_done"]
        self.assertEqual(len(done), 1)
        entry = done[0]
        for field in ("agent_name", "category", "success_score", "efficiency_score",
                      "stability_score", "resource_score", "learning_score", "duration_ms"):
            self.assertIn(field, entry, f"Missing field: {field}")


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

    def test_classify_task(self):
        self.queue.enqueue("classify", {"task_type": "echo"})
        ran = self.worker.run_once()
        self.assertTrue(ran)
        done = [e for e in self.tl.entries() if e["event"] == "task_done"]
        self.assertEqual(len(done), 1)
        self.assertEqual(done[0]["result"], {"category": "communication", "task_type": "echo"})

    def test_classify_unknown_type(self):
        self.queue.enqueue("classify", {"task_type": "totally_unknown"})
        self.worker.run_once()
        done = [e for e in self.tl.entries() if e["event"] == "task_done"]
        self.assertIn("category", done[0]["result"])
        self.assertEqual(done[0]["result"]["category"], "unknown")

    def test_ledger_entry_has_agent_name(self):
        self.queue.enqueue("echo", {"message": "agent-name-test"})
        self.worker.run_once()
        done = [e for e in self.tl.entries() if e["event"] == "task_done"]
        self.assertEqual(done[0]["agent_name"], "basic_worker")

    def test_ledger_entry_has_scores(self):
        self.queue.enqueue("echo", {"message": "scores-test"})
        self.worker.run_once()
        done = [e for e in self.tl.entries() if e["event"] == "task_done"]
        entry = done[0]
        self.assertIn("success_score", entry)
        self.assertIn("efficiency_score", entry)
        self.assertIn("stability_score", entry)


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

    # ------------------------------------------------------------------
    # v0.2 score tests
    # ------------------------------------------------------------------

    def _make_evaluator(self, done=0, failed=0):
        """Return a fresh Evaluator with the given done/failed task counts."""
        _rm("state.json", "task_ledger.json")
        m = Memory()
        tl = Ledger(os.path.join(_TMPDIR, "task_ledger.json"))
        for i in range(done):
            tl.record({"event": "task_done", "task_id": str(i)})
        for i in range(failed):
            tl.record({"event": "task_failed", "task_id": str(done + i)})
        return Evaluator(tl, m)

    def test_all_five_scores_present(self):
        ev = self._make_evaluator(done=3, failed=1)
        stats = ev.evaluate()
        for key in ("success_score", "efficiency_score", "stability_score",
                    "resource_score", "learning_score"):
            self.assertIn(key, stats, f"Missing key: {key}")

    def test_success_score_all_done(self):
        ev = self._make_evaluator(done=5, failed=0)
        stats = ev.evaluate()
        self.assertAlmostEqual(stats["success_score"], 1.0)

    def test_success_score_no_tasks(self):
        ev = self._make_evaluator(done=0, failed=0)
        stats = ev.evaluate()
        self.assertAlmostEqual(stats["success_score"], 0.0)

    def test_stability_score_mixed(self):
        # 3 done, 1 failed → total=4, stability = 1 - 1/4 = 0.75
        ev = self._make_evaluator(done=3, failed=1)
        stats = ev.evaluate()
        self.assertAlmostEqual(stats["stability_score"], 0.75)

    def test_resource_score_neutral(self):
        ev = self._make_evaluator(done=2, failed=0)
        stats = ev.evaluate()
        self.assertAlmostEqual(stats["resource_score"], 0.5)

    def test_learning_score_grows_with_done(self):
        ev = self._make_evaluator(done=10, failed=0)
        stats = ev.evaluate()
        self.assertAlmostEqual(stats["learning_score"], 1.0)

    def test_learning_score_below_cap(self):
        ev = self._make_evaluator(done=5, failed=0)
        stats = ev.evaluate()
        self.assertAlmostEqual(stats["learning_score"], 0.5)

    def test_efficiency_score_neutral_when_no_duration(self):
        ev = self._make_evaluator(done=2, failed=0)
        stats = ev.evaluate()
        self.assertAlmostEqual(stats["efficiency_score"], 0.5)

    def test_scores_persisted_in_memory(self):
        ev = self._make_evaluator(done=4, failed=1)
        ev.evaluate()
        m2 = Memory()
        for key in ("success_score", "efficiency_score", "stability_score",
                    "resource_score", "learning_score"):
            self.assertIsNotNone(m2.get(key), f"Not persisted: {key}")


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


# ---------------------------------------------------------------------------
# ProblemMatrix tests
# ---------------------------------------------------------------------------

class TestProblemMatrix(unittest.TestCase):
    def test_known_types(self):
        pm = ProblemMatrix()
        self.assertEqual(pm.categorize("echo"), "communication")
        self.assertEqual(pm.categorize("noop"), "maintenance")

    def test_unknown_type_returns_unknown(self):
        pm = ProblemMatrix()
        self.assertEqual(pm.categorize("totally_unknown"), "unknown")

    def test_empty_string_returns_unknown(self):
        pm = ProblemMatrix()
        self.assertEqual(pm.categorize(""), "unknown")

    def test_custom_mapping(self):
        pm = ProblemMatrix({"custom": "custom_category"})
        self.assertEqual(pm.categorize("custom"), "custom_category")
        self.assertEqual(pm.categorize("echo"), "unknown")

    def test_classify_type(self):
        pm = ProblemMatrix()
        self.assertEqual(pm.categorize("classify"), "classification")


# ---------------------------------------------------------------------------
# AgentMatrix tests
# ---------------------------------------------------------------------------

class TestAgentMatrix(unittest.TestCase):
    def test_known_categories(self):
        am = AgentMatrix()
        self.assertEqual(am.resolve("communication"), "basic_worker")
        self.assertEqual(am.resolve("maintenance"), "basic_worker")
        self.assertEqual(am.resolve("unknown"), "basic_worker")

    def test_unrecognised_category_falls_back(self):
        am = AgentMatrix()
        self.assertEqual(am.resolve("totally_new_category"), "basic_worker")

    def test_custom_mapping(self):
        am = AgentMatrix({"special": "special_agent"})
        self.assertEqual(am.resolve("special"), "special_agent")
        self.assertEqual(am.resolve("other"), "basic_worker")


# ---------------------------------------------------------------------------
# Matcher tests
# ---------------------------------------------------------------------------

class TestMatcher(unittest.TestCase):
    def test_echo_task(self):
        category, agent = match({"type": "echo", "payload": {}})
        self.assertEqual(category, "communication")
        self.assertEqual(agent, "basic_worker")

    def test_noop_task(self):
        category, agent = match({"type": "noop"})
        self.assertEqual(category, "maintenance")
        self.assertEqual(agent, "basic_worker")

    def test_unknown_type_falls_back(self):
        category, agent = match({"type": "mystery_type"})
        self.assertEqual(category, "unknown")
        self.assertEqual(agent, "basic_worker")

    def test_missing_type_key_falls_back(self):
        category, agent = match({})
        self.assertEqual(category, "unknown")
        self.assertEqual(agent, "basic_worker")

    def test_returns_tuple_of_two_strings(self):
        result = match({"type": "echo"})
        self.assertIsInstance(result, tuple)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], str)
        self.assertIsInstance(result[1], str)

    def test_non_dict_task_does_not_raise(self):
        category, agent = match(None)
        self.assertEqual(category, "unknown")
        self.assertEqual(agent, "basic_worker")

    def test_unhashable_type_value_does_not_raise(self):
        category, agent = match({"type": ["list", "value"]})
        self.assertEqual(category, "unknown")
        self.assertEqual(agent, "basic_worker")

    def test_classify_task(self):
        category, agent = match({"type": "classify"})
        self.assertEqual(category, "classification")
        self.assertEqual(agent, "basic_worker")


# ---------------------------------------------------------------------------
# Safety layer tests (v0.4)
# ---------------------------------------------------------------------------

class TestTaskWhitelist(unittest.TestCase):
    def test_allowed_types_accepted(self):
        wl = TaskWhitelist()
        for t in ("echo", "noop", "classify"):
            self.assertTrue(wl.is_allowed(t))

    def test_unknown_type_rejected(self):
        wl = TaskWhitelist()
        self.assertFalse(wl.is_allowed("dangerous_op"))
        self.assertFalse(wl.is_allowed(""))
        self.assertFalse(wl.is_allowed("rm -rf"))

    def test_check_returns_true_for_allowed(self):
        wl = TaskWhitelist()
        task = {"id": "abc", "type": "echo"}
        self.assertTrue(wl.check(task))

    def test_check_returns_false_for_rejected(self):
        wl = TaskWhitelist()
        task = {"id": "xyz", "type": "bad_task"}
        self.assertFalse(wl.check(task))

    def test_check_handles_non_dict_task(self):
        wl = TaskWhitelist()
        self.assertFalse(wl.check(None))
        self.assertFalse(wl.check("not a dict"))

    def test_custom_whitelist(self):
        wl = TaskWhitelist(["custom_op"])
        self.assertTrue(wl.is_allowed("custom_op"))
        self.assertFalse(wl.is_allowed("echo"))

    def test_allowed_property_is_frozenset(self):
        wl = TaskWhitelist()
        self.assertIsInstance(wl.allowed, frozenset)


class TestKillSwitch(unittest.TestCase):
    def setUp(self):
        _rm("STOP")

    def tearDown(self):
        _rm("STOP")

    def test_no_stop_file_returns_false(self):
        self.assertFalse(check_kill_switch(_TMPDIR))

    def test_stop_file_returns_true(self):
        stop_path = os.path.join(_TMPDIR, "STOP")
        open(stop_path, "w").close()
        self.assertTrue(check_kill_switch(_TMPDIR))

    def test_consume_removes_stop_file(self):
        stop_path = os.path.join(_TMPDIR, "STOP")
        open(stop_path, "w").close()
        result = consume_kill_switch(_TMPDIR)
        self.assertTrue(result)
        self.assertFalse(os.path.exists(stop_path))

    def test_consume_returns_false_when_no_file(self):
        result = consume_kill_switch(_TMPDIR)
        self.assertFalse(result)

    def test_consume_safe_on_nonexistent_dir(self):
        # Should not raise even if data_dir does not exist
        result = consume_kill_switch("/nonexistent/path/xyz")
        self.assertFalse(result)


class TestOrchestratorKillSwitch(unittest.TestCase):
    def setUp(self):
        _rm("state.json", "queue.json", "task_ledger.json", "agent_ledger.json", "STOP")

    def tearDown(self):
        _rm("STOP")

    def test_stop_file_halts_loop_before_processing(self):
        import genus.queue as _qmod
        # Place STOP file before run
        stop_path = os.path.join(_TMPDIR, "STOP")
        open(stop_path, "w").close()
        # Pre-set bootstrapped so seeding is skipped
        m = Memory()
        m.set("bootstrapped", True)
        # Enqueue a task that should NOT be processed
        q = TaskQueue()
        q.enqueue("echo", {"message": "should-not-run"})
        orc = Orchestrator(tick_delay=0)
        orc.run()
        # Task must still be pending (not processed)
        q2 = TaskQueue()
        self.assertEqual(q2.pending_count(), 1)
        # STOP file must be removed
        self.assertFalse(os.path.exists(stop_path))


class TestOrchestratorWhitelist(unittest.TestCase):
    def setUp(self):
        _rm("state.json", "queue.json", "task_ledger.json", "agent_ledger.json")

    def test_whitelisted_task_is_processed(self):
        m = Memory()
        m.set("bootstrapped", True)
        q = TaskQueue()
        q.enqueue("echo", {"message": "allowed"})
        orc = Orchestrator(tick_delay=0)
        orc.run()
        m2 = Memory()
        self.assertEqual(m2.get("tasks_done"), 1)
        self.assertEqual(m2.get("tasks_failed"), 0)

    def test_non_whitelisted_task_is_rejected(self):
        m = Memory()
        m.set("bootstrapped", True)
        q = TaskQueue()
        q.enqueue("dangerous_op", {"payload": "bad"})
        orc = Orchestrator(tick_delay=0)
        orc.run()
        # Rejected task is marked failed, not done
        q2 = TaskQueue()
        failed = [t for t in q2._queue if t["status"] == "failed"]
        self.assertEqual(len(failed), 1)


class TestQueueWhitelistGuard(unittest.TestCase):
    def setUp(self):
        _rm("queue.json")

    def test_load_from_json_file_rejects_non_whitelisted(self):
        import json as _json
        tasks = [
            {"type": "echo", "payload": {}},
            {"type": "dangerous_op", "payload": {}},
            {"type": "noop"},
        ]
        path = os.path.join(_TMPDIR, "ext_queue_whitelist.json")
        with open(path, "w") as fh:
            _json.dump(tasks, fh)
        wl = TaskWhitelist()
        q = TaskQueue()
        count = q.load_from_json_file(path, whitelist=wl)
        # Only echo and noop should be accepted (dangerous_op rejected)
        self.assertEqual(count, 2)
        self.assertEqual(q.pending_count(), 2)

    def test_load_from_json_file_no_whitelist_accepts_all_valid(self):
        """Without whitelist, original behaviour is preserved."""
        import json as _json
        tasks = [
            {"type": "echo"},
            {"type": "anything_goes"},
        ]
        path = os.path.join(_TMPDIR, "ext_queue_no_wl.json")
        with open(path, "w") as fh:
            _json.dump(tasks, fh)
        q = TaskQueue()
        count = q.load_from_json_file(path)
        self.assertEqual(count, 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
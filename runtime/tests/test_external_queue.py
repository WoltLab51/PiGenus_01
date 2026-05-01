"""Tests for PiGenus v0.5 external task queue feature.

Run with:
    python -m unittest tests/test_external_queue.py   (from runtime/)
    python runtime/tests/test_external_queue.py       (from repo root)
"""

import json
import os
import subprocess
import sys
import tempfile
import unittest

# Make the 'genus' package importable from any working directory.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

# ---------------------------------------------------------------------------
# Redirect all persistence to a temporary directory for isolation.
# ---------------------------------------------------------------------------

_TMP_OBJ = tempfile.TemporaryDirectory(prefix="pigenus_extq_test_")
_TMPDIR = _TMP_OBJ.name


def tearDownModule():
    _TMP_OBJ.cleanup()


def _repatch_to_tmpdir():
    """Re-apply DATA_DIR / QUEUE_FILE patches so our _TMPDIR always wins.

    Both this file and test_genus.py patch the same genus.queue module globals
    at import time.  Whichever file is imported last wins, so we re-patch at
    the start of every test to guarantee isolation.
    """
    import importlib
    for module_name in ("genus.memory", "genus.queue", "genus.ledger", "genus.logger"):
        mod = sys.modules.get(module_name) or importlib.import_module(module_name)
        mod.DATA_DIR = _TMPDIR
        if hasattr(mod, "QUEUE_FILE"):
            mod.QUEUE_FILE = os.path.join(_TMPDIR, "queue.json")
        if hasattr(mod, "STATE_FILE"):
            mod.STATE_FILE = os.path.join(_TMPDIR, "state.json")
        if hasattr(mod, "TASK_LEDGER_FILE"):
            mod.TASK_LEDGER_FILE = os.path.join(_TMPDIR, "task_ledger.json")
        if hasattr(mod, "AGENT_LEDGER_FILE"):
            mod.AGENT_LEDGER_FILE = os.path.join(_TMPDIR, "agent_ledger.json")


# Initial patch at import time.
_repatch_to_tmpdir()

from genus.queue import TaskQueue
from genus.safety import TaskWhitelist
from genus.external_queue import (
    load_external_queue,
    EXT_QUEUE_FILE,
    EXT_QUEUE_PROCESSED,
    EXT_QUEUE_FAILED,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rm(*filenames):
    for name in filenames:
        path = os.path.join(_TMPDIR, name)
        if os.path.exists(path):
            os.remove(path)


def _write_queue(tasks, name=EXT_QUEUE_FILE):
    path = os.path.join(_TMPDIR, name)
    with open(path, "w") as fh:
        json.dump(tasks, fh)
    return path


def _write_raw(content, name=EXT_QUEUE_FILE):
    """Write raw bytes/string without JSON encoding."""
    path = os.path.join(_TMPDIR, name)
    with open(path, "w") as fh:
        fh.write(content)
    return path


# ---------------------------------------------------------------------------
# Base class: re-patches module globals before each test for isolation.
# ---------------------------------------------------------------------------

def _reset_genus_logger():
    """Close and remove all handlers from the 'genus' logger.

    Forces the next ``get_logger()`` call to recreate the FileHandler using
    the current module-level ``DATA_DIR``.  Without this, a FileHandler set up
    during an earlier test (pointing at a different tmpdir) would persist
    indefinitely, causing log writes to go to the wrong directory.
    """
    import logging
    lgr = logging.getLogger("genus")
    for h in list(lgr.handlers):
        h.close()
        lgr.removeHandler(h)


class _Base(unittest.TestCase):
    """Ensures our _TMPDIR is always the active DATA_DIR / QUEUE_FILE.

    Both test_genus.py and this file patch the same genus.queue module globals
    at import time.  Re-patching in setUp guarantees isolation even when both
    test files run in the same pytest process.  The original values are saved
    in setUp and restored in tearDown so that tests in other files are not
    affected.

    The 'genus' logger's FileHandler is also reset so that log entries written
    during these tests are directed at the current _TMPDIR rather than whatever
    directory an earlier test file had configured.
    """

    def setUp(self):
        # Save current module globals so tearDown can restore them.
        import genus.queue as _qmod
        import genus.memory as _mmod
        import genus.logger as _lgmod
        import genus.ledger as _ldmod
        self._saved = {
            "queue.DATA_DIR": _qmod.DATA_DIR,
            "queue.QUEUE_FILE": _qmod.QUEUE_FILE,
            "memory.DATA_DIR": _mmod.DATA_DIR,
            "memory.STATE_FILE": _mmod.STATE_FILE,
            "logger.DATA_DIR": _lgmod.DATA_DIR,
            "ledger.DATA_DIR": _ldmod.DATA_DIR,
            "ledger.TASK_LEDGER_FILE": _ldmod.TASK_LEDGER_FILE,
            "ledger.AGENT_LEDGER_FILE": _ldmod.AGENT_LEDGER_FILE,
        }
        # Reset the logger so it rebuilds its FileHandler after re-patching.
        _reset_genus_logger()
        _repatch_to_tmpdir()
        _rm(EXT_QUEUE_FILE, EXT_QUEUE_PROCESSED, EXT_QUEUE_FAILED, "queue.json")

    def tearDown(self):
        import genus.queue as _qmod
        import genus.memory as _mmod
        import genus.logger as _lgmod
        import genus.ledger as _ldmod
        _qmod.DATA_DIR = self._saved["queue.DATA_DIR"]
        _qmod.QUEUE_FILE = self._saved["queue.QUEUE_FILE"]
        _mmod.DATA_DIR = self._saved["memory.DATA_DIR"]
        _mmod.STATE_FILE = self._saved["memory.STATE_FILE"]
        _lgmod.DATA_DIR = self._saved["logger.DATA_DIR"]
        _ldmod.DATA_DIR = self._saved["ledger.DATA_DIR"]
        _ldmod.TASK_LEDGER_FILE = self._saved["ledger.TASK_LEDGER_FILE"]
        _ldmod.AGENT_LEDGER_FILE = self._saved["ledger.AGENT_LEDGER_FILE"]
        # Reset the logger again so subsequent tests (e.g. test_genus.py) get
        # a fresh FileHandler pointing at their own data directory.
        _reset_genus_logger()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestLoadValidQueue(_Base):
    """Loading a well-formed queue file with supported task types."""

    def test_loads_echo_task(self):
        _write_queue([{"type": "echo", "payload": {"message": "hi"}}])
        q = TaskQueue()
        count = load_external_queue(_TMPDIR, q, whitelist=TaskWhitelist())
        self.assertEqual(count, 1)
        self.assertEqual(q.pending_count(), 1)

    def test_loads_noop_task(self):
        _write_queue([{"type": "noop"}])
        q = TaskQueue()
        count = load_external_queue(_TMPDIR, q, whitelist=TaskWhitelist())
        self.assertEqual(count, 1)

    def test_loads_classify_task(self):
        _write_queue([{"type": "classify", "payload": {"task_type": "some_text"}}])
        q = TaskQueue()
        count = load_external_queue(_TMPDIR, q, whitelist=TaskWhitelist())
        self.assertEqual(count, 1)

    def test_loads_multiple_tasks(self):
        _write_queue([
            {"type": "echo", "payload": {"message": "a"}},
            {"type": "noop"},
            {"type": "classify", "payload": {"task_type": "x"}},
        ])
        q = TaskQueue()
        count = load_external_queue(_TMPDIR, q, whitelist=TaskWhitelist())
        self.assertEqual(count, 3)
        self.assertEqual(q.pending_count(), 3)

    def test_returns_zero_when_file_absent(self):
        # No file written; should return 0 without error.
        q = TaskQueue()
        count = load_external_queue(_TMPDIR, q)
        self.assertEqual(count, 0)


class TestLoadMixedValidInvalidTasks(_Base):
    """Mixed queue: some tasks accepted, some rejected."""

    def test_unknown_type_is_rejected(self):
        _write_queue([
            {"type": "echo", "payload": {"message": "ok"}},
            {"type": "dangerous_op"},
        ])
        q = TaskQueue()
        count = load_external_queue(_TMPDIR, q, whitelist=TaskWhitelist())
        self.assertEqual(count, 1)
        self.assertEqual(q.pending_count(), 1)

    def test_task_without_type_is_skipped(self):
        _write_queue([
            {"payload": "no type here"},
            {"type": "noop"},
        ])
        q = TaskQueue()
        count = load_external_queue(_TMPDIR, q, whitelist=TaskWhitelist())
        self.assertEqual(count, 1)

    def test_non_dict_item_is_skipped(self):
        _write_queue([
            "just a string",
            {"type": "noop"},
        ])
        q = TaskQueue()
        count = load_external_queue(_TMPDIR, q, whitelist=TaskWhitelist())
        self.assertEqual(count, 1)

    def test_all_invalid_returns_zero(self):
        _write_queue([
            {"type": "evil"},
            {"type": "dangerous_op"},
        ])
        q = TaskQueue()
        count = load_external_queue(_TMPDIR, q, whitelist=TaskWhitelist())
        self.assertEqual(count, 0)


class TestLoadMalformedFile(_Base):
    """Malformed JSON must never crash the loader."""

    def test_malformed_json_returns_zero(self):
        _write_raw("{this is not valid json!!!")
        q = TaskQueue()
        count = load_external_queue(_TMPDIR, q)
        self.assertEqual(count, 0)

    def test_malformed_json_does_not_raise(self):
        _write_raw("null\x00\xff")
        q = TaskQueue()
        try:
            load_external_queue(_TMPDIR, q)
        except Exception as exc:
            self.fail(f"load_external_queue raised unexpectedly: {exc}")

    def test_non_list_json_returns_zero(self):
        _write_raw('{"type": "echo"}')  # dict, not list
        q = TaskQueue()
        count = load_external_queue(_TMPDIR, q)
        self.assertEqual(count, 0)


class TestUnknownTaskTypesRejected(_Base):
    """Unknown task types must be rejected without crashing."""

    def test_unknown_type_not_enqueued(self):
        _write_queue([{"type": "rm_rf"}, {"type": "sudo_shutdown"}])
        q = TaskQueue()
        count = load_external_queue(_TMPDIR, q, whitelist=TaskWhitelist())
        self.assertEqual(count, 0)
        self.assertEqual(q.pending_count(), 0)

    def test_custom_whitelist_accepts_only_listed(self):
        _write_queue([
            {"type": "allowed_type"},
            {"type": "banned_type"},
        ])
        q = TaskQueue()
        wl = TaskWhitelist(allowed=["allowed_type"])
        count = load_external_queue(_TMPDIR, q, whitelist=wl)
        self.assertEqual(count, 1)


class TestArchiveProcessed(_Base):
    """Successful load archives the file as external_queue.processed.json."""

    def test_processed_file_created_after_success(self):
        _write_queue([{"type": "noop"}])
        q = TaskQueue()
        load_external_queue(_TMPDIR, q, whitelist=TaskWhitelist())
        self.assertTrue(
            os.path.exists(os.path.join(_TMPDIR, EXT_QUEUE_PROCESSED)),
            "expected external_queue.processed.json to exist",
        )

    def test_original_file_removed_after_success(self):
        _write_queue([{"type": "noop"}])
        q = TaskQueue()
        load_external_queue(_TMPDIR, q, whitelist=TaskWhitelist())
        self.assertFalse(
            os.path.exists(os.path.join(_TMPDIR, EXT_QUEUE_FILE)),
            "expected external_queue.json to be gone",
        )

    def test_failed_file_absent_after_success(self):
        _write_queue([{"type": "noop"}])
        q = TaskQueue()
        load_external_queue(_TMPDIR, q, whitelist=TaskWhitelist())
        self.assertFalse(
            os.path.exists(os.path.join(_TMPDIR, EXT_QUEUE_FAILED))
        )


class TestArchiveFailed(_Base):
    """Malformed or all-rejected load archives the file as external_queue.failed.json."""

    def test_failed_file_created_after_malformed(self):
        _write_raw("not json at all")
        q = TaskQueue()
        load_external_queue(_TMPDIR, q)
        self.assertTrue(
            os.path.exists(os.path.join(_TMPDIR, EXT_QUEUE_FAILED)),
            "expected external_queue.failed.json to exist",
        )

    def test_failed_file_created_when_all_rejected(self):
        _write_queue([{"type": "bad_type"}, {"type": "another_bad"}])
        q = TaskQueue()
        load_external_queue(_TMPDIR, q, whitelist=TaskWhitelist())
        self.assertTrue(
            os.path.exists(os.path.join(_TMPDIR, EXT_QUEUE_FAILED))
        )

    def test_processed_file_absent_after_failure(self):
        _write_raw("not json at all")
        q = TaskQueue()
        load_external_queue(_TMPDIR, q)
        self.assertFalse(
            os.path.exists(os.path.join(_TMPDIR, EXT_QUEUE_PROCESSED))
        )

    def test_original_file_removed_after_failure(self):
        _write_raw("not json at all")
        q = TaskQueue()
        load_external_queue(_TMPDIR, q)
        self.assertFalse(
            os.path.exists(os.path.join(_TMPDIR, EXT_QUEUE_FILE))
        )


class TestSubmitTaskCLI(_Base):
    """Tests for the submit_task.py CLI helper."""

    # Path to the CLI script
    _SCRIPT = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..", "submit_task.py",
    )

    def _call_submit(self, message):
        """Invoke submit_echo_task directly (no subprocess) using the tmpdir."""
        # Import function directly to keep tests fast and isolated.
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "submit_task", self._SCRIPT
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod.submit_echo_task(message, data_dir=_TMPDIR)

    def test_creates_queue_file(self):
        self._call_submit("hello world")
        self.assertTrue(
            os.path.exists(os.path.join(_TMPDIR, "external_queue.json"))
        )

    def test_queue_file_contains_echo_task(self):
        self._call_submit("test message")
        with open(os.path.join(_TMPDIR, "external_queue.json")) as fh:
            tasks = json.load(fh)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["type"], "echo")
        self.assertEqual(tasks[0]["payload"]["message"], "test message")

    def test_appends_to_existing_file(self):
        self._call_submit("first")
        self._call_submit("second")
        with open(os.path.join(_TMPDIR, "external_queue.json")) as fh:
            tasks = json.load(fh)
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0]["payload"]["message"], "first")
        self.assertEqual(tasks[1]["payload"]["message"], "second")

    def test_direct_submit_writes_expected_task(self):
        """Successful submission should write the expected task into the tmpdir."""
        self._call_submit("subprocess test")
        with open(os.path.join(_TMPDIR, "external_queue.json")) as fh:
            tasks = json.load(fh)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["type"], "echo")
        self.assertEqual(tasks[0]["payload"]["message"], "subprocess test")

    def test_subprocess_no_args_exits_nonzero(self):
        result = subprocess.run(
            [sys.executable, self._SCRIPT],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(result.returncode, 0)

    def test_appends_even_when_existing_file_is_malformed(self):
        """If external_queue.json is malformed, start fresh rather than crash."""
        bad_path = os.path.join(_TMPDIR, "external_queue.json")
        with open(bad_path, "w") as fh:
            fh.write("not json")
        self._call_submit("recovery")
        with open(bad_path) as fh:
            tasks = json.load(fh)
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0]["payload"]["message"], "recovery")


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""External task queue loader for PiGenus v0.5.

Handles loading, validating, enqueuing, and archiving of tasks from an
externally supplied ``external_queue.json`` file.  Each concern is kept in
its own function so the module stays small and testable.

File lifecycle
--------------
* ``external_queue.json``           – input file placed by external tooling
* ``external_queue.processed.json`` – archived here when ≥1 task was accepted
* ``external_queue.failed.json``    – archived here when the file was malformed
                                       or yielded zero accepted tasks

Never raises; all exceptions are caught and logged.
"""

import json
import os

from .logger import get_logger

logger = get_logger()

EXT_QUEUE_FILE = "external_queue.json"
EXT_QUEUE_PROCESSED = "external_queue.processed.json"
EXT_QUEUE_FAILED = "external_queue.failed.json"

# Maximum characters to show from a payload value in log messages.
_MAX_PAYLOAD_LOG = 80


def _truncate(value, max_len: int = _MAX_PAYLOAD_LOG) -> str:
    """Return a shortened repr of *value* for safe log output."""
    s = repr(value)
    return s if len(s) <= max_len else s[:max_len] + "…"


def _archive(src: str, dest: str) -> None:
    """Rename *src* to *dest*, logging the outcome.  Never raises."""
    try:
        os.replace(src, dest)
        logger.info("ExternalQueue | archived %s → %s", src, dest)
    except OSError as exc:
        logger.warning(
            "ExternalQueue | could not archive %s → %s: %s", src, dest, exc
        )


def load_external_queue(data_dir: str, queue, whitelist=None) -> int:
    """Load ``external_queue.json`` from *data_dir*, validate, enqueue, archive.

    Parameters
    ----------
    data_dir:
        Directory that contains (or will contain) ``external_queue.json``.
    queue:
        A ``TaskQueue`` instance whose ``enqueue`` method will be called for
        each accepted task.
    whitelist:
        Optional ``TaskWhitelist`` instance.  When provided, tasks whose
        ``type`` is not in the whitelist are rejected (logged) and skipped.

    Returns
    -------
    int
        Number of tasks successfully enqueued.  Returns 0 when the file does
        not exist, cannot be parsed, or contains no valid tasks.
    """
    ext_path = os.path.join(data_dir, EXT_QUEUE_FILE)

    if not os.path.exists(ext_path):
        return 0

    # --- Parse JSON -------------------------------------------------------
    try:
        with open(ext_path, "r") as fh:
            data = json.load(fh)
    except Exception as exc:
        logger.warning(
            "ExternalQueue | failed to parse %s: %s", ext_path, exc
        )
        _archive(ext_path, os.path.join(data_dir, EXT_QUEUE_FAILED))
        return 0

    if not isinstance(data, list):
        logger.warning(
            "ExternalQueue | expected a JSON array in %s, got %s",
            ext_path, type(data).__name__,
        )
        _archive(ext_path, os.path.join(data_dir, EXT_QUEUE_FAILED))
        return 0

    logger.info(
        "ExternalQueue | loaded %s with %d item(s)", ext_path, len(data)
    )

    # --- Validate and enqueue tasks ---------------------------------------
    accepted = 0
    for item in data:
        if not isinstance(item, dict):
            logger.warning(
                "ExternalQueue | REJECTED non-dict item: %s", _truncate(item)
            )
            continue

        task_type = item.get("type", "")
        if not task_type:
            logger.warning(
                "ExternalQueue | REJECTED task without 'type': %s",
                _truncate(item),
            )
            continue

        task_type = str(task_type)

        if whitelist is not None and not whitelist.is_allowed(task_type):
            logger.warning(
                "ExternalQueue | REJECTED type=%r — not in whitelist", task_type
            )
            continue

        payload = item.get("payload")
        payload_dict = payload if isinstance(payload, dict) else {}

        queue.enqueue(task_type, payload_dict)
        logger.info(
            "ExternalQueue | ACCEPTED type=%r payload=%s",
            task_type, _truncate(payload),
        )
        accepted += 1

    # --- Archive ---------------------------------------------------------
    dest_name = EXT_QUEUE_PROCESSED if accepted > 0 else EXT_QUEUE_FAILED
    _archive(ext_path, os.path.join(data_dir, dest_name))

    logger.info(
        "ExternalQueue | accepted %d / %d task(s) from external queue",
        accepted, len(data),
    )
    return accepted

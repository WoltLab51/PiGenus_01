#!/usr/bin/env python3
"""CLI helper to submit a task to the PiGenus external queue.

Usage::

    python3 runtime/submit_task.py "hello world"

Writes a single ``echo`` task to ``runtime/data/external_queue.json``.
If the file already exists its contents are preserved and the new task is
appended to the list.

stdlib only — no third-party dependencies.
"""

import json
import os
import sys


def _data_dir() -> str:
    """Return the absolute path to the runtime/data/ directory."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def submit_echo_task(message: str, data_dir: str = None) -> str:
    """Append an echo task for *message* to external_queue.json.

    Parameters
    ----------
    message:
        The text payload for the echo task.
    data_dir:
        Directory where ``external_queue.json`` will be written.
        Defaults to ``runtime/data/``.

    Returns
    -------
    str
        Absolute path of the queue file that was written.
    """
    if data_dir is None:
        data_dir = _data_dir()

    os.makedirs(data_dir, exist_ok=True)
    queue_path = os.path.join(data_dir, "external_queue.json")

    tasks = []
    if os.path.exists(queue_path):
        try:
            with open(queue_path, "r") as fh:
                existing = json.load(fh)
            if isinstance(existing, list):
                tasks = existing
        except Exception:
            # Malformed file — start fresh rather than crashing.
            tasks = []

    tasks.append({"type": "echo", "payload": {"message": message}})

    with open(queue_path, "w") as fh:
        json.dump(tasks, fh, indent=2)

    return queue_path


def main():
    if len(sys.argv) < 2:
        print("Usage: submit_task.py <message>", file=sys.stderr)
        sys.exit(1)

    message = sys.argv[1]
    queue_path = submit_echo_task(message)
    print(f"Task submitted: echo '{message}'")
    print(f"Queue file: {queue_path}")


if __name__ == "__main__":
    main()

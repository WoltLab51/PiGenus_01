"""PiGenus v0.4 – entrypoint.

Run from the runtime/ directory (or any location):
    python run_genus.py

Persistence files are written to runtime/data/:
    state.json        – persistent key-value memory
    queue.json        – task queue
    task_ledger.json  – record of completed / failed tasks
    agent_ledger.json – record of agent activity
    events.log        – timestamped event log
"""

import sys
import os

# Ensure the runtime/ directory is on the path so 'genus' is importable
# regardless of the working directory from which this script is invoked.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from genus.orchestrator import Orchestrator


def main():
    print("PiGenus v0.4 starting...")
    orchestrator = Orchestrator()
    orchestrator.run()


if __name__ == "__main__":
    main()

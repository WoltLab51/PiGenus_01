"""Event logger for PiGenus.

Writes timestamped entries to data/events.log and to stdout.
"""

import logging
import os

# Resolve data/ relative to this file's parent directory (runtime/)
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")


def get_logger(name: str = "genus") -> logging.Logger:
    """Return a logger that writes to data/events.log and stdout."""
    os.makedirs(DATA_DIR, exist_ok=True)
    log_path = os.path.join(DATA_DIR, "events.log")

    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

        fh = logging.FileHandler(log_path)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    return logger

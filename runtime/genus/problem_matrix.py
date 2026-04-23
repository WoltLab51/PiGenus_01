"""Problem Matrix for PiGenus v0.1.

Maps task type strings to problem categories.  Unknown task types fall back
to the ``"unknown"`` category so the rest of the pipeline never crashes on
unexpected input.
"""

# Default mapping: task_type → problem_category
_DEFAULT_MAPPING: dict = {
    "echo": "communication",
    "noop": "maintenance",
}

_FALLBACK_CATEGORY = "unknown"


class ProblemMatrix:
    """Maps a task ``type`` string to a *problem_category*.

    Parameters
    ----------
    mapping:
        Optional override for the built-in type→category table.  When
        *None* the module-level default mapping is used.
    """

    def __init__(self, mapping: dict = None):
        self._mapping = mapping if mapping is not None else _DEFAULT_MAPPING

    def categorize(self, task_type: str) -> str:
        """Return the problem category for *task_type*.

        Falls back to ``"unknown"`` for any unrecognised type.
        """
        return self._mapping.get(task_type, _FALLBACK_CATEGORY)

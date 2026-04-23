"""Agent Matrix for PiGenus v0.1.

Maps problem categories to agent names.  Unknown categories fall back to
``"basic_worker"`` so the pipeline always has a runnable agent.
"""

# Default mapping: problem_category → agent_name
_DEFAULT_MAPPING: dict = {
    "communication": "basic_worker",
    "maintenance": "basic_worker",
    "unknown": "basic_worker",
}

_FALLBACK_AGENT = "basic_worker"


class AgentMatrix:
    """Maps a *problem_category* to an *agent_name*.

    Parameters
    ----------
    mapping:
        Optional override for the built-in category→agent table.  When
        *None* the module-level default mapping is used.
    """

    def __init__(self, mapping: dict = None):
        self._mapping = mapping if mapping is not None else _DEFAULT_MAPPING

    def resolve(self, problem_category: str) -> str:
        """Return the agent name for *problem_category*.

        Falls back to ``"basic_worker"`` for any unrecognised category.
        """
        return self._mapping.get(problem_category, _FALLBACK_AGENT)

"""Matcher for PiGenus v0.1.

Combines ProblemMatrix and AgentMatrix to resolve a task dict to a
``(problem_category, agent_name)`` pair.

This module is intentionally side-effect free (no I/O) so it can be used
and tested without any filesystem access.
"""

from .problem_matrix import ProblemMatrix
from .agent_matrix import AgentMatrix

# Module-level singleton instances using default mappings.
_problem_matrix = ProblemMatrix()
_agent_matrix = AgentMatrix()


def match(task: dict) -> tuple:
    """Resolve *task* to ``(problem_category, agent_name)``.

    Parameters
    ----------
    task:
        Task dict; should contain a ``"type"`` key.  A missing key is
        treated as an empty string and categorised as ``"unknown"``.
        Non-dict values and unhashable/non-string ``"type"`` values are
        handled gracefully – the function never raises.

    Returns
    -------
    tuple[str, str]:
        ``(problem_category, agent_name)`` – never raises, always returns a
        valid pair thanks to the fallback defaults in both matrices.
    """
    if not isinstance(task, dict):
        task = {}
    raw_type = task.get("type", "")
    task_type = raw_type if isinstance(raw_type, str) else ""
    category = _problem_matrix.categorize(task_type)
    agent_name = _agent_matrix.resolve(category)
    return category, agent_name

"""Whether an answer fits its question: the one check agent-runs makes before a paused run
continues, and the harness makes for a run it keeps in its own process (no agent-runs).

* :func:`schema_problem`: ``expects`` is a JSON Schema at all (checked when the question is
  asked, so a malformed question fails where it was written, not when someone answers it).
* :func:`answer_problem`: a resolution answers the interrupt, an ``ANSWER`` fits ``expects``
  (else is one of ``options``), and the corrected value of an ``EDIT`` of a question fits
  ``expects``. ``APPROVE``, ``REJECT`` and ``CANCEL`` carry no value to check. A tool call's
  edited arguments are checked against the tool's own schema by whoever knows the tool (the
  harness), since the interrupt does not carry it.

Both return why, in words, or ``None``.
"""

from __future__ import annotations

from typing import Any

from jsonschema import SchemaError
from jsonschema.exceptions import best_match
from jsonschema.validators import validator_for
from trellis.contracts.runs import Interrupt, InterruptDecision, InterruptResolution


def schema_problem(expects: dict[str, Any] | None) -> str | None:
    """Why ``expects`` is not a JSON Schema, or ``None`` when it is (or there is none)."""
    if expects is None:
        return None
    try:
        validator_for(expects).check_schema(expects)
    except SchemaError as error:
        return f"expects is not a valid JSON Schema: {error.message}"
    return None


def answer_problem(interrupt: Interrupt, resolution: InterruptResolution) -> str | None:
    """Why ``resolution`` cannot answer ``interrupt``, or ``None`` when it can."""
    if not resolution.resolves(interrupt):
        return f"it answers {resolution.interrupt_id}, not {interrupt.interrupt_id}"
    if resolution.decision is InterruptDecision.ANSWER:
        return _fit_problem(interrupt, resolution.answer, choosing=True)
    if resolution.decision is InterruptDecision.EDIT and interrupt.tool_call is None:
        return _fit_problem(interrupt, resolution.payload, choosing=False)
    return None


def _fit_problem(interrupt: Interrupt, value: Any, *, choosing: bool) -> str | None:
    expects = interrupt.expects
    if expects is not None:
        if problem := schema_problem(expects):
            return problem
        error = best_match(validator_for(expects)(expects).iter_errors(value))
        if error is None:
            return None
        where = "".join(f"[{part!r}]" for part in error.absolute_path)
        return f"the answer{where} does not fit what was asked: {error.message}"
    if choosing and interrupt.options and value not in interrupt.options:
        return f"the answer {value!r} is not one of the options {interrupt.options}"
    return None

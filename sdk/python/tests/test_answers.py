"""trellis.runs.answers: the one check of an answer against its question."""

from __future__ import annotations

from typing import Any

import pytest
from trellis.contracts.runs import (
    Interrupt,
    InterruptDecision,
    InterruptReason,
    InterruptResolution,
)
from trellis.contracts.tool import ToolCall
from trellis.runs.answers import answer_problem, schema_problem


def _asked(**fields: Any) -> Interrupt:
    fields.setdefault("question", "ok?")
    return Interrupt(tenant_id="acme", run_id="run_1", **fields)


def _answer(
    interrupt: Interrupt, decision: InterruptDecision, **fields: Any
) -> InterruptResolution:
    return InterruptResolution(
        interrupt_id=interrupt.interrupt_id, run_id=interrupt.run_id, decision=decision, **fields
    )


SQL = {
    "type": "object",
    "properties": {"sql": {"type": "string"}, "rows": {"type": "integer", "maximum": 100}},
    "required": ["sql"],
}


def test_an_answer_must_fit_what_was_asked() -> None:
    asked = _asked(ui="form", expects=SQL)
    fits = _answer(asked, InterruptDecision.ANSWER, answer={"sql": "x"})
    assert answer_problem(asked, fits) is None
    missing = _answer(asked, InterruptDecision.ANSWER, answer={"rows": 3})
    assert answer_problem(asked, missing) == (
        "the answer does not fit what was asked: 'sql' is a required property"
    )
    too_many = _answer(asked, InterruptDecision.ANSWER, answer={"sql": "x", "rows": 500})
    assert answer_problem(asked, too_many) == (
        "the answer['rows'] does not fit what was asked: 500 is greater than the maximum of 100"
    )


@pytest.mark.parametrize(
    "decision", [InterruptDecision.APPROVE, InterruptDecision.REJECT, InterruptDecision.CANCEL]
)
def test_approve_reject_and_cancel_carry_no_value_to_check(decision: InterruptDecision) -> None:
    asked = _asked(ui="form", expects=SQL)
    assert answer_problem(asked, _answer(asked, decision)) is None


def test_an_answer_to_a_choice_is_one_of_its_options() -> None:
    asked = _asked(reason=InterruptReason.CHOICE, ui="choice", options=["ACME", "Globex"])
    assert answer_problem(asked, _answer(asked, InterruptDecision.ANSWER, answer="ACME")) is None
    wrong = _answer(asked, InterruptDecision.ANSWER, answer="Initech")
    assert answer_problem(asked, wrong) == (
        "the answer 'Initech' is not one of the options ['ACME', 'Globex']"
    )


def test_a_free_answer_without_expects_or_options_is_any_value() -> None:
    asked = _asked(ui="form")
    assert answer_problem(asked, _answer(asked, InterruptDecision.ANSWER, answer=[1, 2])) is None


def test_a_correction_to_a_review_fits_what_was_asked() -> None:
    schema = {"type": "object", "properties": {"amount": {"type": "number"}}}
    asked = _asked(reason=InterruptReason.REVIEW, ui="table", expects=schema)
    fits = _answer(asked, InterruptDecision.EDIT, payload={"amount": 12.5})
    assert answer_problem(asked, fits) is None
    wrong = _answer(asked, InterruptDecision.EDIT, payload={"amount": "lots"})
    assert answer_problem(asked, wrong) == (
        "the answer['amount'] does not fit what was asked: 'lots' is not of type 'number'"
    )


def test_an_edit_of_a_choice_is_not_held_to_its_options() -> None:
    asked = _asked(reason=InterruptReason.CHOICE, ui="choice", options=["a"])
    assert answer_problem(asked, _answer(asked, InterruptDecision.EDIT, payload={"x": 1})) is None


def test_a_tool_calls_edited_arguments_are_left_to_whoever_knows_the_tool() -> None:
    asked = _asked(
        reason=InterruptReason.APPROVAL, tool_call=ToolCall(tool="refund", idempotency_key="k")
    )
    edit = _answer(asked, InterruptDecision.EDIT, payload={"amount": "anything"})
    assert answer_problem(asked, edit) is None
    # a free answer to an approval (the tool does not run, the model reads it) is any value
    assert answer_problem(asked, _answer(asked, InterruptDecision.ANSWER, answer=42)) is None


def test_an_answer_to_another_interrupt_is_refused() -> None:
    asked = _asked()
    other = InterruptResolution(
        interrupt_id="int_other", run_id=asked.run_id, decision=InterruptDecision.ANSWER
    )
    assert answer_problem(asked, other) == f"it answers int_other, not {asked.interrupt_id}"


def test_expects_must_be_a_json_schema() -> None:
    assert schema_problem(None) is None
    assert schema_problem(SQL) is None
    assert schema_problem({"type": "not-a-type"}) == (
        "expects is not a valid JSON Schema: 'not-a-type' is not valid under any of the given "
        "schemas"
    )
    # an interrupt stored with a malformed schema is refused at answer time, saying why
    asked = _asked(ui="form", expects={"type": "not-a-type"})
    problem = answer_problem(asked, _answer(asked, InterruptDecision.ANSWER, answer=1))
    assert problem is not None and problem.startswith("expects is not a valid JSON Schema")

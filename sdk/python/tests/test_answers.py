"""trellis.runs.answers: the one check of an answer against its question."""

from __future__ import annotations

import subprocess
import sys
from typing import Any

import pytest
from trellis.contracts.runs import (
    Interrupt,
    InterruptDecision,
    InterruptReason,
    InterruptResolution,
    Option,
)
from trellis.contracts.tool import ToolCall
from trellis.runs.answers import ANSWER_FORMATS, answer_problem, schema_problem


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


DATE = {"type": "string", "format": "date"}


@pytest.mark.parametrize("value", ["2026-11-14", "2024-02-29"])
def test_a_date_question_takes_an_iso_8601_calendar_date(value: str) -> None:
    """F-DATE: a ``date`` answer is an ISO 8601 calendar date (``YYYY-MM-DD``)."""
    asked = _asked(expects=DATE)
    assert answer_problem(asked, _answer(asked, InterruptDecision.ANSWER, answer=value)) is None


@pytest.mark.parametrize("value", ["tomorrow", "2026-13-40", "", "2025-02-29", "20261114"])
def test_a_date_question_refuses_an_answer_that_is_not_a_date(value: str) -> None:
    """F-DATE: anything else is refused, as any misfit is (the format is checked, not only
    the type)."""
    asked = _asked(expects=DATE)
    refused = answer_problem(asked, _answer(asked, InterruptDecision.ANSWER, answer=value))
    assert refused == f"the answer does not fit what was asked: {value!r} is not a 'date'"


def test_a_date_inside_a_form_is_checked_and_its_neighbours_keep_working() -> None:
    """The format is checked wherever it sits; the other types are checked as before."""
    form = {
        "type": "object",
        "properties": {"day": DATE, "note": {"type": "string"}, "count": {"type": "integer"}},
    }
    asked = _asked(ui="form", expects=form)
    fits = {"day": "2026-11-14", "note": "tomorrow", "count": 3}
    assert answer_problem(asked, _answer(asked, InterruptDecision.ANSWER, answer=fits)) is None
    late = _answer(asked, InterruptDecision.ANSWER, answer={**fits, "day": "next week"})
    assert answer_problem(asked, late) == (
        "the answer['day'] does not fit what was asked: 'next week' is not a 'date'"
    )
    wrong = _answer(asked, InterruptDecision.ANSWER, answer={**fits, "count": "3"})
    assert answer_problem(asked, wrong) == (
        "the answer['count'] does not fit what was asked: '3' is not of type 'integer'"
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
    elsewhere = InterruptResolution(
        interrupt_id=asked.interrupt_id, run_id="run_other", decision=InterruptDecision.ANSWER
    )
    assert answer_problem(asked, elsewhere) == "it answers run run_other, not run_1"


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


def test_an_answer_picks_an_options_value_never_its_label() -> None:
    asked = _asked(
        reason=InterruptReason.CHOICE,
        ui="choice",
        options=["ACME", Option(value="globex", label="Globex GmbH")],
    )
    assert answer_problem(asked, _answer(asked, InterruptDecision.ANSWER, answer="globex")) is None
    assert answer_problem(asked, _answer(asked, InterruptDecision.ANSWER, answer="ACME")) is None
    label = _answer(asked, InterruptDecision.ANSWER, answer="Globex GmbH")
    assert answer_problem(asked, label) == (
        "the answer 'Globex GmbH' is not one of the options ['ACME', 'globex']"
    )


def test_several_picks_are_a_list_of_distinct_option_values() -> None:
    asked = _asked(ui="choice", options=["a", Option(value="b"), "c"], multiple=True)

    def problem(answer: Any) -> str | None:
        return answer_problem(asked, _answer(asked, InterruptDecision.ANSWER, answer=answer))

    assert problem(["a", "c"]) is None
    assert problem([]) is None
    assert problem("a") == "the answer 'a' is not a list of the options ['a', 'b', 'c']"
    assert problem(["a", "z", 1]) == (
        "the answer picks ['z', 1], which are not among the options ['a', 'b', 'c']"
    )
    assert problem(["b", "b"]) == "the answer picks an option more than once"


def test_several_picks_described_by_expects_fit_expects() -> None:
    listed = {"type": "array", "items": {"type": "integer"}, "uniqueItems": True}
    asked = _asked(ui="form", expects=listed, multiple=True)
    assert answer_problem(asked, _answer(asked, InterruptDecision.ANSWER, answer=[1, 2])) is None
    wrong = _answer(asked, InterruptDecision.ANSWER, answer=[1, 1])
    assert answer_problem(asked, wrong) == (
        "the answer does not fit what was asked: [1, 1] has non-unique elements"
    )


def test_an_answer_from_the_askers_own_screen_still_fits_expects() -> None:
    asked = _asked(component="refund-review", props={"order": 91}, expects=SQL)
    wrong = _answer(asked, InterruptDecision.ANSWER, answer={"rows": 1})
    assert answer_problem(asked, wrong) == (
        "the answer does not fit what was asked: 'sql' is a required property"
    )
    free = _asked(component="refund-review")
    assert answer_problem(free, _answer(free, InterruptDecision.ANSWER, answer={"x": 1})) is None


@pytest.mark.parametrize("remember", ["run", "always"])
def test_only_an_approval_of_a_tool_call_is_remembered_beyond_its_call(remember: str) -> None:
    call = _asked(reason=InterruptReason.APPROVAL, tool_call=ToolCall(tool="refund"))
    remembered = _answer(call, InterruptDecision.APPROVE, remember=remember, comment="ok today")
    assert answer_problem(call, remembered) is None
    question = _asked()
    assert answer_problem(
        question, _answer(question, InterruptDecision.APPROVE, remember=remember)
    ) == ("only an approval of a tool call is remembered beyond its call")


#: Each enforced format: a value it takes, and one it refuses.
FORMATS = {
    "date": ("2026-11-14", "2026-13-40"),
    "email": ("ada@example.com", "ada"),
    "ipv4": ("10.0.0.1", "10.0.0.256"),
    "ipv6": ("::1", "::g"),
    "regex": ("^a+$", "("),
    "uuid": ("12345678-1234-5678-1234-567812345678", "1234"),
}


def test_the_enforced_formats_are_one_named_set() -> None:
    assert sorted(ANSWER_FORMATS) == sorted(FORMATS)


@pytest.mark.parametrize("name", sorted(FORMATS))
def test_each_enforced_format_takes_a_good_value_and_refuses_a_bad_one(name: str) -> None:
    good, bad = FORMATS[name]
    asked = _asked(expects={"type": "string", "format": name})
    assert answer_problem(asked, _answer(asked, InterruptDecision.ANSWER, answer=good)) is None
    refused = answer_problem(asked, _answer(asked, InterruptDecision.ANSWER, answer=bad))
    assert refused == f"the answer does not fit what was asked: {bad!r} is not a {name!r}"


@pytest.mark.parametrize("name", ["date-time", "time", "idn-hostname", "hostname", "uri"])
def test_a_format_outside_the_set_is_any_string(name: str) -> None:
    """Not checked anywhere, so no server refuses what another takes (``idn-hostname`` is
    checked by jsonschema when ``idna`` is installed, as it is here)."""
    asked = _asked(expects={"type": "string", "format": name})
    assert answer_problem(asked, _answer(asked, InterruptDecision.ANSWER, answer="!! no")) is None
    wrong_type = answer_problem(asked, _answer(asked, InterruptDecision.ANSWER, answer=3))
    assert wrong_type == "the answer does not fit what was asked: 3 is not of type 'string'"


#: The optional libraries jsonschema's other format checkers import.
OPTIONAL = (
    "fqdn",
    "idna",
    "rfc3987",
    "rfc3986_validator",
    "rfc3987_syntax",
    "rfc3339_validator",
    "webcolors",
    "jsonpointer",
    "uri_template",
    "isoduration",
)

#: Run in a fresh interpreter: the libraries made absent (``absent``) or present (a stand-in
#: ``rfc3339_validator`` that refuses anything) before jsonschema is imported; prints the set
#: enforced and whether a nonsense date-time and a bad date are refused.
PROBE = """
import sys, types
import httpx  # (it needs idna itself; jsonschema is not imported yet)
assert "jsonschema" not in sys.modules
if sys.argv[1] == "absent":
    for name in {optional!r}:
        sys.modules[name] = None
else:
    stand_in = types.ModuleType("rfc3339_validator")
    stand_in.validate_rfc3339 = lambda value: False
    sys.modules["rfc3339_validator"] = stand_in
from trellis.contracts.runs import Interrupt, InterruptDecision, InterruptResolution
from trellis.runs.answers import _FORMATS, answer_problem
def refused(fmt, value):
    expects = {{"type": "string", "format": fmt}}
    asked = Interrupt(tenant_id="t", run_id="r", question="q", expects=expects)
    answer = InterruptResolution(interrupt_id=asked.interrupt_id, run_id="r",
                                 decision=InterruptDecision.ANSWER, answer=value)
    return answer_problem(asked, answer) is not None
print(sorted(_FORMATS.checkers), refused("date-time", "nonsense"), refused("date", "2026-13-40"))
"""


@pytest.mark.parametrize("libraries", ["absent", "present"])
def test_the_enforced_set_does_not_depend_on_what_is_installed(libraries: str) -> None:
    probe = PROBE.format(optional=OPTIONAL)
    ran = subprocess.run(
        [sys.executable, "-c", probe, libraries], capture_output=True, text=True, check=True
    )
    assert ran.stdout.split("\n")[0] == f"{sorted(ANSWER_FORMATS)} False True"

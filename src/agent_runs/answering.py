"""Who may answer a paused run: the one rule ``POST /v1/runs/{run_id}/resume`` applies, under
the run's lock and before anything is written, to the assignee the run waits on now (an
escalation may have moved it). A ``CANCEL`` is an answer like any other.

1. A key that administers the tenant (``admin``, or the operator's ``platform`` key) answers
   any run.
2. A key that may act for anyone (``"*"`` in ``may_act_as``, the Memory Service's default)
   answers any run: the application holding it vouches for the reviewer it names.
3. A key restricted to listed principals answers as one of them (the ``reviewer``; none is
   the key itself), and only a run assigned to that principal or to nobody. A run assigned
   to a group (``role:finance``) is refused: agent-runs cannot see who is in it, so the
   application's key or an admin key answers it.

Principals compare as ``kind:id``, a bare id being a user's (``priya`` is ``user:priya``);
the reviewer is stored as given. Reading runs, the inbox and the worker routes are not
answering: they stay tenant-wide for every key.
"""

from __future__ import annotations

from typing import Final

from agent_runs.domain.errors import Forbidden
from agent_runs.keys import KeyInfo

#: The registry's roles that administer a whole tenant.
ADMINISTERING_ROLES: Final = frozenset({"admin", "platform"})
#: Principal kinds that name one person, agent or key; any other (``role:``, ``group:``)
#: names a group, whose members agent-runs cannot see.
INDIVIDUAL_KINDS: Final = frozenset({"user", "agent", "key"})


def as_principal(name: str) -> str:
    """``name`` as ``kind:id``: as given when it has a kind, else a user's id."""
    return name if ":" in name else f"user:{name}"


def require_may_answer(key: KeyInfo, assignee: str | None, reviewer: str | None) -> None:
    """Refuse (403) a key that may not answer, as ``reviewer``, a run assigned to
    ``assignee``; the detail says why and what would."""
    if key.role in ADMINISTERING_ROLES or "*" in key.may_act_as:
        return
    allowed = ", ".join(key.may_act_as) or key.principal
    answering = as_principal(reviewer) if reviewer else key.principal
    if not key.may_act_for(answering):
        raise Forbidden(f"this key may not act for {answering}; it may act only for {allowed}")
    if assignee is None:
        return
    assigned = as_principal(assignee)
    if assigned.partition(":")[0] not in INDIVIDUAL_KINDS:
        raise Forbidden(
            f"the run is assigned to {assigned}, a group: a key restricted to listed people "
            "cannot answer it; answer with the application's key or an admin key"
        )
    if assigned != answering:
        raise Forbidden(
            f"the run is assigned to {assigned}, not {answering}; this key may act only for "
            f"{allowed}"
        )

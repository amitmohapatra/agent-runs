"""Who may answer a paused run: the one rule ``POST /v1/runs/{run_id}/resume`` applies, under
the run's lock and before anything is written, to the assignee the run waits on now (an
escalation may have moved it). A ``CANCEL`` is an answer like any other, and who may answer
a run may cancel it, whatever its status (``POST /v1/runs/{run_id}/cancel``; a run that is
not paused is assigned to nobody).

1. A key that administers the tenant (``admin``, or the operator's ``platform`` key) answers
   any run.
2. A key that may act for anyone (``"*"`` in ``may_act_as``, the Memory Service's default)
   answers any run: the application holding it vouches for the reviewer it names.
3. A key restricted to listed principals answers as one of them (the ``reviewer``; none is
   the key itself), and only a run assigned to that principal or to nobody. A run assigned
   to a group (``role:finance``) is refused: agent-runs cannot see who is in it, so the
   application's key or an admin key answers it.

A run's inbox (``POST``/``GET /v1/runs/{run_id}/messages``) is its user's: an administering
key or one that may act for anyone reaches any run's; a key restricted to listed people only
the runs of a user it may act for, as that user (``require_may_message``).

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


def require_may_cancel(key: KeyInfo, assignee: str | None) -> None:
    """Refuse (403) a key that could not answer a run assigned to ``assignee`` as any
    principal it may act for: who may answer a run may cancel it."""
    as_assignee = assignee if assignee and key.may_act_for(as_principal(assignee)) else None
    require_may_answer(key, assignee, as_assignee)


def require_may_message(key: KeyInfo, user: str | None, sender: str | None) -> str:
    """The principal a message to (or a read of) the inbox of a run of ``user`` is from, as
    ``key`` may send it: ``sender`` when named, else the key's own principal, or the run's
    user for a key restricted to listed people. Refused (403): a restricted key on a run of a
    user it may not act for, or naming someone else."""
    named = as_principal(sender) if sender else None
    if key.role in ADMINISTERING_ROLES or "*" in key.may_act_as:
        return named or key.principal
    allowed = ", ".join(key.may_act_as) or key.principal
    owner = as_principal(user) if user else None
    if owner is None or not key.may_act_for(owner):
        raise Forbidden(f"this key may not reach this run's inbox; it may act only for {allowed}")
    if named is not None and named != owner:
        raise Forbidden(f"this key may not send as {named}; it may act only for {allowed}")
    return owner

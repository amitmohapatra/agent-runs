"""Keyset pages: one convention for every listing.

A store fetches one row more than the page: its presence is the proof that a next page
exists, and the page's ``after`` is the position of the last row returned (the values of the
columns the listing is ordered by, which are immutable, so a row written between two
requests is neither skipped nor repeated). The API turns ``after`` into an opaque cursor
(``api/pagination.py``) and back.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Page[T]:
    items: list[T]
    #: where the next page starts, or ``None`` when this is the last one
    after: dict[str, Any] | None


def page_of[R, T](
    rows: Sequence[R],
    *,
    limit: int,
    item: Callable[[R], T],
    position: Callable[[R], Mapping[str, Any]],
) -> Page[T]:
    """Split ``limit + 1`` fetched rows into a page and the position after it."""
    kept = rows[:limit]
    after = dict(position(kept[-1])) if len(rows) > limit and kept else None
    return Page([item(row) for row in kept], after)

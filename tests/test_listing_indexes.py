"""Each listing a client pages by one filter is served by an index that leads with the tenant
and that filter, then the listing's order: a user's runs (``user_id``: a person's tasks) and a
thread's runs (``thread_id``: a conversation's earlier turns and its tasks), at any table size."""

from __future__ import annotations

import pytest
from sqlalchemy import text


@pytest.mark.parametrize("column", ["user_id", "thread_id"])
async def test_the_listing_by_one_filter_has_its_own_index(app, column: str) -> None:
    async with app.state.engine.connect() as conn:
        rows = (
            await conn.execute(
                text("select indexdef from pg_indexes where tablename = 'agent_runs'")
            )
        ).scalars()
        leading = [d.split("(", 1)[1] for d in rows]
    assert f"tenant_id, {column}, created_at)" in leading, leading

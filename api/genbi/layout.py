"""Pure dashboard-layout merge, per the v0.1 runbook's layout rules.

The v0.1 runbook (``analytics/superset/build_dashboard.py``) established three
Superset-4.1 constraints the hard way; every function here preserves them:

1. The layout travels nested under ``positions`` inside ``json_metadata``.
2. Every container node (ROW/COLUMN) carries ``meta`` with at least
   ``meta.background`` — the SPA's grid components crash without it.
3. CHART-<id> nodes carry explicit non-zero ``meta.width``/``meta.height``
   (no zero-size orphans); charts are wrapped in COLUMN nodes on a 16-column grid.

Merging is append-only and deterministic: existing curated rows are untouched,
the "Ask → Save" markdown header is created exactly once, and each promoted
question occupies the grid row derived from its question hash — so re-promoting
a question never duplicates rows.
"""

from __future__ import annotations

from typing import Any

from api.genbi.slugs import genbi_column_id, genbi_row_id

BACKGROUND_TRANSPARENT = "BACKGROUND_TRANSPARENT"
BACKGROUND_WHITE = "BACKGROUND_WHITE"

HEADER_ROW_ID = "ROW-GENBI-ASK-SAVE-HEADER"
HEADER_MARKDOWN_ID = "MARKDOWN-GENBI-ASK-SAVE"
HEADER_TEXT = (
    "### Ask → Save\n\nCharts below are promoted natural-language answers, saved "
    "from GenBI with their governed SQL. Curated dealer KPI charts live above."
)


def fresh_layout() -> dict[str, Any]:
    """The minimal v2 skeleton: ROOT -> GRID, no children."""
    return {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "parents": ["ROOT_ID"], "children": []},
    }


def ensure_root(layout: dict[str, Any]) -> dict[str, Any]:
    """Backfill the v2 skeleton keys on a layout that may be empty or partial."""
    layout.setdefault("DASHBOARD_VERSION_KEY", "v2")
    layout.setdefault("ROOT_ID", {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]})
    grid = layout.setdefault(
        "GRID_ID", {"type": "GRID", "id": "GRID_ID", "parents": ["ROOT_ID"], "children": []}
    )
    grid.setdefault("children", [])
    return layout


def ensure_ask_save_header(layout: dict[str, Any]) -> dict[str, Any]:
    """Idempotently place the markdown section header above the GenBI rows."""
    if HEADER_ROW_ID in layout:
        # Self-heal: the markdown component reads meta.code on Superset 4.1.1
        # (rendering falls back to its placeholder without it). Backfill the
        # content keys when an older layout omitted them; never overwrite
        # content a user has since edited.
        node = layout.get(HEADER_MARKDOWN_ID)
        if isinstance(node, dict):
            node.setdefault("data", HEADER_TEXT)
            meta = node.setdefault("meta", {})
            meta.setdefault("code", HEADER_TEXT)
            meta.setdefault("text", HEADER_TEXT)
        return layout
    layout[HEADER_ROW_ID] = {
        "type": "ROW",
        "id": HEADER_ROW_ID,
        "parents": ["ROOT_ID", "GRID_ID"],
        "children": [HEADER_MARKDOWN_ID],
        "meta": {"background": BACKGROUND_TRANSPARENT},
    }
    layout["GRID_ID"]["children"].append(HEADER_ROW_ID)
    layout[HEADER_MARKDOWN_ID] = {
        "type": "MARKDOWN",
        "id": HEADER_MARKDOWN_ID,
        "parents": ["ROOT_ID", "GRID_ID", HEADER_ROW_ID],
        "data": HEADER_TEXT,
        "meta": {
            "background": BACKGROUND_WHITE,
            "width": 16,
            "height": 3,
            "text": HEADER_TEXT,
            "code": HEADER_TEXT,  # Superset 4.1.1's Markdown component reads meta.code
        },
    }
    return layout


def append_chart_row(
    layout: dict[str, Any],
    *,
    question: str,
    chart_id: int,
    chart_uuid: str,
    width: int = 6,
    height: int = 50,
) -> dict[str, Any]:
    """Append (or keep, when re-promoting) this question's grid row.

    Deterministic row/column ids make re-promotion a no-op at the layout level:
    the chart id inside is stable because the chart itself is updated in place.
    """
    row_id = genbi_row_id(question)
    if row_id in layout:
        return layout
    ensure_root(layout)
    ensure_ask_save_header(layout)
    column_id = genbi_column_id(question)
    chart_node_id = f"CHART-{chart_id}"
    layout[row_id] = {
        "type": "ROW",
        "id": row_id,
        "parents": ["ROOT_ID", "GRID_ID"],
        "children": [column_id],
        "meta": {"background": BACKGROUND_TRANSPARENT},
    }
    layout["GRID_ID"]["children"].append(row_id)
    layout[column_id] = {
        "type": "COLUMN",
        "id": column_id,
        "parents": ["ROOT_ID", "GRID_ID", row_id],
        "children": [chart_node_id],
        "meta": {"background": BACKGROUND_TRANSPARENT, "width": width},
    }
    layout[chart_node_id] = {
        "type": "CHART",
        "id": chart_node_id,
        "chartId": chart_id,
        "parents": ["ROOT_ID", "GRID_ID", row_id, column_id],
        "meta": {
            "chartId": chart_id,
            "uuid": chart_uuid,
            "width": width,
            "height": height,
        },
    }
    return layout

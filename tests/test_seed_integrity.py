"""Seed data integrity: manifest checksums, GL balance, book reconciliation.

These tests guard the committed seed export itself — the demo's credibility
depends on the seeded books being internally consistent.
"""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import duckdb
import pytest

REPO = Path(__file__).resolve().parents[1]
DEALER = REPO / "seed" / "dealer_export"
WINDOW_DAYS = 232  # Jan 6 - Aug 25 2026 invoice window


def test_manifest_checksums_verify_against_files():
    manifest = json.loads((DEALER / "manifest.json").read_text(encoding="utf-8"))
    files = manifest["files"] if isinstance(manifest, dict) else manifest
    for entry in files:
        name = entry["name"]
        expected = entry["sha256"]
        content = (DEALER / name).read_bytes()
        assert hashlib.sha256(content).hexdigest() == expected, f"{name} fails checksum"


def test_gl_journals_balance():
    """Every journal entry's debits equal its credits."""
    journals: dict[str, tuple[float, float]] = {}
    with (DEALER / "gl_entries.csv").open() as fh:
        for row in csv.DictReader(fh):
            debits, credits = journals.setdefault(row["journal_no"], (0.0, 0.0))
            journals[row["journal_no"]] = (
                debits + float(row["debit_amt"] or 0),
                credits + float(row["credit_amt"] or 0),
            )
    unbalanced = {j: v for j, v in journals.items() if abs(v[0] - v[1]) > 0.01}
    assert not unbalanced, f"unbalanced journals: {unbalanced}"


def test_gl_reconciles_to_transactional_books():
    """GL revenue/COGS accruals track the invoice book (tolerance 2%)."""
    con = duckdb.connect()
    book = con.execute(
        """
        SELECT
          SUM(invoiced_qty * unit_price + freight_amt + tax_amt),
          SUM(invoiced_qty * unit_cost)
        FROM read_csv_auto(?, header = true)
        """,
        [str(DEALER / "invoice_lines.csv")],
    ).fetchone()
    gl = con.execute(
        """
        SELECT
          SUM(CASE WHEN account = '1100' THEN debit_amt ELSE 0 END),
          SUM(CASE WHEN account = '5000' THEN debit_amt ELSE 0 END)
        FROM read_csv_auto(?, header = true)
        WHERE description LIKE 'Sales accrual%'
           OR description LIKE 'COGS %'
        """,
        [str(DEALER / "gl_entries.csv")],
    ).fetchone()
    gl_revenue, gl_cogs = float(gl[0]), float(gl[1])
    book_revenue, book_cogs = float(book[0]), float(book[1])
    assert gl_revenue == pytest.approx(book_revenue, rel=0.02)
    assert gl_cogs == pytest.approx(book_cogs, rel=0.02)
    con.close()


def test_books_land_in_plausible_dealer_ranges():
    """Guard rails so the demo never regresses to implausible economics."""
    con = duckdb.connect()
    revenue, cogs, avg_inv = con.execute(
        """
        SELECT
          (SELECT SUM(invoiced_qty * unit_price) FROM read_csv_auto('seed/dealer_export/invoice_lines.csv')),
          (SELECT SUM(invoiced_qty * unit_cost) FROM read_csv_auto('seed/dealer_export/invoice_lines.csv')),
          (SELECT AVG(m) FROM (
             SELECT snapshot_date, SUM(inventory_value) AS m
             FROM read_csv_auto('seed/dealer_export/inventory_snapshots.csv') GROUP BY 1))
        """
    ).fetchone()
    revenue, cogs, avg_inv = float(revenue), float(cogs), float(avg_inv)
    ann = 365 / WINDOW_DAYS
    gross_margin = 1 - cogs / revenue
    turns = cogs * ann / avg_inv
    gmroi = (revenue - cogs) * ann / avg_inv
    assert 0.18 <= gross_margin <= 0.35, f"gross margin {gross_margin:.3f} out of range"
    assert 3.0 <= turns <= 8.0, f"inventory turns {turns:.2f} out of range"
    assert 1.0 <= gmroi <= 4.0, f"GMROI {gmroi:.2f} out of range"
    con.close()

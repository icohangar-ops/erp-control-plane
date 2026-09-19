#!/usr/bin/env python3
"""Generate the seeded sample-dealer export for the demo path.

Produces a deterministic, plausible CSV export from a fictional acquired dealer
("Ridgeline Lumber & Supply", three Tri-Cities TN/VA branches) exactly as its
legacy ERP's nightly job would drop them on SFTP: flat files plus a manifest
control file declaring filenames, SHA-256 checksums, and row counts.

Determinism: every random draw comes from a single ``random.Random(42)``.
Re-running regenerates byte-identical files.

Files are written to ``seed/dealer_export/`` (the "dealer drop") and
``seed/reference/`` (platform-owned reference data: branches, COA mapping,
close calendar — curated by corporate finance during onboarding, not exported
by the dealer's ERP).

Usage: python scripts/generate_seed.py
"""

from __future__ import annotations

import hashlib
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEALER_DIR = REPO_ROOT / "seed" / "dealer_export"
REFERENCE_DIR = REPO_ROOT / "seed" / "reference"

BATCH_ID = "RIDGELINE-2026-09-19-B001"
SCHEMA_VERSION = "1.0.0"

rng = random.Random(42)

# Calibrated so the seeded books reconcile to plausible dealer economics:
# ~$12M annualized revenue across 39 FTE (~$320k/FTE), 4-6 inventory turns,
# DSO ~50, DPO ~40. Transaction quantities draw at LINE_QTY_SCALE x the
# catalog's per-line share of weekly demand; month-end stock scales with
# STOCK_WEEKS_SCALE; GL opening balances derive from the books (see
# generate_gl_entries) instead of independent magnitudes.
LINE_QTY_SCALE = 200
STOCK_SCALE = 15

# ---------------------------------------------------------------------------
# Master data
# ---------------------------------------------------------------------------

BRANCHES = [
    # branch_code, name, region, owned_since, fte_count
    ("RL-BRI", "Bristol Yard", "Tri-Cities", "1994-03-01", 18),
    ("RL-KPT", "Kingsport Yard", "Tri-Cities", "2001-06-15", 12),
    ("RL-JCB", "Johnson City Yard", "Tri-Cities", "2026-03-15", 9),  # acquired mid-2026
]

# item_no, description, category, subcategory, uom, unit_cost, list_price, base_weekly_demand
ITEMS = [
    ("RM-1001", "SPF Dimension Lumber 2x4x8 #2", "Lumber & Panels", "Dimension Lumber", "EA", 2.86, 3.98, 240),
    ("RM-1002", "SPF Dimension Lumber 2x6x10 #2", "Lumber & Panels", "Dimension Lumber", "EA", 4.95, 6.85, 150),
    ("RM-1003", "SPF Dimension Lumber 2x8x12 #1", "Lumber & Panels", "Dimension Lumber", "EA", 8.25, 11.40, 60),
    ("RM-1010", "OSB Sheathing 7/16 4x8", "Lumber & Panels", "Sheathing", "EA", 13.50, 18.75, 110),
    ("RM-1011", "Plywood Sheathing 11/32 4x8", "Lumber & Panels", "Sheathing", "EA", 17.95, 24.90, 70),
    ("RM-1020", "PT Lumber 2x4x8 Ground Contact", "Lumber & Panels", "Treated", "EA", 4.05, 5.60, 55),
    ("RM-1021", "PT Plywood 3/4 4x8", "Lumber & Panels", "Treated", "EA", 28.10, 38.90, 18),
    ("RM-1022", "5/4x6 Radius Edge Deck Board 12'", "Lumber & Panels", "Decking", "EA", 6.45, 8.95, 85),
    ("FS-2001", "Framing Nails 16d Bright 5 lb", "Framing & Fasteners", "Nails", "BX", 20.60, 28.50, 40),
    ("FS-2002", "Deck Screws 3in coated 5 lb", "Framing & Fasteners", "Screws", "BX", 23.60, 32.75, 45),
    ("FS-2010", "Simpson H2.5A Hurricane Tie", "Framing & Fasteners", "Connectors", "EA", 1.33, 1.85, 160),
    ("FS-2011", "Simpson HD10 Holdown", "Framing & Fasteners", "Connectors", "EA", 34.90, 48.50, 12),
    ("CM-3001", "QUIKRETE 80 lb 3500 psi", "Concrete & Masonry", "Bagged Concrete", "BG", 4.95, 6.85, 130),
    ("CM-3002", "QUIKRETE 60 lb", "Concrete & Masonry", "Bagged Concrete", "BG", 3.90, 5.40, 60),
    ("CM-3010", "Masonry Cement Type N", "Concrete & Masonry", "Masonry", "BG", 9.30, 12.90, 25),
    ("CM-3020", "Rebar #4 x 20 ft Grade 60", "Concrete & Masonry", "Rebar", "EA", 10.25, 14.25, 35),
    ("RF-4001", "Architectural Shingles BNDL", "Roofing", "Shingles", "BNDL", 27.90, 38.75, 90),
    ("RF-4002", "Ridge Cap Shingles BNDL", "Roofing", "Shingles", "BNDL", 37.65, 52.30, 20),
    ("RF-4010", "#15 Asphalt Felt 432 sf Roll", "Roofing", "Underlayment", "RL", 21.50, 29.90, 30),
    ("RF-4011", "Synthetic Underlayment 10 sq", "Roofing", "Underlayment", "RL", 78.85, 109.50, 15),
    ("RF-4020", "Aluminum Drip Edge 10 ft", "Roofing", "Flashing", "LF", 1.69, 2.35, 120),
    ("PT-5001", "Interior Paint Flat Gallon", "Paint & Interiors", "Paint", "EA", 32.20, 44.75, 35),
    ("PT-5002", "Bonding Primer Gallon", "Paint & Interiors", "Primer", "EA", 23.70, 32.90, 20),
    ("PT-5010", "Drywall 4x8x1/2", "Paint & Interiors", "Drywall", "EA", 9.05, 12.60, 75),
    ("PT-5011", "All-Purpose Joint Compound 4.5 gal", "Paint & Interiors", "Drywall", "BG", 13.25, 18.40, 22),
    ("DW-6001", "Interior 6-Panel Door Prehung", "Doors & Windows", "Doors", "EA", 64.10, 89.00, 9),
    ("DW-6002", "Entry Door Slab Fiberglass", "Doors & Windows", "Doors", "EA", 208.10, 289.00, 4),
    ("DW-6010", "Double-Hung Window 3x5 Low-E", "Doors & Windows", "Windows", "EA", 224.60, 312.00, 5),
    ("SX-7001", "Vinyl Siding Double 4 Square", "Siding & Exterior", "Siding", "SQ", 68.05, 94.50, 14),
    ("SX-7010", "Aluminum Soffit 12 ft White", "Siding & Exterior", "Soffit", "EA", 10.65, 14.80, 40),
    ("TL-9001", "Acrylic Latex Caulk 10.1 oz", "Tools & Accessories", "Sealants", "EA", 4.50, 6.25, 65),
    ("TL-9002", "Construction Adhesive 28 oz", "Tools & Accessories", "Adhesives", "EA", 6.40, 8.90, 45),
]

SALESPEOPLE = [
    ("S101", "Dana Whitfield", "RL-BRI"),
    ("S102", "Marcus Boyd", "RL-BRI"),
    ("S103", "Elena Cruz", "RL-KPT"),
    ("S104", "Ray Troxell", "RL-JCB"),
    ("S105", "Tamika Slate", "RL-KPT"),
    ("S001", "Counter Sales", "RL-BRI"),
]

CUSTOMERS = [
    ("CT-1001", "Hensley Framing LLC", "CONTRACTOR", "NET30", 60000, "2147 Volcano Rd", "Bristol", "TN", "37620"),
    ("CT-1002", "Crouch & Sons Drywall", "CONTRACTOR", "NET30", 35000, "88 Oakwood Ave", "Kingsport", "TN", "37660"),
    ("CT-1003", "Triple Creek Roofing", "CONTRACTOR", "NET30", 45000, "1 Creekside Dr", "Bristol", "TN", "37620"),
    ("CT-1004", "Mountain View Builders", "CONTRACTOR", "NET30", 80000, "4500 Highway 11W", "Bristol", "VA", "24201"),
    ("CT-1005", "Bowman Electric Inc", "CONTRACTOR", "NET30", 20000, "92 Chestnut St", "Kingsport", "TN", "37663"),
    ("CT-1006", "Appalachian Exteriors", "CONTRACTOR", "NET45", 30000, "610 Sunset Dr", "Johnson City", "TN", "37601"),
    ("CT-1007", "Steele Renovations", "CONTRACTOR", "NET30", 15000, "77 Mill Pond Rd", "Blountville", "TN", "37617"),
    ("CT-1008", "Fairview Homes Inc", "CONTRACTOR", "NET30", 55000, "3900 Fairview Rd", "Grey", "TN", "37618"),
    ("BL-3001", "Colonial Development Group", "BUILDER", "NET45", 150000, "1200 Market St Ste 400", "Kingsport", "TN", "37662"),
    ("BL-3002", "High Point Custom Homes", "BUILDER", "NET45", 90000, "55 High Point Ct", "Johnson City", "TN", "37615"),
    ("RT-2001", "Walk-In Retail", "RETAIL", "CASH", 0, "1100 Volunteer Pkwy", "Bristol", "TN", "37620"),
    ("RT-2002", "Doyle Handyman Services", "RETAIL", "CASH", 0, "19 Locust Ln", "Bristol", "TN", "37620"),
    ("RT-2003", "Vance Property Management", "RETAIL", "NET15", 8000, "580 Willow St", "Kingsport", "TN", "37664"),
]

VENDORS = [
    ("VN-001", "Southeast Panel & Lumber Co", "NET30", 7),
    ("VN-002", "Apex Fasteners Inc", "NET30", 10),
    ("VN-003", "Volunteer Cement Works", "NET21", 5),
    ("VN-004", "RidgeLine Roofing Supply", "NET30", 12),
    ("VN-005", "TriSummit Paint Co", "NET30", 9),
    ("VN-006", "BlueRidge Millwork", "NET30", 14),
    ("VN-007", "National Door & Millwork", "NET30", 21),
    ("VN-008", "GreenLine Building Products", "NET21", 8),
]

CATEGORY_VENDOR = {
    "Lumber & Panels": "VN-001",
    "Framing & Fasteners": "VN-002",
    "Concrete & Masonry": "VN-003",
    "Roofing": "VN-004",
    "Paint & Interiors": "VN-005",
    "Doors & Windows": "VN-007",
    "Siding & Exterior": "VN-008",
    "Tools & Accessories": "VN-005",
}

# ---------------------------------------------------------------------------
# CSV writers
# ---------------------------------------------------------------------------


def write_csv(path: Path, header: list[str], rows: list[list | str | float | int | None]) -> None:
    """Write one dealer-export CSV with quoted fields, ISO dates, CRLF-free LF."""
    path.parent.mkdir(parents=True, exist_ok=True)
    out = [",".join(_q(h) for h in header)]
    for row in rows:
        out.append(",".join(_q(v) for v in row))
    path.write_text("\n".join(out) + "\n", encoding="utf-8")


def _q(value: object) -> str:
    text = "" if value is None else str(value)
    if "," in text or '"' in text:
        return '"' + text.replace('"', '""') + '"'
    return text


def money(value: float) -> str:
    return f"{value:.2f}"


# ---------------------------------------------------------------------------
# Transaction generation
# ---------------------------------------------------------------------------

HOLIDAYS = {date(2026, 1, 1), date(2026, 5, 25), date(2026, 7, 3), date(2026, 9, 7)}

SEASON_FACTOR = {1: 0.70, 2: 0.80, 3: 0.95, 4: 1.10, 5: 1.25, 6: 1.30, 7: 1.20, 8: 1.10}

# Weeks-of-supply target per category drives base stocking levels (turns ~4-6 blended)
WEEKS_OF_SUPPLY = {
    "Lumber & Panels": 5.5,
    "Framing & Fasteners": 9.0,
    "Concrete & Masonry": 7.0,
    "Roofing": 8.0,
    "Paint & Interiors": 10.0,
    "Doors & Windows": 12.0,
    "Siding & Exterior": 10.0,
    "Tools & Accessories": 11.0,
}

# Branch demand weight and restock cadence
BRANCH_WEIGHT = {"RL-BRI": 1.0, "RL-KPT": 0.75, "RL-JCB": 0.55}


def _branch_for_item(item_no: str) -> str:
    """Assign a stable 'home branch' per item but allow spillover to others."""
    return rng.choices(
        list(BRANCH_WEIGHT), weights=[BRANCH_WEIGHT[b] for b in BRANCH_WEIGHT], k=1
    )[0]


def generate_items() -> list[list]:
    rows = []
    for item_no, desc, cat, sub, uom, cost, price, _demand in ITEMS:
        rows.append([item_no, desc, cat, sub, uom, money(cost), money(price), "ACTIVE"])
    return rows


def generate_salespeople() -> list[list]:
    return [[code, name, branch] for code, name, branch in SALESPEOPLE]


def generate_customers() -> list[list]:
    return [[c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7], c[8]] for c in CUSTOMERS]


def generate_vendors() -> list[list]:
    return [[v[0], v[1], v[2], v[3]] for v in VENDORS]


def _month_demand_scale(month: int) -> float:
    return SEASON_FACTOR[month]


def generate_sales_order_lines() -> list[list]:
    """One row per sales order line, Jan 5 2026 through Aug 28 2026."""
    rows: list[list] = []
    order_seq = 10000
    contractors = [c for c in CUSTOMERS if c[2] in ("CONTRACTOR", "BUILDER")]
    retail = [c for c in CUSTOMERS if c[2] == "RETAIL"]
    day = date(2026, 1, 5)
    end = date(2026, 8, 28)
    while day <= end:
        if day.weekday() == 6 or day in HOLIDAYS:
            day += timedelta(days=1)
            continue
        month_f = _month_demand_scale(day.month)
        sat = day.weekday() == 5
        n_orders = rng.randint(2, 4) if not sat else rng.randint(2, 5)
        for _ in range(n_orders):
            order_seq += 1
            order_no = f"SO-{order_seq}"
            # Saturday orders skew retail; weekdays skew contractors
            customer = rng.choice(retail if sat and rng.random() < 0.6 else contractors if not sat else retail)
            branch = rng.choices(
                list(BRANCH_WEIGHT), weights=[BRANCH_WEIGHT[b] for b in BRANCH_WEIGHT], k=1
            )[0]
            sp_candidates = [s for s in SALESPEOPLE if s[2] == branch and s[0] != "S001"]
            salesperson = rng.choice(sp_candidates)[0] if customer[2] != "RETAIL" else "S001"
            promised = day + timedelta(days=rng.randint(2, 7))
            n_lines = rng.randint(1, 5)
            # Order decides shipment state once (all lines share it)
            shipped = None
            status = "SHIPPED"
            if day >= date(2026, 8, 21):
                status, shipped = "OPEN", None
            elif rng.random() < 0.045:
                status, shipped = "CANCELLED", None
            elif rng.random() < 0.12:
                status, shipped = "PARTIAL", day + timedelta(days=rng.randint(1, 4))
            else:
                shipped = day + timedelta(days=rng.randint(1, 4))
                # ~9% of shipped orders are late vs promise
                if rng.random() < 0.09:
                    shipped = promised + timedelta(days=rng.randint(1, 5))
            for line_no in range(1, n_lines + 1):
                item_no, _d, _c, _s, uom, cost, price, demand = rng.choice(ITEMS)
                scale = BRANCH_WEIGHT[branch] * month_f
                qty = max(1, round(demand / 40 * scale * rng.uniform(0.5, 2.2) * LINE_QTY_SCALE))
                if item_no.startswith(("DW", "SX", "RF-4011", "FS-2011")):
                    qty = max(1, round(qty / 10 * 8))  # big-ticket items: temper the scale
                if status == "CANCELLED":
                    filled, cancelled = 0, qty
                elif status == "PARTIAL":
                    filled = max(1, round(qty * rng.uniform(0.45, 0.85)))
                    cancelled = 0
                elif status == "OPEN":
                    filled, cancelled = 0, 0
                else:
                    filled, cancelled = qty, 0
                    if rng.random() < 0.07:  # line-level short fill on shipped lines
                        filled = max(1, round(qty * rng.uniform(0.5, 0.9)))
                if customer[2] == "BUILDER":
                    disc = rng.uniform(0.86, 0.94)
                elif customer[2] == "CONTRACTOR":
                    disc = rng.uniform(0.90, 1.0)
                else:
                    disc = 1.0
                unit_price = round(price * disc, 2)
                rows.append([
                    order_no, line_no, day.isoformat(), customer[0], branch, salesperson,
                    item_no, uom, qty, filled, cancelled, money(unit_price), money(cost),
                    promised.isoformat(), shipped.isoformat() if shipped else "", status,
                ])
        day += timedelta(days=1)
    return rows


def generate_invoice_lines(order_lines: list[list]) -> list[list]:
    """Invoice each shipped/partial order line at fill; retail adds tax, freight on bulk."""
    rows: list[list] = []
    invoice_seq = 50000
    # Group order lines by order_no so freight is charged once per invoice
    by_order: dict[str, list[list]] = {}
    for line in order_lines:
        if line[15] in ("SHIPPED", "PARTIAL") and line[9] > 0:  # filled_qty
            by_order.setdefault(line[0], []).append(line)
    for order_no, lines in by_order.items():
        invoice_seq += 1
        invoice_no = f"INV-{invoice_seq}"
        invoice_date = date.fromisoformat(lines[0][14])  # shipped_date
        customer_no = lines[0][3]
        customer = next(c for c in CUSTOMERS if c[0] == customer_no)
        for line_no, line in enumerate(lines, start=1):
            _order_no, _ln, _od, _cust, _br, _sp, item_no, uom, _q, filled, _c, unit_price, unit_cost, _pd, _sd, _st = line
            item = next(i for i in ITEMS if i[0] == item_no)
            unit_price, unit_cost = float(unit_price), float(unit_cost)
            freight = round(filled * 0.02 * item[6], 2) if item[6] >= 12 and filled > 5 else 0.0  # bulk lumber
            tax = round(filled * unit_price * 0.0975, 2) if customer[2] == "RETAIL" else 0.0
            rows.append([
                invoice_no, line_no, invoice_date.isoformat(), order_no, customer_no, line[4],
                item_no, uom, filled, money(unit_price), money(unit_cost), money(freight), money(tax),
            ])
    return rows


def generate_purchase_order_lines() -> list[list]:
    """Weekly restock POs per branch; received vs ordered drives vendor fill + PPV."""
    rows: list[list] = []
    po_seq = 20000
    day = date(2026, 1, 5)
    end = date(2026, 8, 28)
    while day <= end:
        for branch in BRANCH_WEIGHT:
            if rng.random() < 0.75:
                po_seq += 1
                po_no = f"PO-{po_seq}"
                vendor_no = rng.choice(VENDORS)[0]
                items_for_vendor = [i for i in ITEMS if CATEGORY_VENDOR[i[2]] == vendor_no]
                n_lines = min(len(items_for_vendor), rng.randint(2, 4))
                chosen = rng.sample(items_for_vendor, n_lines)
                for line_no, item in enumerate(chosen, start=1):
                    item_no, _d, _c, _s, uom, cost, _p, demand = item
                    ordered = max(
                        5, round(demand / 10 * BRANCH_WEIGHT[branch] * rng.uniform(0.8, 1.6) * LINE_QTY_SCALE)
                    )
                    received, received_date = 0, None
                    if day <= date(2026, 8, 14):
                        r = rng.random()
                        if r < 0.70:
                            received = ordered
                        elif r < 0.94:
                            received = max(1, round(ordered * rng.uniform(0.4, 0.9)))
                        received_date = day + timedelta(days=rng.randint(2, 9))
                    ppv_factor = rng.uniform(0.95, 1.08)
                    actual = round(cost * ppv_factor, 2)
                    rows.append([
                        po_no, line_no, day.isoformat(), vendor_no, branch, item_no, uom,
                        ordered, received, money(actual), money(cost),  # actual, standard
                        received_date.isoformat() if received_date else "",
                    ])
        day += timedelta(days=7)
    return rows


def generate_inventory_snapshots() -> list[list]:
    """Month-end periodic snapshots Apr-Aug 2026 (Apr seeds May averages)."""
    rows: list[list] = []
    snap_dates = [date(2026, 4, 30), date(2026, 5, 31), date(2026, 6, 30), date(2026, 7, 31), date(2026, 8, 31)]
    for snap in snap_dates:
        for branch, weight in BRANCH_WEIGHT.items():
            month_f = SEASON_FACTOR[snap.month] if snap.month <= 8 else 1.0
            for item_no, _d, cat, _s, _u, cost, _p, demand in ITEMS:
                weeks = WEEKS_OF_SUPPLY[cat]
                base_stock = demand / 4.33 * weeks * weight * month_f * STOCK_SCALE
                on_hand = max(0, round(base_stock * rng.uniform(0.55, 1.5)))
                allocated = round(on_hand * rng.uniform(0.0, 0.25)) if rng.random() < 0.5 else 0
                on_order = round(base_stock * rng.uniform(0.1, 0.6)) if rng.random() < 0.4 else 0
                backorder = round(base_stock * rng.uniform(0.02, 0.2)) if rng.random() < 0.18 else 0
                rows.append([
                    snap.isoformat(), branch, item_no, on_hand, allocated, on_order, backorder,
                    money(cost), money(on_hand * cost),
                ])
    return rows


GL_ACCOUNTS = [
    ("1010", "Cash - Operating"),
    ("1100", "Accounts Receivable"),
    ("1200", "Inventory"),
    ("2000", "Accounts Payable"),
    ("3900", "Owner Equity"),
    ("4000", "Sales Revenue"),
    ("4010", "Freight Recovered"),
    ("5000", "Cost of Goods Sold"),
    ("6100", "Payroll Expense"),
    ("6200", "Rent Expense"),
    ("6300", "Utilities Expense"),
    ("6400", "Insurance Expense"),
    ("6500", "Supplies Expense"),
    ("6900", "Owner Compensation"),
]


def generate_gl_entries(
    invoice_lines: list[list], po_lines: list[list], inventory_snapshots: list[list]
) -> list[list]:
    """Monthly journal lines per branch: revenue, COGS, collections, purchases, opex.

    Opening balances DERIVE FROM THE BOOKS (annualized revenue run-rate,
    measured stock position) so balance-sheet KPIs (DSO/DPO/turns) reconcile
    to the transactional volume instead of floating on independent magnitudes.
    Book-derived amounts are already branch-filtered and must NOT be scaled by
    branch weight again; weight allocation applies only to group-level opex
    constants.
    """
    rows: list[list] = []
    journal_seq = 1

    def journal(lines: list[list]) -> None:
        nonlocal journal_seq
        journal_no = f"JE-{journal_seq:05d}"
        journal_seq += 1
        for line_no, line in enumerate(lines, start=1):
            rows.append([journal_no, line_no, *line])

    # ---- book-derived planning figures -------------------------------------
    invoice_dates = [date.fromisoformat(r[2]) for r in invoice_lines]
    window_days = (max(invoice_dates) - min(invoice_dates)).days + 1
    window_revenue = sum(
        float(r[9]) * float(r[8]) + float(r[11]) + float(r[12]) for r in invoice_lines
    )
    annual_revenue = window_revenue * 365.0 / window_days
    # Branch revenue shares from the actual book
    branch_revenue: dict[str, float] = {}
    for r in invoice_lines:
        branch_revenue[r[5]] = branch_revenue.get(r[5], 0.0) + float(r[9]) * float(r[8])
    revenue_share = {b: v / max(window_revenue, 1.0) for b, v in branch_revenue.items()}
    # Measured stock position (latest snapshot per branch) anchors opening inventory
    snapshot_dates = sorted({r[0] for r in inventory_snapshots})
    latest_snap = snapshot_dates[0]  # April 30 = first measured position
    branch_stock: dict[str, float] = {}
    for r in inventory_snapshots:
        if r[0] == latest_snap:
            branch_stock[r[1]] = branch_stock.get(r[1], 0.0) + float(r[8])  # inventory_value

    # Opening balances Jan 1 per branch: AR ~25 days of the branch's revenue
    # run-rate, inventory from the measured stock position, AP light, cash and
    # equity plug to balance.
    for branch, weight in BRANCH_WEIGHT.items():
        share = revenue_share.get(branch, weight)  # fall back to weight for pre-acquisition
        opening_ar = round(annual_revenue * 25 / 365 * share, 2)
        opening_inventory = round(branch_stock.get(branch, 0.0), 2)
        opening_ap = round(annual_revenue * 8 / 365 * share, 2)
        assets = 420000 * weight + opening_ar + opening_inventory
        opening_equity = round(assets - opening_ap, 2)
        journal([
            [date(2026, 1, 1), branch, "1010", "Opening balance", round(420000 * weight, 2), 0],
            [date(2026, 1, 1), branch, "1100", "Opening balance", opening_ar, 0],
            [date(2026, 1, 1), branch, "1200", "Opening balance", opening_inventory, 0],
            [date(2026, 1, 1), branch, "2000", "Opening balance", 0, opening_ap],
            [date(2026, 1, 1), branch, "3900", "Opening balance", 0, opening_equity],
        ])

    months = [(1, 31), (2, 28), (3, 31), (4, 30), (5, 31), (6, 30), (7, 31), (8, 31)]
    for month, _last in months:
        month_end = date(2026, month, _last)
        for branch, weight in BRANCH_WEIGHT.items():
            branch_invoices = [r for r in invoice_lines if r[5] == branch and r[2].startswith(f"2026-{month:02d}")]
            sales = sum(float(r[9]) * float(r[8]) + float(r[11]) + float(r[12]) for r in branch_invoices)
            cogs = sum(float(r[10]) * float(r[8]) for r in branch_invoices)
            branch_pos = [r for r in po_lines if r[4] == branch and r[2].startswith(f"2026-{month:02d}")]
            purchases = sum(float(r[9]) * float(r[7]) for r in branch_pos if r[8] > 0)
            collections = round(sales * 0.97, 2)
            payments = round(purchases * 0.90, 2)

            def scale(v: float) -> float:
                """Allocate a GROUP-LEVEL constant to a branch by weight."""
                return round(v * weight, 2)

            # Book-derived amounts are branch-actual: no weight scaling here.
            journal([
                [month_end, branch, "1100", f"Sales accrual {month_end:%B}", round(sales, 2), 0],
                [month_end, branch, "4000", f"Sales accrual {month_end:%B}", 0, round(sales * 0.985, 2)],
                [month_end, branch, "4010", f"Freight recovered {month_end:%B}", 0, round(sales * 0.015, 2)],
                [month_end, branch, "5000", f"COGS {month_end:%B}", round(cogs, 2), 0],
                [month_end, branch, "1200", f"COGS relief {month_end:%B}", 0, round(cogs, 2)],
                [month_end, branch, "1010", f"Customer collections {month_end:%B}", collections, 0],
                [month_end, branch, "1100", f"Customer collections {month_end:%B}", 0, collections],
                [month_end, branch, "1200", f"Inventory receipts {month_end:%B}", round(purchases, 2), 0],
                [month_end, branch, "2000", f"Inventory receipts {month_end:%B}", 0, round(purchases, 2)],
                [month_end, branch, "2000", f"Vendor payments {month_end:%B}", payments, 0],
                [month_end, branch, "1010", f"Vendor payments {month_end:%B}", 0, payments],
                [month_end, branch, "6100", f"Payroll {month_end:%B}", scale(130000), 0],
                [month_end, branch, "1010", f"Payroll {month_end:%B}", 0, scale(130000)],
                [month_end, branch, "6200", f"Rent {month_end:%B}", scale(26000), 0],
                [month_end, branch, "1010", f"Rent {month_end:%B}", 0, scale(26000)],
                [month_end, branch, "6300", f"Utilities {month_end:%B}", scale(7200), 0],
                [month_end, branch, "1010", f"Utilities {month_end:%B}", 0, scale(7200)],
                [month_end, branch, "6500", f"Supplies {month_end:%B}", scale(4200), 0],
                [month_end, branch, "1010", f"Supplies {month_end:%B}", 0, scale(4200)],
                [month_end, branch, "6900", f"Distributions {month_end:%B}", scale(30000), 0],
                [month_end, branch, "1010", f"Distributions {month_end:%B}", 0, scale(30000)],
            ])
            if month in (3, 6):
                journal([
                    [month_end, branch, "6400", f"Insurance premium {month_end:%B}", scale(14500), 0],
                    [month_end, branch, "1010", f"Insurance premium {month_end:%B}", 0, scale(14500)],
                ])
    return rows


# ---------------------------------------------------------------------------
# Reference seeds (platform-owned, curated during onboarding)
# ---------------------------------------------------------------------------


def write_reference_seeds() -> None:
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    write_csv(
        REFERENCE_DIR / "branches.csv",
        ["branch_code", "branch_name", "region", "owned_since", "fte_count"],
        [list(b) for b in BRANCHES],
    )
    coa_rows = []
    group_map = {
        "1010": ("GRP-1000", "DIRECT"), "1100": ("GRP-1100", "DIRECT"),
        "1200": ("GRP-1200", "DIRECT"), "2000": ("GRP-2000", "DIRECT"),
        "3900": ("GRP-3900", "DIRECT"), "4000": ("GRP-4000", "DIRECT"),
        "4010": ("GRP-4010", "DIRECT"), "5000": ("GRP-5000", "DIRECT"),
        "6100": ("GRP-5100", "DIRECT"), "6200": ("GRP-5200", "DIRECT"),
        "6300": ("GRP-5300", "DIRECT"), "6400": ("GRP-5400", "DIRECT"),
        "6500": ("GRP-5500", "DIRECT"),
        "6900": ("GRP-5900", "ADDBACK"),  # owner comp: normalizing add-back on the bridge
    }
    for acct, name in GL_ACCOUNTS:
        target, rule = group_map[acct]
        coa_rows.append([
            "ridgeline_lumber", acct, name, target, "2026-01-01", "9999-12-31", rule, "TRUE",
        ])
    write_csv(
        REFERENCE_DIR / "coa_mapping.csv",
        ["source_company_id", "source_gl_account", "source_account_name", "target_consolidated_account",
         "effective_start_date", "effective_end_date", "mapping_rule_type", "active_flag"],
        coa_rows,
    )
    close_rows = []
    for month, last in [(1, 31), (2, 28), (3, 31), (4, 30), (5, 31), (6, 30), (7, 31)]:
        period_end = date(2026, month, last)
        close_day = period_end + timedelta(days=int(rng.uniform(10, 17)))
        # Nudge onto a weekday
        while close_day.weekday() >= 5:
            close_day += timedelta(days=1)
        close_rows.append([period_end.isoformat(), close_day.isoformat()])
    write_csv(
        REFERENCE_DIR / "close_calendar.csv",
        ["period_end_date", "close_completed_on"],
        close_rows,
    )


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def write_manifest(files: dict[str, int]) -> None:
    manifest = {
        "batch_id": BATCH_ID,
        "generated_at": "2026-09-19T08:30:00Z",
        "source_company": "ridgeline_lumber",
        "schema_version": SCHEMA_VERSION,
        "files": [],
    }
    for name, row_count in sorted(files.items()):
        digest = hashlib.sha256((DEALER_DIR / name).read_bytes()).hexdigest()
        manifest["files"].append({
            "name": name,
            "sha256": digest,
            "rows": row_count,
            "encoding": "utf-8",
            "delimiter": ",",
        })
    (DEALER_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    DEALER_DIR.mkdir(parents=True, exist_ok=True)
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)

    items = generate_items()
    people = generate_salespeople()
    customers = generate_customers()
    vendors = generate_vendors()
    order_lines = generate_sales_order_lines()
    invoice_lines = generate_invoice_lines(order_lines)
    po_lines = generate_purchase_order_lines()
    snapshots = generate_inventory_snapshots()
    gl_entries = generate_gl_entries(invoice_lines, po_lines, snapshots)

    write_csv(DEALER_DIR / "items.csv",
              ["item_no", "description", "category", "subcategory", "uom", "unit_cost", "list_price", "item_status"], items)
    write_csv(DEALER_DIR / "salespeople.csv",
              ["salesperson_code", "salesperson_name", "home_branch"], people)
    write_csv(DEALER_DIR / "customers.csv",
              ["customer_no", "customer_name", "customer_class", "terms", "credit_limit",
               "address1", "city", "state", "postal_code"], customers)
    write_csv(DEALER_DIR / "vendors.csv",
              ["vendor_no", "vendor_name", "terms", "lead_time_days"], vendors)
    write_csv(DEALER_DIR / "sales_order_lines.csv",
              ["order_no", "line_no", "order_date", "customer_no", "branch_code", "salesperson_code",
               "item_no", "uom", "ordered_qty", "filled_qty", "cancelled_qty", "unit_price",
               "unit_cost", "promised_date", "shipped_date", "order_status"], order_lines)
    write_csv(DEALER_DIR / "purchase_order_lines.csv",
              ["po_no", "line_no", "po_date", "vendor_no", "branch_code", "item_no", "uom",
               "ordered_qty", "received_qty", "unit_cost_actual", "unit_cost_standard", "received_date"], po_lines)
    write_csv(DEALER_DIR / "invoice_lines.csv",
              ["invoice_no", "line_no", "invoice_date", "order_no", "customer_no", "branch_code",
               "item_no", "uom", "invoiced_qty", "unit_price", "unit_cost", "freight_amt", "tax_amt"], invoice_lines)
    write_csv(DEALER_DIR / "inventory_snapshots.csv",
              ["snapshot_date", "branch_code", "item_no", "on_hand_qty", "allocated_qty",
               "on_order_qty", "backorder_qty", "unit_cost", "inventory_value"], snapshots)
    write_csv(DEALER_DIR / "gl_entries.csv",
              ["journal_no", "line_no", "entry_date", "branch_code", "account", "description",
               "debit_amt", "credit_amt"], gl_entries)

    counts = {
        "items.csv": len(items), "salespeople.csv": len(people), "customers.csv": len(customers),
        "vendors.csv": len(vendors), "sales_order_lines.csv": len(order_lines),
        "purchase_order_lines.csv": len(po_lines), "invoice_lines.csv": len(invoice_lines),
        "inventory_snapshots.csv": len(snapshots), "gl_entries.csv": len(gl_entries),
    }

    write_reference_seeds()
    write_manifest(counts)

    print(f"seed written to {DEALER_DIR} and {REFERENCE_DIR}")
    for name, count in sorted(counts.items()):
        print(f"  {name}: {count} rows")


if __name__ == "__main__":
    main()

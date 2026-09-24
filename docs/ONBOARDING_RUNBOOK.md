# Onboarding runbook — acquiring a new dealer

Per-acquisition playbook: connect the ERP, validate the books, publish KPIs.
Day-1/30/60/90 shape; each phase gates the next.

## Day 1 — connect and register (target: same week as close)
1. **Inventory the ERP.** Identify the system (see `connectors/sources.yml`
   for the covered list) and its extraction surface:
   file export (CSV/SFTP), API, or read-only SQL replica. Capture access in
   the environment only — secrets never enter the repo or tickets.
2. **Enable or write the connector.** Working connector → add a source entry.
   Skeleton → follow `CONNECTOR_GUIDE.md` (afternoon scope for file-based
   surfaces). Backfill at least 24 months of history plus open orders/AR/AP.
3. **Register the source** in the control plane
   (`python -m connectors.cli register`), extract, and quarantine-check:
   zero unexplained quarantine records before proceeding.
4. **Land raw KPIs.** Run the dbt build for the new tenant's staging schema and
   eyeball staging row counts against the dealer's own reports (order count,
   invoice count, AR balance). Mismatch = extraction bug or missing feed.

## Day 30 — standardize and reconcile
1. **Crosswalk the masters.** Map source items, customers, vendors, and
   locations to golden-master IDs (`crosswalk_source_*` tables). Flag rather
   than merge ambiguous matches; record decisions.
2. **Map the chart of accounts.** Fill `ref_coa_mapping` for every source GL
   account with an effective window; unmapped accounts fail the marts build
   loudly (that is the control working).
3. **Reconcile to the books.** Tie GL entries to AR/AP/inventory balances the
   dealer closed with. Differences get a documented cause (timing, cutoff,
   scope) — do not adjust source extracts silently.
4. **First joint reporting pack.** GMROI, turns, gross margin, fill rates,
   DSO/DPO for the acquired dealer on the canonical model, side by side with
   their legacy reports for the same period.

## Day 60 — operationalize
1. **Schedule increments.** Move extraction from one-off backfills to
   Dagster-scheduled incremental runs; alert on stale watermarks and
   freshness checks.
2. **KPI sign-off.** Dealer GM and ops leads sign off on the canonical KPI
   values vs. their known numbers; open variances get owners.
3. **Onboard the team.** BI access via Superset RLS (per-dealer
   `source_system` isolation), connector on-call rotation, runbook review.

## Day 90 — consolidate and optimize
1. **Cross-dealer views.** Golden-master crosswalks complete enough for group
   roll-ups: same-item sales, vendor concentration, branch benchmarking.
2. **Procurement leverage.** Vendor fill rate + PPV marts feed buying-group
   negotiations across the group.
3. **Close acceleration.** Track close cycle time against the pre-acquisition
   baseline; target the next dealer close running through the platform in
   days, not weeks.
4. **Retire the legacy extraction** (SFTP dumps / direct DB reads) once two
   clean months of platform-side reconciliation are on record.

## Escalation triggers
- Reconciliation variance > 1% on revenue or inventory → stop, investigate.
- Quarantine rate > 0.5% of files for 3 consecutive runs → extraction bug.
- Any credential found outside the secret store → rotate immediately.

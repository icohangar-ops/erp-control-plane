"""CLI for the connector layer: plan (dry-run), register, extract, status.

Examples (run from the repo root):

    python -m connectors.cli plan                      # plan every source, no network
    python -m connectors.cli plan --source csvsftp_ridgeline
    python -m connectors.cli register                  # register enabled sources
    python -m connectors.cli extract --source csvsftp_ridgeline
    python -m connectors.cli status
"""

from __future__ import annotations

import argparse
import json
import sys

from connectors.base import BaseConnector, ConnectorError
from connectors.registry import build_registry_connectors
from control_plane.config import ControlPlaneConfig
from control_plane.store import open_store


def _pick(connectors: list[BaseConnector], source_id: str | None) -> list[BaseConnector]:
    if source_id is None:
        return connectors
    matched = [c for c in connectors if c.source.source_id == source_id]
    if not matched:
        known = ", ".join(c.source.source_id for c in connectors)
        raise ConnectorError(f"unknown source '{source_id}'; known sources: {known}")
    return matched


def cmd_plan(args: argparse.Namespace) -> int:
    connectors = _pick(build_registry_connectors(), args.source)
    for conn in connectors:
        print(json.dumps(conn.dry_run(), indent=2, default=str))
    return 0


def cmd_register(args: argparse.Namespace) -> int:
    config = ControlPlaneConfig.from_env()
    store = open_store(config)
    connectors = _pick(build_registry_connectors(config, store), args.source)
    failures = 0
    for conn in connectors:
        if not conn.source.enabled:
            print(f"o {conn.source.source_id}: disabled (template) — skipped")
            continue
        problems = conn.validate_config()
        if problems:
            failures += 1
            print(f"x {conn.source.source_id}: " + "; ".join(problems))
            continue
        registration = conn.register()
        print(
            f"+ {conn.source.source_id}: registered erp={conn.erp_id} "
            f"fingerprint={registration.config_fingerprint}"
        )
    return 1 if failures else 0


def cmd_extract(args: argparse.Namespace) -> int:
    config = ControlPlaneConfig.from_env()
    store = open_store(config)
    connectors = _pick(build_registry_connectors(config, store), args.source)
    failures = 0
    for conn in connectors:
        if not conn.source.enabled:
            print(f"o {conn.source.source_id}: disabled — skipped")
            continue
        try:
            conn.register()
        except ConnectorError as exc:
            failures += 1
            print(f"x {conn.source.source_id}: registration failed: {exc}")
            continue
        entities = [args.entity] if args.entity else conn.entities()
        for entity in entities:
            result = conn.extract(entity)
            print(
                f"+ {conn.source.source_id}/{entity}: {result.rows_extracted} rows -> "
                f"{result.parquet_path} (watermark {result.watermark_before!r} -> "
                f"{result.watermark_after!r})"
            )
    return 1 if failures else 0


def cmd_status(args: argparse.Namespace) -> int:
    config = ControlPlaneConfig.from_env()
    store = open_store(config)
    sources = store.list_sources()
    print(f"control plane: backend={config.backend} env={config.environment}")
    print(f"registered sources: {len(sources)}")
    for source in sources:
        print(f"  - {source.source_id} (erp={source.erp})")
    quarantine = store.list_quarantine()
    print(f"quarantined files: {len(quarantine)}")
    for record in quarantine:
        print(f"  ! {record.file_name}: {record.reason_code} — {record.detail[:100]}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="connectors.cli", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    plan = sub.add_parser("plan", help="dry-run: describe extraction plans without any network calls")
    plan.add_argument("--source", help="limit to one source_id")
    plan.set_defaults(func=cmd_plan)

    register = sub.add_parser("register", help="register enabled sources in the control plane")
    register.add_argument("--source", help="limit to one source_id")
    register.set_defaults(func=cmd_register)

    extract = sub.add_parser("extract", help="extract entities to Parquet (registering first)")
    extract.add_argument("--source", help="limit to one source_id")
    extract.add_argument("--entity", help="extract a single entity instead of all")
    extract.set_defaults(func=cmd_extract)

    status = sub.add_parser("status", help="show registered sources and quarantine state")
    status.set_defaults(func=cmd_status)

    args = parser.parse_args(argv)
    try:
        return int(args.func(args))  # type: ignore[attr-defined]
    except ConnectorError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

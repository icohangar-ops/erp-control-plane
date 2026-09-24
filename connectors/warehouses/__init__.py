"""Direct-connect warehouse configs (spec §5 last row).

The five analytical warehouses the FastAPI connectivity layer and dbt can
attach to — Snowflake, BigQuery, ClickHouse, Trino, Databricks. Deliberately
NOT connectors: there is no extraction surface here (spec §5 row 10 says
"connection configs only"), so this package parses and validates
``warehouses.yml`` into typed configs and nothing else.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

WAREHOUSES_YML = Path(__file__).with_name("warehouses.yml")


@dataclass(frozen=True)
class WarehouseConfig:
    """One direct-connect warehouse endpoint (connection config, no extraction)."""

    warehouse_id: str
    engine: str
    description: str
    driver: str
    read_only: bool
    settings: dict[str, str]
    required_settings: tuple[str, ...]

    def validate(self) -> list[str]:
        """Required settings must resolve to non-empty values (after interpolation)."""
        missing = [name for name in self.required_settings if not self.settings.get(name)]
        if missing:
            return [
                f"warehouse '{self.warehouse_id}' is missing required settings: "
                f"{', '.join(missing)} (connection stays unregistered until set)"
            ]
        return []


class WarehouseConfigError(Exception):
    """The warehouses.yml registry is malformed — a code/config bug, not runtime."""


def load_warehouse_configs(warehouses_path: Path = WAREHOUSES_YML) -> list[WarehouseConfig]:
    """Parse warehouses.yml into typed configs (env-resolved; no connections)."""
    if not warehouses_path.exists():
        raise WarehouseConfigError(f"warehouse registry file not found: {warehouses_path}")
    raw: dict[str, Any] = yaml.safe_load(warehouses_path.read_text(encoding="utf-8")) or {}
    entries = raw.get("warehouses")
    if not isinstance(entries, list):
        raise WarehouseConfigError(f"{warehouses_path} must declare a 'warehouses' list")
    configs: list[WarehouseConfig] = []
    seen: set[str] = set()
    for entry in entries:
        warehouse_id = entry.get("warehouse_id")
        engine = entry.get("engine")
        if not warehouse_id or not engine:
            raise WarehouseConfigError(f"warehouse entry missing warehouse_id/engine: {entry}")
        if warehouse_id in seen:
            raise WarehouseConfigError(f"duplicate warehouse_id in registry: {warehouse_id}")
        seen.add(warehouse_id)
        configs.append(
            WarehouseConfig(
                warehouse_id=warehouse_id,
                engine=engine,
                description=(entry.get("description", "") or "").strip(),
                driver=entry.get("driver", ""),
                read_only=bool(entry.get("read_only", True)),
                settings=_interpolate(entry.get("settings") or {}),
                required_settings=tuple(entry.get("required_settings") or ()),
            )
        )
    return configs


def _interpolate(raw: dict[str, Any]) -> dict[str, str]:
    """Resolve ${VAR:-default} references (same convention as sources.yml)."""
    from connectors.registry import resolve_env

    resolved: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(value, str):
            value = str(value)
        resolved[str(key)] = resolve_env(value)
    return resolved

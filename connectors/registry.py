"""Config-driven source registry.

``sources.yml`` is the single catalog of sources; this module resolves env-var
interpolation in settings, maps each ``erp`` id to its connector class, and
hands out constructed connectors. Adding a new acquisition = adding a block to
sources.yml + (when its ERP is new) a connector class — nothing else.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

from connectors.base import BaseConnector, ConnectorError
from connectors.bistrack.connector import BisTrackConnector
from connectors.csv_sftp import CsvSftpConnector
from connectors.d365_bc.connector import DynamicsBcConnector
from connectors.dmsi_agility.connector import DmsiAgilityConnector
from connectors.eci_spruce.connector import EciSpruceConnector
from connectors.epicor_eclipse.connector import EpicorEclipseConnector
from connectors.epicor_p21.connector import EpicorP21Connector
from connectors.netsuite.connector import NetsuiteConnector
from control_plane.config import ControlPlaneConfig
from control_plane.models import SourceConfig
from control_plane.store import ControlPlaneStore, open_store

_ENV_PATTERN = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}")

#: erp id -> connector class. Register new connectors here (and in sources.yml).
CONNECTOR_CLASSES: dict[str, type[BaseConnector]] = {
    CsvSftpConnector.erp_id: CsvSftpConnector,
    NetsuiteConnector.erp_id: NetsuiteConnector,
    BisTrackConnector.erp_id: BisTrackConnector,
    DmsiAgilityConnector.erp_id: DmsiAgilityConnector,
    EpicorP21Connector.erp_id: EpicorP21Connector,
    EpicorEclipseConnector.erp_id: EpicorEclipseConnector,
    EciSpruceConnector.erp_id: EciSpruceConnector,
    DynamicsBcConnector.erp_id: DynamicsBcConnector,
}

SOURCES_YML = Path(__file__).with_name("sources.yml")


def resolve_env(value: str) -> str:
    """Resolve ``${VAR}`` / ``${VAR:-default}`` references against the environment."""

    def _sub(match: re.Match[str]) -> str:
        name, default = match.group("name"), match.group("default")
        env_value = os.environ.get(name)
        if env_value is not None:
            return env_value
        if default is not None:
            return default
        raise ConnectorError(
            f"environment variable '{name}' is required by the source registry but not set"
        )

    return _ENV_PATTERN.sub(_sub, value)


def _interpolate_settings(raw: dict[str, Any]) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for key, value in raw.items():
        if not isinstance(value, str):
            value = str(value)
        resolved[str(key)] = resolve_env(value)
    return resolved


def load_source_configs(sources_path: Path = SOURCES_YML) -> list[SourceConfig]:
    """Parse sources.yml into typed source configs (env-resolved, not registered)."""
    if not sources_path.exists():
        raise ConnectorError(f"source registry file not found: {sources_path}")
    raw = yaml.safe_load(sources_path.read_text(encoding="utf-8")) or {}
    entries = raw.get("sources")
    if not isinstance(entries, list):
        raise ConnectorError(f"{sources_path} must declare a 'sources' list")

    configs: list[SourceConfig] = []
    seen: set[str] = set()
    for entry in entries:
        source_id = entry.get("source_id")
        erp = entry.get("erp")
        if not source_id or not erp:
            raise ConnectorError(f"source entry missing source_id/erp: {entry}")
        if source_id in seen:
            raise ConnectorError(f"duplicate source_id in registry: {source_id}")
        seen.add(source_id)
        if erp not in CONNECTOR_CLASSES:
            raise ConnectorError(
                f"source '{source_id}' references erp '{erp}' with no registered connector class; "
                f"known: {', '.join(sorted(CONNECTOR_CLASSES))}"
            )
        configs.append(
            SourceConfig(
                source_id=source_id,
                erp=erp,
                description=entry.get("description", "") or "",
                settings=_interpolate_settings(entry.get("settings") or {}),
                enabled=bool(entry.get("enabled", True)),
            )
        )
    return configs


def build_connector(
    source: SourceConfig,
    config: ControlPlaneConfig | None = None,
    store: ControlPlaneStore | None = None,
) -> BaseConnector:
    """Construct the connector for a source config."""
    connector_cls = CONNECTOR_CLASSES[source.erp]
    config = config or ControlPlaneConfig.from_env()
    store = store or open_store(config)
    return connector_cls(source, store, config)


def build_registry_connectors(
    config: ControlPlaneConfig | None = None, store: ControlPlaneStore | None = None
) -> list[BaseConnector]:
    """Construct connectors for every source in sources.yml (enabled or not)."""
    return [build_connector(source, config, store) for source in load_source_configs()]

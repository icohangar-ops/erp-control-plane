"""connectors — bespoke ERP extraction connectors for the control plane.

Every connector implements the :class:`connectors.base.BaseConnector` contract
and is registered in :mod:`connectors.registry` (driven by ``sources.yml``).
"""

from connectors.base import (
    PROVENANCE_COLUMNS,
    BaseConnector,
    ConnectorError,
    ConnectorMaturity,
    ConnectorNotConfigured,
    ConnectorNotImplemented,
    ExtractionMode,
    ExtractionPlan,
    ExtractedEntity,
)

__all__ = [
    "BaseConnector",
    "ConnectorError",
    "ConnectorMaturity",
    "ConnectorNotConfigured",
    "ConnectorNotImplemented",
    "ExtractionMode",
    "ExtractionPlan",
    "ExtractedEntity",
    "PROVENANCE_COLUMNS",
]

"""Cloud ERP REST connector package (Plex / Dynamics 365) — spec §5, 15-class matrix.

One connector class, per-tenant provider profiles (auth + pagination +
watermark knobs in settings); see :mod:`connectors.cloud_erp_rest.connector`.
"""

from connectors.cloud_erp_rest.connector import (
    ENTITY_MAPS,
    ENTITY_PATHS,
    PROFILES,
    CloudErpProfile,
    CloudErpRestConnector,
)

__all__ = [
    "ENTITY_MAPS",
    "ENTITY_PATHS",
    "PROFILES",
    "CloudErpProfile",
    "CloudErpRestConnector",
]

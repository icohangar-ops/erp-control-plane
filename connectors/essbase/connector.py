"""Oracle Essbase REST metadata connector.

The adapter uses the documented Essbase 21c REST v1 surfaces:

* ``GET /applications`` — application metadata
* ``GET /applications/{application}/databases`` — cube metadata

These are full snapshots, so the shared anti-join reconciliation handles
removed applications/cubes.  Essbase data exports are intentionally not mixed
into this metadata connector: Oracle's export API is an asynchronous job that
writes a ZIP to an Outbox and needs a tenant-specific download policy.
Validate the tenant's base path (normally ending in ``/essbase/rest/v1``)
before enabling a source.  Authentication supports Basic credentials and a
pre-issued bearer token; secrets are environment-only through ``sources.yml``.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, ClassVar
from urllib.parse import quote

import httpx

from connectors.base import (
    BaseConnector,
    ConnectorError,
    ConnectorMaturity,
    ExtractionMode,
    ExtractionPlan,
)


class EssbaseConnector(BaseConnector):
    """Extract Essbase applications and cubes as full metadata snapshots."""

    erp_id = "essbase"
    maturity = ConnectorMaturity.IMPLEMENTED
    full_snapshot = True
    extraction_notes = (
        "Oracle Essbase REST v1 metadata: applications and application databases "
        "(cubes), paged with offset/limit.  The tenant base_url must include the "
        "Essbase REST context, normally /essbase/rest/v1.  Metadata is a full "
        "snapshot; Essbase export jobs and Outbox ZIP downloads are a separate "
        "tenant/onboarding surface and are not silently inferred here."
    )
    natural_key_fields: ClassVar[dict[str, tuple[str, ...]]] = {
        "applications": ("application_name",),
        "cubes": ("application_name", "cube_name"),
    }
    PAGE_SIZE = 100
    PAGE_LIMIT = 10_000

    def __init__(self, source, store, config) -> None:
        super().__init__(source, store, config)
        self._http_client: httpx.Client | None = None

    def entities(self) -> list[str]:
        return list(self.natural_key_fields)

    def validate_config(self) -> list[str]:
        settings = self.source.settings
        problems: list[str] = []
        if not (settings.get("base_url") or "").strip():
            problems.append("base_url is required (Essbase REST v1 root)")
        auth_mode = (settings.get("auth_mode") or "basic").strip().lower()
        if auth_mode not in {"basic", "bearer"}:
            problems.append("auth_mode must be 'basic' or 'bearer'")
        elif auth_mode == "basic":
            if not (settings.get("username") or "").strip():
                problems.append("username is required when auth_mode=basic")
            if not (settings.get("password") or "").strip():
                problems.append("password is required when auth_mode=basic")
        elif not (settings.get("token") or "").strip():
            problems.append("token is required when auth_mode=bearer")
        return problems

    def describe_extraction(self, entity: str) -> ExtractionPlan:
        if entity not in self.entities():
            raise ConnectorError(
                f"entity '{entity}' is not exposed by {self.erp_id}; "
                f"available: {', '.join(self.entities())}"
            )
        surface = (
            "GET {base_url}/applications with offset/limit pagination"
            if entity == "applications"
            else "GET {base_url}/applications/{application}/databases with offset/limit pagination"
        )
        return ExtractionPlan(
            entity=entity,
            surface=surface,
            incremental_key=None,
            notes=self.extraction_notes,
        )

    def _iter_records(
        self, entity: str, mode: ExtractionMode, watermark: str | None
    ) -> Iterator[dict[str, object]]:
        # Essbase metadata list endpoints do not expose a portable modified
        # watermark; the full snapshot remains the safe incremental behavior.
        if entity == "applications":
            for record in self._paged("applications"):
                yield self._application_record(record)
            return
        if entity == "cubes":
            for app in self._paged("applications"):
                app_name = self._name(app)
                if not app_name:
                    raise ConnectorError("Essbase application response is missing name")
                for record in self._paged(f"applications/{quote(app_name, safe='')}/databases"):
                    yield self._cube_record(app_name, record)
            return
        raise ConnectorError(f"unsupported Essbase entity: {entity}")

    def _application_record(self, record: dict[str, Any]) -> dict[str, object]:
        name = self._name(record)
        if not name:
            raise ConnectorError("Essbase application response is missing name")
        return {
            "application_name": name,
            "owner": record.get("owner"),
            "status": record.get("status"),
            "application_type": record.get("type"),
            "description": record.get("description"),
            "modified_time": record.get("modifiedTime"),
        }

    def _cube_record(self, application_name: str, record: dict[str, Any]) -> dict[str, object]:
        name = self._name(record)
        if not name:
            raise ConnectorError(
                f"Essbase database response for {application_name!r} is missing name"
            )
        return {
            "application_name": application_name,
            "cube_name": name,
            "owner": record.get("owner"),
            "status": record.get("status"),
            "cube_type": record.get("type") or record.get("databaseType"),
            "description": record.get("description"),
            "modified_time": record.get("modifiedTime"),
        }

    def _paged(self, path: str) -> Iterator[dict[str, Any]]:
        offset = 0
        for _ in range(self.PAGE_LIMIT):
            payload = self._get_json(path, {"offset": str(offset), "limit": str(self.PAGE_SIZE)})
            if isinstance(payload, list):
                records = payload
                total = None
            elif isinstance(payload, dict) and isinstance(payload.get("items"), list):
                records = payload["items"]
                total = payload.get("totalResults")
            else:
                raise ConnectorError(
                    f"Essbase {path} response must contain an items list; got "
                    f"{type(payload).__name__}"
                )
            for record in records:
                if not isinstance(record, dict):
                    raise ConnectorError(f"Essbase {path} returned a non-object item")
                yield record
            offset += len(records)
            if not records or (isinstance(total, int) and offset >= total) or len(records) < self.PAGE_SIZE:
                return
        raise ConnectorError(f"Essbase {path} exceeded the {self.PAGE_LIMIT} page safety limit")

    def _get_json(self, path: str, params: dict[str, str]) -> Any:
        base_url = (self.source.settings.get("base_url") or "").strip().rstrip("/")
        response = self._client().get(f"{base_url}/{path.lstrip('/')}", params=params, headers=self._headers())
        try:
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise ConnectorError(f"Essbase REST call failed: {exc}") from exc
        return response.json()

    def _headers(self) -> dict[str, str]:
        settings = self.source.settings
        if (settings.get("auth_mode") or "basic").strip().lower() == "bearer":
            return {"Authorization": f"Bearer {settings['token']}", "Accept": "application/json"}
        return {"Accept": "application/json"}

    def _client(self) -> httpx.Client:
        if self._http_client is None:
            auth_mode = (self.source.settings.get("auth_mode") or "basic").strip().lower()
            auth = None
            if auth_mode == "basic":
                auth = (
                    self.source.settings.get("username", ""),
                    self.source.settings.get("password", ""),
                )
            self._http_client = httpx.Client(timeout=60.0, auth=auth)
        return self._http_client

    @staticmethod
    def _name(record: dict[str, Any]) -> str:
        return str(record.get("name") or record.get("applicationName") or record.get("databaseName") or "").strip()

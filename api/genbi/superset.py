"""Superset REST client for the GenBI persistence loop (spec §4.2).

A second caller of the v0.1 idempotent provisioning loop
(``analytics/superset/build_dashboard.py``), not a new mechanism:

1. ``POST /api/v1/security/login`` -> access token
2. ``GET  /api/v1/security/csrf_token/`` with the Bearer token -> CSRF token
   (+ session cookie, held by the httpx client); mutating calls carry the CSRF
   header (both header spellings, matching the runbook and the SPA) and Referer
3. dataset create-or-update by ``table_name`` — ``database`` is ALWAYS the
   caller-supplied READ_ONLY DuckDB database id
4. chart create-or-update by ``slice_name`` (the deterministic slug)
5. dashboard create-or-update by ``slug``, layout written the runbook-proven
   way: positions nested under ``json_metadata``

The transport (``httpx.Client``) is injectable so tests drive a fake Superset.
"""

from __future__ import annotations

import json
from typing import Any

import httpx


class SupersetError(Exception):
    """A Superset REST call failed; carries the status code and response body."""

    def __init__(self, method: str, path: str, status: int, body: str) -> None:
        super().__init__(f"Superset {method} {path} failed with {status}: {body[:300]}")
        self.method = method
        self.path = path
        self.status = status
        self.body = body


class SupersetClient:
    """Session-scoped client: lazy login, CSRF headers on every mutating call."""

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        *,
        client: httpx.Client | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self._client = client or httpx.Client(timeout=timeout)
        self._token: str | None = None
        self._csrf: str | None = None

    # ------------------------------------------------------------ auth
    def authenticate(self) -> None:
        """POST login -> Bearer token; GET csrf_token/ -> CSRF token + cookie."""
        response = self._client.post(
            f"{self.base_url}/api/v1/security/login",
            json={
                "username": self.username,
                "password": self.password,
                "provider": "db",
                "refresh": True,
            },
        )
        self._raise(response, "POST", "/api/v1/security/login")
        self._token = response.json()["access_token"]
        response = self._client.get(
            f"{self.base_url}/api/v1/security/csrf_token/",
            headers={"Authorization": f"Bearer {self._token}"},
        )
        self._raise(response, "GET", "/api/v1/security/csrf_token/")
        self._csrf = response.json()["result"]

    def _headers(self) -> dict[str, str]:
        if not self._token or not self._csrf:
            self.authenticate()
        return {
            "Authorization": f"Bearer {self._token}",
            "X-CSRFToken": self._csrf or "",
            "X-CSRF-Token": self._csrf or "",
            "Referer": self.base_url,
        }

    # ------------------------------------------------------------ plumbing
    def request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        response = self._client.request(
            method, f"{self.base_url}{path}", headers=self._headers(), **kwargs
        )
        self._raise(response, method, path)
        body = response.text.strip()
        return json.loads(body) if body else {}

    @staticmethod
    def _raise(response: httpx.Response, method: str, path: str) -> None:
        if response.is_error:
            raise SupersetError(method, path, response.status_code, response.text)

    def find_one(self, path: str, col: str, value: Any) -> dict[str, Any] | None:
        """First result of an ``eq`` filter on ``col`` — the runbook's find_one."""
        query = json.dumps({"filters": [{"col": col, "opr": "eq", "value": value}]})
        result = self.request("GET", path, params={"q": query})
        items = result.get("result", [])
        return items[0] if items else None

    # ------------------------------------------------------------ objects
    def get_database(self, database_id: int) -> dict[str, Any]:
        result = self.request("GET", f"/api/v1/database/{database_id}")
        item = result.get("result", {})
        if "sqlalchemy_uri" not in item:
            # Superset 4.1.1 omits sqlalchemy_uri from the item schema (it is in
            # list_select_columns only, and the list endpoint does not allow
            # filtering by id). Page the list and match the id so the spec
            # §4.2.6 server-side READ_ONLY check can run against the real URI.
            page = 0
            listed: dict[str, Any] | None = None
            while listed is None and page < 10:
                query = json.dumps({"page": page, "page_size": 100})
                rows = self.request("GET", "/api/v1/database/", params={"q": query}).get(
                    "result", []
                )
                listed = next((r for r in rows if int(r.get("id", -1)) == database_id), None)
                if listed is None and len(rows) < 100:
                    break
                page += 1
            if listed and "sqlalchemy_uri" in listed:
                item = {**listed, **item}
        return item

    def update_database(self, database_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        """PUT a database; the response echoes the STORED fields (incl. the URI).

        Used as the read-back for the spec §4.2.6 check on Superset builds that
        hide ``sqlalchemy_uri`` on every read path: re-asserting an unchanged
        field returns the server-side stored value without altering it.
        """
        result = self.request("PUT", f"/api/v1/database/{database_id}", json=payload)
        return result.get("result", {})

    def ensure_dataset(
        self,
        database_id: int,
        table_name: str,
        *,
        schema: str | None = None,
        sql: str | None = None,
    ) -> tuple[int, bool]:
        """Create-or-update-by-name dataset; returns (id, created)."""
        existing = self.find_one("/api/v1/dataset/", "table_name", table_name)
        if existing:
            return int(existing["id"]), False
        payload: dict[str, Any] = {"database": database_id, "table_name": table_name}
        if schema:
            payload["schema"] = schema
        if sql:
            payload["sql"] = sql
        result = self.request("POST", "/api/v1/dataset/", json=payload)
        return int(result["id"]), True

    def chart_uuid(self, chart_id: int) -> str:
        result = self.request("GET", f"/api/v1/chart/{chart_id}")
        return str(result.get("result", {}).get("uuid", ""))

    def ensure_chart(
        self,
        slice_name: str,
        *,
        viz_type: str,
        datasource_id: int,
        params: dict[str, Any],
        description: str = "",
    ) -> tuple[int, str, bool]:
        """Create-or-update-by-slug chart; returns (id, uuid, created)."""
        payload = {
            "slice_name": slice_name,
            "viz_type": viz_type,
            "datasource_id": datasource_id,
            "datasource_type": "table",
            "params": json.dumps(params),
            "description": description,
        }
        existing = self.find_one("/api/v1/chart/", "slice_name", slice_name)
        if existing:
            chart_id = int(existing["id"])
            self.request("PUT", f"/api/v1/chart/{chart_id}", json=payload)
            uuid_value = str(existing.get("uuid") or "") or self.chart_uuid(chart_id)
            return chart_id, uuid_value, False
        result = self.request("POST", "/api/v1/chart/", json=payload)
        chart_id = int(result["id"])
        uuid_value = str(result.get("uuid") or "") or self.chart_uuid(chart_id)
        return chart_id, uuid_value, True

    def ensure_dashboard(self, slug: str, title: str) -> tuple[int, bool]:
        """Create-or-update-by-slug dashboard (slugs are unique in Superset)."""
        existing = self.find_one("/api/v1/dashboard/", "slug", slug)
        if existing:
            return int(existing["id"]), False
        result = self.request(
            "POST",
            "/api/v1/dashboard/",
            json={"dashboard_title": title, "slug": slug, "published": True},
        )
        return int(result["id"]), True

    # ------------------------------------------------------------ layout
    def get_positions(self, dashboard_id: int) -> dict[str, Any]:
        """Current v2 layout, or {} when the dashboard has none yet.

        Prefers ``position_json`` (the canonical column the runbook writes via
        ``DashboardDAO.set_dash_metadata``), falling back to ``positions`` inside
        ``json_metadata``.
        """
        result = self.request("GET", f"/api/v1/dashboard/{dashboard_id}").get("result", {})
        raw = result.get("position_json") or ""
        if isinstance(raw, str) and raw.strip():
            return json.loads(raw)
        metadata = result.get("json_metadata") or ""
        if isinstance(metadata, str) and metadata.strip():
            parsed = json.loads(metadata)
            return parsed.get("positions", {})
        return {}

    def put_positions(self, dashboard_id: int, positions: dict[str, Any]) -> None:
        """Write the layout the only way the SPA survives (runbook constraint 1):
        positions nested under ``json_metadata``."""
        json_metadata = {
            "refresh_frequency": 0,
            "default_filters": "{}",
            "native_filter_configuration": [],
            "chart_configuration": {},
            "color_scheme_domain": [],
            "label_colors": {},
            "shared_label_colors": {},
            "expanded_slices": {},
            "cross_filters_enabled": True,
            "positions": positions,
        }
        self.request(
            "PUT",
            f"/api/v1/dashboard/{dashboard_id}",
            json={"json_metadata": json.dumps(json_metadata), "published": True},
        )

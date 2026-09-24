"""Minimal API-boundary authentication for sensitive control-plane routes.

Production callers must send ``X-Control-Plane-API-Key``. Local/test/demo mode
may omit it for the documented offline fixtures only.
"""

from __future__ import annotations

import hmac
import os

from fastapi import Header, HTTPException, status


def require_control_plane_api_key(
    x_control_plane_api_key: str | None = Header(default=None),
) -> None:
    environment = os.getenv("ENVIRONMENT", "production").strip().lower()
    configured = os.getenv("CONTROL_PLANE_API_KEY", "").strip()
    if environment in {"local", "test", "demo"} and not configured:
        return
    if not configured:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="CONTROL_PLANE_API_KEY is not configured",
        )
    if not x_control_plane_api_key or not hmac.compare_digest(x_control_plane_api_key, configured):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid control-plane API key",
        )

import pytest
from fastapi import HTTPException

from api.auth import require_control_plane_api_key


def test_local_mode_allows_offline_fixture(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "test")
    monkeypatch.delenv("CONTROL_PLANE_API_KEY", raising=False)
    assert require_control_plane_api_key(None) is None


def test_production_requires_and_validates_api_key(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("CONTROL_PLANE_API_KEY", "expected")
    with pytest.raises(HTTPException) as missing:
        require_control_plane_api_key(None)
    assert missing.value.status_code == 401
    assert require_control_plane_api_key("expected") is None

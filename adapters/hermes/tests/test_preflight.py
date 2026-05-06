"""Unit tests for the Hermes adapter preflight check."""
from __future__ import annotations

import httpx
import pytest
import respx


@pytest.fixture(autouse=True)
def _hermes_env(monkeypatch):
    monkeypatch.setenv("HERMES_BASE_URL", "http://localhost:8642")
    monkeypatch.setenv("HERMES_TOKEN", "test-token")
    monkeypatch.setenv("HERMES_MODEL", "hermes-test")
    monkeypatch.setenv("HERMES_TIMEOUT_SEC", "10")


@pytest.mark.asyncio
@respx.mock
async def test_preflight_passes_when_models_endpoint_returns_200():
    from adapters.hermes.adapter import preflight, PreflightError  # noqa: F401
    respx.get("http://localhost:8642/v1/models").mock(
        return_value=httpx.Response(200, json={"data": [{"id": "hermes"}]}))
    await preflight()  # must not raise


@pytest.mark.asyncio
@respx.mock
async def test_preflight_fails_on_connect_error():
    from adapters.hermes.adapter import preflight, PreflightError
    respx.get("http://localhost:8642/v1/models").mock(
        side_effect=httpx.ConnectError("refused"))
    with pytest.raises(PreflightError, match="hermes_unreachable"):
        await preflight()


@pytest.mark.asyncio
@respx.mock
async def test_preflight_fails_on_401():
    from adapters.hermes.adapter import preflight, PreflightError
    respx.get("http://localhost:8642/v1/models").mock(
        return_value=httpx.Response(401, json={"error": "unauthorized"}))
    with pytest.raises(PreflightError, match="hermes_auth_failed"):
        await preflight()


@pytest.mark.asyncio
@respx.mock
async def test_preflight_fails_on_500():
    from adapters.hermes.adapter import preflight, PreflightError
    respx.get("http://localhost:8642/v1/models").mock(
        return_value=httpx.Response(500))
    with pytest.raises(PreflightError, match="hermes_unhealthy"):
        await preflight()


@pytest.mark.asyncio
@respx.mock
async def test_preflight_fails_on_timeout():
    from adapters.hermes.adapter import preflight, PreflightError
    respx.get("http://localhost:8642/v1/models").mock(
        side_effect=httpx.ReadTimeout("slow"))
    with pytest.raises(PreflightError, match="hermes_unreachable"):
        await preflight()

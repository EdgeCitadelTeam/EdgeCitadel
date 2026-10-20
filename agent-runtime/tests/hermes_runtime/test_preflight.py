"""Unit tests for the Hermes Plugin preflight check."""

from __future__ import annotations

import httpx
import pytest
import respx


@pytest.fixture(autouse=True)
def _hermes_env(monkeypatch, tmp_path):
    token_file = tmp_path / "hermes-token"
    token_file.write_text("test-token\n")
    monkeypatch.setenv("HERMES_BASE_URL", "http://localhost:8642")
    monkeypatch.setenv("HERMES_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("HERMES_MODEL", "hermes-test")
    monkeypatch.setenv("HERMES_TIMEOUT_SEC", "10")


@pytest.mark.asyncio
@respx.mock
async def test_preflight_passes_when_models_endpoint_returns_200():
    from edgecitadel_hermes_plugin.adapter import preflight, PreflightError  # noqa: F401

    respx.get("http://localhost:8642/v1/models").mock(
        return_value=httpx.Response(200, json={"data": [{"id": "hermes"}]})
    )
    await preflight()  # must not raise


@pytest.mark.asyncio
@respx.mock
async def test_preflight_fails_on_connect_error():
    from edgecitadel_hermes_plugin.adapter import preflight, PreflightError

    respx.get("http://localhost:8642/v1/models").mock(
        side_effect=httpx.ConnectError("refused")
    )
    with pytest.raises(PreflightError, match="hermes_unreachable"):
        await preflight()


@pytest.mark.asyncio
@respx.mock
async def test_preflight_fails_on_401():
    from edgecitadel_hermes_plugin.adapter import preflight, PreflightError

    respx.get("http://localhost:8642/v1/models").mock(
        return_value=httpx.Response(401, json={"error": "unauthorized"})
    )
    with pytest.raises(PreflightError, match="hermes_auth_failed"):
        await preflight()


@pytest.mark.asyncio
@respx.mock
async def test_preflight_fails_on_403():
    from edgecitadel_hermes_plugin.adapter import preflight, PreflightError

    respx.get("http://localhost:8642/v1/models").mock(
        return_value=httpx.Response(403, json={"error": "forbidden"})
    )
    with pytest.raises(PreflightError, match="hermes_auth_failed"):
        await preflight()


@pytest.mark.asyncio
@respx.mock
async def test_preflight_fails_on_500():
    from edgecitadel_hermes_plugin.adapter import preflight, PreflightError

    respx.get("http://localhost:8642/v1/models").mock(return_value=httpx.Response(500))
    with pytest.raises(PreflightError, match="hermes_unhealthy"):
        await preflight()


@pytest.mark.asyncio
@respx.mock
async def test_preflight_fails_on_timeout():
    from edgecitadel_hermes_plugin.adapter import preflight, PreflightError

    respx.get("http://localhost:8642/v1/models").mock(
        side_effect=httpx.ReadTimeout("slow")
    )
    with pytest.raises(PreflightError, match="hermes_unreachable"):
        await preflight()


@pytest.mark.asyncio
async def test_preflight_rejects_empty_token_file(monkeypatch, tmp_path):
    from edgecitadel_hermes_plugin.adapter import preflight

    token_file = tmp_path / "empty-token"
    token_file.write_text("\n")
    monkeypatch.setenv("HERMES_TOKEN_FILE", str(token_file))
    with pytest.raises(ValueError, match="HERMES_TOKEN_FILE is empty"):
        await preflight()

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_dashboard_ui_html_served(async_client: AsyncClient):
    """Verifies that GET /dashboard/ serves the production HTML interface."""
    res = await async_client.get("/dashboard/", follow_redirects=True)
    assert res.status_code == 200
    assert "text/html" in res.headers.get("content-type", "")
    content = res.text
    assert "<title>DISTASKENG // Real-Time Topology Dashboard</title>" in content
    assert "id=\"graph-svg\"" in content
    assert "id=\"inspector-panel\"" in content
    assert "id=\"event-feed-panel\"" in content
    assert "https://cdn.jsdelivr.net/npm/d3@7" in content

    # Test without trailing slash
    res_no_slash = await async_client.get("/dashboard", follow_redirects=True)
    assert res_no_slash.status_code == 200
    assert "<title>DISTASKENG // Real-Time Topology Dashboard</title>" in res_no_slash.text


@pytest.mark.asyncio
async def test_dashboard_static_assets(async_client: AsyncClient):
    """Verifies that dashboard CSS and JS assets are served cleanly."""
    css_res = await async_client.get("/dashboard/style.css")
    assert css_res.status_code == 200
    assert "--bg-root: #020404" in css_res.text

    js_res = await async_client.get("/dashboard/app.js")
    assert js_res.status_code == 200
    assert "DISTASKENG Topology Dashboard" in js_res.text

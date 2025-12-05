import pytest
from httpx import AsyncClient
from app import app

import asyncio

@pytest.mark.asyncio
async def test_healthz():
    async with AsyncClient(app=app, base_url="http://test") as ac:
        r = await ac.get("/healthz")
        assert r.status_code == 200
        assert "status" in r.json()

"""Scraping the four data streams from every demo-website replica."""

from __future__ import annotations

import asyncio

import httpx

from . import config

STREAM_PATHS = {
    "frontend": "/logs/frontend",
    "server": "/logs/server",
    "database": "/metrics/db",
    "users": "/reports/users",
}


async def _get(client: httpx.AsyncClient, url: str):
    try:
        r = await client.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:  # noqa: BLE001
        return {"_error": f"{type(e).__name__}: {e}"[:160]}


async def fetch_stream(role: str, limit: int = 50, targets: list[str] | None = None) -> dict:
    """Return {"records": [...], "status": {instance: status}, "unreachable": [...]}.

    Every replica is scraped directly (not through the load balancer), the
    way Prometheus scrapes each pod -- otherwise the LB would hand us a
    random replica's logs each time.
    """
    targets = targets or config.DEMO_TARGETS
    path = STREAM_PATHS[role]
    async with httpx.AsyncClient() as client:
        data = await asyncio.gather(*[_get(client, f"{t}{path}?limit={limit}") for t in targets])
        statuses = await asyncio.gather(*[_get(client, f"{t}/admin/status") for t in targets])

    records, status, unreachable = [], {}, []
    for t, d, s in zip(targets, data, statuses):
        if isinstance(d, dict) and "_error" in d:
            unreachable.append(t)
            continue
        inst = s.get("instance", t) if isinstance(s, dict) else t
        status[inst] = s
        for rec in d:
            rec.setdefault("instance", inst)
            records.append(rec)
    records.sort(key=lambda r: r.get("timestamp", ""))
    return {"records": records, "status": status, "unreachable": unreachable}


async def fetch_all(limit: int = 50, targets: list[str] | None = None) -> dict:
    roles = list(STREAM_PATHS)
    results = await asyncio.gather(*[fetch_stream(r, limit, targets) for r in roles])
    return dict(zip(roles, results))

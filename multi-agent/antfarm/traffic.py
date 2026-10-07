"""Async traffic generator: hammers the load-balanced site and measures
how the load balancer spread the requests across replicas."""

from __future__ import annotations

import asyncio
import time
from collections import Counter

import httpx


def _pct(sorted_vals, q):
    if not sorted_vals:
        return 0.0
    return round(sorted_vals[min(len(sorted_vals) - 1, int(q * len(sorted_vals)))], 1)


DEMO_LOGIN = {"email": "demo@sideb.store", "password": "dropday2026"}


async def generate_traffic(base_url: str, count: int = 60, concurrency: int = 10,
                           path: str = "/api/checkout", rps: float | None = None, mix: str = "checkout") -> dict:
    """mix: "checkout" (all checkouts), "login" (all sign-ins), "mixed" (alternating) --
    a shopper signs in, then checks out."""
    sem = asyncio.Semaphore(concurrency)
    by_instance, by_status, by_inst_status = Counter(), Counter(), Counter()
    latencies: list[float] = []
    gap = 1.0 / rps if rps else 0.0

    async with httpx.AsyncClient(timeout=30) as client:
        async def one(i: int):
            if gap:
                await asyncio.sleep(i * gap)
            async with sem:
                t0 = time.perf_counter()
                try:
                    is_login = mix == "login" or (mix == "mixed" and i % 2 == 0)
                    if is_login:
                        r = await client.post(f"{base_url}/auth/login", json=DEMO_LOGIN)
                    else:
                        r = await client.post(f"{base_url}{path}")
                    inst = r.headers.get("X-Instance", "unknown")
                    code = r.status_code
                except Exception as e:  # noqa: BLE001
                    inst, code = "unreachable", type(e).__name__
                latencies.append((time.perf_counter() - t0) * 1000)
                by_instance[inst] += 1
                by_status[str(code)] += 1
                by_inst_status[f"{inst}:{code}"] += 1

        t_start = time.perf_counter()
        await asyncio.gather(*[one(i) for i in range(count)])
        wall = time.perf_counter() - t_start

    lat = sorted(latencies)
    ok = sum(n for c, n in by_status.items() if c.startswith("2"))
    return {
        "target": base_url + (path if mix == "checkout" else f" ({mix}: /auth/login + {path})"),
        "requests": count,
        "concurrency": concurrency,
        "wall_time_s": round(wall, 2),
        "throughput_rps": round(count / wall, 1) if wall else 0,
        "success_rate_pct": round(100 * ok / count, 1) if count else 0,
        "by_instance": dict(sorted(by_instance.items())),
        "by_status": dict(by_status),
        "by_instance_status": dict(sorted(by_inst_status.items())),
        "latency_ms": {"p50": _pct(lat, 0.5), "p95": _pct(lat, 0.95), "p99": _pct(lat, 0.99),
                       "max": round(lat[-1], 1) if lat else 0},
    }

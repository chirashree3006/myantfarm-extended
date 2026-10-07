"""
Coordinator microservice (2 replicas behind the load balancer).

    POST /incident/analyze  {"mode": "extended" | "paper", "source": "live" | "paper_static",
                             "scenario": "<for DQ scoring>"}
         extended (C3x): 4 monitoring agents IN PARALLEL -> fusion -> plan -> team tickets
         paper    (C3) : Diagnosis -> Planner -> Risk agents in sequence (the paper's design)
    POST /incident/remediate -> apply the mitigation on the demo site
    POST /control/reset | /control/fault | /control/traffic | /control/restock
    GET  /control/status, /health, /metrics
    POST /chaos/down|up

If an agent service is unreachable the coordinator runs that agent
in-process (degraded mode) instead of failing the whole analysis.
"""

import asyncio
import time

import httpx
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from antfarm import chaos, config
from antfarm.agents import ROLES, run_agent
from antfarm.baseline import build_incident_description
from antfarm.fusion import brief_actions, fuse
from antfarm.llm import get_client
from antfarm.paper import PAPER_INCIDENT, run_c3_paper
from antfarm.scoring import GROUND_TRUTH, score_actions
from antfarm.streams import fetch_all, fetch_stream
from antfarm.traffic import generate_traffic

app = FastAPI(title="multi-agent coordinator")
chaos.install(app)


async def _fan(method: str, path: str, json=None) -> dict:
    async with httpx.AsyncClient(timeout=30) as c:
        async def one(t):
            try:
                r = await c.request(method, f"{t}{path}", json=json)
                return r.json() if r.status_code < 500 else {"error": f"HTTP {r.status_code}", "target": t}
            except Exception as e:  # noqa: BLE001
                return {"error": type(e).__name__, "target": t}
        res = await asyncio.gather(*[one(t) for t in config.DEMO_TARGETS])
    return {(r.get("instance") or r.get("target", "").split("//")[-1].split(":")[0] or f"t{i}"): r
            for i, r in enumerate(res)}


async def _call_agent(client: httpx.AsyncClient, role: str, limit: int, use_llm: bool) -> dict:
    t0 = time.perf_counter()
    try:
        r = await client.post(f"{config.AGENT_URLS[role]}/analyze", json={"limit": limit, "use_llm": use_llm})
        r.raise_for_status()
        f = r.json()
        f["mode"] = "remote"
        f["via"] = r.headers.get("X-Upstream", "")
    except Exception as e:  # noqa: BLE001
        stream = await fetch_stream(role, limit)
        f = await run_agent(role, stream, get_client() if use_llm else None)
        f["mode"] = "local-fallback"
        f["fallback_reason"] = type(e).__name__
    f["round_trip_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    return f


class AnalyzeRequest(BaseModel):
    limit: int = 50
    scenario: str | None = None
    mode: str = "extended"     # extended (C3x) | paper (C3)
    source: str = "live"       # live | paper_static
    use_llm: bool = True


async def _extended(req: AnalyzeRequest) -> dict:
    t0 = time.perf_counter()
    async with httpx.AsyncClient(timeout=config.LLM_TIMEOUT_S + 30) as client:
        results = await asyncio.gather(*[_call_agent(client, r, req.limit, req.use_llm) for r in ROLES])
    findings = dict(zip(ROLES, results))
    t_agents = time.perf_counter()
    status = {k: v for k, v in (await _fan("GET", "/admin/status")).items() if "error" not in v}
    brief = await fuse(findings, status, get_client() if req.use_llm else None)
    out = {"condition": "C3x", "mode": "extended", "served_by": config.INSTANCE_ID, "brief": brief,
           "agents": findings,
           "degraded_agents": [r for r, f in findings.items()
                               if f.get("mode") == "local-fallback" or "budget" in f.get("llm", {}).get("error", "")],
           "timing_ms": {"agents_parallel": round((t_agents - t0) * 1000, 1),
                         "fusion_and_synthesis": round((time.perf_counter() - t_agents) * 1000, 1),
                         "total": round((time.perf_counter() - t0) * 1000, 1)}}
    if req.scenario in GROUND_TRUTH:
        acts = brief_actions(brief)
        out["dq"] = score_actions(acts, req.scenario, brief["summary"] + "\n" + "\n".join(acts))
    return out


async def _paper(req: AnalyzeRequest) -> dict:
    t0 = time.perf_counter()
    incident = PAPER_INCIDENT if req.source == "paper_static" else build_incident_description(await fetch_all(req.limit))
    res = await run_c3_paper(incident, get_client())
    out = {"condition": "C3", "mode": "paper", "served_by": config.INSTANCE_ID, **res,
           "incident_description": incident, "timing_ms": {"total": round((time.perf_counter() - t0) * 1000, 1)}}
    if req.scenario in GROUND_TRUTH:
        out["dq"] = score_actions(res["actions"], req.scenario, res["brief"])
    return out


@app.post("/incident/analyze")
async def analyze(req: AnalyzeRequest):
    if req.mode == "paper":
        return await _paper(req)
    if req.source == "paper_static":
        return JSONResponse({"error": "extended mode needs live telemetry; use mode=paper for paper_static"},
                            status_code=400)
    return await _extended(req)


class RemediateRequest(BaseModel):
    dry_run: bool = True


@app.post("/incident/remediate")
async def remediate(req: RemediateRequest):
    """Closed loop: re-diagnose (rules only, fast), then apply the priority-1
    mitigation on the demo site (restart = reset replica, rollback = fault off)."""
    result = await _extended(AnalyzeRequest(use_llm=False))
    brief = result["brief"]
    rid = brief["diagnosis"]["id"]
    plan = []
    if rid not in ("none", "unknown"):
        first = brief["action_plan"][0]
        plan = [f"{first['type']}: {first['action']}"] + \
               [f"POST /admin/reset on {i}" for i in (brief["affected_instances"] or ["all replicas"])]
    if req.dry_run or not plan:
        return {"diagnosis": rid, "dry_run": True, "would_execute": plan}
    executed = await _fan("POST", "/admin/reset")
    return {"diagnosis": rid, "dry_run": False, "executed": plan, "results": executed}


class FaultRequest(BaseModel):
    mode: str = "leak"


class TrafficRequest(BaseModel):
    count: int = 60
    concurrency: int = 10
    mix: str = "mixed"   # checkout | login | mixed


@app.post("/control/reset")
async def control_reset():
    return await _fan("POST", "/admin/reset")


@app.post("/control/restock")
async def control_restock():
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(f"{config.DEMO_LB_URL}/admin/restock")
    return r.json()


@app.post("/control/fault")
async def control_fault(req: FaultRequest):
    return await _fan("POST", "/admin/fault", json={"mode": req.mode})


@app.post("/control/traffic")
async def control_traffic(req: TrafficRequest):
    """Real user-style traffic (sign-ins + checkouts) through the website load balancer."""
    return await generate_traffic(config.DEMO_LB_URL, min(req.count, 2000), min(req.concurrency, 100), mix=req.mix)


# ---------------------------------------------------------------------------
# Traffic surge (flash-sale flood) + live numbers for the backend console
# ---------------------------------------------------------------------------
surge_stats = {"running": False, "sent": 0, "ok": 0, "rejected": 0, "queued": 0, "started": 0.0, "until": 0.0, "gen": 0}


async def _flood(seconds: int, concurrency: int):
    """Shoppers stampede the drop: browse, sign in, check out -- as fast as they can."""
    surge_stats["gen"] += 1
    gen = surge_stats["gen"]          # a newer surge replaces this one
    surge_stats.update(running=True, sent=0, ok=0, rejected=0, queued=0, started=time.time(),
                       until=time.time() + seconds)
    alive = lambda: surge_stats["gen"] == gen and surge_stats["running"] and time.time() < surge_stats["until"]  # noqa: E731
    login = {"email": "demo@sideb.store", "password": "dropday2026"}
    base = config.DEMO_LB_URL

    async def shopper(c: httpx.AsyncClient, n: int):
        i = n
        while alive():
            i += 1
            try:
                if i % 3 == 0:
                    r = await c.get(f"{base}/api/products")
                elif i % 3 == 1:
                    r = await c.post(f"{base}/auth/login", json=login)
                else:
                    r = await c.post(f"{base}/api/checkout")
                surge_stats["sent"] += 1
                code = r.status_code
                surge_stats["ok" if code < 400 else "queued" if code == 429 else "rejected"] += 1
                if code == 429:
                    await asyncio.sleep(1)
            except Exception:  # noqa: BLE001
                surge_stats["rejected"] += 1
                await asyncio.sleep(0.2)

    async def watchdog():
        # a reset (fault back to none) on any coordinator stops the flood
        while alive():
            await asyncio.sleep(2)
            st = await _fan("GET", "/admin/status")
            modes = {v.get("fault_mode") for v in st.values() if "error" not in v}
            if modes and "surge" not in modes:
                break
        if surge_stats["gen"] == gen:
            surge_stats["running"] = False

    async with httpx.AsyncClient(timeout=15, limits=httpx.Limits(max_connections=concurrency + 20)) as c:
        await asyncio.gather(watchdog(), *[shopper(c, k) for k in range(concurrency)])
    if surge_stats["gen"] == gen:
        surge_stats["running"] = False


class SurgeRequest(BaseModel):
    seconds: int = 120
    concurrency: int = 150


@app.post("/control/surge")
async def control_surge(req: SurgeRequest):
    await _fan("POST", "/admin/fault", json={"mode": "surge"})
    asyncio.create_task(_flood(max(10, min(req.seconds, 600)), max(5, min(req.concurrency, 200))))
    return {"surge": "started", "seconds": req.seconds, "concurrency": req.concurrency, "by": config.INSTANCE_ID}


@app.post("/control/surge/stop")
async def control_surge_stop():
    surge_stats["running"] = False
    return await _fan("POST", "/admin/fault", json={"mode": "none"})


class ExecuteRequest(BaseModel):
    action: str


@app.post("/incident/execute")
async def incident_execute(req: ExecuteRequest):
    """Run one remediation action for real (used by the backend console and by C3x auto-remediation)."""
    t0 = time.time()
    if req.action == "scale_out":
        standby = [t for t in config.DEMO_TARGETS if "web4" in t]
        if not standby:
            return JSONResponse({"error": "no standby replica configured (web4)"}, status_code=400)
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(f"{standby[0]}/admin/standby", json={"on": False})
        result = {"web4": r.json()}
    elif req.action == "raise_capacity":
        result = await _fan("POST", "/admin/capacity", json={"inflight": 40})
    elif req.action == "waiting_room":
        result = await _fan("POST", "/admin/waiting-room", json={"on": True})
    else:
        return JSONResponse({"error": "action must be scale_out, raise_capacity or waiting_room"}, status_code=400)
    return {"action": req.action, "done_at": t0, "result": result}


@app.get("/control/live")
async def control_live():
    st = await _fan("GET", "/admin/status")
    reps = []
    for name, v in sorted(st.items()):
        if "error" in v:
            reps.append({"instance": name, "down": True})
            continue
        reps.append({k: v.get(k) for k in ("instance", "rps", "inflight", "capacity", "standby", "waiting_room",
                                            "rejected", "queued", "hits", "fault_mode", "total_requests", "login_attempts",
                                            "chaos_down")})
    up = [r for r in reps if not r.get("down")]
    return {"t": time.time(), "surge": dict(surge_stats), "replicas": reps,
            "rps": round(sum(r["rps"] or 0 for r in up), 1),
            "inflight": sum(r["inflight"] or 0 for r in up),
            "capacity": sum((r["capacity"] or 0) for r in up if not r["standby"]),
            "rejected": sum(r["rejected"] or 0 for r in up),
            "queued": sum(r["queued"] or 0 for r in up),
            "hits": sum(r["hits"] or 0 for r in up),
            "served": sum((r["total_requests"] or 0) + (r["login_attempts"] or 0) for r in up)}


@app.get("/control/status")
async def control_status():
    return await _fan("GET", "/admin/status")


@app.get("/health")
async def health():
    return {"status": "up", "instance": config.INSTANCE_ID}


@app.get("/metrics")
async def metrics():
    llm = get_client()
    return {"instance": config.INSTANCE_ID, "llm": llm.metrics(), "llm_health": await llm.health()}

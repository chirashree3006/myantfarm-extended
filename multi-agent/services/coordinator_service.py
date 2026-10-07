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

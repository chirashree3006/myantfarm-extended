"""
Coordinator microservice (2 replicas behind the load balancer).

    POST /incident/analyze  {"mode": "extended" | "paper", "source": "live" | "paper_static",
                             "scenario": "<for DQ scoring>"}
         extended (C3x): 4 monitoring agents IN PARALLEL -> fusion -> plan -> team tickets
         paper    (C3) : Diagnosis -> Planner -> Risk agents in sequence (the paper's design)
    POST /incident/remediate -> apply the mitigation on the demo site
    POST /control/reset | /control/fault | /control/traffic | /control/restock
    GET  /control/status, /health, /metrics
    POST /control/ambient {"on": true}   background shoppers (sign in, browse, buy, sign out)
    GET  /control/activity               latest log lines from every replica, newest first
    POST /chaos/down|up

If an agent service is unreachable the coordinator runs that agent
in-process (degraded mode) instead of failing the whole analysis.
"""

import asyncio
import random
import time
import zlib

import httpx
from fastapi import FastAPI, Request
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


async def _event(source: str, kind: str, text: str):
    """Write to the shared control-room log (stored on the shop, read by both consoles)."""
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            await c.post(f"{config.DEMO_LB_URL}/admin/events", json={"source": source, "kind": kind, "text": text})
    except Exception:  # noqa: BLE001
        pass


def _src(request: Request | None) -> str:
    return (request.headers.get("x-console") if request else None) or "api"


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
async def analyze(req: AnalyzeRequest, request: Request):
    if req.mode == "paper":
        out = await _paper(req)
        await _event(_src(request), "agents", f"C3 paper pipeline answered (DQ {out.get('dq', {}).get('dq', '-')})")
        return out
    if req.source == "paper_static":
        return JSONResponse({"error": "extended mode needs live telemetry; use mode=paper for paper_static"},
                            status_code=400)
    out = await _extended(req)
    d = out["brief"]["diagnosis"]
    await _event(_src(request), "agents", f"Multi-agent (C3x) diagnosis: {d['title']} ({d['confidence_pct']}% confidence)")
    return out


class RemediateRequest(BaseModel):
    dry_run: bool = True


@app.post("/incident/remediate")
async def remediate(req: RemediateRequest, request: Request):
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
    await _event(_src(request), "fix", f"Fix applied for {rid}: {plan[0]}")
    return {"diagnosis": rid, "dry_run": False, "executed": plan, "results": executed}


class FaultRequest(BaseModel):
    mode: str = "leak"


class TrafficRequest(BaseModel):
    count: int = 60
    concurrency: int = 10
    mix: str = "mixed"   # checkout | login | mixed


@app.post("/control/reset")
async def control_reset(request: Request):
    surge_stats["running"] = False
    out = await _fan("POST", "/admin/reset")
    await _event(_src(request), "info", "Reset: faults off, 12 workers per replica, web4 on standby, waiting room off")
    return out


@app.post("/control/restock")
async def control_restock():
    async with httpx.AsyncClient(timeout=10) as c:
        r = await c.post(f"{config.DEMO_LB_URL}/admin/restock")
    return r.json()


@app.post("/control/fault")
async def control_fault(req: FaultRequest, request: Request):
    await _event(_src(request), "fault" if req.mode != "none" else "fix",
                 f"Fault set on every replica: {req.mode}" if req.mode != "none" else "Fault cleared on every replica")
    return await _fan("POST", "/admin/fault", json={"mode": req.mode})


@app.post("/control/traffic")
async def control_traffic(req: TrafficRequest, request: Request):
    """Real user-style traffic (sign-ins + checkouts) through the website load balancer."""
    out = await generate_traffic(config.DEMO_LB_URL, min(req.count, 2000), min(req.concurrency, 100), mix=req.mix)
    await _event(_src(request), "traffic", f"Traffic burst: {out.get('requests')} requests, {out.get('success_rate_pct')}% ok, "
                 + ", ".join(f"{k} {v}" for k, v in sorted(out.get("by_instance", {}).items())))
    return out


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
async def control_surge(req: SurgeRequest, request: Request):
    await _fan("POST", "/admin/fault", json={"mode": "surge"})
    await _event(_src(request), "traffic", f"Flash sale started: {req.concurrency} shoppers for {req.seconds} s")
    asyncio.create_task(_flood(max(10, min(req.seconds, 600)), max(5, min(req.concurrency, 200))))
    return {"surge": "started", "seconds": req.seconds, "concurrency": req.concurrency, "by": config.INSTANCE_ID}


@app.post("/control/surge/stop")
async def control_surge_stop(request: Request):
    surge_stats["running"] = False
    await _event(_src(request), "info", "Flash sale stopped")
    return await _fan("POST", "/admin/fault", json={"mode": "none"})


class ExecuteRequest(BaseModel):
    action: str
    value: int | None = None      # raise_capacity: workers per replica (default 40)
    on: bool = True               # waiting_room / scale_out: on or off
    service: str | None = None    # rollback: which service (empty = the one the diagnosis named)
    version: str | None = None    # rollback: which version to go back to


FIXED_BY = {"rollback": ("auth_regression", "leak"), "add_index": ("slow_db",)}


@app.post("/incident/execute")
async def incident_execute(req: ExecuteRequest, request: Request):
    """Run one remediation action for real (used by the backend console and by C3x auto-remediation)."""
    t0 = time.time()
    if req.action == "scale_out":
        standby = [t for t in config.DEMO_TARGETS if "web4" in t]
        if not standby:
            return JSONResponse({"error": "no standby replica configured (web4)"}, status_code=400)
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(f"{standby[0]}/admin/standby", json={"on": not req.on})
        result = {"web4": r.json()}
    elif req.action == "raise_capacity":
        result = await _fan("POST", "/admin/capacity", json={"inflight": max(1, min(req.value or 40, 500))})
    elif req.action == "waiting_room":
        result = await _fan("POST", "/admin/waiting-room", json={"on": req.on})
    elif req.action in FIXED_BY:
        # rollback a deploy / add the missing index: only the RIGHT fix clears the fault
        st = {k: v for k, v in (await _fan("GET", "/admin/status")).items() if "error" not in v}
        cur = next(iter(st.values()), {})
        mode = cur.get("fault_mode", "none")
        right = mode in FIXED_BY[req.action]
        if right and req.action == "rollback" and req.service:
            right = req.service == cur.get("service") and (req.version or "") == cur.get("previous_version")
        if right:
            result = await _fan("POST", "/admin/fault", json={"mode": "none"})
        what = (f"rolled back {req.service or cur.get('service')} to {req.version or cur.get('previous_version')}"
                if req.action == "rollback" else "added index on orders(user_id)")
        await _event(_src(request), "fix" if right else "info",
                     f"{what}: {'errors stopped' if right else 'did not fix the problem (' + mode + ' still active)'}")
        return {"action": req.action, "done_at": t0, "fixed": right, "fault_was": mode, "what": what}
    else:
        return JSONResponse({"error": "action must be scale_out, raise_capacity, waiting_room, rollback or add_index"},
                            status_code=400)
    msg = {"scale_out": "web4 added to the load-balancer pool",
           "raise_capacity": f"worker capacity set to {max(1, min(req.value or 40, 500))} on every replica",
           "waiting_room": "waiting room " + ("on" if req.on else "off")}[req.action]
    await _event(_src(request), "fix", msg)
    return {"action": req.action, "done_at": t0, "result": result}


@app.get("/control/logs")
async def control_logs(instance: str = "web1", limit: int = 15):
    """Raw server logs of one replica, for the backend console's terminal (what an engineer would tail)."""
    target = next((t for t in config.DEMO_TARGETS if f"//{instance}:" in t or t.rstrip("/").endswith(instance)), None)
    if not target:
        return JSONResponse({"error": f"no such replica: {instance}"}, status_code=404)
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(f"{target}/logs/server", params={"limit": max(1, min(limit, 100))})
        return {"instance": instance, "records": r.json() if r.status_code == 200 else []}
    except Exception as e:  # noqa: BLE001
        return JSONResponse({"error": f"{instance} unreachable ({type(e).__name__})"}, status_code=502)


# ---------------------------------------------------------------------------
# Background shoppers: a normal day on the shop. A handful of simulated people
# sign in (sometimes mistyping the password), browse, check out and sign out,
# spread across the replicas by the load balancer. Each coordinator replica
# drives its own half of the shoppers; the on/off switch lives on the shop
# replicas (state["ambient"]) so both coordinators agree.
# ---------------------------------------------------------------------------
SIM_SHOPPERS = ["Aarav Mehta", "Diya Sharma", "Kabir Rao", "Ananya Iyer", "Vihaan Gupta", "Meera Nair",
                "Rohan Das", "Isha Kapoor", "Arjun Reddy", "Sara Thomas", "Dev Malhotra", "Nisha Pillai",
                "Ira Banerjee", "Aditya Joshi"]
SIM_PASSWORD = "vinylfan2026"
ambient_stats = {"actions": 0, "signed_in": 0, "last": ""}


def _sim_email(name: str) -> str:
    return name.lower().replace(" ", ".") + "@shoppers.sideb.store"


async def _ambient_loop():
    me = zlib.crc32(config.INSTANCE_ID.encode()) % 2          # coord1 and coord2 take different halves
    mine = [n for i, n in enumerate(SIM_SHOPPERS) if i % 2 == me] or SIM_SHOPPERS
    tokens: dict[str, str] = {}
    base = config.DEMO_LB_URL
    await asyncio.sleep(8)
    async with httpx.AsyncClient(timeout=10) as c:
        for n in mine:   # make sure the accounts exist (409 = already there)
            try:
                r = await c.post(f"{base}/auth/register", json={"email": _sim_email(n), "password": SIM_PASSWORD, "name": n})
                if r.status_code == 200:   # a new account comes back signed in; sign it out again
                    await c.post(f"{base}/auth/logout", headers={"Authorization": f"Bearer {r.json()['token']}"})
            except Exception:  # noqa: BLE001
                pass
        tokens.clear()   # registering signs in; start everyone signed out
        on, checked = True, 0.0
        while True:
            try:
                if time.time() - checked > 3:
                    checked = time.time()
                    st = await c.get(f"{config.DEMO_TARGETS[0]}/admin/status")
                    on = bool(st.json().get("ambient", True))
                if not on:
                    tokens.clear()
                    await asyncio.sleep(2)
                    continue
                n = random.choice(mine)
                hdr = {"Authorization": f"Bearer {tokens[n]}"} if n in tokens else {}
                if n not in tokens:
                    wrong = random.random() < 0.15
                    r = await c.post(f"{base}/auth/login", json={"email": _sim_email(n),
                                                                "password": "vinylfan" if wrong else SIM_PASSWORD})
                    if r.status_code == 200:
                        tokens[n] = r.json()["token"]
                    what = "sign-in (wrong password)" if wrong else "sign-in"
                else:
                    roll = random.random()
                    if roll < 0.55:
                        await c.get(f"{base}/api/products", headers=hdr)
                        r = await c.post(f"{base}/api/presence", headers=hdr)
                        what = "browse"
                    elif roll < 0.72:
                        r = await c.post(f"{base}/api/checkout", headers=hdr)
                        what = "checkout"
                    else:
                        r = await c.post(f"{base}/auth/logout", headers=hdr)
                        tokens.pop(n, None)
                        what = "sign-out"
                ambient_stats.update(actions=ambient_stats["actions"] + 1, signed_in=len(tokens),
                                     last=f"{n}: {what} -> {r.status_code}")
            except Exception:  # noqa: BLE001
                await asyncio.sleep(1)
            await asyncio.sleep(random.uniform(0.6, 1.8))


@app.on_event("startup")
async def _start_ambient():
    asyncio.create_task(_ambient_loop())


class AmbientRequest(BaseModel):
    on: bool = True


@app.post("/control/ambient")
async def control_ambient(req: AmbientRequest, request: Request):
    await _event(_src(request), "info", "Normal-day shoppers " + ("on" if req.on else "off"))
    return await _fan("POST", "/admin/ambient", json={"on": req.on})


@app.get("/control/activity")
async def control_activity(limit: int = 30):
    """Newest log lines from every replica, merged: what the shop is doing right now."""
    limit = max(1, min(limit, 100))
    async with httpx.AsyncClient(timeout=8) as c:
        async def one(t):
            try:
                r = await c.get(f"{t}/logs/server", params={"limit": limit})
                return r.json() if r.status_code == 200 else []
            except Exception:  # noqa: BLE001
                return []
        res = await asyncio.gather(*[one(t) for t in config.DEMO_TARGETS])
    lines = sorted((x for rs in res for x in rs), key=lambda x: x.get("timestamp", ""), reverse=True)[:limit]
    return {"lines": [{k: x.get(k) for k in ("timestamp", "instance", "level", "message", "endpoint")} for x in lines],
            "ambient": dict(ambient_stats)}


class EventIn(BaseModel):
    source: str = "api"
    kind: str = "info"
    text: str


@app.post("/control/event")
async def control_event(ev: EventIn):
    """For actions that don't pass through the coordinator (e.g. /chaos crashes, single-agent answers)."""
    await _event(ev.source, ev.kind, ev.text)
    return {"ok": True}


@app.get("/control/events")
async def control_events(since_id: int = 0, limit: int = 60):
    try:
        async with httpx.AsyncClient(timeout=5) as c:
            r = await c.get(f"{config.DEMO_LB_URL}/admin/events", params={"since_id": since_id, "limit": limit})
        return {"events": r.json() if r.status_code == 200 else []}
    except Exception:  # noqa: BLE001
        return {"events": []}


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
                                            "chaos_down", "ambient", "total_failures", "login_failures", "service",
                                            "deploy_version", "previous_version", "pool_utilization_pct")})
    up = [r for r in reps if not r.get("down")]
    return {"t": time.time(), "surge": dict(surge_stats), "replicas": reps,
            "rps": round(sum(r["rps"] or 0 for r in up), 1),
            "inflight": sum(r["inflight"] or 0 for r in up),
            "capacity": sum((r["capacity"] or 0) for r in up if not r["standby"]),
            "rejected": sum(r["rejected"] or 0 for r in up),
            "queued": sum(r["queued"] or 0 for r in up),
            "hits": sum(r["hits"] or 0 for r in up),
            "failures": sum(r["total_failures"] or 0 for r in up),
            "fault_mode": next((r["fault_mode"] for r in up if r["fault_mode"] not in (None, "none")), "none"),
            "down": [r["instance"] for r in reps if r.get("down")],
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

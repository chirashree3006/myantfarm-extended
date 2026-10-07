"""
One specialist agent as a microservice. The role comes from AGENT_ROLE
(frontend | server | database | users) -- the same image runs all four.

    POST /analyze   {"limit": 50}  -> structured finding for this role
    GET  /health, /metrics
    POST /chaos/down|up            -> fault-tolerance experiments
"""

import os
import time

from fastapi import FastAPI
from pydantic import BaseModel

from antfarm import chaos, config
from antfarm.agents import ROLES, run_agent
from antfarm.llm import get_client
from antfarm.streams import fetch_stream

ROLE = os.getenv("AGENT_ROLE", "database")
assert ROLE in ROLES, f"AGENT_ROLE must be one of {ROLES}"

app = FastAPI(title=f"{ROLE} agent")
chaos.install(app)


class AnalyzeRequest(BaseModel):
    limit: int = 50
    use_llm: bool = True


@app.post("/analyze")
async def analyze(req: AnalyzeRequest):
    t0 = time.perf_counter()
    stream = await fetch_stream(ROLE, req.limit)
    finding = await run_agent(ROLE, stream, get_client() if req.use_llm else None)
    finding["served_by"] = config.INSTANCE_ID
    finding["elapsed_ms"] = round((time.perf_counter() - t0) * 1000, 1)
    return finding


@app.get("/health")
async def health():
    return {"status": "up", "role": ROLE, "instance": config.INSTANCE_ID}


@app.get("/metrics")
async def metrics():
    return {"role": ROLE, "instance": config.INSTANCE_ID, "llm": get_client().metrics()}

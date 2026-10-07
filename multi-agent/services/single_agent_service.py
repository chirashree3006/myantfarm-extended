"""
Condition C2 -- the single-agent copilot, as one microservice (one
instance, like the paper's "Copilot" container).

    POST /analyze  {"scenario": "leak", "source": "live" | "paper_static"}
        -> answer, extracted actions, DQ (paper rubric) when scenario given
"""

import time

from fastapi import FastAPI
from pydantic import BaseModel

from antfarm import chaos, config
from antfarm.baseline import run_single_agent
from antfarm.llm import get_client
from antfarm.paper import PAPER_INCIDENT
from antfarm.scoring import GROUND_TRUTH, score_text
from antfarm.streams import fetch_all

app = FastAPI(title="single-agent copilot (C2)")
chaos.install(app)


class AnalyzeRequest(BaseModel):
    limit: int = 50
    scenario: str | None = None
    source: str = "live"


@app.post("/analyze")
async def analyze(req: AnalyzeRequest):
    t0 = time.perf_counter()
    if req.source == "paper_static":
        out = await run_single_agent(None, get_client(), incident=PAPER_INCIDENT)
    else:
        out = await run_single_agent(await fetch_all(req.limit), get_client())
    out.update({"condition": "C2", "served_by": config.INSTANCE_ID,
                "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1)})
    if req.scenario in GROUND_TRUTH:
        out["dq"] = score_text(out["answer"] if out["llm"]["ok"] else "", req.scenario)
    return out


@app.get("/health")
async def health():
    return {"status": "up", "instance": config.INSTANCE_ID}


@app.get("/metrics")
async def metrics():
    return {"instance": config.INSTANCE_ID, "llm": get_client().metrics()}

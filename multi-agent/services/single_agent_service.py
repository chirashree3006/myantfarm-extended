"""
Condition C2 -- the single-agent copilot, as one microservice (one
instance, like the paper's "Copilot" container).

    POST /analyze  {"scenario": "leak", "source": "live" | "paper_static"}
        -> answer, extracted actions, DQ (paper rubric) when scenario given
    POST /chat     {"history": [{"role": "user"|"agent", "text": ...}], "question": "..."}
        -> follow-up answer from the same single model over the same flattened incident text
"""

import asyncio
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


class ChatTurn(BaseModel):
    role: str
    text: str


class ChatRequest(BaseModel):
    question: str
    history: list[ChatTurn] = []
    limit: int = 50


_incident_cache: dict = {"t": 0.0, "text": ""}


@app.post("/chat")
async def chat(req: ChatRequest):
    """Follow-up questions to the single agent. Same model, same one flat prompt: it still has
    no per-stream specialists, no routing and no tools, so it can only talk, not act."""
    t0 = time.perf_counter()
    if time.time() - _incident_cache["t"] > 20 or not _incident_cache["text"]:
        from antfarm.baseline import build_incident_description
        _incident_cache.update(t=time.time(), text=build_incident_description(await fetch_all(req.limit)))
    convo = "\n".join(f"{'Engineer' if t.role == 'user' else 'Assistant'}: {t.text[:600]}" for t in req.history[-6:])
    prompt = ("You are an on-call engineer assistant helping during a live incident.\n\n"
              f"{_incident_cache['text']}\n\n{convo}\nEngineer: {req.question[:500]}\nAssistant:")
    try:
        r = await asyncio.wait_for(get_client().generate(prompt, agent="single-agent-chat", num_predict=200),
                                   timeout=config.AGENT_LLM_BUDGET_S)
        answer = r.text.strip() if r.ok and r.text.strip() else f"(no answer: {r.error or 'empty reply'})"
        ok = r.ok
    except asyncio.TimeoutError:
        answer, ok = f"(no answer within {config.AGENT_LLM_BUDGET_S:.0f} s: the model is busy)", False
    return {"answer": answer, "ok": ok, "served_by": config.INSTANCE_ID,
            "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1)}


@app.get("/health")
async def health():
    return {"status": "up", "instance": config.INSTANCE_ID}


@app.get("/metrics")
async def metrics():
    return {"instance": config.INSTANCE_ID, "llm": get_client().metrics()}

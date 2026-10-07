"""
Faithful reproduction of the reference paper's conditions
(arXiv:2511.15755, "Multi-Agent LLM Orchestration Achieves Deterministic,
High-Quality Decision Support for Incident Response").

  C1  simulated manual dashboard analysis: T2U ~ N(120 s, 6.5 s), DQ = 0
  C2  single-agent copilot: one LLM call on the incident telemetry
      (see baseline.py, same prompt as phase 1)
  C3  multi-agent: a non-LLM coordinator runs three specialised agents
      SEQUENTIALLY -- Diagnosis -> Remediation Planner -> Risk Assessor --
      each receiving the previous one's output, then aggregates a brief.

The paper's prompts are not printed in the paper; the role prompts below
follow the roles exactly as the paper describes them.

PAPER_INCIDENT is the paper's single scenario ("authentication service
regression post deployment, 45% error rate on login endpoints, database
connections at 85% capacity"), reconstructed as a telemetry block.
"""

from __future__ import annotations

import random
import re
import time

from .llm import TinyLlamaClient

PAPER_INCIDENT = """INCIDENT REPORT
Title: Authentication service regression post deployment
Service: auth-service (deployed v2.3.1 at 14:02 UTC; previous stable version v2.3.0)
Symptoms:
  - Error rate on login endpoints (/api/login, /api/token): 45% (baseline 0.3%)
  - p95 login latency: 2400ms (baseline 180ms)
  - Database connections: 85% of pool capacity (baseline 40%)
  - Error log: "AuthTokenSigner: KeyError 'kid'" on 45% of login requests
  - Support tickets: users report "something went wrong" when signing in
Other services: checkout-service and catalog-service healthy."""


def simulate_c1(rng: random.Random) -> dict:
    """C1 as defined in the paper: simulated manual analysis, timing from
    practitioner estimates, no structured actions (DQ = 0)."""
    return {"condition": "C1", "t2u_s": max(0.0, rng.gauss(120.0, 6.5)), "answer": "", "actions": []}


DIAGNOSIS = ("You are the Diagnosis Specialist on an incident response team. Analyze the incident "
             "telemetry below and identify the most likely root cause in 2-3 sentences.\n\n{incident}\n\nRoot cause:")
PLANNER = ("You are the Remediation Planner on an incident response team.\n"
           "Incident:\n{incident}\n\nDiagnosis from the Diagnosis Specialist:\n{diagnosis}\n\n"
           "Write exactly 3 numbered remediation steps. Each step must name the specific service, "
           "version number and command to run.\n1.")
RISK = ("You are the Risk Assessor on an incident response team.\n"
        "Remediation plan:\n{plan}\n\nIn 2-3 sentences, state the main risks of this plan and how to mitigate them.")


def _numbered(text: str) -> list[str]:
    items = re.findall(r"(?:^|\n)\s*\d+[.)]\s*(.+)", text)
    return [i.strip() for i in items if i.strip()]


async def run_c3_paper(incident: str, llm: TinyLlamaClient) -> dict:
    """Sequential 3-agent pipeline + non-LLM coordinator (the paper's C3)."""
    t0 = time.perf_counter()
    d = await llm.generate(DIAGNOSIS.format(incident=incident), agent="c3-diagnosis", num_predict=150)
    p = await llm.generate(PLANNER.format(incident=incident, diagnosis=d.text or "(none)"),
                           agent="c3-planner", num_predict=220)
    plan_text = "1. " + p.text if p.text and not p.text.lstrip().startswith("1") else p.text
    r = await llm.generate(RISK.format(plan=plan_text or "(none)"), agent="c3-risk", num_predict=150)

    # Non-LLM coordinator: aggregate the three outputs into a brief.
    actions = _numbered(plan_text)[:3]
    brief_text = (f"DIAGNOSIS: {d.text}\n\nACTIONS:\n" +
                  "\n".join(f"{i}. {a}" for i, a in enumerate(actions, 1)) +
                  f"\n\nRISKS: {r.text}")
    return {
        "condition": "C3",
        "diagnosis": d.text, "plan": plan_text, "risk": r.text,
        "actions": actions, "brief": brief_text,
        "agents": {
            "diagnosis": {"ok": d.ok, "latency_ms": d.latency_ms, "backend": d.backend, "error": d.error},
            "planner": {"ok": p.ok, "latency_ms": p.latency_ms, "backend": p.backend, "error": p.error},
            "risk": {"ok": r.ok, "latency_ms": r.latency_ms, "backend": r.backend, "error": r.error},
        },
        "elapsed_ms": round((time.perf_counter() - t0) * 1000, 1),
    }

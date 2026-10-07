"""
Single-agent baseline (the paper's "copilot" condition), as a library.

Identical method to single-agent/single_agent.py: all four streams are
flattened into ONE incident description and ONE TinyLlama call is asked
to diagnose it. The only differences from v1 are that it scrapes every
replica and goes through the shared TinyLlamaClient, so both conditions
use the same model, decoding settings and load-balanced backends --
a fair comparison.
"""

from __future__ import annotations

from .llm import TinyLlamaClient


def build_incident_description(streams: dict) -> str:
    fe = streams["frontend"]["records"][-5:]
    srv = streams["server"]["records"][-5:]
    db = streams["database"]["records"][-3:]
    rep = streams["users"]["records"][-3:]
    sts = list(streams["server"]["status"].values())
    status = next((x for x in sts if x.get("fault_mode") not in (None, "none")), sts[0] if sts else {})
    total_req = sum(s.get("total_requests", 0) for s in streams["server"]["status"].values())
    total_fail = sum(s.get("total_failures", 0) for s in streams["server"]["status"].values())

    parts = [
        "INCIDENT REPORT",
        f"Latest deploy: {status.get('service')} {status.get('deploy_version')} at {status.get('deploy_time')} "
        f"(previous version {status.get('previous_version')})",
        f"Total requests: {total_req}, failures: {total_fail}",
        "",
        "Frontend errors (browser side):",
        *([f"  - {e['message']}" for e in fe] or ["  (none)"]),
        "",
        "Server logs (backend):",
        *([f"  - [{e['level']}] {e['message']}" for e in srv] or ["  (none)"]),
        "",
        "Database metrics:",
        *([f"  - {m['active_connections']}/{m['pool_size']} connections in use "
           f"({m['pool_utilization_pct']}% utilization, {m['avg_wait_time_ms']}ms avg wait)" for m in db]
          or ["  (none)"]),
        "",
        "User support tickets:",
        *([f"  - {r['text']}" for r in rep] or ["  (none)"]),
    ]
    return "\n".join(parts)


def build_prompt(incident_text: str) -> str:
    return (
        "You are an on-call engineer assistant. Analyze the incident below "
        "and tell the team what is wrong and what to do about it.\n\n"
        f"{incident_text}\n\n"
        "What is happening and what should we do?"
    )


async def run_single_agent(streams: dict | None, llm: TinyLlamaClient, incident: str | None = None) -> dict:
    """C2. Pass live `streams`, or a ready `incident` text (paper_static scenario)."""
    incident = incident or build_incident_description(streams)
    r = await llm.generate(build_prompt(incident), agent="single-agent", num_predict=300)
    return {
        "incident_description": incident,
        "answer": r.text if r.ok else f"(no answer: {r.error})",
        "llm": {"ok": r.ok, "latency_ms": r.latency_ms, "backend": r.backend, "error": r.error,
                "output_tokens": r.output_tokens},
    }

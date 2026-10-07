"""
Multi-agent incident response -- runnable WITHOUT Docker (native Windows).

Four specialist agents (Frontend, Server, Database, User Reports) each
read ONLY their own data stream, in parallel, each with its own narrow
TinyLlama call. The coordinator then fuses their structured findings into
one brief: diagnosis -> action plan -> risk.

Prerequisites:
    pip install -r requirements.txt
    demo-website running at http://localhost:8080   (uvicorn app:app --port 8080)
    Ollama running with tinyllama pulled             (ollama pull tinyllama)

Run:
    python multi_agent.py                         # analyse whatever is happening now
    python multi_agent.py --scenario leak         # reset, inject leak, send traffic, analyse
    python multi_agent.py --scenario slow_db --compare   # also run the single-agent baseline + DQ
    python multi_agent.py --scenario auth_regression --compare   # the paper's own incident
    python multi_agent.py --no-llm                # rules-only (no Ollama needed)

Env overrides: DEMO_TARGETS=http://localhost:8080,http://localhost:8081  OLLAMA_URLS=...
"""

import argparse
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import httpx  # noqa: E402

from antfarm import config  # noqa: E402
from antfarm.agents import ROLES, run_agent  # noqa: E402
from antfarm.baseline import run_single_agent  # noqa: E402
from antfarm.fusion import brief_actions, fuse  # noqa: E402
from antfarm.llm import get_client  # noqa: E402
from antfarm.scoring import score_actions, score_text  # noqa: E402
from antfarm.streams import fetch_all  # noqa: E402
from antfarm.traffic import generate_traffic  # noqa: E402

LINE = "=" * 64


async def setup_scenario(mode: str, count: int):
    async with httpx.AsyncClient(timeout=30) as c:
        for t in config.DEMO_TARGETS:
            await c.post(f"{t}/admin/reset")
            await c.post(f"{t}/admin/restock")
            if mode != "healthy":
                await c.post(f"{t}/admin/fault", json={"mode": mode})
    if count:
        tr = await generate_traffic(config.DEMO_LB_URL, count, 10, mix="mixed")
        print(f"Traffic: {tr['requests']} req, success {tr['success_rate_pct']}%, "
              f"by instance {tr['by_instance']}, p95 {tr['latency_ms']['p95']}ms")


async def main(args):
    llm = None if args.no_llm else get_client()
    if args.scenario:
        print(LINE + f"\nSetting up scenario: {args.scenario}\n" + LINE)
        await setup_scenario(args.scenario, args.traffic)

    print(LINE + "\nScraping 4 data streams from " + ", ".join(config.DEMO_TARGETS) + "\n" + LINE)
    streams = await fetch_all()

    print("Running 4 specialist agents in parallel" + ("" if llm else " (rules only)") + " ...")
    findings_list = await asyncio.gather(*[run_agent(r, streams[r], llm) for r in ROLES])
    findings = dict(zip(ROLES, findings_list))
    for r, f in findings.items():
        print(f"\n[{r.upper()} AGENT] status={f['status']} signals={f['signals']}")
        for e in f["evidence"]:
            print(f"   - {e}")
        if f["llm"]["ok"]:
            print(f"   TinyLlama: \"{f['llm']['observation']}\" ({f['llm']['latency_ms']:.0f}ms)")
        elif llm:
            print(f"   TinyLlama: (unusable output -> rules kept) {f['llm']['error']}")

    brief = await fuse(findings, streams["server"]["status"], llm)
    d = brief["diagnosis"]
    print("\n" + LINE + "\nCOORDINATOR BRIEF\n" + LINE)
    print(f"Summary ({brief['synthesis']['mode']}): {brief['summary']}\n")
    print(f"ROOT CAUSE: {d['title']}  [{d['confidence_pct']}% confidence, "
          f"corroborated by {', '.join(d['supporting_agents']) or '-'}]")
    print("EVIDENCE:")
    for e in brief["evidence_chain"]:
        print(f"   - {e}")
    print("ACTION PLAN:")
    for a in brief["action_plan"]:
        print(f"   {a['priority']}. [{a['type']}] ({a['owner']}) {a['action']}\n        $ {a['command']}")
    r = brief["risk"]
    print(f"RISK: {r['level']} | blast radius {r['blast_radius_pct']}% | {r['customer_impact']}")
    for n in r["action_risks"]:
        print(f"   ! {n}")

    print("TEAM TICKETS (work split by owner):")
    for t in brief["team_tickets"]:
        if t["page"]:
            print(f"   [{t['team']}] P{t['priority']}: " + "; ".join(a["action"][:70] for a in t["actions"]))
    spared = [t["team"] for t in brief["team_tickets"] if not t["page"]]
    if spared:
        print(f"   not paged: {', '.join(spared)}")

    if args.scenario and args.scenario != "healthy":
        acts = brief_actions(brief)
        multi_dq = score_actions(acts, args.scenario, brief["summary"] + "\n" + "\n".join(acts))
        print(f"\nMulti-agent DQ = {multi_dq['dq']}  {json.dumps({k: multi_dq[k] for k in ('validity', 'specificity', 'correctness', 'actions')})}")
        if args.compare and llm:
            print("\n" + LINE + "\nSINGLE-AGENT BASELINE (same data, one prompt)\n" + LINE)
            single = await run_single_agent(streams, llm)
            print(single["answer"])
            sdq = score_text(single["answer"] if single["llm"]["ok"] else "", args.scenario)
            print(f"\nSingle-agent DQ = {sdq['dq']}  {json.dumps({k: sdq[k] for k in ('validity', 'specificity', 'correctness', 'actions')})}")

    if llm:
        await llm.aclose()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--scenario", choices=["leak", "slow_db", "auth_regression", "healthy"])
    p.add_argument("--traffic", type=int, default=60, help="requests to send when --scenario is set")
    p.add_argument("--compare", action="store_true", help="also run the single-agent baseline")
    p.add_argument("--no-llm", action="store_true")
    asyncio.run(main(p.parse_args()))

"""
Load and fault-tolerance experiment: single-agent copilot (C2) vs the
extended multi-agent system (C3x), on a live incident.

Part A -- load ramp. K incident analyses arrive at once (K = 1, 4, 8, 16),
each with an on-call SLA (default 60 s to a usable answer). C2 is one
service making one big LLM call per request, so requests queue behind the
shared model. C3x spreads work over 2 coordinators x 4 agents, and every
agent has an LLM time budget: if the model is saturated, the agent returns
its rule-based finding on time instead of making the incident wait.

Part B -- chaos. Components are crashed through the gateway's
/chaos/<service>/down switch (answers 503, like a dead container) while
analyses and shopper traffic keep running:
    1. front instance crash   : C2 loses `single-agent` | C3x loses `coord1`
    2. specialist agent crash : `agent-database` (C3x falls back in-process)
    3. website replica crash  : `web2` (load balancer routes shoppers around it)
  with --docker (run on the Docker host):
    4. one LLM replica down   : docker compose stop ollama1
    5. LLM tier outage        : docker compose stop ollama1 ollama2

    python resilience.py                                   # local, both parts
    python resilience.py --base http://<VM-IP> --levels 1 4 8 16 --sla 60
    python resilience.py --docker                          # on the VM / laptop running Docker

Writes results/resilience_<time>.{md,json,png}.
"""

import argparse
import asyncio
import json
import os
import statistics as st
import subprocess
import sys
import time

import httpx

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(ROOT, "multi-agent"))
from antfarm.traffic import generate_traffic  # noqa: E402

SCENARIO = "leak"
URL = {"C2": "/api/single/analyze", "C3x": "/api/multi/incident/analyze"}


async def call(c, base, cond, sla):
    t0 = time.perf_counter()
    try:
        r = await c.post(base + URL[cond], json={"scenario": SCENARIO, "mode": "extended"}, timeout=sla)
        dt = time.perf_counter() - t0
        if r.status_code != 200:
            return {"ok": False, "latency_s": dt, "error": f"HTTP {r.status_code}", "dq": 0.0, "rc": False}
        j = r.json()
        if cond == "C2" and not j["llm"]["ok"]:
            return {"ok": False, "latency_s": dt, "error": "no answer: " + j["llm"]["error"][:60], "dq": 0.0, "rc": False}
        dq = j.get("dq") or {}
        out = {"ok": True, "latency_s": dt, "error": "", "dq": dq.get("dq", 0.0),
               "rc": bool(dq.get("root_cause_correct")), "served_by": j.get("served_by")}
        if cond == "C3x":
            out["degraded"] = len(j.get("degraded_agents", []))
            out["teams"] = len(j["brief"]["teams_paged"])
            out["diagnosis"] = j["brief"]["diagnosis"]["id"]
        return out
    except httpx.TimeoutException:
        return {"ok": False, "latency_s": sla, "error": f"missed SLA ({sla:.0f}s)", "dq": 0.0, "rc": False}
    except Exception as e:  # noqa: BLE001
        return {"ok": False, "latency_s": time.perf_counter() - t0, "error": type(e).__name__, "dq": 0.0, "rc": False}


def summarize(res):
    lat = sorted(r["latency_s"] for r in res if r["ok"])
    n = len(res)
    return {"n": n, "success_pct": round(100 * sum(r["ok"] for r in res) / n, 1) if n else 0,
            "p50_s": round(lat[len(lat) // 2], 1) if lat else None,
            "p95_s": round(lat[min(len(lat) - 1, int(0.95 * len(lat)))], 1) if lat else None,
            "dq_mean": round(st.mean(r["dq"] for r in res), 3) if n else 0,
            "rc_pct": round(100 * sum(r["rc"] for r in res) / n, 1) if n else 0,
            "degraded_agents_mean": round(st.mean(r.get("degraded", 0) for r in res if r["ok"]), 2)
            if any(r["ok"] for r in res) else None,
            "served_by": sorted({r.get("served_by") for r in res if r.get("served_by")}),
            "errors": sorted({r["error"] for r in res if r["error"]})[:4]}


async def prepare(c, base, traffic=60):
    await c.post(f"{base}/api/multi/control/reset")
    await c.post(f"{base}/api/multi/control/fault", json={"mode": SCENARIO})
    await c.post(f"{base}/api/multi/control/traffic", json={"count": traffic, "concurrency": 10, "mix": "mixed"})


async def part_a(c, a):
    out = []
    await prepare(c, a.base)
    for k in a.levels:
        for cond in ("C2", "C3x"):
            t0 = time.perf_counter()
            res = await asyncio.gather(*[call(c, a.base, cond, a.sla) for _ in range(k)])
            s = summarize(res)
            s.update(level=k, condition=cond, wall_s=round(time.perf_counter() - t0, 1))
            out.append(s)
            print(f"[load K={k:>2}] {cond:<4} success {s['success_pct']:>5}%  p50 {s['p50_s']}s  p95 {s['p95_s']}s  "
                  f"DQ {s['dq_mean']}  {('degraded agents/brief ' + str(s['degraded_agents_mean'])) if cond == 'C3x' else ''}"
                  f"  {s['errors']}", flush=True)
    return out


async def chaos(c, base, svc, up=False, seconds=600):
    r = await c.post(f"{base}/chaos/{svc}/{'up' if up else 'down'}", json={"seconds": seconds})
    return r.status_code


def dc(*args):
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, capture_output=True, text=True)


async def part_b(c, a):
    exps = [
        ("Front instance crash", {"C2": ["single-agent"], "C3x": ["coord1"]}, False),
        ("Specialist agent crash (agent-database)", {"C2": [], "C3x": ["agent-database"]}, False),
        ("Website replica crash (web2)", {"C2": ["web2"], "C3x": ["web2"]}, True),
    ]
    if a.docker:
        exps += [("One LLM replica down (ollama1)", {"docker": ["ollama1"]}, False),
                 ("LLM tier outage (ollama1+ollama2)", {"docker": ["ollama1", "ollama2"]}, False)]
    out = []
    for name, kill, shop in exps:
        await prepare(c, a.base)
        row = {"experiment": name}
        if "docker" in kill:
            dc("stop", *kill["docker"])
            await asyncio.sleep(3)
            for cond in ("C2", "C3x"):
                res = [await call(c, a.base, cond, a.sla) for _ in range(a.n)]
                row[cond] = summarize(res)
            dc("start", *kill["docker"])
            await asyncio.sleep(10)
        else:
            for cond in ("C2", "C3x"):
                for svc in kill[cond]:
                    await chaos(c, a.base, svc)
                await asyncio.sleep(2)
                res = [await call(c, a.base, cond, a.sla) for _ in range(a.n)]
                row[cond] = summarize(res)
                if shop and cond == "C3x":
                    # shoppers during the crash, with no other fault active
                    await c.post(f"{a.base}/api/multi/control/reset")
                    tr = await generate_traffic(f"{a.base}/shop-api", 120, 15, mix="mixed")
                    row["shop_traffic"] = {"success_pct": tr["success_rate_pct"], "by_instance": tr["by_instance"]}
                for svc in kill[cond]:
                    await chaos(c, a.base, svc, up=True)
        out.append(row)
        print(f"[chaos] {name}: C2 success {row['C2']['success_pct']}% DQ {row['C2']['dq_mean']} | "
              f"C3x success {row['C3x']['success_pct']}% DQ {row['C3x']['dq_mean']} "
              f"rc {row['C3x']['rc_pct']}% degraded {row['C3x']['degraded_agents_mean']}"
              + (f" | shop {row['shop_traffic']}" if "shop_traffic" in row else ""), flush=True)
    return out


def report(A, B, a):
    L = ["# Load and fault-tolerance: single agent (C2) vs multi-agent (C3x)", "",
         f"Target `{a.base}` · live `{SCENARIO}` incident · SLA {a.sla:.0f}s to a usable answer "
         "(a request that misses the SLA counts as a failure, DQ 0)", ""]
    if A:
        L += ["## A. Concurrent incident analyses", "",
              "| Simultaneous analyses | Condition | Success % | p50 (s) | p95 (s) | Mean DQ | Root cause correct % | Degraded agents / brief | Served by |",
              "|---|---|---|---|---|---|---|---|---|"]
        for s in A:
            L.append(f"| {s['level']} | {s['condition']} | {s['success_pct']} | {s['p50_s']} | {s['p95_s']} | {s['dq_mean']} | "
                     f"{s['rc_pct']} | {s['degraded_agents_mean'] if s['condition'] == 'C3x' else '—'} | "
                     f"{', '.join(s['served_by'])} |")
        L.append("")
    if B:
        L += ["## B. Component failures", "",
              "| Failure | C2 success % | C2 DQ | C3x success % | C3x DQ | C3x root cause % | C3x degraded agents | Shopper requests OK % (served by) |",
              "|---|---|---|---|---|---|---|---|"]
        for r in B:
            L.append(f"| {r['experiment']} | {r['C2']['success_pct']} | {r['C2']['dq_mean']} | {r['C3x']['success_pct']} | "
                     f"{r['C3x']['dq_mean']} | {r['C3x']['rc_pct']} | {r['C3x']['degraded_agents_mean']} | "
                     + (f"{r['shop_traffic']['success_pct']} ({', '.join(r['shop_traffic']['by_instance'])})"
                        if "shop_traffic" in r else "—") + " |")
        L += ["", "Why the difference: C2 is a single service with one LLM call and nothing to fall back to. "
              "C3x runs two coordinator replicas behind least-conn load balancing, falls back to in-process agents "
              "when an agent service is down, and degrades to rule-based findings when the LLM is slow or gone, "
              "so the diagnosis and the per-team action plan still arrive on time.", ""]
    return "\n".join(L) + "\n"


def plot(path, A, B):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return False
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.2))
    col = {"C2": "#8a93a6", "C3x": "#FF4F9A"}
    if A:
        ax = axes[0]
        for cond in ("C2", "C3x"):
            xs = [s["level"] for s in A if s["condition"] == cond]
            ys = [s["success_pct"] for s in A if s["condition"] == cond]
            ax.plot(xs, ys, marker="o", color=col[cond], label=cond, lw=2)
        ax.set_xlabel("simultaneous incident analyses")
        ax.set_ylabel("answered within SLA (%)")
        ax.set_ylim(-5, 105)
        ax.set_title("Load")
        ax.legend(frameon=False)
        ax.spines[["top", "right"]].set_visible(False)
    if B:
        ax = axes[1]
        names = [r["experiment"].split(" (")[0] for r in B]
        for j, cond in enumerate(("C2", "C3x")):
            ax.barh([i + (j - .5) * .38 for i in range(len(B))], [r[cond]["success_pct"] for r in B], .38,
                    color=col[cond], label=cond)
        ax.set_yticks(range(len(B)), names)
        ax.set_xlabel("analyses succeeding during the failure (%)")
        ax.set_title("Fault tolerance")
        ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(path, dpi=140)
    return True


async def main(a):
    a.base = a.base.rstrip("/")
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    async with httpx.AsyncClient(timeout=a.sla + 30) as c:
        A = await part_a(c, a) if not a.skip_load else []
        B = await part_b(c, a) if not a.skip_chaos else []
        await c.post(f"{a.base}/api/multi/control/reset")
    md = report(A, B, a)
    base = os.path.join(HERE, "results", f"resilience_{time.strftime('%Y%m%d_%H%M%S')}")
    with open(base + ".md", "w", encoding="utf-8") as f:
        f.write(md)
    with open(base + ".json", "w", encoding="utf-8") as f:
        json.dump({"load": A, "chaos": B}, f, indent=2)
    plotted = plot(base + ".png", A, B)
    print("\n" + md + f"\nsaved {base}.md/.json" + ("/.png" if plotted else ""))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="http://localhost")
    p.add_argument("--levels", nargs="+", type=int, default=[1, 4, 8, 16])
    p.add_argument("--sla", type=float, default=60.0, help="seconds to a usable answer")
    p.add_argument("--n", type=int, default=3, help="analyses per condition per chaos experiment")
    p.add_argument("--docker", action="store_true", help="also stop LLM containers (run on the Docker host)")
    p.add_argument("--skip-load", action="store_true")
    p.add_argument("--skip-chaos", action="store_true")
    asyncio.run(main(p.parse_args()))

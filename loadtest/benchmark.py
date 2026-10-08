"""
Paper reproduction benchmark -- arXiv:2511.15755, run end to end.

Conditions (as in the paper, plus our extension)
  C1   simulated manual analysis (T2U ~ N(120, 6.5) s, DQ = 0)      [paper]
  C2   single-agent copilot, one LLM call                           [paper]
  C3   multi-agent: Diagnosis -> Planner -> Risk, non-LLM coordinator [paper]
  C3x  extended multi-agent: 4 parallel monitoring agents + evidence
       fusion + team routing (this project)

Scenarios
  paper_static     the paper's exact incident as fixed telemetry text (C1, C2, C3)
  auth_regression  the paper's incident reproduced LIVE on the shop (all conditions)
  leak, slow_db    additional live incidents (all conditions)
  + healthy checks: no fault injected -> does C3x raise a false alarm?

Metrics: T2U, DQ = 0.4 V + 0.3 S + 0.3 R (paper rubric), actions per
brief, actionability (DQ > 0.5), one-way ANOVA + Welch t-tests with
Bonferroni correction + Cohen's d. Also root-cause correctness (ours).

    python benchmark.py                          # 10 trials per scenario, local
    python benchmark.py --paper-full             # 116 trials, like the paper (hours on CPU)
    python benchmark.py --base http://<VM-IP> --trials 20
    python benchmark.py --scenarios paper_static --conditions C1 C2 C3   # pure paper replication

Writes results/benchmark_<time>.{md,csv,json,png}.
"""

import argparse
import asyncio
import csv
import json
import os
import random
import statistics as st
import sys
import time
from collections import defaultdict

import httpx

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "multi-agent"))
from antfarm.stats import compare, fmt_p  # noqa: E402

LIVE = {"auth_regression", "leak", "slow_db"}
PAPER = {  # Tables I-III of the paper
    "C1": {"t2u": 120.39, "t2u_sd": 5.92, "dq": 0.000, "dq_sd": 0.000, "actions": 0.00, "actionable": 0.0},
    "C2": {"t2u": 41.61, "t2u_sd": 17.31, "dq": 0.403, "dq_sd": 0.023, "actions": 2.01, "actionable": 1.7,
           "V": 1.000, "S": 0.007, "R": 0.003},
    "C3": {"t2u": 40.31, "t2u_sd": 17.32, "dq": 0.692, "dq_sd": 0.000, "actions": 3.00, "actionable": 100.0,
           "V": 1.000, "S": 0.557, "R": 0.417},
}


async def post(c, url, body=None):
    t0 = time.perf_counter()
    r = await c.post(url, json=body or {})
    r.raise_for_status()
    j = r.json()
    j["_latency_s"] = time.perf_counter() - t0
    return j


async def prepare(c, base, scenario, traffic):
    await post(c, f"{base}/api/multi/control/reset")
    await post(c, f"{base}/api/multi/control/restock")
    if scenario != "healthy":
        await post(c, f"{base}/api/multi/control/fault", {"mode": scenario})
    return await post(c, f"{base}/api/multi/control/traffic", {"count": traffic, "concurrency": 10, "mix": "mixed"})


def mkrow(trial, scenario, cond, res=None, error="", t2u=None):
    dq = (res or {}).get("dq") or {}
    row = {"trial": trial, "scenario": scenario, "condition": cond, "ok": not error, "error": error,
           "t2u_s": round(t2u if t2u is not None else (res or {}).get("_latency_s", 0), 2),
           "dq": dq.get("dq", 0.0), "validity": dq.get("validity", 0.0), "specificity": dq.get("specificity", 0.0),
           "correctness": dq.get("correctness", 0.0), "actions": dq.get("actions", 0),
           "actionable": bool(dq.get("actionable")), "root_cause_correct": bool(dq.get("root_cause_correct")),
           "served_by": (res or {}).get("served_by", ""), "synthesis": "", "teams_paged": "", "output": ""}
    if res and cond == "C3x":
        b = res["brief"]
        row.update(synthesis=b["synthesis"]["mode"], teams_paged=len(b["teams_paged"]),
                   diagnosis=b["diagnosis"]["id"], output=b["summary"][:240])
    elif res and cond == "C3":
        row["output"] = res.get("brief", "")[:240].replace("\n", " | ")
    elif res and cond == "C2":
        row["output"] = res.get("answer", "")[:240].replace("\n", " ")
    return row


async def run_condition(c, base, cond, scenario, trial, rng):
    source = "paper_static" if scenario == "paper_static" else "live"
    try:
        if cond == "C1":
            return mkrow(trial, scenario, cond, t2u=max(0.0, rng.gauss(120.0, 6.5)))
        if cond == "C2":
            res = await post(c, f"{base}/api/single/analyze", {"scenario": scenario, "source": source})
        elif cond == "C3":
            res = await post(c, f"{base}/api/multi/incident/analyze",
                             {"scenario": scenario, "source": source, "mode": "paper"})
        else:
            res = await post(c, f"{base}/api/multi/incident/analyze", {"scenario": scenario, "mode": "extended"})
        return mkrow(trial, scenario, cond, res)
    except Exception as e:  # noqa: BLE001
        return mkrow(trial, scenario, cond, error=f"{type(e).__name__}: {str(e)[:120]}", t2u=0)


def agg(rows):
    def m(k):
        v = [r[k] for r in rows]
        return round(st.mean(v), 3) if v else 0.0

    def sd(k):
        v = [r[k] for r in rows]
        return round(st.pstdev(v), 3) if len(v) > 1 else 0.0
    n = len(rows)
    return {"n": n, "ok": sum(r["ok"] for r in rows), "t2u": m("t2u_s"), "t2u_sd": sd("t2u_s"), "dq": m("dq"),
            "dq_sd": sd("dq"), "V": m("validity"), "S": m("specificity"), "R": m("correctness"),
            "actions": m("actions"), "actionable": round(100 * sum(r["actionable"] for r in rows) / n, 1) if n else 0,
            "rc": round(100 * sum(r["root_cause_correct"] for r in rows) / n, 1) if n else 0}


def ratio(a, b):
    return "—" if not b else f"{a / b:.0f}×" if a / b >= 10 else f"{a / b:.1f}×"


def report(rows, healthy, a):
    by = defaultdict(list)
    for r in rows:
        by[(r["scenario"], r["condition"])].append(r)
    scen = [s for s in a.scenarios if any(k[0] == s for k in by)]
    conds = [cc for cc in ("C1", "C2", "C3", "C3x") if cc in a.conditions]
    S = {k: agg(v) for k, v in by.items()}
    L = ["# Reproduction of arXiv:2511.15755 — results", "",
         f"Target `{a.base}` · {a.trials} trials per condition per scenario · model TinyLlama (temperature 0.7, seed 42)",
         "", "C1 is simulated exactly as in the paper. C3 is the paper's sequential Diagnosis → Planner → Risk pipeline. "
         "C3x is this project's extended multi-agent system.", ""]

    L += ["## Table I — T2U and decision quality", "",
          "| Scenario | Cond. | n | T2U mean (s) | T2U sd | DQ mean | DQ sd | Actions | Actionable % | Root cause correct % |",
          "|---|---|---|---|---|---|---|---|---|---|"]
    for s in scen:
        for cc in conds:
            if (s, cc) in S:
                x = S[(s, cc)]
                L.append(f"| {s} | {cc} | {x['n']} | {x['t2u']:.2f} | {x['t2u_sd']:.2f} | {x['dq']:.3f} | {x['dq_sd']:.3f} | "
                         f"{x['actions']:.2f} | {x['actionable']} | {x['rc'] if cc != 'C1' else '—'} |")

    L += ["", "## Table II — DQ components", "",
          "| Scenario | Component | C2 | C3 | C3x | C3 vs C2 | C3x vs C2 |", "|---|---|---|---|---|---|---|"]
    for s in scen:
        for comp, name in (("V", "Validity"), ("S", "Specificity"), ("R", "Correctness")):
            c2, c3, c3x = (S.get((s, k), {}).get(comp) for k in ("C2", "C3", "C3x"))
            f = lambda v: "—" if v is None else f"{v:.3f}"  # noqa: E731
            L.append(f"| {s} | {name} | {f(c2)} | {f(c3)} | {f(c3x)} | "
                     f"{ratio(c3 or 0, c2) if c3 is not None else '—'} | {ratio(c3x or 0, c2) if c3x is not None else '—'} |")

    L += ["", "## Paper vs this reproduction (paper's scenario)", "",
          "| Cond. | Paper T2U | Ours T2U (static / live) | Paper DQ | Ours DQ (static / live) | Paper actionable % | Ours (static / live) |",
          "|---|---|---|---|---|---|---|"]
    for cc in ("C1", "C2", "C3", "C3x"):
        if cc not in conds:
            continue
        p = PAPER.get(cc, {})
        st_, lv = S.get(("paper_static", cc)), S.get(("auth_regression", cc))
        g = lambda x, k, fm: fm.format(x[k]) if x else "—"  # noqa: E731
        L.append(f"| {cc} | {p.get('t2u', '—')} | {g(st_, 't2u', '{:.1f}')} / {g(lv, 't2u', '{:.1f}')} | {p.get('dq', '—')} | "
                 f"{g(st_, 'dq', '{:.3f}')} / {g(lv, 'dq', '{:.3f}')} | {p.get('actionable', '—')} | "
                 f"{g(st_, 'actionable', '{}')} / {g(lv, 'actionable', '{}')} |")

    L += ["", "## Statistical tests (DQ)", "",
          "One-way ANOVA across conditions, then pairwise Welch t-tests at Bonferroni-corrected α.", ""]
    stats_out = {}
    for s in scen:
        groups = {cc: [r["dq"] for r in by[(s, cc)]] for cc in conds if (s, cc) in by}
        res = compare(groups)
        stats_out[s] = res
        if not res.get("anova"):
            continue
        an = res["anova"]
        L.append(f"**{s}** — F({an['df'][0]},{an['df'][1]}) = {an['F']}, p {fmt_p(an['p'])}; α = {res['bonferroni_alpha']:.4f}")
        L.append("")
        L.append("| Pair | t | df | p | Cohen's d | Significant |")
        L.append("|---|---|---|---|---|---|")
        for pr in res["pairs"]:
            L.append(f"| {pr['a']} vs {pr['b']} | {pr['t']} | {pr['df']} | {fmt_p(pr['p'])} | {pr['cohens_d']} | "
                     f"{'yes' if pr['significant'] else 'no'} |")
        L.append("")

    x3 = [r for r in rows if r["condition"] == "C3x" and r["ok"]]
    if x3:
        llm_syn = sum(1 for r in x3 if r["synthesis"] == "llm")
        L += ["## Extended system (C3x) extras", "",
              f"* TinyLlama summary accepted in {llm_syn}/{len(x3)} briefs (the others used the validated template).",
              f"* Teams paged per incident: {st.mean([r['teams_paged'] for r in x3]):.1f} of 5 "
              "(each gets only its own evidence and actions).",
              f"* Healthy checks (no fault): {sum(1 for h in healthy if not h['false_alarm'])}/{len(healthy)} "
              "correctly reported no incident." if healthy else "", ""]
    fails = [r for r in rows if not r["ok"]]
    if fails:
        L += [f"**{len(fails)} trial(s) failed** (counted as DQ 0): " +
              "; ".join(f"{r['scenario']}/{r['condition']}#{r['trial']}: {r['error']}" for r in fails[:6]), ""]
    return "\n".join(L) + "\n", S, stats_out


def plot(path, S, a):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return False
    scen = [s for s in a.scenarios if any(k[0] == s for k in S)]
    conds = [cc for cc in ("C1", "C2", "C3", "C3x") if cc in a.conditions]
    colors = {"C1": "#b8bcc6", "C2": "#8a93a6", "C3": "#3255FF", "C3x": "#FF4F9A"}
    fig, axes = plt.subplots(1, 3, figsize=(15, 4.2))
    w = 0.8 / len(conds)
    for ax, (k, title) in zip(axes, (("dq", "Decision quality (DQ)"), ("actionable", "Actionable briefs (%)"),
                                     ("t2u", "Time to usable understanding (s)"))):
        for j, cc in enumerate(conds):
            vals = [S.get((s, cc), {}).get(k, 0) for s in scen]
            ax.bar([i + (j - (len(conds) - 1) / 2) * w for i in range(len(scen))], vals, w, label=cc, color=colors[cc])
        ax.set_xticks(range(len(scen)), scen, rotation=15)
        ax.set_title(title)
        ax.spines[["top", "right"]].set_visible(False)
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc="upper center", ncol=len(conds), frameon=False)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    fig.savefig(path, dpi=140)
    return True


async def main(a):
    a.base = a.base.rstrip("/")
    if a.paper_full:
        a.trials = 116
    rng = random.Random(42)
    rows, healthy = [], []
    os.makedirs(os.path.join(HERE, "results"), exist_ok=True)
    async with httpx.AsyncClient(timeout=a.timeout) as c:
        # background shoppers off: measure only the experiment's own traffic
        try:
            await c.post(f"{a.base}/api/multi/control/ambient", json={"on": False})
        except Exception:  # noqa: BLE001
            pass
        for s in a.scenarios:
            conds = [cc for cc in a.conditions if not (s == "paper_static" and cc == "C3x")]
            for t in range(1, a.trials + 1):
                if s in LIVE:
                    tr = await prepare(c, a.base, s, a.traffic)
                    print(f"[{s} #{t}] traffic ok {tr['success_rate_pct']}% {tr['by_instance']}", flush=True)
                line = []
                for cc in conds:
                    r = await run_condition(c, a.base, cc, s, t, rng)
                    rows.append(r)
                    line.append(f"{cc} DQ {r['dq']:.2f} T2U {r['t2u_s']:.1f}s" + ("" if r["ok"] else " FAIL"))
                print(f"   {s} #{t}: " + " | ".join(line), flush=True)
        for t in range(1, a.healthy_checks + 1):
            await prepare(c, a.base, "healthy", a.traffic)
            try:
                res = await post(c, f"{a.base}/api/multi/incident/analyze", {"mode": "extended"})
                fa = res["brief"]["incident_detected"]
            except Exception:  # noqa: BLE001
                fa = True
            healthy.append({"trial": t, "false_alarm": fa})
            print(f"[healthy #{t}] C3x false alarm: {fa}", flush=True)
        await post(c, f"{a.base}/api/multi/control/reset")

    md, S, stats_out = report(rows, healthy, a)
    base = os.path.join(HERE, "results", f"benchmark_{time.strftime('%Y%m%d_%H%M%S')}")
    keys = list(dict.fromkeys(k for r in rows for k in r))
    with open(base + ".csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)
    with open(base + ".json", "w", encoding="utf-8") as f:
        json.dump({"summary": {f"{k[0]}/{k[1]}": v for k, v in S.items()}, "stats": stats_out,
                   "healthy_checks": healthy, "rows": rows}, f, indent=2, default=str)
    with open(base + ".md", "w", encoding="utf-8") as f:
        f.write(md)
    plotted = plot(base + ".png", S, a)
    print("\n" + md)
    print(f"saved {base}.md/.csv/.json" + ("/.png" if plotted else "  (pip install matplotlib for the chart)"))


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="http://localhost", help="gateway URL")
    p.add_argument("--trials", type=int, default=10)
    p.add_argument("--paper-full", action="store_true", help="116 trials per condition, as in the paper")
    p.add_argument("--scenarios", nargs="+", default=["paper_static", "auth_regression", "leak", "slow_db"],
                   choices=["paper_static", "auth_regression", "leak", "slow_db"])
    p.add_argument("--conditions", nargs="+", default=["C1", "C2", "C3", "C3x"], choices=["C1", "C2", "C3", "C3x"])
    p.add_argument("--healthy-checks", type=int, default=3)
    p.add_argument("--traffic", type=int, default=60)
    p.add_argument("--timeout", type=float, default=900)
    asyncio.run(main(p.parse_args()))

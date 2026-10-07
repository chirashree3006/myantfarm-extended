"""
The four specialist monitoring agents.

Each agent sees ONLY its own data stream (that's the point of the
decomposition) and produces a structured finding:

    1. deterministic feature extraction  -> signals, evidence, metrics
    2. one narrow TinyLlama call          -> a short natural-language
       observation about *its* stream only, in JSON mode

The rule layer guarantees a correct, structured finding even when the
1B model rambles; the LLM layer adds the analyst-style observation. Both
are returned so you can measure how often the model agrees with the data
(`llm.agrees_with_rules`).
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict

import asyncio

from . import config
from .llm import TinyLlamaClient

ROLES = ("frontend", "server", "database", "users")

SEV_NAMES = {0: "healthy", 1: "degraded", 2: "degraded", 3: "critical"}

_POOL_RE = re.compile(r"(\d+)/(\d+) connections")


def _base(role: str) -> dict:
    return {
        "agent": role,
        "status": "healthy",
        "severity": 0,
        "signals": [],
        "evidence": [],
        "metrics": {},
        "affected_instances": [],
        "hypothesis": "No anomaly in this stream.",
        "deploy_version": None,
    }


def _finish(f: dict) -> dict:
    f["status"] = SEV_NAMES[f["severity"]]
    f["affected_instances"] = sorted(set(f["affected_instances"]))
    return f


# --------------------------------------------------------------------------
# 1. Rule-based feature extraction, one function per agent
# --------------------------------------------------------------------------


def analyze_frontend(stream: dict) -> dict:
    f = _base("frontend")
    recs = stream["records"]
    if not recs:
        return _finish(f)
    codes = Counter(r.get("status_code") for r in recs)
    pages = Counter(r.get("page") for r in recs)
    msgs = Counter(r.get("message") for r in recs)
    f["metrics"] = {"error_count": len(recs), "status_codes": dict(codes),
                    "pages": dict(pages)}
    f["affected_instances"] = [r["instance"] for r in recs]
    login_errs = [r for r in recs if r.get("page") == "/login"]
    checkout_500 = [r for r in recs if r.get("status_code") == 500 and r.get("page") != "/login"]
    if checkout_500:
        f["signals"].append("http_500")
    if codes.get(504):
        f["signals"].append("http_504")
    if login_errs and not codes.get(503):
        f["signals"].append("login_failures")
    if codes.get(503):
        f["signals"].append("http_503")
    top_msg, top_n = msgs.most_common(1)[0]
    page = pages.most_common(1)[0][0]
    f["evidence"].append(f"{len(recs)} browser errors on {page}; most common: \"{top_msg}\" (x{top_n})")
    f["evidence"].append("status codes: " + ", ".join(f"HTTP {c} x{n}" for c, n in codes.items()))
    f["severity"] = 3 if len(recs) >= 10 else 2
    if codes.get(503, 0) >= len(recs) / 2:
        f["hypothesis"] = f"The site is returning 503 Service Unavailable to shoppers ({codes[503]} errors): servers are refusing requests."
    elif login_errs and len(login_errs) >= len(recs) / 2:
        f["hypothesis"] = f"Users cannot sign in: {len(login_errs)} sign-in requests failed with HTTP 500 on /login."
    elif "http_504" in f["signals"] and "http_500" not in f["signals"]:
        f["hypothesis"] = f"Checkout requests on {page} are timing out (HTTP 504) -- a slow backend dependency."
    else:
        f["hypothesis"] = f"Checkout requests on {page} are failing server-side (HTTP 500), not a client bug."
    return _finish(f)


def analyze_server(stream: dict) -> dict:
    f = _base("server")
    recs = stream["records"]
    if not recs:
        return _finish(f)
    levels = Counter(r.get("level") for r in recs)
    versions = Counter(r.get("deploy_version") for r in recs if r.get("deploy_version"))
    f["deploy_version"] = versions.most_common(1)[0][0] if versions else None
    exhausted = [r for r in recs if "pool exhausted" in r.get("message", "").lower()]
    auth = [r for r in recs if r.get("level") == "ERROR" and r.get("endpoint", "").startswith("/auth")]
    timeouts = [r for r in recs if "timeout" in r.get("message", "").lower()]
    slow = [r for r in recs if "slow query" in r.get("message", "").lower()]
    overload = [r for r in recs if "exceed worker capacity" in r.get("message", "")]
    spikes = [r for r in recs if r.get("message", "").startswith("Traffic spike")]
    total = len(recs)
    errors = levels.get("ERROR", 0)
    f["metrics"] = {"log_lines": total, "levels": dict(levels),
                    "error_rate_pct": round(100 * errors / total, 1),
                    "pool_exhausted_errors": len(exhausted),
                    "query_timeouts": len(timeouts), "slow_queries": len(slow)}
    if exhausted:
        f["signals"].append("pool_exhausted")
        m = _POOL_RE.search(exhausted[-1]["message"])
        used = f"{m.group(1)}/{m.group(2)}" if m else "all"
        f["evidence"].append(
            f"{len(exhausted)} x ERROR \"Connection pool exhausted: {used} connections in use\" "
            f"on {exhausted[-1].get('endpoint', '/api/checkout')}")
        f["affected_instances"] += [r["instance"] for r in exhausted]
    if timeouts or slow:
        f["signals"].append("slow_queries")
        qms = [r.get("query_ms") for r in timeouts + slow if r.get("query_ms")]
        avg = round(sum(qms) / len(qms)) if qms else None
        sample = (timeouts or slow)[-1]["message"]
        f["evidence"].append(f"{len(slow)} slow queries + {len(timeouts)} query timeouts"
                             + (f", avg {avg}ms" if avg else "") + f"; e.g. \"{sample[:110]}\"")
        f["metrics"]["avg_query_ms"] = avg
        f["affected_instances"] += [r["instance"] for r in timeouts + slow]
        if timeouts:
            f["signals"].append("query_timeouts")
    if spikes:
        f["signals"].append("traffic_spike")
        f["evidence"].append(f"traffic spike warnings: \"{spikes[-1]['message']}\" on "
                             + ", ".join(sorted({r['instance'] for r in spikes})))
        f["affected_instances"] += [r["instance"] for r in spikes]
    if overload:
        f["signals"].append("overload")
        f["metrics"]["rejected_requests"] = len(overload)
        f["evidence"].append(f"{len(overload)} x ERROR \"{overload[-1]['message'][:110]}\"")
        f["affected_instances"] += [r["instance"] for r in overload]
    auth = [r for r in auth if "exceed worker capacity" not in r.get("message", "")]
    if auth:
        f["signals"].append("auth_errors")
        f["metrics"]["auth_errors"] = len(auth)
        f["evidence"].append(f"{len(auth)} x ERROR on {auth[-1]['endpoint']}: \"{auth[-1]['message'][:120]}\"")
        f["affected_instances"] += [r["instance"] for r in auth]
    err_recs = [r for r in recs if r.get("level") == "ERROR"]
    err_versions = {(r.get("service"), r.get("deploy_version")) for r in err_recs if r.get("deploy_version")}
    if len(err_versions) == 1:
        svc, ver = next(iter(err_versions))
        f["service"], f["deploy_version"] = svc, ver
        f["signals"].append("deploy_regression")
        f["evidence"].append(f"all {len(err_recs)} errors come from {svc} {ver} (the latest deploy)")
    elif len(err_versions) > 1:
        f["evidence"].append(f"errors span deploys {sorted(v for _, v in err_versions)}")
    if errors == 0 and not slow:
        return _finish(f)
    f["severity"] = 3 if f["metrics"]["error_rate_pct"] >= 20 else 2 if errors else 1
    if overload and len(overload) >= max(len(auth), len(exhausted)):
        f["hypothesis"] = ("Workers are saturated by a traffic spike: requests beyond worker capacity are "
                           "rejected with 503.")
    elif auth and len(auth) >= len(exhausted):
        f["hypothesis"] = (f"Login handler in {f.get('service', 'auth-service')} {f['deploy_version']} throws on "
                           "token signing -- a regression shipped with the latest deploy.")
    elif exhausted:
        f["hypothesis"] = ("Backend cannot acquire DB connections: the connection pool is exhausted, "
                           "so /api/checkout returns 500.")
    elif slow or timeouts:
        f["hypothesis"] = ("Backend is blocked on slow SQL (orders lookup doing a full table scan); "
                           "requests exceed the timeout.")
    return _finish(f)


def analyze_database(stream: dict) -> dict:
    f = _base("database")
    recs = stream["records"]
    if not recs:
        return _finish(f)
    by_inst = defaultdict(list)
    for r in recs:
        by_inst[r["instance"]].append(r)
    per = {}
    leak_insts, sat_insts, slow_insts, high_insts = [], [], [], []
    for inst, rows in by_inst.items():
        conns = [r["active_connections"] for r in rows]
        waits = [r["avg_wait_time_ms"] for r in rows]
        last = rows[-1]
        never_released = all(b >= a for a, b in zip(conns, conns[1:]))
        grew = conns[-1] - conns[0]
        recent_wait = sum(waits[-10:]) / len(waits[-10:])
        per[inst] = {"pool": f"{last['active_connections']}/{last['pool_size']}",
                     "utilization_pct": last["pool_utilization_pct"],
                     "avg_wait_ms": round(recent_wait, 1),
                     "samples": len(rows), "monotonic_growth": never_released and grew >= 3}
        if never_released and grew >= 3:
            leak_insts.append(inst)
        if last["pool_utilization_pct"] >= 100:
            sat_insts.append(inst)
        if recent_wait >= 200 and last["pool_utilization_pct"] < 80:
            slow_insts.append(inst)
        recent_util = [r["pool_utilization_pct"] for r in rows[-10:]]
        avg_util = sum(recent_util) / len(recent_util)
        per[inst]["avg_utilization_pct"] = round(avg_util, 1)
        if 75 <= avg_util < 100 and not (never_released and grew >= 3) and recent_wait < 200:
            high_insts.append(inst)
    f["metrics"] = {"per_instance": per}
    if leak_insts:
        f["signals"].append("pool_leak_growth")
        f["evidence"].append(
            "active connections only ever increase (never released) on " + ", ".join(sorted(leak_insts)))
    if sat_insts:
        f["signals"].append("pool_saturated")
        f["evidence"].append(
            "pool at 100% utilization on " + ", ".join(
                f"{i} ({per[i]['pool']}, wait {per[i]['avg_wait_ms']}ms)" for i in sorted(sat_insts)))
    if slow_insts:
        f["signals"].append("high_query_latency")
        f["evidence"].append(
            "query wait high while pool is NOT saturated on " + ", ".join(
                f"{i} ({per[i]['utilization_pct']}% util, {per[i]['avg_wait_ms']}ms)" for i in sorted(slow_insts)))
    if high_insts:
        f["signals"].append("pool_high")
        f["evidence"].append("connection pool running hot (~" + str(round(sum(
            per[i]["avg_utilization_pct"] for i in high_insts) / len(high_insts))) + "% of capacity) but connections "
            "are being released on " + ", ".join(sorted(high_insts)))
    f["affected_instances"] = leak_insts + sat_insts + slow_insts + high_insts
    if sat_insts or (leak_insts and slow_insts == []):
        f["severity"] = 3 if sat_insts else 2
        f["hypothesis"] = ("Connection leak: connections are acquired and never returned to the pool "
                           "until it is exhausted.")
    elif slow_insts:
        f["severity"] = 2
        f["hypothesis"] = "Pool capacity is fine; individual queries are slow (missing index / bad plan)."
    elif high_insts:
        f["severity"] = 1
        f["hypothesis"] = "Pool is at ~85% capacity: something is holding connections longer than usual (verify the pool)."
    return _finish(f)


def analyze_users(stream: dict) -> dict:
    f = _base("users")
    recs = stream["records"]
    if not recs:
        return _finish(f)
    fail = [r for r in recs if re.search(r"fail|error|spins", r.get("text", ""), re.I)]
    slow = [r for r in recs if re.search(r"slow|time[sd]? ?out|long wait", r.get("text", ""), re.I)]
    login = [r for r in recs if re.search(r"log ?in|sign ?in|password", r.get("text", ""), re.I)]
    down = [r for r in recs if re.search(r"is down|nothing loads|not loading", r.get("text", ""), re.I)]
    if down:
        f["signals"].append("user_complaints_down")
    if login:
        f["signals"].append("user_complaints_login")
    f["metrics"] = {"tickets": len(recs), "failure_tickets": len(fail), "slowness_tickets": len(slow)}
    f["affected_instances"] = [r["instance"] for r in recs]
    if fail:
        f["signals"].append("user_complaints_failure")
    if slow:
        f["signals"].append("user_complaints_slow")
    f["evidence"].append(f"{len(recs)} support tickets; latest: \"{recs[-1]['text']}\"")
    f["severity"] = 2 if len(recs) >= 3 else 1
    if down and len(down) >= len(recs) / 2:
        f["hypothesis"] = "Customers report the whole site is down during the drop."
    elif login and len(login) >= len(recs) / 2:
        f["hypothesis"] = "Customers cannot sign in to their accounts."
    elif fail and not slow:
        f["hypothesis"] = "Users cannot complete checkout -- customer-facing outage."
    else:
        f["hypothesis"] = "Users report checkout is very slow / timing out."
    return _finish(f)


RULES = {"frontend": analyze_frontend, "server": analyze_server,
         "database": analyze_database, "users": analyze_users}

# --------------------------------------------------------------------------
# 2. Narrow LLM call per agent
# --------------------------------------------------------------------------

AGENT_TITLES = {
    "frontend": "Frontend monitoring agent. You only see browser-side errors.",
    "server": "Server monitoring agent. You only see backend application logs.",
    "database": "Database monitoring agent. You only see database connection-pool metrics.",
    "users": "User-reports agent. You only see customer support tickets.",
}

AGREE_KEYWORDS = {
    "pool_exhausted": ("pool", "connection"),
    "pool_leak_growth": ("leak", "not released", "never released", "connection"),
    "pool_saturated": ("pool", "100", "exhaust", "full"),
    "high_query_latency": ("slow", "latency", "wait", "query"),
    "slow_queries": ("slow", "query", "timeout"),
    "http_500": ("500", "fail", "error"),
    "http_504": ("504", "timeout", "slow"),
    "user_complaints_failure": ("fail", "checkout", "error"),
    "user_complaints_slow": ("slow", "timeout"),
    "user_complaints_login": ("log", "sign"),
    "login_failures": ("login", "sign", "500"),
    "auth_errors": ("auth", "login", "token"),
    "deploy_regression": ("deploy", "version", "v2"),
    "pool_high": ("pool", "connection", "85", "capacity"),
    "http_503": ("503", "unavailable", "down"),
    "overload": ("capacity", "overload", "reject", "traffic"),
    "traffic_spike": ("traffic", "spike", "req/s"),
    "user_complaints_down": ("down", "load"),
}


def build_agent_prompt(finding: dict) -> str:
    facts = "\n".join(f"- {e}" for e in finding["evidence"]) or "- no errors or anomalies"
    return (
        f"You are the {AGENT_TITLES[finding['agent']]}\n"
        f"Facts from your data:\n{facts}\n\n"
        "Reply ONLY with JSON with two keys: "
        "\"observation\" (one short sentence: what these facts show) and "
        "\"severity\" (one of: none, low, medium, high)."
    )


async def run_agent(role: str, stream: dict, llm: TinyLlamaClient | None) -> dict:
    finding = RULES[role](stream)
    finding["unreachable_targets"] = stream.get("unreachable", [])
    finding["llm"] = {"ok": False, "observation": "", "latency_ms": 0, "backend": "",
                      "agrees_with_rules": None, "error": ""}
    if llm is not None:
        coro = llm.generate_json(build_agent_prompt(finding), agent=role,
                                 required_keys=("observation",), num_predict=120)
        try:
            # Graceful degradation: if the shared LLM is overloaded, don't make the
            # incident wait -- return the rule-based finding on time.
            r = await (asyncio.wait_for(coro, config.AGENT_LLM_BUDGET_S) if config.AGENT_LLM_BUDGET_S > 0 else coro)
        except asyncio.TimeoutError:
            from .llm import LLMResult
            r = LLMResult(agent=role, error=f"budget_exceeded ({config.AGENT_LLM_BUDGET_S:.0f}s) -> rules only",
                          latency_ms=config.AGENT_LLM_BUDGET_S * 1000)
        obs = str((r.data or {}).get("observation", "")).strip()
        agrees = None
        if r.ok and finding["signals"]:
            low = obs.lower()
            agrees = any(k in low for s in finding["signals"] for k in AGREE_KEYWORDS.get(s, ()))
        elif r.ok:
            agrees = True
        finding["llm"] = {"ok": r.ok, "observation": obs, "severity": (r.data or {}).get("severity"),
                          "latency_ms": r.latency_ms, "backend": r.backend,
                          "agrees_with_rules": agrees, "error": r.error}
    return finding

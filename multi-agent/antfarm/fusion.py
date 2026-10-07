"""
Extended coordinator (condition C3x): evidence fusion -> diagnosis ->
action plan -> risk -> per-team tickets.

Phase-1 lesson: TinyLlama (1.1B) could not synthesise four agents' free
text into a diagnosis. So this coordinator has two layers:

  1. Deterministic fusion. Agent findings are structured (signals,
     evidence, instances), so cross-source correlation is an explicit,
     auditable scoring of competing hypotheses. Independent agents that
     corroborate each other raise confidence -- the core multi-agent idea.
  2. Constrained LLM synthesis. TinyLlama only writes the 2-sentence
     human summary of an already-decided diagnosis, under a time budget.
     If it drops the root cause or runs out of time, the coordinator uses
     a template and reports `synthesis.mode = "template-fallback"`.

It also splits the work: every action has an owning team, and each team
gets a ticket with only its own evidence and actions ("team_tickets").
Teams with nothing to do are not paged.
"""

from __future__ import annotations

import asyncio

from . import config
from .llm import TinyLlamaClient

HYPOTHESES = {
    "db_connection_leak": {
        "title": "Database connection-pool leak in checkout-service (/api/checkout)",
        "weights": {"pool_exhausted": 3, "pool_leak_growth": 3, "pool_saturated": 2,
                    "http_500": 1, "user_complaints_failure": 1},
        "keywords": ("pool", "connection", "leak"),
        "symptom": "some checkouts are failing",
    },
    "slow_db_queries": {
        "title": "Slow database queries (orders lookup without an index)",
        "weights": {"slow_queries": 3, "high_query_latency": 3, "query_timeouts": 1,
                    "http_504": 1, "user_complaints_slow": 1},
        "keywords": ("slow", "query", "index", "latency"),
        "symptom": "checkout is slow and some orders time out",
    },
    "traffic_surge": {
        "title": "Traffic surge beyond web-tier capacity (flash-sale overload)",
        "weights": {"overload": 3, "traffic_spike": 2, "http_503": 2, "user_complaints_down": 1},
        "keywords": ("traffic", "surge", "capacity", "load", "overload", "scale"),
        "symptom": "the shop is overloaded by drop traffic and some shoppers see an error",
    },
    "auth_regression": {
        "title": "Authentication service regression after deployment",
        "weights": {"auth_errors": 3, "login_failures": 2, "user_complaints_login": 1,
                    "deploy_regression": 1, "pool_high": 1},
        "keywords": ("auth", "login", "rollback", "roll back", "deploy"),
        "symptom": "some customers can't sign in",
    },
}

TEAM_OF_AGENT = {"frontend": "Frontend team", "server": "Backend team",
                 "database": "Database team", "users": "Customer support"}
ALL_TEAMS = ["SRE on-call", "Backend team", "Database team", "Frontend team", "Customer support"]
MIN_SCORE = 3


def _score(findings: dict) -> dict:
    out = {}
    for hid, h in HYPOTHESES.items():
        score, by = 0, {}
        for role, f in findings.items():
            pts = sum(h["weights"].get(s, 0) for s in f.get("signals", []))
            if pts:
                by[role] = pts
                score += pts
        out[hid] = {"score": score, "max": sum(h["weights"].values()),
                    "supporting_agents": sorted(by), "by_agent": by}
    return out


def _act(priority, type_, owner, action, command, execute=None):
    a = {"priority": priority, "type": type_, "owner": owner, "action": action, "command": command}
    if execute:
        a["execute"] = execute   # the coordinator can run this action itself (POST /incident/execute)
    return a


def _actions(root: str, insts: list[str], all_insts: list[str], dep: dict) -> list[dict]:
    names = " ".join(insts or all_insts or ["web1", "web2", "web3"])
    subset = bool(insts) and len(insts) < len(all_insts)
    svc, ver, prev = dep.get("service") or "checkout-service", dep.get("version") or "current", dep.get("previous") or "previous"
    A: list[dict] = []
    if root == "db_connection_leak":
        A.append(_act(1, "mitigate", "SRE on-call",
                      f"Restart {names.replace(' ', ', ')} one at a time to recycle the exhausted DB connection pools "
                      "and restore checkout immediately.", f"docker compose restart {names}"))
        if subset:
            A.append(_act(2, "mitigate", "SRE on-call",
                          f"Drain {', '.join(insts)} from the Nginx web_pool upstream until restarted.",
                          "mark the server 'down' in gateway/nginx.conf && docker compose exec gateway nginx -s reload"))
        A.append(_act(2, "rollback", "Backend team",
                      f"Rollback {svc} deployment to {prev}: {ver} acquires a DB connection in /api/checkout "
                      "and never releases it.",
                      f"git checkout {prev} && docker compose up -d --build {names}"))
        A.append(_act(3, "fix", "Backend team",
                      "Release the database connection in a finally block in /api/checkout "
                      "(try: conn = pool.acquire() ... finally: pool.release(conn)).",
                      "patch demo-website/app.py do_checkout()"))
        A.append(_act(4, "prevent", "SRE on-call",
                      "Alert when database connection pool utilisation > 80% for 2 minutes.",
                      "alert rule: pool_utilization_pct > 80 for 2m"))
    elif root == "slow_db_queries":
        A.append(_act(1, "investigate", "Database team", "Confirm the full table scan on the orders lookup.",
                      "EXPLAIN ANALYZE SELECT * FROM orders WHERE user_id = 42;"))
        A.append(_act(1, "mitigate", "Database team", "Create the missing index on orders.user_id (online, no table lock).",
                      "CREATE INDEX CONCURRENTLY idx_orders_user_id ON orders(user_id);"))
        A.append(_act(2, "mitigate", "Backend team",
                      "Until the index is built, raise the checkout query timeout from 600ms to 2000ms.",
                      f"SLOW_QUERY_TIMEOUT_MS=2000 docker compose up -d {names}"))
        A.append(_act(3, "rollback", "Backend team",
                      f"If {svc} {ver} introduced the new orders query, rollback {svc} deployment to {prev}.",
                      f"git checkout {prev} && docker compose up -d --build {names}"))
        A.append(_act(4, "prevent", "Database team",
                      "Alert on database query latency p95 > 300ms and review EXPLAIN plans for new queries.",
                      "alert rule: db_avg_wait_ms > 300 for 5m"))
    elif root == "traffic_surge":
        A.append(_act(1, "mitigate", "SRE on-call",
                      "Scale out the web tier: bring standby replica web4 into the load-balancer pool.",
                      "POST /incident/execute {\"action\": \"scale_out\"}  (docker compose up -d --scale web=4)",
                      execute="scale_out"))
        A.append(_act(1, "mitigate", "Backend team",
                      "Raise worker capacity on every replica from 12 to 40 in-flight requests.",
                      "POST /incident/execute {\"action\": \"raise_capacity\"}  (WORKER_CAPACITY=40)",
                      execute="raise_capacity"))
        A.append(_act(2, "mitigate", "Frontend team",
                      "Turn on the waiting room so shoppers over capacity queue and retry instead of seeing an error.",
                      "POST /incident/execute {\"action\": \"waiting_room\"}",
                      execute="waiting_room"))
        A.append(_act(3, "verify", "Database team",
                      "Verify the database connection pool keeps up with the extra workers (utilisation under 80%).",
                      "curl -s http://localhost:8080/admin/status"))
        A.append(_act(4, "prevent", "SRE on-call",
                      "Autoscaling rule: add a web replica when in-flight requests stay above 80% of capacity for 30 seconds.",
                      "alert rule: inflight / capacity > 0.8 for 30s -> scale web +1"))
    elif root == "auth_regression":
        A.append(_act(1, "rollback", "Backend team",
                      f"Rollback {svc} deployment to {prev}: {ver} crashes while signing login tokens "
                      "(KeyError 'kid').",
                      f"AUTH_SERVICE_VERSION={prev} docker compose up -d {names}"))
        A.append(_act(2, "verify", "Database team",
                      "Verify the database connection pool after the rollback: utilisation should fall from "
                      "~85% back under 50%.",
                      "curl -s http://localhost:8080/admin/status"))
        A.append(_act(3, "mitigate", "Frontend team",
                      "Show a sign-in status banner; already signed-in customers can still check out.",
                      "feature flag: signin_banner=on"))
        A.append(_act(4, "prevent", "SRE on-call",
                      f"Release {svc} behind a 5% canary with automatic rollback when login errors exceed 2%.",
                      "canary: auth-service weight=5, rollback_on login_error_rate>2%"))
    else:
        A.append(_act(1, "investigate", "SRE on-call",
                      "Signals are weak or conflicting: collect more traffic and re-run the agents.",
                      "POST /api/multi/control/traffic {\"count\": 60}"))
    return A


def _risk(root: str, findings: dict, insts: list[str], all_insts: list[str]) -> dict:
    fe = findings.get("frontend", {}).get("metrics", {})
    srv = findings.get("server", {}).get("metrics", {})
    err_rate = srv.get("error_rate_pct", 0)
    blast = round(100 * len(insts) / len(all_insts)) if all_insts else 0
    level = "high" if err_rate >= 20 or blast >= 67 else "medium" if err_rate > 0 else "low"
    notes = {
        "db_connection_leak": [
            "Restarting a replica drops its in-flight requests; restart one at a time behind the load balancer.",
            "A restart is temporary: the pool leaks again under traffic until the rollback or fix ships.",
            "The rollback removes the v2.3.0 session-validation feature until the fix ships."],
        "slow_db_queries": [
            "CREATE INDEX CONCURRENTLY is online but adds write load while it builds.",
            "Raising timeouts holds connections longer and can turn slowness into pool exhaustion."],
        "traffic_surge": [
            "More workers per replica raise database load; watch the connection pool after scaling.",
            "The waiting room delays shoppers instead of failing them; turn it off once traffic drops."],
        "auth_regression": [
            "Rolling back auth-service invalidates tokens signed by v2.3.1; affected users must sign in again.",
            "If the pool stays near 85% after the rollback, something else is holding connections."],
    }.get(root, [])
    return {"level": level, "customer_impact": f"{fe.get('error_count', 0)} failed requests seen in browsers; "
                                               f"server error rate {err_rate}%",
            "blast_radius_pct": blast, "affected_instances": insts, "action_risks": notes}


def _tickets(root: str, title: str, findings: dict, actions: list[dict], evidence_by_role: dict) -> list[dict]:
    if root == "none":
        return []
    tickets = {}
    for t in ALL_TEAMS:
        tickets[t] = {"team": t, "page": False, "priority": None, "actions": [], "evidence": []}
    for a in actions:
        tk = tickets[a["owner"]]
        tk["actions"].append({k: a[k] for k in ("priority", "type", "action", "command")})
        tk["page"] = True
        tk["priority"] = min(tk["priority"] or 9, a["priority"])
    for role, ev in evidence_by_role.items():
        tickets[TEAM_OF_AGENT[role]]["evidence"] += ev
    sym = HYPOTHESES.get(root, {}).get("symptom", "some customers are affected")
    sup = tickets["Customer support"]
    sup["page"] = True
    sup["priority"] = sup["priority"] or 2
    sup["actions"].append({"priority": 2, "type": "communicate",
                           "action": "Post this on the status page and use it as the ticket macro.",
                           "command": f"\"We're aware {sym}. Engineers have found the cause and a fix is "
                                      "rolling out. Failed orders were not charged. Next update in 30 minutes.\""})
    sre = tickets["SRE on-call"]
    sre["page"] = True
    sre["priority"] = sre["priority"] or 1
    sre["evidence"].insert(0, f"Incident commander: diagnosis = {title}")
    out = sorted(tickets.values(), key=lambda t: (not t["page"], t["priority"] or 9))
    return out


async def fuse(findings: dict, status: dict, llm: TinyLlamaClient | None) -> dict:
    """findings: {role: finding}; status: {instance: /admin/status payload}"""
    scores = _score(findings)
    best_id = max(scores, key=lambda h: scores[h]["score"])
    best = scores[best_id]
    any_alarm = any(f.get("severity", 0) >= 2 for f in findings.values())
    all_insts = sorted(status) or sorted({i for f in findings.values() for i in f.get("affected_instances", [])})

    if best["score"] >= MIN_SCORE:
        rid, title = best_id, HYPOTHESES[best_id]["title"]
        conf = round(100 * best["score"] / best["max"])
        if len(best["supporting_agents"]) >= 3:
            conf = min(99, conf + 10)
    elif any_alarm:
        rid, title, conf = "unknown", "Unclassified anomaly", 20
    else:
        rid, title, conf = "none", "No incident", 95

    root = {"id": rid, "title": title, "confidence_pct": conf,
            "supporting_agents": best["supporting_agents"] if rid not in ("none", "unknown") else [],
            "hypothesis_scores": scores}

    relevant = root["supporting_agents"] or [r for r, f in findings.items() if f.get("severity", 0) >= 2]
    insts = sorted({i for r in relevant for i in findings[r].get("affected_instances", [])})
    # strongest evidence first: order agents by how much they support the diagnosis
    order = sorted(("database", "server", "frontend", "users"),
                   key=lambda r: -best["by_agent"].get(r, 0) if rid == best_id else 0)
    ev_by_role = {r: findings[r].get("evidence", []) for r in order if r in relevant}
    evidence = [f"[{r}] {e}" for r, evs in ev_by_role.items() for e in evs]

    st = next((s for s in status.values() if s.get("fault_mode") not in (None, "none")), None) \
        or next(iter(status.values()), {}) if status else {}
    srv = findings.get("server", {})
    dep = {"service": srv.get("service") or st.get("service"),
           "version": srv.get("deploy_version") or st.get("deploy_version"),
           "previous": st.get("previous_version")}

    actions = _actions(rid, insts, all_insts, dep) if rid != "none" else []
    risk = _risk(rid, findings, insts, all_insts)
    tickets = _tickets(rid, title, findings, actions, ev_by_role)

    template = (f"Root cause: {title} ({conf}% confidence, corroborated by {len(root['supporting_agents'])} "
                f"independent agents). Key evidence: {evidence[0] if evidence else 'n/a'}. "
                f"Do first: {actions[0]['action'] if actions else 'nothing'}") if rid != "none" else \
        "No incident: all four agents report healthy streams."
    synthesis = {"mode": "template", "llm_ok": False, "latency_ms": 0, "backend": "", "error": ""}
    summary = template
    if llm is not None and rid != "none":
        prompt = (
            "You are the incident coordinator. The diagnosis is already decided.\n"
            f"Root cause: {title}.\n"
            "Evidence:\n" + "\n".join(f"- {e}" for e in evidence[:4]) + "\n"
            f"First action: {actions[0]['action'] if actions else 'investigate'}\n\n"
            "Reply ONLY with JSON with one key \"summary\": two sentences for the on-call engineer "
            "stating the root cause and the first action."
        )
        coro = llm.generate_json(prompt, agent="coordinator", required_keys=("summary",), num_predict=160)
        try:
            budget = config.SYNTH_LLM_BUDGET_S
            r = await (asyncio.wait_for(coro, budget) if budget > 0 else coro)
            text = str((r.data or {}).get("summary", "")).strip()
            kws = HYPOTHESES.get(rid, {}).get("keywords", ())
            valid = r.ok and len(text) > 30 and (not kws or any(k in text.lower() for k in kws))
            synthesis = {"mode": "llm" if valid else "template-fallback", "llm_ok": r.ok,
                         "latency_ms": r.latency_ms, "backend": r.backend,
                         "error": r.error or ("" if valid else "summary dropped the root cause"),
                         "raw_llm_summary": text}
            if valid:
                summary = text
        except asyncio.TimeoutError:
            synthesis = {"mode": "template-fallback", "llm_ok": False, "latency_ms": config.SYNTH_LLM_BUDGET_S * 1000,
                         "backend": "", "error": f"budget_exceeded ({config.SYNTH_LLM_BUDGET_S:.0f}s)"}

    return {
        "incident_detected": rid != "none",
        "summary": summary,
        "template_summary": template,
        "diagnosis": root,
        "evidence_chain": evidence,
        "affected_instances": insts,
        "deploy": dep,
        "action_plan": actions,
        "risk": risk,
        "team_tickets": tickets,
        "teams_paged": [t["team"] for t in tickets if t["page"]],
        "synthesis": synthesis,
    }


def brief_actions(brief: dict) -> list[str]:
    """The recommended actions as text, for the DQ scorer (same rubric for every condition)."""
    return [f"{a['action']} {a['command']}" for a in brief["action_plan"]]

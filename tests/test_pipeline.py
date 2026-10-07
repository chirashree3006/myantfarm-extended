"""Unit tests for the deterministic parts (no network, no model).

    cd myantfarm-extended && python -m pytest tests -q
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "multi-agent"))

from antfarm.agents import RULES  # noqa: E402
from antfarm.fusion import brief_actions, fuse  # noqa: E402
from antfarm.llm import extract_json  # noqa: E402
from antfarm.scoring import extract_actions, score_actions, score_text  # noqa: E402
from antfarm.stats import compare  # noqa: E402

STATUS = {"web1": {"instance": "web1", "service": "checkout-service", "deploy_version": "v2.3.0",
                   "previous_version": "v2.2.4", "fault_mode": "leak"},
          "web2": {"instance": "web2", "service": "checkout-service", "deploy_version": "v2.3.0",
                   "previous_version": "v2.2.4", "fault_mode": "leak"}}
AUTH_STATUS = {"web1": {"instance": "web1", "service": "auth-service", "deploy_version": "v2.3.1",
                        "previous_version": "v2.3.0", "fault_mode": "auth_regression"}}


def leak_streams():
    db = [{"instance": "web1", "active_connections": i, "pool_size": 10,
           "pool_utilization_pct": i * 10.0, "avg_wait_time_ms": 10 if i < 10 else 1500,
           "timestamp": f"t{i:02d}"} for i in range(1, 11)] + \
         [{"instance": "web1", "active_connections": 10, "pool_size": 10, "pool_utilization_pct": 100.0,
           "avg_wait_time_ms": 1800, "timestamp": "t20"}]
    srv = [{"instance": "web1", "level": "ERROR", "deploy_version": "v2.3.0", "endpoint": "/api/checkout",
            "message": "Connection pool exhausted: 10/10 connections in use", "timestamp": "t2"}] * 6 + \
          [{"instance": "web1", "level": "INFO", "deploy_version": "v2.3.0", "message": "Checkout completed successfully",
            "timestamp": "t1"}] * 4
    fe = [{"instance": "web1", "status_code": 500, "page": "/checkout",
           "message": "Checkout failed: TypeError: Failed to fetch", "timestamp": "t3"}] * 6
    us = [{"instance": "web1", "text": "Checkout button just spins and then fails.", "timestamp": "t4"}] * 2
    return {"frontend": {"records": fe}, "server": {"records": srv},
            "database": {"records": db}, "users": {"records": us}}


def slow_streams():
    db = [{"instance": "web2", "active_connections": 1, "pool_size": 10, "pool_utilization_pct": 10.0,
           "avg_wait_time_ms": 600 + i, "timestamp": f"t{i}"} for i in range(10)]
    srv = [{"instance": "web2", "level": "WARN", "deploy_version": "v2.3.0", "query_ms": 500,
            "message": "Slow query: SELECT * FROM orders WHERE user_id=? took 500ms", "timestamp": "t1"}] * 5 + \
          [{"instance": "web2", "level": "ERROR", "deploy_version": "v2.3.0", "query_ms": 800,
            "message": "Query timeout after 800ms: SELECT * FROM orders WHERE user_id=?", "timestamp": "t2"}] * 3
    fe = [{"instance": "web2", "status_code": 504, "page": "/checkout",
           "message": "Checkout failed: 504 Gateway Timeout", "timestamp": "t3"}] * 3
    us = [{"instance": "web2", "text": "Checkout is extremely slow, sometimes it times out.", "timestamp": "t4"}]
    return {"frontend": {"records": fe}, "server": {"records": srv},
            "database": {"records": db}, "users": {"records": us}}


def auth_streams():
    db = [{"instance": "web1", "active_connections": 8 + i % 2, "pool_size": 10,
           "pool_utilization_pct": 80.0 + 10 * (i % 2), "avg_wait_time_ms": 10, "timestamp": f"t{i:02d}"}
          for i in range(12)]
    srv = [{"instance": "web1", "level": "ERROR", "deploy_version": "v2.3.1", "service": "auth-service",
            "endpoint": "/auth/login", "message": "Login failed: AuthTokenSigner raised KeyError('kid')",
            "timestamp": "t2"}] * 9 + \
          [{"instance": "web1", "level": "INFO", "deploy_version": "v2.3.0", "service": "checkout-service",
            "endpoint": "/api/checkout", "message": "Checkout completed successfully", "timestamp": "t1"}] * 10
    fe = [{"instance": "web1", "status_code": 500, "page": "/login",
           "message": "Sign-in failed: 500 Internal Server Error", "timestamp": "t3"}] * 9
    us = [{"instance": "web1", "text": "I can't log in. It says something went wrong after I enter my password.",
           "timestamp": "t4"}] * 3
    return {"frontend": {"records": fe}, "server": {"records": srv},
            "database": {"records": db}, "users": {"records": us}}


def run(streams, status=STATUS):
    findings = {r: RULES[r](s) for r, s in streams.items()}
    return asyncio.run(fuse(findings, status, None))


def dq(brief, scenario):
    acts = brief_actions(brief)
    return score_actions(acts, scenario, brief["summary"] + " " + " ".join(acts))


def test_leak_diagnosed_with_corroboration():
    b = run(leak_streams())
    assert b["diagnosis"]["id"] == "db_connection_leak"
    assert len(b["diagnosis"]["supporting_agents"]) >= 3
    assert any(a["type"] == "rollback" and "v2.2.4" in a["command"] for a in b["action_plan"])
    d = dq(b, "leak")
    assert d["actionable"] and d["root_cause_correct"]
    assert "Backend team" in b["teams_paged"] and "Customer support" in b["teams_paged"]


def test_slow_db_not_confused_with_leak():
    b = run(slow_streams())
    assert b["diagnosis"]["id"] == "slow_db_queries"
    assert any("CREATE INDEX" in a for a in brief_actions(b))
    assert dq(b, "slow_db")["root_cause_correct"]


def test_paper_scenario_auth_regression():
    b = run(auth_streams(), AUTH_STATUS)
    assert b["diagnosis"]["id"] == "auth_regression"
    assert "v2.3.0" in b["action_plan"][0]["action"] and "auth-service" in b["action_plan"][0]["action"]
    d = dq(b, "auth_regression")
    assert d["actionable"] and d["root_cause_correct"] and d["dq"] > 0.6


def test_healthy():
    empty = {r: {"records": []} for r in ("frontend", "server", "database", "users")}
    b = run(empty)
    assert not b["incident_detected"] and b["team_tickets"] == []


def test_vague_answer_scores_low():
    vague = "There seem to be issues. The team should investigate further and check recent changes."
    s = score_text(vague, "paper_static")
    assert s["validity"] == 1.0 and s["specificity"] == 0 and not s["actionable"]   # paper C2 pattern: ~0.40


def test_paper_rubric_reproduces_paper_c3_components():
    # Specificity tiers 1.0 / 0.67 / 0 and correctness tiers 0.75 / 0.25 / 0.25 -> paper's 0.557 and 0.417
    acts = ["Rollback auth-service deployment to v2.3.0",         # S 1.0  R: 5/9 tokens -> 0.75
            "Check the connection pool on auth-service",          # S 0.67 R: 2/9 -> 0.25
            "Tell customers about the delay"]                     # S 0
    s = score_actions(acts, "paper_static")
    assert s["specificity"] == round((1.0 + 0.67 + 0) / 3, 3)
    assert s["validity"] == 1.0


def test_action_extraction_and_stats():
    assert extract_actions("1. Restart web1\n2. Roll back to v2.2.4") == ["Restart web1", "Roll back to v2.2.4"]
    st = compare({"C2": [0.40, 0.41, 0.40, 0.43], "C3": [0.69, 0.69, 0.70, 0.69]})
    assert st["pairs"][0]["significant"]


def test_extract_json_from_chatter():
    assert extract_json('Sure! {"observation": "pool full", "severity": "high"} hope it helps') == \
        {"observation": "pool full", "severity": "high"}
    assert extract_json("no json here") is None


SURGE_STATUS = {"web1": {"instance": "web1", "service": "web-tier", "deploy_version": "v2.3.0",
                         "previous_version": "v2.2.4", "fault_mode": "surge"}}


def surge_streams():
    db = [{"instance": "web1", "active_connections": 3, "pool_size": 10, "pool_utilization_pct": 30.0,
           "avg_wait_time_ms": 12, "timestamp": f"t{i}"} for i in range(10)]
    srv = [{"instance": "web1", "level": "WARN", "deploy_version": "v2.3.0", "service": "web-tier",
            "message": "Traffic spike: 106.2 req/s on this replica (normal under 5)", "timestamp": "t1"}] * 3 + \
          [{"instance": "web1", "level": "ERROR", "deploy_version": "v2.3.0", "service": "web-tier",
            "endpoint": "/auth/login",
            "message": "Request rejected: 106.2 req/s arriving exceed worker capacity 12 (traffic spike, 12 in flight)",
            "timestamp": "t2"}] * 20
    fe = [{"instance": "web1", "status_code": 503, "page": "/login",
           "message": "Side B is down: 503 Service Unavailable", "timestamp": "t3"}] * 20
    us = [{"instance": "web1", "text": "The site keeps showing 'Side B is down' during the drop. Nothing loads.",
           "timestamp": "t4"}] * 2
    return {"frontend": {"records": fe}, "server": {"records": srv},
            "database": {"records": db}, "users": {"records": us}}


def test_traffic_surge_scales_out_not_rollback():
    b = run(surge_streams(), SURGE_STATUS)
    assert b["diagnosis"]["id"] == "traffic_surge"
    execs = {a.get("execute") for a in b["action_plan"]}
    assert {"scale_out", "raise_capacity", "waiting_room"} <= execs
    assert not any(a["type"] == "rollback" for a in b["action_plan"])
    d = dq(b, "surge")
    assert d["actionable"] and d["root_cause_correct"]

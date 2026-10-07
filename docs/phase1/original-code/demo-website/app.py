"""
Demo website with a deliberately seeded bug: a database connection-pool leak.

When the bug is switched ON, every request to /api/checkout "borrows" a DB
connection but never gives it back (simulating a real leak, e.g. a missing
connection.close() after a new session-validation call). Once the pool is
exhausted, requests start failing with 500s -- which is exactly the kind of
incident your four monitoring agents (frontend / server / db / user reports)
are meant to detect and diagnose.

Run locally:
    pip install -r requirements.txt
    uvicorn app:app --reload --port 8080

Key endpoints:
    POST /api/checkout            -- the "real" endpoint users hit
    POST /admin/toggle-bug        -- flip the bug on/off
    POST /admin/reset             -- reset pool + all logs to a clean state
    POST /admin/simulate-traffic  -- fire N simulated requests at once
    GET  /admin/status            -- current pool + bug state

    GET  /logs/frontend           -- browser-side error log   (Frontend Agent)
    GET  /logs/server             -- backend application log  (Server Agent)
    GET  /metrics/db              -- DB pool metrics history  (Database Agent)
    GET  /reports/users           -- simulated support tickets (User Reports Agent)
"""

import random
import time
from collections import deque
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Demo Website - Seeded Connection Leak")

# Allow the agent services (running as other containers/ports) to call in freely.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# In-memory "backend" state
# ---------------------------------------------------------------------------

POOL_SIZE = 10          # total DB connections available -- small on purpose,
                         # so the leak exhausts it after only a few requests
DEPLOY_VERSION = "v2.3.0"
DEPLOY_TIME = datetime.now(timezone.utc).isoformat()

state = {
    "bug_enabled": False,
    "active_connections": 0,
    "total_requests": 0,
    "total_failures": 0,
}

MAX_LOG_ENTRIES = 200
frontend_errors = deque(maxlen=MAX_LOG_ENTRIES)
server_logs = deque(maxlen=MAX_LOG_ENTRIES)
db_metrics_history = deque(maxlen=MAX_LOG_ENTRIES)
user_reports = deque(maxlen=MAX_LOG_ENTRIES)


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def record_db_metric():
    """Snapshot current pool health -- this is what the Database Agent reads."""
    db_metrics_history.append(
        {
            "timestamp": now(),
            "active_connections": state["active_connections"],
            "pool_size": POOL_SIZE,
            "pool_utilization_pct": round(
                100 * state["active_connections"] / POOL_SIZE, 1
            ),
            "avg_wait_time_ms": (
                round(random.uniform(5, 15), 1)
                if state["active_connections"] < POOL_SIZE
                else round(random.uniform(800, 2500), 1)  # waiting on exhausted pool
            ),
        }
    )


# ---------------------------------------------------------------------------
# The endpoint users actually hit
# ---------------------------------------------------------------------------


@app.post("/api/checkout")
def checkout():
    state["total_requests"] += 1
    request_id = f"req_{state['total_requests']:05d}"

    # Try to "borrow" a DB connection.
    if state["active_connections"] >= POOL_SIZE:
        # Pool exhausted -- this is the incident.
        state["total_failures"] += 1
        record_db_metric()

        server_logs.append(
            {
                "timestamp": now(),
                "level": "ERROR",
                "request_id": request_id,
                "message": "Connection pool exhausted: "
                f"{state['active_connections']}/{POOL_SIZE} connections in use",
                "endpoint": "/api/checkout",
                "deploy_version": DEPLOY_VERSION,
            }
        )
        frontend_errors.append(
            {
                "timestamp": now(),
                "request_id": request_id,
                "message": "Checkout failed: TypeError: Failed to fetch",
                "page": "/checkout",
                "status_code": 500,
            }
        )
        # Once failures start piling up, users start complaining.
        if state["total_failures"] % 3 == 0:
            user_reports.append(
                {
                    "timestamp": now(),
                    "ticket_id": f"tix_{state['total_failures']:04d}",
                    "text": "Checkout button just spins and then fails. "
                    "Tried 3 times, same error every time.",
                }
            )
        return {"status": "error", "message": "Service temporarily unavailable"}, 500

    # Successful path: borrow the connection.
    state["active_connections"] += 1
    record_db_metric()
    time.sleep(random.uniform(0.01, 0.05))  # simulate query time

    server_logs.append(
        {
            "timestamp": now(),
            "level": "INFO",
            "request_id": request_id,
            "message": "Checkout completed successfully",
            "endpoint": "/api/checkout",
            "deploy_version": DEPLOY_VERSION,
        }
    )

    # THE BUG: if enabled, the connection is never released (leak).
    # If disabled, behave correctly and give the connection back.
    if not state["bug_enabled"]:
        state["active_connections"] -= 1

    return {"status": "ok", "request_id": request_id}


# ---------------------------------------------------------------------------
# Admin controls -- for you (or a trial runner script) to drive the demo
# ---------------------------------------------------------------------------


@app.post("/admin/toggle-bug")
def toggle_bug():
    state["bug_enabled"] = not state["bug_enabled"]
    return {"bug_enabled": state["bug_enabled"]}


@app.post("/admin/reset")
def reset():
    state.update(
        {
            "bug_enabled": False,
            "active_connections": 0,
            "total_requests": 0,
            "total_failures": 0,
        }
    )
    frontend_errors.clear()
    server_logs.clear()
    db_metrics_history.clear()
    user_reports.clear()
    return {"status": "reset"}


class TrafficRequest(BaseModel):
    count: int = 20


@app.post("/admin/simulate-traffic")
def simulate_traffic(req: TrafficRequest):
    """Fire N simulated checkout requests in a row -- this is how you'll
    trigger an incident on demand for a controlled trial."""
    results = [checkout() for _ in range(req.count)]
    failures = sum(1 for r in results if isinstance(r, tuple))
    return {
        "requests_sent": req.count,
        "failures": failures,
        "pool_state": f"{state['active_connections']}/{POOL_SIZE}",
    }


@app.get("/admin/status")
def status():
    return {
        **state,
        "pool_size": POOL_SIZE,
        "deploy_version": DEPLOY_VERSION,
        "deploy_time": DEPLOY_TIME,
    }


# ---------------------------------------------------------------------------
# The four data streams your monitoring agents will read
# ---------------------------------------------------------------------------


@app.get("/logs/frontend")
def get_frontend_errors(limit: int = 50):
    return list(frontend_errors)[-limit:]


@app.get("/logs/server")
def get_server_logs(limit: int = 50):
    return list(server_logs)[-limit:]


@app.get("/metrics/db")
def get_db_metrics(limit: int = 50):
    return list(db_metrics_history)[-limit:]


@app.get("/reports/users")
def get_user_reports(limit: int = 50):
    return list(user_reports)[-limit:]


@app.get("/")
def root():
    return {
        "service": "demo-website",
        "status": "running",
        "bug_enabled": state["bug_enabled"],
        "hint": "POST /admin/toggle-bug to enable the leak, then "
        "POST /admin/simulate-traffic to trigger an incident.",
    }

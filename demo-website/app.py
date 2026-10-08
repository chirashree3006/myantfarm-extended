"""
"Side B" -- a limited vinyl-drop record store. This is the production
system the monitoring agents watch. It runs as several replicas (web1..3)
behind the Nginx load balancer.

Real shop features
    - email/password accounts and Google sign-in (Google Identity Services)
    - product catalogue with shared stock, cart checkout, order history
    - stateless JWT sessions, so any replica can serve any user
    - shared SQLite database on a Docker volume (users, products, orders)

Seeded faults (switch per replica with POST /admin/fault)
    leak             DB connection-pool leak in checkout-service v2.3.0:
                     connections are never released -> pool exhausted -> 500s
    slow_db          missing index on orders.user_id -> slow queries -> 504s
    auth_regression  the reference paper's scenario: auth-service v2.3.1
                     regression after deploy -> ~45% of logins fail, DB pool
                     held at ~85% capacity. Ground truth: roll back
                     auth-service to v2.3.0 and verify the connection pool.

Chaos switch (fault-tolerance experiments)
    POST /chaos/down {"seconds": 60}  -> this replica answers 503 to everything
    POST /chaos/up                    -> back to normal

Monitoring streams (one per specialist agent)
    GET /logs/frontend  /logs/server  /metrics/db  /reports/users

Run locally (single instance):
    pip install -r requirements.txt
    uvicorn app:app --port 8080
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import random
import secrets
import socket
import sqlite3
import threading
import time
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import httpx
import jwt
from fastapi import FastAPI, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

INSTANCE_ID = os.getenv("INSTANCE_ID", socket.gethostname())
DATA_DIR = os.getenv("DATA_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))
DB_PATH = os.path.join(DATA_DIR, "shop.db")
JWT_SECRET = os.getenv("JWT_SECRET", "change-me-in-.env")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
POOL_SIZE = int(os.getenv("POOL_SIZE", "10"))
SLOW_QUERY_TIMEOUT_MS = int(os.getenv("SLOW_QUERY_TIMEOUT_MS", "600"))
AUTH_FAILURE_RATE = float(os.getenv("AUTH_FAILURE_RATE", "0.45"))
DEMO_EMAIL, DEMO_PASSWORD = "demo@sideb.store", "dropday2026"

FAULT_MODES = ("none", "leak", "slow_db", "auth_regression", "surge")
# Worker capacity: in-flight shop requests one replica can hold. A flash-sale
# surge makes every request slow (hot product rows, DB contention), so the
# workers fill up and extra requests are rejected with 503 -- the site "crashes".
DEFAULT_CAPACITY = int(os.getenv("WORKER_CAPACITY", "12"))
STANDBY_DEFAULT = os.getenv("STANDBY", "0") == "1"
AMBIENT_DEFAULT = os.getenv("AMBIENT", "1") == "1"   # background shoppers on a normal day   # web4 starts as a standby replica
SHOP_PATHS = ("/auth/", "/api/")
# (service that was just deployed, its version, last good version)
DEPLOYS = {
    "none": ("checkout-service", "v2.3.0", "v2.2.4"),
    "leak": ("checkout-service", "v2.3.0", "v2.2.4"),
    "slow_db": ("checkout-service", "v2.3.0", "v2.2.4"),
    "auth_regression": ("auth-service", "v2.3.1", "v2.3.0"),
    "surge": ("checkout-service", "v2.3.0", "v2.2.4"),
}

app = FastAPI(title=f"Side B record store ({INSTANCE_ID})")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
                   allow_headers=["*"], expose_headers=["X-Instance"])

# ---------------------------------------------------------------------------
# In-memory per-replica state (each app server has its own connection pool)
# ---------------------------------------------------------------------------
state = {"fault_mode": "none", "active_connections": 0, "total_requests": 0,
         "total_failures": 0, "login_attempts": 0, "login_failures": 0,
         "deploy_time": datetime.now(timezone.utc).isoformat(), "down_until": 0.0,
         "capacity": DEFAULT_CAPACITY, "inflight": 0, "rejected": 0, "queued": 0, "hits": 0,
         "waiting_room": False, "standby": STANDBY_DEFAULT, "ambient": AMBIENT_DEFAULT}
recent_hits: deque = deque(maxlen=20000)     # timestamps of shop requests (for req/s)
last_spike_log = [0.0]
lock = threading.Lock()

MAX_LOG = 200
frontend_errors: deque = deque(maxlen=MAX_LOG)
server_logs: deque = deque(maxlen=MAX_LOG)
db_metrics_history: deque = deque(maxlen=MAX_LOG)
user_reports: deque = deque(maxlen=MAX_LOG)


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def deploy() -> tuple[str, str, str]:
    return DEPLOYS[state["fault_mode"]]


# ---------------------------------------------------------------------------
# Middleware: replica tag + chaos switch
# ---------------------------------------------------------------------------
def rps(window: float = 5.0) -> float:
    cutoff = time.time() - window
    return round(sum(1 for t in reversed(recent_hits) if t >= cutoff) / window, 1)


def _overload(path: str) -> JSONResponse:
    """Worker pool full: reject. With the waiting room on, queue the shopper instead."""
    if state["waiting_room"]:
        state["queued"] += 1
        return JSONResponse({"error": "waiting_room", "message": "Side B is busy with the drop. You're in the queue; "
                             "this page retries by itself."}, status_code=429, headers={"Retry-After": "3"})
    state["rejected"] += 1
    state["total_failures"] += 1
    rid = f"{INSTANCE_ID}-rej_{state['rejected']:05d}"
    page = "/login" if path.startswith("/auth") else "/checkout" if "checkout" in path else "/"
    server_logs.append({"timestamp": now(), "instance": INSTANCE_ID, "level": "ERROR", "request_id": rid,
                        "message": f"Request rejected: {rps()} req/s arriving exceed worker capacity "
                                   f"{state['capacity']} (traffic spike, {state['inflight']} in flight)",
                        "endpoint": path, "service": "web-tier", "deploy_version": DEPLOYS["surge"][1]})
    frontend_errors.append({"timestamp": now(), "instance": INSTANCE_ID, "request_id": rid,
                            "message": "Side B is down: 503 Service Unavailable", "page": page, "status_code": 503})
    if state["rejected"] % 25 == 0:
        _user_report("The site keeps showing 'Side B is down' during the drop. Nothing loads.")
    return JSONResponse({"error": "overloaded", "message": "Side B is down: too many shoppers at once. "
                         "Nothing was charged. Try again in a minute."}, status_code=503)


@app.middleware("http")
async def chaos_and_tag(request: Request, call_next):
    path = request.url.path
    if time.time() < state["down_until"] and not path.startswith("/chaos"):
        resp = JSONResponse({"error": f"{INSTANCE_ID} is down (chaos test)"}, status_code=503)
    elif path.startswith(SHOP_PATHS) and state["standby"]:
        # standby replica: not in service yet, the load balancer skips it
        resp = JSONResponse({"error": f"{INSTANCE_ID} is a standby replica (not in service)"}, status_code=503)
    elif path.startswith(SHOP_PATHS):
        recent_hits.append(time.time())
        state["hits"] += 1
        r = rps()
        if r >= 40 and time.time() - last_spike_log[0] > 3:
            last_spike_log[0] = time.time()
            server_logs.append({"timestamp": now(), "instance": INSTANCE_ID, "level": "WARN",
                                "request_id": "-", "message": f"Traffic spike: {r} req/s on this replica (normal under 5)",
                                "endpoint": path, "service": "web-tier", "deploy_version": DEPLOYS["surge"][1]})
        # workers are full, or (during a surge) arrivals outrun what the workers can serve per second
        if state["inflight"] >= state["capacity"] or (state["fault_mode"] == "surge" and r > state["capacity"] * 2):
            resp = _overload(path)
        else:
            state["inflight"] += 1
            try:
                if state["fault_mode"] == "surge":
                    await asyncio.sleep(random.uniform(0.5, 0.9))   # flash-sale contention
                resp = await call_next(request)
            finally:
                state["inflight"] -= 1
    else:
        resp = await call_next(request)
    resp.headers["X-Instance"] = INSTANCE_ID
    return resp


# ---------------------------------------------------------------------------
# Shared database (users, products, orders)
# ---------------------------------------------------------------------------
PRODUCTS = [
    # id, title, artist, year, price_inr, stock, pressing, palette(sleeve, ink, label, vinyl), pattern, tracks, notes
    ("sb-014", "Graceful Degradation", "Ant Colony Collective", 2026, 2899, 300,
     "180g pink marble", ["#FF4F9A", "#3255FF", "#FFD23F", "#FF8CC0"], "rings",
     ["Fallback Mode", "Heartbeat", "Quorum", "Half-Open Circuit", "Steady State"],
     "Twelve musicians, no conductor. Recorded live to tape in one take per side."),
    ("sb-013", "Least Connections", "Mira Vale", 2026, 2499, 250,
     "140g clear blue", ["#3255FF", "#FFD23F", "#FF4F9A", "#7FA0FF"], "grid",
     ["Round Robin", "Sticky Session", "Upstream", "Weight 3"],
     "Bedroom synth-pop about long-distance friendships and slow Wi-Fi."),
    ("sb-012", "Cold Start", "Northbound Static", 2025, 2199, 400,
     "180g black", ["#1B1F3B", "#FF4F9A", "#EEF0F3", "#0A0A0A"], "stripes",
     ["Boot", "First Request", "Warm Pool", "Idle Timeout"],
     "Instrumental post-rock. Side B is one 19-minute track."),
    ("sb-011", "Paper Lanterns, Kochi", "Anjali Rao Trio", 2025, 2699, 200,
     "180g yellow splatter", ["#FFD23F", "#1B1F3B", "#FF4F9A", "#FFE58A"], "dots",
     ["Fort Kochi, 6am", "Backwater", "Chinese Nets", "Lantern Song"],
     "Piano trio recorded in a Mattancherry warehouse during the monsoon."),
    ("sb-010", "Heartbeat Interval", "Sora & the Replicas", 2025, 2399, 300,
     "half pink / half blue", ["#FF4F9A", "#1B1F3B", "#3255FF", "#FF4F9A"], "waves",
     ["Ping", "Pong", "Missed Beat", "Leader Election"],
     "Dance-punk with a drum machine that was definitely not in time."),
    ("sb-009", "Monsoon Dub Versions", "Dub Kolkata Soundsystem", 2024, 2999, 150,
     "180g green", ["#2E9E6B", "#FFD23F", "#1B1F3B", "#2E9E6B"], "rings",
     ["Howrah Echo", "Rain Delay", "Tram Line Version", "Ganga Bass"],
     "Reworked dub plates from a decade of rooftop sound-system nights."),
    ("sb-008", "Failover Lullabies", "Ines Marrow", 2024, 1999, 350,
     "140g white", ["#EEF0F3", "#3255FF", "#FF4F9A", "#F7F7F7"], "grid",
     ["Standby", "Secondary", "Promote", "Sleep Now"],
     "Soft folk for people who get paged at 3am."),
    ("sb-007", "Rollback to Summer", "The v2.3.0s", 2024, 2299, 250,
     "180g orange", ["#FF7A3D", "#1B1F3B", "#FFD23F", "#FF7A3D"], "stripes",
     ["Revert", "Known Good", "Hotfix Heart", "Tag It"],
     "Jangle-pop. Every chorus is about going back to how things were."),
]


@contextmanager
def db():
    con = sqlite3.connect(DB_PATH, timeout=15)
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    finally:
        con.close()


def hash_pw(pw: str, salt: str | None = None) -> str:
    salt = salt or secrets.token_hex(8)
    h = hashlib.pbkdf2_hmac("sha256", pw.encode(), salt.encode(), 60_000).hex()
    return f"{salt}${h}"


def check_pw(pw: str, stored: str) -> bool:
    if not stored or "$" not in stored:
        return False
    salt = stored.split("$", 1)[0]
    return hmac.compare_digest(hash_pw(pw, salt), stored)


def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    for attempt in range(10):  # several replicas may start at once
        try:
            with db() as con:
                con.execute("PRAGMA journal_mode=WAL")
                con.executescript("""
                CREATE TABLE IF NOT EXISTS users(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE NOT NULL,
                    name TEXT, pw_hash TEXT, provider TEXT NOT NULL, picture TEXT, created TEXT);
                CREATE TABLE IF NOT EXISTS products(
                    id TEXT PRIMARY KEY, title TEXT, artist TEXT, year INT, price INT,
                    stock INT, initial_stock INT, pressing TEXT, palette TEXT, pattern TEXT,
                    tracks TEXT, notes TEXT);
                CREATE TABLE IF NOT EXISTS events(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, source TEXT, kind TEXT, text TEXT);
                CREATE TABLE IF NOT EXISTS presence(
                    user_id INTEGER PRIMARY KEY, name TEXT, picture TEXT, provider TEXT, last_seen REAL);
                CREATE TABLE IF NOT EXISTS orders(
                    id TEXT PRIMARY KEY, user_id INT, items TEXT, total INT,
                    instance TEXT, created TEXT);
                """)
                for p in PRODUCTS:
                    con.execute("INSERT OR IGNORE INTO products VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                                (p[0], p[1], p[2], p[3], p[4], p[5], p[5], p[6], json.dumps(p[7]),
                                 p[8], json.dumps(p[9]), p[10]))
                con.execute("INSERT OR IGNORE INTO users(email,name,pw_hash,provider,created) VALUES(?,?,?,?,?)",
                            (DEMO_EMAIL, "Demo Collector", hash_pw(DEMO_PASSWORD), "password", now()))
            return
        except sqlite3.OperationalError:
            time.sleep(0.3 * (attempt + 1))


init_db()


# ---------------------------------------------------------------------------
# Sessions (stateless JWT so every replica accepts every session)
# ---------------------------------------------------------------------------
def issue_token(user: sqlite3.Row) -> dict:
    touch_presence(user["id"])
    payload = {"sub": str(user["id"]), "email": user["email"], "name": user["name"],
               "exp": datetime.now(timezone.utc) + timedelta(days=7)}
    return {"token": jwt.encode(payload, JWT_SECRET, algorithm="HS256"),
            "user": {"id": user["id"], "email": user["email"], "name": user["name"],
                     "provider": user["provider"], "picture": user["picture"]}}


def current_user(authorization: str | None) -> dict | None:
    if not authorization or not authorization.lower().startswith("bearer "):
        return None
    try:
        return jwt.decode(authorization[7:], JWT_SECRET, algorithms=["HS256"])
    except jwt.PyJWTError:
        return None


def err(msg: str, code: int) -> JSONResponse:
    return JSONResponse({"error": msg}, status_code=code)


# ---------------------------------------------------------------------------
# Telemetry helpers (what the agents read)
# ---------------------------------------------------------------------------
def held_connections() -> int:
    # auth_regression: long-lived sessions from the bad deploy hold ~85% of the pool
    return random.choice((8, 9)) if state["fault_mode"] == "auth_regression" else 0


def record_db_metric(wait_ms: float | None = None):
    active = min(POOL_SIZE, state["active_connections"] + held_connections())
    if state["fault_mode"] == "auth_regression":
        active = min(active, POOL_SIZE - 1)  # sessions hold ~85%; the pool itself still works
    if wait_ms is None:
        wait_ms = random.uniform(5, 15) if active < POOL_SIZE else random.uniform(800, 2500)
    db_metrics_history.append({
        "timestamp": now(), "instance": INSTANCE_ID, "active_connections": active,
        "pool_size": POOL_SIZE, "pool_utilization_pct": round(100 * active / POOL_SIZE, 1),
        "avg_wait_time_ms": round(wait_ms, 1)})


def _log(level: str, request_id: str, message: str, endpoint: str = "/api/checkout", **extra):
    service, version, _ = deploy()
    if endpoint == "/api/checkout" and service == "auth-service":
        service, version = "checkout-service", "v2.3.0"
    server_logs.append({"timestamp": now(), "instance": INSTANCE_ID, "level": level,
                        "request_id": request_id, "message": message, "endpoint": endpoint,
                        "service": service, "deploy_version": version, **extra})


def _frontend_error(request_id: str, message: str, status_code: int, page: str = "/checkout"):
    frontend_errors.append({"timestamp": now(), "instance": INSTANCE_ID, "request_id": request_id,
                            "message": message, "page": page, "status_code": status_code})


def _rid(kind: str) -> str:
    return f"{INSTANCE_ID}-{kind}_{secrets.token_hex(3)}"


def _who(email: str) -> str:
    """First part of an email, for log lines (never the full address)."""
    return (email or "someone").split("@")[0]


def _user_report(text: str):
    user_reports.append({"timestamp": now(), "instance": INSTANCE_ID,
                         "ticket_id": f"tix_{INSTANCE_ID}_{len(user_reports) + 1:04d}", "text": text})


# ---------------------------------------------------------------------------
# Checkout core: the DB-pool logic with the seeded faults
# ---------------------------------------------------------------------------
def do_checkout() -> tuple[dict, int]:
    with lock:
        state["total_requests"] += 1
        rid = f"{INSTANCE_ID}-req_{state['total_requests']:05d}"
        mode = state["fault_mode"]
        if state["active_connections"] >= POOL_SIZE:
            state["total_failures"] += 1
            record_db_metric()
            _log("ERROR", rid, f"Connection pool exhausted: {state['active_connections']}/{POOL_SIZE} connections in use")
            _frontend_error(rid, "Checkout failed: TypeError: Failed to fetch", 500)
            if state["total_failures"] % 3 == 0:
                _user_report("Checkout button just spins and then fails. Tried 3 times, same error every time.")
            return {"status": "error", "error": "checkout_unavailable", "request_id": rid}, 500
        state["active_connections"] += 1

    query_ms = random.uniform(250, 900) if mode == "slow_db" else random.uniform(10, 50)
    time.sleep(query_ms / 1000)

    with lock:
        record_db_metric(wait_ms=query_ms if mode == "slow_db" else None)
        if mode == "slow_db" and query_ms > SLOW_QUERY_TIMEOUT_MS:
            state["total_failures"] += 1
            state["active_connections"] -= 1
            _log("ERROR", rid, f"Query timeout after {query_ms:.0f}ms: SELECT * FROM orders WHERE user_id=? "
                               "(full table scan, no index on orders.user_id)", query_ms=round(query_ms, 1))
            _frontend_error(rid, "Checkout failed: 504 Gateway Timeout", 504)
            if state["total_failures"] % 3 == 0:
                _user_report("Checkout is extremely slow, sometimes it times out after a long wait.")
            return {"status": "error", "error": "checkout_timeout", "request_id": rid}, 504
        if mode == "slow_db":
            _log("WARN", rid, f"Slow query: SELECT * FROM orders WHERE user_id=? took {query_ms:.0f}ms",
                 query_ms=round(query_ms, 1))
        else:
            _log("INFO", rid, "Checkout completed successfully")
        if mode != "leak":  # THE LEAK: connection never returned
            state["active_connections"] -= 1
    return {"status": "ok", "request_id": rid, "instance": INSTANCE_ID}, 200


def auth_fault_check() -> JSONResponse | None:
    """auth_regression: the new token signer in auth-service v2.3.1 crashes on ~45% of logins."""
    with lock:
        state["login_attempts"] += 1
        rid = f"{INSTANCE_ID}-auth_{state['login_attempts']:05d}"
        record_db_metric()
        if state["fault_mode"] != "auth_regression" or random.random() >= AUTH_FAILURE_RATE:
            return None
        state["login_failures"] += 1
        state["total_failures"] += 1
        _log("ERROR", rid, "Login failed: AuthTokenSigner raised KeyError('kid') while signing session "
                           "token (introduced in auth-service v2.3.1)", endpoint="/auth/login")
        _frontend_error(rid, "Sign-in failed: 500 Internal Server Error", 500, page="/login")
        if state["login_failures"] % 3 == 0:
            _user_report("I can't log in. It says something went wrong after I enter my password.")
    return err("Sign-in is temporarily unavailable. Your password was not the problem; try again.", 500)


# ---------------------------------------------------------------------------
# Shop API
# ---------------------------------------------------------------------------
class RegisterReq(BaseModel):
    email: str
    password: str
    name: str = ""


class LoginReq(BaseModel):
    email: str
    password: str


class GoogleReq(BaseModel):
    credential: str


@app.get("/auth/config")
def auth_config():
    return {"google_client_id": GOOGLE_CLIENT_ID or None, "demo_email": DEMO_EMAIL,
            "demo_password": DEMO_PASSWORD}


@app.post("/auth/register")
def register(req: RegisterReq):
    email = req.email.strip().lower()
    if "@" not in email or len(req.password) < 8:
        return err("Use a valid email and a password of at least 8 characters.", 400)
    try:
        with db() as con:
            con.execute("INSERT INTO users(email,name,pw_hash,provider,created) VALUES(?,?,?,?,?)",
                        (email, req.name.strip() or email.split("@")[0], hash_pw(req.password), "password", now()))
            user = con.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
    except sqlite3.IntegrityError:
        return err("An account with this email already exists. Sign in instead.", 409)
    _log("INFO", _rid("auth"), f"New account: {user['name']} signed up", endpoint="/auth/register")
    return issue_token(user)


@app.post("/auth/login")
def login(req: LoginReq):
    fault = auth_fault_check()
    if fault:
        return fault
    with db() as con:
        user = con.execute("SELECT * FROM users WHERE email=?", (req.email.strip().lower(),)).fetchone()
    if not user or not check_pw(req.password, user["pw_hash"]):
        _log("WARN", _rid("auth"), f"Sign-in rejected: wrong password for {_who(req.email)}", endpoint="/auth/login")
        return err("That email and password don't match an account.", 401)
    _log("INFO", _rid("auth"), f"Signed in: {user['name'] or _who(user['email'])} (password)", endpoint="/auth/login")
    return issue_token(user)


@app.post("/auth/google")
async def google_login(req: GoogleReq):
    if not GOOGLE_CLIENT_ID:
        return err("Google sign-in isn't configured on this server (GOOGLE_CLIENT_ID is empty).", 501)
    fault = auth_fault_check()
    if fault:
        return fault
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get("https://oauth2.googleapis.com/tokeninfo", params={"id_token": req.credential})
        info = r.json()
    except Exception:  # noqa: BLE001
        return err("Couldn't reach Google to verify the sign-in. Try again.", 502)
    if r.status_code != 200 or info.get("aud") != GOOGLE_CLIENT_ID or \
            info.get("iss") not in ("accounts.google.com", "https://accounts.google.com") or \
            str(info.get("email_verified")).lower() != "true":
        return err("Google sign-in couldn't be verified.", 401)
    email = info["email"].lower()
    with db() as con:
        con.execute("INSERT OR IGNORE INTO users(email,name,provider,picture,created) VALUES(?,?,?,?,?)",
                    (email, info.get("name") or email.split("@")[0], "google", info.get("picture"), now()))
        con.execute("UPDATE users SET picture=COALESCE(?, picture) WHERE email=?", (info.get("picture"), email))
        user = con.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
    _log("INFO", _rid("auth"), f"Signed in: {user['name'] or _who(email)} (Google)", endpoint="/auth/google")
    return issue_token(user)


ONLINE_WINDOW_S = 90


def touch_presence(user_id: int):
    """Mark a signed-in shopper as online (shared SQLite, so every replica sees everyone)."""
    try:
        with db() as con:
            u = con.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
            if u:
                con.execute("INSERT OR REPLACE INTO presence VALUES(?,?,?,?,?)",
                            (u["id"], u["name"] or u["email"].split("@")[0], u["picture"], u["provider"], time.time()))
    except sqlite3.OperationalError:
        pass


def online_users() -> list[dict]:
    with db() as con:
        rows = con.execute("SELECT * FROM presence WHERE last_seen > ? ORDER BY last_seen DESC",
                           (time.time() - ONLINE_WINDOW_S,)).fetchall()
    return [{"name": r["name"], "picture": r["picture"], "provider": r["provider"],
             "seen_s_ago": round(time.time() - r["last_seen"])} for r in rows]


@app.post("/api/presence")
def presence(authorization: str | None = Header(None)):
    u = current_user(authorization)
    if u:
        touch_presence(int(u["sub"]))
    people = online_users()
    return {"count": len(people), "people": people}


@app.get("/api/online")
def online():
    people = online_users()
    return {"count": len(people), "people": people}


@app.post("/auth/logout")
def logout(authorization: str | None = Header(None)):
    u = current_user(authorization)
    if u:
        with db() as con:
            con.execute("DELETE FROM presence WHERE user_id=?", (int(u["sub"]),))
        _log("INFO", _rid("auth"), f"Signed out: {u.get('name') or _who(u.get('email', ''))}", endpoint="/auth/logout")
    return {"ok": True}


@app.get("/auth/me")
def me(authorization: str | None = Header(None)):
    u = current_user(authorization)
    if not u:
        return err("Not signed in.", 401)
    with db() as con:
        user = con.execute("SELECT * FROM users WHERE id=?", (int(u["sub"]),)).fetchone()
    if not user:
        return err("Not signed in.", 401)
    return issue_token(user)["user"]


@app.get("/api/products")
def products():
    with db() as con:
        rows = con.execute("SELECT * FROM products ORDER BY id DESC").fetchall()
    return [{**dict(r), "palette": json.loads(r["palette"]), "tracks": json.loads(r["tracks"])} for r in rows]


class Item(BaseModel):
    id: str
    qty: int = 1


class CheckoutReq(BaseModel):
    items: list[Item] = []


@app.post("/api/checkout")
def checkout(req: CheckoutReq | None = None, authorization: str | None = Header(None)):
    """With items: a real order (requires sign-in). Without items: a synthetic
    checkout used by the load generator -- same DB-pool path, no stock change."""
    items = (req.items if req else []) or []
    user = current_user(authorization)
    if items and not user:
        return err("Sign in to check out.", 401)
    body, code = do_checkout()
    if code != 200 or not items:
        if code != 200:
            body["message"] = ("We couldn't reach our order database, so nothing was charged. "
                               "Try again in a minute." if code == 500 else
                               "The order system is responding slowly and the request timed out. "
                               "Nothing was charged. Try again.")
        return JSONResponse(body, status_code=code)
    total, lines = 0, []
    with db() as con:
        for it in items:
            qty = max(1, min(it.qty, 3))
            p = con.execute("SELECT * FROM products WHERE id=?", (it.id,)).fetchone()
            if not p:
                con.rollback()
                return err(f"Unknown record {it.id}.", 404)
            cur = con.execute("UPDATE products SET stock=stock-? WHERE id=? AND stock>=?", (qty, it.id, qty))
            if cur.rowcount == 0:
                con.rollback()
                return err(f"“{p['title']}” sold out before your order went through.", 409)
            total += p["price"] * qty
            lines.append({"id": it.id, "title": p["title"], "artist": p["artist"], "qty": qty, "price": p["price"]})
        oid = f"SB-{secrets.token_hex(3).upper()}"
        con.execute("INSERT INTO orders VALUES(?,?,?,?,?,?)",
                    (oid, int(user["sub"]), json.dumps(lines), total, INSTANCE_ID, now()))
    return {"status": "ok", "order_id": oid, "total": total, "items": lines, "instance": INSTANCE_ID}


@app.get("/api/orders")
def my_orders(authorization: str | None = Header(None)):
    u = current_user(authorization)
    if not u:
        return err("Sign in to see your orders.", 401)
    with db() as con:
        rows = con.execute("SELECT * FROM orders WHERE user_id=? ORDER BY created DESC", (int(u["sub"]),)).fetchall()
    return [{**dict(r), "items": json.loads(r["items"])} for r in rows]


# ---------------------------------------------------------------------------
# Admin controls (coordinator / benchmark drive these)
# ---------------------------------------------------------------------------
@app.post("/admin/toggle-bug")
def toggle_bug():
    with lock:
        state["fault_mode"] = "none" if state["fault_mode"] == "leak" else "leak"
    return {"bug_enabled": state["fault_mode"] == "leak", "fault_mode": state["fault_mode"], "instance": INSTANCE_ID}


class FaultReq(BaseModel):
    mode: str = "leak"


@app.post("/admin/fault")
def set_fault(req: FaultReq):
    if req.mode not in FAULT_MODES:
        return err(f"mode must be one of {FAULT_MODES}", 400)
    with lock:
        if req.mode == "none" and state["fault_mode"] == "leak":
            state["active_connections"] = 0   # a rollback redeploys the service, which frees the leaked connections
        state["fault_mode"] = req.mode
        state["deploy_time"] = now()  # the faulty deploy "just happened"
    return {"fault_mode": req.mode, "instance": INSTANCE_ID}


@app.post("/admin/reset")
def reset():
    with lock:
        state.update({"fault_mode": "none", "active_connections": 0, "total_requests": 0,
                      "total_failures": 0, "login_attempts": 0, "login_failures": 0,
                      "capacity": DEFAULT_CAPACITY, "rejected": 0, "queued": 0, "hits": 0,
                      "waiting_room": False, "standby": STANDBY_DEFAULT})
        recent_hits.clear()
        for q in (frontend_errors, server_logs, db_metrics_history, user_reports):
            q.clear()
    return {"status": "reset", "instance": INSTANCE_ID}


class CapacityReq(BaseModel):
    inflight: int = 40


class FlagReq(BaseModel):
    on: bool = True


@app.post("/admin/capacity")
def set_capacity(req: CapacityReq):
    """'Add workers': raise how many requests this replica handles at once."""
    state["capacity"] = max(1, min(req.inflight, 500))
    return {"instance": INSTANCE_ID, "capacity": state["capacity"]}


@app.post("/admin/waiting-room")
def set_waiting_room(req: FlagReq):
    """Queue extra shoppers (HTTP 429 + auto-retry) instead of failing them."""
    state["waiting_room"] = req.on
    return {"instance": INSTANCE_ID, "waiting_room": state["waiting_room"]}


class EventReq(BaseModel):
    source: str = "api"      # ops console | backend console | agents | load test
    kind: str = "info"       # info | fault | fix | traffic | crash | alert
    text: str


@app.post("/admin/events")
def add_event(req: EventReq):
    """Shared control-room log: both consoles write here and both read it, so an action on one
    page shows up on the other. Stored in the shared SQLite, so every replica serves the same list."""
    with db() as con:
        cur = con.execute("INSERT INTO events(ts,source,kind,text) VALUES(?,?,?,?)",
                          (time.time(), req.source[:40], req.kind[:20], req.text[:400]))
        con.execute("DELETE FROM events WHERE id < ?", (cur.lastrowid - 500,))
    return {"id": cur.lastrowid}


@app.get("/admin/events")
def list_events(since_id: int = 0, limit: int = 60):
    with db() as con:
        if since_id:
            rows = con.execute("SELECT * FROM events WHERE id > ? ORDER BY id LIMIT ?", (since_id, limit)).fetchall()
        else:
            rows = con.execute("SELECT * FROM (SELECT * FROM events ORDER BY id DESC LIMIT ?) ORDER BY id", (limit,)).fetchall()
    return [dict(r) for r in rows]


@app.post("/admin/ambient")
def set_ambient(req: FlagReq):
    state["ambient"] = req.on
    return {"ambient": req.on, "instance": INSTANCE_ID}


@app.post("/admin/standby")
def set_standby(req: FlagReq):
    """Scale out: a standby replica (web4) joins the load-balancer pool when standby is turned off."""
    state["standby"] = req.on
    return {"instance": INSTANCE_ID, "standby": state["standby"]}


@app.post("/admin/restock")
def restock():
    with db() as con:
        con.execute("UPDATE products SET stock=initial_stock")
    return {"status": "restocked"}


class TrafficReq(BaseModel):
    count: int = 20


@app.post("/admin/simulate-traffic")
def simulate_traffic(req: TrafficReq):
    results = [do_checkout() for _ in range(req.count)]
    return {"instance": INSTANCE_ID, "requests_sent": req.count,
            "failures": sum(1 for _, c in results if c >= 400),
            "pool_state": f"{state['active_connections']}/{POOL_SIZE}"}


@app.get("/admin/status")
def status():
    service, version, prev = deploy()
    active = state["active_connections"] + (8 if state["fault_mode"] == "auth_regression" else 0)
    return {**{k: v for k, v in state.items() if k != "down_until"},
            "bug_enabled": state["fault_mode"] == "leak", "instance": INSTANCE_ID,
            "pool_size": POOL_SIZE, "pool_utilization_pct": round(100 * min(active, POOL_SIZE) / POOL_SIZE, 1),
            "service": service, "deploy_version": version, "previous_version": prev,
            "chaos_down": time.time() < state["down_until"], "rps": rps()}


@app.get("/health")
def health():
    return {"status": "up", "instance": INSTANCE_ID}


# ---------------------------------------------------------------------------
# Chaos switch
# ---------------------------------------------------------------------------
class ChaosReq(BaseModel):
    seconds: int = 60


@app.post("/chaos/down")
def chaos_down(req: ChaosReq | None = None):
    secs = max(1, min((req.seconds if req else 60), 600))
    state["down_until"] = time.time() + secs
    return {"instance": INSTANCE_ID, "down_for_s": secs}


@app.post("/chaos/up")
def chaos_up():
    state["down_until"] = 0.0
    return {"instance": INSTANCE_ID, "down": False}


@app.get("/chaos/status")
def chaos_status():
    left = max(0.0, state["down_until"] - time.time())
    return {"instance": INSTANCE_ID, "down": left > 0, "seconds_left": round(left)}


# ---------------------------------------------------------------------------
# The four monitoring streams
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
    return {"service": "side-b-store", "instance": INSTANCE_ID, "fault_mode": state["fault_mode"],
            "hint": "POST /admin/fault {\"mode\": \"leak|slow_db|auth_regression\"}"}

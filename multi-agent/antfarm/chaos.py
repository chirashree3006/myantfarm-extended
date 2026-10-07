"""Chaos switch for fault-tolerance experiments.

POST /chaos/down {"seconds": 60} makes this instance answer 503 to every
request (like a crashed container), so Nginx marks it failed and routes
around it. POST /chaos/up restores it. Used by loadtest/resilience.py and
the ops dashboard.
"""

import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import config

_state = {"down_until": 0.0}


class ChaosReq(BaseModel):
    seconds: int = 60


def install(app: FastAPI):
    @app.middleware("http")
    async def _chaos(request: Request, call_next):
        if time.time() < _state["down_until"] and not request.url.path.startswith("/chaos"):
            resp = JSONResponse({"error": f"{config.INSTANCE_ID} is down (chaos test)"}, status_code=503)
        else:
            resp = await call_next(request)
        resp.headers["X-Served-By"] = config.INSTANCE_ID
        return resp

    @app.post("/chaos/down")
    async def chaos_down(req: ChaosReq | None = None):
        secs = max(1, min((req.seconds if req else 60), 600))
        _state["down_until"] = time.time() + secs
        return {"instance": config.INSTANCE_ID, "down_for_s": secs}

    @app.post("/chaos/up")
    async def chaos_up():
        _state["down_until"] = 0.0
        return {"instance": config.INSTANCE_ID, "down": False}

    @app.get("/chaos/status")
    async def chaos_status():
        left = max(0.0, _state["down_until"] - time.time())
        return {"instance": config.INSTANCE_ID, "down": left > 0, "seconds_left": round(left)}

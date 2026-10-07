"""
Extended TinyLlama interface for multiple agents.

The v1 scripts did a bare `urllib` POST to one Ollama URL. With several
agents calling the model at the same time that breaks down (timeouts,
one slow call blocking others, no way to tell which agent used what).
This client adds:

  * multiple Ollama backends with client-side round-robin load balancing
    and automatic failover (a failing backend is benched for a cooldown)
  * a concurrency limit (semaphore) so N agents don't overload a CPU box
  * deterministic decoding (temperature 0 + fixed seed) -- the paper's
    "deterministic decision support" property
  * JSON mode (Ollama `format: "json"`) + tolerant JSON extraction and
    key validation, because a 1B model often wraps JSON in chatter
  * retries with backoff and a per-call timeout
  * per-agent and per-backend metrics (calls, failures, latency, tokens)
    exposed at /metrics by every service
"""

from __future__ import annotations

import asyncio
import itertools
import json
import re
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field

import httpx

from . import config


@dataclass
class LLMResult:
    text: str = ""
    ok: bool = False
    agent: str = ""
    backend: str = ""
    latency_ms: float = 0.0
    prompt_tokens: int = 0
    output_tokens: int = 0
    attempts: int = 0
    error: str = ""
    data: dict | None = None  # parsed JSON when json mode was used

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class _Stats:
    calls: int = 0
    failures: int = 0
    total_latency_ms: float = 0.0
    output_tokens: int = 0
    latencies: list = field(default_factory=list)

    def add(self, r: LLMResult):
        self.calls += 1
        if not r.ok:
            self.failures += 1
        self.total_latency_ms += r.latency_ms
        self.output_tokens += r.output_tokens
        self.latencies.append(r.latency_ms)
        self.latencies = self.latencies[-500:]

    def summary(self) -> dict:
        lat = sorted(self.latencies)
        pick = lambda q: round(lat[min(len(lat) - 1, int(q * len(lat)))], 1) if lat else 0.0
        return {
            "calls": self.calls,
            "failures": self.failures,
            "avg_latency_ms": round(self.total_latency_ms / self.calls, 1) if self.calls else 0.0,
            "p50_ms": pick(0.5),
            "p95_ms": pick(0.95),
            "output_tokens": self.output_tokens,
        }


class TinyLlamaClient:
    COOLDOWN_S = 15.0

    def __init__(
        self,
        backends: list[str] | None = None,
        model: str | None = None,
        max_concurrency: int | None = None,
        timeout_s: float | None = None,
        retries: int | None = None,
    ):
        self.backends = backends or list(config.OLLAMA_URLS)
        self.model = model or config.MODEL
        self.timeout_s = timeout_s or config.LLM_TIMEOUT_S
        self.retries = config.LLM_RETRIES if retries is None else retries
        self._sem = asyncio.Semaphore(max_concurrency or config.LLM_MAX_CONCURRENCY)
        self._rr = itertools.cycle(range(len(self.backends)))
        self._benched_until: dict[str, float] = {}
        self._client: httpx.AsyncClient | None = None
        self.by_agent: dict[str, _Stats] = defaultdict(_Stats)
        self.by_backend: dict[str, _Stats] = defaultdict(_Stats)

    # ------------------------------------------------------------------ infra
    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self.timeout_s)
        return self._client

    async def aclose(self):
        if self._client is not None:
            await self._client.aclose()

    def _next_backend(self) -> str:
        now = time.monotonic()
        for _ in range(len(self.backends)):
            b = self.backends[next(self._rr)]
            if self._benched_until.get(b, 0) <= now:
                return b
        # everything benched -> try the one that recovers soonest
        return min(self.backends, key=lambda b: self._benched_until.get(b, 0))

    async def health(self) -> dict:
        out = {}
        for b in self.backends:
            try:
                r = await self.client.get(f"{b}/api/tags", timeout=5)
                models = [m.get("name", "") for m in r.json().get("models", [])]
                out[b] = {"up": True, "model_loaded": any(m.startswith(self.model) for m in models)}
            except Exception as e:  # noqa: BLE001
                out[b] = {"up": False, "error": type(e).__name__}
        return out

    def metrics(self) -> dict:
        return {
            "model": self.model,
            "backends": self.backends,
            "benched": {b: round(t - time.monotonic(), 1)
                        for b, t in self._benched_until.items() if t > time.monotonic()},
            "by_agent": {k: v.summary() for k, v in self.by_agent.items()},
            "by_backend": {k: v.summary() for k, v in self.by_backend.items()},
        }

    # --------------------------------------------------------------- generate
    async def generate(
        self,
        prompt: str,
        *,
        agent: str = "default",
        system: str | None = None,
        json_mode: bool = False,
        num_predict: int | None = None,
    ) -> LLMResult:
        result = LLMResult(agent=agent)
        if config.LLM_DISABLED:
            result.error = "llm_disabled"
            return result

        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": config.LLM_TEMPERATURE,
                "seed": config.LLM_SEED,
                "num_predict": num_predict or config.LLM_NUM_PREDICT,
            },
        }
        if system:
            payload["system"] = system
        if json_mode:
            payload["format"] = "json"

        start = time.perf_counter()
        async with self._sem:
            for attempt in range(1, self.retries + 2):
                backend = self._next_backend()
                result.attempts = attempt
                result.backend = backend
                t0 = time.perf_counter()
                try:
                    r = await self.client.post(f"{backend}/api/generate", json=payload)
                    r.raise_for_status()
                    body = r.json()
                    result.text = (body.get("response") or "").strip()
                    result.prompt_tokens = int(body.get("prompt_eval_count") or 0)
                    result.output_tokens = int(body.get("eval_count") or 0)
                    result.ok = bool(result.text)
                    result.error = "" if result.ok else "empty_response"
                    self.by_backend[backend].add(
                        LLMResult(ok=result.ok, latency_ms=(time.perf_counter() - t0) * 1000,
                                  output_tokens=result.output_tokens))
                    if result.ok:
                        break
                except Exception as e:  # noqa: BLE001
                    result.error = f"{type(e).__name__}: {e}"[:200]
                    self._benched_until[backend] = time.monotonic() + self.COOLDOWN_S
                    self.by_backend[backend].add(
                        LLMResult(ok=False, latency_ms=(time.perf_counter() - t0) * 1000))
                    await asyncio.sleep(0.5 * attempt)

        result.latency_ms = round((time.perf_counter() - start) * 1000, 1)
        self.by_agent[agent].add(result)
        return result

    async def generate_json(
        self,
        prompt: str,
        *,
        agent: str = "default",
        required_keys: tuple[str, ...] = (),
        system: str | None = None,
        num_predict: int | None = None,
    ) -> LLMResult:
        """Ask for JSON and validate it. `result.data` is the parsed dict,
        or None if the model produced nothing usable (callers then fall back
        to their deterministic rule-based output)."""
        r = await self.generate(prompt, agent=agent, system=system, json_mode=True,
                                num_predict=num_predict)
        if r.ok:
            data = extract_json(r.text)
            if data is not None and all(k in data and str(data[k]).strip() for k in required_keys):
                r.data = data
            else:
                r.ok = False
                r.error = "invalid_json_or_missing_keys"
        return r


_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def extract_json(text: str) -> dict | None:
    """Tolerant JSON extraction: TinyLlama often adds prose around JSON."""
    if not text:
        return None
    try:
        v = json.loads(text)
        return v if isinstance(v, dict) else None
    except json.JSONDecodeError:
        pass
    m = _JSON_RE.search(text)
    if not m:
        return None
    candidate = m.group(0)
    # progressively trim trailing junk after the last closing brace
    for end in range(len(candidate), 0, -1):
        if candidate[end - 1] != "}":
            continue
        try:
            v = json.loads(candidate[:end])
            return v if isinstance(v, dict) else None
        except json.JSONDecodeError:
            continue
    return None


# One shared client per process.
_shared: TinyLlamaClient | None = None


def get_client() -> TinyLlamaClient:
    global _shared
    if _shared is None:
        _shared = TinyLlamaClient()
    return _shared

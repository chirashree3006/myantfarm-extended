"""Central configuration -- everything is overridable with environment
variables so the same code runs natively on Windows, in Docker Compose,
and on a cloud VM."""

import os


def _list(name: str, default: str) -> list[str]:
    return [x.strip().rstrip("/") for x in os.getenv(name, default).split(",") if x.strip()]


# Demo-website replicas the agents scrape directly (one URL per replica).
DEMO_TARGETS = _list("DEMO_TARGETS", "http://localhost:8080")

# The load-balanced public entry point of the website (traffic goes here).
DEMO_LB_URL = os.getenv("DEMO_LB_URL", DEMO_TARGETS[0]).rstrip("/")

# One or more Ollama backends. Several URLs => client-side load balancing
# with failover. In Docker this is a single URL pointing at the Nginx
# LLM load balancer, which spreads calls over ollama1/ollama2.
OLLAMA_URLS = _list("OLLAMA_URLS", os.getenv("OLLAMA_URL", "http://localhost:11434"))

MODEL = os.getenv("MODEL", "tinyllama")
LLM_TIMEOUT_S = float(os.getenv("LLM_TIMEOUT_S", "180"))
LLM_MAX_CONCURRENCY = int(os.getenv("LLM_MAX_CONCURRENCY", "4"))
LLM_RETRIES = int(os.getenv("LLM_RETRIES", "2"))
LLM_SEED = int(os.getenv("LLM_SEED", "42"))
# Paper settings: temperature 0.7 with fixed seed 42 (deterministic per prompt).
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.7"))
LLM_NUM_PREDICT = int(os.getenv("LLM_NUM_PREDICT", "200"))
# Time budgets for the multi-agent LLM calls. If the shared model is
# overloaded the agent/coordinator returns its rule-based result on time
# instead of making the incident wait (graceful degradation). 0 = no budget.
AGENT_LLM_BUDGET_S = float(os.getenv("AGENT_LLM_BUDGET_S", "25"))
SYNTH_LLM_BUDGET_S = float(os.getenv("SYNTH_LLM_BUDGET_S", "15"))
# Set LLM_DISABLED=1 to run the whole pipeline without a model
# (rule-based only) -- handy for tests or a very weak machine.
LLM_DISABLED = os.getenv("LLM_DISABLED", "0") == "1"

# Agent endpoints the coordinator calls. In Docker these go through the
# internal Nginx agent load balancer.
AGENT_URLS = {
    "frontend": os.getenv("AGENT_FRONTEND_URL", "http://localhost:9001"),
    "server": os.getenv("AGENT_SERVER_URL", "http://localhost:9002"),
    "database": os.getenv("AGENT_DATABASE_URL", "http://localhost:9003"),
    "users": os.getenv("AGENT_USERS_URL", "http://localhost:9004"),
}

INSTANCE_ID = os.getenv("INSTANCE_ID", "local")

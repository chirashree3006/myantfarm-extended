# MyAntFarm-Extended

**A reproduction and extension of arXiv:2511.15755: multi-agent LLM incident response with
TinyLlama, running against a real, load-balanced online shop and deployed to the cloud.**

The paper claims that, with the same 1.1B model, a team of specialised agents gives actionable
incident advice in 100% of trials, against 1.7% for a single-agent copilot. This project:

1. **Reproduces the paper exactly.** It implements conditions C1, C2 and C3, the paper's
   scenario, its DQ rubric (validity, specificity, correctness), T2U, and its statistics (ANOVA,
   Bonferroni t-tests, Cohen's d). See [`docs/PAPER_REPRODUCTION.md`](docs/PAPER_REPRODUCTION.md).
2. **Runs it on a live system.** *Side B* is a vinyl-drop shop with email and Google sign-in, a
   cart and checkout, running as 3 replicas behind Nginx. The paper's incident (an auth-service
   regression) and two others can be injected while shopper traffic flows.
3. **Extends the multi-agent design (C3x).** Four monitoring agents run in parallel, evidence is
   fused, and the action plan is split into per-team tickets.
4. **Tests the cloud-architecture claims.** It measures concurrent incidents against an SLA,
   crashes components (chaos), checks load-balancer failover, and checks graceful degradation
   when the model is overloaded or down.

## Architecture

```
  shoppers ──► :80 /            Side B shop (static)   ─┐
  on-call  ──► :80 /ops/        ops console             │
                                                        ▼
             ┌────────────────────── gateway (Nginx) ──────────────────────────┐
             │ /shop-api ─► web_pool   least_conn, retry on crash (503/error)   │
             │ /api/multi ─► coord1, coord2      /api/single ─► single-agent    │
             │ :8090 agent tier (per-role pools)  :11434 LLM tier (2 replicas) │
             │ /chaos/<service>/down|up  fault injection                        │
             └─────────────────────────────────────────────────────────────────┘
  web1 web2 web3   Side B replicas: users/products/orders in shared SQLite, JWT sessions,
                   seeded faults: auth_regression (paper) · leak · slow_db
  coord1 coord2    C3x: 4 agents in parallel → fusion → plan → team tickets
                   C3 : Diagnosis → Planner → Risk (paper pipeline)
  agent-frontend · agent-server · agent-database · agent-users   (one stream each, every replica)
  single-agent     C2 copilot (one call, one instance)
  ollama1 ollama2  TinyLlama 1.1B, temperature 0.7, seed 42
```

## Quick start (Windows, Docker Desktop)

```powershell
cd C:\Users\HP\Documents\myantfarm-extended
copy .env.example .env          # optional: add GOOGLE_CLIENT_ID (see deploy/DEPLOY_GUIDE.md, section E)
powershell -ExecutionPolicy Bypass -File scripts\start.ps1        # add -Lite on an 8 GB laptop
```

* **Shop:** http://localhost. Sign in with *Use the demo account* (demo@sideb.store / dropday2026),
  create an account, or use Google. Add records and check out.
* **Ops console:** http://localhost/ops/. Inject **Auth regression (paper)**, then **Send traffic**,
  then **C2 vs C3x side by side**. The console also shows team tickets, the load test and chaos buttons.

Things to try on the shop while a fault is on: with *Connection leak*, checkouts fail with a clear
message and nothing is charged. With *Auth regression*, about 45% of sign-ins fail. Crash `web2`
on the ops console and keep shopping: the load balancer routes around it.

### Traffic-surge demo (backend on-call console)

Open **http://localhost/backend/** next to the shop.

1. **Reset everything → Start flash-sale surge.** 150 simulated shoppers hit the shop through the
   load balancer. Each replica handles 12 requests at a time, so the shop crashes: sign-in in the shop
   tab shows **"Side B is down"**. The pager fires when more than 20% of requests fail.
2. **Single agent → Respond.** One TinyLlama call (C2) gives generic advice. You then do 6 manual steps
   (read logs, find the cause, scale out, raise capacity, waiting room, notify support).
3. **Reset → Start surge → Multi-agent → Respond.** Four monitor agents run in parallel. The coordinator
   diagnoses `traffic_surge` and proposes a plan. **Approve and run the plan** (1 click) adds standby
   replica web4 to the pool, raises capacity 12→40 and turns on the waiting room. Each team gets its own ticket.
4. The **Scoreboard** compares time to diagnosis, time to recovery, human actions, failed shopper
   requests and DQ. The % gains are measured on that run, so they change from run to run.

Remediation API: `POST /api/multi/control/surge {"seconds":120,"concurrency":150}`,
`POST /api/multi/incident/execute {"action":"scale_out"|"raise_capacity"|"waiting_room"}`,
`GET /api/multi/control/live`.

## Free cloud hosting (no card)

Open the repo in **GitHub Codespaces** (Code → Codespaces → 4-core machine). The `.devcontainer/` starts everything; then make port 80 public. Full steps are in `deploy/DEPLOY_GUIDE.md`, section G.

## Experiments (run from the laptop against local or cloud)

```powershell
cd loadtest; pip install -r requirements.txt
python benchmark.py                       # paper reproduction: C1/C2/C3/C3x x 4 scenarios, 10 trials
python benchmark.py --paper-full          # 116 trials per condition, as in the paper (slow on CPU)
python resilience.py --levels 1 4 8 16 --sla 60     # load + chaos: C2 vs C3x
python resilience.py --docker             # + LLM replica / LLM tier outage (run on the Docker host)
python traffic.py -n 1000 -c 50           # raw website load-balancer test
```

Each writes a Markdown report, a chart, and CSV/JSON files to `loadtest/results/`. Put the
Markdown tables into your report.

| Experiment | Question it answers |
|---|---|
| `benchmark.py`, Tables I–III and stats | Does the paper's result reproduce (DQ, actionability, T2U, variance, significance)? Does it hold on live incidents? |
| `resilience.py`, part A | When incidents pile up, which design still returns a usable answer within the SLA? |
| `resilience.py`, part B | What happens when a coordinator, an agent, a website replica or the LLM dies? |
| team tickets | How is the work split between Backend, Database, Frontend, Support and SRE? |

### Why the multi-agent system holds up under load and failure
* **No single point of failure.** C2 is one service making one call. C3x runs 2 coordinator
  replicas behind least-conn load balancing, and Nginx retries a crashed replica's request on the
  other one.
* **Agents fall back.** If an agent service is down, the coordinator runs that agent in-process.
* **Graceful degradation.** Each agent's LLM call has a time budget (`AGENT_LLM_BUDGET_S`). When
  the shared model is saturated or down, the agent returns its rule-based finding on time. The
  diagnosis and plan still arrive, marked as degraded. C2 has nothing to fall back to.
* **Work is split.** Each team gets only its own evidence and actions, so the backend team isn't
  reading frontend logs and support gets a ready status-page message.

## Project layout

```
demo-website/app.py      Side B API: auth (email + Google), products, orders, faults, chaos, 4 streams
gateway/www/index.html   Side B shop UI          gateway/www/ops/index.html  ops console
gateway/www/backend/     backend on-call console (traffic-surge demo, single vs multi-agent)
gateway/nginx.conf       all load balancers + chaos routing
multi-agent/antfarm/     llm.py (TinyLlama client: LB, failover, budgets, JSON mode, metrics)
                         agents.py · fusion.py (C3x) · paper.py (C1, C3) · baseline.py (C2)
                         scoring.py (paper DQ) · stats.py · streams.py · traffic.py · chaos.py
multi-agent/services/    agent_service · coordinator_service · single_agent_service
multi-agent/multi_agent.py   no-Docker CLI
loadtest/                benchmark.py · resilience.py · traffic.py → results/
deploy/                  setup-vm.sh · DEPLOY_GUIDE.md        docs/PAPER_REPRODUCTION.md
docs/phase1/             Problem_Statement.docx · Demo_Walkthrough.docx · original-code/ (the 50% submission)
scripts/                 start.ps1 · demo.ps1     tests/  unit tests + mock Ollama
```

Tests: `python -m pytest tests -q`. To run the whole stack without a model, use `tests/mock_ollama.py`
or set `LLM_DISABLED=1`.

## Limitations (state these in the report)
* The paper doesn't publish its prompts. The C3 prompts follow its role descriptions.
* Absolute T2U depends on the CPU. Compare conditions on the same machine.
* C3x's quality comes from decomposition plus explicit fusion. TinyLlama alone cannot synthesise
  across agents, which was the phase-1 finding.
* The faults are simulated inside the app. The traffic, load balancing, failures and model calls are real.

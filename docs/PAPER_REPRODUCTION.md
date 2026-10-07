# How this project tests the paper

**Paper:** *Multi-Agent LLM Orchestration Achieves Deterministic, High-Quality Decision Support
for Incident Response* (arXiv:2511.15755, MyAntFarm.ai).

**Claim:** with the same small model (TinyLlama 1.1B), a coordinator orchestrating specialised
agents produces actionable incident recommendations far more often than a single-agent copilot:
100% vs 1.7% of trials actionable, specificity 80× higher, correctness 140× higher, zero variance,
at about the same time to usable understanding (~40 s).

## Every element of the paper and where it lives

| Paper element | Paper definition | Implementation |
|---|---|---|
| **C1** baseline | Simulated manual dashboard analysis, T2U ~ N(120 s, 6.5 s), no actions (DQ 0) | `antfarm/paper.py: simulate_c1`, used in `loadtest/benchmark.py` |
| **C2** single agent | FastAPI copilot, one LLM call over the incident telemetry | `services/single_agent_service.py` + `antfarm/baseline.py` (same prompt as phase 1) |
| **C3** multi-agent | Non-LLM coordinator; Diagnosis → Remediation Planner → Risk Assessor, each receiving the previous output | `antfarm/paper.py: run_c3_paper`, exposed as `POST /api/multi/incident/analyze {"mode":"paper"}` |
| Scenario | "Authentication service regression post deployment", 45% login error rate, DB connections 85% | `paper_static` (fixed text, `antfarm/paper.py: PAPER_INCIDENT`) **and** `auth_regression`, the same incident running live on the shop (`demo-website/app.py`) |
| Ground truth | "rollback auth-service deployment to v2.3.0 verify database connection pool" | `antfarm/scoring.py: GROUND_TRUTH` |
| **T2U** | Incident onset to first coherent summary, from API timestamps | request → response time of each analysis (`benchmark.py`) |
| **DQ** | 0.40·Validity + 0.30·Specificity + 0.30·Correctness | `antfarm/scoring.py` |
| Validity | Ratio of technically feasible actions | per action: no impossible values (>100%, negative counts), no contradictions |
| Specificity | Regex: versions `v?\d+.\d+(.\d+)?`, commands `kubectl\|docker\|systemctl\|aws\|gcloud`, service names. 1.0 / 0.67 / 0.33 / 0 | identical tiers, averaged over actions |
| Correctness | Token overlap with ground truth: ≥70% → 1.0, 50–69% → 0.75, 30–49% → 0.5, 10–29% → 0.25 | identical tiers, averaged over actions (this reproduces the paper's C3 value 0.417 = (0.75+0.25+0.25)/3) |
| Actionability | DQ > 0.5 | identical |
| Statistics | One-way ANOVA; pairwise t-tests, Bonferroni α = 0.05/3; Cohen's d | `antfarm/stats.py` (dependency-free, p-values checked against SciPy) |
| Trials | 116 per condition | `python benchmark.py --paper-full` (default 10 per condition for a quick run) |
| Model settings | TinyLlama 1.1B via Ollama, temperature 0.7, seed 42 | `LLM_TEMPERATURE=0.7`, `LLM_SEED=42` (defaults) |
| Deployment | Docker Compose microservices | `docker-compose.yml`, plus Nginx load balancing and a cloud VM |

## What this project adds beyond the paper

1. **C3x, an extended multi-agent design.** Four monitoring agents (frontend, server, database,
   user reports) each read one live stream from every replica, in parallel. A fusion coordinator
   scores competing hypotheses, so independent agents corroborate each other. TinyLlama does
   narrow tasks only: one observation per agent and the final summary.
2. **Live incidents instead of fixed text.** The paper's scenario runs live on a real shop with
   sign-ins and checkouts (`auth_regression`). Two more faults (`leak`, `slow_db`) check that the
   result holds when the root cause changes.
3. **Root-cause correctness.** Token overlap can reward the right words used for the wrong
   reason, so every brief is also checked against the injected fault.
4. **False-alarm check.** Healthy runs confirm that C3x reports no incident when nothing is wrong.
5. **Cloud architecture experiments** (`loadtest/resilience.py`). These test the operational
   claims: concurrent incidents against an SLA, crashed components, website replica failover, and
   LLM outages.
6. **Work split by team.** Each C3x action has an owner. Every team gets a ticket with only its
   own evidence and actions, and teams with nothing to do aren't paged.

## Known deviations (put these in the report)

* The paper doesn't print its prompts. The C3 role prompts follow the roles as the paper
  describes them.
* `PAPER_INCIDENT` reconstructs the paper's telemetry from its description: service, version,
  45% error rate, 85% DB capacity.
* T2U depends on hardware. The paper's ~40 s came from CPU inference on a 16 GB machine.
  Compare conditions on the same machine.
* With C3x the gain comes from decomposition plus explicit fusion. Phase 1 showed that TinyLlama
  alone cannot synthesise across agents.

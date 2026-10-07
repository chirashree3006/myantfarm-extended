"""
Decision Quality (DQ) exactly as defined in the reference paper
(arXiv:2511.15755, Section "Primary Metrics"):

    DQ_i = 0.40 * V_i + 0.30 * S_i + 0.30 * R_i      actionable  <=>  DQ > 0.5

  V  validity     ratio of technically feasible actions (no impossible
                  values such as >100% or negative counts, no contradictions)
  S  specificity  per action: 1.0 specific identifiers (version numbers
                  v?\\d+.\\d+(.\\d+)? or commands kubectl|docker|systemctl|aws|gcloud),
                  0.67 service names without versions, 0.33 generic
                  categories, 0.0 vague directives
  R  correctness  per action: token overlap with the ground-truth action
                  |tokens(action) ∩ tokens(gt)| / |tokens(gt)| mapped to
                  1.0 (>=70%), 0.75 (50-69%), 0.50 (30-49%), 0.25 (10-29%), 0 (<10%)

S and R are averaged over the extracted actions (that reproduces the
paper's C3 values, e.g. R = (0.75+0.25+0.25)/3 = 0.417). An output with
no actions scores 0 on all three (the paper's C1 convention).

Extension (not in the paper): `root_cause_correct` checks the named cause
against the injected fault, because token overlap alone can reward an
answer that mentions the right words for the wrong reason.
"""

from __future__ import annotations

import re

WEIGHTS = {"validity": 0.40, "specificity": 0.30, "correctness": 0.30}
ACTIONABLE_THRESHOLD = 0.5

GROUND_TRUTH = {
    # the paper's own scenario (static text and our live replica of it)
    "paper_static": "rollback auth-service deployment to v2.3.0 verify database connection pool",
    "auth_regression": "rollback auth-service deployment to v2.3.0 verify database connection pool",
    # our additional live scenarios, written in the same style
    "leak": "rollback checkout-service deployment to v2.2.4 restart replicas release database connection pool",
    "slow_db": "create index on orders user_id verify slow database query explain analyze",
}

ROOT_CAUSE = {
    "paper_static": ([r"auth", r"login", r"deploy|regression|v2\.3\.1"], [r"\bleak\b", r"missing index"]),
    "auth_regression": ([r"auth", r"login", r"deploy|regression|v2\.3\.1"], [r"\bleak\b", r"missing index"]),
    "leak": ([r"leak", r"(not|never) (being )?(released|returned)", r"pool (is )?exhaust"],
             [r"slow quer", r"missing index", r"auth"]),
    "slow_db": ([r"slow (sql|quer|database)", r"missing index|no index|full table scan"],
                [r"\bleak\b", r"pool (is )?exhaust"]),
}

VERSION_RE = re.compile(r"\bv?\d+\.\d+(\.\d+)?\b", re.I)
COMMAND_RE = re.compile(r"\b(kubectl|docker|systemctl|aws|gcloud)\b", re.I)
SERVICE_RE = re.compile(
    r"\b(auth-service|checkout-service|orders?(\.user_id)?|user_id|nginx|ollama|postgres|mysql|redis|"
    r"web\d|coord\d|connection pool|/api/\w+|/auth/\w+|/login|/checkout|idx_\w+)\b", re.I)
GENERIC_RE = re.compile(
    r"\b(database|server|service|logs?|system|application|network|configuration|deploy(ment)?|code|"
    r"infrastructure|monitoring|backend|frontend|cache|memory|cpu|traffic|load)\b", re.I)
ACTION_VERBS = re.compile(
    r"\b(roll ?back|revert|restart|recycle|reboot|verify|check|investigate|monitor|review|increase|"
    r"decrease|scale|add|create|run|deploy|redeploy|fix|patch|release|close|drain|disable|enable|update|"
    r"upgrade|downgrade|contact|notify|escalate|analy[sz]e|inspect|look into|optimi[sz]e|clear|flush|"
    r"kill|stop|start|reset|configure|set|raise|lower|apply|test|ensure|implement|communicate|post)\b", re.I)
IMPOSSIBLE = [re.compile(r"\b(1[0-9]{2,}|[2-9]\d{2,})(\.\d+)?\s?%"), re.compile(r"(?<![\w.])-\d+\s*(connections|requests|%)")]


def tokens(text: str) -> set[str]:
    return {t.strip(".-") for t in re.findall(r"[a-z0-9][a-z0-9.\-_]*", text.lower()) if t.strip(".-")}


def extract_actions(text: str, max_actions: int | None = None) -> list[str]:
    """Numbered / bulleted lines first; otherwise sentences that contain an action verb."""
    if not text:
        return []
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    listed = [re.sub(r"^\s*(\d+[.)]|[-*•])\s*", "", l) for l in lines if re.match(r"^\s*(\d+[.)]|[-*•])\s+", l)]
    if listed:
        acts = [a for a in listed if ACTION_VERBS.search(a)] or listed
    else:
        sents = re.split(r"(?<=[.!?])\s+", " ".join(lines))
        acts = [s.strip() for s in sents if ACTION_VERBS.search(s)]
    return acts[:max_actions] if max_actions else acts


def _spec(action: str) -> float:
    if VERSION_RE.search(action) or COMMAND_RE.search(action):
        return 1.0
    if SERVICE_RE.search(action):
        return 0.67
    if GENERIC_RE.search(action):
        return 0.33
    return 0.0


def _corr(action: str, gt: set[str]) -> float:
    overlap = len(tokens(action) & gt) / len(gt)
    if overlap >= 0.70:
        return 1.0
    if overlap >= 0.50:
        return 0.75
    if overlap >= 0.30:
        return 0.50
    if overlap >= 0.10:
        return 0.25
    return 0.0


def _valid(action: str, all_text: str) -> float:
    if any(p.search(action) for p in IMPOSSIBLE):
        return 0.0
    low = all_text.lower()
    for verb in ("roll back", "rollback", "restart"):
        if verb in action.lower() and (f"do not {verb}" in low or f"don't {verb}" in low):
            return 0.0
    return 1.0


def score_actions(actions: list[str], scenario: str, full_text: str = "") -> dict:
    gt = tokens(GROUND_TRUTH[scenario])
    n = len(actions)
    if n == 0:
        V = S = R = 0.0
    else:
        V = sum(_valid(a, full_text or " ".join(actions)) for a in actions) / n
        S = sum(_spec(a) for a in actions) / n
        R = sum(_corr(a, gt) for a in actions) / n
    dq = WEIGHTS["validity"] * V + WEIGHTS["specificity"] * S + WEIGHTS["correctness"] * R
    good, bad = ROOT_CAUSE[scenario]
    txt = full_text or " ".join(actions)
    rc = any(re.search(p, txt, re.I) for p in good) and not any(re.search(p, txt, re.I) for p in bad)
    return {"dq": round(dq, 3), "validity": round(V, 3), "specificity": round(S, 3),
            "correctness": round(R, 3), "actions": n, "actionable": dq > ACTIONABLE_THRESHOLD,
            "root_cause_correct": rc}


def score_text(text: str, scenario: str, max_actions: int | None = None) -> dict:
    acts = extract_actions(text, max_actions)
    out = score_actions(acts, scenario, text)
    out["extracted_actions"] = acts
    return out

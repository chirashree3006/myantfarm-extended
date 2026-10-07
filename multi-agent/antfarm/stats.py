"""The paper's statistical tests, dependency-free:
one-way ANOVA, pairwise t-tests with Bonferroni correction, Cohen's d.
p-values use the regularised incomplete beta function (Numerical Recipes)."""

from __future__ import annotations

import math
import statistics as st


def _betacf(a, b, x, it=300, eps=3e-14):
    qab, qap, qam = a + b, a + 1, a - 1
    c, d = 1.0, 1 - qab * x / qap
    d = 1 / (d if abs(d) > 1e-300 else 1e-300)
    h = d
    for m in range(1, it + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1 + aa * d
        d = 1 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1 + aa / c if abs(1 + aa / c) > 1e-300 else 1e-300
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1 + aa * d
        d = 1 / (d if abs(d) > 1e-300 else 1e-300)
        c = 1 + aa / c if abs(1 + aa / c) > 1e-300 else 1e-300
        de = d * c
        h *= de
        if abs(de - 1) < eps:
            break
    return h


def betai(a, b, x):
    if x <= 0:
        return 0.0
    if x >= 1:
        return 1.0
    bt = math.exp(math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b) + a * math.log(x) + b * math.log(1 - x))
    if x < (a + 1) / (a + b + 2):
        return bt * _betacf(a, b, x) / a
    return 1 - bt * _betacf(b, a, 1 - x) / b


def f_sf(F, d1, d2):
    """P(F_{d1,d2} > F)"""
    if F <= 0 or math.isnan(F):
        return 1.0
    if math.isinf(F):
        return 0.0
    return betai(d2 / 2, d1 / 2, d2 / (d2 + d1 * F))


def t_two_sided(t, df):
    if math.isinf(t):
        return 0.0
    return betai(df / 2, 0.5, df / (df + t * t))


def anova(groups: dict[str, list[float]]) -> dict:
    gs = [g for g in groups.values() if len(g) > 1]
    k, n = len(gs), sum(len(g) for g in gs)
    if k < 2:
        return {}
    grand = sum(sum(g) for g in gs) / n
    ssb = sum(len(g) * (st.mean(g) - grand) ** 2 for g in gs)
    ssw = sum(sum((x - st.mean(g)) ** 2 for x in g) for g in gs)
    d1, d2 = k - 1, n - k
    F = (ssb / d1) / (ssw / d2) if ssw > 0 else (math.inf if ssb > 0 else 0.0)
    return {"F": round(F, 2) if not math.isinf(F) else "inf", "df": [d1, d2], "p": f_sf(F, d1, d2)}


def welch_t(a: list[float], b: list[float]) -> dict:
    ma, mb = st.mean(a), st.mean(b)
    va, vb = st.variance(a) if len(a) > 1 else 0.0, st.variance(b) if len(b) > 1 else 0.0
    se2 = va / len(a) + vb / len(b)
    if se2 == 0:
        t, df = (math.inf if ma != mb else 0.0), len(a) + len(b) - 2
    else:
        t = (ma - mb) / math.sqrt(se2)
        df = se2 ** 2 / ((va / len(a)) ** 2 / max(1, len(a) - 1) + (vb / len(b)) ** 2 / max(1, len(b) - 1))
    p = t_two_sided(abs(t), df) if t != 0 else 1.0
    return {"t": round(t, 2) if not math.isinf(t) else "inf", "df": round(df, 1), "p": p}


def cohens_d(a: list[float], b: list[float]) -> float | str:
    va, vb = st.variance(a) if len(a) > 1 else 0.0, st.variance(b) if len(b) > 1 else 0.0
    sp = math.sqrt(((len(a) - 1) * va + (len(b) - 1) * vb) / max(1, len(a) + len(b) - 2))
    diff = st.mean(a) - st.mean(b)
    if sp == 0:
        return "inf" if diff else 0.0
    return round(diff / sp, 2)


def compare(groups: dict[str, list[float]], alpha: float = 0.05) -> dict:
    names = [k for k, v in groups.items() if len(v) > 1]
    pairs = [(a, b) for i, a in enumerate(names) for b in names[i + 1:]]
    corrected = alpha / max(1, len(pairs))
    out = {"anova": anova({k: groups[k] for k in names}), "bonferroni_alpha": corrected, "pairs": []}
    for a, b in pairs:
        t = welch_t(groups[a], groups[b])
        out["pairs"].append({"a": a, "b": b, **t, "cohens_d": cohens_d(groups[a], groups[b]),
                             "significant": t["p"] < corrected})
    return out


def fmt_p(p: float) -> str:
    return "<0.001" if p < 0.001 else f"{p:.3f}"

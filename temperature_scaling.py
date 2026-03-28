"""
temperature_scaling.py — Phase 46
==================================
Platt/temperature scaling to fix overconfident Bayesian probabilities.

The walk-forward backtest (Phase 45) showed:
  - 62%+ confidence tier actually hits at 42.9%  → +22.5% overconfidence gap
  - 56-61% confidence tier actually hits at 52.4% → +6.4% overconfidence gap

Temperature scaling fits a single scalar T on a VALIDATION set (never test set)
then divides logits by T before the final sigmoid:

    calibrated_prob = sigmoid(logit(raw_prob) / T)

T > 1 compresses probabilities toward 0.5 (fixes overconfidence).
T < 1 spreads probabilities away from 0.5 (fixes underconfidence).

Key constraint: T is fit on val fold only. Never see test fold during fitting.
"""

import math
from typing import List, Tuple


def _logit(p: float) -> float:
    """Safe logit — clamp to avoid log(0)."""
    p = max(1e-7, min(1 - 1e-7, p))
    return math.log(p / (1 - p))


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def _log_loss(probs: List[float], outcomes: List[int]) -> float:
    """Binary cross-entropy."""
    total = 0.0
    for p, y in zip(probs, outcomes):
        p = max(1e-7, min(1 - 1e-7, p))
        total += -(y * math.log(p) + (1 - y) * math.log(1 - p))
    return total / len(probs)


def fit_temperature(
    raw_probs: List[float],
    outcomes: List[int],
    t_search_range: Tuple[float, float] = (0.5, 3.0),
    steps: int = 100
) -> float:
    """
    Grid search for optimal temperature T on validation set.

    Args:
        raw_probs: Model raw probabilities (before calibration)
        outcomes:  1 = hit, 0 = miss
        t_search_range: (min_T, max_T) for grid search
        steps: Number of T values to try

    Returns:
        Optimal T (float). T > 1 = was overconfident. T < 1 = was underconfident.
    """
    if len(raw_probs) < 10:
        # Not enough validation data — return neutral T
        return 1.0

    best_t = 1.0
    best_loss = float("inf")
    t_min, t_max = t_search_range

    for i in range(steps + 1):
        T = t_min + (t_max - t_min) * (i / steps)
        calibrated = [_sigmoid(_logit(p) / T) for p in raw_probs]
        loss = _log_loss(calibrated, outcomes)
        if loss < best_loss:
            best_loss = loss
            best_t = T

    return best_t


def apply_temperature(raw_prob: float, T: float) -> float:
    """
    Apply temperature scaling to a single probability.

    Args:
        raw_prob: Raw model probability (0-1)
        T: Temperature scalar (fit on validation set)

    Returns:
        Calibrated probability (0-1)
    """
    if T == 1.0:
        return raw_prob
    return _sigmoid(_logit(raw_prob) / T)


def apply_temperature_batch(raw_probs: List[float], T: float) -> List[float]:
    """Apply temperature scaling to a list of probabilities."""
    return [apply_temperature(p, T) for p in raw_probs]


def calibration_report(
    probs: List[float],
    outcomes: List[int],
    n_bins: int = 5,
    label: str = ""
) -> dict:
    """
    Compute reliability curve metrics.

    Returns dict with:
        bins: list of {confidence_mid, predicted_rate, actual_rate, n, gap}
        ece: Expected Calibration Error (lower = better, 0.0 = perfect)
        brier: Brier score
    """
    n = len(probs)
    if n == 0:
        return {"bins": [], "ece": None, "brier": None}

    # Brier
    brier = sum((p - y) ** 2 for p, y in zip(probs, outcomes)) / n

    # ECE
    bin_edges = [i / n_bins for i in range(n_bins + 1)]
    bins = []
    ece = 0.0

    for i in range(n_bins):
        lo, hi = bin_edges[i], bin_edges[i + 1]
        mask = [(lo <= p < hi) for p in probs]
        # include hi in last bin
        if i == n_bins - 1:
            mask = [(lo <= p <= hi) for p in probs]

        bucket_probs = [p for p, m in zip(probs, mask) if m]
        bucket_outcomes = [y for y, m in zip(outcomes, mask) if m]

        if not bucket_probs:
            continue

        pred_rate = sum(bucket_probs) / len(bucket_probs)
        actual_rate = sum(bucket_outcomes) / len(bucket_outcomes)
        gap = pred_rate - actual_rate
        bins.append({
            "confidence_mid": round((lo + hi) / 2, 2),
            "predicted_rate": round(pred_rate, 3),
            "actual_rate": round(actual_rate, 3),
            "n": len(bucket_probs),
            "gap": round(gap, 3)
        })
        ece += abs(gap) * len(bucket_probs) / n

    return {
        "label": label,
        "n": n,
        "bins": bins,
        "ece": round(ece, 4),
        "brier": round(brier, 4)
    }


def print_calibration_report(report: dict) -> None:
    """Pretty-print a calibration report."""
    label = report.get("label", "")
    print(f"\n── Calibration Report {label} ──")
    print(f"   n={report['n']}  ECE={report['ece']}  Brier={report['brier']}")
    print(f"   {'Bin':>6}  {'Predicted':>10}  {'Actual':>10}  {'n':>5}  {'Gap':>8}")
    for b in report["bins"]:
        flag = "⚠ OVER" if b["gap"] > 0.05 else ("⚠ UNDER" if b["gap"] < -0.05 else "OK")
        print(f"   {b['confidence_mid']:>6.2f}  {b['predicted_rate']:>10.3f}  "
              f"{b['actual_rate']:>10.3f}  {b['n']:>5}  {b['gap']:>+8.3f}  {flag}")


# ─── Integration point for live_dispatcher.py ───────────────────────────────
# Usage pattern inside dispatcher:
#
# from temperature_scaling import apply_temperature
#
# # T is stored per-agent in agent_unit_sizing table after each val fold refit
# # Default T=1.0 until first val fold accumulates ≥30 graded picks
# T = agent_temperature.get(agent_name, 1.0)
# calibrated_prob = apply_temperature(leg.implied_prob, T)
#
# The settlement process (nightly_recap.py) should:
# 1. After accumulating 30+ graded picks for an agent on val window
# 2. Call fit_temperature(val_probs, val_outcomes) → new T
# 3. Store T in agent_unit_sizing table (add `temperature` column)
# 4. Dispatcher reads T at dispatch time
# ─────────────────────────────────────────────────────────────────────────────

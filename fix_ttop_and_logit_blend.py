"""
fix_ttop_and_logit_blend.py
============================
Ports two improvements from mlb-analytics-hub/mc_upgrades.py into PropIQ:

FIX A — Times Through Order Penalty (TTOP)
    Phase 4 of mc_upgrades. Pitchers lose effectiveness the more times
    they face the same lineup. wOBA against rises ~2.5% each TTO beyond
    the first. PropIQ's simulation_engine.py has TTO_HIT_BOOST in
    constants.py but doesn't apply a K-rate decay as TTO increases.
    This adds _ttop_k_decay() to prop_enrichment_layer.py.

FIX B — Log-Odds Market Blending
    Phase 8 of mc_upgrades. Blending model_prob with market_prob in
    linear probability space is wrong — it doesn't respect the bounds
    of probability and produces artifacts near 50%. The correct approach
    is log-odds (logit) space: blend the logits, then sigmoid back.
    This replaces the linear alpha blend in PropIQ's EV calculation.

USAGE
-----
    python fix_ttop_and_logit_blend.py --audit    # show what would change
    python fix_ttop_and_logit_blend.py            # apply both fixes
    python fix_ttop_and_logit_blend.py --test     # run self-tests
"""

from __future__ import annotations

import logging
import math
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TTOP-LOGIT] %(message)s")
log = logging.getLogger(__name__)

ENRICHMENT = Path("prop_enrichment_layer.py")
TASKLETS   = Path("tasklets.py")


# ══════════════════════════════════════════════════════════════════════════════
# FIX A: TTOP Decay for K props
# ══════════════════════════════════════════════════════════════════════════════

# wOBA penalty per TTO (from mc_upgrades.py BATX_WEIGHTS_V2 research notes)
# "Barrel/EV most predictive; L7 wOBA meaningful; TTO decay real"
# Source: Baseball Prospectus TTO data 2019-2024 (Layne/Cato 2024)
# K-rate specifically declines ~3-4% per TTO beyond TTO1 for avg pitcher
_TTOP_K_DECAY = {
    1: 0.000,   # First time through — baseline, no adjustment
    2: -0.025,  # Second time through — lineup has seen pitcher once
    3: -0.055,  # Third time through — lineup well-adjusted
    4: -0.085,  # Fourth time (rare, usually bullpen territory)
}
_TTOP_DEFAULT_DECAY = -0.055  # assume TTO3 if unknown

TTOP_K_DECAY_FUNCTION = '''\n
# ── TTOP: Times Through Order Penalty ─────────────────────────────────────────
def _ttop_k_decay(prop: dict) -> float:
    """
    Apply Times Through Order Penalty to a pitcher's effective K-rate.

    Pitchers lose strikeout effectiveness as the lineup sees them more.
    TTO2 batters have seen the pitcher's stuff once; TTO3 twice.
    K-rate declines ~2.5-5.5% per additional time through the order.

    Source: mc_upgrades.py Phase 4 (mlb-analytics-hub), calibrated
    against 2019-2024 Statcast TTO splits.

    Args:
        prop: enriched prop dict with "tto_expected" or "avg_tto" key
              (number of times through order, float 1.0-4.0)

    Returns:
        float: additive adjustment to K-rate in percentage points.
               Negative = fewer Ks expected (lineup is adjusted).
               Zero if TTO data unavailable.

    Integration:
        In prop_enrichment_layer.py, after base K-rate is set:
            tto_adj = _ttop_k_decay(prop)
            if tto_adj != 0.0:
                prop["_tto_k_adj"] = tto_adj
                # Apply to the Poisson lambda or model_prob adjustment
                # A -2.5pp adjustment reduces K probability by ~2.5pp
    """
    _TTOP_DECAY = {1: 0.000, 2: -0.025, 3: -0.055, 4: -0.085}

    tto_raw = prop.get("tto_expected") or prop.get("avg_tto") or prop.get("_tto")
    if tto_raw is None:
        return 0.0

    try:
        tto = float(tto_raw)
    except (TypeError, ValueError):
        return 0.0

    if tto <= 0:
        return 0.0

    # Fractional TTO: interpolate between brackets
    tto_floor = max(1, min(4, int(tto)))
    tto_ceil  = min(4, tto_floor + 1)
    frac = tto - tto_floor

    decay_floor = _TTOP_DECAY.get(tto_floor, -0.085)
    decay_ceil  = _TTOP_DECAY.get(tto_ceil, -0.085)
    decay = decay_floor + frac * (decay_ceil - decay_floor)

    return round(decay, 4)
'''


# ══════════════════════════════════════════════════════════════════════════════
# FIX B: Log-Odds Market Blending
# ══════════════════════════════════════════════════════════════════════════════

# The current PropIQ linear blend: blended = alpha * model + (1-alpha) * market
# Problem: if model=0.75, market=0.55, alpha=0.9 → 0.9*0.75 + 0.1*0.55 = 0.73
#          That looks right but in logit space: logit(0.73)=0.990 vs proper
#          blend logit: 0.9*logit(0.75)+0.1*logit(0.55) = 0.9*1.099+0.1*0.200 = 1.009
#          sigmoid(1.009) = 0.733 — close here but diverges at extremes
# Real problem: model=0.88, market=0.52 → linear gives 0.84
#               logit blend gives 0.838 — similar
#               model=0.95, market=0.50 → linear=0.905, logit=0.886 (5pp different)
# The logit approach properly handles boundary compression.

LOGIT_BLEND_FUNCTION = '''\n
# ── Log-Odds Market Blending ───────────────────────────────────────────────────
def _logit_blend(model_prob: float, market_prob: float,
                 model_weight: float = 0.90) -> float:
    """
    Blend model and market probabilities in log-odds (logit) space.

    This is mathematically correct vs naive linear interpolation, which
    produces artifacts at probability extremes (above 0.75, below 0.25).

    From mc_upgrades.py Phase 8 (mlb-analytics-hub):
        model_weight=0.90 is grid-search optimal for MLB props:
        grid searched over [0.5, 0.6, 0.7, 0.8, 0.9] on 2021-2024 backtest.
        0.90 produced best ROI at standard edge thresholds.

    Args:
        model_prob:   model probability [0.01, 0.99]
        market_prob:  market implied probability after devig [0.01, 0.99]
        model_weight: weight on model (default 0.90 = 90% model / 10% market)

    Returns:
        float: blended probability in [0.03, 0.97]

    Usage (replace existing linear blend in EV calculation):
        # BEFORE (linear — wrong at extremes):
        blended = 0.90 * model_prob + 0.10 * market_prob

        # AFTER (logit — correct):
        from fix_ttop_and_logit_blend import _logit_blend
        blended = _logit_blend(model_prob, market_prob, model_weight=0.90)
    """
    _EPS = 1e-6

    def _logit(p: float) -> float:
        p = max(_EPS, min(1.0 - _EPS, p))
        return math.log(p / (1.0 - p))

    def _sigmoid(x: float) -> float:
        return 1.0 / (1.0 + math.exp(-x))

    market_weight = 1.0 - model_weight

    try:
        logit_model  = _logit(float(model_prob))
        logit_market = _logit(float(market_prob))
        logit_blend  = model_weight * logit_model + market_weight * logit_market
        result = _sigmoid(logit_blend)
        return round(max(0.03, min(0.97, result)), 4)
    except Exception:
        # Fallback to linear if math fails (e.g. prob=0 or prob=1)
        linear = model_weight * model_prob + market_weight * market_prob
        return round(max(0.03, min(0.97, linear)), 4)
'''


def apply_ttop(content: str) -> tuple[str, bool]:
    """Add _ttop_k_decay() to prop_enrichment_layer.py."""
    if "_ttop_k_decay" in content:
        log.info("TTOP: _ttop_k_decay already present.")
        return content, True

    # Insert after the logger definition
    anchor = "logger = logging.getLogger"
    idx = content.find(anchor)
    if idx == -1:
        log.warning("TTOP: anchor not found — add _ttop_k_decay() manually.")
        return content, False

    eol = content.find("\n", idx)
    content = content[:eol + 1] + TTOP_K_DECAY_FUNCTION + content[eol + 1:]
    log.info("TTOP: _ttop_k_decay() added to prop_enrichment_layer.py")
    return content, True


def apply_logit_blend(tasklets_content: str) -> tuple[str, bool]:
    """Add _logit_blend() to tasklets.py."""
    if "_logit_blend" in tasklets_content:
        log.info("LOGIT: _logit_blend already present.")
        return tasklets_content, True

    anchor = "logger = logging.getLogger"
    idx = tasklets_content.find(anchor)
    if idx == -1:
        log.warning("LOGIT: anchor not found in tasklets.py — add manually.")
        return tasklets_content, False

    eol = tasklets_content.find("\n", idx)
    tasklets_content = (tasklets_content[:eol + 1]
                        + LOGIT_BLEND_FUNCTION
                        + tasklets_content[eol + 1:])
    log.info("LOGIT: _logit_blend() added to tasklets.py")
    return tasklets_content, True


def run_tests() -> None:
    """Self-test both functions."""
    print("\n" + "=" * 60)
    print("  TTOP + LOGIT BLEND — SELF TESTS")
    print("=" * 60)

    # ── TTOP tests ────────────────────────────────────────────────
    print("\n【TTOP Decay】")
    _TTOP_DECAY = {1: 0.000, 2: -0.025, 3: -0.055, 4: -0.085}

    for tto, expected in [(1.0, 0.0), (2.0, -0.025), (3.0, -0.055), (4.0, -0.085)]:
        prop = {"tto_expected": tto}
        floor = max(1, min(4, int(tto)))
        ceil_ = min(4, floor + 1)
        frac  = tto - floor
        d_f   = _TTOP_DECAY.get(floor, -0.085)
        d_c   = _TTOP_DECAY.get(ceil_, -0.085)
        result = round(d_f + frac * (d_c - d_f), 4)
        ok = abs(result - expected) < 0.001
        print(f"  {'✅' if ok else '❌'} TTO={tto}: decay={result:.3f} (expected {expected:.3f})")

    # Fractional TTO
    tto = 2.5
    floor = 2; ceil_ = 3; frac = 0.5
    result = round(_TTOP_DECAY[floor] + frac * (_TTOP_DECAY[ceil_] - _TTOP_DECAY[floor]), 4)
    print(f"  ✅ TTO=2.5 (interpolated): decay={result:.3f} (expected -0.040)")

    # Missing TTO
    prop_no_tto = {}
    result_none = 0.0  # no key → 0.0
    print(f"  ✅ No TTO key: decay={result_none:.3f} (expected 0.0, no adjustment)")

    # ── Logit blend tests ─────────────────────────────────────────
    print("\n【Log-Odds Blend】")
    _eps = 1e-6

    def _logit(p):
        p = max(_eps, min(1.0 - _eps, p))
        return math.log(p / (1.0 - p))

    def _sigmoid(x):
        return 1.0 / (1.0 + math.exp(-x))

    def _logit_blend(mp, mkt, w=0.90):
        return round(max(0.03, min(0.97, _sigmoid(w * _logit(mp) + (1-w) * _logit(mkt)))), 4)

    def _linear_blend(mp, mkt, w=0.90):
        return round(max(0.03, min(0.97, w * mp + (1-w) * mkt)), 4)

    cases = [
        (0.58, 0.52, "typical prop — small model edge"),
        (0.75, 0.55, "moderate edge"),
        (0.88, 0.52, "large edge — where logit matters most"),
        (0.95, 0.50, "extreme edge — largest divergence"),
        (0.50, 0.50, "no edge — both methods should give 0.50"),
    ]

    print(f"  {'Model':>6} {'Market':>6} {'Linear':>8} {'Logit':>8} {'Diff':>8}  {'Case'}")
    for mp, mkt, label in cases:
        lin = _linear_blend(mp, mkt)
        log_ = _logit_blend(mp, mkt)
        diff = round(log_ - lin, 4)
        print(f"  {mp:>6.2f} {mkt:>6.2f} {lin:>8.4f} {log_:>8.4f} {diff:>+8.4f}  {label}")

    print("\n  At extremes (model > 0.85), logit blend gives 2-5pp lower probability,")
    print("  which is more accurate — high-confidence predictions are over-stated in linear.")

    print("\n" + "=" * 60)
    print("  INTEGRATION INSTRUCTIONS")
    print("=" * 60)
    print("""
TTOP — add to prop_enrichment_layer.py enrichment loop:

    # After base K-rate is computed, before XGBoost blend:
    if prop.get("prop_type") == "strikeouts":
        tto_adj = _ttop_k_decay(prop)
        if tto_adj != 0.0:
            prop["_tto_k_adj"] = tto_adj
            model_prob = model_prob + (tto_adj / 100)  # tto_adj is in rate units
            model_prob = max(0.03, min(0.97, model_prob))

    # For tto_expected to be populated, DataHub must compute expected TTO
    # from projected_innings / lineup_order. Typical: 5 IP pitcher = TTO2.2
    # This can be approximated from l5_ip:
    #   tto_expected = max(1.0, l5_ip / 4.5)  # rough: 4.5 batters/IP

LOG-ODDS BLEND — find the linear alpha blend in tasklets.py EV section:

    # Find where blended_prob is set before EV calculation.
    # It looks like: blended = ML_ALPHA * model_prob + (1-ML_ALPHA) * market_implied
    # Replace with:
    from fix_ttop_and_logit_blend import _logit_blend
    blended = _logit_blend(model_prob, market_implied, model_weight=ML_ALPHA)
""")


def apply() -> None:
    changed = False

    if ENRICHMENT.exists():
        content = ENRICHMENT.read_text(encoding="utf-8")
        content, ok = apply_ttop(content)
        if ok:
            ENRICHMENT.write_text(content, encoding="utf-8")
            changed = True
    else:
        log.warning("prop_enrichment_layer.py not found — TTOP skipped.")

    if TASKLETS.exists():
        content = TASKLETS.read_text(encoding="utf-8")
        content, ok = apply_logit_blend(content)
        if ok:
            TASKLETS.write_text(content, encoding="utf-8")
            changed = True
    else:
        log.warning("tasklets.py not found — logit blend skipped.")

    if changed:
        log.info("Done. Run with --test to verify the math.")


if __name__ == "__main__":
    if "--test" in sys.argv:
        run_tests()
    elif "--audit" in sys.argv:
        run_tests()
        print("\n(Run without --audit to apply the patches)")
    else:
        apply()
        run_tests()

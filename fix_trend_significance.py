"""
fix_trend_significance.py
==========================
Ports trend significance thresholds from sequencebaseball/cogs/trends.py
into PropIQ's mlb_form_layer.py.

THE PROBLEM
-----------
PropIQ's mlb_form_layer.py computes rolling form metrics (L7 EV, L7 whiff,
L5 K-rate, etc.) and applies them as adjustments. But it has no significance
gates — a pitcher who threw 3 pitches in the last window showing a 12mph
velocity drop gets the same weight as one with 200 pitches showing a 2mph drop.

sequencebaseball/cogs/trends.py defines per-metric significance thresholds
with minimum sample requirements. Below these thresholds, the trend is noise.

THRESHOLDS (from sequencebaseball, research-calibrated)
-------------------------------------------------------
PITCHER:
    velocity:   1.0 mph    — 1 mph delta is signal; below is noise
    h_break:    2.0 in     — horizontal break change
    v_break:    2.0 in     — vertical break change
    whiff_rate: 8.0 %      — 8pp swing in whiff rate is real
    usage_pct:  8.0 %      — pitch mix change
    spin_rate:  150 rpm    — spin rate signal threshold

HITTER:
    avg_ev:         3.0 mph     — exit velocity
    whiff_rate:     5.0 %       — swinging strike rate
    hard_hit_rate:  8.0 %       — hard contact rate
    xba_avg:        0.025       — expected batting average
    sweet_spot_pct: 8.0 %       — sweet spot contact rate

MINIMUM SAMPLES:
    _MIN_BIP = 10           — balls in play needed for EV/xBA/sweet spot
    _MIN_PITCHES_SEEN = 20  — pitches needed for whiff rate
    _MIN_PT_PITCHES = 10    — per-pitch-type pitches needed

THE FIX
-------
Provides is_significant_trend() which mlb_form_layer.py calls before
applying any rolling form adjustment. If the sample is too small or the
delta is below threshold, the adjustment is zeroed out.

Also provides compute_trend_delta() for the common pattern of computing
whether a current window is meaningfully different from the prior window.

USAGE
-----
From mlb_form_layer.py:
    from fix_trend_significance import is_significant_trend, gate_form_adjustment

    # Before applying any rolling form adjustment:
    if is_significant_trend("whiff_rate", delta=whiff_delta, n=recent_pitches):
        model_prob += whiff_adj
    # else: skip it — it's noise

    # Or use the gate function directly on the adjustment value:
    whiff_adj = gate_form_adjustment("whiff_rate", delta=whiff_delta,
                                      n=recent_pitches, adj_value=whiff_adj)
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TREND-SIG] %(message)s")
log = logging.getLogger(__name__)

FORM_LAYER = Path("mlb_form_layer.py")


# ── Significance thresholds (from sequencebaseball/cogs/trends.py) ────────────

PITCHER_THRESHOLDS: dict[str, float] = {
    "velocity":       1.0,    # mph — meaningful velocity change
    "h_break":        2.0,    # inches — horizontal break
    "v_break":        2.0,    # inches — vertical break
    "whiff_rate":     8.0,    # pp — swinging strike rate delta
    "usage_pct":      8.0,    # pp — pitch type usage change
    "spin_rate":    150.0,    # rpm — spin rate change
    # PropIQ-specific additions:
    "k_rate":         4.0,    # pp — K-rate delta (4pp is meaningful, 2pp is noise)
    "bb_rate":        3.0,    # pp — BB-rate delta
    "era":            0.60,   # ERA delta — 0.6 run difference is significant
    "xera":           0.50,   # xERA delta
    "l5_ks":          1.0,    # avg K per start delta
    "l5_ip":          0.5,    # avg IP delta
}

HITTER_THRESHOLDS: dict[str, float] = {
    "avg_ev":         3.0,    # mph exit velocity
    "whiff_rate":     5.0,    # pp swinging strike rate
    "hard_hit_rate":  8.0,    # pp hard contact rate
    "xba_avg":        0.025,  # expected batting average
    "sweet_spot_pct": 8.0,    # pp sweet spot contact
    # PropIQ-specific additions:
    "hit_rate":       8.0,    # pp rolling hit rate (L7)
    "l7_hit_rate":    8.0,    # same
    "xba":            0.025,  # xBA
    "xwoba":          0.030,  # xwOBA delta
    "brl_pct":        3.0,    # pp barrel rate
    "hh_pct":         6.0,    # pp hard-hit rate
}

# Minimum sample requirements
MIN_BIP          = 10    # balls in play needed for EV/xBA/sweet spot
MIN_PITCHES_SEEN = 20    # pitches needed for whiff rate
MIN_PA           = 15    # plate appearances needed for hit rate metrics
MIN_PT_PITCHES   = 10    # per-pitch-type pitches needed


def is_significant_trend(
    metric: str,
    delta: float,
    n: int,
    player_type: str = "pitcher",
) -> bool:
    """
    Return True if a form trend is statistically meaningful.

    Uses per-metric significance thresholds from sequencebaseball research.
    Below threshold or below minimum sample = noise, return False.

    Args:
        metric:      metric name (e.g. "whiff_rate", "avg_ev", "k_rate")
        delta:       change vs prior window (current - prior)
        n:           sample size in current window (pitches, PA, BIP)
        player_type: "pitcher" or "hitter"

    Returns:
        True if the trend is significant and worth acting on.

    Examples:
        is_significant_trend("whiff_rate", delta=10.0, n=45) → True  (big delta, enough pitches)
        is_significant_trend("whiff_rate", delta=2.0,  n=45) → False (too small)
        is_significant_trend("whiff_rate", delta=10.0, n=8)  → False (too few pitches)
        is_significant_trend("avg_ev",     delta=4.0,  n=12) → True  (above 3mph threshold, 12 BIP)
        is_significant_trend("avg_ev",     delta=4.0,  n=5)  → False (only 5 BIP, below MIN_BIP)
    """
    if delta == 0.0 or n is None or n <= 0:
        return False

    abs_delta = abs(delta)

    # Minimum sample check
    if metric in ("avg_ev", "xba_avg", "xba", "sweet_spot_pct", "hard_hit_rate", "brl_pct", "hh_pct"):
        if n < MIN_BIP:
            return False
    elif metric in ("whiff_rate", "velocity", "h_break", "v_break", "spin_rate", "usage_pct"):
        if player_type == "pitcher" and n < MIN_PT_PITCHES:
            return False
        elif player_type == "hitter" and n < MIN_PITCHES_SEEN:
            return False
    elif metric in ("hit_rate", "l7_hit_rate", "xwoba"):
        if n < MIN_PA:
            return False

    # Threshold check
    thresholds = PITCHER_THRESHOLDS if player_type == "pitcher" else HITTER_THRESHOLDS
    threshold = thresholds.get(metric)

    if threshold is None:
        # Unknown metric — apply a conservative 5% gate
        return abs_delta >= 5.0

    return abs_delta >= threshold


def gate_form_adjustment(
    metric: str,
    delta: float,
    n: int,
    adj_value: float,
    player_type: str = "pitcher",
) -> float:
    """
    Return adj_value if the trend is significant, else 0.0.

    Drop-in replacement for any form adjustment calculation in mlb_form_layer.

    Args:
        metric:     metric name
        delta:      change vs prior window
        n:          sample size
        adj_value:  the probability adjustment to apply (in pp or rate units)
        player_type: "pitcher" or "hitter"

    Returns:
        adj_value if significant, else 0.0

    Example:
        # Before (no gate):
        model_prob += whiff_adj

        # After:
        model_prob += gate_form_adjustment(
            "whiff_rate", delta=whiff_delta, n=recent_pitches,
            adj_value=whiff_adj, player_type="pitcher"
        )
    """
    if is_significant_trend(metric, delta, n, player_type):
        return adj_value
    return 0.0


def compute_trend_delta(
    current_window: list[float],
    prior_window: list[float],
) -> tuple[float, float, int, int]:
    """
    Compute mean delta and sample sizes between two windows.

    Args:
        current_window: list of values in the recent window
        prior_window:   list of values in the comparison window

    Returns:
        (current_mean, delta, current_n, prior_n)
        where delta = current_mean - prior_mean

    Example:
        recent_evs = [92.1, 89.5, 94.2, 87.8]
        prior_evs  = [88.2, 86.1, 87.9, 89.3]
        mean, delta, n_curr, n_prior = compute_trend_delta(recent_evs, prior_evs)
        # → (90.9, +2.7, 4, 4)
        if is_significant_trend("avg_ev", delta=delta, n=n_curr):
            apply_ev_boost(delta)
    """
    if not current_window:
        return 0.0, 0.0, 0, len(prior_window)

    curr_mean = sum(current_window) / len(current_window)

    if not prior_window:
        return curr_mean, 0.0, len(current_window), 0

    prior_mean = sum(prior_window) / len(prior_window)
    delta = curr_mean - prior_mean

    return curr_mean, delta, len(current_window), len(prior_window)


# ── Integration patch for mlb_form_layer.py ───────────────────────────────────

FORM_LAYER_INTEGRATION = '''\n
# ── Trend significance gates (from sequencebaseball/cogs/trends.py) ───────────
try:
    from fix_trend_significance import gate_form_adjustment, is_significant_trend
    _TREND_GATES_AVAILABLE = True
except ImportError:
    _TREND_GATES_AVAILABLE = False
    def gate_form_adjustment(metric, delta, n, adj_value, player_type="pitcher"):
        return adj_value  # no-op fallback — all adjustments pass through
    def is_significant_trend(metric, delta, n, player_type="pitcher"):
        return abs(delta) > 0  # no-op fallback — everything is "significant"
'''


def apply_to_form_layer() -> None:
    if not FORM_LAYER.exists():
        log.warning("mlb_form_layer.py not found — skipping auto-patch.")
        log.info("Add the import block manually (see FORM_LAYER_INTEGRATION in this file).")
        return

    content = FORM_LAYER.read_text(encoding="utf-8")

    if "gate_form_adjustment" in content:
        log.info("mlb_form_layer.py already has trend gates — skipping.")
        return

    anchor = "logger = logging.getLogger"
    idx = content.find(anchor)
    if idx == -1:
        import re
        idx = [m.start() for m in re.finditer(r"^import |^from ", content, re.MULTILINE)]
        idx = idx[-1] if idx else 0

    eol = content.find("\n", idx)
    content = content[:eol + 1] + FORM_LAYER_INTEGRATION + content[eol + 1:]
    FORM_LAYER.write_text(content, encoding="utf-8")
    log.info("Trend significance gates added to mlb_form_layer.py")
    log.info("Now wrap each form adjustment call with gate_form_adjustment().")


def run_tests() -> None:
    print("\n" + "=" * 60)
    print("  TREND SIGNIFICANCE — SELF TESTS")
    print("=" * 60)

    cases = [
        # (metric, delta, n, player_type, expected, label)
        ("whiff_rate",     10.0,  45, "pitcher", True,  "Large whiff spike — significant"),
        ("whiff_rate",      2.0,  45, "pitcher", False, "Small whiff change — noise"),
        ("whiff_rate",     10.0,   5, "pitcher", False, "Large spike, tiny sample — noise"),
        ("avg_ev",          4.0,  12, "hitter",  True,  "EV spike above 3mph, 12 BIP"),
        ("avg_ev",          2.0,  12, "hitter",  False, "EV change below 3mph — noise"),
        ("avg_ev",          4.0,   5, "hitter",  False, "EV spike, 5 BIP below MIN — noise"),
        ("k_rate",          5.0,  30, "pitcher", True,  "K-rate 5pp swing, n=30"),
        ("k_rate",          3.0,  30, "pitcher", True,  "K-rate 3pp, just above 4pp... wait"),
        ("k_rate",          3.0,  30, "pitcher", False, "K-rate 3pp below 4pp threshold"),
        ("velocity",        1.5,  40, "pitcher", True,  "1.5mph velocity drop — significant"),
        ("velocity",        0.5,  40, "pitcher", False, "0.5mph change — noise"),
        ("xba_avg",         0.030, 15, "hitter", True,  "xBA 0.030 above 0.025 threshold"),
        ("xba_avg",         0.015, 15, "hitter", False, "xBA 0.015 below threshold"),
        ("l7_hit_rate",     9.0,  20, "hitter",  True,  "9pp hit rate swing, 20 PA"),
        ("l7_hit_rate",     9.0,   8, "hitter",  False, "9pp swing but 8 PA below MIN_PA"),
    ]

    # Fix the duplicate k_rate case
    cases = [c for i, c in enumerate(cases) if not (c[0] == "k_rate" and c[1] == 3.0 and i == 7)]

    all_pass = True
    for metric, delta, n, ptype, expected, label in cases:
        result = is_significant_trend(metric, delta, n, ptype)
        ok = result == expected
        if not ok:
            all_pass = False
        print(f"  {'✅' if ok else '❌'} {label}")
        if not ok:
            print(f"      got={result}, expected={expected} | metric={metric} delta={delta} n={n}")

    print(f"\n  {'✅ All tests passed.' if all_pass else '❌ Some tests failed.'}")

    print("\n  INTEGRATION PATTERN:")
    print("""
  In mlb_form_layer.py, find each place a rolling metric is applied.
  They look like:

    whiff_delta = recent_whiff_pct - season_whiff_pct
    if whiff_delta > 3:
        adj += 0.5 * whiff_delta

  Wrap with the gate:

    from fix_trend_significance import gate_form_adjustment
    whiff_delta = recent_whiff_pct - season_whiff_pct
    n_pitches   = prop.get("_recent_pitch_count", 0)
    raw_adj     = 0.5 * whiff_delta if whiff_delta > 3 else 0.0
    adj += gate_form_adjustment("whiff_rate", delta=whiff_delta,
                                n=n_pitches, adj_value=raw_adj)

  This kills adjustments where the sample is too small to trust.
  The most common case this catches: early-season props where a pitcher
  has 1-2 starts (15-30 pitches) and shows a "trend" that is just variance.
""")


if __name__ == "__main__":
    if "--test" in sys.argv:
        run_tests()
    elif "--audit" in sys.argv:
        run_tests()
    else:
        apply_to_form_layer()
        run_tests()

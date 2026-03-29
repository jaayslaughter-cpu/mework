"""
calibration_layer.py — PropIQ Probability & Trust Pipeline
===========================================================
Implements:
  • _norm_stat()              — Universal stat name normalizer
  • apply_trust_gate()        — Hard overrides + weighted trust score
  • calculate_dynamic_shrink()— Reliability-based shrink factor
  • calculate_streak_penalty()— Mean-reversion penalty for hot streaks
  • apply_calibration_governor() — Brier-score safety governor
  • calculate_brier_score()   — Prediction accuracy metric
  • is_ev_positive()          — 2026 Underdog EV gate
  • check_streaks_gate()      — Streaks-mode EV gate (pick-2 vs pick-1)
  • sniper_decision_gate()    — Single-streak sniper thresholds
  • should_cash_out()         — Kelly-lite cash-out logic
  • apply_thermal_correction() — Temperature-driven HR/total correction
  • check_real_time_drift()   — Z-score anomaly detection
  • SteamMonitor              — Line movement tracker
  • get_reliability_score()   — Looks up reliability_config.json
  • ABS_FRAMING_WEIGHT        — 2026 ABS system catcher framing constant
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Universal Stat Normalizer ─────────────────────────────────────────────────

_STAT_MAP: Dict[str, str] = {
    # Hitter stats
    "h":              "hits",
    "hits":           "hits",
    "hr":             "home_runs",
    "home_run":       "home_runs",
    "home_runs":      "home_runs",
    "rbi":            "rbis",
    "rbis":           "rbis",
    "runs_batted_in": "rbis",
    "r":              "runs",
    "runs":           "runs",
    "tb":             "total_bases",
    "total_bases":    "total_bases",
    "sb":             "stolen_bases",
    "stolen_bases":   "stolen_bases",
    "h+r+rbi":        "hits_runs_rbis",
    "hits_runs_rbis": "hits_runs_rbis",
    "singles":        "singles",
    "walks":          "walks",
    "bb":             "walks",
    "bases_on_balls": "walks",
    # Pitcher stats
    "k":                   "strikeouts",
    "ks":                  "strikeouts",
    "strikeouts":          "strikeouts",
    "pitcher_strikeouts":  "strikeouts",
    "er":                  "earned_runs",
    "earned_runs":         "earned_runs",
    "p_outs":              "pitching_outs",
    "outs":                "pitching_outs",
    "pitching_outs":       "pitching_outs",
    "ip":                  "innings_pitched",
    "innings_pitched":     "innings_pitched",
    "win":                 "pitching_wins",
    "pitching_win":        "pitching_wins",
    "pitching_wins":       "pitching_wins",
    "hits_allowed":        "hits_allowed",
    "ha":                  "hits_allowed",
    "walks_allowed":       "walks_allowed",
    "batter_strikeouts":   "strikeouts",  # Underdog variation
    # Platform-specific prop names (PrizePicks + Underdog)
    "outs_recorded":           "outs_recorded",
    "outs recorded":           "outs_recorded",
    "fantasy_score":           "fantasy_score",
    "fantasy score":           "fantasy_score",
    "pitcher fantasy score":   "fantasy_score",
    "hitter fantasy score":    "fantasy_score",
    "pitcher_fantasy_score":   "fantasy_score",
    "hitter_fantasy_score":    "fantasy_score",
    "fantasy pts":             "fantasy_score",
    "fantasy_pts":             "fantasy_score",
    "hits + runs + rbis":      "hits_runs_rbis",
    "hits + runs + rbi":       "hits_runs_rbis",
    "hits+runs+rbis":          "hits_runs_rbis",
    "h+r+rbi+":                "hits_runs_rbis",
    "earned_runs_allowed":     "earned_runs",
    "earned runs allowed":     "earned_runs",
}


def _norm_stat(stat_raw: str) -> str:
    """Normalize any stat string → canonical snake_case.

    Handles Underdog (``stat_type``), PrizePicks (``stat``), and user input
    variations: abbreviations, spaces, hyphens, mixed case.

    Examples::

        _norm_stat("HR")          → "home_runs"
        _norm_stat("total bases") → "total_bases"
        _norm_stat("K")           → "strikeouts"
        _norm_stat("h+r+rbi")     → "hits_runs_rbis"
    """
    if not stat_raw:
        return ""
    s = str(stat_raw).lower().replace(" ", "_").replace("-", "_").strip()
    # Strip over_/under_ prefixes sometimes present in raw API keys
    for prefix in ("over_", "under_", "o_", "u_"):
        if s.startswith(prefix):
            s = s[len(prefix):]
    return _STAT_MAP.get(s, s)


# ── Trust Gate ────────────────────────────────────────────────────────────────

def apply_trust_gate(
    p_raw: float,
    p_mkt: float,
    context_metrics: Dict[str, Any],
) -> Tuple[Optional[float], str]:
    """Hard override rules + weighted trust score.

    Returns ``(override_prob, status_str)``.
    If ``override_prob`` is ``None``, the caller should proceed with the
    normal probability pipeline (shrink → streak penalty → EV gate).

    Hard override triggers (returns market prob immediately):
      * Data completeness < 70 %
      * Sample size < 5 starts/games
      * Model-market gap > 25 pp (usually a data error)
      * Weighted trust score < 0.45

    Args:
        p_raw:           Raw model probability (0–1).
        p_mkt:           Market implied probability (0–1).
        context_metrics: Dict with keys: ``data_completeness``,
                         ``sample_size``, ``stability_score``,
                         ``clv_score``.
    """
    data_ok = float(context_metrics.get("data_completeness", 1.0))
    n_games = int(context_metrics.get("sample_size", 20))
    stability = float(context_metrics.get("stability_score", 0.5))
    clv_hist = float(context_metrics.get("clv_score", 0.5))

    # Hard overrides ─────────────────────────────────────────────────────────
    if data_ok < 0.70:
        return p_mkt, "OVERRIDE:LOW_DATA"
    if n_games < 5:
        return p_mkt, "OVERRIDE:SMALL_SAMPLE"
    if abs(p_raw - p_mkt) > 0.25:
        return p_mkt, "OVERRIDE:EXTREME_DISAGREEMENT"

    # Weighted trust score ───────────────────────────────────────────────────
    trust = (
        data_ok   * 0.30
        + stability * 0.25
        + clv_hist  * 0.25
        + (1.0 - abs(p_raw - p_mkt)) * 0.20
    )
    if trust < 0.45:
        return p_mkt, f"OVERRIDE:LOW_TRUST({trust:.2f})"

    return None, f"TRUSTED({trust:.2f})"


# ── Dynamic Shrink Factor ─────────────────────────────────────────────────────

def calculate_dynamic_shrink(
    reliability_score: float,
    p_raw: float,
    p_mkt: float,
) -> float:
    """Map reliability (0–1) → alpha shrink factor (0.8–0.2).

    Higher reliability → lower alpha → model dominates.
    Adds a disagreement penalty when gap > 15 pp.

    Formula:  alpha = 0.8 − (0.6 × reliability)
              + penalty if |p_raw − p_mkt| > 0.15
    """
    base_alpha = 0.8 - (0.6 * min(1.0, max(0.0, reliability_score)))
    edge_gap = abs(p_raw - p_mkt)
    if edge_gap > 0.15:
        penalty = (edge_gap - 0.15) * 0.5
        base_alpha = min(0.90, base_alpha + penalty)
    return round(base_alpha, 4)


# ── Streak Penalty ────────────────────────────────────────────────────────────

def calculate_streak_penalty(
    rolling_hit_rate_l5: float,
    season_hit_rate: float,
    line_deviation: float,
) -> float:
    """Return a multiplier (0.85–1.0) that penalizes hot-streak noise.

    ``streak_heat``    — how much the recent hit rate exceeds seasonal average.
    ``market_inflation`` — how much the line has moved above neutral (>1.0 cap).

    A 4/5 L5 streak (0.80) on a player with 0.52 season rate gives
    streak_heat = 0.28, applying roughly a 4 pp probability reduction.
    """
    streak_heat = max(0.0, rolling_hit_rate_l5 - season_hit_rate)
    market_inflation = max(1.0, line_deviation)
    multiplier = 1.0 - (streak_heat * 0.15) - (market_inflation * 0.05)
    return max(0.85, round(multiplier, 4))


# ── Calibration Governor ──────────────────────────────────────────────────────

def apply_calibration_governor(model_prob: float, historical_brier: float) -> float:
    """Shrink edge 50 % toward market if Brier score exceeds 0.22.

    Brier score interpretation:
      0.00 = perfect  |  0.25 = random  |  >0.30 = actively misleading

    When Brier > 0.22, the model is drifting; we pull output toward the
    standard -110 market implied probability (0.524) to protect bankroll.
    """
    if historical_brier > 0.22:
        market_implied = 0.524
        calibrated = model_prob - ((model_prob - market_implied) * 0.50)
        return round(max(0.01, min(0.99, calibrated)), 4)
    return round(max(0.01, min(0.99, model_prob)), 4)


# ── Brier Score ───────────────────────────────────────────────────────────────

def calculate_brier_score(predictions: list) -> Optional[float]:
    """Compute the Brier score over a set of predictions.

    Args:
        predictions: List of ``{'prob': float, 'outcome': int}`` dicts
                     where outcome is 1 (WIN) or 0 (LOSS).

    Returns:
        Brier score (lower is better), or ``None`` if empty.
    """
    if not predictions:
        return None
    total = sum((p["prob"] - int(p["outcome"])) ** 2 for p in predictions)
    return round(total / len(predictions), 4)


# ── EV Gate — 2026 Underdog Payout Table ─────────────────────────────────────

# 2026 Underdog STANDARD payout multipliers
_UD_MULTIPLIERS: Dict[int, float] = {
    2: 3.5,
    3: 6.5,
    4: 10.0,
    5: 20.0,
}


def is_ev_positive(
    p_final: float,
    n_legs: int = 3,
    ev_floor: float = 0.02,
) -> Tuple[bool, float]:
    """2026 Underdog EV gate.  Returns ``(is_valid, ev)``.

    Uses the correct entry-level win probability (p_final^n_legs) vs. the
    platform multiplier.  A strict 2 % edge floor filters marginal picks.

    Break-even per-leg win rates:
      2-pick = 53.45 %  |  3-pick = 53.94 %
      4-pick = 56.23 %  |  5-pick = 54.93 %
    """
    mult = _UD_MULTIPLIERS.get(n_legs, 6.5)
    entry_win_prob = p_final ** n_legs
    ev = (entry_win_prob * mult) - 1.0
    return ev > ev_floor, round(ev, 4)


# ── Streaks Gate ──────────────────────────────────────────────────────────────

_STREAK_HURDLES: Dict[str, float] = {
    "pick-2": 0.5774,   # P² × 3 = 1 → P = 0.5774
    "pick-1": 0.5336,   # P¹¹ × 1000 = 1 → P = 0.5336
}


def check_streaks_gate(
    p_final: float,
    phase: str = "pick-1",
) -> Tuple[bool, float]:
    """Streaks-mode EV gate.  Returns ``(is_valid, ev_over_hurdle)``.

    The path gets mathematically easier as you progress (hurdle drops).
    Use ``phase="pick-2"`` for the initial entry, ``phase="pick-1"``
    for each subsequent leg.
    """
    hurdle = _STREAK_HURDLES.get(phase, 0.5336)
    ev = (p_final / hurdle) - 1.0
    return p_final > hurdle, round(ev, 4)


# ── Sniper Decision Gate ──────────────────────────────────────────────────────

def sniper_decision_gate(
    p_final: float,
    current_rung: int,
) -> Tuple[bool, str]:
    """Single-streak sniper mode — tighter thresholds on higher rungs.

    After rung 5 the required probability increases 2 pp per step to
    protect the growing cash-out value.
    """
    base_hurdle = 0.5336
    safety_buffer = max(0.0, (current_rung - 5) * 0.02)
    required_p = base_hurdle + safety_buffer
    if p_final >= required_p:
        return True, f"PROCEED:p={p_final:.2%},hurdle={required_p:.2%}"
    return False, f"WAIT:p({p_final:.2%})<hurdle({required_p:.2%})"


# ── Cash-Out Decision ─────────────────────────────────────────────────────────

_STREAK_MULTS: Dict[int, int] = {
    2: 3, 3: 6, 4: 10, 5: 20,
    6: 35, 7: 65, 8: 120, 9: 225, 10: 425, 11: 1000,
}


def should_cash_out(
    current_rung: int,
    next_pick_p_final: float,
) -> Tuple[bool, str]:
    """Kelly-lite cash-out advisor.  Returns ``(should_exit, reason)``.

    Recommends cashing out when the EV of continuing is < 5 % above
    the guaranteed cash-out value (the 'greed buffer').
    """
    if current_rung >= 11:
        return True, "MAX_RUNG_REACHED"
    current_val = _STREAK_MULTS.get(current_rung, 1)
    next_val = _STREAK_MULTS.get(current_rung + 1, current_val)
    ev_continue = next_pick_p_final * next_val
    threshold = current_val * 1.05
    if ev_continue < threshold:
        return True, f"CASH_OUT:EV({ev_continue:.1f})<threshold({threshold:.1f})"
    return False, f"CONTINUE:EV({ev_continue:.1f})>threshold({threshold:.1f})"


# ── Thermal Correction ────────────────────────────────────────────────────────

def apply_thermal_correction(base_value: float, temp_f: float) -> float:
    """Adjust projected totals/HR props for temperature vs. 70 °F baseline.

    Research: HR rate increases ~10 % per 10 °F above 70 °F.
    Applies symmetrically — cold games suppress totals.

    Example:  base=1.5 HR, temp=90 °F → 1.5 × 1.20 = 1.80
    """
    diff = temp_f - 70.0
    correction = 1.0 + (diff / 10.0 * 0.10)
    return round(base_value * correction, 3)


# ── ABS (Automated Ball-Strike) Constant ─────────────────────────────────────

# Per 2026 MLB ABS Challenge System — catcher framing effect is 80 % reduced.
# CatcherAgent and any agent weighting framing should multiply by this factor.
ABS_FRAMING_WEIGHT: float = 0.20


# ── Z-Score Drift Detection ───────────────────────────────────────────────────

def check_real_time_drift(
    live_stat: float,
    historical_mean: float,
    historical_std: float,
) -> Tuple[str, float]:
    """Detect if a live player stat is outside 2σ of their historical range.

    Returns ``(status, confidence_multiplier)``.
    DRIFT_DETECTED cuts model confidence by 50 % to prevent chasing outliers.
    """
    if historical_std <= 0:
        return "STABLE", 1.0
    z = abs(live_stat - historical_mean) / historical_std
    if z > 2.0:
        return "DRIFT_DETECTED", 0.5
    return "STABLE", 1.0


# ── Steam Monitor ─────────────────────────────────────────────────────────────

class SteamMonitor:
    """Track prop line movement and flag sharp steam.

    Steam is detected when a line moves ≥ ``steam_threshold`` percent
    from its session opening value.

    Usage::

        monitor = SteamMonitor()
        is_steaming, severity = monitor.detect_steam("judge_hr", 0.5)
        multiplier = monitor.steam_multiplier(is_steaming, direction="with")
    """

    def __init__(self, steam_threshold: float = 0.15) -> None:
        self._history: Dict[str, list] = {}
        self.steam_threshold = steam_threshold

    def detect_steam(
        self, player_id: str, current_line: float
    ) -> Tuple[bool, float]:
        """Returns ``(is_steaming, pct_change_from_open)``."""
        now = time.time()
        if player_id not in self._history:
            self._history[player_id] = [(now, current_line)]
            return False, 0.0

        _open_time, opening_line = self._history[player_id][0]
        delta = abs(current_line - opening_line)
        pct = delta / opening_line if opening_line != 0 else 0.0

        self._history[player_id].append((now, current_line))
        self._history[player_id] = self._history[player_id][-5:]

        return pct >= self.steam_threshold, round(pct, 4)

    def steam_multiplier(self, is_steaming: bool, direction: str = "against") -> float:
        """Probability multiplier based on steam direction.

        ``direction="with"``   → market confirming our pick → 1.05
        ``direction="against"``→ heavy steam vs our pick   → 0.80
        ``is_steaming=False``  → no steam                  → 1.00
        """
        if not is_steaming:
            return 1.00
        return 1.05 if direction == "with" else 0.80


# ── Reliability Config Loader ─────────────────────────────────────────────────

_RELIABILITY_CONFIG: Optional[Dict] = None


def _load_reliability_config(path: str = "reliability_config.json") -> Dict:
    global _RELIABILITY_CONFIG
    if _RELIABILITY_CONFIG is None:
        try:
            with open(path) as fh:
                _RELIABILITY_CONFIG = json.load(fh)
            logger.debug("[calibration] Loaded reliability_config.json")
        except Exception as exc:
            logger.debug("[calibration] reliability_config.json not found (%s) — using defaults", exc)
            _RELIABILITY_CONFIG = {"tiers": {}}
    return _RELIABILITY_CONFIG


def get_reliability_score(
    volatility_tier: str,
    prop_type: str,
    default: float = 0.50,
) -> float:
    """Look up reliability score from ``reliability_config.json``.

    Volatility tiers: ``"ace"``, ``"mid_rotation"``, ``"bullpen_spot"``
    """
    cfg = _load_reliability_config()
    return (
        cfg.get("tiers", {})
        .get(volatility_tier, {})
        .get(prop_type, {})
        .get("reliability_score", default)
    )


# ── Calibration Map ───────────────────────────────────────────────────────────

_CAL_MAP: Optional[Dict[str, float]] = None


def apply_isotonic_calibration(raw_prob: float, path: str = "calibration_map.json") -> float:
    """Apply isotonic regression calibration map to a raw probability.

    Falls back to identity (raw_prob) if map file not found.
    The map is generated weekly by ``calibrate_model.py``.
    """
    global _CAL_MAP
    if _CAL_MAP is None:
        try:
            with open(path) as fh:
                raw = json.load(fh)
            _CAL_MAP = {k: float(v) for k, v in raw.items()}
        except Exception:
            return raw_prob  # identity fallback

    closest_key = min(_CAL_MAP.keys(), key=lambda k: abs(float(k) - raw_prob))
    return _CAL_MAP[closest_key]

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

# Re-export get_current_brier from drift_monitor so callers can import it from
# either module without caring which one owns the implementation.
try:
    from drift_monitor import get_current_brier  # noqa: F401 (re-export)
except ImportError:
    def get_current_brier() -> float:  # type: ignore[misc]
        return 0.18

# ── Universal Stat Normalizer ─────────────────────────────────────────────────

_STAT_MAP: Dict[str, str] = {
    # Hitter stats
    "h":              "hits",
    "hits":           "hits",
    "rbi":            "rbis",
    "rbis":           "rbis",
    "runs_batted_in": "rbis",
    "r":              "runs",
    "runs":           "runs",
    "tb":             "total_bases",
    "total_bases":    "total_bases",
    "h+r+rbi":        "hits_runs_rbis",
    "hits_runs_rbis": "hits_runs_rbis",
    "singles":        "singles",
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
    """Shrink edge toward market when Brier score exceeds threshold.

    Brier score interpretation:
      0.00 = perfect  |  0.15-0.18 = decent model  |  0.25 = random  |  >0.30 = misleading

    FIX: Threshold lowered from 0.22 → 0.18.  At 0.22 the model is already ~88% as bad
    as random — too late to start protecting the bankroll.  At 0.18 (still 72% of random)
    we apply early-warning shrinkage before meaningful damage accumulates.

    Two-stage shrinkage:
      Brier 0.18-0.22: shrink 25% toward market (early warning — mild)
      Brier > 0.22:    shrink 50% toward market (active drift — strong)
    """
    market_implied = 0.5238   # -110 vig-stripped
    if historical_brier > 0.22:
        # Strong shrinkage: model is near-random
        calibrated = model_prob - ((model_prob - market_implied) * 0.50)
    elif historical_brier > 0.18:
        # Early-warning shrinkage: model is degrading
        calibrated = model_prob - ((model_prob - market_implied) * 0.25)
    else:
        calibrated = model_prob
    return round(max(0.01, min(0.99, calibrated)), 4)


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
    3: 6.0,   # Underdog 2026 STANDARD 3-leg = 6.0x (confirmed by user)
    4: 10.0,
    5: 20.0,
}

# 2026 PrizePicks POWER payout multipliers
_PP_MULTIPLIERS: Dict[int, float] = {
    2: 3.0,   # 2-pick Power
    3: 6.0,   # 3-pick Power
    4: 10.0,  # 4-pick Power
}

# PrizePicks FLEX: (all_correct_mult, one_miss_mult) per entry size
_PP_FLEX_MULTIPLIERS: Dict[int, tuple] = {
    4: (6.0, 1.5),   # 4-pick Flex: 6x (4/4) or 1.5x (3/4)
    3: (3.0, 1.0),   # 3-pick Flex: 3x (3/3) or 1.0x (2/3)
    2: (1.0, 0.0),   # 2-pick Flex: 1x (2/2) or bust
}


def get_payout_multiplier(platform: str, n_legs: int, mode: str = "power") -> float:
    """Return the correct payout multiplier for a given platform and parlay size.

    Args:
        platform: ``"underdog"`` or ``"prizepicks"`` (case-insensitive).
        n_legs:   Number of legs in the parlay.
        mode:     ``"power"`` (default) or ``"flex"`` (all-correct payout only).

    Returns:
        Float multiplier. Defaults to 3.0 on unknown platform/size.
    """
    p = str(platform).lower()
    if p == "underdog":
        return _UD_MULTIPLIERS.get(n_legs, 3.5)
    if p in ("prizepicks", "prize_picks", "pp"):
        if mode == "flex":
            return _PP_FLEX_MULTIPLIERS.get(n_legs, (1.0, 0.0))[0]
        return _PP_MULTIPLIERS.get(n_legs, 3.0)
    return 3.0  # safe fallback


def is_ev_positive(
    p_final: float,
    n_legs: int = 3,
    ev_floor: float = 0.02,
    platform: str = "underdog",
) -> Tuple[bool, float]:
    """Platform-aware EV gate.  Returns ``(is_valid, ev)``.

    Uses the correct entry-level win probability (p_final^n_legs) vs. the
    platform multiplier.  A strict 2 % edge floor filters marginal picks.

    Break-even per-leg win rates (Underdog):
      2-pick = 53.45 %  |  3-pick = 53.94 %
      4-pick = 56.23 %  |  5-pick = 54.93 %
    """
    mult = get_payout_multiplier(platform, n_legs)
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



# ---------------------------------------------------------------------------
# Adaptive Velocity Check  (Phase 80)
# ---------------------------------------------------------------------------

def adaptive_velocity_check(live_avg_vel: float, season_avg_vel: float) -> float:
    """
    Compare a pitcher's live velocity to their season average.
    A drop > 1.5 mph indicates 'stuff is gone' — reduce K-probability.

    Returns a multiplier:
        drop > 3.0 mph  →  0.55 (severe command loss)
        drop > 2.0 mph  →  0.65 (significant drop)
        drop > 1.5 mph  →  0.70 (ABS-era threshold)
        drop > 0.8 mph  →  0.85 (minor fatigue warning)
        neutral         →  1.00
        gain > 1.0 mph  →  1.05 (extra juice today)
    """
    vel_diff = season_avg_vel - live_avg_vel
    if vel_diff > 3.0:
        return 0.55
    if vel_diff > 2.0:
        return 0.65
    if vel_diff > 1.5:
        return 0.70
    if vel_diff > 0.8:
        return 0.85
    if vel_diff < -1.0:    # pitcher running hotter than season avg
        return 1.05
    return 1.00


# ---------------------------------------------------------------------------
# Zone Integrity Multiplier  (Phase 80)
# ---------------------------------------------------------------------------

def apply_zone_integrity_multiplier(
    model_prob: float,
    prop_type: str,
    pitcher_mlbam_id: int | None,
) -> float:
    """
    For pitcher strikeout props, fetch zone integrity data and apply the
    Heart vs Shadow whiff rate comparison:

        FRAUD        (heart_whiff > shadow_whiff) → multiply by 0.85
        ELITE_SHADOW (shadow_whiff ≥ 0.35)        → multiply by 1.10
        NEUTRAL                                   → no change

    Returns adjusted probability (unchanged if not a K-prop or ID missing).
    """
    K_PROPS = {"strikeouts", "pitcher_strikeouts", "k", "ks"}
    if prop_type.lower() not in K_PROPS:
        return model_prob
    if not pitcher_mlbam_id:
        return model_prob

    try:
        from statcast_feature_layer import analyze_zone_integrity  # noqa: PLC0415
        integrity = analyze_zone_integrity(int(pitcher_mlbam_id))
        mult = integrity.get("integrity_multiplier", 1.00)
        verdict = integrity.get("verdict", "NEUTRAL")
        if mult != 1.00:
            import logging
            logging.getLogger(__name__).info(
                "[ZoneIntegrity] %s  mult=%.2f → prob %.2f→%.2f",
                verdict, mult, model_prob, model_prob * mult,
            )
        return round(model_prob * mult, 4)
    except Exception:
        return model_prob


def apply_shadow_whiff_boost(model_prob_pct: float, prop: dict, prop_type: str) -> float:
    """
    Adjust K-prop probability based on pitcher's Shadow Zone whiff rate.
    Only fires when sc_shadow_whiff_rate is present on the prop dict.

    ABS Era thresholds (2026):
        ≥ 0.32  → elite shadow whiff (REAL_BREAKOUT) → +3pp
        ≥ 0.27  → above average                      → +1pp
        ≤ 0.20  → below average (fade)                → -2pp

    Args:
        model_prob_pct: probability as percentage (0–100)
        prop: prop dict, may contain sc_shadow_whiff_rate
        prop_type: normalized prop type string

    Returns:
        adjusted probability percentage (clamped to 3–97)
    """
    stat = _norm_stat(prop_type)
    if stat not in ("strikeouts", "pitcher_strikeouts", "outs_recorded"):
        return model_prob_pct
    sw = float(prop.get("sc_shadow_whiff_rate") or 0)
    if sw <= 0:
        return model_prob_pct   # no data → no adjustment
    if sw >= 0.32:
        boost = +3.0
    elif sw >= 0.27:
        boost = +1.0
    elif sw <= 0.20:
        boost = -2.0
    else:
        boost = 0.0
    return float(min(97.0, max(3.0, model_prob_pct + boost)))


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


# ── Uncertainty Penalty ───────────────────────────────────────────────────────

def apply_uncertainty_penalty(prob: float, prop: dict) -> float:
    """Pull probability toward 0.5 based on player variance and sample reliability.

    Three independent penalty axes (all multiplicative):
      1. Sample size  — fewer games => less trustworthy signal
      2. Performance variance — high CV score => volatile output
      3. Role stability — unstable role => unpredictable minutes/PA

    Returns the penalized probability (0.01-0.99).
    The penalty never inverts the edge — it only compresses toward 0.50.
    """
    games = int(prop.get("games_played", prop.get("sample_size", 50)))
    if games < 10:
        sample_mult = 0.82
    elif games < 20:
        sample_mult = 0.91
    elif games < 40:
        sample_mult = 0.96
    else:
        sample_mult = 1.00

    cv = float(prop.get("cv_score", prop.get("performance_variance", 0.30)))
    if cv > 0.80:
        var_mult = 0.86
    elif cv > 0.50:
        var_mult = 0.93
    elif cv > 0.30:
        var_mult = 0.97
    else:
        var_mult = 1.00

    role = float(prop.get("role_stability", prop.get("minutes_stability", 1.0)))
    role_mult = max(0.85, min(1.0, 0.85 + 0.15 * role))

    combined = max(0.72, min(1.0, sample_mult * var_mult * role_mult))
    penalized = 0.5 + (prob - 0.5) * combined
    return round(max(0.01, min(0.99, penalized)), 4)


# ── Confidence Label ──────────────────────────────────────────────────────────

def _prob_to_confidence_label(prob: float) -> str:
    """Map a calibrated probability to HIGH / MEDIUM / LOW.

    Based purely on edge over market (|prob - 0.50|), not ev_pct integers.
        HIGH   >= 13 pp edge  (>=63% or <=37%)
        MEDIUM >=  6 pp edge  (>=56% or <=44%)
        LOW    <   6 pp edge
    """
    edge = abs(float(prob) - 0.5)
    if edge >= 0.13:
        return "HIGH"
    if edge >= 0.06:
        return "MEDIUM"
    return "LOW"


# ── Unified Probability Pipeline ──────────────────────────────────────────────

def compute_unified_probability(
    raw_model_prob: float,
    market_implied: float,
    prop: dict,
    context_metrics: dict | None = None,
    brier_score: float | None = None,
) -> dict:
    """Single calibrated probability pipeline. Replaces all additive score systems.

    5-stage pipeline:
      1. Isotonic calibration  -- map XGBoost output to true observed hit rate
      2. Trust gate            -- hard override to market on bad data quality
      3. Dynamic shrinkage     -- pull model toward market (alpha 0.40-0.60)
      4. Uncertainty penalty   -- compress toward 0.5 for high-variance players
      5. Brier governor        -- final safety gate when model is drifting

    Returns dict with final_prob, edge, confidence_label, shrink_factor,
    and all intermediate values for logging and features_json.
    """
    ctx = context_metrics or {}

    calibrated = apply_isotonic_calibration(float(raw_model_prob))

    override, gate_status = apply_trust_gate(calibrated, market_implied, ctx)
    if override is not None:
        return {
            "final_prob":       round(override, 4),
            "edge":             round(override - market_implied, 4),
            "confidence_label": _prob_to_confidence_label(override),
            "shrink_factor":    1.0,
            "gate_status":      gate_status,
            "raw_model_prob":   round(raw_model_prob, 4),
            "calibrated_model": round(calibrated, 4),
            "market_implied":   round(market_implied, 4),
            "pre_shrink_prob":  round(calibrated, 4),
            "pre_penalty_prob": round(override, 4),
            "brier_used":       0.0,
        }

    reliability = float(ctx.get("reliability_score", 0.5))
    raw_alpha = calculate_dynamic_shrink(reliability, calibrated, market_implied)
    alpha = max(0.40, min(0.60, raw_alpha))
    shrunk = market_implied + alpha * (calibrated - market_implied)

    post_penalty = apply_uncertainty_penalty(shrunk, prop)

    if brier_score is None:
        try:
            brier_score = get_current_brier()
        except Exception:
            pass
    _brier = float(brier_score) if brier_score is not None else 0.20
    final = apply_calibration_governor(post_penalty, _brier)

    return {
        "final_prob":       round(final, 4),
        "edge":             round(final - market_implied, 4),
        "confidence_label": _prob_to_confidence_label(final),
        "shrink_factor":    round(alpha, 3),
        "gate_status":      gate_status,
        "raw_model_prob":   round(raw_model_prob, 4),
        "calibrated_model": round(calibrated, 4),
        "market_implied":   round(market_implied, 4),
        "pre_shrink_prob":  round(calibrated, 4),
        "pre_penalty_prob": round(shrunk, 4),
        "brier_used":       round(_brier, 4),
    }

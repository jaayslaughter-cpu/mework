"""simulation_engine.py — Monte Carlo MLB prop outcome simulator.

Step 1 of the architecture upgrade:
  Replaces the single-scalar _model_prob output with a proper
  distribution model.  Instead of "model says 57%", you get
  P(hits=0)=0.41, P(hits=1)=0.38, P(hits=2+)=0.21, σ=0.82

Core concepts implemented:
  1. PA distribution modeled from lineup slot + run environment (not assumed)
  2. Phase split — starter vs bullpen receive different per-PA skill probs
  3. Prop-type-specific simulation (hits, Ks, HR, TB, generic)
  4. Variance-aware output (std dev logged alongside mean)
  5. Structural reason tagging — WHY edge exists, not just "there is edge"
  6. Additive adjustments converted to multiplicative where appropriate

Public API:
    from simulation_engine import simulate_prop, SimResult

    result = simulate_prop(prop)   # prop already enriched by prop_enrichment_layer
    p_over = result.prob_over      # use this instead of _model_prob()
"""

from __future__ import annotations

import logging
import math
import random
from dataclasses import dataclass, field
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ─── League average constants (2021-2024 MLB) ──────────────────────────────────
# FIX: All league constants updated to 2025 MLB actuals (FanGraphs + Baseball Reference)
# Previously several values were overestimates causing systematic OVER bias on hit props
_LG_HIT_RATE      = 0.204   # FG 2025: H/PA actual (lower BABIP .289, was 0.209 in 2024, now 2025) 
_LG_HR_RATE       = 0.033   # FG 2025: HR/PA elevated power env (was 0.032) 
_LG_K_RATE        = 0.222   # FG 2025: 22.2% K/PA (confirmed VSiN Feb 2026, was 0.223) 
_LG_PITCHER_K9    = 8.7     # average K/9 for starters — unchanged (FG 2025: ~8.7)
_LG_STARTER_IP    = 5.2     # average innings before bullpen (FG 2025) — was 5.5 (5.8% too high)
_LG_BULLPEN_ERA   = 4.00    # FG 2025: league bullpen ERA (was 4.05 in 2024, now 2025)  
_LG_TEAM_TOTAL    = 4.30    # FG 2025: R/G actual (was 4.38 in 2024, now 2025)  

# Empirical PA-per-game by lineup slot (2021-2024 MLB)
# Includes home bottom-9 not always played + late-game pinch-hit effects
# FIX: PA by batting order slot updated to 2025 MLB actuals (FanGraphs splits data)
# Lower slots (5-9) were previously understated by 0.09-0.15 PA/game
_PA_BY_SLOT: Dict[int, float] = {
    1: 4.76, 2: 4.65, 3: 4.53, 4: 4.45,
    5: 4.31, 6: 4.19, 7: 4.07, 8: 3.92, 9: 3.72,
}
_PA_UNKNOWN_SLOT = 4.20  # batting slot fallback — not an ERA value, keep as-is
# FIX: Corrected using true avg 5.2 IP × 4.30 BF/IP = 22.36 total starter BF
# Distributed by slot: slot 1 sees starter ~3.1 PA, slot 9 ~2.4 PA
_STARTER_PA_BY_SLOT: Dict[int, float] = {
    1: 3.10, 2: 3.05, 3: 2.95, 4: 2.90,
    5: 2.82, 6: 2.73, 7: 2.65, 8: 2.56, 9: 2.43,
}

# Total bases distribution conditional on a hit (1B/2B/3B/HR weights)
# Used to sample TB value when a hit occurs
_TB_WEIGHTS = [0.668, 0.191, 0.015, 0.126]   # 1B, 2B, 3B, HR — FIX: updated to 2024 MLB hit distribution
_TB_VALUES  = [1,     2,     3,     4]


# ─── Result type ───────────────────────────────────────────────────────────────

@dataclass
class SimResult:
    prob_over:    float                       # P(outcome >= line)
    prob_under:   float                       # P(outcome <  line)
    mean:         float                       # expected outcome value
    std:          float                       # standard deviation (variance signal)
    dist:         Dict[str, float]            # full histogram {"0": p, "1": p, "2+": p, ...}
    edge_reasons: List[str] = field(default_factory=list)   # structural edge tags
    starter_prob: Optional[float] = None     # P(over) vs starter phase only
    bullpen_prob: Optional[float] = None     # P(over) vs bullpen phase only
    prop:         Optional[dict]  = None     # reference to source prop (for Bernoulli Drama penalty)

def _clamp(v: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, float(v or 0.0)))


def _safe(prop: dict, key: str, default: float) -> float:
    """Pull a numeric key from prop, return default if missing/None/0."""
    val = prop.get(key)
    try:
        f = float(val)
        return f if not math.isnan(f) else default
    except (TypeError, ValueError):
        return default


def _sample_pa(mean_pa: float) -> int:
    """Sample PA count from a Poisson distribution centred on mean_pa."""
    # Using Knuth's algorithm for small lambda
    lam = max(1.0, mean_pa)
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while p > L:
        k += 1
        p *= random.random()
    return max(1, k - 1)


def _get_pa_mean(prop: dict) -> float:
    """Return expected PA count adjusted for lineup slot and team run environment."""
    slot = int(_safe(prop, "_batting_order_slot", 0))
    base_pa = _PA_BY_SLOT.get(slot, _PA_UNKNOWN_SLOT)

    # Adjust by team implied total (shifts opportunity up/down)
    team_total = _safe(prop, "_team_implied_total", _LG_TEAM_TOTAL)
    if team_total <= 0:
        team_total = _LG_TEAM_TOTAL
    run_factor = team_total / _LG_TEAM_TOTAL       # 1.0 = average
    run_factor = _clamp(run_factor, 0.70, 1.35)    # cap extremes

    return base_pa * run_factor


def _get_starter_pa_fraction(prop: dict, mean_pa: float) -> float:
    """Return expected PA that fall in the starter phase."""
    slot = int(_safe(prop, "_batting_order_slot", 0))
    starter_pa = _STARTER_PA_BY_SLOT.get(slot, 3.10)
    return _clamp(min(starter_pa, mean_pa * 0.85), 0.0, mean_pa)


# ─── Hit probability builders ─────────────────────────────────────────────────

def _hit_prob_per_pa_vs_starter(prop: dict) -> float:
    """P(hit) per plate appearance against the starting pitcher."""
    # Pitcher skill inputs
    k_pct   = _safe(prop, "_k_pct",  0.222)   # FG 2025: 22.2% pitcher K% (was 0.225)
    bb_pct  = _safe(prop, "_bb_pct", 0.080)
    whip    = _safe(prop, "_whip",   1.30)

    # Batter skill inputs
    woba    = _safe(prop, "_woba",     0.308)  # FG 2025: wOBA actual (was 0.312)
    wrc_p   = _safe(prop, "_wrc_plus", 100.0)
    whiff_h = _safe(prop, "_pitch_whiff_vs_hand", 0.25)

    # Base hit rate adjusted by pitcher
    pitcher_hit_factor = _clamp(1.30 / max(whip, 0.50), 0.70, 1.40)

    # Batter wOBA adjustment: 0.312 = average (2024 MLB); scale ±20%
    batter_hit_factor  = _clamp(woba / 0.308, 0.70, 1.45)   # FG 2025: center 0.308

    # Whiff rate: higher whiff → more Ks → fewer balls in play
    whiff_factor = _clamp(1.0 - (whiff_h - 0.25) * 1.5, 0.60, 1.20)

    p_hit = _LG_HIT_RATE * pitcher_hit_factor * batter_hit_factor * whiff_factor
    return _clamp(p_hit, 0.08, 0.45)


def _hit_prob_per_pa_vs_bullpen(prop: dict) -> float:
    """P(hit) per plate appearance against bullpen.
    Bullpen quality shifts the probability relative to starter.
    Elite bullpen (ERA 2.5) → fewer hits; weak bullpen (ERA 5.5) → more.
    """
    bp_era    = _safe(prop, "_bullpen_era", _LG_BULLPEN_ERA)
    batter_hit_factor = _clamp(_safe(prop, "_woba", 0.308) / 0.308, 0.70, 1.45)  # FG 2025

    # Bullpen ERA factor: 4.1 = average; below = good bullpen (fewer hits), above = weak
    era_factor = _clamp(bp_era / _LG_BULLPEN_ERA, 0.70, 1.50)

    p_hit = _LG_HIT_RATE * era_factor * batter_hit_factor
    return _clamp(p_hit, 0.08, 0.45)


def _hr_prob_per_pa(prop: dict, phase: str = "starter") -> float:
    """P(home run) per plate appearance. Phase = 'starter' or 'bullpen'."""
    iso     = _safe(prop, "_iso",    0.160)   # FG 2025: ISO 0.160 (was 0.150)
    temp    = _safe(prop, "_temp_f", 72.0)
    wind    = _safe(prop, "_wind_speed", 5.0)
    is_dome = bool(prop.get("_is_dome"))

    iso_factor  = _clamp(iso / 0.160, 0.30, 2.20)   # FG 2025: normalized to 0.160
    temp_factor = _clamp(1.0 + (temp - 72.0) * 0.003, 0.85, 1.20)  # +3% per 10°F above 72
    wind_factor = 1.0
    if not is_dome and wind > 10:
        wind_factor = _clamp(1.0 + (wind - 10) * 0.005, 1.0, 1.25)

    bp_factor = 1.0
    if phase == "bullpen":
        bp_era = _safe(prop, "_bullpen_era", _LG_BULLPEN_ERA)
        bp_factor = _clamp(bp_era / _LG_BULLPEN_ERA, 0.70, 1.50)

    return _clamp(_LG_HR_RATE * iso_factor * temp_factor * wind_factor * bp_factor,
                  0.005, 0.12)


def _k_prob_per_pa_vs_starter(prop: dict) -> float:
    """P(strikeout) per plate appearance for the BATTER against starter."""
    k_pct   = _safe(prop, "_k_pct",  0.222)  # FG 2025
    csw     = _safe(prop, "_csw_pct", 0.28)
    o_swing = _safe(prop, "_o_swing", 0.316)  # FG 2025: O-Swing actual (was 0.318)

    # Pitcher K% is the dominant signal
    k_factor = _clamp(k_pct / 0.225, 0.50, 1.80)
    # CSW (called + whiff) adds information
    csw_factor = _clamp(csw / 0.280, 0.80, 1.30)

    return _clamp(_LG_K_RATE * k_factor * csw_factor, 0.05, 0.50)


# ─── Prop-type simulators ─────────────────────────────────────────────────────

def _simulate_hitter_hits(prop: dict, line: float, n_sims: int) -> SimResult:
    """Monte Carlo simulation for batter hits props."""
    mean_pa       = _get_pa_mean(prop)
    starter_pa    = _get_starter_pa_fraction(prop, mean_pa)

    p_hit_start   = _hit_prob_per_pa_vs_starter(prop)
    p_hit_bull    = _hit_prob_per_pa_vs_bullpen(prop)

    # Weather — variance signal (doesn't shift mean much, shifts std dev)
    wind    = _safe(prop, "_wind_speed", 5.0)
    temp    = _safe(prop, "_temp_f", 72.0)
    is_dome = bool(prop.get("_is_dome"))
    if not is_dome:
        # Extreme conditions add ~5-8% std dev without changing mean
        weather_var_boost = _clamp(1.0 + max(0.0, wind - 10) * 0.008 + max(0.0, temp - 85) * 0.003,
                                   1.0, 1.15)
    else:
        weather_var_boost = 1.0

    hit_counts = []
    for _ in range(n_sims):
        total_pa = _sample_pa(mean_pa)
        # Determine starter PA vs bullpen PA for this sim
        s_pa = min(round(starter_pa + random.gauss(0, 0.5)), total_pa)
        s_pa = max(0, s_pa)
        b_pa = total_pa - s_pa

        hits = 0
        # PA vs starter
        for _ in range(s_pa):
            if random.random() < p_hit_start:
                hits += 1
        # PA vs bullpen
        for _ in range(b_pa):
            if random.random() < p_hit_bull * weather_var_boost:
                hits += 1

        hit_counts.append(hits)

    return _build_result(hit_counts, line,
                         starter_prob_hint=_hits_phase_prob(starter_pa, p_hit_start, line),
                         bullpen_prob_hint=_hits_phase_prob(mean_pa - starter_pa, p_hit_bull, line),
                         prop=prop)


def _hits_phase_prob(pa: float, p_hit: float, line: float) -> float:
    """Approximate P(hits >= line) for a single-phase PA block via Poisson."""
    lam = max(0.01, pa * p_hit)
    target = int(math.ceil(line))
    # P(X >= target) = 1 - P(X <= target-1)
    cum = 0.0
    for k in range(target):
        cum += (lam**k * math.exp(-lam)) / math.factorial(k)
    return _clamp(1.0 - cum)


def _simulate_pitcher_strikeouts(prop: dict, line: float, n_sims: int) -> SimResult:
    """Monte Carlo simulation for pitcher Ks prop."""
    # Pitcher faces ~27 outs * (1/(1-k_pct-bb_pct)) batters per 9 innings
    k_pct   = _safe(prop, "_k_pct",  0.222)  # FG 2025
    bb_pct  = _safe(prop, "_bb_pct", 0.080)
    ip_mean = _safe(prop, "_starter_ip_projection", _LG_STARTER_IP)  # optional; may be 0
    if ip_mean < 1.0:
        ip_mean = _LG_STARTER_IP

    # Batters faced per inning: MLB average ~4.30 (FanGraphs 2022-2024 starters)
    # BF/IP = 3.0 + 3.0 * (bb_pct + BABIP + HBP_rate)
    # This yields 4.29 at avg bb_pct=0.08, matching MLB observed 4.30 BF/IP
    bf_per_ip = 3.0 + 3.0 * (bb_pct + 0.35)
    mean_bf   = ip_mean * bf_per_ip

    # K rate adjusted by CSW and platoon whiff
    csw_pct = _safe(prop, "_csw_pct", 0.280)
    whiff_h = _safe(prop, "_pitch_whiff_vs_hand", 0.25)
    csw_factor   = _clamp(csw_pct / 0.280, 0.80, 1.30)
    whiff_factor = _clamp(whiff_h / 0.250, 0.85, 1.25)
    effective_k_pct = _clamp(k_pct * csw_factor * whiff_factor, 0.05, 0.55)

    k_counts = []
    for _ in range(n_sims):
        bf = _sample_pa(mean_bf)
        ks = sum(1 for _ in range(bf) if random.random() < effective_k_pct)
        k_counts.append(ks)

    return _build_result(k_counts, line, prop=prop)


def _simulate_hitter_total_bases(prop: dict, line: float, n_sims: int) -> SimResult:
    """Monte Carlo simulation for total bases props.
    Separates HR (4 TB) from singles (1), doubles (2), triples (3).
    This is important — TB lines are sensitive to HR variance.
    """
    mean_pa    = _get_pa_mean(prop)
    starter_pa = _get_starter_pa_fraction(prop, mean_pa)

    p_hit_s  = _hit_prob_per_pa_vs_starter(prop)
    p_hit_b  = _hit_prob_per_pa_vs_bullpen(prop)
    p_hr_s   = _hr_prob_per_pa(prop, "starter")
    p_hr_b   = _hr_prob_per_pa(prop, "bullpen")

    tb_totals = []
    for _ in range(n_sims):
        total_pa = _sample_pa(mean_pa)
        s_pa     = min(round(starter_pa + random.gauss(0, 0.5)), total_pa)
        s_pa     = max(0, s_pa)
        b_pa     = total_pa - s_pa

        tb = 0
        for phase_pa, p_hit, p_hr in ((s_pa, p_hit_s, p_hr_s), (b_pa, p_hit_b, p_hr_b)):
            for _ in range(phase_pa):
                r = random.random()
                if r < p_hr:
                    tb += 4
                elif r < p_hit:
                    # Sample TB value (1B, 2B, 3B) — HR already handled above
                    w   = _TB_WEIGHTS[:3]
                    tot = sum(w)
                    x   = random.random() * tot
                    cum = 0.0
                    for wt, tv in zip(w, _TB_VALUES[:3]):
                        cum += wt
                        if x <= cum:
                            tb += tv
                            break
        tb_totals.append(tb)

    return _build_result(tb_totals, line, prop=prop)


def _simulate_generic(prop: dict, line: float, n_sims: int) -> SimResult:
    """Fallback simulation using Poisson draws calibrated to the prop line.
    Used for RBI, runs, ER, pitching_outs, hits_allowed, fantasy score,
    and other props where specific skill breakdown isn't modeled.
    """
    implied_p = _safe(prop, "implied_prob", 52.4) / 100.0
    nudge     = (_safe(prop, "_bayesian_nudge", 0.0) +
                 _safe(prop, "_cv_nudge",       0.0) +
                 _safe(prop, "_form_adj",        0.0))
    model_p   = _clamp(implied_p + nudge)

    # Calibrate Poisson λ so that P(X ≥ ceil(line)) ≈ model_p.
    # Binary search: find λ that matches the model-implied over probability.
    target = math.ceil(line)
    lo, hi = 0.01, max(line * 4, 5.0)
    for _ in range(60):
        mid = (lo + hi) / 2.0
        # Poisson CDF: P(X < target) = sum_{k=0}^{target-1} e^{-λ} λ^k / k!
        cdf = 0.0
        for k in range(target):
            cdf += (mid ** k * math.exp(-mid)) / math.factorial(k)
        p_over = 1.0 - cdf
        if p_over < model_p:
            lo = mid
        else:
            hi = mid
    lam = (lo + hi) / 2.0

    # Poisson draws using Knuth's algorithm (no scipy dependency)
    counts = []
    for _ in range(n_sims):
        L = math.exp(-lam)
        k = 0
        p = 1.0
        while True:
            p *= random.random()
            if p <= L:
                break
            k += 1
        counts.append(k)
    return _build_result(counts, line, prop=prop)


# ─── Result builder ───────────────────────────────────────────────────────────

def _build_result(counts: List[int], line: float,
                  prop: Optional[dict] = None,
                  starter_prob_hint: Optional[float] = None,
                  bullpen_prob_hint: Optional[float] = None) -> SimResult:
    """Convert raw sim counts into a SimResult."""
    n = len(counts)
    if n == 0:
        return SimResult(0.5, 0.5, 0.0, 0.0, {})

    mean = sum(counts) / n
    variance = sum((c - mean)**2 for c in counts) / n
    std  = math.sqrt(variance)

    over_count = sum(1 for c in counts if c >= math.ceil(line))
    prob_over  = over_count / n
    prob_under = 1.0 - prob_over

    # Build full distribution histogram
    max_val = min(max(counts), 15)
    dist: Dict[str, float] = {}
    for v in range(max_val + 1):
        p = sum(1 for c in counts if c == v) / n
        if p > 0.001:
            dist[str(v)] = round(p, 4)
    tail = sum(1 for c in counts if c > max_val) / n
    if tail > 0.001:
        dist[f"{max_val + 1}+"] = round(tail, 4)

    # Structural reason tagging
    reasons: List[str] = []
    if prop:
        slot    = int(_safe(prop, "_batting_order_slot", 0))
        bp_era  = _safe(prop, "_bullpen_era", _LG_BULLPEN_ERA)
        wind    = _safe(prop, "_wind_speed", 5.0)
        k_pct   = _safe(prop, "_k_pct", 0.225)
        is_dome = bool(prop.get("_is_dome"))

        if slot in (1, 2, 3):
            reasons.append("lineup_slot_top3")
        if bp_era > 4.80:
            reasons.append("weak_bullpen")
        elif bp_era < 3.50:
            reasons.append("elite_bullpen")
        if not is_dome and wind > 12:
            reasons.append("wind_jump")
        if k_pct > 0.28:
            reasons.append("high_k_pitcher")
        elif k_pct < 0.17:
            reasons.append("low_k_pitcher")
        if prop.get("_lineup_difficulty") == "HIGH":
            reasons.append("high_lineup_chase")
        form_adj = _safe(prop, "_form_adj", 0.0)
        if abs(form_adj) > 0.04:
            reasons.append("strong_form_signal")

        # Bernoulli structural signals
        b_tier = prop.get("_bernoulli_tier")
        b_melt = float(prop.get("_bernoulli_meltdown", 0.0) or 0.0)
        b_drama = float(prop.get("_bernoulli_drama", 0.0) or 0.0)
        if b_tier == "S":
            reasons.append("bernoulli_s_tier")
        elif b_tier == "A":
            reasons.append("bernoulli_a_tier")
        elif b_tier == "D":
            reasons.append("bernoulli_d_tier")
        if b_melt > 8.0:
            reasons.append("bernoulli_meltdown")
        if b_drama > 35.0:
            reasons.append("bernoulli_high_drama")

    return SimResult(
        prob_over=round(prob_over, 4),
        prob_under=round(prob_under, 4),
        mean=round(mean, 3),
        std=round(std, 3),
        dist=dist,
        edge_reasons=reasons,
        starter_prob=round(starter_prob_hint, 4) if starter_prob_hint is not None else None,
        bullpen_prob=round(bullpen_prob_hint, 4) if bullpen_prob_hint is not None else None,
        prop=prop,   # store prop ref so variance_penalty can read Bernoulli Drama%
    )


# ─── Variance-aware confidence calibration ────────────────────────────────────

def variance_penalty(result: SimResult) -> float:
    """Return a confidence reduction factor based on outcome variance.

    For PITCHER props: uses the Bernoulli Drama% as the primary variance signal.
    Drama% measures combinatorial entropy of the pitcher's IP/DivR line —
    verified to match Murray2061/Bernoullis-on-the-Mound outputs exactly.
    Drama 0% (pure shutout) = penalty 1.00; Drama 35%+ = penalty 0.70.

    For BATTER props: uses coefficient of variation from Monte Carlo std/mean.
    cv ~ 0.5 = normal; cv ~ 1.0+ = high variance prop.

    Returns a multiplier in [0.70, 1.00]:
        1.00 = no penalty (tight distribution / dominant pitcher line)
        0.70 = maximum penalty (very wide distribution / high-drama pitcher)
    """
    # Use Bernoulli Drama penalty if available (pitcher props)
    bernoulli_penalty = result.prop.get("_bernoulli_drama_penalty") if result.prop else None
    if bernoulli_penalty is not None:
        return round(float(bernoulli_penalty), 4)

    # Fallback: coefficient of variation from Monte Carlo distribution
    if result.mean <= 0:
        return 0.85
    cv = result.std / result.mean
    # cv ~ 0.5 = normal; cv ~ 1.0+ = high variance prop
    penalty = _clamp(1.0 - max(0.0, cv - 0.5) * 0.30, 0.70, 1.00)
    return round(penalty, 4)


# ─── Main entrypoint ──────────────────────────────────────────────────────────

def simulate_prop(prop: dict, n_sims: int = 10_000) -> SimResult:
    """Simulate a prop and return full outcome distribution.

    Args:
        prop:   Enriched prop dict from prop_enrichment_layer.enrich_props().
                Must contain at minimum: prop_type, line, implied_prob.
        n_sims: Number of Monte Carlo iterations (default 10k).

    Returns:
        SimResult with prob_over, prob_under, mean, std, dist, edge_reasons.
    """
    prop_type = str(prop.get("prop_type") or "").lower().replace(" ", "_")
    line      = float(prop.get("line") or 1.5)

    try:
        if prop_type in ("hits", "hit", "h"):
            return _simulate_hitter_hits(prop, line, n_sims)

        if prop_type in ("total_bases", "tb", "total_base"):
            return _simulate_hitter_total_bases(prop, line, n_sims)

        if "strikeout" in prop_type and ("pitcher" in prop_type or
                                          prop.get("_is_pitcher_prop")):
            return _simulate_pitcher_strikeouts(prop, line, n_sims)

        # Strikeouts on a batter prop (e.g. batter K rate) → use hitter model
        if "strikeout" in prop_type:
            # Batter K prop: simulate PA and count Ks
            mean_pa  = _get_pa_mean(prop)
            p_k_s    = _k_prob_per_pa_vs_starter(prop)
            # Bullpen Ks ≈ slightly lower (fresher arms, but less starter-level K%)
            p_k_b    = _clamp(p_k_s * 0.90, 0.05, 0.45)
            starter_pa = _get_starter_pa_fraction(prop, mean_pa)
            k_counts = []
            for _ in range(n_sims):
                total_pa = _sample_pa(mean_pa)
                s_pa = min(round(starter_pa + random.gauss(0, 0.5)), total_pa)
                s_pa = max(0, s_pa)
                b_pa = total_pa - s_pa
                ks = sum(1 for _ in range(s_pa) if random.random() < p_k_s)
                ks += sum(1 for _ in range(b_pa) if random.random() < p_k_b)
                k_counts.append(ks)
            return _build_result(k_counts, line, prop=prop)

        # Generic fallback
        return _simulate_generic(prop, line, n_sims)

    except Exception as exc:  # pragma: no cover
        logger.warning("[SimEngine] Error simulating %s %s: %s",
                       prop.get("player", "?"), prop_type, exc)
        # Safe fallback — return market-implied probability, no distribution
        ip = _safe(prop, "implied_prob", 52.4) / 100.0
        return SimResult(
            prob_over=round(ip, 4),
            prob_under=round(1.0 - ip, 4),
            mean=round(line * ip, 3),
            std=0.0,
            dist={},
            edge_reasons=["fallback"],
        )


# ─── Convenience: team-implied total injection ────────────────────────────────

def inject_team_total(prop: dict, hub: dict) -> None:
    """Write _team_implied_total onto prop from hub game totals (if available).

    Books anchor player lines to team totals.  This function reads the
    game O/U and team moneyline to approximate the team's implied run total,
    then writes it onto the prop so the PA distribution model can use it.

    Call this BEFORE simulate_prop().
    """
    team = str(prop.get("team") or "").upper()
    games = hub.get("games") or hub.get("context", {}).get("games", [])

    for game in games:
        home = str(game.get("home_team") or "").upper()
        away = str(game.get("away_team") or "").upper()
        if team not in (home, away):
            continue

        ou = game.get("over_under") or game.get("total") or game.get("ou_line")
        if not ou:
            continue

        try:
            total = float(ou)
        except (TypeError, ValueError):
            continue

        # Split total by run line or equal split as fallback
        home_ml = game.get("home_moneyline")
        away_ml = game.get("away_moneyline")
        if home_ml and away_ml:
            try:
                hml = int(home_ml)
                # Correct implied probability:
                # Positive ML (underdog):  100 / (ml + 100)
                home_win = (abs(hml) / (abs(hml) + 100)) if hml < 0 else (100 / (hml + 100))
                away_win = 1.0 - home_win
                home_share = _clamp(home_win * 0.55 + 0.225)  # favourite scores more
                away_share = 1.0 - home_share
                prop["_team_implied_total"] = total * (home_share if team == home else away_share)
            except Exception:
                prop["_team_implied_total"] = total / 2.0
        else:
            prop["_team_implied_total"] = total / 2.0

        return

    # No game found — leave unset (simulation uses _LG_TEAM_TOTAL default)

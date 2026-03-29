"""
generate_pick.py — Master entry point for the PropIQ pipeline.

Railway calls generate_pick() for every prop from Underdog / PrizePicks.
It runs the full 5-stage pipeline and returns a pick dict with final_prob
and edge, or None if no edge exists.

5-Stage Pipeline
----------------
Stage 1: Normalize stat type          (_norm_stat)
Stage 2: Zone integrity fraud detect  (apply_zone_integrity_multiplier)
Stage 3: Lineup chase analysis        (get_lineup_chase_score)
Stage 4: Adaptive weighting           (integrity × chase → model_prob)
Stage 5: Calibrated probability       (compute_unified_probability)

Minimum qualifying edge: MIN_EDGE (default 4 pp over market implied).
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────

MIN_EDGE: float = 0.04           # 4 pp minimum EV over market implied
BREAK_EVEN: float = 0.5336       # -115 standard UD line break-even
MAX_PROB: float = 0.82           # hard ceiling — prevents over-confidence
MIN_PROB: float = 0.30           # hard floor

# Stat types where zone integrity / shadow whiff matters (pitcher K-props only)
_PITCHER_K_STATS = {"strikeouts", "pitcher_strikeouts", "k", "ks", "strikeouts_pitcher"}

# ──────────────────────────────────────────────────────────────────────────────
# Graceful imports
# ──────────────────────────────────────────────────────────────────────────────

try:
    from calibration_layer import (
        _norm_stat,
        apply_zone_integrity_multiplier,
        compute_unified_probability,
        _prob_to_confidence_label,
        sniper_decision_gate,
    )
    _CAL_OK = True
except ImportError:
    _CAL_OK = False
    def _norm_stat(s: str) -> str:                                      # type: ignore[misc]
        return (s or "").lower().replace(" ", "_")
    def apply_zone_integrity_multiplier(p, pt, pid):   return p        # type: ignore[misc]
    def _prob_to_confidence_label(p):                  return "MEDIUM" # type: ignore[misc]
    def sniper_decision_gate(p, rung):                 return True, "" # type: ignore[misc]
    def compute_unified_probability(**kw):                              # type: ignore[misc]
        p = kw.get("raw_model_prob", 0.5)
        m = kw.get("market_implied", 0.535)
        return {"final_prob": p, "edge": p - m, "confidence_label": "MEDIUM",
                "shrink_factor": 0.5, "gate_status": "no_cal"}

try:
    from lineup_chase_layer import get_lineup_chase_score
    _CHASE_OK = True
except ImportError:
    _CHASE_OK = False
    def get_lineup_chase_score(*a, **kw):               # type: ignore[misc]
        return {"k_prob_adjustment": 0.0, "o_swing_avg": 0.25,
                "is_k_target": False, "difficulty": 0.5}

try:
    from live_dispatcher import _BASE_RATES
    _BR_OK = True
except ImportError:
    _BR_OK = False
    _BASE_RATES: dict = {}  # type: ignore[assignment]

# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────

def _american_to_implied(american: int) -> float:
    """Convert American odds to implied probability (0–1)."""
    if american < 0:
        return (-american) / (-american + 100.0)
    return 100.0 / (american + 100.0)


def _base_rate_prob(stat_norm: str, line: float, side: str) -> float:
    """Historical hit-rate baseline for a stat type / line bracket.

    Returns probability as a fraction (0–1).
    Source: live_dispatcher._BASE_RATES — same table used by _model_prob.
    Falls back to 0.50 if stat type unknown.
    """
    if not _BR_OK or stat_norm not in _BASE_RATES:
        return 0.50
    rates = sorted(_BASE_RATES[stat_norm], key=lambda x: x[0])
    base = rates[0][1]
    for bkt_line, bkt_prob in rates:
        if line >= bkt_line:
            base = bkt_prob
        else:
            break
    if side.lower() == "under":
        base = 1.0 - base
    return float(base)


def _apply_enrichment_nudges(raw_prob: float, prop: dict) -> float:
    """Apply Bayesian / CV / form nudges set by prop_enrichment_layer."""
    nudge = (
        float(prop.get("_bayesian_nudge", 0.0))
        + float(prop.get("_cv_nudge",       0.0))
        + float(prop.get("_form_adj",        0.0))
    )
    return min(MAX_PROB, max(MIN_PROB, raw_prob + nudge))


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def generate_pick(
    raw_prop: dict,
    hub: Optional[dict] = None,
    side: str = "OVER",
    current_rung: int = 0,
    min_edge: float = MIN_EDGE,
) -> Optional[dict]:
    """
    Master pick generator.  Runs every Underdog / PrizePicks prop through
    the full 5-stage pipeline and returns a verified pick dict or None.

    Parameters
    ----------
    raw_prop     : Single prop dict from _get_props() (already enriched by
                   prop_enrichment_layer.enrich_props()).
    hub          : DataHub snapshot (optional — used for lineup context).
    side         : "OVER" or "UNDER" (default "OVER").
    current_rung : Current parlay leg count for sniper_decision_gate (0 = single).
    min_edge     : Minimum EV threshold (default 4 pp).

    Returns
    -------
    dict with player, stat, line, side, final_prob, edge, confidence,
    or None if the prop doesn't clear the edge threshold.
    """

    # ── Stage 1: Normalize ──────────────────────────────────────────────────
    raw_stat   = raw_prop.get("prop_type") or raw_prop.get("stat_type", "")
    stat_norm  = _norm_stat(raw_stat)
    player     = raw_prop.get("player") or raw_prop.get("player_name", "Unknown")
    line       = float(raw_prop.get("line", 1.5))
    pitch_id   = raw_prop.get("mlbam_id") or raw_prop.get("player_id")

    # Implied probability from market odds
    odds_american = (
        raw_prop.get("over_american", -115)
        if side == "OVER"
        else raw_prop.get("under_american", -115)
    )
    market_implied = _american_to_implied(int(odds_american))

    # ── Stage 2: Zone Integrity Fraud Detector ──────────────────────────────
    # Reads heart-vs-shadow whiff rates from statcast cache.
    # FRAUD (heart zone dominance) → multiplier < 1 → compresses probability.
    # ELITE_SHADOW dominance → multiplier > 1 → boosts probability.
    # Non-K props return multiplier = 1.0 (no-op).
    raw_base_prob = _base_rate_prob(stat_norm, line, side)

    # Convert to percentage for apply_zone_integrity_multiplier (expects 0–100)
    integrity_prob_pct = apply_zone_integrity_multiplier(
        raw_base_prob * 100.0,
        stat_norm,
        pitch_id,
    )
    integrity_prob = min(MAX_PROB, max(MIN_PROB, integrity_prob_pct / 100.0))

    logger.debug(
        "[generate_pick] %s %s %.1f | zone_integrity: %.3f → %.3f",
        player, stat_norm, line, raw_base_prob, integrity_prob,
    )

    # ── Stage 3: Lineup Chase Analysis ─────────────────────────────────────
    # K-props only: adjust upward if opposing lineup chases out-of-zone pitches.
    # Non-K props: chase_adj = 0.
    chase_adj = 0.0
    is_k_prop = stat_norm in _PITCHER_K_STATS

    if is_k_prop and _CHASE_OK:
        opposing_team = raw_prop.get("opposing_team", "")
        ctx_lineups   = (hub or {}).get("lineups", []) if hub else raw_prop.get("_context_lineups", [])
        if opposing_team and ctx_lineups:
            try:
                chase = get_lineup_chase_score(opposing_team, ctx_lineups)
                chase_adj = float(chase.get("k_prob_adjustment", 0.0))
                logger.debug(
                    "[generate_pick] %s chase_adj=%.3f (o_swing=%.2f)",
                    player, chase_adj, chase.get("o_swing_avg", 0),
                )
            except Exception as exc:
                logger.debug("[generate_pick] chase_score error: %s", exc)

    # ── Stage 4: Adaptive Weighting ─────────────────────────────────────────
    # Combine zone integrity result + chase adjustment + enrichment nudges.
    adapted_prob = min(MAX_PROB, max(MIN_PROB, integrity_prob + chase_adj))
    # Apply Bayesian / CV / form nudges from prop_enrichment_layer
    adapted_prob = _apply_enrichment_nudges(adapted_prob, raw_prop)

    logger.debug(
        "[generate_pick] %s adapted_prob=%.3f (post-chase+nudges)",
        player, adapted_prob,
    )

    # ── Stage 5: Isotonic Calibration via compute_unified_probability ────────
    # Applies the full Phase 83 pipeline:
    #   isotonic calibration → trust gate → dynamic shrinkage →
    #   uncertainty penalty → Brier governor → ONE final_prob
    unified = compute_unified_probability(
        raw_model_prob=adapted_prob,
        market_implied=market_implied,
        prop=raw_prop,
    )

    final_prob       = float(unified["final_prob"])
    edge             = float(unified["edge"])
    confidence_label = str(unified["confidence_label"])
    shrink_factor    = float(unified.get("shrink_factor", 0.5))
    gate_status      = str(unified.get("gate_status", "normal"))

    # ── Sniper Decision Gate ─────────────────────────────────────────────────
    # Raises minimum threshold on later parlay rungs (post-rung 5: +2pp per leg).
    # Prevents chasing low-confidence legs deep into a parlay.
    gate_pass, gate_reason = sniper_decision_gate(final_prob, current_rung)
    if not gate_pass:
        logger.debug(
            "[generate_pick] %s rung=%d sniper REJECT: %s (final_prob=%.3f)",
            player, current_rung, gate_reason, final_prob,
        )
        return None

    # ── Final EV Check ───────────────────────────────────────────────────────
    if edge < min_edge:
        logger.debug(
            "[generate_pick] %s edge=%.3f < min=%.3f — no pick",
            player, edge, min_edge,
        )
        return None

    pick = {
        # Identity
        "player":         player,
        "player_name":    player,
        "stat":           stat_norm,
        "prop_type":      stat_norm,
        "line":           line,
        "side":           side,
        # Probability stack
        "raw_base_prob":  round(raw_base_prob,    4),
        "integrity_prob": round(integrity_prob,   4),
        "chase_adj":      round(chase_adj,        4),
        "adapted_prob":   round(adapted_prob,     4),
        "final_prob":     round(final_prob,       4),
        "market_implied": round(market_implied,   4),
        "edge":           round(edge,             4),
        # Decision metadata
        "confidence":     confidence_label,
        "shrink_factor":  round(shrink_factor,    3),
        "gate_status":    gate_status,
        "is_k_prop":      is_k_prop,
        # Market
        "odds_american":  odds_american,
        # Recommendation
        "recommendation": "HIGHER" if side == "OVER" else "LOWER",
    }

    logger.info(
        "[generate_pick] ✅ %s | %s %.1f %s | final_prob=%.3f edge=%.3f conf=%s",
        player, stat_norm, line, side,
        final_prob, edge, confidence_label,
    )
    return pick


def generate_picks_batch(
    props: list[dict],
    hub: Optional[dict] = None,
    min_edge: float = MIN_EDGE,
) -> list[dict]:
    """
    Run generate_pick() over an entire prop list.

    Evaluates both OVER and UNDER for every prop, applies rung-aware
    sniper gate per qualifying pick, and returns all picks sorted by
    edge descending.

    Parameters
    ----------
    props    : Enriched prop list from prop_enrichment_layer.enrich_props().
    hub      : DataHub snapshot.
    min_edge : Minimum EV threshold.

    Returns
    -------
    List of pick dicts, sorted by edge descending.
    """
    picks: list[dict] = []
    rung = 0

    for prop in props:
        for side in ("OVER", "UNDER"):
            pick = generate_pick(
                raw_prop=prop,
                hub=hub,
                side=side,
                current_rung=rung,
                min_edge=min_edge,
            )
            if pick:
                picks.append(pick)
                rung += 1

    return sorted(picks, key=lambda p: p["edge"], reverse=True)

"""
settlement_engine.py
====================
Parlay settlement logic for PropIQ nightly recap.

Matches parlay legs against ESPN box-score stats and determines
WIN / LOSS / PUSH outcome for each leg and the overall parlay.

Public API
----------
    settle_parlay(parlay_id, agent_name, date, stake, legs_data, player_stats)
        → ParlayResult

    ParlayResult.outcome:      'WIN' | 'LOSS' | 'PUSH'
    ParlayResult.units_profit: signed float (positive = profit)
    ParlayResult.legs:         list[LegResult]
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

_DEFAULT_PAYOUT_MULTIPLIER = 2.0   # 2× on winning FLEX parlay


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class LegResult:
    """Outcome for a single parlay leg."""

    player_name: str
    prop_type:   str
    side:        str    # 'over' | 'under'
    line:        float
    actual:      float  # actual stat from ESPN; -1.0 if unavailable
    outcome:     str    # 'WIN' | 'LOSS' | 'PUSH'


@dataclass
class ParlayResult:
    """Outcome for a complete parlay."""

    parlay_id:    int
    agent_name:   str
    outcome:      str    # 'WIN' | 'LOSS' | 'PUSH'
    units_profit: float
    legs:         list[LegResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Player name matching
# ---------------------------------------------------------------------------

def _name_match(a: str, b: str) -> bool:
    """
    Fuzzy player name match.

    Tries exact match first, then last-name + first-initial fallback to
    handle ESPN vs PrizePicks/Underdog name discrepancies.
    """
    a = a.lower().strip()
    b = b.lower().strip()
    if a == b:
        return True
    a_parts = a.split()
    b_parts = b.split()
    if a_parts and b_parts:
        return a_parts[-1] == b_parts[-1] and a_parts[0][0] == b_parts[0][0]
    return False


# ---------------------------------------------------------------------------
# Prop-type → ESPN stats key mapping
# ---------------------------------------------------------------------------

_PROP_STAT_KEY: dict[str, str | None] = {
    "hits":           "hits",
    "rbis":           "rbis",
    "runs":           "runs",
    "total_bases":    "total_bases",
    "hits_runs_rbis": "hits_runs_rbis",
    "strikeouts":     "strikeouts",
    "earned_runs":    "earned_runs",
    "pitching_outs":  "outs_pitched",
    "hits_allowed":   "hits_allowed",
    "fantasy_hitter": None,     # fantasy points — push (no single stat key)
    "fantasy_pitcher": None,
}


# ---------------------------------------------------------------------------
# Single-leg settlement
# ---------------------------------------------------------------------------

def settle_leg(leg: dict, player_stats: dict[str, dict]) -> LegResult:
    """
    Settle one parlay leg against ESPN box-score stats.

    Falls back to PUSH if the player cannot be matched or the prop type
    is not directly observable (e.g. fantasy points).
    """
    pname     = (leg.get("player_name") or "").strip()
    prop_type = (leg.get("prop_type")   or "").lower().strip()
    side      = (leg.get("side")        or "over").lower().strip()
    line      = float(leg.get("line") or 0)

    # Locate player in ESPN stats
    matched_stats: dict | None = None
    for stats_name, stats_dict in player_stats.items():
        if _name_match(pname, stats_name):
            matched_stats = stats_dict
            break

    if matched_stats is None:
        logger.debug("[Settlement] No ESPN match for '%s' — PUSH", pname)
        return LegResult(
            player_name=pname, prop_type=prop_type,
            side=side, line=line, actual=-1.0, outcome="PUSH",
        )

    # Map prop type to stat key
    stat_key = _PROP_STAT_KEY.get(prop_type)
    if not stat_key:
        # Unsupported prop (fantasy points, etc.) → PUSH
        return LegResult(
            player_name=pname, prop_type=prop_type,
            side=side, line=line, actual=-1.0, outcome="PUSH",
        )

    actual = float(matched_stats.get(stat_key) or 0.0)

    # Determine outcome
    if side == "over":
        if actual > line:
            outcome = "WIN"
        elif actual == line:
            outcome = "PUSH"
        else:
            outcome = "LOSS"
    else:  # under
        if actual < line:
            outcome = "WIN"
        elif actual == line:
            outcome = "PUSH"
        else:
            outcome = "LOSS"

    logger.debug(
        "[Settlement] %s %s %s %.1f — actual %.1f → %s",
        pname, prop_type, side, line, actual, outcome,
    )
    return LegResult(
        player_name=pname, prop_type=prop_type,
        side=side, line=line, actual=actual, outcome=outcome,
    )


# ---------------------------------------------------------------------------
# Full-parlay settlement
# ---------------------------------------------------------------------------

def settle_parlay(
    parlay_id:    int,
    agent_name:   str,
    stake:        float,
    legs_data:    list[dict],
    player_stats: dict[str, dict],
) -> ParlayResult:
    """
    Settle a complete parlay.

    Rules:
      - Any LOSS = parlay LOSS, units_profit = -stake
      - All PUSH = parlay PUSH, units_profit = 0
      - All non-push legs WIN = parlay WIN,
            units_profit = stake × _DEFAULT_PAYOUT_MULTIPLIER - stake
      - Mixed WIN/PUSH (no losses) = parlay WIN on the winning legs
    """
    if not legs_data:
        return ParlayResult(
            parlay_id=parlay_id, agent_name=agent_name,
            outcome="PUSH", units_profit=0.0,
        )

    leg_results = [settle_leg(leg, player_stats) for leg in legs_data]

    wins   = sum(1 for lr in leg_results if lr.outcome == "WIN")
    losses = sum(1 for lr in leg_results if lr.outcome == "LOSS")
    pushes = sum(1 for lr in leg_results if lr.outcome == "PUSH")

    if losses > 0:
        outcome      = "LOSS"
        units_profit = -stake
    elif wins == 0 and pushes == len(leg_results):
        outcome      = "PUSH"
        units_profit = 0.0
    else:
        # At least one WIN, no losses
        outcome      = "WIN"
        units_profit = stake * _DEFAULT_PAYOUT_MULTIPLIER - stake

    return ParlayResult(
        parlay_id=parlay_id,
        agent_name=agent_name,
        outcome=outcome,
        units_profit=round(units_profit, 2),
        legs=leg_results,
    )

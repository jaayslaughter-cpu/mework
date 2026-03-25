"""
settlement_engine.py
====================
Evaluates each leg of a pending parlay against actual ESPN player stats
and determines parlay outcome: WIN | LOSS | PUSH.

Prop type → ESPN stat mapping:
  strikeouts      → so_pitched   (pitcher)
  earned_runs     → er           (pitcher)
  hits            → h            (batter)
  runs            → r            (batter)
  rbis            → rbi          (batter)
  home_runs       → hr           (batter)
  total_bases     → tb           (batter)
  stolen_bases    → sb           (batter)
  walks           → bb           (batter)  or bb_allowed (pitcher context)
  hits_runs_rbis  → h + r + rbi  (batter combo)
  fantasy_hitter  → PUSH (can't settle from ESPN)
  fantasy_pitcher → PUSH (can't settle from ESPN)

Leg settlement rules:
  Over  line X: actual > X  → WIN,  actual == X → PUSH,  actual < X → LOSS
  Under line X: actual < X  → WIN,  actual == X → PUSH,  actual > X → LOSS

Parlay settlement rules (all legs must WIN for parlay to WIN):
  - Any LOSS           → parlay = LOSS
  - All WIN            → parlay = WIN
  - Mix of WIN + PUSH  → parlay = WIN (push legs are treated as removed)
  - All PUSH           → parlay = PUSH
"""

from __future__ import annotations

import difflib
import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# Prop types that cannot be settled from ESPN (mark as PUSH)
_UNSETTLEABLE_PROPS = {"fantasy_hitter", "fantasy_pitcher"}

# Prop type → (stat_key, player_role)
# player_role: "batter" or "pitcher"
_PROP_STAT_MAP: dict[str, tuple[str, str]] = {
    "strikeouts":     ("so_pitched",  "pitcher"),
    "earned_runs":    ("er",          "pitcher"),
    "hits":           ("h",           "batter"),
    "runs":           ("r",           "batter"),
    "rbis":           ("rbi",         "batter"),
    "home_runs":      ("hr",          "batter"),
    "total_bases":    ("tb",          "batter"),
    "stolen_bases":   ("sb",          "batter"),
    "walks":          ("bb",          "batter"),
    "hits_runs_rbis": ("hits_runs_rbis", "batter"),  # special combo
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class LegResult:
    player_name: str
    prop_type:   str
    side:        str    # "Over" | "Under"
    line:        float
    actual:      float  # -1.0 if stat not found
    outcome:     str    # "WIN" | "LOSS" | "PUSH" | "UNSETTLED"


@dataclass
class ParlayResult:
    parlay_id:    int
    agent_name:   str
    date:         str
    outcome:      str            # "WIN" | "LOSS" | "PUSH"
    units_profit: float
    stake:        float
    legs:         list[LegResult]
    payout_mult:  float          # combined decimal odds


# ---------------------------------------------------------------------------
# Name matching
# ---------------------------------------------------------------------------

def _fuzzy_match(name: str, candidates: list[str], cutoff: float = 0.75) -> Optional[str]:
    """Return best fuzzy match for name in candidates, or None."""
    name_norm = name.strip().lower()
    if name_norm in candidates:
        return name_norm

    matches = difflib.get_close_matches(name_norm, candidates, n=1, cutoff=cutoff)
    if matches:
        logger.debug("Fuzzy match: '%s' → '%s'", name_norm, matches[0])
        return matches[0]

    # Try last name match
    last = name_norm.split()[-1] if name_norm else ""
    last_matches = [c for c in candidates if c.split()[-1] == last] if last else []
    if len(last_matches) == 1:
        logger.debug("Last-name match: '%s' → '%s'", name_norm, last_matches[0])
        return last_matches[0]

    return None


# ---------------------------------------------------------------------------
# Leg settlement
# ---------------------------------------------------------------------------

def settle_leg(
    player_name: str,
    prop_type: str,
    side: str,
    line: float,
    player_stats: dict[str, dict],
) -> LegResult:
    """Settle a single parlay leg against ESPN stats."""

    # Unsettleable prop types
    if prop_type in _UNSETTLEABLE_PROPS:
        return LegResult(
            player_name=player_name, prop_type=prop_type, side=side, line=line,
            actual=-1.0, outcome="PUSH",
        )

    # Unknown prop type
    if prop_type not in _PROP_STAT_MAP:
        logger.warning("Unknown prop_type '%s' for %s — marking UNSETTLED", prop_type, player_name)
        return LegResult(
            player_name=player_name, prop_type=prop_type, side=side, line=line,
            actual=-1.0, outcome="UNSETTLED",
        )

    stat_key, _role = _PROP_STAT_MAP[prop_type]

    # Find player in stats dict (fuzzy match)
    candidates = list(player_stats.keys())
    matched_name = _fuzzy_match(player_name, candidates)
    if matched_name is None:
        logger.warning(
            "ESPN: player '%s' not found in box scores — marking UNSETTLED", player_name
        )
        return LegResult(
            player_name=player_name, prop_type=prop_type, side=side, line=line,
            actual=-1.0, outcome="UNSETTLED",
        )

    stats = player_stats[matched_name]

    # Get actual value
    if stat_key == "hits_runs_rbis":
        h   = stats.get("h",   -1)
        r   = stats.get("r",   -1)
        rbi = stats.get("rbi", -1)
        if any(v < 0 for v in [h, r, rbi]):
            actual = -1.0
        else:
            actual = float(h + r + rbi)
    else:
        actual = float(stats.get(stat_key, -1))

    if actual < 0:
        logger.warning(
            "ESPN: stat '%s' unavailable for '%s' — marking UNSETTLED",
            stat_key, matched_name,
        )
        return LegResult(
            player_name=player_name, prop_type=prop_type, side=side, line=line,
            actual=actual, outcome="UNSETTLED",
        )

    # Evaluate outcome
    if side.lower() == "over":
        if actual > line:
            outcome = "WIN"
        elif actual == line:
            outcome = "PUSH"
        else:
            outcome = "LOSS"
    else:  # Under
        if actual < line:
            outcome = "WIN"
        elif actual == line:
            outcome = "PUSH"
        else:
            outcome = "LOSS"

    return LegResult(
        player_name=player_name, prop_type=prop_type, side=side, line=line,
        actual=actual, outcome=outcome,
    )


# ---------------------------------------------------------------------------
# Parlay settlement
# ---------------------------------------------------------------------------

def _calc_payout_mult(num_legs: int, stake: float) -> float:
    """
    Simple DFS parlay payout estimate based on leg count.
    PrizePicks / Underdog standard payouts:
        2-leg: 3× | 3-leg: 5× | 4-leg: 10× | 5-leg: 20×
    """
    mult_map = {2: 3.0, 3: 5.0, 4: 10.0, 5: 20.0}
    return mult_map.get(num_legs, 3.0)


def settle_parlay(
    parlay_id: int,
    agent_name: str,
    date: str,
    stake: float,
    legs_data: list[dict],
    player_stats: dict[str, dict],
) -> ParlayResult:
    """
    Settle all legs and compute parlay outcome + profit.

    legs_data: list of dicts with keys: player_name, prop_type, side, line
    """
    leg_results = []
    for leg in legs_data:
        result = settle_leg(
            player_name=leg.get("player_name", ""),
            prop_type=leg.get("prop_type", ""),
            side=leg.get("side", "Under"),
            line=float(leg.get("line", 0)),
            player_stats=player_stats,
        )
        leg_results.append(result)

    # Parlay logic: UNSETTLED counts as PUSH for settlement purposes
    outcomes = [r.outcome if r.outcome != "UNSETTLED" else "PUSH" for r in leg_results]

    if "LOSS" in outcomes:
        parlay_outcome = "LOSS"
        units_profit = -1.0  # lost 1 unit (stake)
    elif all(o == "WIN" for o in outcomes):
        parlay_outcome = "WIN"
        payout_mult = _calc_payout_mult(len(leg_results), stake)
        units_profit = payout_mult - 1.0  # net profit in units
    elif all(o == "PUSH" for o in outcomes):
        parlay_outcome = "PUSH"
        units_profit = 0.0
    else:
        # Mix of WIN + PUSH: treat as reduced parlay WIN
        win_count = outcomes.count("WIN")
        payout_mult = _calc_payout_mult(max(win_count, 1), stake)
        parlay_outcome = "WIN"
        units_profit = payout_mult - 1.0

    payout_mult = _calc_payout_mult(len(leg_results), stake)

    return ParlayResult(
        parlay_id=parlay_id,
        agent_name=agent_name,
        date=date,
        outcome=parlay_outcome,
        units_profit=round(units_profit, 2),
        stake=stake,
        legs=leg_results,
        payout_mult=payout_mult,
    )

"""
settlement_engine.py
====================
Settles individual prop legs and full parlay slips.

Called by nightly_recap.py:
    result = settle_parlay(
        parlay_id, agent_name, date, stake, legs_data, player_stats
    )

PR #392: Recreated after accidental deletion in PR #375.
         Added fantasy_score calculation for both UD and PP platforms.
         Correct prop_type → ESPN stat key mapping throughout.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Platform payout multipliers
# ---------------------------------------------------------------------------

# Underdog PowerPlay
_UD_MULTIPLIERS: dict[int, float] = {2: 3.0, 3: 6.0, 5: 10.0}

# PrizePicks Power
_PP_MULTIPLIERS: dict[int, float] = {2: 3.0, 3: 5.0, 4: 10.0, 5: 20.0}


# ---------------------------------------------------------------------------
# prop_type → ESPN stat key
# None = computed (fantasy_score) or PUSH (unknown)
# ---------------------------------------------------------------------------

_PROP_TO_ESPN_STAT: dict[str, Optional[str]] = {
    # Batter props
    "hits":              "hits",
    "total_bases":       "total_bases",
    "home_runs":         "home_runs",
    "runs":              "runs",
    "rbi":               "rbi",
    "rbis":              "rbis",
    "stolen_bases":      "stolen_bases",
    "hitter_strikeouts": "strikeouts",    # ESPN batter Ks key
    "hits_runs_rbis":    "hits_runs_rbis",
    "doubles":           "doubles",
    "triples":           "triples",
    "singles":           None,            # computed: hits - 2B - 3B - HR
    # Pitcher props
    "pitching_outs":     "pitching_outs",
    "strikeouts":        "strikeouts",    # pitcher Ks
    "earned_runs":       "earned_runs",
    "walks_allowed":     "base_on_balls", # ESPN pitcher walks key
    "innings_pitched":   "innings_pitched",
    # Composite — computed from individual stats
    "fantasy_score":     None,
}


# ---------------------------------------------------------------------------
# Fantasy score calculation (PR #392)
# ---------------------------------------------------------------------------

def _calc_fantasy_score(pstats: dict, platform: str) -> float:
    """
    Compute fantasy score from ESPN box-score stats.

    Underdog MLB Hitter:
        HR×10 + 3B×8 + 2B×5 + 1B×3 + BB×3 + HBP×3 + RBI×2 + R×2 + SB×4

    Underdog MLB Pitcher:
        IP×3 + K×3 + QS×5 + W×5 + ER×−3

    PrizePicks MLB Hitter:
        1B×3 + 2B×5 + 3B×8 + HR×10 + R×2 + RBI×2 + BB×2 + HBP×2 + SB×5

    PrizePicks MLB Pitcher:
        W×6 + QS×4 + ER×−3 + K×3 + Out×1  (Out = individual outs, not innings)
    """
    is_pitcher = pstats.get("is_pitcher", False)
    p = platform.lower()
    is_ud = "underdog" in p or p in ("ud", "underdog fantasy")

    if is_pitcher:
        outs = float(pstats.get("pitching_outs", 0.0))
        ip   = outs / 3.0
        k    = float(pstats.get("strikeouts",    0.0))
        qs   = float(pstats.get("quality_start", 0.0))
        w    = float(pstats.get("wins",          0.0))
        er   = float(pstats.get("earned_runs",   0.0))

        if is_ud:
            return round(ip * 3 + k * 3 + qs * 5 + w * 5 + er * -3, 2)
        else:  # PrizePicks
            return round(w * 6 + qs * 4 + er * -3 + k * 3 + outs * 1, 2)

    else:
        # Batter
        hits    = float(pstats.get("hits",        0.0))
        doubles = float(pstats.get("doubles",     0.0))
        triples = float(pstats.get("triples",     0.0))
        hr      = float(pstats.get("home_runs",   0.0))
        singles = max(0.0, hits - doubles - triples - hr)
        bb      = float(pstats.get("base_on_balls", 0.0))
        hbp     = float(pstats.get("hit_by_pitch", 0.0))
        rbi     = float(pstats.get("rbi",  0.0) or pstats.get("rbis", 0.0))
        r       = float(pstats.get("runs",        0.0))
        sb      = float(pstats.get("stolen_bases", 0.0))

        if is_ud:
            return round(
                hr * 10 + triples * 8 + doubles * 5 + singles * 3
                + bb * 3 + hbp * 3 + rbi * 2 + r * 2 + sb * 4,
                2,
            )
        else:  # PrizePicks
            return round(
                singles * 3 + doubles * 5 + triples * 8 + hr * 10
                + r * 2 + rbi * 2 + bb * 2 + hbp * 2 + sb * 5,
                2,
            )


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class LegResult:
    player_name: str
    prop_type:   str
    side:        str
    line:        float
    actual:      float   # −1.0 means no data (graded as PUSH)
    outcome:     str     # WIN / LOSS / PUSH


@dataclass
class ParlayResult:
    parlay_id:    int
    outcome:      str    # WIN / LOSS / PUSH
    units_profit: float
    legs: List[LegResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_actual(
    prop_type:    str,
    pstats:       dict,
    platform:     str = "",
) -> Optional[float]:
    """
    Return the actual stat value for a given prop_type from ESPN player stats.
    Returns None for truly unknown prop types (logged as WARNING → PUSH).
    """
    if prop_type == "fantasy_score":
        return _calc_fantasy_score(pstats, platform)

    if prop_type == "singles":
        hits    = float(pstats.get("hits",      0.0))
        doubles = float(pstats.get("doubles",   0.0))
        triples = float(pstats.get("triples",   0.0))
        hr      = float(pstats.get("home_runs", 0.0))
        return max(0.0, hits - doubles - triples - hr)

    stat_key = _PROP_TO_ESPN_STAT.get(prop_type)
    if stat_key is None:
        if prop_type not in _PROP_TO_ESPN_STAT:
            logger.warning(
                "[Settlement] Unknown prop_type '%s' — grading leg as PUSH", prop_type
            )
        return None

    val = pstats.get(stat_key)

    # walks_allowed special fallback chain (PR #371)
    if val is None and prop_type == "walks_allowed":
        val = pstats.get("base_on_balls", 0.0)

    return float(val) if val is not None else 0.0


def _grade_leg(actual: Optional[float], line: float, side: str) -> str:
    """Grade one leg: WIN / LOSS / PUSH."""
    if actual is None:
        return "PUSH"

    side_upper = (side or "").upper()
    is_over  = side_upper in ("OVER",  "HIGHER", "MORE", "H", "OVER/HIGHER")
    is_under = side_upper in ("UNDER", "LOWER",  "LESS", "L", "UNDER/LOWER")

    if actual == line:
        return "PUSH"
    if is_over:
        return "WIN" if actual > line else "LOSS"
    if is_under:
        return "WIN" if actual < line else "LOSS"

    logger.warning("[Settlement] Unrecognised side '%s' — grading leg as PUSH", side)
    return "PUSH"


def _detect_platform(legs_data: list) -> str:
    """Best-effort platform detection from leg data."""
    for leg in legs_data:
        p = (leg.get("platform") or "").lower()
        if p:
            return p
    return ""


def _payout_multiplier(n_legs: int, platform: str) -> float:
    """Return the payout multiplier for a winning parlay."""
    p = platform.lower()
    if "prizepicks" in p or p == "pp":
        return _PP_MULTIPLIERS.get(n_legs, 3.0)
    return _UD_MULTIPLIERS.get(n_legs, 3.0)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def settle_parlay(
    parlay_id:    int,
    agent_name:   str,
    date:         str,
    stake:        float,
    legs_data:    list,
    player_stats: dict,
) -> ParlayResult:
    """
    Settle a parlay slip.

    Parameters
    ----------
    parlay_id    : DB row id from propiq_season_record
    agent_name   : e.g. 'EVHunter'
    date         : 'YYYY-MM-DD'
    stake        : wager amount in units
    legs_data    : list of dicts — keys: player_name, prop_type, side, line, [platform]
    player_stats : dict keyed by lowercase player name → stat dict (espn_scraper output)

    Returns
    -------
    ParlayResult with .outcome, .units_profit, .legs
    """
    platform = _detect_platform(legs_data)
    leg_results: List[LegResult] = []

    for leg in legs_data:
        player_name  = (leg.get("player_name") or "").strip()
        prop_type    = (leg.get("prop_type")   or "").strip()
        side         = (leg.get("side")        or "").strip()
        line         = float(leg.get("line", 0.0))
        leg_platform = (leg.get("platform") or platform).lower()

        # Stat lookup — case-insensitive, with partial-name fallback
        pstats = player_stats.get(player_name.lower())
        if pstats is None:
            name_lower = player_name.lower()
            for k, v in player_stats.items():
                if name_lower in k or k in name_lower:
                    pstats = v
                    logger.debug(
                        "[Settlement] Fuzzy name match: '%s' → '%s'",
                        player_name, k
                    )
                    break

        if not pstats:
            logger.warning(
                "[Settlement] No ESPN stats for '%s' (parlay %s) — PUSH",
                player_name, parlay_id,
            )
            leg_results.append(LegResult(
                player_name=player_name,
                prop_type=prop_type,
                side=side,
                line=line,
                actual=-1.0,
                outcome="PUSH",
            ))
            continue

        actual  = _resolve_actual(prop_type, pstats, leg_platform)
        outcome = _grade_leg(actual, line, side)

        logger.info(
            "[Settlement] %s | %s %s %.1f → actual=%.2f → %s",
            player_name, prop_type, side, line,
            actual if actual is not None else -1.0,
            outcome,
        )

        leg_results.append(LegResult(
            player_name=player_name,
            prop_type=prop_type,
            side=side,
            line=line,
            actual=actual if actual is not None else -1.0,
            outcome=outcome,
        ))

    # ── Aggregate ────────────────────────────────────────────────────────────
    if not leg_results:
        return ParlayResult(parlay_id=parlay_id, outcome="PUSH", units_profit=0.0, legs=[])

    outcomes = [lr.outcome for lr in leg_results]

    if any(o == "LOSS" for o in outcomes):
        parlay_outcome = "LOSS"
        units_profit   = -stake
    elif all(o == "WIN" for o in outcomes):
        parlay_outcome = "WIN"
        mult           = _payout_multiplier(len(leg_results), platform)
        units_profit   = round(stake * mult - stake, 2)
    else:
        # All WIN/PUSH with at least one PUSH → full push (stake returned)
        parlay_outcome = "PUSH"
        units_profit   = 0.0

    logger.info(
        "[Settlement] Parlay %s (%s) → %s  %+.2fu  [%s]",
        parlay_id, agent_name, parlay_outcome, units_profit,
        ", ".join(outcomes),
    )

    return ParlayResult(
        parlay_id=parlay_id,
        outcome=parlay_outcome,
        units_profit=units_profit,
        legs=leg_results,
    )

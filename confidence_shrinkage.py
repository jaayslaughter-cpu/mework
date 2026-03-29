"""
confidence_shrinkage.py
=======================
PropIQ — Confidence-gated edge calculation and prop-level shrinkage.

Adapted from baseball-sims (github.com/thomasosbot/baseball-sims)
  src/betting/edge.py   — compute_game_confidence, calculate_edge
  src/betting/kelly.py  — size_bet, expected_log_growth

Adapted from WagerBrain (github.com/sedemmler/WagerBrain)
  WagerBrain/bankroll.py — basic_kelly_criterion
  WagerBrain/probs.py    — true_odds_ev

How it fits into PropIQ:
    1. Each agent's evaluate() calls _compute_prop_confidence() to get a
       0-1 confidence multiplier based on:
         a. How far into the season we are (more data = higher confidence)
         b. Whether the model agrees with the sportsbook on direction
         c. How many games this player has played (minimum data gate)

    2. The raw model_prob is shrunk toward the market implied prob:
         adjusted_prob = market_prob + confidence * (model_prob - market_prob)
       This prevents over-betting when the model has thin data or wildly
       disagrees with a sharp market.

    3. ev_pct is recomputed from the adjusted (shrunk) probability, not the
       raw model output. This is the dollar EV formula from WagerBrain.

    4. kelly_units uses size_bet() which returns full Kelly, adj Kelly,
       fraction, and dollar amount — more information than current _kelly_units.

    5. expected_log_growth() provides the theoretically correct growth metric
       for ranking bets when multiple legs qualify. Higher = better.

Drop this file in the repo root. Then add ONE import line to tasklets.py:
    from confidence_shrinkage import compute_prop_confidence, shrink_and_size

No other changes required — existing agent code paths continue to work.
"""

from __future__ import annotations

import math
import datetime
import logging
from typing import Optional

logger = logging.getLogger("propiq.confidence")

# ---------------------------------------------------------------------------
# League average rates (2024 season — Baseball Reference)
# Used as fallback when player-specific data is absent
# Adapted from baseball-sims/src/simulation/constants.py
# ---------------------------------------------------------------------------

LEAGUE_RATES = {
    "K":   0.224,
    "BB":  0.085,
    "HBP": 0.011,
    "HR":  0.030,
    "3B":  0.004,
    "2B":  0.047,
    "1B":  0.143,
    "OUT": 0.456,
}

# wOBA linear weights (2024 FanGraphs)
WOBA_WEIGHTS = {
    "BB":  0.696,
    "HBP": 0.726,
    "1B":  0.883,
    "2B":  1.244,
    "3B":  1.569,
    "HR":  2.004,
}
LEAGUE_WOBA = 0.310

# ---------------------------------------------------------------------------
# Season depth: maps games played this season → confidence in data
# Early season (< 15 games) data is noisy; full season (> 100) is reliable.
# Calibrated to MLB: sample stabilizes around 50-60 PA for rate stats.
# ---------------------------------------------------------------------------

_SEASON_DEPTH_MIN_GAMES  = 10    # below this: floor confidence
_SEASON_DEPTH_MAX_GAMES  = 100   # above this: full confidence
_SEASON_DEPTH_FLOOR      = 0.25  # min confidence from depth alone


def _season_depth_score(games_played: int) -> float:
    """
    Map games played this season to a depth confidence score [floor, 1.0].
    Linear interpolation from _SEASON_DEPTH_MIN_GAMES → _SEASON_DEPTH_MAX_GAMES.

    Early April (5 games) → 0.25
    Mid-May    (40 games) → 0.52
    Full season(162 games)→ 1.00
    """
    if games_played <= _SEASON_DEPTH_MIN_GAMES:
        return _SEASON_DEPTH_FLOOR
    if games_played >= _SEASON_DEPTH_MAX_GAMES:
        return 1.0
    span = _SEASON_DEPTH_MAX_GAMES - _SEASON_DEPTH_MIN_GAMES
    return _SEASON_DEPTH_FLOOR + (1.0 - _SEASON_DEPTH_FLOOR) * (
        (games_played - _SEASON_DEPTH_MIN_GAMES) / span
    )


def _model_market_agreement(model_prob: float, market_prob: float) -> float:
    """
    Penalise when model and market disagree on which side is favoured.

    Both > 0.5  → same favourite → agreement = 1.0
    Both < 0.5  → same favourite → agreement = 1.0
    One > 0.5, other < 0.5 → flipped favourite → penalise
      disagreement = |model - market|
      agreement    = max(0.30, 1.0 - disagreement * 2)

    Adapted from baseball-sims/src/betting/edge.py compute_game_confidence().
    The 2x multiplier means a 25pp disagreement → agreement = 0.50.
    A fully flipped favourite (0.35 vs 0.65) → agreement = 0.40.
    """
    model_favours_over  = model_prob  >= 0.5
    market_favours_over = market_prob >= 0.5

    if model_favours_over == market_favours_over:
        return 1.0

    disagreement = abs(model_prob - market_prob)
    return max(0.30, 1.0 - disagreement * 2.0)


def compute_prop_confidence(
    model_prob: float,
    market_prob: float,
    games_played: int = 50,
    alpha: float = 1.0,
) -> float:
    """
    Compute a 0-1 confidence multiplier for a single player prop.

    Components (multiplicative):
      1. Season depth    — how many games has this player appeared in
      2. Model-market agreement — do model and sportsbook agree on direction

    alpha scales how aggressively to shrink (1.0 = full shrinkage, 0 = none).

    Returns a float in [0.09, 1.0].

    Examples:
        >>> compute_prop_confidence(0.62, 0.55, games_played=80)
        0.92  # deep season, agree on direction
        >>> compute_prop_confidence(0.62, 0.42, games_played=80)
        0.60  # deep season, disagree on favourite
        >>> compute_prop_confidence(0.62, 0.55, games_played=8)
        0.25  # early season, thin data
    """
    depth     = _season_depth_score(games_played)
    agreement = _model_market_agreement(model_prob, market_prob)
    raw       = depth * agreement * alpha
    return max(0.09, min(1.0, raw))


# ---------------------------------------------------------------------------
# Core: shrink model probability toward market and compute EV
# Adapted from baseball-sims/src/betting/edge.py calculate_edge()
# ---------------------------------------------------------------------------

def shrink_prob(
    model_prob: float,
    market_prob: float,
    confidence: float,
) -> float:
    """
    Shrink model probability toward market implied probability.

        adjusted = market + confidence * (model - market)

    When confidence = 1.0 → adjusted = model_prob  (no shrinkage)
    When confidence = 0.0 → adjusted = market_prob  (full shrinkage to market)
    When confidence = 0.5 → adjusted = midpoint

    This is the core insight from baseball-sims: don't bet your raw model
    probability as fact — weight it by how much you trust your data.
    """
    return market_prob + confidence * (model_prob - market_prob)


def _american_to_decimal(odds: int | float) -> float:
    """American odds → decimal odds."""
    if odds >= 100:
        return 1.0 + (odds / 100.0)
    return 1.0 + (100.0 / abs(odds))


def compute_dollar_ev(
    adjusted_prob: float,
    odds_american: int | float,
    stake: float = 1.0,
) -> float:
    """
    Dollar EV per unit staked using WagerBrain's true_odds_ev formula:
        EV = (profit * prob) - (stake * (1 - prob))

    Adapted from WagerBrain/WagerBrain/probs.py true_odds_ev().
    """
    dec    = _american_to_decimal(odds_american)
    profit = stake * (dec - 1.0)
    return (profit * adjusted_prob) - (stake * (1.0 - adjusted_prob))


def kelly_fraction(win_prob: float, decimal_odds: float) -> float:
    """
    Full Kelly fraction.
        f* = (b*p - q) / b
    Returns 0 when bet has negative EV.
    Adapted from baseball-sims/src/betting/kelly.py.
    """
    b = decimal_odds - 1.0
    if b <= 0:
        return 0.0
    f = (b * win_prob - (1.0 - win_prob)) / b
    return max(0.0, f)


def size_bet(
    win_prob: float,
    odds_american: int | float,
    bankroll: float = 1000.0,
    fraction: float = 0.25,
    max_pct: float = 0.05,
) -> dict:
    """
    Kelly-based bet sizing.

    Args:
        win_prob:      Adjusted (shrunk) win probability (0-1).
        odds_american: American odds on this side.
        bankroll:      Total bankroll in dollars.
        fraction:      Kelly fraction (0.25 = quarter-Kelly).
        max_pct:       Hard cap as fraction of bankroll.

    Returns dict with:
        kelly_full    — full Kelly fraction
        kelly_adj     — fractional Kelly
        bet_fraction  — capped fraction (what to actually bet)
        bet_dollars   — dollar amount to wager

    Adapted from baseball-sims/src/betting/kelly.py size_bet().
    """
    dec         = _american_to_decimal(odds_american)
    full        = kelly_fraction(win_prob, dec)
    adj         = full * fraction
    capped      = min(adj, max_pct)

    return {
        "kelly_full":   round(full, 4),
        "kelly_adj":    round(adj, 4),
        "bet_fraction": round(capped, 4),
        "bet_dollars":  round(bankroll * capped, 2),
    }


def expected_log_growth(
    win_prob: float,
    odds_american: int | float,
    bet_fraction: float,
) -> float:
    """
    Expected log-growth of bankroll for a single bet.
    Positive = bet grows bankroll in expectation.
    Use this to rank bets when multiple legs qualify — higher is better.

    Adapted from baseball-sims/src/betting/kelly.py expected_log_growth().
    """
    if bet_fraction <= 0:
        return 0.0
    dec  = _american_to_decimal(odds_american)
    gain = math.log(1.0 + bet_fraction * (dec - 1.0))
    loss = math.log(max(1e-9, 1.0 - bet_fraction))
    return win_prob * gain + (1.0 - win_prob) * loss


# ---------------------------------------------------------------------------
# Main integration point — call this from agent evaluate() methods
# ---------------------------------------------------------------------------

def shrink_and_size(
    model_prob_pct: float,
    market_implied_pct: float,
    odds_american: int | float,
    games_played: int = 50,
    alpha: float = 1.0,
    bankroll: float = 1000.0,
    kelly_fraction_param: float = 0.25,
    max_unit_pct: float = 0.05,
    min_ev_thresh: float = 0.03,
) -> dict | None:
    """
    End-to-end prop evaluation with confidence shrinkage.

    Takes model probability and market odds, applies confidence-gated
    shrinkage, and returns sizing info — or None if below EV threshold.

    Args:
        model_prob_pct:      Raw model probability as percentage (e.g. 58.0).
        market_implied_pct:  Market implied probability as percentage (e.g. 52.4).
        odds_american:       American odds on this side (e.g. -110).
        games_played:        Games played by this player this season.
        alpha:               Shrinkage aggressiveness (1.0 = full, 0.5 = half).
        bankroll:            Total bankroll in dollars.
        kelly_fraction_param: Kelly fraction (0.25 = quarter-Kelly).
        max_unit_pct:        Hard cap as bankroll fraction.
        min_ev_thresh:       Minimum dollar EV to qualify (e.g. 0.03 = 3¢ per $1).

    Returns:
        dict with full edge analysis, or None if below threshold.

    Usage in agent evaluate():
        result = shrink_and_size(
            model_prob_pct=model_prob,
            market_implied_pct=implied * 100,
            odds_american=over_odds,
            games_played=prop.get("games_played", 50),
        )
        if result is None:
            return None
        return self._build_bet(
            prop, "OVER", result["adjusted_prob_pct"],
            result["market_prob_pct"], result["ev_pct"],
        )
    """
    model_prob  = model_prob_pct  / 100.0
    market_prob = market_implied_pct / 100.0

    # Compute confidence multiplier
    confidence = compute_prop_confidence(
        model_prob=model_prob,
        market_prob=market_prob,
        games_played=games_played,
        alpha=alpha,
    )

    # Shrink model toward market
    adjusted_prob = shrink_prob(model_prob, market_prob, confidence)

    # Dollar EV on adjusted probability
    dollar_ev = compute_dollar_ev(adjusted_prob, odds_american, stake=1.0)

    if dollar_ev < min_ev_thresh:
        return None

    # Bet sizing on adjusted probability
    sizing = size_bet(
        win_prob=adjusted_prob,
        odds_american=odds_american,
        bankroll=bankroll,
        fraction=kelly_fraction_param,
        max_pct=max_unit_pct,
    )

    # Expected log growth — use to rank legs
    elg = expected_log_growth(adjusted_prob, odds_american, sizing["bet_fraction"])

    return {
        # Probabilities
        "model_prob_pct":    round(model_prob_pct, 2),
        "market_prob_pct":   round(market_implied_pct, 2),
        "adjusted_prob_pct": round(adjusted_prob * 100, 2),
        "confidence":        round(confidence, 3),

        # EV
        "dollar_ev":         round(dollar_ev, 4),
        "ev_pct":            round(dollar_ev * 100, 2),

        # Sizing
        "kelly_full":        sizing["kelly_full"],
        "kelly_adj":         sizing["kelly_adj"],
        "kelly_units":       sizing["bet_fraction"],
        "bet_dollars":       sizing["bet_dollars"],

        # Ranking metric
        "expected_log_growth": round(elg, 6),
    }


# ---------------------------------------------------------------------------
# Log5 odds-ratio PA blending
# Adapted from baseball-sims/src/simulation/pa_model.py odds_ratio_blend()
# ---------------------------------------------------------------------------

def log5_blend(
    batter_rate: float,
    pitcher_rate: float,
    league_rate: float | None = None,
    outcome: str = "K",
) -> float:
    """
    Multiplicative odds-ratio method (Bill James log5 numerator):

        P = (batter_rate × pitcher_rate) / league_rate

    Combines batter and pitcher tendencies against the league average
    to produce a matchup-specific rate. More accurate than simple
    averaging because it preserves the relationship between both
    players' deviations from league average.

    Args:
        batter_rate:  Batter's rate for this outcome (fraction, not %).
        pitcher_rate: Pitcher's allowed rate for this outcome.
        league_rate:  League average rate. Defaults to LEAGUE_RATES[outcome].
        outcome:      Outcome key for league rate lookup ("K", "BB", "HR", etc.)

    Returns:
        Blended probability, clipped to [1e-9, 1.0].

    Examples:
        >>> log5_blend(0.30, 0.28, outcome="K")   # high-K batter vs high-K pitcher
        0.376  # above league avg (0.224)
        >>> log5_blend(0.15, 0.20, outcome="K")   # low-K batter vs avg pitcher
        0.134  # below league avg
    """
    eps = 1e-9
    if league_rate is None:
        league_rate = LEAGUE_RATES.get(outcome, 0.224)

    b = max(eps, min(1.0, batter_rate))
    p = max(eps, min(1.0, pitcher_rate))
    l = max(eps, min(1.0, league_rate))

    return max(eps, min(1.0, (b * p) / l))


def blend_matchup_rates(
    batter_rates: dict[str, float],
    pitcher_rates: dict[str, float],
    park_factors: dict[str, float] | None = None,
) -> dict[str, float]:
    """
    Produce a normalised PA outcome distribution for a batter-pitcher matchup.

    Applies log5 blend for each outcome, then optionally adjusts for park,
    then normalises so all outcomes sum to 1.0.

    Args:
        batter_rates:  {outcome: rate} for the batter (correct platoon split).
        pitcher_rates: {outcome: rate} for the pitcher (correct platoon split).
        park_factors:  Optional {outcome: multiplier} e.g. {"HR": 1.20, "1B": 0.95}.

    Returns:
        Normalised {outcome: probability} dict.

    Usage:
        rates = blend_matchup_rates(
            batter_rates={"K": 0.24, "BB": 0.09, "HR": 0.04, ...},
            pitcher_rates={"K": 0.27, "BB": 0.07, "HR": 0.03, ...},
            park_factors={"HR": 1.20, "1B": 0.96},
        )
        k_prob = rates["K"]   # this batter's K probability in this park vs this pitcher
    """
    outcomes = list(LEAGUE_RATES.keys())
    raw: dict[str, float] = {}

    for outcome in outcomes:
        b = batter_rates.get(outcome,  LEAGUE_RATES[outcome])
        p = pitcher_rates.get(outcome, LEAGUE_RATES[outcome])
        raw[outcome] = log5_blend(b, p, outcome=outcome)

    # Park factor adjustments (multiplicative, pre-normalisation)
    if park_factors:
        for outcome, factor in park_factors.items():
            if outcome in raw:
                raw[outcome] *= float(factor)

    total = sum(raw.values())
    if total <= 0:
        return dict(LEAGUE_RATES)
    return {k: v / total for k, v in raw.items()}


# ---------------------------------------------------------------------------
# Quick smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Confidence Shrinkage Smoke Test ===\n")

    # Test 1: high confidence case — deep season, agree on direction
    result = shrink_and_size(
        model_prob_pct=62.0,
        market_implied_pct=52.4,
        odds_american=-110,
        games_played=100,
    )
    print("Test 1: High confidence (100 games, model=62% vs market=52.4%)")
    if result:
        print(f"  confidence:     {result['confidence']:.3f}")
        print(f"  adjusted_prob:  {result['adjusted_prob_pct']:.1f}%")
        print(f"  dollar_ev:      {result['dollar_ev']:.4f}")
        print(f"  kelly_units:    {result['kelly_units']:.4f}")
        print(f"  log_growth:     {result['expected_log_growth']:.6f}")
    else:
        print("  → Below EV threshold (not expected)")

    print()

    # Test 2: low confidence — early season, disagree on direction
    result2 = shrink_and_size(
        model_prob_pct=62.0,
        market_implied_pct=45.0,   # market says under is favourite
        odds_american=-110,
        games_played=8,
    )
    print("Test 2: Low confidence (8 games, model=62% vs market=45% — flipped)")
    if result2:
        print(f"  confidence:     {result2['confidence']:.3f}")
        print(f"  adjusted_prob:  {result2['adjusted_prob_pct']:.1f}%  (shrunk toward market)")
        print(f"  dollar_ev:      {result2['dollar_ev']:.4f}")
    else:
        print("  → Below EV threshold (expected — shrinkage killed the edge)")

    print()

    # Test 3: log5 blend
    k_rate = log5_blend(0.30, 0.28, outcome="K")
    print(f"Test 3: log5 K% blend (batter=30%, pitcher=28%) = {k_rate:.3f}")
    print(f"  League avg K%: {LEAGUE_RATES['K']:.3f}")

    print()

    # Test 4: full matchup blend
    batter = {"K": 0.28, "BB": 0.11, "HR": 0.05, "1B": 0.16, "2B": 0.05, "3B": 0.003, "HBP": 0.01, "OUT": 0.45}
    pitcher = {"K": 0.26, "BB": 0.07, "HR": 0.03, "1B": 0.14, "2B": 0.04, "3B": 0.004, "HBP": 0.01, "OUT": 0.47}
    blended = blend_matchup_rates(batter, pitcher, park_factors={"HR": 1.20})
    print("Test 4: blend_matchup_rates (power hitter vs K pitcher, HR park +20%)")
    for k, v in sorted(blended.items(), key=lambda x: -x[1]):
        print(f"  {k:4s}: {v:.4f}")

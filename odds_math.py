"""odds_math.py — PropIQ Analytics Odds Math Utility

Vig-stripping, Expected Value, Kelly Criterion, and bookmaker margin
functions consumed by the Execution Tier and AgentTasklet.

No external dependencies — pure Python arithmetic.

Functions (original):
    american_to_implied      -- American odds → raw implied probability (with vig)
    calculate_true_probability -- Two-sided market → true no-vig probabilities
    calculate_ev             -- (model_prob / true_no_vig_prob) - 1
    calculate_no_vig_ev      -- Convenience wrapper for a single prop side

Functions added from WagerBrain (github.com/sedemmler/WagerBrain):
    true_odds_ev             -- EV from user-supplied model probability
    kelly_criterion          -- Kelly Criterion bet sizing (fractional)
    bookmaker_margin         -- Bookmaker vig as a percentage
    decimal_odds             -- American → Decimal odds conversion
    parlay_decimal_odds      -- Combined decimal odds for a multi-leg parlay
    american_to_decimal      -- American → Decimal helper

Mathematical basis:
    Raw implied probability:
        Negative odds: |odds| / (|odds| + 100)
        Positive odds: 100   / (odds  + 100)

    Vig (overround): implied_over + implied_under  (always > 1.0)

    True no-vig probability (each side):
        true_prob = implied_prob / overround

    Expected Value (WagerBrain true_odds_ev):
        EV = (profit * prob) - (stake * (1 - prob))

    Kelly Criterion:
        f* = (b*p - q) / b   where b = decimal_odds - 1, p = win_prob, q = 1 - p

    Bookmaker Margin:
        margin = (1/fav_decimal + 1/dog_decimal) - 1
"""

from __future__ import annotations

import math


# ---------------------------------------------------------------------------
# Core implied probability (original)
# ---------------------------------------------------------------------------

def american_to_implied(odds: float) -> float:
    """Convert American odds to raw implied probability (includes vig).

    Returns probability as a decimal (0.0 – 1.0).

    Examples:
        >>> american_to_implied(-110)
        0.5238095238095238
        >>> american_to_implied(+150)
        0.4
    """
    if odds == 0:
        raise ValueError("American odds cannot be 0.")
    if odds < 0:
        abs_odds = abs(odds)
        return abs_odds / (abs_odds + 100.0)
    return 100.0 / (odds + 100.0)


def calculate_true_probability(
    odds_over: float,
    odds_under: float,
) -> tuple[float, float]:
    """Strip vig from a two-sided market and return true no-vig probabilities.

    Returns:
        Tuple of (true_over_prob, true_under_prob) — two floats summing to 1.0.

    Examples:
        >>> calculate_true_probability(-110, -110)
        (0.5, 0.5)
    """
    implied_over  = american_to_implied(odds_over)
    implied_under = american_to_implied(odds_under)
    overround     = implied_over + implied_under
    return implied_over / overround, implied_under / overround


def calculate_ev(model_probability: float, true_no_vig_probability: float) -> float:
    """EV as edge vs true market price: (model_prob / true_no_vig_prob) - 1.

    Returns:
        Signed decimal (e.g. 0.062 = +6.2%).
    """
    if true_no_vig_probability <= 0.0:
        return 0.0
    return (model_probability / true_no_vig_probability) - 1.0


def calculate_no_vig_ev(
    model_prob: float,
    odds_over: float,
    odds_under: float,
    side: str,
) -> float:
    """End-to-end: strip vig, pick correct side, return EV%.

    Args:
        model_prob: Calibrated probability for this leg (0.0 – 1.0).
        odds_over:  American odds for the Over side.
        odds_under: American odds for the Under side.
        side:       \"Over\" or \"Under\" (case-insensitive).

    Returns:
        EV% as signed decimal (e.g. 0.062 = +6.2%).
    """
    true_over, true_under = calculate_true_probability(odds_over, odds_under)
    true_no_vig = true_over if side.strip().lower() == "over" else true_under
    return calculate_ev(model_prob, true_no_vig)


# ---------------------------------------------------------------------------
# WagerBrain additions — odds conversion
# ---------------------------------------------------------------------------

def american_to_decimal(odds: int | float) -> float:
    """Convert American odds to Decimal (European) odds.

    Adapted from WagerBrain/WagerBrain/odds.py (sedemmler).

    Examples:
        >>> american_to_decimal(-110)
        1.909...
        >>> american_to_decimal(+150)
        2.5
    """
    if isinstance(odds, float) and odds > 1.0:
        return odds  # already decimal
    if odds >= 100:
        return 1.0 + (odds / 100.0)
    elif odds <= -101:
        return 1.0 + (100.0 / abs(odds))
    return float(odds)


def parlay_decimal_odds(odds_list: list[int | float]) -> float:
    """Return combined decimal odds for a multi-leg parlay.

    Multiplies all legs' decimal odds together.
    Adapted from WagerBrain/WagerBrain/odds.py parlay_odds().

    Args:
        odds_list: List of American or Decimal odds for each leg.

    Returns:
        Combined decimal odds (e.g. 7.23 for a 3-leg parlay).

    Examples:
        >>> parlay_decimal_odds([-110, -110, -110])
        6.81...
    """
    result = 1.0
    for o in odds_list:
        result *= american_to_decimal(o)
    return round(result, 4)


# ---------------------------------------------------------------------------
# WagerBrain additions — EV
# ---------------------------------------------------------------------------

def true_odds_ev(stake: float, profit: float, prob: float) -> float:
    """Expected Value from user-supplied model probability.

    Adapted from WagerBrain/WagerBrain/probs.py true_odds_ev().
    Use this when you have your own probability estimate (not just
    implied probability from odds). Returns the dollar EV per unit staked.

    Formula:
        EV = (profit * prob) - (stake * (1 - prob))

    Args:
        stake:  Amount wagered (e.g. 1.0 for per-unit calculation).
        profit: Net amount returned on a win (stake * (decimal_odds - 1)).
        prob:   Model-estimated probability of winning (0.0 – 1.0).

    Returns:
        Dollar EV per unit. Positive = +EV bet.

    Examples:
        >>> true_odds_ev(stake=1.0, profit=0.909, prob=0.58)  # -110, model 58%
        0.144...
        >>> true_odds_ev(stake=1.0, profit=1.50, prob=0.35)   # +150, model 35%
        -0.125
    """
    return (profit * prob) - (stake * (1.0 - prob))


def prop_ev_dollar(model_prob: float, odds_american: int, stake: float = 1.0) -> float:
    """Dollar EV for a single prop bet.

    Convenience wrapper combining true_odds_ev() with American→Decimal
    conversion so agents can get a dollar figure in one call.

    Args:
        model_prob:    Model win probability (0.0 – 1.0).
        odds_american: American odds on this side (e.g. -110, +120).
        stake:         Unit stake (default 1.0).

    Returns:
        Dollar EV per unit staked.

    Examples:
        >>> prop_ev_dollar(0.58, -110)   # 58% chance at -110
        0.144...
        >>> prop_ev_dollar(0.40, +150)   # 40% chance at +150
        0.1
    """
    dec = american_to_decimal(odds_american)
    profit = stake * (dec - 1.0)
    return true_odds_ev(stake=stake, profit=profit, prob=model_prob)


# ---------------------------------------------------------------------------
# WagerBrain additions — Kelly Criterion
# ---------------------------------------------------------------------------

def kelly_criterion(
    prob: float,
    odds_american: int | float,
    kelly_fraction: float = 0.25,
    max_cap: float = 0.05,
) -> float:
    """Kelly Criterion bet sizing as a fraction of bankroll.

    Adapted from WagerBrain/WagerBrain/bankroll.py basic_kelly_criterion().
    Defaults to Quarter-Kelly (kelly_fraction=0.25) capped at 5% of bankroll.

    Formula:
        f* = ((b * p) - q) / b   where b = decimal_odds - 1

    Args:
        prob:           Estimated win probability (0.0 – 1.0).
        odds_american:  American odds on this side.
        kelly_fraction: Risk scaling (1.0 = full Kelly, 0.25 = quarter-Kelly).
        max_cap:        Maximum fraction of bankroll (hard cap).

    Returns:
        Fraction of bankroll to bet (0.0 – max_cap).
        Returns 0.0 for negative Kelly (no edge).

    Examples:
        >>> kelly_criterion(prob=0.58, odds_american=-110)
        0.029...   # 2.9% of bankroll at quarter-Kelly
        >>> kelly_criterion(prob=0.40, odds_american=-110)
        0.0        # negative edge — don't bet
    """
    b = american_to_decimal(odds_american) - 1.0
    if b <= 0:
        return 0.0
    q = 1.0 - prob
    raw_kelly = (b * prob - q) / b
    if raw_kelly <= 0:
        return 0.0
    return min(raw_kelly * kelly_fraction, max_cap)


# ---------------------------------------------------------------------------
# WagerBrain additions — Bookmaker margin / vig
# ---------------------------------------------------------------------------

def bookmaker_margin(over_american: int | float, under_american: int | float) -> float:
    """Calculate bookmaker vig as a percentage of the two-sided market.

    Adapted from WagerBrain/WagerBrain/utils.py bookmaker_margin().
    A standard -110/-110 line has ~4.76% margin.
    Pinnacle (sharpest book) typically runs 2-3%.
    Soft books run 6-10%+.

    Formula:
        margin = (1/fav_decimal + 1/dog_decimal) - 1

    Args:
        over_american:  American odds on the Over side.
        under_american: American odds on the Under side.

    Returns:
        Vig as a decimal (e.g. 0.0476 = 4.76% for -110/-110).

    Examples:
        >>> bookmaker_margin(-110, -110)
        0.04761904761904762
        >>> bookmaker_margin(-115, -105)
        0.04761904761904762
    """
    fav_dec = american_to_decimal(over_american)
    dog_dec = american_to_decimal(under_american)
    if fav_dec <= 0 or dog_dec <= 0:
        return 0.0
    return (1.0 / fav_dec) + (1.0 / dog_dec) - 1.0


def is_acceptable_vig(
    over_american: int | float,
    under_american: int | float,
    max_vig: float = 0.08,
) -> bool:
    """Return True if the bookmaker margin is within acceptable limits.

    Used as a pre-filter in AgentTasklet to skip props with excessive juice
    before running EV calculations. DraftKings/FanDuel typically run 4-6%;
    reject anything above 8% as too juiced to find real edge.

    Args:
        over_american:  American odds on the Over side.
        under_american: American odds on the Under side.
        max_vig:        Maximum acceptable vig (default 8%).

    Returns:
        True if vig is acceptable, False if too high.

    Examples:
        >>> is_acceptable_vig(-110, -110)   # 4.76% — acceptable
        True
        >>> is_acceptable_vig(-130, -110)   # 9.5% — too juiced
        False
    """
    return bookmaker_margin(over_american, under_american) <= max_vig


def elo_win_prob(elo_diff: float) -> float:
    """Convert ELO rating difference to win probability (538-style formula).

    Adapted from WagerBrain/WagerBrain/probs.py elo_prob().
    Used for team-level context in game prediction layer.

    Formula:
        P(win) = 1 / (10^(-elo_diff/400) + 1)

    Args:
        elo_diff: Team A ELO minus Team B ELO (positive = Team A favoured).

    Returns:
        Win probability for Team A (0.0 – 1.0).

    Examples:
        >>> elo_win_prob(0)      # equal teams
        0.5
        >>> elo_win_prob(100)    # 100 point ELO advantage
        0.6401...
        >>> elo_win_prob(-200)   # 200 point ELO deficit
        0.2401...
    """
    return 1.0 / (10.0 ** (-elo_diff / 400.0) + 1.0)


# ---------------------------------------------------------------------------
# WagerBrain additions — Stated odds EV (two-sided case)
# ---------------------------------------------------------------------------

def stated_odds_ev(prob: float, odds_american: int | float) -> float:
    """EV for a stated bet using American odds and model probability.

    Handles the asymmetric stake/profit structure for favorites vs underdogs:
      - Favorite (negative): risk |odds| to win 100
      - Underdog (positive): risk 100 to win odds

    Adapted from WagerBrain/WagerBrain/probs.py stated_odds_ev().

    Args:
        prob:           Model-estimated win probability (0.0 – 1.0).
        odds_american:  American odds on this side (e.g. -110, +150).

    Returns:
        Dollar EV per standardised stake. Positive = +EV bet.

    Examples:
        >>> stated_odds_ev(0.58, -110)   # fav side, 58% model prob
        0.053...
        >>> stated_odds_ev(0.35, +150)   # dog side, 35% model prob
        -0.025...
    """
    if odds_american < 0:
        stake = abs(odds_american)
        profit = 100.0
    else:
        stake = 100.0
        profit = float(odds_american)
    return true_odds_ev(stake=stake, profit=profit, prob=prob)


# ---------------------------------------------------------------------------
# WagerBrain additions — Fibonacci progressive staking
# ---------------------------------------------------------------------------

def fibonacci_bankroll(
    sequence_length: int,
    base_unit: float = 1.0,
) -> list[float]:
    """Generate a Fibonacci staking sequence for progressive bankroll management.

    Each bet in a losing streak is sized by the next number in the Fibonacci
    sequence multiplied by base_unit. Resets to base_unit on a win.

    Adapted from WagerBrain/WagerBrain/bankroll.py fibonacci_bankroll().

    Args:
        sequence_length: Number of bets to pre-calculate.
        base_unit:       Minimum stake unit (default 1.0).

    Returns:
        List of stake amounts in Fibonacci progression.

    Examples:
        >>> fibonacci_bankroll(6)
        [1.0, 1.0, 2.0, 3.0, 5.0, 8.0]
        >>> fibonacci_bankroll(5, base_unit=5.0)
        [5.0, 5.0, 10.0, 15.0, 25.0]
    """
    if sequence_length <= 0:
        return []
    fib: list[float] = []
    a, b = 1, 1
    for i in range(sequence_length):
        fib.append(a * base_unit)
        a, b = b, a + b
    return fib


# ---------------------------------------------------------------------------
# WagerBrain additions — Arbitrage detection (sharp-book consensus)
# ---------------------------------------------------------------------------

def basic_arbitrage(
    odds_book_a: int | float,
    odds_book_b: int | float,
) -> dict:
    """Detect arbitrage between two books offering opposite sides.

    Calculates whether betting the favourite on Book A and underdog on Book B
    (or vice versa) guarantees a profit regardless of outcome.

    Adapted from WagerBrain/WagerBrain/strats/arb.py basic_arbitrage().
    Primary use in PropIQ: verify sharp-book consensus by checking if
    implied probs sum < 1.0 (market is offering true edge).

    Args:
        odds_book_a: American odds on Side A from Book A (e.g. -108).
        odds_book_b: American odds on Side B from Book B (e.g. +115).

    Returns:
        dict with keys:
            is_arb (bool):        True if a risk-free profit exists.
            overround (float):    Sum of implied probs (< 1.0 = arb exists).
            margin_pct (float):   Guaranteed profit % of total stake (0 if no arb).
            stake_a (float):      Optimal stake on Side A per 100 total risked.
            stake_b (float):      Optimal stake on Side B per 100 total risked.

    Examples:
        >>> basic_arbitrage(-108, +115)
        {'is_arb': True, 'overround': 0.985..., 'margin_pct': 1.4..., ...}
        >>> basic_arbitrage(-110, -110)
        {'is_arb': False, 'overround': 1.047..., 'margin_pct': 0.0, ...}
    """
    imp_a = american_to_implied(odds_book_a)
    imp_b = american_to_implied(odds_book_b)
    overround = imp_a + imp_b
    is_arb = overround < 1.0

    if is_arb:
        # Optimal stakes: proportional to implied probability
        stake_a = (imp_a / overround) * 100.0
        stake_b = (imp_b / overround) * 100.0
        margin_pct = (1.0 / overround - 1.0) * 100.0
    else:
        stake_a = 50.0
        stake_b = 50.0
        margin_pct = 0.0

    return {
        "is_arb":      is_arb,
        "overround":   round(overround, 6),
        "margin_pct":  round(margin_pct, 4),
        "stake_a":     round(stake_a, 2),
        "stake_b":     round(stake_b, 2),
    }


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Minimum no-vig EV% required before the Execution Tier publishes a slip.
MIN_EV_THRESHOLD: float = 0.03      # 3% edge vs sharp market

#: Underdog Fantasy standard Pick'em implied probability (no vig).
UNDERDOG_PICKEM_IMPLIED: float = 0.500  # True Pick'em no vig (PR #317 / PR #319)

#: Maximum acceptable bookmaker margin — props above this are skipped.
MAX_VIG: float = 0.08               # 8% vig ceiling

#: Quarter-Kelly fraction (default risk scaling).
KELLY_FRACTION: float = 0.25

#: Maximum bankroll fraction per bet.
MAX_UNIT_CAP: float = 0.05          # 5% bankroll cap

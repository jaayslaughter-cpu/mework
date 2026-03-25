"""odds_math.py — PropIQ Analytics Odds Math Utility

Vig-stripping and Expected Value functions consumed by the Execution Tier.
No external dependencies — pure Python arithmetic.

All public functions accept American odds (e.g. -110, +150).

Functions:
    american_to_implied      -- American odds → raw implied probability (with vig)
    calculate_true_probability -- Two-sided market → true no-vig probabilities
    calculate_ev             -- (model_prob / true_no_vig_prob) - 1
    calculate_no_vig_ev      -- Convenience wrapper for a single prop side

Mathematical basis:
    Raw implied probability:
        Negative odds: |odds| / (|odds| + 100)
        Positive odds: 100   / (odds  + 100)

    Vig (overround): implied_over + implied_under  (always > 1.0)

    True no-vig probability (each side):
        true_prob = implied_prob / overround

    Expected Value (edge vs sharp market):
        EV% = (model_probability / true_no_vig_probability) - 1

    Interpretation:
        EV% = 0.00  → model agrees with sharp market
        EV% = 0.10  → model thinks the prop is 10% more likely than the market
        EV% = -0.05 → model is below the sharp line — skip this prop
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def american_to_implied(odds: float) -> float:
    """Convert American odds to raw implied probability (includes vig).

    Args:
        odds: American odds integer or float (e.g. -110, +150, -140).

    Returns:
        Implied probability as a decimal (0.0 – 1.0).

    Raises:
        ValueError: If ``odds`` is 0 or would produce a nonsensical result.

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

    Converts both sides of a 2-way player prop market from American odds to
    implied probabilities, sums them to find the overround (vig), then
    normalises each side so the two true probabilities sum to exactly 1.0.

    Args:
        odds_over:  American odds for the Over side (e.g. -110).
        odds_under: American odds for the Under side (e.g. -110).

    Returns:
        Tuple of (true_over_prob, true_under_prob) — two floats that sum to 1.0.

    Examples:
        >>> calculate_true_probability(-110, -110)
        (0.5, 0.5)
        >>> calculate_true_probability(-140, +115)
        (0.5638..., 0.4361...)
    """
    implied_over = american_to_implied(odds_over)
    implied_under = american_to_implied(odds_under)
    overround = implied_over + implied_under          # e.g. 1.0476 for -110/-110
    true_over = implied_over / overround
    true_under = implied_under / overround
    return true_over, true_under


def calculate_ev(model_probability: float, true_no_vig_probability: float) -> float:
    """Calculate Expected Value as a percentage edge vs the true market price.

    Formula:
        EV% = (model_probability / true_no_vig_probability) - 1

    A positive EV% means our model believes this outcome is more likely than
    the sharp market's true (no-vig) probability implies.  A strict +3% gate
    (:data:`MIN_EV_THRESHOLD`) is enforced in the Execution Tier before any
    slip is published to Discord.

    Args:
        model_probability:      Calibrated XGBoost probability (0.0 – 1.0).
        true_no_vig_probability: True no-vig probability for the same side
                                 from :func:`calculate_true_probability`.

    Returns:
        EV as a signed decimal fraction (e.g. 0.062 = +6.2%).
        Returns 0.0 if ``true_no_vig_probability`` is zero to avoid division.

    Examples:
        >>> calculate_ev(0.58, 0.50)   # model thinks 58%, market says 50%
        0.16
        >>> calculate_ev(0.52, 0.535)  # model below market — negative EV
        -0.028...
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
    """End-to-end convenience: strip vig, pick correct side, return EV%.

    Combines :func:`calculate_true_probability` and :func:`calculate_ev`
    into a single call for use inside the Execution Tier's per-leg loop.

    Args:
        model_prob: Calibrated XGBoost probability for this leg (0.0 – 1.0).
        odds_over:  American odds for the Over side.
        odds_under: American odds for the Under side.
        side:       ``"Over"`` or ``"Under"`` (case-insensitive).

    Returns:
        EV% as a signed decimal (e.g. 0.062 = +6.2%).

    Examples:
        >>> calculate_no_vig_ev(0.58, -110, -110, "Over")
        0.16
        >>> calculate_no_vig_ev(0.42, -110, -110, "Under")
        -0.16
    """
    true_over, true_under = calculate_true_probability(odds_over, odds_under)
    true_no_vig = true_over if side.strip().lower() == "over" else true_under
    return calculate_ev(model_prob, true_no_vig)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Minimum no-vig EV% required before the Execution Tier publishes a slip.
#: Hard-coded here so the threshold is defined in one place and imported
#: by :mod:`execution_agents`.
MIN_EV_THRESHOLD: float = 0.03   # 3 % edge vs sharp market

#: Underdog Fantasy's standard Pick'em implied probability (no vig).
#: Used as the baseline comparison when sportsbook odds are unavailable.
UNDERDOG_PICKEM_IMPLIED: float = 0.535

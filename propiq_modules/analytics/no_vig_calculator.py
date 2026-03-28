"""
NoVigCalculator — Remove sportsbook juice to find true fair-value odds.

Formula:
  1. Convert American odds → decimal implied probability
  2. Sum all outcome implied probs (overround = vig)
  3. Normalize: true_prob = implied_prob / total_overround
  4. EV = xgboost_prob × (decimal_odds - 1) - (1 - xgboost_prob)
  5. No-vig EV = xgboost_prob × (no_vig_decimal - 1) - (1 - xgboost_prob)
"""

from typing import Optional
import logging

logger = logging.getLogger(__name__)


class NoVigCalculator:
    """
    Proper vig removal for MLB prop and game odds.
    Supports 2-outcome (over/under) and multi-outcome markets.
    """

    # ── Conversion helpers ────────────────────────────────────────────────────

    @staticmethod
    def american_to_decimal(american: int) -> float:
        """Convert American odds to decimal odds."""
        if american >= 100:
            return (american / 100) + 1.0
        else:
            return (100 / abs(american)) + 1.0

    @staticmethod
    def decimal_to_american(decimal: float) -> int:
        """Convert decimal odds to American odds."""
        if decimal >= 2.0:
            return int(round((decimal - 1) * 100))
        else:
            return int(round(-100 / (decimal - 1)))

    @staticmethod
    def american_to_implied_prob(american: int) -> float:
        """Raw implied probability (INCLUDES vig)."""
        if american >= 0:
            return 100.0 / (100.0 + american)
        else:
            return abs(american) / (abs(american) + 100.0)

    @staticmethod
    def decimal_to_implied_prob(decimal: float) -> float:
        return 1.0 / decimal

    # ── No-vig core ───────────────────────────────────────────────────────────

    @classmethod
    def remove_vig_two_outcome(
        cls, over_american: int, under_american: int
    ) -> dict:
        """
        Remove vig from a 2-outcome market (typical prop over/under).

        Returns:
            {
                "over_implied": 0.455,      # raw book implied
                "under_implied": 0.583,     # raw book implied
                "overround": 1.038,         # total vig (3.8%)
                "over_true_prob": 0.439,    # no-vig true probability
                "under_true_prob": 0.561,
                "over_fair_american": 128,  # no-vig fair line
                "under_fair_american": -128,
                "vig_pct": 3.8,
            }
        """
        over_impl = cls.american_to_implied_prob(over_american)
        under_impl = cls.american_to_implied_prob(under_american)
        overround = over_impl + under_impl

        over_true = over_impl / overround
        under_true = under_impl / overround

        over_fair_dec = 1.0 / over_true
        under_fair_dec = 1.0 / under_true

        return {
            "over_implied": round(over_impl, 4),
            "under_implied": round(under_impl, 4),
            "overround": round(overround, 4),
            "over_true_prob": round(over_true, 4),
            "under_true_prob": round(under_true, 4),
            "over_fair_american": cls.decimal_to_american(over_fair_dec),
            "under_fair_american": cls.decimal_to_american(under_fair_dec),
            "vig_pct": round((overround - 1.0) * 100, 2),
        }

    @classmethod
    def best_no_vig_price(cls, books_over_odds: dict[str, int]) -> dict:
        """
        Given multiple books' over odds, find the no-vig fair price
        using the sharpest (lowest-vig) book as anchor.

        books_over_odds: {"dk": 120, "fd": 115, "betmgm": 110, "bet365": 118}
        """
        # Build consensus true prob across all books
        true_probs = []
        for book, over_american in books_over_odds.items():
            # Assume symmetric 5% vig (typical props)
            implied = cls.american_to_implied_prob(over_american)
            # Rough no-vig: divide by 1.05 (standard prop juice)
            true_prob = implied / 1.05
            true_probs.append(true_prob)

        consensus_prob = sum(true_probs) / len(true_probs)
        fair_decimal = 1.0 / consensus_prob
        fair_american = cls.decimal_to_american(fair_decimal)
        best_american = max(books_over_odds.values())
        best_book = max(books_over_odds, key=lambda b: books_over_odds[b])

        return {
            "consensus_true_prob": round(consensus_prob, 4),
            "fair_american": fair_american,
            "best_book": best_book,
            "best_american": best_american,
            "edge_vs_fair": best_american - fair_american,
        }

    # ── EV calculation ────────────────────────────────────────────────────────

    @classmethod
    def calculate_ev(
        cls,
        model_prob: float,
        american_odds: int,
        unit_size: float = 1.0,
    ) -> dict:
        """
        True EV using model probability vs book odds.
        model_prob: float [0, 1]
        american_odds: e.g. 120 or -135
        Returns EV in % terms and unit terms.
        """
        decimal = cls.american_to_decimal(american_odds)
        implied = cls.american_to_implied_prob(american_odds)

        # Standard EV formula
        ev_pct = (model_prob * (decimal - 1) - (1 - model_prob)) * 100
        ev_units = unit_size * (model_prob * (decimal - 1) - (1 - model_prob))

        edge_vs_implied = (model_prob - implied) * 100

        status = "🟢 GREEN" if ev_pct > 5 else "🟡 YELLOW" if ev_pct > 0 else "🔴 RED"

        return {
            "ev_pct": round(ev_pct, 2),
            "ev_units": round(ev_units, 3),
            "model_prob": round(model_prob * 100, 1),
            "implied_prob": round(implied * 100, 1),
            "edge_vs_implied": round(edge_vs_implied, 1),
            "decimal_odds": round(decimal, 3),
            "status": status,
        }

    @classmethod
    def calculate_no_vig_ev(
        cls,
        model_prob: float,
        over_american: int,
        under_american: int,
        unit_size: float = 1.0,
    ) -> dict:
        """
        EV using TRUE no-vig probability (removes book juice first).
        This is the correct method — never reference vig-inflated implied probs.
        """
        vig_data = cls.remove_vig_two_outcome(over_american, under_american)
        true_prob = vig_data["over_true_prob"]
        fair_american = vig_data["over_fair_american"]

        # EV vs book price
        ev_vs_book = cls.calculate_ev(model_prob, over_american, unit_size)
        # True edge vs no-vig fair price
        true_edge_pct = (model_prob - true_prob) * 100

        verdict = "BUY" if true_edge_pct > 3 else "MARGINAL" if true_edge_pct > 0 else "PASS"

        return {
            **ev_vs_book,
            "no_vig_true_prob": round(true_prob * 100, 1),
            "fair_american": fair_american,
            "true_edge_pct": round(true_edge_pct, 2),
            "vig_pct": vig_data["vig_pct"],
            "verdict": verdict,
            "display": f"{verdict} ({over_american:+d} > fair {fair_american:+d}) = {true_edge_pct:+.1f}% true edge",
        }

    @classmethod
    def parlay_ev(cls, legs: list[dict]) -> dict:
        """
        Multi-leg parlay EV. Each leg: {"model_prob": 0.62, "american": 120}
        Returns combined EV with correlated/independent assumption.
        """
        combined_model_prob = 1.0
        combined_book_prob = 1.0

        for leg in legs:
            combined_model_prob *= leg["model_prob"]
            combined_book_prob *= cls.american_to_implied_prob(leg["american"])

        # Parlay payout
        combined_decimal = 1.0
        for leg in legs:
            combined_decimal *= cls.american_to_decimal(leg["american"])
        parlay_american = cls.decimal_to_american(combined_decimal)

        ev_pct = (combined_model_prob * (combined_decimal - 1) - (1 - combined_model_prob)) * 100

        return {
            "legs": len(legs),
            "combined_model_prob": round(combined_model_prob * 100, 2),
            "combined_implied_prob": round(combined_book_prob * 100, 2),
            "parlay_american": parlay_american,
            "ev_pct": round(ev_pct, 2),
            "status": "🟢 GREEN" if ev_pct > 3 else "🟡 YELLOW" if ev_pct > 0 else "🔴 RED",
        }

    @classmethod
    def kelly_criterion(
        cls,
        model_prob: float,
        american_odds: int,
        bankroll: float = 1000.0,
        kelly_fraction: float = 0.25,  # fractional kelly for safety
    ) -> dict:
        """
        Kelly Criterion bet sizing. Uses 1/4 Kelly by default (conservative).
        """
        decimal = cls.american_to_decimal(american_odds)
        b = decimal - 1  # net odds
        p = model_prob
        q = 1.0 - p
        full_kelly = (b * p - q) / b if b > 0 else 0
        fractional_kelly = max(0.0, full_kelly * kelly_fraction)
        bet_amount = bankroll * fractional_kelly

        return {
            "full_kelly_pct": round(full_kelly * 100, 2),
            "recommended_kelly_pct": round(fractional_kelly * 100, 2),
            "bet_amount": round(bet_amount, 2),
            "note": f"1/{int(1/kelly_fraction)}x Kelly (conservative)",
        }

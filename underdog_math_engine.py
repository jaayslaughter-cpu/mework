"""
underdog_math_engine.py
=======================
PropIQ Analytics — Underdog Fantasy EV & Entry-Type Engine

Implements the exact Underdog Fantasy payout tables (as of March 2026) for
both Flex and Standard Pick'em entries. Calculates true Expected Value for
each slip and recommends whether to enter as INSURED (Flex) or STANDARD.

Entry Type Definitions
----------------------
- STANDARD : All picks must be correct. Higher multiplier, no insurance.
- FLEX (INSURED) : 1 incorrect pick still pays (reduced). 6-8 leg entries
  can absorb 2 incorrect picks. Void/Tie reverts to next smaller entry.

Payout Tables (sourced from Underdog official help page, verified Mar 2026)
---------------------------------------------------------------------------

FLEX PERFECT MULTIPLIERS:
  3-leg: 3.25x   4-leg: 6x    5-leg: 10x
  6-leg: 25x     7-leg: 40x   8-leg: 80x

FLEX + 1 INCORRECT:
  3-leg: 1.09x   4-leg: 1.5x   5-leg: 2.5x
  6-leg: 2.6x    7-leg: 2.75x  8-leg: 3.0x

FLEX + 2 INCORRECT (6-8 leg only):
  6-leg: 0.25x   7-leg: 0.5x   8-leg: 1.0x

TIE / VOID REVERT CHAIN:
  8 → 7 (40x) → 6 (25x) → 5 (10x) → 4 (6x) → 3 (3.25x) → 2-std (3.5x)

STANDARD MULTIPLIERS (all picks correct):
  2-leg: 3.5x   3-leg: 5.0x   4-leg: 10.0x
  5-leg: 20.0x  6-leg: 40.0x

EV Formula
----------
For a slip of n legs with individual win probabilities [p1, p2, ..., pn]:

  P_all  = ∏ p_i
  P_k1   = Σ_i  [ (1 - p_i) × ∏_{j≠i} p_j ]          # exactly 1 wrong
  P_k2   = Σ_{i<j} [ (1-p_i)(1-p_j) × ∏_{k≠i,k≠j} p_k ]  # exactly 2 wrong

  Standard EV  = P_all × standard_multiplier - 1
  Flex EV      = P_all × flex_perfect
               + P_k1  × flex_1_loss
               [+ P_k2 × flex_2_loss  (6-8 legs only)]
               - 1

All EV values are expressed as decimal return on $1 entry (e.g. 0.045 = +4.5%).
"""

from __future__ import annotations

import logging
from itertools import combinations
from typing import Dict, List, NamedTuple, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Payout Tables
# ---------------------------------------------------------------------------

# Flex payouts: leg_count → (perfect_mult, one_loss_mult, two_loss_mult | None)
FLEX_PAYOUTS: Dict[int, Tuple[float, float, Optional[float]]] = {
    3: (3.25, 1.09, None),
    4: (6.00, 1.50, None),
    5: (10.0, 2.50, None),
    6: (25.0, 2.60, 0.25),
    7: (40.0, 2.75, 0.50),
    8: (80.0, 3.00, 1.00),
}

# Standard payouts: leg_count → multiplier (all picks correct, no insurance)
# FIX: 3-leg STANDARD was 5.0x — user confirmed 6.0x (matches calibration_layer, tasklets, settlement_engine)
STANDARD_PAYOUTS: Dict[int, float] = {
    2: 3.50,
    3: 6.00,
    4: 10.0,
    5: 20.0,
    6: 40.0,
}

# Tie/void revert chain: current_legs → (reverted_legs, reverted_multiplier)
FLEX_VOID_REVERT: Dict[int, Tuple[int, float]] = {
    8: (7, 40.0),
    7: (6, 25.0),
    6: (5, 10.0),
    5: (4, 6.00),
    4: (3, 3.25),
    3: (2, 3.50),   # reverts to 2-pick Standard
}

# Kelly fraction cap — never size more than 10% of bankroll on a single slip
KELLY_FRACTION: float = 0.50   # half-Kelly
MAX_UNIT_SIZE: float = 0.10    # 10% bankroll ceiling


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

class SlipEvaluation(NamedTuple):
    """Full evaluation result for a single slip."""

    # Core EV metrics
    standard_ev: float          # EV if entered as Standard
    flex_ev: float              # EV if entered as Flex
    total_ev: float             # Best of the two (used for filtering)

    # Entry recommendation
    recommended_entry_type: str     # "FLEX" or "STANDARD"
    recommended_multiplier: float   # The multiplier for the recommended type

    # Probability breakdown
    p_all_correct: float
    p_one_loss: float
    p_two_loss: float           # 0.0 for legs < 6

    # Sizing
    recommended_unit_size: float    # Fractional Kelly, capped at MAX_UNIT_SIZE

    # Human-readable verdict
    verdict: str               # e.g. "🛡️ INSURED (FLEX) — 3-leg +6.2% EV"


# ---------------------------------------------------------------------------
# Core Engine
# ---------------------------------------------------------------------------

class UnderdogMathEngine:
    """
    Evaluates Pick'em slips against Underdog Fantasy's published payout tables.

    Usage
    -----
    engine = UnderdogMathEngine()
    probs  = [0.58, 0.62, 0.55]          # calibrated win probabilities per leg
    result = engine.evaluate_slip(probs)
    print(result.recommended_entry_type)  # "FLEX" or "STANDARD"
    print(result.total_ev)                # e.g. 0.062
    print(result.verdict)                 # "🛡️ INSURED (FLEX) — 3-leg +6.2% EV"
    """

    def __init__(self, kelly_fraction: float = KELLY_FRACTION,
                 max_unit_size: float = MAX_UNIT_SIZE) -> None:
        self.kelly_fraction = kelly_fraction
        self.max_unit_size = max_unit_size

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate_slip(self, leg_probs: List[float]) -> SlipEvaluation:
        """
        Evaluate a slip and return full EV + entry-type recommendation.

        Parameters
        ----------
        leg_probs : list of float
            Calibrated win probabilities for each leg (e.g. [0.58, 0.62, 0.55]).
            Must be between 3 and 8 legs to be eligible for Flex.

        Returns
        -------
        SlipEvaluation
            Complete evaluation including standard_ev, flex_ev, recommended
            entry type, multiplier, and Kelly unit size.

        Raises
        ------
        ValueError
            If fewer than 2 or more than 8 legs are provided.
        """
        n = len(leg_probs)
        if not (2 <= n <= 8):
            raise ValueError(
                f"Slip must have 2–8 legs. Got {n}."
            )

        # Validate individual probs
        for i, p in enumerate(leg_probs):
            if not (0.0 < p < 1.0):
                raise ValueError(
                    f"Leg {i} probability {p} must be in (0, 1)."
                )

        # --- Probability components ---
        p_all = self._prob_all_correct(leg_probs)
        p_k1 = self._prob_exactly_k_wrong(leg_probs, k=1)
        p_k2 = self._prob_exactly_k_wrong(leg_probs, k=2) if n >= 6 else 0.0

        # --- Standard EV ---
        std_mult = STANDARD_PAYOUTS.get(n)
        if std_mult is not None:
            standard_ev = p_all * std_mult - 1.0
        else:
            # Leg count not in standard table — no standard option
            standard_ev = float("-inf")

        # --- Flex EV ---
        if n in FLEX_PAYOUTS:
            perfect_mult, one_loss_mult, two_loss_mult = FLEX_PAYOUTS[n]
            flex_ev = (
                p_all * perfect_mult
                + p_k1 * one_loss_mult
                + (p_k2 * two_loss_mult if two_loss_mult is not None else 0.0)
                - 1.0
            )
        else:
            # 2-leg entries are Standard-only
            flex_ev = float("-inf")

        # --- Recommendation ---
        if flex_ev >= standard_ev:
            entry_type = "FLEX"
            best_ev = flex_ev
            best_mult = FLEX_PAYOUTS[n][0] if n in FLEX_PAYOUTS else 0.0
        else:
            entry_type = "STANDARD"
            best_ev = standard_ev
            best_mult = std_mult if std_mult else 0.0

        # --- Kelly sizing ---
        unit_size = self._kelly_unit(best_ev)

        # --- Human verdict string ---
        ev_pct = best_ev * 100
        icon = "🛡️ INSURED" if entry_type == "FLEX" else "⚡ STANDARD"
        ev_sign = "+" if ev_pct >= 0 else ""
        verdict = (
            f"{icon} ({entry_type}) — {n}-leg "
            f"{ev_sign}{ev_pct:.1f}% EV"
        )

        logger.debug(
            "evaluate_slip: %d legs | std_ev=%.4f | flex_ev=%.4f | "
            "type=%s | unit=%.2f",
            n, standard_ev, flex_ev, entry_type, unit_size,
        )

        return SlipEvaluation(
            standard_ev=standard_ev,
            flex_ev=flex_ev,
            total_ev=best_ev,
            recommended_entry_type=entry_type,
            recommended_multiplier=best_mult,
            p_all_correct=p_all,
            p_one_loss=p_k1,
            p_two_loss=p_k2,
            recommended_unit_size=unit_size,
            verdict=verdict,
        )

    def evaluate_void_impact(
        self, n_legs: int, n_voids: int
    ) -> Tuple[int, float]:
        """
        Simulate the impact of Tie/Void reverts on a Flex entry.

        Walks the FLEX_VOID_REVERT chain n_voids times from the starting
        leg count and returns the final (effective_legs, effective_multiplier).

        Parameters
        ----------
        n_legs : int
            Original number of legs in the entry (3–8).
        n_voids : int
            Number of Ties/Voids that occurred.

        Returns
        -------
        (effective_legs, effective_multiplier) : Tuple[int, float]
        """
        current_legs = n_legs
        current_mult = FLEX_PAYOUTS.get(n_legs, (0.0,))[0]

        for _ in range(n_voids):
            if current_legs in FLEX_VOID_REVERT:
                current_legs, current_mult = FLEX_VOID_REVERT[current_legs]
            else:
                # Already at minimum (2-leg std), can't revert further
                break

        return current_legs, current_mult

    def find_optimal_combo(
        self,
        prop_pool: List[Dict],
        combo_size: int = 3,
    ) -> Optional[Dict]:
        """
        Given a pool of prop dicts (each with a 'true_prob' field), find the
        combination of ``combo_size`` legs that maximises total_ev.

        Parameters
        ----------
        prop_pool : list of dict
            Each dict must have at minimum: {"player": str, "prop": str,
            "line": float, "side": str, "true_prob": float}.
        combo_size : int
            Number of legs per slip (3–8). Default 3.

        Returns
        -------
        dict with keys:
            legs, evaluation (SlipEvaluation), total_ev
        Or None if no positive-EV combo exists.
        """
        if len(prop_pool) < combo_size:
            logger.warning(
                "find_optimal_combo: pool size %d < combo_size %d",
                len(prop_pool), combo_size,
            )
            return None

        best_ev: float = float("-inf")
        best_combo: Optional[Tuple] = None
        best_eval: Optional[SlipEvaluation] = None

        for combo in combinations(prop_pool, combo_size):
            probs = [leg["true_prob"] for leg in combo]
            try:
                ev_result = self.evaluate_slip(probs)
            except ValueError as exc:
                logger.debug("Skipping combo: %s", exc)
                continue

            if ev_result.total_ev > best_ev:
                best_ev = ev_result.total_ev
                best_combo = combo
                best_eval = ev_result

        if best_combo is None or best_ev <= 0.0:
            return None

        return {
            "legs": list(best_combo),
            "evaluation": best_eval,
            "total_ev": best_ev,
        }

    # ------------------------------------------------------------------
    # Static Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def american_to_prob(american_odds: int) -> float:
        """
        Convert American moneyline odds to implied (vig-inclusive) probability.

        Examples
        --------
        american_to_prob(-110) → 0.5238
        american_to_prob(+130) → 0.4348
        """
        if american_odds < 0:
            return abs(american_odds) / (abs(american_odds) + 100)
        return 100 / (american_odds + 100)

    @staticmethod
    def no_vig_prob(odds_list: List[int]) -> List[float]:
        """
        Remove the sportsbook overround from a list of American odds and
        return true (no-vig) probabilities that sum to 1.0.

        This is the core calculation used by LineValueScanner to convert
        sharp book lines (e.g. Pinnacle) into true market probabilities
        before comparing against the Underdog baseline.

        Algorithm
        ---------
        1. Convert each line to implied probability: p_raw = |odds| / (|odds| + 100)
           (for negatives) or 100 / (odds + 100) (for positives).
        2. Sum all raw probs → overround (typically 1.04–1.06 for sharp books).
        3. Divide each raw prob by the overround to normalise to 1.0.

        Parameters
        ----------
        odds_list : list of int
            American odds for both sides of a market, e.g. [-120, +102].

        Returns
        -------
        list of float
            Normalised true probabilities, one per input line.

        Example
        -------
        >>> UnderdogMathEngine.no_vig_prob([-120, +102])
        [0.547, 0.453]   # sum = 1.000
        """
        raw = [
            abs(o) / (abs(o) + 100) if o < 0 else 100 / (o + 100)
            for o in odds_list
        ]
        overround = sum(raw)
        return [p / overround for p in raw]

    @staticmethod
    def underdog_implied_prob(legs: int, entry_type: str = "FLEX") -> float:
        """
        Return Underdog Fantasy's implied win probability baked into their
        payout structure (i.e. the break-even hit rate for a given entry).

        Break-even for Standard: p_all = 1 / multiplier
        Break-even for Flex (approx): solved numerically assuming equal-prob legs.

        Parameters
        ----------
        legs : int
        entry_type : str  "FLEX" or "STANDARD"

        Returns
        -------
        float — break-even per-leg probability
        """
        if entry_type == "STANDARD":
            mult = STANDARD_PAYOUTS.get(legs)
            if mult is None:
                raise ValueError(f"No standard payout for {legs} legs.")
            # p^n * mult = 1  →  p = (1/mult)^(1/n)
            return (1.0 / mult) ** (1.0 / legs)

        # Flex — approximate using Newton's method on a symmetric slip
        if legs not in FLEX_PAYOUTS:
            raise ValueError(f"No flex payout for {legs} legs.")
        perfect, one_loss, two_loss = FLEX_PAYOUTS[legs]

        # Iterate to find p such that flex_ev(p,...,p) = 0
        p = 0.50
        for _ in range(200):
            # symmetric: P(all) = p^n, P(k1) = n*(1-p)*p^(n-1)
            from math import comb
            p_all = p ** legs
            p_k1 = legs * (1 - p) * (p ** (legs - 1))
            p_k2 = (
                comb(legs, 2) * ((1 - p) ** 2) * (p ** (legs - 2))
                if legs >= 6 and two_loss is not None else 0.0
            )
            ev = (
                p_all * perfect
                + p_k1 * one_loss
                + p_k2 * (two_loss or 0.0)
                - 1.0
            )
            # Derivative (numerical)
            dp = 1e-6
            p2 = p + dp
            p2_all = p2 ** legs
            p2_k1 = legs * (1 - p2) * (p2 ** (legs - 1))
            p2_k2 = (
                comb(legs, 2) * ((1 - p2) ** 2) * (p2 ** (legs - 2))
                if legs >= 6 and two_loss is not None else 0.0
            )
            ev2 = (
                p2_all * perfect
                + p2_k1 * one_loss
                + p2_k2 * (two_loss or 0.0)
                - 1.0
            )
            grad = (ev2 - ev) / dp
            if abs(grad) < 1e-12:
                break
            p = p - ev / grad
            p = max(0.01, min(0.99, p))

        return p

    # ------------------------------------------------------------------
    # Private Helpers
    # ------------------------------------------------------------------

    def _prob_all_correct(self, probs: List[float]) -> float:
        """Product of all individual win probabilities."""
        result = 1.0
        for p in probs:
            result *= p
        return result

    def _prob_exactly_k_wrong(self, probs: List[float], k: int) -> float:
        """
        Calculate probability that exactly k legs lose.

        Uses the inclusion formula over all C(n, k) subsets of losing legs:
            P(exactly k wrong) = Σ_{S ⊆ legs, |S|=k}
                [ ∏_{i ∈ S} (1 - p_i) × ∏_{j ∉ S} p_j ]
        """
        total = 0.0
        n = len(probs)
        for losing_indices in combinations(range(n), k):
            losing_set = set(losing_indices)
            prob = 1.0
            for i, p in enumerate(probs):
                prob *= (1 - p) if i in losing_set else p
            total += prob
        return total

    def _kelly_unit(self, ev: float) -> float:
        """
        Fractional Kelly criterion for unit sizing.

        Full Kelly = edge / odds_decimal_net
        We approximate odds_decimal_net as the expected payout (EV + 1) and
        apply self.kelly_fraction (default 0.5 = half-Kelly).

        Capped at MAX_UNIT_SIZE to prevent over-sizing on illiquid DFS.

        Parameters
        ----------
        ev : float
            Expected value as decimal (e.g. 0.062 for +6.2%).

        Returns
        -------
        float — fraction of bankroll to risk (e.g. 0.03 = 3%).
        """
        if ev <= 0.0:
            return 0.0
        # Kelly = (bp - q) / b  where b = net decimal odds, p = win prob, q = 1-p
        # For DFS slips we treat the EV as the net edge directly
        # Simplified: kelly ≈ edge * fraction
        kelly = ev * self.kelly_fraction
        return min(kelly, self.max_unit_size)


# ---------------------------------------------------------------------------
# Convenience function for external callers (execution_agents.py)
# ---------------------------------------------------------------------------

def evaluate_slip(
    leg_probs: List[float],
    kelly_fraction: float = KELLY_FRACTION,
) -> SlipEvaluation:
    """
    Module-level convenience wrapper around UnderdogMathEngine.evaluate_slip().

    Parameters
    ----------
    leg_probs : list of float
    kelly_fraction : float

    Returns
    -------
    SlipEvaluation
    """
    engine = UnderdogMathEngine(kelly_fraction=kelly_fraction)
    return engine.evaluate_slip(leg_probs)


# ---------------------------------------------------------------------------
# Quick sanity demo (python underdog_math_engine.py)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    engine = UnderdogMathEngine()

    print("=" * 65)
    print("UNDERDOG MATH ENGINE — PAYOUT TABLE SANITY CHECK")
    print("=" * 65)

    # --- Break-even per-leg probability for each entry type ---
    print("\n📊 Break-Even Per-Leg Probability (what Underdog needs you to hit):\n")
    print(f"{'Legs':<6} {'Standard':>12} {'Flex':>12}")
    print("-" * 32)
    for legs in range(3, 9):
        try:
            std_be = engine.underdog_implied_prob(legs, "STANDARD")
        except ValueError:
            std_be = float("nan")
        try:
            flex_be = engine.underdog_implied_prob(legs, "FLEX")
        except ValueError:
            flex_be = float("nan")
        print(f"{legs:<6} {std_be:>11.1%} {flex_be:>11.1%}")

    # --- Sample slip evaluations ---
    print("\n\n🎯 Sample Slip Evaluations:\n")

    test_cases = [
        ("EVHunter 3-leg",      [0.58, 0.62, 0.60]),
        ("UnderMachine 4-leg",  [0.57, 0.59, 0.61, 0.58]),
        ("F5Agent 5-leg",       [0.55, 0.57, 0.56, 0.58, 0.59]),
        ("MLEdgeAgent 6-leg",   [0.60, 0.63, 0.58, 0.62, 0.61, 0.59]),
        ("Low-edge 3-leg",      [0.51, 0.52, 0.50]),
    ]

    for label, probs in test_cases:
        result = engine.evaluate_slip(probs)
        print(f"  {label}")
        print(f"    {result.verdict}")
        print(f"    Standard EV: {result.standard_ev:+.2%}  |  "
              f"Flex EV: {result.flex_ev:+.2%}")
        print(f"    P(all correct): {result.p_all_correct:.2%}  |  "
              f"P(1 loss): {result.p_one_loss:.2%}")
        print(f"    Recommended unit: {result.recommended_unit_size:.1%} of bankroll\n")

    # --- No-vig example ---
    print("📐 No-Vig Probability Example (Pinnacle -118 / +100):")
    sharp_probs = engine.no_vig_prob([-118, 100])
    print(f"    Over: {sharp_probs[0]:.3f}  Under: {sharp_probs[1]:.3f}  "
          f"Sum: {sum(sharp_probs):.3f}")
    print()

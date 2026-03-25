"""
api/services/risk_management.py
Kelly Criterion bankroll management integrated from BetTrack patterns.
Includes full kelly_criterion module + PortfolioOptimizer for cross-prop allocation.

PEP 8 compliant.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
HALF_KELLY_DIVISOR = 2.0       # always use fractional Kelly
HARD_CAP_PCT       = 0.10      # max 10% bankroll on any single bet
MIN_BET_PCT        = 0.005     # minimum 0.5% to bother placing
MAX_PORTFOLIO_PCT  = 0.30      # max 30% of bankroll across all open bets
CORRELATION_PENALTY = 0.25     # reduce size by 25% for correlated bets


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class BetSizing:
    """Kelly-derived sizing for a single bet."""
    player_name:    str
    prop_type:      str
    direction:      str          # "over" | "under"
    line:           float
    model_prob:     float
    true_prob:      float
    ev_pct:         float
    kelly_fraction: float        # raw Kelly fraction
    half_kelly:     float        # half Kelly (recommended)
    capped_fraction: float       # after hard cap
    units:          float        # final unit size
    bankroll_pct:   float        # % of bankroll this represents
    rationale:      str = ""


@dataclass
class PortfolioAllocation:
    """Optimal cross-bet sizing across a slate of props."""
    total_bets:      int
    total_bankroll_pct: float
    bets:            list[BetSizing] = field(default_factory=list)
    diversification_score: float = 0.0  # 1 = perfectly diversified
    expected_portfolio_ev: float = 0.0
    max_correlated_exposure: float = 0.0
    warnings:        list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Kelly Criterion core (from BetTrack kelly_criterion module)
# ---------------------------------------------------------------------------
class KellyCriterion:
    """
    Kelly Criterion bet sizing with fractional Kelly and hard cap.

    Formula:
        f* = (b*p - q) / b
        where:
            b = decimal odds - 1  (profit per unit staked)
            p = true probability of winning
            q = 1 - p

    Fractional Kelly: f = f* / kelly_divisor  (default: 2 = Half Kelly)
    Hard cap: min(f, HARD_CAP_PCT)
    """

    def __init__(
        self,
        kelly_divisor: float = HALF_KELLY_DIVISOR,
        hard_cap:      float = HARD_CAP_PCT,
        min_bet:       float = MIN_BET_PCT,
    ) -> None:
        self._divisor  = kelly_divisor
        self._hard_cap = hard_cap
        self._min_bet  = min_bet

    def decimal_odds(self, american: int) -> float:
        """Convert American odds to decimal (European) format."""
        if american > 0:
            return (american / 100.0) + 1.0
        return (100.0 / abs(american)) + 1.0

    def full_kelly(self, prob: float, american_odds: int) -> float:
        """Compute raw (full) Kelly fraction."""
        dec    = self.decimal_odds(american_odds)
        b      = dec - 1.0          # net profit per unit
        p      = prob
        q      = 1.0 - p
        if b <= 0 or p <= 0:
            return 0.0
        f_star = (b * p - q) / b
        return max(f_star, 0.0)

    def fractional_kelly(self, prob: float, american_odds: int) -> float:
        """Fractional Kelly = full_kelly / divisor."""
        return self.full_kelly(prob, american_odds) / self._divisor

    def capped_kelly(self, prob: float, american_odds: int) -> float:
        """Fractional Kelly clamped to [min_bet, hard_cap]."""
        fk = self.fractional_kelly(prob, american_odds)
        if fk < self._min_bet:
            return 0.0   # not worth betting
        return min(fk, self._hard_cap)

    def size(
        self,
        player_name:  str,
        prop_type:    str,
        direction:    str,
        line:         float,
        model_prob:   float,
        true_prob:    float,
        american_odds: int,
        ev_pct:       float,
    ) -> BetSizing:
        """Full BetSizing with all intermediate values exposed."""
        full_k  = self.full_kelly(model_prob, american_odds)
        half_k  = full_k / self._divisor
        capped  = self.capped_kelly(model_prob, american_odds)

        rationale = (
            f"Kelly={full_k:.3f} → Half={half_k:.3f} → Capped={capped:.3f} "
            f"(model={model_prob:.3f}, true={true_prob:.3f}, EV={ev_pct:.1%})"
        )

        return BetSizing(
            player_name=player_name,
            prop_type=prop_type,
            direction=direction,
            line=line,
            model_prob=model_prob,
            true_prob=true_prob,
            ev_pct=ev_pct,
            kelly_fraction=round(full_k,  4),
            half_kelly=round(half_k,    4),
            capped_fraction=round(capped, 4),
            units=round(capped,         4),
            bankroll_pct=round(capped * 100, 2),
            rationale=rationale,
        )


# ---------------------------------------------------------------------------
# Portfolio Optimizer
# ---------------------------------------------------------------------------
class PortfolioOptimizer:
    """
    Proposes optimal bet sizing across the full prop universe for a given slate.

    Responsibilities:
    - Aggregate individual Kelly sizes from KellyCriterion
    - Detect correlated bets (same game_id or same player) and reduce exposure
    - Enforce total portfolio cap (MAX_PORTFOLIO_PCT)
    - Redistribute freed units to highest-EV uncorrelated bets
    - Output PortfolioAllocation with all sizing decisions + warnings

    Usage:
        optimizer = PortfolioOptimizer()
        allocation = optimizer.optimize(prop_edges_list)
        # prop_edges_list: list of PropEdge dicts from agent pipeline
    """

    def __init__(
        self,
        kelly_divisor: float = HALF_KELLY_DIVISOR,
        hard_cap:      float = HARD_CAP_PCT,
        portfolio_cap: float = MAX_PORTFOLIO_PCT,
        corr_penalty:  float = CORRELATION_PENALTY,
    ) -> None:
        self._kelly    = KellyCriterion(kelly_divisor, hard_cap)
        self._port_cap = portfolio_cap
        self._corr_pen = corr_penalty

    def optimize(
        self,
        prop_edges: list[dict[str, Any]],
        american_odds: int = -110,
    ) -> PortfolioAllocation:
        """
        Main entry point.
        prop_edges: list of PropEdge-compatible dicts from agent pipeline.
        Returns PortfolioAllocation with recommended unit sizes.
        """
        if not prop_edges:
            return PortfolioAllocation(total_bets=0, total_bankroll_pct=0.0)

        # Sort by EV descending — highest-value bets get first allocation
        sorted_edges = sorted(
            prop_edges,
            key=lambda x: x.get("ev_pct", x.get("edge_pct", 0.0)),
            reverse=True,
        )

        sizings: list[BetSizing] = []
        total_pct = 0.0
        warnings: list[str] = []
        seen_game_ids: dict[str, float]   = {}   # game_id → allocated %
        seen_players:  dict[str, float]   = {}   # player_name → allocated %

        for edge in sorted_edges:
            player    = edge.get("player_name", "Unknown")
            prop_type = edge.get("prop_type",   "unknown")
            direction = "over"   # default; agents set direction via model_probability > 0.5
            line      = float(edge.get("line",             0.5))
            m_prob    = float(edge.get("model_probability", 0.5))
            true_p    = float(edge.get("consensus_prob_over", m_prob))
            ev_pct    = float(edge.get("ev_pct", edge.get("edge_pct", 0.0)))
            game_id   = str(edge.get("game_id",  ""))

            sizing = self._kelly.size(
                player_name=player,
                prop_type=prop_type,
                direction=direction,
                line=line,
                model_prob=m_prob,
                true_prob=true_p,
                american_odds=american_odds,
                ev_pct=ev_pct,
            )

            if sizing.units <= 0:
                continue

            # Correlation penalty: same game → reduce
            corr_mult = 1.0
            if game_id and game_id in seen_game_ids:
                if seen_game_ids[game_id] > 0.05:   # >5% already in this game
                    corr_mult *= (1.0 - self._corr_pen)
                    warnings.append(
                        f"Correlation penalty ({self._corr_pen:.0%}) applied: "
                        f"{player}/{prop_type} shares game_id {game_id}"
                    )

            # Same player across props → halve second bet
            if player in seen_players and seen_players[player] > 0.03:
                corr_mult *= 0.5
                warnings.append(
                    f"Same-player penalty (50%) applied: {player}/{prop_type}")

            adjusted_units = round(sizing.units * corr_mult, 4)

            # Portfolio cap check
            if total_pct + adjusted_units > self._port_cap:
                remaining = max(self._port_cap - total_pct, 0.0)
                if remaining < self._kelly._min_bet:
                    warnings.append(
                        f"Portfolio cap ({self._port_cap:.0%}) reached. "
                        f"Skipping {player}/{prop_type}"
                    )
                    continue
                adjusted_units = round(remaining, 4)
                warnings.append(
                    f"Portfolio cap trim: {player}/{prop_type} → {adjusted_units:.3f}")

            sizing.units        = adjusted_units
            sizing.bankroll_pct = round(adjusted_units * 100, 2)
            total_pct          += adjusted_units

            seen_game_ids[game_id] = seen_game_ids.get(game_id, 0.0) + adjusted_units
            seen_players[player]   = seen_players.get(player, 0.0) + adjusted_units
            sizings.append(sizing)

        # Diversification score: 1 = all bets equal size, 0 = one bet takes all
        if len(sizings) > 1:
            unit_vals = [s.units for s in sizings]
            total     = sum(unit_vals)
            shares    = [u / total for u in unit_vals]
            hhi       = sum(s ** 2 for s in shares)   # Herfindahl index
            n         = len(sizings)
            div_score = round(1.0 - (hhi - 1 / n) / (1.0 - 1 / n), 4) if n > 1 else 0.0
        else:
            div_score = 0.0

        # Expected portfolio EV
        exp_ev = float(np.mean([s.ev_pct * s.units for s in sizings])) if sizings else 0.0

        return PortfolioAllocation(
            total_bets=len(sizings),
            total_bankroll_pct=round(total_pct * 100, 2),
            bets=sizings,
            diversification_score=div_score,
            expected_portfolio_ev=round(exp_ev, 4),
            warnings=warnings,
        )

    def to_discord_summary(self, alloc: PortfolioAllocation) -> str:
        """Format allocation summary for Discord notification."""
        lines = [
            f"📊 **Portfolio Allocation** — {alloc.total_bets} bets",
            f"💰 Total exposure: {alloc.total_bankroll_pct:.1f}% of bankroll",
            f"📈 Expected portfolio EV: {alloc.expected_portfolio_ev:.2%}",
            f"🔀 Diversification score: {alloc.diversification_score:.2f}",
            "",
        ]
        for b in alloc.bets[:10]:
            lines.append(
                f"• {b.player_name} {b.prop_type.upper()} {b.direction.upper()} "
                f"{b.line} — {b.units:.3f}u ({b.bankroll_pct:.1f}%) EV={b.ev_pct:.1%}"
            )
        if alloc.warnings:
            lines.append("\n⚠️ " + " | ".join(alloc.warnings[:3]))
        return "\n".join(lines)

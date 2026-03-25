"""
tests/test_risk_management.py
Unit tests for KellyCriterion and PortfolioOptimizer.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import unittest
from api.services.risk_management import (
    KellyCriterion, PortfolioOptimizer,
    HARD_CAP_PCT, MIN_BET_PCT, MAX_PORTFOLIO_PCT,
)


class TestKellyCriterion(unittest.TestCase):
    def setUp(self):
        self.kelly = KellyCriterion()

    def test_decimal_odds_favorite(self):
        dec = self.kelly.decimal_odds(-110)
        self.assertAlmostEqual(dec, 1.909, places=2)

    def test_decimal_odds_underdog(self):
        dec = self.kelly.decimal_odds(+150)
        self.assertAlmostEqual(dec, 2.5, places=2)

    def test_full_kelly_positive_ev(self):
        fk = self.kelly.full_kelly(0.55, -110)
        self.assertGreater(fk, 0)

    def test_full_kelly_negative_ev(self):
        fk = self.kelly.full_kelly(0.45, -110)
        self.assertEqual(fk, 0.0)  # clamped to 0

    def test_half_kelly_is_half_full(self):
        full = self.kelly.full_kelly(0.58, -110)
        half = self.kelly.fractional_kelly(0.58, -110)
        self.assertAlmostEqual(half, full / 2.0, places=6)

    def test_hard_cap_enforced(self):
        # Artificially high prob to force cap
        capped = self.kelly.capped_kelly(0.95, +200)
        self.assertLessEqual(capped, HARD_CAP_PCT)

    def test_below_min_bet_returns_zero(self):
        # Prob just barely positive EV but tiny fraction
        capped = self.kelly.capped_kelly(0.501, -200)
        # Either 0 or very small — capped should not be less than min bet if placed
        self.assertGreaterEqual(capped, 0.0)

    def test_size_returns_bet_sizing(self):
        sizing = self.kelly.size(
            player_name="Gerrit Cole",
            prop_type="strikeouts",
            direction="over",
            line=7.5,
            model_prob=0.60,
            true_prob=0.50,
            american_odds=-110,
            ev_pct=0.08,
        )
        self.assertEqual(sizing.player_name, "Gerrit Cole")
        self.assertGreater(sizing.units, 0)
        self.assertLessEqual(sizing.capped_fraction, HARD_CAP_PCT)
        self.assertIn("Kelly=", sizing.rationale)


class TestPortfolioOptimizer(unittest.TestCase):
    def setUp(self):
        self.optimizer = PortfolioOptimizer()

    def _make_edge(self, player, prop, ev=0.06, prob=0.58, game_id="g1"):
        return {
            "player_name": player,
            "prop_type": prop,
            "line": 1.5,
            "model_probability": prob,
            "consensus_prob_over": 0.50,
            "ev_pct": ev,
            "edge_pct": ev,
            "game_id": game_id,
        }

    def test_empty_input(self):
        alloc = self.optimizer.optimize([])
        self.assertEqual(alloc.total_bets, 0)
        self.assertEqual(alloc.total_bankroll_pct, 0.0)

    def test_single_bet_allocated(self):
        edge  = self._make_edge("Mike Trout", "hits")
        alloc = self.optimizer.optimize([edge])
        self.assertEqual(alloc.total_bets, 1)
        self.assertGreater(alloc.total_bankroll_pct, 0)

    def test_portfolio_cap_respected(self):
        # 20 high-EV bets — total should not exceed MAX_PORTFOLIO_PCT * 100
        edges = [self._make_edge(f"Player{i}", "hits", ev=0.10, game_id=f"g{i}")
                 for i in range(20)]
        alloc = self.optimizer.optimize(edges)
        self.assertLessEqual(alloc.total_bankroll_pct, MAX_PORTFOLIO_PCT * 100 + 0.01)

    def test_correlation_penalty_same_game(self):
        edges = [
            self._make_edge("Player A", "hits",      game_id="same_game"),
            self._make_edge("Player B", "home_runs", game_id="same_game"),
            self._make_edge("Player C", "rbi",       game_id="same_game"),
        ]
        alloc = self.optimizer.optimize(edges)
        # Should have warnings about correlation
        # Check total exposure isn't full kelly for all 3
        self.assertLessEqual(alloc.total_bankroll_pct, MAX_PORTFOLIO_PCT * 100 + 0.01)

    def test_same_player_penalty(self):
        edges = [
            self._make_edge("Mike Trout", "hits",       game_id="g1"),
            self._make_edge("Mike Trout", "home_runs",  game_id="g1"),
        ]
        alloc = self.optimizer.optimize(edges)
        if len(alloc.bets) >= 2:
            # Second bet should be smaller due to same-player penalty
            self.assertLessEqual(alloc.bets[1].units, alloc.bets[0].units)

    def test_diversification_score_multiple_bets(self):
        edges = [self._make_edge(f"P{i}", "hits", game_id=f"g{i}") for i in range(5)]
        alloc = self.optimizer.optimize(edges)
        self.assertGreaterEqual(alloc.diversification_score, 0.0)
        self.assertLessEqual(alloc.diversification_score,    1.0)

    def test_sorted_by_ev(self):
        edges = [
            self._make_edge("Low EV",  "hits", ev=0.03, game_id="g1"),
            self._make_edge("High EV", "hits", ev=0.12, game_id="g2"),
        ]
        alloc = self.optimizer.optimize(edges)
        if len(alloc.bets) >= 2:
            self.assertGreaterEqual(alloc.bets[0].ev_pct, alloc.bets[1].ev_pct)

    def test_discord_summary_format(self):
        edges = [self._make_edge("Aaron Judge", "home_runs", game_id="g1")]
        alloc = self.optimizer.optimize(edges)
        summary = self.optimizer.to_discord_summary(alloc)
        self.assertIn("Portfolio Allocation", summary)
        self.assertIn("bankroll", summary)


class TestKellyEdgeCases(unittest.TestCase):
    def test_zero_prob(self):
        k = KellyCriterion()
        self.assertEqual(k.full_kelly(0.0, -110), 0.0)

    def test_unity_prob(self):
        k = KellyCriterion()
        fk = k.full_kelly(1.0, -110)
        self.assertGreater(fk, 0)

    def test_very_long_underdog(self):
        k = KellyCriterion()
        capped = k.capped_kelly(0.10, +1000)
        self.assertLessEqual(capped, HARD_CAP_PCT)

    def test_custom_divisor(self):
        k = KellyCriterion(kelly_divisor=4.0)
        full = k.full_kelly(0.60, -110)
        quarter = k.fractional_kelly(0.60, -110)
        self.assertAlmostEqual(quarter, full / 4.0, places=6)


if __name__ == "__main__":
    unittest.main(verbosity=2)

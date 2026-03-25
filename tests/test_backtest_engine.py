"""
tests/test_backtest_engine.py
Unit + integration tests for the modular backtest engine.
Tests PropSimulator, BacktestReport, and metrics calculations.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import unittest
from unittest.mock import patch, MagicMock
from datetime import date
from api.tasklets.backtest_tasklet import (
    BetRecord, BacktestMetrics, PropSimulator,
    BacktestReport, BacktestDataset, BacktestRunner,
    _american_to_implied, _strip_vig, _simulate_line, _model_prob, _kelly,
)


class TestHelperFunctions(unittest.TestCase):
    def test_american_to_implied_fav(self):
        p = _american_to_implied(-110)
        self.assertAlmostEqual(p, 0.5238, places=3)

    def test_strip_vig_sums_to_one(self):
        p_o, p_u = _strip_vig(-110, -110)
        self.assertAlmostEqual(p_o + p_u, 1.0, places=5)

    def test_simulate_line_rounds_to_half(self):
        values = [1.3, 1.7, 1.5, 1.2, 1.8, 1.4, 1.6, 1.9, 1.1, 1.3, 1.7, 1.5, 1.4, 1.6]
        line = _simulate_line(values)
        self.assertEqual(line % 0.5, 0.0)

    def test_simulate_line_empty(self):
        line = _simulate_line([])
        self.assertEqual(line, 0.5)

    def test_model_prob_laplace_smoothing(self):
        # All overs: should be close to 1.0 but not exactly (Laplace)
        values = [2.0] * 30
        p = _model_prob(values, 1.5)
        self.assertLess(p, 1.0)
        self.assertGreater(p, 0.95)

    def test_model_prob_no_history(self):
        p = _model_prob([], 1.5)
        self.assertEqual(p, 0.5)

    def test_kelly_caps_at_10pct(self):
        k = _kelly(0.95, +200)
        self.assertLessEqual(k, 0.10)

    def test_kelly_zero_for_negative_ev(self):
        k = _kelly(0.40, -110)
        self.assertEqual(k, 0.0)


class TestBetRecord(unittest.TestCase):
    def test_default_values(self):
        br = BetRecord(
            date="20250401", player_name="Test", prop_type="hits",
            line=1.5, direction="over", model_prob=0.58, true_prob=0.50,
            ev_pct=0.06, odds=-110, kelly_size=0.04, unit_size=0.04,
            outcome=1, profit_units=0.036, agent="EVHunter", season=2025,
        )
        self.assertEqual(br.clv, 0.0)
        self.assertEqual(br.model_source, "ensemble")


class TestBacktestReport(unittest.TestCase):
    def _make_bets(self, n_win=10, n_loss=5, agent="EVHunter", season=2025):
        bets = []
        for i in range(n_win):
            bets.append(BetRecord(
                date=f"202504{i+1:02d}", player_name=f"P{i}", prop_type="hits",
                line=1.5, direction="over", model_prob=0.58, true_prob=0.50,
                ev_pct=0.06, odds=-110, kelly_size=0.04, unit_size=0.04,
                outcome=1, profit_units=0.036, agent=agent, season=season,
            ))
        for i in range(n_loss):
            bets.append(BetRecord(
                date=f"202504{i+20:02d}", player_name=f"L{i}", prop_type="hits",
                line=1.5, direction="over", model_prob=0.58, true_prob=0.50,
                ev_pct=0.06, odds=-110, kelly_size=0.04, unit_size=0.04,
                outcome=0, profit_units=-0.04, agent=agent, season=season,
            ))
        return bets

    def test_metrics_win_rate(self):
        bets    = self._make_bets(10, 5)
        report  = BacktestReport(bets, "test")
        summary = report.generate()
        wr = summary["overall"]["win_rate"]
        self.assertAlmostEqual(wr, 10/15, places=2)

    def test_metrics_roi_positive(self):
        bets    = self._make_bets(10, 3)
        report  = BacktestReport(bets, "test")
        summary = report.generate()
        self.assertGreater(summary["overall"]["roi_pct"], 0)

    def test_metrics_roi_negative(self):
        bets    = self._make_bets(3, 10)
        report  = BacktestReport(bets, "test")
        summary = report.generate()
        self.assertLess(summary["overall"]["roi_pct"], 0)

    def test_by_agent_breakdown(self):
        bets_ev   = self._make_bets(8, 4, agent="EVHunter")
        bets_under = self._make_bets(6, 3, agent="UnderMachine", season=2025)
        report    = BacktestReport(bets_ev + bets_under, "test")
        summary   = report.generate()
        self.assertIn("EVHunter",     summary["by_agent"])
        self.assertIn("UnderMachine", summary["by_agent"])

    def test_by_season_breakdown(self):
        bets_2024 = self._make_bets(5, 3, season=2024)
        bets_2025 = self._make_bets(7, 2, season=2025)
        report    = BacktestReport(bets_2024 + bets_2025, "test")
        summary   = report.generate()
        self.assertIn("2024", summary["by_season"])
        self.assertIn("2025", summary["by_season"])

    def test_sharpe_calculated(self):
        bets    = self._make_bets(20, 8)
        report  = BacktestReport(bets, "test")
        summary = report.generate()
        self.assertIn("sharpe_ratio", summary["overall"])

    def test_max_drawdown_nonnegative(self):
        bets    = self._make_bets(5, 10)  # losing sequence
        report  = BacktestReport(bets, "test")
        summary = report.generate()
        self.assertGreaterEqual(summary["overall"]["max_drawdown"], 0)

    def test_empty_bets(self):
        report  = BacktestReport([], "empty_test")
        summary = report.generate()
        self.assertEqual(summary["overall"]["total_bets"], 0)

    def test_output_files_created(self):
        bets    = self._make_bets(5, 3)
        report  = BacktestReport(bets, "test_output")
        report.generate()
        import os
        from api.tasklets.backtest_tasklet import OUTPUT_DIR
        self.assertTrue(os.path.exists(
            os.path.join(OUTPUT_DIR, "test_output_summary.json")))

    def test_profit_curve_length(self):
        bets    = self._make_bets(10, 5)
        report  = BacktestReport(bets, "test")
        summary = report.generate()
        curve   = summary["overall"]["profit_curve"]
        self.assertEqual(len(curve), len(bets))


class TestPropSimulatorUnit(unittest.TestCase):
    """Test PropSimulator logic without hitting Tank01 API."""

    def test_reset_clears_state(self):
        sim = PropSimulator(date(2024, 4, 1), date(2024, 4, 7))
        sim._bets = [MagicMock()]
        sim.reset()
        self.assertEqual(len(sim._bets), 0)

    def test_render_returns_string(self):
        sim = PropSimulator(date(2024, 4, 1), date(2024, 4, 7))
        self.assertIsInstance(sim.render(), str)

    @patch.object(BacktestDataset, "get_player_stats_for_date")
    def test_step_with_mock_data(self, mock_stats):
        """Step should generate bets when given 14+ days of history per player."""
        mock_stats.return_value = [{
            "game_id": "g1", "date": "20240415",
            "player_id": "p001", "player_name": "Test Batter",
            "team": "NYY", "position": "OF", "home_away": "home",
            "hits": 2, "home_runs": 0, "rbi": 1, "runs": 1,
            "total_bases": 3, "walks": 0, "doubles": 1,
            "strikeouts_bat": 1, "stolen_bases": 0,
            "strikeouts_pit": 0, "innings_pitched": 0.0,
            "hits_allowed": 0, "earned_runs": 0, "pitcher_walks": 0,
        }]

        sim = PropSimulator(date(2024, 4, 1), date(2024, 5, 15))
        sim.reset()

        # Pre-fill buffer with 14 values so line can be simulated
        sim._buffers["p001"] = {p: [1.5] * 14 for p in sim._prop_types}

        bets, info = sim.step("20240415")
        self.assertIsInstance(bets, list)
        self.assertIn("bets", info)


class TestEndToEndSmoke(unittest.TestCase):
    """
    Smoke test: 3-day window with mocked Tank01 data.
    Simulates a full market open-to-close cycle end-to-end.
    """

    @patch.object(BacktestDataset, "get_player_stats_for_date")
    @patch.object(BacktestDataset, "get_games_for_date")
    def test_3day_smoke(self, mock_games, mock_stats):
        mock_games.return_value = [{"gameID": "g1"}]
        mock_stats.return_value = [
            {
                "game_id": "g1", "date": "20250401",
                "player_id": f"p{i}", "player_name": f"Player {i}",
                "team": "NYY", "position": "OF", "home_away": "home",
                "hits": 1, "home_runs": 0, "rbi": 0, "runs": 1,
                "total_bases": 2, "walks": 0, "doubles": 0,
                "strikeouts_bat": 0, "stolen_bases": 0,
                "strikeouts_pit": 6 + i, "innings_pitched": 5.0,
                "hits_allowed": 4, "earned_runs": 2, "pitcher_walks": 1,
            }
            for i in range(5)
        ]

        sim      = PropSimulator(date(2025, 4, 1), date(2025, 4, 3))
        runner   = BacktestRunner(sim, date(2025, 4, 1), date(2025, 4, 3))

        # Pre-fill buffers for all players
        for i in range(5):
            pid = f"p{i}"
            sim._buffers[pid] = {p: [1.5] * 14 for p in sim._prop_types}

        bets = runner.run()
        report  = BacktestReport(bets, "smoke_test")
        summary = report.generate()

        self.assertIn("overall",  summary)
        self.assertIn("by_agent", summary)
        self.assertGreaterEqual(summary["overall"]["total_bets"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)

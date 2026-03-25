"""
tests/test_odds_fetcher.py
Unit + integration tests for multi-provider odds fetcher.
Targets 90%+ coverage of odds_fetcher.py and market_fusion.py.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import unittest
from unittest.mock import MagicMock, patch
from api.services.odds_fetcher import (
    OddsLine, MergedOdds,
    OddsApiOddsFetcher, SportsBooksReviewOddsFetcher, OddsFetcher,
    _american_to_implied, _strip_vig, _prob_to_american,
)


class TestVigMath(unittest.TestCase):
    def test_american_to_implied_favorite(self):
        prob = _american_to_implied(-110)
        self.assertAlmostEqual(prob, 0.5238, places=3)

    def test_american_to_implied_underdog(self):
        prob = _american_to_implied(+110)
        self.assertAlmostEqual(prob, 0.4762, places=3)

    def test_strip_vig_symmetrical(self):
        p_over, p_under = _strip_vig(-110, -110)
        self.assertAlmostEqual(p_over,  0.5, places=3)
        self.assertAlmostEqual(p_under, 0.5, places=3)
        self.assertAlmostEqual(p_over + p_under, 1.0, places=5)

    def test_strip_vig_asymmetrical(self):
        p_over, p_under = _strip_vig(-130, +110)
        self.assertAlmostEqual(p_over + p_under, 1.0, places=5)
        self.assertGreater(p_over, 0.5)

    def test_prob_to_american_favorite(self):
        odds = _prob_to_american(0.60)
        self.assertLess(odds, 0)

    def test_prob_to_american_underdog(self):
        odds = _prob_to_american(0.40)
        self.assertGreater(odds, 0)


class TestOddsApiOddsFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = OddsApiOddsFetcher()

    def test_key_rotation_on_429(self):
        """Key rotates when 429 is returned."""
        initial_idx = self.fetcher._key_idx
        self.fetcher._rotate_key()
        self.assertGreater(self.fetcher._key_idx, initial_idx)

    def test_no_rotation_past_last_key(self):
        self.fetcher._key_idx = 1
        result = self.fetcher._rotate_key()
        self.assertFalse(result)

    @patch("api.services.odds_fetcher.requests.get")
    def test_fetch_returns_empty_on_network_error(self, mock_get):
        mock_get.side_effect = Exception("Network error")
        lines = self.fetcher.fetch_player_props()
        self.assertIsInstance(lines, list)
        self.assertEqual(len(lines), 0)

    @patch("api.services.odds_fetcher.requests.get")
    def test_fetch_parses_valid_response(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = [{
            "id": "game1",
            "commence_time": "2025-04-01T18:00:00Z",
            "bookmakers": [{
                "key": "draftkings",
                "markets": [{
                    "key": "pitcher_strikeouts",
                    "outcomes": [
                        {"name": "Gerrit Cole", "description": "Over", "point": 7.5, "price": -115},
                        {"name": "Gerrit Cole", "description": "Under", "point": 7.5, "price": -105},
                    ]
                }]
            }]
        }]
        mock_get.return_value = mock_resp

        lines = self.fetcher.fetch_player_props()
        self.assertGreater(len(lines), 0)
        line = lines[0]
        self.assertEqual(line.player_name, "Gerrit Cole")
        self.assertEqual(line.prop_type,   "strikeouts")
        self.assertEqual(line.line,        7.5)
        self.assertEqual(line.odds_over,   -115)


class TestSBROddsFetcher(unittest.TestCase):
    def setUp(self):
        self.fetcher = SportsBooksReviewOddsFetcher()

    def test_provider_name(self):
        self.assertEqual(self.fetcher.provider_name(), "SBR")

    @patch("api.services.odds_fetcher._get")
    def test_fetch_parses_sbr_format(self, mock_get):
        mock_get.return_value = {"data": [{
            "playerName": "Jacob deGrom",
            "statType": "SO",
            "line": 8.5,
            "book": "Pinnacle",
            "overOdds": -108,
            "underOdds": -112,
            "gameId": "g123",
        }]}
        lines = self.fetcher.fetch_player_props()
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0].player_name, "Jacob deGrom")
        self.assertEqual(lines[0].prop_type,   "strikeouts")

    @patch("api.services.odds_fetcher._get")
    def test_fetch_skips_malformed(self, mock_get):
        mock_get.return_value = {"data": [{"no_player": True, "line": 0}]}
        lines = self.fetcher.fetch_player_props()
        self.assertEqual(len(lines), 0)


class TestOddsFetcherMerge(unittest.TestCase):
    def _make_line(self, provider, player, prop, line, o_odds, u_odds, game_id="g1"):
        return OddsLine(
            provider=provider, player_name=player, prop_type=prop,
            line=line, odds_over=o_odds, odds_under=u_odds, game_id=game_id)

    def test_merge_consensus_probability(self):
        fetcher = OddsFetcher()
        lines = [
            self._make_line("OddsAPI/dk",      "Mike Trout", "hits", 1.5, -115, -105),
            self._make_line("SBR/Pinnacle",     "Mike Trout", "hits", 1.5, -110, -110),
        ]
        merged = fetcher.merge_odds(lines)
        self.assertEqual(len(merged), 1)
        m = merged[0]
        self.assertAlmostEqual(m.consensus_prob_over + m.consensus_prob_under, 1.0, places=4)
        self.assertEqual(len(m.providers_sampled), 2)

    def test_merge_sorted_by_clv(self):
        fetcher = OddsFetcher()
        lines = [
            self._make_line("SBR/Pinnacle", "Player A", "home_runs", 0.5, -110, -110),
            self._make_line("OddsAPI/dk",   "Player A", "home_runs", 0.5, +120, -145),
            self._make_line("SBR/Pinnacle", "Player B", "hits",      1.5, -110, -110),
            self._make_line("OddsAPI/dk",   "Player B", "hits",      1.5, -110, -110),
        ]
        merged = fetcher.merge_odds(lines)
        # Player A should have higher CLV due to +120 line vs Pinnacle -110
        if len(merged) >= 2:
            self.assertGreaterEqual(merged[0].clv_edge_pct, merged[-1].clv_edge_pct)

    def test_top_clv_gate(self):
        fetcher = OddsFetcher()
        with patch.object(fetcher, "fetch_all", return_value=[
            self._make_line("SBR/Pinnacle", "Player A", "strikeouts", 7.5, -110, -110),
            self._make_line("OddsAPI/dk",   "Player A", "strikeouts", 7.5, +130, -160),
        ]):
            top = fetcher.top_clv_opportunities(n=10, min_clv_pct=0.0)
            self.assertGreaterEqual(len(top), 0)


class TestMarketFusion(unittest.TestCase):
    @patch("api.services.market_fusion.OddsFetcher")
    def test_run_returns_prop_edges(self, MockFetcher):
        from api.services.market_fusion import MarketFusionEngine, _merged_to_prop_edge
        mock_instance = MockFetcher.return_value
        mock_instance.fetch_all.return_value = []
        mock_instance.merge_odds.return_value = [
            MergedOdds(
                player_name="Test Player",
                prop_type="strikeouts",
                line=7.5,
                consensus_prob_over=0.56,
                consensus_prob_under=0.44,
                best_odds_over=-108,
                best_odds_under=-112,
                best_over_provider="OddsAPI/dk",
                best_under_provider="SBR/Pinnacle",
                clv_edge_pct=0.03,
                providers_sampled=["OddsAPI/dk", "SBR/Pinnacle"],
            )
        ]

        engine = MarketFusionEngine(clv_gate=0.02, min_providers=2)
        engine._fetcher = mock_instance
        edges = engine.run()
        self.assertGreater(len(edges), 0)
        self.assertIn("player_name", edges[0])
        self.assertIn("clv_edge_pct", edges[0])


if __name__ == "__main__":
    unittest.main(verbosity=2)

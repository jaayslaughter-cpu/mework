"""
tests/test_strikeout_model.py
Unit tests for StrikeoutFeatureEngineer, XGBoost, RandomForest, and Ensemble.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import unittest
import numpy as np
from api.services.strikeout_model import (
    StrikeoutFeatures, StrikeoutFeatureEngineer,
    StrikeoutXGBoost, StrikeoutRandomForest, StrikeoutEnsemble,
    StrikeoutPropModel,
)


class TestStrikeoutFeatures(unittest.TestCase):
    def test_feature_array_length(self):
        f = StrikeoutFeatures()
        arr = f.to_array()
        self.assertEqual(len(arr), len(StrikeoutFeatures.feature_names()))

    def test_feature_names_count(self):
        names = StrikeoutFeatures.feature_names()
        self.assertEqual(len(names), 34)

    def test_array_dtype(self):
        f = StrikeoutFeatures()
        arr = f.to_array()
        self.assertEqual(arr.dtype, np.float32)


class TestStrikeoutFeatureEngineer(unittest.TestCase):
    def setUp(self):
        self.eng = StrikeoutFeatureEngineer()
        self.pitcher = {
            "k_rate_l14": 9.8, "k_pct_l14": 0.28,
            "whip_l14": 1.1,   "era_l14": 3.20,
            "fastball_pct": 0.52, "bb_usage": 0.28,
            "fastball_velo": 95.5,
            "whiff_rate_fb": 0.22, "whiff_rate_slider": 0.38,
            "ip_avg_l4": 5.8,
        }
        self.opponent = {"k_pct_l14": 0.24, "wrc_plus": 105}
        self.context  = {
            "park_code": "NYY", "wind_speed": 8.0, "temp_f": 65.0,
            "is_home": 1, "days_rest": 4, "month": 7, "prop_line": 7.5,
        }

    def test_builds_features(self):
        feats = self.eng.build(self.pitcher, self.opponent, self.context)
        self.assertIsInstance(feats, StrikeoutFeatures)
        self.assertEqual(feats.k_rate_l14, 9.8)
        self.assertEqual(feats.line, 7.5)
        self.assertEqual(feats.home_away, 1)

    def test_arsenal_cluster_fb_dominant(self):
        p = dict(self.pitcher)
        p["fastball_pct"] = 0.60
        feats = self.eng.build(p, self.opponent, self.context)
        self.assertEqual(feats.arsenal_cluster, 0)

    def test_arsenal_cluster_breaking_ball(self):
        p = dict(self.pitcher)
        p["fastball_pct"] = 0.35
        p["bb_usage"]     = 0.45
        feats = self.eng.build(p, self.opponent, self.context)
        self.assertEqual(feats.arsenal_cluster, 1)

    def test_park_k_factor_coors(self):
        ctx = dict(self.context)
        ctx["park_code"] = "COL"
        feats = self.eng.build(self.pitcher, self.opponent, ctx)
        self.assertLess(feats.park_k_factor, 1.0)

    def test_park_k_factor_unknown(self):
        ctx = dict(self.context)
        ctx["park_code"] = "XYZ"
        feats = self.eng.build(self.pitcher, self.opponent, ctx)
        self.assertEqual(feats.park_k_factor, 1.0)


class TestModelsUntrained(unittest.TestCase):
    """Verify untrained models return 0.5/0.5 gracefully."""

    def test_xgb_untrained_returns_half(self):
        m = StrikeoutXGBoost()
        p_o, p_u = m.predict_proba(StrikeoutFeatures())
        self.assertAlmostEqual(p_o, 0.5)
        self.assertAlmostEqual(p_u, 0.5)

    def test_rf_untrained_returns_half(self):
        m = StrikeoutRandomForest()
        p_o, p_u = m.predict_proba(StrikeoutFeatures())
        self.assertAlmostEqual(p_o, 0.5)

    def test_ensemble_untrained_returns_half(self):
        e = StrikeoutEnsemble()
        p_o, p_u = e.predict_proba(StrikeoutFeatures())
        self.assertAlmostEqual(p_o, 0.5)


class TestEnsembleMethods(unittest.TestCase):
    """Test ensemble with mocked base models."""

    def _patched_ensemble(self, method: str, xgb_prob: float, rf_prob: float):
        e = StrikeoutEnsemble(method=method)
        e.xgb._is_trained = True
        e.rf._is_trained  = True
        e.xgb.predict_proba = lambda f: (xgb_prob, 1 - xgb_prob)
        e.rf.predict_proba  = lambda f: (rf_prob,  1 - rf_prob)
        return e

    def test_average_method(self):
        e = self._patched_ensemble("average", 0.65, 0.55)
        p_o, _ = e.predict_proba(StrikeoutFeatures())
        expected = 0.65 * 0.65 + 0.55 * 0.35
        self.assertAlmostEqual(p_o, expected, places=4)

    def test_bagging_method_uses_median(self):
        e = self._patched_ensemble("bagging", 0.70, 0.60)
        p_o, _ = e.predict_proba(StrikeoutFeatures())
        self.assertAlmostEqual(p_o, 0.65, places=3)

    def test_probability_clamped(self):
        e = self._patched_ensemble("average", 0.99, 0.99)
        p_o, p_u = e.predict_proba(StrikeoutFeatures())
        self.assertLessEqual(p_o, 0.99)
        self.assertGreaterEqual(p_u, 0.01)


class TestStrikeoutPropModel(unittest.TestCase):
    def setUp(self):
        self.model = StrikeoutPropModel()
        self.pitcher = {"name": "Shane Bieber", "k_rate_l14": 11.2, "ip_avg_l4": 6.0}
        self.opp     = {"k_pct_l14": 0.25}
        self.ctx     = {"prop_line": 7.5, "month": 6, "park_code": "CLE", "is_home": 1}

    def test_predict_returns_prediction(self):
        pred = self.model.predict(self.pitcher, self.opp, self.ctx)
        self.assertEqual(pred.player_name, "Shane Bieber")
        self.assertEqual(pred.prop_type,   "strikeouts")
        self.assertEqual(pred.line,        7.5)
        self.assertAlmostEqual(pred.prob_over + pred.prob_under, 1.0, places=4)

    def test_confidence_range(self):
        pred = self.model.predict(self.pitcher, self.opp, self.ctx)
        self.assertGreaterEqual(pred.confidence, 0.0)
        self.assertLessEqual(pred.confidence,    1.0)

    def test_to_prop_edge(self):
        pred = self.model.predict(self.pitcher, self.opp, self.ctx)
        edge = self.model.to_prop_edge(pred)
        self.assertIn("player_name",       edge)
        self.assertIn("model_probability", edge)
        self.assertIn("prop_type",         edge)
        self.assertEqual(edge["prop_type"], "strikeouts")

    def test_batch_predict(self):
        batch = [
            {"pitcher_stats": self.pitcher, "opponent_stats": self.opp, "context": self.ctx},
            {"pitcher_stats": {"name": "Max Scherzer"}, "opponent_stats": {}, "context": {}},
        ]
        results = self.model.batch_predict(batch)
        self.assertEqual(len(results), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""
tests/test_prop_model.py
Phase 16 — Strikeout Props Integration test suite.

Covers:
  - api/services/mlb_data.py  (PlateDisciplineStats, PitcherClusterEngine,
                                MatchupEncoder, AdvancedFeatureBuilder,
                                MLBDataValidator)
  - api/services/prop_model.py (XGBStrikeoutModel, RandomForestPropModel,
                                 EnsemblePropModel [avg/stack/blend],
                                 compare_models, ModelComparisonResult)

Pure-Python tests (no real I/O, no RabbitMQ, no Redis, no external APIs).
All heavy-ML tests use synthetic data generated inline.
"""

from __future__ import annotations

import sys
import types
import unittest

import numpy as np

# ---------------------------------------------------------------------------
# Stubs for optional dependencies not installed in the test environment
# ---------------------------------------------------------------------------
def _stub_pika() -> None:
    if "pika" not in sys.modules:
        pika = types.ModuleType("pika")
        pika.BlockingConnection = object  # type: ignore[attr-defined]
        pika.ConnectionParameters = object  # type: ignore[attr-defined]
        pika.PlainCredentials = object  # type: ignore[attr-defined]
        pika.exceptions = types.ModuleType("pika.exceptions")  # type: ignore[attr-defined]
        sys.modules["pika"] = pika
        sys.modules["pika.exceptions"] = pika.exceptions  # type: ignore[attr-defined]


def _stub_redis() -> None:
    if "redis" not in sys.modules:
        redis = types.ModuleType("redis")
        redis.Redis = object  # type: ignore[attr-defined]
        redis.ConnectionError = ConnectionError  # type: ignore[attr-defined]
        redis.exceptions = types.ModuleType("redis.exceptions")  # type: ignore[attr-defined]
        sys.modules["redis"] = redis
        sys.modules["redis.exceptions"] = redis.exceptions  # type: ignore[attr-defined]


def _stub_requests() -> None:
    if "requests" not in sys.modules:
        req = types.ModuleType("requests")

        class FakeSession:
            def __init__(self):
                self.headers = {}
            def get(self, *a, **kw):
                raise RuntimeError("no network in test")
            def post(self, *a, **kw):
                raise RuntimeError("no network in test")
            def update(self, *a, **kw):
                pass

        req.Session = FakeSession  # type: ignore[attr-defined]
        req.exceptions = types.ModuleType("requests.exceptions")  # type: ignore[attr-defined]
        req.exceptions.RequestException = Exception  # type: ignore[attr-defined]
        sys.modules["requests"] = req
        sys.modules["requests.exceptions"] = req.exceptions  # type: ignore[attr-defined]


def _stub_xgboost() -> None:
    """
    Replace xgboost.XGBClassifier with a LogisticRegression wrapper.
    Only used in the sandbox where xgboost cannot be compiled from source.
    Production deployments use the real xgboost library.
    """
    try:
        import xgboost  # noqa: F401
        return   # real xgboost available — no stub needed
    except ImportError:
        pass

    from sklearn.linear_model import LogisticRegression as _LR

    class _FakeXGBClassifier(_LR):
        """Minimal XGBClassifier stand-in backed by LogisticRegression."""

        def __init__(self, **kwargs):
            # Accept all XGBoost kwargs; map only the ones LR understands
            super().__init__(
                C            = 1.0 / max(float(kwargs.get("reg_lambda", 1.0)), 1e-6),
                max_iter     = 500,
                random_state = int(kwargs.get("random_state", 42)),
                n_jobs       = int(kwargs.get("n_jobs", 1)),
            )
            self.feature_importances_: np.ndarray = np.array([])

        def fit(self, X, y, eval_set=None, early_stopping_rounds=None,  # noqa: ARG002
                verbose=None, **kwargs):  # noqa: ARG002
            super().fit(X, y)
            n = X.shape[1] if hasattr(X, "shape") else 1
            self.feature_importances_ = np.ones(n, dtype=np.float32) / n
            return self

        def set_params(self, **params):
            return self

    xgb_mod = types.ModuleType("xgboost")
    xgb_mod.XGBClassifier = _FakeXGBClassifier  # type: ignore[attr-defined]
    sys.modules["xgboost"] = xgb_mod


_stub_pika()
_stub_redis()
_stub_requests()
_stub_xgboost()

# ---------------------------------------------------------------------------
# Import modules under test
# ---------------------------------------------------------------------------
from api.services.mlb_data import (  # noqa: E402
    AdvancedFeatureBuilder,
    AdvancedPitcherFeatures,
    CLUSTER_K_AVGS,
    CLUSTER_LABELS,
    HandednessSplit,
    MatchupEncoder,
    MLBDataValidator,
    PitcherClusterEngine,
    PlateDisciplineStats,
)
from api.services.prop_model import (  # noqa: E402
    EnsemblePropModel,
    ModelComparisonResult,
    RandomForestPropModel,
    XGBStrikeoutModel,
    compare_models,
)


# ---------------------------------------------------------------------------
# Synthetic data helpers
# ---------------------------------------------------------------------------
RNG = np.random.default_rng(42)


def _make_xy(n: int = 200, n_features: int = 12) -> tuple[np.ndarray, np.ndarray]:
    """Generate synthetic (X, y) with a weak but learnable signal."""
    X = RNG.standard_normal((n, n_features)).astype(np.float32)
    # y correlates with first feature (simulates k_rate_l7 signal)
    logit = X[:, 0] * 0.8 + RNG.standard_normal(n) * 0.5
    y = (logit > 0).astype(np.int32)
    return X, y


def _pitcher_stats(**kw) -> dict:
    base = {
        "k_rate_l14":      8.5,
        "era_l14":         3.20,
        "whip_l14":        1.15,
        "k_rate_l7":       9.0,
        "k_rate_l30":      8.0,
        "fastball_pct":    0.52,
        "breaking_ball_pct": 0.30,
        "fastball_velo":   94.5,
        "spin_rate_fb":    2350.0,
        "whiff_rate_fb":   0.25,
    }
    base.update(kw)
    return base


def _opponent_stats(**kw) -> dict:
    base = {
        "k_pct_l14":   0.24,
        "contact_pct": 0.76,
        "wrc_plus":    98.0,
        "lhb_pct":     0.42,
        "o_swing_pct": 0.32,
        "swstr_pct":   0.12,
    }
    base.update(kw)
    return base


# ===========================================================================
# 1. PlateDisciplineStats
# ===========================================================================
class TestPlateDisciplineStats(unittest.TestCase):

    def test_defaults_sum_to_valid_score(self):
        p = PlateDisciplineStats()
        score = p.k_tendency_score()
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 1.0)

    def test_high_swstr_increases_k_tendency(self):
        low_swstr  = PlateDisciplineStats(swstr_pct=0.05, contact_pct=0.85)
        high_swstr = PlateDisciplineStats(swstr_pct=0.20, contact_pct=0.65)
        self.assertGreater(high_swstr.k_tendency_score(), low_swstr.k_tendency_score())

    def test_from_dict_round_trips(self):
        d = {"o_swing_pct": 0.35, "swstr_pct": 0.13, "contact_pct": 0.74}
        p = PlateDisciplineStats.from_dict(d)
        self.assertAlmostEqual(p.o_swing_pct, 0.35)
        self.assertAlmostEqual(p.swstr_pct,   0.13)

    def test_from_dict_uses_defaults_for_missing(self):
        p = PlateDisciplineStats.from_dict({})
        self.assertAlmostEqual(p.o_swing_pct, 0.30)
        self.assertAlmostEqual(p.contact_pct, 0.78)


# ===========================================================================
# 2. PitcherClusterEngine
# ===========================================================================
class TestPitcherClusterEngine(unittest.TestCase):

    def setUp(self):
        self.engine = PitcherClusterEngine()

    def test_rule_based_fb_dominant(self):
        cluster = self.engine.predict({"fastball_pct": 0.60})
        self.assertEqual(cluster, 0)

    def test_rule_based_breaking_ball(self):
        cluster = self.engine.predict({"fastball_pct": 0.40, "breaking_ball_pct": 0.45})
        self.assertEqual(cluster, 1)

    def test_rule_based_offspeed_mix(self):
        cluster = self.engine.predict({
            "fastball_pct": 0.40,
            "breaking_ball_pct": 0.30,
            "offspeed_pct": 0.30,
        })
        self.assertEqual(cluster, 2)

    def test_rule_based_mixed(self):
        cluster = self.engine.predict({
            "fastball_pct": 0.45,
            "breaking_ball_pct": 0.35,
            "offspeed_pct": 0.20,
        })
        self.assertEqual(cluster, 3)

    def test_cluster_label_returned(self):
        for cid, label in CLUSTER_LABELS.items():
            self.assertEqual(self.engine.cluster_label(cid), label)

    def test_cluster_k_avgs_all_present(self):
        for cid in CLUSTER_LABELS:
            self.assertIn(cid, CLUSTER_K_AVGS)
            self.assertGreater(CLUSTER_K_AVGS[cid], 0.10)
            self.assertLess(CLUSTER_K_AVGS[cid], 0.50)

    def test_fit_predicts_integer_cluster(self):
        """Fit on a tiny corpus and verify output is an int in [0, 3]."""
        profiles = [
            {"fastball_pct": 0.60, "breaking_ball_pct": 0.25, "fastball_velo": 95.0,
             "spin_rate_fb": 2400.0, "whiff_rate_fb": 0.22},
            {"fastball_pct": 0.35, "breaking_ball_pct": 0.50, "fastball_velo": 91.0,
             "spin_rate_fb": 2600.0, "whiff_rate_fb": 0.35},
            {"fastball_pct": 0.40, "breaking_ball_pct": 0.30, "fastball_velo": 92.0,
             "spin_rate_fb": 2200.0, "whiff_rate_fb": 0.18},
            {"fastball_pct": 0.45, "breaking_ball_pct": 0.30, "fastball_velo": 93.0,
             "spin_rate_fb": 2300.0, "whiff_rate_fb": 0.21},
        ]
        self.engine.fit(profiles)
        result = self.engine.predict(profiles[0])
        self.assertIn(result, [0, 1, 2, 3])


# ===========================================================================
# 3. MatchupEncoder
# ===========================================================================
class TestMatchupEncoder(unittest.TestCase):

    def setUp(self):
        self.encoder = MatchupEncoder()

    def _splits(self, lhb_k=0.22, rhb_k=0.22) -> HandednessSplit:
        return HandednessSplit(k_pct_vs_lhb=lhb_k, k_pct_vs_rhb=rhb_k)

    def test_keys_present(self):
        result = self.encoder.encode(self._splits(), opp_lhb_pct=0.40)
        self.assertIn("weighted_k_pct_matchup", result)
        self.assertIn("handedness_advantage",   result)
        self.assertIn("split_disparity",        result)

    def test_advantage_in_valid_range(self):
        result = self.encoder.encode(self._splits(), opp_lhb_pct=0.50)
        self.assertGreaterEqual(result["handedness_advantage"], -1.0)
        self.assertLessEqual(result["handedness_advantage"],     1.0)

    def test_high_k_pitcher_has_positive_advantage(self):
        elite = self._splits(lhb_k=0.30, rhb_k=0.30)
        result = self.encoder.encode(elite, opp_lhb_pct=0.50)
        self.assertGreater(result["handedness_advantage"], 0.0)

    def test_zero_split_disparity_for_equal_splits(self):
        result = self.encoder.encode(self._splits(0.25, 0.25), opp_lhb_pct=0.50)
        self.assertAlmostEqual(result["split_disparity"], 0.0)

    def test_split_disparity_positive_when_splits_differ(self):
        result = self.encoder.encode(self._splits(0.30, 0.18), opp_lhb_pct=0.50)
        self.assertGreater(result["split_disparity"], 0.0)

    def test_weighted_k_pct_respects_lineup_composition(self):
        # Pitcher dominates LHBs: should be higher weighted_k_pct against LHB-heavy lineup
        splits = self._splits(lhb_k=0.35, rhb_k=0.18)
        lhb_heavy = self.encoder.encode(splits, opp_lhb_pct=0.80)
        rhb_heavy = self.encoder.encode(splits, opp_lhb_pct=0.20)
        self.assertGreater(
            lhb_heavy["weighted_k_pct_matchup"],
            rhb_heavy["weighted_k_pct_matchup"],
        )


# ===========================================================================
# 4. AdvancedFeatureBuilder
# ===========================================================================
class TestAdvancedFeatureBuilder(unittest.TestCase):

    def setUp(self):
        self.builder = AdvancedFeatureBuilder()

    def test_build_returns_dataclass(self):
        result = self.builder.build(_pitcher_stats(), _opponent_stats(), {})
        self.assertIsInstance(result, AdvancedPitcherFeatures)

    def test_opp_k_tendency_in_range(self):
        result = self.builder.build(_pitcher_stats(), _opponent_stats(), {})
        self.assertGreaterEqual(result.opp_k_tendency, 0.0)
        self.assertLessEqual(result.opp_k_tendency, 1.0)

    def test_cluster_id_valid(self):
        result = self.builder.build(_pitcher_stats(), _opponent_stats(), {})
        self.assertIn(result.arsenal_cluster, [0, 1, 2, 3])

    def test_data_quality_score_reflects_populated_fields(self):
        # With all optional fields present, quality should be high
        rich_pitcher = _pitcher_stats(
            o_swing_pct=0.31,
            swstr_pct=0.12,
            k_pct_vs_lhb=0.26,
            k_pct_vs_rhb=0.22,
            spin_rate_fb=2380.0,
            whiff_rate_slider=0.38,
        )
        result = self.builder.build(rich_pitcher, _opponent_stats(), {})
        self.assertGreater(result.data_quality_score, 0.5)

    def test_to_extra_array_length(self):
        result = self.builder.build(_pitcher_stats(), _opponent_stats(), {})
        arr = result.to_extra_array()
        self.assertEqual(len(arr), len(AdvancedPitcherFeatures.extra_feature_names()))

    def test_build_full_array_extends_base(self):
        base_arr = np.zeros(34, dtype=np.float32)
        extended = self.builder.build_full_array(
            base_arr, _pitcher_stats(), _opponent_stats(), {}
        )
        expected_len = 34 + len(AdvancedPitcherFeatures.extra_feature_names())
        self.assertEqual(len(extended), expected_len)


# ===========================================================================
# 5. MLBDataValidator
# ===========================================================================
class TestMLBDataValidator(unittest.TestCase):

    def setUp(self):
        self.validator = MLBDataValidator()

    def _valid_pair(self):
        return _pitcher_stats(), _opponent_stats()

    def test_valid_data_passes(self):
        ok, warns = self.validator.validate(*self._valid_pair())
        self.assertTrue(ok)

    def test_missing_required_pitcher_field_fails(self):
        p = _pitcher_stats()
        del p["k_rate_l14"]
        ok, warns = self.validator.validate(p, _opponent_stats())
        self.assertFalse(ok)
        self.assertTrue(any("k_rate_l14" in w for w in warns))

    def test_missing_opponent_field_is_soft_warning(self):
        o = _opponent_stats()
        del o["k_pct_l14"]
        ok, warns = self.validator.validate(_pitcher_stats(), o)
        # Soft warning — should not fail
        self.assertTrue(ok)
        self.assertTrue(any("k_pct_l14" in w for w in warns))

    def test_out_of_range_hard_field_fails(self):
        p = _pitcher_stats(era_l14=99.0)   # impossible ERA
        ok, warns = self.validator.validate(p, _opponent_stats())
        self.assertFalse(ok)

    def test_out_of_range_soft_field_warns_but_passes(self):
        o = _opponent_stats(wrc_plus=400.0)   # above 250 bound (soft)
        ok, warns = self.validator.validate(_pitcher_stats(), o)
        self.assertTrue(ok)
        self.assertTrue(any("wrc_plus" in w for w in warns))

    def test_validate_batch_separates_valid_invalid(self):
        rows = [
            (_pitcher_stats(), _opponent_stats()),              # valid
            ({}, _opponent_stats()),                            # invalid (missing required)
        ]
        valid_idx, invalid_reports = self.validator.validate_batch(rows)
        self.assertIn(0, valid_idx)
        self.assertEqual(invalid_reports[0][0], 1)


# ===========================================================================
# 6. XGBStrikeoutModel
# ===========================================================================
class TestXGBStrikeoutModel(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.X, cls.y = _make_xy(n=150)
        cls.model = XGBStrikeoutModel(tune=False)   # skip GridSearchCV for speed
        cls.model.train(cls.X, cls.y)

    def test_predict_shape(self):
        probs = self.model.predict(self.X)
        self.assertEqual(probs.shape, (len(self.X),))

    def test_predict_in_probability_range(self):
        probs = self.model.predict(self.X)
        self.assertTrue(np.all(probs >= 0.0))
        self.assertTrue(np.all(probs <= 1.0))

    def test_untrained_model_returns_half(self):
        m = XGBStrikeoutModel(tune=False)
        probs = m.predict(self.X[:5])
        np.testing.assert_array_equal(probs, np.full(5, 0.5))

    def test_best_params_set_after_grid_search(self):
        X, y = _make_xy(n=100)
        m = XGBStrikeoutModel(tune=True)
        m.train(X, y)
        # After tuning, best_params_ should have at least one entry
        self.assertIsInstance(m.best_params_, dict)

    def test_feature_importances_with_names(self):
        names = [f"f{i}" for i in range(self.X.shape[1])]
        imps = self.model.feature_importances(names)
        self.assertEqual(len(imps), self.X.shape[1])
        # Should be sorted descending
        vals = list(imps.values())
        self.assertEqual(vals, sorted(vals, reverse=True))


# ===========================================================================
# 7. RandomForestPropModel
# ===========================================================================
class TestRandomForestPropModel(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.X, cls.y = _make_xy(n=120)
        cls.model = RandomForestPropModel()
        cls.model.train(cls.X, cls.y)

    def test_predict_shape(self):
        probs = self.model.predict(self.X)
        self.assertEqual(probs.shape, (len(self.X),))

    def test_predict_in_probability_range(self):
        probs = self.model.predict(self.X)
        self.assertTrue(np.all(probs >= 0.0))
        self.assertTrue(np.all(probs <= 1.0))

    def test_untrained_model_returns_half(self):
        m = RandomForestPropModel()
        probs = m.predict(self.X[:5])
        np.testing.assert_array_equal(probs, np.full(5, 0.5))


# ===========================================================================
# 8. EnsemblePropModel — all three modes
# ===========================================================================
class TestEnsemblePropModelAverage(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.X, cls.y = _make_xy(n=120)
        cls.model = EnsemblePropModel(mode="average")
        cls.model.train(cls.X, cls.y)

    def test_predict_shape(self):
        probs = self.model.predict(self.X)
        self.assertEqual(probs.shape, (len(self.X),))

    def test_values_clipped(self):
        probs = self.model.predict(self.X)
        self.assertTrue(np.all(probs >= 0.01))
        self.assertTrue(np.all(probs <= 0.99))

    def test_weighted_average_between_extremes(self):
        """Ensemble average must lie strictly between base model predictions."""
        xgb_probs = self.model.models[0].predict(self.X)
        rf_probs  = self.model.models[1].predict(self.X)
        ens_probs = self.model.predict(self.X)
        # Ensemble should be a weighted average: min(xgb,rf) ≤ ens ≤ max(xgb,rf)
        lo = np.minimum(xgb_probs, rf_probs)
        hi = np.maximum(xgb_probs, rf_probs)
        within = np.all((ens_probs >= lo - 1e-5) & (ens_probs <= hi + 1e-5))
        self.assertTrue(within)


class TestEnsemblePropModelStack(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.X, cls.y = _make_xy(n=150)
        cls.model = EnsemblePropModel(mode="stack")
        cls.model.train(cls.X, cls.y)

    def test_meta_learner_trained(self):
        self.assertIsNotNone(self.model._meta)

    def test_predict_shape(self):
        probs = self.model.predict(self.X)
        self.assertEqual(probs.shape, (len(self.X),))

    def test_values_clipped(self):
        probs = self.model.predict(self.X)
        self.assertTrue(np.all(probs >= 0.01))
        self.assertTrue(np.all(probs <= 0.99))


class TestEnsemblePropModelBlend(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.X, cls.y = _make_xy(n=150)
        cls.model = EnsemblePropModel(mode="blend")
        cls.model.train(cls.X, cls.y)

    def test_meta_learner_trained(self):
        self.assertIsNotNone(self.model._meta)

    def test_predict_shape(self):
        probs = self.model.predict(self.X)
        self.assertEqual(probs.shape, (len(self.X),))

    def test_values_clipped_to_range(self):
        probs = self.model.predict(self.X)
        self.assertTrue(np.all(probs >= 0.01))
        self.assertTrue(np.all(probs <= 0.99))


# ===========================================================================
# 9. compare_models
# ===========================================================================
class TestCompareModels(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.X, cls.y = _make_xy(n=200)
        split = 150
        cls.X_train, cls.X_test = cls.X[:split], cls.X[split:]
        cls.y_train, cls.y_test = cls.y[:split], cls.y[split:]

        cls.rf = RandomForestPropModel()
        cls.rf.train(cls.X_train, cls.y_train)

        cls.xgb = XGBStrikeoutModel(tune=False)
        cls.xgb.train(cls.X_train, cls.y_train)

        cls.results = compare_models(
            models=[("RF", cls.rf), ("XGB", cls.xgb)],
            X_test=cls.X_test,
            y_test=cls.y_test,
        )

    def test_returns_list(self):
        self.assertIsInstance(self.results, list)

    def test_correct_number_of_results(self):
        self.assertEqual(len(self.results), 2)

    def test_results_are_model_comparison_result(self):
        for r in self.results:
            self.assertIsInstance(r, ModelComparisonResult)

    def test_sorted_by_roi_descending(self):
        rois = [r.roi_pct for r in self.results]
        self.assertEqual(rois, sorted(rois, reverse=True))

    def test_accuracy_in_valid_range(self):
        for r in self.results:
            self.assertGreaterEqual(r.accuracy, 0.0)
            self.assertLessEqual(r.accuracy,    1.0)

    def test_f1_in_valid_range(self):
        for r in self.results:
            self.assertGreaterEqual(r.f1, 0.0)
            self.assertLessEqual(r.f1,    1.0)

    def test_log_loss_positive(self):
        for r in self.results:
            self.assertGreater(r.log_loss, 0.0)

    def test_summary_is_string(self):
        for r in self.results:
            s = r.summary()
            self.assertIsInstance(s, str)
            self.assertIn(r.label, s)


# ===========================================================================
# 10. Integration: AdvancedFeatureBuilder → EnsemblePropModel
# ===========================================================================
class TestAdvancedFeaturesIntoEnsemble(unittest.TestCase):
    """
    End-to-end test: build advanced feature arrays → train ensemble → predict.
    Validates that mlb_data.py and prop_model.py integrate without errors.
    """

    def test_full_pipeline(self):
        builder = AdvancedFeatureBuilder()
        pitchers = [
            (_pitcher_stats(k_rate_l14=9.0 + i * 0.5), _opponent_stats(), {"month": 6})
            for i in range(60)
        ]

        arrays  = []
        targets = []
        for p_stats, o_stats, ctx in pitchers:
            base_arr = np.zeros(34, dtype=np.float32)
            base_arr[0] = float(p_stats["k_rate_l14"])   # k_rate_l14 as first feature
            extended = builder.build_full_array(base_arr, p_stats, o_stats, ctx)
            arrays.append(extended)
            # Simple synthetic target: K rate > league average
            targets.append(1 if p_stats["k_rate_l14"] > 9.0 else 0)

        X = np.array(arrays, dtype=np.float32)
        y = np.array(targets, dtype=np.int32)

        model = EnsemblePropModel(mode="average")
        model.train(X, y)
        probs = model.predict(X)

        self.assertEqual(probs.shape[0], len(pitchers))
        self.assertTrue(np.all(probs >= 0.01))
        self.assertTrue(np.all(probs <= 0.99))


if __name__ == "__main__":
    unittest.main()

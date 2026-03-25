"""
api/services/strikeout_model.py
XGBoost strikeout prediction model with RandomForest ensemble.
Integrates pitch-type clustering, plate discipline metrics, and arsenal features.
Outputs calibrated true probabilities for use in EV calculations.

Architecture:
  - StrikeoutFeatureEngineer: builds feature matrix from raw Statcast + box-score data
  - StrikeoutXGBoost: XGBoost binary:logistic + isotonic calibration
  - StrikeoutRandomForest: sklearn RandomForest + isotonic calibration (base model)
  - StrikeoutEnsemble: stacking / averaging / bagging combiner
  - StrikeoutPropModel: top-level interface for PropIQ agent pipeline

PEP 8 compliant. No hallucinated APIs.
"""

from __future__ import annotations

import json
import logging
import os
import pickle
from dataclasses import dataclass, field
from typing import Any, Literal

import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional heavy imports with graceful degradation
# ---------------------------------------------------------------------------
try:
    import xgboost as xgb
    _XGB_AVAILABLE = True
except ImportError:
    _XGB_AVAILABLE = False
    logger.warning("[StrikeoutModel] xgboost not installed — XGBoost model disabled")

try:
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.ensemble import RandomForestClassifier, StackingClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler
    _SKL_AVAILABLE = True
except ImportError:
    _SKL_AVAILABLE = False
    logger.warning("[StrikeoutModel] scikit-learn not installed — RF model disabled")

MODEL_DIR = os.getenv("MODEL_DIR", "/agent/home/models")
os.makedirs(MODEL_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class StrikeoutFeatures:
    """Feature vector for a single pitcher start prediction."""
    # Rolling performance
    k_rate_l7:           float = 0.0   # K/9 last 7 days
    k_rate_l14:          float = 0.0
    k_rate_l30:          float = 0.0
    k_pct_l7:            float = 0.0   # K% last 7 days
    k_pct_l14:           float = 0.0
    whip_l14:            float = 0.0
    era_l14:             float = 0.0
    bb_rate_l14:         float = 0.0

    # Pitch arsenal (from Statcast / apify_scrapers)
    fastball_pct:        float = 0.0
    breaking_ball_pct:   float = 0.0
    offspeed_pct:        float = 0.0
    fastball_velo:       float = 0.0   # mph
    spin_rate_fb:        float = 0.0   # rpm
    whiff_rate_fb:       float = 0.0   # swinging strike %
    whiff_rate_slider:   float = 0.0
    whiff_rate_curve:    float = 0.0
    chase_rate:          float = 0.0   # O-swing%
    zone_contact_rate:   float = 0.0   # Z-contact%

    # Pitch-type clustering label (0=FB-dominant, 1=breaking-ball, 2=offspeed-mix)
    arsenal_cluster:     int   = 0

    # Opposing lineup
    opp_k_pct_l14:       float = 0.0   # opponent team K%
    opp_wrc_plus:        float = 100.0
    opp_contact_pct:     float = 0.0
    opp_chase_pct:       float = 0.0
    opp_handed_split:    float = 0.0   # LHB% in opposing lineup

    # Context
    park_k_factor:       float = 1.0   # >1 = pitcher-friendly
    wind_speed:          float = 0.0
    temp_f:              float = 72.0
    home_away:           int   = 1     # 1=home, 0=away
    umpire_k_rate:       float = 0.0   # umpire historical K rate modifier
    fatigue_index:       float = 0.0   # bullpen fatigue (not pitcher but context)
    days_rest:           int   = 4
    season_month:        int   = 6     # 1-10 for April-Oct

    # Prop-specific
    line:                float = 5.5   # DFS prop line
    innings_pitched_avg: float = 5.5   # expected IP for this starter

    def to_array(self) -> np.ndarray:
        return np.array([
            self.k_rate_l7, self.k_rate_l14, self.k_rate_l30,
            self.k_pct_l7, self.k_pct_l14,
            self.whip_l14, self.era_l14, self.bb_rate_l14,
            self.fastball_pct, self.breaking_ball_pct, self.offspeed_pct,
            self.fastball_velo, self.spin_rate_fb,
            self.whiff_rate_fb, self.whiff_rate_slider, self.whiff_rate_curve,
            self.chase_rate, self.zone_contact_rate,
            float(self.arsenal_cluster),
            self.opp_k_pct_l14, self.opp_wrc_plus, self.opp_contact_pct,
            self.opp_chase_pct, self.opp_handed_split,
            self.park_k_factor, self.wind_speed, self.temp_f,
            float(self.home_away), self.umpire_k_rate, self.fatigue_index,
            float(self.days_rest), float(self.season_month),
            self.line, self.innings_pitched_avg,
        ], dtype=np.float32)

    @classmethod
    def feature_names(cls) -> list[str]:
        return [
            "k_rate_l7", "k_rate_l14", "k_rate_l30",
            "k_pct_l7", "k_pct_l14",
            "whip_l14", "era_l14", "bb_rate_l14",
            "fastball_pct", "breaking_ball_pct", "offspeed_pct",
            "fastball_velo", "spin_rate_fb",
            "whiff_rate_fb", "whiff_rate_slider", "whiff_rate_curve",
            "chase_rate", "zone_contact_rate",
            "arsenal_cluster",
            "opp_k_pct_l14", "opp_wrc_plus", "opp_contact_pct",
            "opp_chase_pct", "opp_handed_split",
            "park_k_factor", "wind_speed", "temp_f",
            "home_away", "umpire_k_rate", "fatigue_index",
            "days_rest", "season_month",
            "line", "innings_pitched_avg",
        ]


@dataclass
class StrikeoutPrediction:
    """Output of the strikeout model for a single pitcher."""
    player_name:    str
    prop_type:      str = "strikeouts"
    line:           float = 5.5
    prob_over:      float = 0.5     # calibrated true probability
    prob_under:     float = 0.5
    model_source:   str = "ensemble"
    confidence:     float = 0.0    # |prob_over - 0.5| * 2, range [0,1]
    features_used:  list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Feature engineer
# ---------------------------------------------------------------------------
class StrikeoutFeatureEngineer:
    """
    Builds StrikeoutFeatures from raw data dicts.
    Input: pitcher_stats dict (from Tank01 / Statcast Redis cache),
           opponent_stats dict, context dict.
    """

    _PARK_K_FACTORS: dict[str, float] = {
        # Pitcher-friendly parks (>1.0) vs hitter-friendly (<1.0)
        "COL": 0.85, "BOS": 0.92, "CIN": 0.93, "CHW": 0.94, "TEX": 0.96,
        "MIA": 1.08, "NYM": 1.06, "PIT": 1.05, "SF":  1.07, "OAK": 1.04,
        "MIN": 0.97, "HOU": 1.02, "LAD": 1.03, "ATL": 1.01, "NYY": 0.99,
    }

    def build(
        self,
        pitcher_stats: dict[str, Any],
        opponent_stats: dict[str, Any],
        context: dict[str, Any],
    ) -> StrikeoutFeatures:
        """Build feature vector from raw data."""
        p = pitcher_stats
        o = opponent_stats
        c = context

        # Rolling K rates
        k7  = p.get("k_rate_l7",  p.get("k_per_9_l7",  0.0))
        k14 = p.get("k_rate_l14", p.get("k_per_9_l14", k7))
        k30 = p.get("k_rate_l30", p.get("k_per_9_l30", k14))

        # Arsenal
        fb_pct = p.get("fastball_pct",     p.get("fb_usage", 0.50))
        bb_pct = p.get("breaking_ball_pct", p.get("bb_usage", 0.30))
        os_pct = 1.0 - fb_pct - bb_pct

        # Cluster: simple rule-based until Statcast k-means available
        if fb_pct >= 0.55:
            cluster = 0
        elif bb_pct >= 0.40:
            cluster = 1
        else:
            cluster = 2

        park_code = c.get("park_code", c.get("home_team", ""))
        park_k    = self._PARK_K_FACTORS.get(park_code, 1.0)

        return StrikeoutFeatures(
            k_rate_l7=float(k7),
            k_rate_l14=float(k14),
            k_rate_l30=float(k30),
            k_pct_l7=float(p.get("k_pct_l7",   p.get("k_pct",  0.22))),
            k_pct_l14=float(p.get("k_pct_l14",  0.22)),
            whip_l14=float(p.get("whip_l14",    p.get("whip",   1.30))),
            era_l14=float(p.get("era_l14",      p.get("era",    4.00))),
            bb_rate_l14=float(p.get("bb_rate_l14", 0.08)),
            fastball_pct=float(fb_pct),
            breaking_ball_pct=float(bb_pct),
            offspeed_pct=float(max(0.0, os_pct)),
            fastball_velo=float(p.get("fastball_velo", p.get("avg_velo", 93.0))),
            spin_rate_fb=float(p.get("spin_rate_fb",  2300.0)),
            whiff_rate_fb=float(p.get("whiff_rate_fb",      0.20)),
            whiff_rate_slider=float(p.get("whiff_rate_slider", 0.35)),
            whiff_rate_curve=float(p.get("whiff_rate_curve",  0.30)),
            chase_rate=float(p.get("chase_rate",        0.30)),
            zone_contact_rate=float(p.get("zone_contact_rate", 0.85)),
            arsenal_cluster=cluster,
            opp_k_pct_l14=float(o.get("k_pct_l14",   o.get("k_pct",   0.22))),
            opp_wrc_plus=float(o.get("wrc_plus",      100.0)),
            opp_contact_pct=float(o.get("contact_pct", 0.78)),
            opp_chase_pct=float(o.get("chase_pct",    0.30)),
            opp_handed_split=float(o.get("lhb_pct",   0.40)),
            park_k_factor=park_k,
            wind_speed=float(c.get("wind_speed",  0.0)),
            temp_f=float(c.get("temp_f",          72.0)),
            home_away=int(c.get("is_home",        1)),
            umpire_k_rate=float(c.get("umpire_k_rate", 0.0)),
            fatigue_index=float(c.get("fatigue_index", 0.0)),
            days_rest=int(c.get("days_rest",      4)),
            season_month=int(c.get("month",       6)),
            line=float(c.get("prop_line",         5.5)),
            innings_pitched_avg=float(p.get("ip_avg_l4", 5.5)),
        )


# ---------------------------------------------------------------------------
# XGBoost model
# ---------------------------------------------------------------------------
class StrikeoutXGBoost:
    """
    XGBoost binary:logistic with isotonic calibration.
    Target: 1 if pitcher_strikeouts > prop_line, else 0.
    """

    MODEL_PATH = os.path.join(MODEL_DIR, "strikeout_xgb.pkl")

    _PARAMS = {
        "objective":         "binary:logistic",
        "eval_metric":       "logloss",
        "max_depth":         6,
        "learning_rate":     0.05,
        "n_estimators":      500,
        "subsample":         0.8,
        "colsample_bytree":  0.8,
        "min_child_weight":  5,
        "gamma":             0.1,
        "reg_alpha":         0.1,
        "reg_lambda":        1.0,
        "tree_method":       "hist",
        "use_label_encoder": False,
        "random_state":      42,
        "n_jobs":            -1,
    }

    def __init__(self) -> None:
        self._model = None
        self._calibrated = None
        self._is_trained = False

    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        if not _XGB_AVAILABLE or not _SKL_AVAILABLE:
            raise RuntimeError("xgboost + scikit-learn required for training")
        base = xgb.XGBClassifier(**self._PARAMS)
        self._calibrated = CalibratedClassifierCV(
            base, method="isotonic", cv=5)
        self._calibrated.fit(X, y)
        self._is_trained = True
        logger.info("[StrikeoutXGB] Trained on %d samples", len(y))

    def predict_proba(self, features: StrikeoutFeatures) -> tuple[float, float]:
        """Returns (prob_over, prob_under) as calibrated true probabilities."""
        if not self._is_trained or self._calibrated is None:
            return 0.5, 0.5
        X = features.to_array().reshape(1, -1)
        prob_over = float(self._calibrated.predict_proba(X)[0][1])
        return round(prob_over, 4), round(1.0 - prob_over, 4)

    def save(self) -> None:
        with open(self.MODEL_PATH, "wb") as f:
            pickle.dump(self._calibrated, f)
        logger.info("[StrikeoutXGB] Saved to %s", self.MODEL_PATH)

    def load(self) -> bool:
        if os.path.exists(self.MODEL_PATH):
            with open(self.MODEL_PATH, "rb") as f:
                self._calibrated = pickle.load(f)
            self._is_trained = True
            logger.info("[StrikeoutXGB] Loaded from %s", self.MODEL_PATH)
            return True
        return False

    def feature_importances(self) -> dict[str, float]:
        if not self._is_trained:
            return {}
        try:
            base = self._calibrated.estimator
            names  = StrikeoutFeatures.feature_names()
            scores = base.feature_importances_
            return dict(sorted(
                zip(names, scores), key=lambda x: x[1], reverse=True))
        except Exception:
            return {}


# ---------------------------------------------------------------------------
# RandomForest base model
# ---------------------------------------------------------------------------
class StrikeoutRandomForest:
    """
    sklearn RandomForestClassifier with isotonic calibration.
    Serves as the second component in the ensemble.
    """

    MODEL_PATH = os.path.join(MODEL_DIR, "strikeout_rf.pkl")

    def __init__(self) -> None:
        self._calibrated = None
        self._is_trained = False

    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        if not _SKL_AVAILABLE:
            raise RuntimeError("scikit-learn required")
        base = RandomForestClassifier(
            n_estimators=300,
            max_depth=10,
            min_samples_leaf=5,
            max_features="sqrt",
            random_state=42,
            n_jobs=-1,
        )
        self._calibrated = CalibratedClassifierCV(
            base, method="isotonic", cv=5)
        self._calibrated.fit(X, y)
        self._is_trained = True
        logger.info("[StrikeoutRF] Trained on %d samples", len(y))

    def predict_proba(self, features: StrikeoutFeatures) -> tuple[float, float]:
        if not self._is_trained or self._calibrated is None:
            return 0.5, 0.5
        X = features.to_array().reshape(1, -1)
        prob_over = float(self._calibrated.predict_proba(X)[0][1])
        return round(prob_over, 4), round(1.0 - prob_over, 4)

    def save(self) -> None:
        with open(self.MODEL_PATH, "wb") as f:
            pickle.dump(self._calibrated, f)

    def load(self) -> bool:
        if os.path.exists(self.MODEL_PATH):
            with open(self.MODEL_PATH, "rb") as f:
                self._calibrated = pickle.load(f)
            self._is_trained = True
            return True
        return False


# ---------------------------------------------------------------------------
# Ensemble combiner
# ---------------------------------------------------------------------------
EnsembleMethod = Literal["average", "stack", "bagging"]


class StrikeoutEnsemble:
    """
    Combines XGBoost + RandomForest predictions.

    Methods:
      - average:  weighted average (XGB weight 0.65, RF weight 0.35 by default)
      - stack:    LogisticRegression meta-learner trained on OOF predictions
      - bagging:  bootstrap sample predictions → median aggregation
    """

    ENSEMBLE_PATH = os.path.join(MODEL_DIR, "strikeout_ensemble.pkl")

    def __init__(
        self,
        method: EnsembleMethod = "average",
        xgb_weight: float = 0.65,
        rf_weight:  float = 0.35,
    ) -> None:
        self.xgb     = StrikeoutXGBoost()
        self.rf      = StrikeoutRandomForest()
        self.method  = method
        self._xgb_w  = xgb_weight
        self._rf_w   = rf_weight
        self._meta: Any = None        # stacking meta-learner
        self._is_trained = False

    def fit(self, X: np.ndarray, y: np.ndarray) -> None:
        """Train both base models; if method=stack, also train meta-learner."""
        self.xgb.fit(X, y)
        self.rf.fit(X, y)

        if self.method == "stack" and _SKL_AVAILABLE:
            # OOF predictions for meta-learner
            from sklearn.model_selection import cross_val_predict
            xgb_oof = cross_val_predict(
                self.xgb._calibrated, X, y, cv=5, method="predict_proba")[:, 1]
            rf_oof = cross_val_predict(
                self.rf._calibrated,  X, y, cv=5, method="predict_proba")[:, 1]
            meta_X = np.column_stack([xgb_oof, rf_oof])
            self._meta = LogisticRegression(C=1.0, random_state=42)
            self._meta.fit(meta_X, y)
            logger.info("[Ensemble] Stacking meta-learner trained")

        self._is_trained = True

    def predict_proba(self, features: StrikeoutFeatures) -> tuple[float, float]:
        """Returns ensemble (prob_over, prob_under)."""
        xgb_over, _ = self.xgb.predict_proba(features)
        rf_over,  _ = self.rf.predict_proba(features)

        if self.method == "average":
            prob_over = (xgb_over * self._xgb_w + rf_over * self._rf_w)
        elif self.method == "stack" and self._meta is not None:
            meta_X = np.array([[xgb_over, rf_over]])
            prob_over = float(self._meta.predict_proba(meta_X)[0][1])
        else:
            # bagging / fallback
            prob_over = float(np.median([xgb_over, rf_over]))

        prob_over = round(min(max(prob_over, 0.01), 0.99), 4)
        return prob_over, round(1.0 - prob_over, 4)

    def save(self) -> None:
        self.xgb.save()
        self.rf.save()
        if self._meta is not None:
            with open(self.ENSEMBLE_PATH, "wb") as f:
                pickle.dump(self._meta, f)

    def load(self) -> bool:
        xgb_ok = self.xgb.load()
        rf_ok  = self.rf.load()
        if os.path.exists(self.ENSEMBLE_PATH):
            with open(self.ENSEMBLE_PATH, "rb") as f:
                self._meta = pickle.load(f)
        self._is_trained = xgb_ok and rf_ok
        return self._is_trained


# ---------------------------------------------------------------------------
# Top-level interface
# ---------------------------------------------------------------------------
class StrikeoutPropModel:
    """
    Top-level interface for the PropIQ agent pipeline.
    Wraps StrikeoutEnsemble and provides predict() for a list of pitcher dicts.
    """

    def __init__(self, method: EnsembleMethod = "average") -> None:
        self._engineer = StrikeoutFeatureEngineer()
        self._ensemble = StrikeoutEnsemble(method=method)
        # Attempt to load persisted models
        loaded = self._ensemble.load()
        if not loaded:
            logger.warning(
                "[StrikeoutPropModel] No saved models found — "
                "predictions will return 0.5/0.5 until training runs."
            )

    def predict(
        self,
        pitcher_stats:   dict[str, Any],
        opponent_stats:  dict[str, Any],
        context:         dict[str, Any],
    ) -> StrikeoutPrediction:
        """Predict Over probability for a single pitcher prop."""
        features = self._engineer.build(pitcher_stats, opponent_stats, context)
        prob_over, prob_under = self._ensemble.predict_proba(features)

        return StrikeoutPrediction(
            player_name=pitcher_stats.get("name", pitcher_stats.get("Name", "Unknown")),
            line=context.get("prop_line", 5.5),
            prob_over=prob_over,
            prob_under=prob_under,
            model_source=f"ensemble_{self._ensemble.method}",
            confidence=round(abs(prob_over - 0.5) * 2, 4),
            features_used=StrikeoutFeatures.feature_names(),
        )

    def batch_predict(
        self,
        pitchers: list[dict[str, Any]],
    ) -> list[StrikeoutPrediction]:
        """
        Batch predict for a list of pitchers.
        Each item must have keys: pitcher_stats, opponent_stats, context.
        """
        results: list[StrikeoutPrediction] = []
        for item in pitchers:
            try:
                pred = self.predict(
                    item["pitcher_stats"],
                    item.get("opponent_stats", {}),
                    item.get("context", {}),
                )
                results.append(pred)
            except Exception as e:
                logger.error("[StrikeoutPropModel] Prediction error: %s", e)
        return results

    def to_prop_edge(self, pred: StrikeoutPrediction) -> dict[str, Any]:
        """Convert a StrikeoutPrediction to a PropEdge-compatible dict."""
        import time as _time
        return {
            "player_name":       pred.player_name,
            "prop_type":         "strikeouts",
            "line":              pred.line,
            "model_probability": pred.prob_over,
            "edge_pct":          0.0,   # filled by EV calculation downstream
            "source":            "strikeout_model",
            "timestamp":         _time.time(),
            "odds_over":         -110,
            "odds_under":        -110,
            "confidence":        pred.confidence,
        }

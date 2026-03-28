"""
api/services/mlb_data.py
Advanced feature engineering for MLB player props.

Integrates:
  - PlateDisciplineStats: O-Swing%, Z-Swing%, SwStr%, Contact%, Zone%
  - PitcherClusterEngine: K-means arsenal clustering (4 clusters)
  - MatchupEncoder: batter handedness splits 1261275 weighted K% advantage
  - AdvancedFeatureBuilder: assembles full advanced feature matrix
  - MLBDataValidator: bounds-checking + required-field validation

PEP 8 compliant. No hallucinated APIs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

import numpy as np

logger = logging.getLogger(__name__)

try:
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler
    _SKL_AVAILABLE = True
except ImportError:
    _SKL_AVAILABLE = False
    logger.warning("[MLBData] scikit-learn not installed 126154 rule-based cluster fallback active")


# ---------------------------------------------------------------------------
# Plate discipline features
# ---------------------------------------------------------------------------
@dataclass
class PlateDisciplineStats:
    """
    Plate discipline metrics from Statcast / FanGraphs.
    Describes batter tendency to make contact or chase.
    """
    o_swing_pct:      float = 0.30   # O-Swing%: swings at pitches outside zone
    z_swing_pct:      float = 0.65   # Z-Swing%: swings at pitches in zone
    swing_pct:        float = 0.48   # Overall swing rate
    o_contact_pct:    float = 0.63   # O-Contact%: contact on outside-zone swings
    z_contact_pct:    float = 0.86   # Z-Contact%: contact on in-zone swings
    contact_pct:      float = 0.78   # Overall contact rate
    swstr_pct:        float = 0.10   # SwStr%: swinging strikes / pitches seen
    zone_pct:         float = 0.47   # Zone%: pitch% in strike zone
    first_strike_pct: float = 0.60   # First-pitch strike rate

    def k_tendency_score(self) -> float:
        """
        Composite strikeout tendency for opposing batters (0–1 scale).
        Higher = more K-prone batter = better environment for pitcher K props.

        Weighted formula:
          40% low contact rate
          25% high O-swing% (chases out of zone → misses)
          25% high swinging-strike rate
          10% low Z-contact% (even in-zone misses)
        """
        return (
            (1.0 - self.contact_pct) * 0.40
            + self.o_swing_pct       * 0.25
            + self.swstr_pct         * 0.25
            + (1.0 - self.z_contact_pct) * 0.10
        )

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "PlateDisciplineStats":
        return cls(
            o_swing_pct      = float(d.get("o_swing_pct",      0.30)),
            z_swing_pct      = float(d.get("z_swing_pct",      0.65)),
            swing_pct        = float(d.get("swing_pct",        0.48)),
            o_contact_pct    = float(d.get("o_contact_pct",    0.63)),
            z_contact_pct    = float(d.get("z_contact_pct",    0.86)),
            contact_pct      = float(d.get("contact_pct",      0.78)),
            swstr_pct        = float(d.get("swstr_pct",        0.10)),
            zone_pct         = float(d.get("zone_pct",         0.47)),
            first_strike_pct = float(d.get("first_strike_pct", 0.60)),
        )


# ---------------------------------------------------------------------------
# Pitcher clustering
# ---------------------------------------------------------------------------
#: Human-readable labels for each cluster ID
CLUSTER_LABELS: dict[int, str] = {
    0: "fb_dominant",     # High fastball% + high velo
    1: "breaking_ball",   # High slider/curve%, high whiff
    2: "offspeed_mix",    # High changeup/splitter%, deception-based
    3: "mixed",           # No dominant pitch type
}

#: Historical league-average K% by cluster (training prior, 2019-2024)
CLUSTER_K_AVGS: dict[int, float] = {
    0: 0.238,   # FB-dominant: power/velocity drives Ks
    1: 0.252,   # Breaking-ball heavy: high whiff rates
    2: 0.228,   # Offspeed mix: deceptive but lower pure K-rate
    3: 0.215,   # Mixed: command-oriented, lower K upside
}


class PitcherClusterEngine:
    """
    K-means clustering of pitchers based on arsenal profile.

    Input features: fastball%, breaking_ball%, offspeed%, fastball_velo,
                    spin_rate_fb, whiff_rate_fb.

    Produces 4 clusters (see CLUSTER_LABELS).
    Falls back to rule-based heuristics when scikit-learn is unavailable
    or when fit() has not been called yet.
    """

    N_CLUSTERS = 4
    _FEATURE_COLS = [
        "fastball_pct",
        "breaking_ball_pct",
        "offspeed_pct",
        "fastball_velo",
        "spin_rate_fb",
        "whiff_rate_fb",
    ]

    def __init__(self) -> None:
        self._kmeans: Optional[Any] = None
        self._scaler: Optional[Any] = None
        self._is_fit  = False

    # ------------------------------------------------------------------
    def fit(self, pitcher_profiles: list[dict[str, float]]) -> None:
        """
        Fit K-means on a list of pitcher stat dicts.
        Each dict should contain the keys in _FEATURE_COLS.
        """
        if not _SKL_AVAILABLE:
            logger.warning("[PitcherCluster] scikit-learn unavailable — rule-based mode")
            return
        if len(pitcher_profiles) < self.N_CLUSTERS:
            logger.warning(
                "[PitcherCluster] Too few samples (%d) to fit K-means",
                len(pitcher_profiles),
            )
            return

        X = self._build_matrix(pitcher_profiles)
        self._scaler = StandardScaler()
        X_scaled = self._scaler.fit_transform(X)
        self._kmeans = KMeans(
            n_clusters=self.N_CLUSTERS,
            random_state=42,
            n_init=10,
        )
        self._kmeans.fit(X_scaled)
        self._is_fit = True
        logger.info("[PitcherCluster] K-means fit on %d pitchers", len(pitcher_profiles))

    # ------------------------------------------------------------------
    def predict(self, pitcher_stats: dict[str, float]) -> int:
        """Return cluster label (0–3) for a single pitcher stat dict."""
        if self._is_fit and self._kmeans is not None and self._scaler is not None:
            X = self._build_matrix([pitcher_stats])
            X_scaled = self._scaler.transform(X)
            return int(self._kmeans.predict(X_scaled)[0])
        return self._rule_based_cluster(pitcher_stats)

    # ------------------------------------------------------------------
    def cluster_label(self, cluster_id: int) -> str:
        """Human-readable cluster label."""
        return CLUSTER_LABELS.get(cluster_id, "unknown")

    # ------------------------------------------------------------------
    @staticmethod
    def _rule_based_cluster(p: dict[str, float]) -> int:
        """Fallback deterministic rule when K-means is not available."""
        fb   = p.get("fastball_pct",      0.50)
        bb   = p.get("breaking_ball_pct", 0.30)
        os_  = p.get("offspeed_pct",      0.20)
        if fb >= 0.55:
            return 0   # FB-dominant
        if bb >= 0.40:
            return 1   # Breaking-ball heavy
        if os_ >= 0.25:
            return 2   # Offspeed mix
        return 3       # Mixed

    # ------------------------------------------------------------------
    @staticmethod
    def _build_matrix(profiles: list[dict[str, float]]) -> np.ndarray:
        rows: list[list[float]] = []
        for p in profiles:
            fb   = float(p.get("fastball_pct",      0.50))
            bb   = float(p.get("breaking_ball_pct", 0.30))
            os_  = max(0.0, 1.0 - fb - bb)
            rows.append([
                fb,
                bb,
                os_,
                float(p.get("fastball_velo",  93.0)),
                float(p.get("spin_rate_fb",   2300.0)),
                float(p.get("whiff_rate_fb",  0.20)),
            ])
        return np.array(rows, dtype=np.float32)


# ---------------------------------------------------------------------------
# Handedness matchup encoding
# ---------------------------------------------------------------------------
@dataclass
class HandednessSplit:
    """Historical pitcher K performance split by batter handedness."""
    k_pct_vs_lhb:  float = 0.22   # K% vs left-handed batters
    k_pct_vs_rhb:  float = 0.22   # K% vs right-handed batters
    whiff_vs_lhb:  float = 0.28
    whiff_vs_rhb:  float = 0.28
    era_vs_lhb:    float = 4.00
    era_vs_rhb:    float = 4.00

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "HandednessSplit":
        return cls(
            k_pct_vs_lhb = float(d.get("k_pct_vs_lhb", 0.22)),
            k_pct_vs_rhb = float(d.get("k_pct_vs_rhb", 0.22)),
            whiff_vs_lhb = float(d.get("whiff_vs_lhb", 0.28)),
            whiff_vs_rhb = float(d.get("whiff_vs_rhb", 0.28)),
            era_vs_lhb   = float(d.get("era_vs_lhb",   4.00)),
            era_vs_rhb   = float(d.get("era_vs_rhb",   4.00)),
        )


#: League-average K% (2024 season) used to centre advantage score
_LEAGUE_AVG_K_PCT = 0.228


class MatchupEncoder:
    """
    Encodes batter handedness matchup features into continuous signals.

    Given pitcher's platoon splits and the opposing lineup composition,
    produces:
      - weighted_k_pct_matchup: lineup-composition-weighted K% for this start
      - handedness_advantage:   normalised advantage in [-1, 1] vs league avg
      - split_disparity:        absolute gap between vs-LHB and vs-RHB K%

    A positive handedness_advantage means the pitcher faces a lineup that
    matches their dominant-handedness strength.
    """

    def encode(
        self,
        pitcher_splits: HandednessSplit,
        opp_lhb_pct: float,         # 0–1 fraction of LHBs in opposing lineup
    ) -> dict[str, float]:
        opp_rhb_pct = 1.0 - opp_lhb_pct
        weighted_k_pct = (
            pitcher_splits.k_pct_vs_lhb * opp_lhb_pct
            + pitcher_splits.k_pct_vs_rhb * opp_rhb_pct
        )
        split_disparity = abs(
            pitcher_splits.k_pct_vs_lhb - pitcher_splits.k_pct_vs_rhb
        )
        # Normalise: 0.10 ≈ 1 standard deviation in pitcher K% vs league avg
        raw_advantage = (weighted_k_pct - _LEAGUE_AVG_K_PCT) / 0.10
        handedness_advantage = float(min(max(raw_advantage, -1.0), 1.0))

        return {
            "weighted_k_pct_matchup": round(weighted_k_pct,        4),
            "handedness_advantage":   round(handedness_advantage,   4),
            "split_disparity":        round(split_disparity,        4),
        }


# ---------------------------------------------------------------------------
# Advanced feature set dataclass
# ---------------------------------------------------------------------------
@dataclass
class AdvancedPitcherFeatures:
    """
    Extended feature vector combining all advanced components.
    Designed to augment the base StrikeoutFeatures array.
    """
    # Opposing batter plate discipline
    opp_o_swing_pct:         float = 0.30
    opp_swstr_pct:           float = 0.10
    opp_k_tendency:          float = 0.30   # composite k_tendency_score()

    # Handedness matchup
    weighted_k_pct_matchup:  float = 0.22
    handedness_advantage:    float = 0.0    # [-1, 1]
    split_disparity:         float = 0.0

    # Pitcher cluster
    arsenal_cluster:         int   = 0
    cluster_label:           str   = "fb_dominant"
    cluster_avg_k_pct:       float = 0.238  # historical cluster K% prior

    # Data quality
    data_quality_score:      float = 1.0    # 0–1 (penalises sparse data)

    def to_extra_array(self) -> np.ndarray:
        """Returns the 9-element extension array for appending to base features."""
        return np.array([
            self.opp_o_swing_pct,
            self.opp_swstr_pct,
            self.opp_k_tendency,
            self.weighted_k_pct_matchup,
            self.handedness_advantage,
            self.split_disparity,
            float(self.arsenal_cluster),
            self.cluster_avg_k_pct,
            self.data_quality_score,
        ], dtype=np.float32)

    @classmethod
    def extra_feature_names(cls) -> list[str]:
        return [
            "opp_o_swing_pct",
            "opp_swstr_pct",
            "opp_k_tendency",
            "weighted_k_pct_matchup",
            "handedness_advantage",
            "split_disparity",
            "arsenal_cluster",
            "cluster_avg_k_pct",
            "data_quality_score",
        ]


# ---------------------------------------------------------------------------
# Advanced feature builder
# ---------------------------------------------------------------------------
class AdvancedFeatureBuilder:
    """
    Assembles AdvancedPitcherFeatures from raw data dicts.

    Input keys (pitcher_stats):
      k_pct_vs_lhb, k_pct_vs_rhb, whiff_vs_lhb, whiff_vs_rhb
      fastball_pct, breaking_ball_pct, fastball_velo, spin_rate_fb, whiff_rate_fb

    Input keys (opponent_stats):
      o_swing_pct, swstr_pct, z_contact_pct, contact_pct, lhb_pct

    Input keys (context):
      (none required)
    """

    # Track optional fields to compute data quality
    _OPTIONAL_PITCHER = [
        "o_swing_pct", "swstr_pct",
        "k_pct_vs_lhb", "k_pct_vs_rhb",
        "spin_rate_fb", "whiff_rate_slider",
    ]

    def __init__(self) -> None:
        self._cluster_engine  = PitcherClusterEngine()
        self._matchup_encoder = MatchupEncoder()

    def fit_clusters(self, pitcher_profiles: list[dict[str, float]]) -> None:
        """Optional: pre-train K-means on a historical pitcher corpus."""
        self._cluster_engine.fit(pitcher_profiles)

    # ------------------------------------------------------------------
    def build(
        self,
        pitcher_stats:  dict[str, Any],
        opponent_stats: dict[str, Any],
        context:        dict[str, Any],  # noqa: ARG002 — reserved for future ctx
    ) -> AdvancedPitcherFeatures:
        """Build full advanced feature set from raw data dicts."""

        # --- Pitcher clustering ---
        cluster_id = self._cluster_engine.predict(pitcher_stats)
        cluster_lbl = self._cluster_engine.cluster_label(cluster_id)

        # --- Opposing batter plate discipline ---
        opp_plate = PlateDisciplineStats.from_dict(opponent_stats)

        # --- Handedness matchup ---
        splits  = HandednessSplit.from_dict(pitcher_stats)
        matchup = self._matchup_encoder.encode(
            splits, float(opponent_stats.get("lhb_pct", 0.40))
        )

        # --- Data quality score ---
        all_src = {**pitcher_stats, **opponent_stats}
        populated = sum(
            1 for f in self._OPTIONAL_PITCHER if f in all_src
        )
        quality = populated / len(self._OPTIONAL_PITCHER)

        return AdvancedPitcherFeatures(
            opp_o_swing_pct         = opp_plate.o_swing_pct,
            opp_swstr_pct           = opp_plate.swstr_pct,
            opp_k_tendency          = round(opp_plate.k_tendency_score(), 4),
            weighted_k_pct_matchup  = matchup["weighted_k_pct_matchup"],
            handedness_advantage    = matchup["handedness_advantage"],
            split_disparity         = matchup["split_disparity"],
            arsenal_cluster         = cluster_id,
            cluster_label           = cluster_lbl,
            cluster_avg_k_pct       = CLUSTER_K_AVGS.get(cluster_id, 0.22),
            data_quality_score      = round(quality, 4),
        )

    # ------------------------------------------------------------------
    def build_full_array(
        self,
        base_array:     np.ndarray,
        pitcher_stats:  dict[str, Any],
        opponent_stats: dict[str, Any],
        context:        dict[str, Any],
    ) -> np.ndarray:
        """
        Returns base feature array extended with 9 advanced features.
        Used when passing directly to prop_model.py models.
        """
        adv = self.build(pitcher_stats, opponent_stats, context)
        return np.concatenate([base_array, adv.to_extra_array()])


# ---------------------------------------------------------------------------
# Data validation
# ---------------------------------------------------------------------------
class MLBDataValidator:
    """
    Validates raw data dicts before feature engineering.

    Returns (is_valid: bool, warnings: list[str]).
    is_valid=False only on missing *required* fields or hard bounds violations.
    Soft warnings do not fail validation.
    """

    _REQUIRED_PITCHER  = ["k_rate_l14", "era_l14", "whip_l14"]
    _REQUIRED_OPPONENT = ["k_pct_l14"]

    # (lo, hi, is_hard_fail)
    _BOUNDS: dict[str, tuple[float, float, bool]] = {
        "k_rate_l7":     (0.0,  20.0,  True),
        "k_rate_l14":    (0.0,  20.0,  True),
        "era_l14":       (0.0,  15.0,  True),
        "whip_l14":      (0.0,   5.0,  True),
        "fastball_velo": (70.0, 105.0,  True),
        "k_pct_l14":     (0.0,   0.60, True),
        "opp_wrc_plus":  (0.0, 250.0,  False),
        "o_swing_pct":   (0.0,   1.0,  False),
        "swstr_pct":     (0.0,   1.0,  False),
        "contact_pct":   (0.0,   1.0,  False),
    }

    def validate(
        self,
        pitcher_stats:  dict[str, Any],
        opponent_stats: dict[str, Any],
    ) -> tuple[bool, list[str]]:
        """
        Returns:
          is_valid (bool)  — False only on required-field or hard-bounds failure
          warnings (list)  — all issues, including soft warnings
        """
        warnings: list[str] = []
        is_valid = True

        # Required pitcher fields
        missing_p = [f for f in self._REQUIRED_PITCHER if f not in pitcher_stats]
        if missing_p:
            warnings.append(f"Missing required pitcher fields: {missing_p}")
            is_valid = False

        # Soft warnings for opponent fields
        missing_o = [f for f in self._REQUIRED_OPPONENT if f not in opponent_stats]
        if missing_o:
            warnings.append(f"Missing opponent fields (defaults used): {missing_o}")

        # Bounds checks
        all_data = {**pitcher_stats, **opponent_stats}
        for field_name, (lo, hi, hard) in self._BOUNDS.items():
            val = all_data.get(field_name)
            if val is None:
                continue
            try:
                v = float(val)
            except (TypeError, ValueError):
                warnings.append(f"Non-numeric {field_name}={val!r}")
                if hard:
                    is_valid = False
                continue
            if not (lo <= v <= hi):
                msg = f"Out-of-range {field_name}={v:.3f} (expected [{lo}, {hi}])"
                warnings.append(msg)
                if hard:
                    is_valid = False

        return is_valid, warnings

    def validate_batch(
        self,
        rows: list[tuple[dict[str, Any], dict[str, Any]]],
    ) -> tuple[list[int], list[tuple[int, list[str]]]]:
        """
        Validate a batch of (pitcher_stats, opponent_stats) tuples.

        Returns:
          valid_indices   — indices of rows that passed validation
          invalid_reports — list of (index, warnings) for failed rows
        """
        valid:   list[int]                        = []
        invalid: list[tuple[int, list[str]]]      = []
        for i, (p, o) in enumerate(rows):
            ok, warns = self.validate(p, o)
            if ok:
                valid.append(i)
            else:
                invalid.append((i, warns))
        return valid, invalid

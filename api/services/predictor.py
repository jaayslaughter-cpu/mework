import os
import logging
import numpy as np
import pandas as pd
import xgboost as xgb
from xgboost import XGBClassifier
from services.fatigue_logic import apply_fatigue_adjustments
from services.usage_vacuums import evaluate_player_context
from services.defensive_contrast import evaluate_defensive_contrast
from services.abs_challenge import apply_abs_to_probability, ABSContext

logger = logging.getLogger(__name__)

MODEL_DIR = os.path.join(os.path.dirname(__file__), "../models")
os.makedirs(MODEL_DIR, exist_ok=True)

# ── Model Registry ──────────────────────────────────────────────────
_models: dict = {}

def _load_model(model_name: str) -> xgb.XGBClassifier | None:
    """Load an XGBoost model from disk with caching. Returns None if not found."""
    if model_name in _models:
        return _models[model_name]

    path = os.path.join(MODEL_DIR, f"{model_name}.json")
    if not os.path.exists(path):
        logger.warning("[Predictor] Model file not found: %s. Using scaffold probability.", path)
        return None

    try:
        model = xgb.XGBClassifier()
        model.load_model(path)
        _models[model_name] = model
        logger.info("[Predictor] Loaded model: %s", model_name)
        return model
    except Exception as e:
        logger.error("[Predictor] Failed to load %s: %s", model_name, e)
        return None


def _select_model_for_prop(prop_category: str) -> tuple[str, XGBClassifier | None]:
    """Map a prop category to the correct model."""
    category_lower = prop_category.lower()
    if "home_run" in category_lower or "hr" in category_lower:
        return "hr_model_v1", _load_model("hr_model_v1")
    elif "total_bases" in category_lower or "xbh" in category_lower:
        return "xbh_model_v1", _load_model("xbh_model_v1")
    else:
        return "prop_model_v1", _load_model("prop_model_v1")


def calculate_implied_probability(american_odds: int) -> float:
    """Convert American odds to implied probability."""
    if american_odds is None:
        return 0.0
    if american_odds < 0:
        return (-american_odds) / (-american_odds + 100)
    else:
        return 100 / (american_odds + 100)


def _build_feature_vector(statcast_data: list) -> np.ndarray | None:
    """
    Build an 8-feature numpy array from statcast pitch records.
    Feature order must match training:
    [release_speed, release_spin_rate, launch_speed, launch_angle,
     is_barrel, is_barrel_expanded, is_hard_hit, is_sweet_spot]
    Returns None if insufficient data.
    """
    if not statcast_data:
        return None

    df = pd.DataFrame(statcast_data)
    required = ['release_speed', 'release_spin_rate', 'launch_speed', 'launch_angle']
    available = [c for c in required if c in df.columns]

    if len(available) < 2:
        return None

    # Fill missing columns with league averages
    defaults = {
        'release_speed': 93.5,
        'release_spin_rate': 2200.0,
        'launch_speed': 88.5,
        'launch_angle': 12.0,
    }
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default

    df = df[list(defaults.keys())].dropna()
    if df.empty:
        return None

    # Aggregate to single representative row (most recent / mean)
    row = df.mean()

    # Derived binary features
    ev = row['launch_speed']
    la = row['launch_angle']
    is_barrel = 1 if (ev >= 98 and 26 <= la <= 30) else 0
    is_barrel_exp = 1 if (ev >= 98 and 24 <= la <= 33) else 0
    is_hard_hit = 1 if ev >= 95 else 0
    is_sweet_spot = 1 if (8 <= la <= 32) else 0

    return np.array([[
        row['release_speed'],
        row['release_spin_rate'],
        row['launch_speed'],
        row['launch_angle'],
        is_barrel,
        is_barrel_exp,
        is_hard_hit,
        is_sweet_spot,
    ]])


def evaluate_edge(
    sportsbook_line: float,
    over_odds: int,
    under_odds: int,
    statcast_data: list,
    fatigue_context: dict = None,
    vacuum_context: dict = None,
    contrast_context: dict = None,
    prop_category: str = "",
    abs_context: ABSContext = None,
) -> dict:
    """
    Core ML Evaluation Logic.
    1. De-vig to get true Vegas probability
    2. Run XGBoost model if statcast features available, else use calibrated scaffold
    3. Apply fatigue, vacuum, and defensive contrast multipliers
    4. Apply ABS (Automated Ball-Strike) challenge multiplier for K/BB props
    5. Calculate edge vs. market

    abs_context: Optional ABSContext dataclass. When provided and prop_category
    is pitcher_strikeouts, batter_strikeouts, batter_walks, or pitcher_walks,
    a height/challenge-rate multiplier is applied. All other prop types are
    unaffected (multiplier = 1.0).
    """
    # 1. Vegas implied probabilities (de-vigged)
    implied_over = calculate_implied_probability(over_odds)
    implied_under = calculate_implied_probability(under_odds)
    vig = implied_over + implied_under - 1.0
    true_vegas_over = implied_over - (vig / 2) if vig > 0 else implied_over

    # 2. XGBoost prediction
    model_name, model = _select_model_for_prop(prop_category)
    feature_vec = _build_feature_vector(statcast_data) if statcast_data else None

    if model is not None and feature_vec is not None:
        try:
            base_model_prob = float(model.predict_proba(feature_vec)[0][1])
            model_source = f"xgboost:{model_name}"
        except Exception as e:
            logger.warning("[Predictor] Model inference failed: %s. Using scaffold.", e)
            base_model_prob = 0.55
            model_source = "scaffold:fallback"
    else:
        # Calibrated scaffold based on prop type until model is trained
        scaffolds = {
            "pitcher_strikeouts": 0.54,
            "batter_total_bases": 0.53,
            "batter_home_runs": 0.51,
            "batter_hits_runs_rbis": 0.55,
        }
        base_model_prob = scaffolds.get(prop_category.lower(), 0.55)
        model_source = "scaffold:pre-training"

    # 3. Apply Fatigue Adjustments
    if fatigue_context:
        player_type = fatigue_context.get("player_type", "batter")
        adjusted_prob = apply_fatigue_adjustments(
            base_projection=base_model_prob,
            player_type=player_type,
            context=fatigue_context,
        )
    else:
        adjusted_prob = base_model_prob

    # 4. Apply Usage Vacuum Boost
    vacuum_multiplier = 1.0
    if vacuum_context:
        player_id = vacuum_context.get("player_id", "unknown")
        vacuum_multiplier = evaluate_player_context(player_id, vacuum_context)
        adjusted_prob = adjusted_prob * vacuum_multiplier

    # 5. Apply Defensive Contrast
    contrast_multiplier = 1.0
    if contrast_context and prop_category:
        contrast_multiplier = evaluate_defensive_contrast(prop_category, contrast_context)
        adjusted_prob = adjusted_prob * contrast_multiplier

    # 6. Apply ABS (Automated Ball-Strike) Challenge Modifier
    # Affects: pitcher_strikeouts, batter_strikeouts, batter_walks, pitcher_walks
    # All other prop types pass through with abs_multiplier = 1.0
    abs_multiplier = 1.0
    abs_applied = False
    if abs_context is not None:
        adjusted_prob, abs_multiplier = apply_abs_to_probability(
            base_probability=adjusted_prob,
            prop_type=prop_category,
            abs_context=abs_context,
        )
        abs_applied = abs_multiplier != 1.0

    model_projected_over_prob = min(adjusted_prob, 0.95)

    # 7. Calculate edge
    edge_percentage = (model_projected_over_prob - true_vegas_over) * 100

    return {
        "line": sportsbook_line,
        "prop_category": prop_category,
        "vegas_implied_over": round(true_vegas_over * 100, 2),
        "model_projected_over": round(model_projected_over_prob * 100, 2),
        "edge_percentage": round(edge_percentage, 2),
        "is_playable": edge_percentage > 3.0,
        "model_source": model_source,
        "fatigue_adjusted": fatigue_context is not None,
        "vacuum_boost_applied": vacuum_multiplier > 1.0,
        "vacuum_multiplier": round(vacuum_multiplier, 3),
        "contrast_boost_applied": contrast_multiplier != 1.0,
        "contrast_multiplier": round(contrast_multiplier, 3),
        "abs_applied": abs_applied,
        "abs_multiplier": round(abs_multiplier, 5),
        "vig_removed": round(vig * 100, 2),
    }

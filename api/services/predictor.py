import os
from services.fatigue_logic import apply_fatigue_adjustments
from services.usage_vacuums import evaluate_player_context
from services.defensive_contrast import evaluate_defensive_contrast

# Placeholder for future trained model
MODEL_DIR = os.path.join(os.path.dirname(__file__), "../models")
os.makedirs(MODEL_DIR, exist_ok=True)

def calculate_implied_probability(american_odds: int) -> float:
    """Convert American odds to implied probability."""
    if american_odds is None:
        return 0.0
    if american_odds < 0:
        return (-american_odds) / (-american_odds + 100)
    else:
        return 100 / (american_odds + 100)

def evaluate_edge(sportsbook_line: float, over_odds: int, under_odds: int, _statcast_data: list, fatigue_context: dict = None, vacuum_context: dict = None, contrast_context: dict = None, prop_category: str = "") -> dict:
    """
    Core ML Evaluation Logic.
    Currently scaffolded to calculate baseline probabilities until the .xgb model is trained.
    Applies fatigue adjustments, usage vacuum boosts, and defensive contrast when context is provided.
    """
    # 1. Calculate Vegas Implied Probabilities
    implied_over = calculate_implied_probability(over_odds)
    implied_under = calculate_implied_probability(under_odds)
    
    # Remove vig (sportsbook juice) for true Vegas probability
    vig = implied_over + implied_under - 1.0
    true_vegas_over = implied_over - (vig / 2) if vig > 0 else implied_over
    
    # 2. XGBoost Prediction Scaffold
    # TODO: Load actual xgb.Booster and run model.predict(DMatrix) based on statcast_data features
    # For scaffold: We simulate a model projection (e.g., historical hit rate)
    base_model_prob = 0.55  # Placeholder: Engine will replace this with real math
    
    # 3. Apply Fatigue Adjustments
    if fatigue_context:
        player_type = fatigue_context.get("player_type", "batter")
        adjusted_prob = apply_fatigue_adjustments(
            base_projection=base_model_prob,
            player_type=player_type,
            context=fatigue_context
        )
    else:
        adjusted_prob = base_model_prob
    
    # 4. Apply Usage Vacuum Boost
    vacuum_multiplier = 1.0
    if vacuum_context:
        player_id = vacuum_context.get("player_id", "unknown")
        vacuum_multiplier = evaluate_player_context(player_id, vacuum_context)
        adjusted_prob = adjusted_prob * vacuum_multiplier
    
    # 5. Apply Defensive Contrast (Pitcher/Hitter Profile Mismatch)
    contrast_multiplier = 1.0
    if contrast_context and prop_category:
        contrast_multiplier = evaluate_defensive_contrast(prop_category, contrast_context)
        adjusted_prob = adjusted_prob * contrast_multiplier
    
    model_projected_over_prob = min(adjusted_prob, 0.95)  # Cap at 95%
    
    # 6. Calculate the Edge
    edge_percentage = (model_projected_over_prob - true_vegas_over) * 100
    
    return {
        "line": sportsbook_line,
        "vegas_implied_over": round(true_vegas_over * 100, 2),
        "model_projected_over": round(model_projected_over_prob * 100, 2),
        "edge_percentage": round(edge_percentage, 2),
        "is_playable": edge_percentage > 3.0,  # Threshold for a +EV bet
        "fatigue_adjusted": fatigue_context is not None,
        "vacuum_boost_applied": vacuum_multiplier > 1.0,
        "vacuum_multiplier": vacuum_multiplier,
        "contrast_boost_applied": contrast_multiplier != 1.0,
        "contrast_multiplier": contrast_multiplier
    }

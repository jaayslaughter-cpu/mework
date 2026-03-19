import xgboost as xgb
import pandas as pd
import numpy as np
import os

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

def evaluate_edge(sportsbook_line: float, over_odds: int, under_odds: int, statcast_data: list) -> dict:
    """
    Core ML Evaluation Logic.
    Currently scaffolded to calculate baseline probabilities until the .xgb model is trained.
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
    model_projected_over_prob = 0.55  # Placeholder: Engine will replace this with real math
    
    # 3. Calculate the Edge
    edge_percentage = (model_projected_over_prob - true_vegas_over) * 100
    
    return {
        "line": sportsbook_line,
        "vegas_implied_over": round(true_vegas_over * 100, 2),
        "model_projected_over": round(model_projected_over_prob * 100, 2),
        "edge_percentage": round(edge_percentage, 2),
        "is_playable": edge_percentage > 3.0  # Threshold for a +EV bet
    }

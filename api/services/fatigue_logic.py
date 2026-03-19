from datetime import datetime, timedelta
import math

def calculate_pitcher_fatigue(days_rest: int, recent_pitch_count_sum: int, is_starter: bool) -> float:
    """
    Calculates a fatigue multiplier for pitchers.
    Returns a float: 1.0 is baseline. < 1.0 means fatigued (expect worse performance).
    """
    multiplier = 1.0
    
    if is_starter:
        # Starters typically need 4-5 days rest. 
        if days_rest < 4:
            multiplier -= 0.05 * (4 - days_rest)
        if recent_pitch_count_sum > 105:
            # Taxed in previous outing
            multiplier -= 0.03
    else:
        # Relievers pitching back-to-back days
        if days_rest == 0:
            multiplier -= 0.08
        elif days_rest == 1 and recent_pitch_count_sum > 30:
            multiplier -= 0.04
            
    return round(max(0.75, multiplier), 3)

def calculate_travel_fatigue(home_team: str, away_team: str, away_team_previous_city: str, rest_days: int) -> float:
    """
    Calculates travel fatigue based on time zone shifts and lack of rest days.
    Returns a multiplier (e.g., 0.96 for heavy travel fatigue).
    """
    multiplier = 1.0
    
    # Timezone Mapping (West = 1, Central = 2, East = 3)
    timezones = {
        # West Coast
        "LAD": 1, "SF": 1, "SD": 1, "SEA": 1, "LAA": 1, "OAK": 1, "ARI": 1, "COL": 1,
        # Central
        "CHC": 2, "CWS": 2, "MIL": 2, "STL": 2, "TEX": 2, "HOU": 2, "MIN": 2, "KC": 2, "CIN": 2, "CLE": 2, "DET": 2, "PIT": 2,
        # East Coast
        "NYY": 3, "NYM": 3, "BOS": 3, "PHI": 3, "BAL": 3, "WSH": 3, "ATL": 3, "MIA": 3, "TB": 3, "TOR": 3,
    }
    
    tz_away_current = timezones.get(away_team, 2)
    tz_away_previous = timezones.get(away_team_previous_city, tz_away_current)
    
    tz_shift = abs(tz_away_current - tz_away_previous)
    
    # If a team travels across 2 time zones with 0 days of rest, apply penalty
    if rest_days == 0 and tz_shift >= 2:
        multiplier -= 0.04
    elif rest_days == 0 and tz_shift == 1:
        multiplier -= 0.015
        
    return round(multiplier, 3)

def apply_fatigue_adjustments(base_projection: float, player_type: str, context: dict) -> float:
    """
    Master function to apply all fatigue multipliers to a base projection.
    """
    final_multiplier = 1.0
    
    if player_type == "pitcher":
        final_multiplier *= calculate_pitcher_fatigue(
            days_rest=context.get("days_rest", 4),
            recent_pitch_count_sum=context.get("recent_pitches", 0),
            is_starter=context.get("is_starter", True)
        )
        
    # Apply travel fatigue to batters and pitchers
    final_multiplier *= calculate_travel_fatigue(
        home_team=context.get("home_team", "UNK"),
        away_team=context.get("away_team", "UNK"),
        away_team_previous_city=context.get("previous_city", "UNK"),
        rest_days=context.get("team_rest_days", 1)
    )
    
    adjusted_projection = base_projection * final_multiplier
    return round(adjusted_projection, 2)

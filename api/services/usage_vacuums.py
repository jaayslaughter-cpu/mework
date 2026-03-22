def calculate_vacated_usage(standard_lineup: list, confirmed_lineup: list, player_usage_weights: dict) -> float:
    """
    Identifies missing players from the standard lineup and calculates the total 'Weight' 
    (Expected Opportunities) that is now vacated and available for the rest of the team.
    """
    missing_players = [player for player in standard_lineup if player not in confirmed_lineup]
    
    # Sum the usage weight of all missing players (e.g., a star might have a weight of 0.25)
    vacated_weight = sum([player_usage_weights.get(player, 0.0) for player in missing_players])
    
    return vacated_weight

def apply_vacuum_boost(_player_id: str, old_lineup_spot: int, new_lineup_spot: int, vacated_weight: float) -> float:
    """
    Calculates a positive multiplier for a player based on their new lineup position 
    and the total vacated usage on the team.
    Returns a float > 1.0.
    """
    multiplier = 1.0
    
    # 1. Lineup Spot Bump (Guaranteed extra Plate Appearances)
    if new_lineup_spot < old_lineup_spot:
        # E.g., Moving from 6th to 3rd is a jump of 3 spots.
        spot_jump = old_lineup_spot - new_lineup_spot
        # Add a roughly 4% baseline projection boost per spot moved up
        multiplier += (spot_jump * 0.04)
        
    # 2. General Usage Absorption
    # If the team's primary run producer is out, everyone gets slightly better pitches to hit
    # and a share of the vacated offensive load.
    if vacated_weight > 0:
        # Player absorbs a fraction of the missing star's weight
        multiplier += (vacated_weight * 0.15)
        
    return round(multiplier, 3)

def evaluate_player_context(player_id: str, context: dict) -> float:
    """
    Master function to be called by the predictor to get the vacuum multiplier.
    """
    standard_lineup = context.get("standard_lineup", [])
    confirmed_lineup = context.get("confirmed_lineup", [])
    player_usage_weights = context.get("usage_weights", {})
    
    old_spot = context.get("standard_lineup_spot", 9)
    new_spot = context.get("confirmed_lineup_spot", 9)
    
    # If no lineup changes, return standard baseline (1.0)
    if standard_lineup == confirmed_lineup and old_spot == new_spot:
        return 1.0
        
    vacated = calculate_vacated_usage(standard_lineup, confirmed_lineup, player_usage_weights)
    
    return apply_vacuum_boost(player_id, old_spot, new_spot, vacated)

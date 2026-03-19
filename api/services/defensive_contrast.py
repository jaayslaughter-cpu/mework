def calculate_profile_mismatch(pitcher_fb_rate: float, hitter_fb_rate: float, park_hr_factor: int) -> float:
    """
    Calculates a multiplier for Total Bases / Home Run props based on Flyball (FB) tendencies.
    Rates should be expressed as decimals (e.g., 42% flyball rate = 0.42).
    park_hr_factor: Baseline is 100. > 100 favors hitters (e.g., Coors Field = 115).
    """
    multiplier = 1.0
    
    # Thresholds for extreme flyball tendencies
    HIGH_FB_PITCHER_THRESHOLD = 0.40
    HIGH_FB_HITTER_THRESHOLD = 0.42
    
    # 1. The "Perfect Storm" Mismatch
    if pitcher_fb_rate > HIGH_FB_PITCHER_THRESHOLD and hitter_fb_rate > HIGH_FB_HITTER_THRESHOLD:
        # Both the pitcher and hitter are trying to put the ball in the air. 
        # This drastically increases the probability of extra-base hits.
        mismatch_severity = (pitcher_fb_rate - HIGH_FB_PITCHER_THRESHOLD) + (hitter_fb_rate - HIGH_FB_HITTER_THRESHOLD)
        multiplier += (mismatch_severity * 1.5)  # Scale the boost by how extreme the rates are
        
    # 2. The "Groundball Trap" Mismatch (Penalty)
    elif pitcher_fb_rate < 0.35 and hitter_fb_rate < 0.35:
        # Sinkerballer vs Groundball hitter = very few extra-base hits
        multiplier -= 0.05
        
    # 3. Apply Park Factor Scaling
    # We only care about park factors if the ball is actually going in the air
    if multiplier > 1.0 and park_hr_factor > 100:
        park_boost = (park_hr_factor - 100) / 100.0  # e.g., 115 -> 0.15 boost
        # Apply half the park boost to the multiplier so we don't over-inflate the projection
        multiplier += (park_boost * 0.5)
        
    elif multiplier > 1.0 and park_hr_factor < 100:
        park_penalty = (100 - park_hr_factor) / 100.0
        multiplier -= (park_penalty * 0.5)

    return round(max(0.70, multiplier), 3)

def evaluate_defensive_contrast(prop_category: str, context: dict) -> float:
    """
    Master function to apply the contrast multiplier.
    Only applies to power-related props.
    """
    # We do not apply flyball contrast to stolen bases or singles
    applicable_props = ["Total Bases", "Home Runs", "Hits+Runs+RBIs", "Earned Runs"]
    
    # Normalize the category name for comparison
    is_applicable = any(prop in prop_category for prop in applicable_props)
    
    if not is_applicable:
        return 1.0
        
    pitcher_fb = context.get("pitcher_fb_rate", 0.38)  # MLB Average ~38%
    hitter_fb = context.get("hitter_fb_rate", 0.38)
    park_factor = context.get("park_hr_factor", 100)
    
    return calculate_profile_mismatch(pitcher_fb, hitter_fb, park_factor)

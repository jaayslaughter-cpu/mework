import pybaseball as pyb
import pandas as pd
from typing import Optional
# Enable caching to prevent redundant network calls and rate limiting
pyb.cache.enable()

def get_player_id(first_name: str, last_name: str) -> Optional[int]:
    """Lookup the MLBAM ID for a specific player."""
    try:
        df = pyb.playerid_lookup(last_name.lower(), first_name.lower())
        if not df.empty:
            return int(df['key_mlbam'].iloc[0])
        return None
    except Exception as e:
        print(f"[Statcast] Error looking up player {first_name} {last_name}: {e}")
        return None

def get_recent_statcast_data(start_date: str, end_date: str) -> list:
    """Fetch pitch-by-pitch Statcast data for a date range."""
    try:
        df = pyb.statcast(start_dt=start_date, end_dt=end_date)
        if df.empty:
            return []
        
        # Filter for core predictive metrics to keep payloads light
        cols = ['pitch_type', 'release_speed', 'batter', 'pitcher', 'events', 
                'description', 'release_spin_rate', 'launch_speed', 'launch_angle']
        
        # Drop rows missing crucial pitch data and return top 100 for API preview
        df_filtered = df[cols].dropna(subset=['pitch_type']).head(100)
        
        # Fill NaNs with None so it serializes cleanly to JSON
        df_filtered = df_filtered.where(pd.notnull(df_filtered), None)
        return df_filtered.to_dict(orient='records')
    except Exception as e:
        print(f"[Statcast] Error fetching data ({start_date} to {end_date}): {e}")
        return []

import xgboost as xgb
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, log_loss, classification_report
import pybaseball as pyb
import requests
import os
import warnings
warnings.filterwarnings('ignore')

pyb.cache.enable()

MODEL_DIR = os.path.join(os.path.dirname(__file__), "../api/models")
os.makedirs(MODEL_DIR, exist_ok=True)

# SportsBlaze Advanced Stats API
SPORTSBLAZE_BATTING_URL = "https://api.sportsblaze.com/mlb/v1/stats/advanced/{season}/batting.json"
SPORTSBLAZE_PITCHING_URL = "https://api.sportsblaze.com/mlb/v1/stats/advanced/{season}/pitching.json"

def fetch_savant_leaderboard(year: int, player_type: str = "batter") -> pd.DataFrame:
    """
    Fetches Baseball Savant leaderboard data using pybaseball.
    Includes Barrels/PA, xwOBA, Hard Hit%, and other Statcast metrics.
    
    Data sources:
    - https://baseballsavant.mlb.com/leaderboard/statcast?type=batter&year={year}
    - https://baseballsavant.mlb.com/leaderboard/statcast?type=pitcher&year={year}
    """
    print(f"Fetching Baseball Savant {player_type} leaderboard for {year}...")
    try:
        if player_type == "batter":
            df = pyb.statcast_batter_expected_stats(year, minPA=50)
        else:
            df = pyb.statcast_pitcher_expected_stats(year, minPA=50)
        
        if df is not None and not df.empty:
            print(f"  Retrieved {len(df)} {player_type} records from Savant")
            return df
        else:
            print(f"  No data returned for {player_type}")
            return pd.DataFrame()
    except Exception as e:
        print(f"  Warning: Could not fetch Savant data via pybaseball: {e}")
        return pd.DataFrame()

def fetch_sportsblaze_stats(season: int, stat_type: str = "batting", api_key: str = None) -> pd.DataFrame:
    """
    Fetches SportsBlaze advanced stats API data.
    Includes xBA, xSLG, xwOBA, barrel rates, swing metrics, and zone analysis.
    
    API: https://api.sportsblaze.com/mlb/v1/stats/advanced/{season}/{type}.json
    """
    if not api_key:
        api_key = os.environ.get("SPORTSBLAZE_API_KEY", "")
    
    if not api_key:
        print(f"  Skipping SportsBlaze {stat_type} (no API key)")
        return pd.DataFrame()
    
    url = SPORTSBLAZE_BATTING_URL if stat_type == "batting" else SPORTSBLAZE_PITCHING_URL
    url = url.format(season=season) + f"?key={api_key}"
    
    print(f"Fetching SportsBlaze {stat_type} stats for {season}...")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        players = data.get("players", [])
        if players:
            records = []
            for p in players:
                record = {"player_id": p["id"], "player_name": p["name"]}
                record.update(p.get("stats", {}))
                records.append(record)
            df = pd.DataFrame(records)
            print(f"  Retrieved {len(df)} {stat_type} records from SportsBlaze")
            return df
    except Exception as e:
        print(f"  Warning: Could not fetch SportsBlaze data: {e}")
    
    return pd.DataFrame()

def fetch_training_data(start_date: str, end_date: str):
    """Fetches historical pitch data for training."""
    print(f"Fetching Statcast data from {start_date} to {end_date}...")
    df = pyb.statcast(start_dt=start_date, end_dt=end_date)
    return df

def fetch_all_data_sources(year: int, start_date: str, end_date: str):
    """
    Aggregates data from multiple sources:
    1. PyBaseball Statcast pitch-level data
    2. Baseball Savant leaderboards (Barrels/PA, xwOBA, Hard Hit%)
    3. SportsBlaze advanced stats (xBA, xSLG, zone metrics)
    """
    print("\n=== FETCHING DATA FROM ALL SOURCES ===\n")
    
    # 1. Pitch-level Statcast data
    statcast_df = fetch_training_data(start_date, end_date)
    
    # 2. Baseball Savant leaderboards
    savant_batters = fetch_savant_leaderboard(year, "batter")
    savant_pitchers = fetch_savant_leaderboard(year, "pitcher")
    
    # 3. SportsBlaze advanced stats (requires API key)
    sportsblaze_batting = fetch_sportsblaze_stats(year, "batting")
    sportsblaze_pitching = fetch_sportsblaze_stats(year, "pitching")
    
    return {
        "statcast": statcast_df,
        "savant_batters": savant_batters,
        "savant_pitchers": savant_pitchers,
        "sportsblaze_batting": sportsblaze_batting,
        "sportsblaze_pitching": sportsblaze_pitching
    }

def engineer_features(df, savant_batters=None, savant_pitchers=None):
    """
    Transforms raw pitch data into predictive features.
    Enriches with Baseball Savant Barrels/PA and other metrics when available.
    """
    print("Engineering features...")
    
    # Focus on balls hit into play
    bip = df.dropna(subset=['launch_speed', 'launch_angle', 'events']).copy()
    
    # Define a "Barrel" (MLB definition: Exit Velo >= 98, optimal launch angle range)
    # Barrel zone expands with higher exit velocity
    bip['is_barrel'] = np.where(
        (bip['launch_speed'] >= 98) & 
        (bip['launch_angle'] >= 26) & 
        (bip['launch_angle'] <= 30), 
        1, 0
    )
    
    # Expanded barrel definition for higher exit velocities
    bip['is_barrel_expanded'] = np.where(
        ((bip['launch_speed'] >= 98) & (bip['launch_angle'] >= 26) & (bip['launch_angle'] <= 30)) |
        ((bip['launch_speed'] >= 99) & (bip['launch_angle'] >= 25) & (bip['launch_angle'] <= 31)) |
        ((bip['launch_speed'] >= 100) & (bip['launch_angle'] >= 24) & (bip['launch_angle'] <= 33)) |
        ((bip['launch_speed'] >= 101) & (bip['launch_angle'] >= 23) & (bip['launch_angle'] <= 35)) |
        ((bip['launch_speed'] >= 102) & (bip['launch_angle'] >= 22) & (bip['launch_angle'] <= 37)) |
        ((bip['launch_speed'] >= 103) & (bip['launch_angle'] >= 21) & (bip['launch_angle'] <= 39)) |
        ((bip['launch_speed'] >= 104) & (bip['launch_angle'] >= 20) & (bip['launch_angle'] <= 41)) |
        ((bip['launch_speed'] >= 105) & (bip['launch_angle'] >= 19) & (bip['launch_angle'] <= 43)) |
        ((bip['launch_speed'] >= 106) & (bip['launch_angle'] >= 18) & (bip['launch_angle'] <= 45)) |
        ((bip['launch_speed'] >= 107) & (bip['launch_angle'] >= 17) & (bip['launch_angle'] <= 47)) |
        ((bip['launch_speed'] >= 108) & (bip['launch_angle'] >= 16) & (bip['launch_angle'] <= 50)),
        1, 0
    )
    
    # Hard hit indicator (95+ mph exit velocity)
    bip['is_hard_hit'] = np.where(bip['launch_speed'] >= 95, 1, 0)
    
    # Sweet spot indicator (8-32 degree launch angle)
    bip['is_sweet_spot'] = np.where(
        (bip['launch_angle'] >= 8) & (bip['launch_angle'] <= 32),
        1, 0
    )
    
    # Merge Savant batter data if available (for Barrels/PA lookup)
    if savant_batters is not None and not savant_batters.empty and 'player_id' in savant_batters.columns:
        # Map batter_id to Savant metrics
        savant_cols = ['player_id', 'barrel_batted_rate', 'hard_hit_percent', 'xwoba', 'xba', 'xslg']
        available_cols = [c for c in savant_cols if c in savant_batters.columns]
        if len(available_cols) > 1:
            savant_subset = savant_batters[available_cols].copy()
            savant_subset = savant_subset.rename(columns={'player_id': 'batter'})
            bip = bip.merge(savant_subset, on='batter', how='left')
            print(f"  Enriched with Savant batter metrics")
    
    # Merge Savant pitcher data if available
    if savant_pitchers is not None and not savant_pitchers.empty and 'player_id' in savant_pitchers.columns:
        pitcher_cols = ['player_id', 'barrel_batted_rate', 'hard_hit_percent', 'xwoba', 'xba', 'xera']
        available_cols = [c for c in pitcher_cols if c in savant_pitchers.columns]
        if len(available_cols) > 1:
            pitcher_subset = savant_pitchers[available_cols].copy()
            pitcher_subset = pitcher_subset.rename(columns={
                'player_id': 'pitcher',
                'barrel_batted_rate': 'pitcher_barrel_rate',
                'hard_hit_percent': 'pitcher_hard_hit_pct',
                'xwoba': 'pitcher_xwoba',
                'xba': 'pitcher_xba',
                'xera': 'pitcher_xera'
            })
            bip = bip.merge(pitcher_subset, on='pitcher', how='left')
            print(f"  Enriched with Savant pitcher metrics")
    
    # Target Variable: Did the event result in a Hit? (1 = Yes, 0 = No)
    hit_events = ['single', 'double', 'triple', 'home_run']
    bip['target_is_hit'] = bip['events'].apply(lambda x: 1 if x in hit_events else 0)
    
    # Extra-base hit target for power prop predictions
    xbh_events = ['double', 'triple', 'home_run']
    bip['target_is_xbh'] = bip['events'].apply(lambda x: 1 if x in xbh_events else 0)
    
    # Home run target
    bip['target_is_hr'] = bip['events'].apply(lambda x: 1 if x == 'home_run' else 0)
    
    # Core ML features
    base_features = [
        'release_speed', 'release_spin_rate', 'launch_speed', 'launch_angle',
        'is_barrel', 'is_barrel_expanded', 'is_hard_hit', 'is_sweet_spot'
    ]
    
    # Add Savant-enriched features if available
    enriched_features = ['barrel_batted_rate', 'hard_hit_percent', 'xwoba', 'xba', 'xslg',
                         'pitcher_barrel_rate', 'pitcher_hard_hit_pct', 'pitcher_xwoba']
    
    features = base_features.copy()
    for feat in enriched_features:
        if feat in bip.columns:
            features.append(feat)
    
    # Clean NaNs before training
    model_df = bip[features + ['target_is_hit', 'target_is_xbh', 'target_is_hr']].dropna(subset=base_features)
    
    # Fill NaN enriched features with league averages
    for feat in enriched_features:
        if feat in model_df.columns:
            model_df[feat] = model_df[feat].fillna(model_df[feat].median())
    
    model_df = model_df.dropna()
    
    return model_df[features], model_df['target_is_hit'], model_df['target_is_xbh'], model_df['target_is_hr']

def train_xgboost():
    # Configuration
    TRAINING_YEAR = 2023
    START_DATE = '2023-04-01'
    END_DATE = '2023-04-15'
    
    # 1. Fetch data from all sources
    data_sources = fetch_all_data_sources(TRAINING_YEAR, START_DATE, END_DATE)
    
    statcast_df = data_sources["statcast"]
    savant_batters = data_sources["savant_batters"]
    savant_pitchers = data_sources["savant_pitchers"]
    
    if statcast_df.empty:
        print("Error: No Statcast data fetched. Aborting training.")
        return
    
    # 2. Prepare Features and Targets (Hit, XBH, HR)
    X, y_hit, y_xbh, y_hr = engineer_features(
        statcast_df, 
        savant_batters=savant_batters,
        savant_pitchers=savant_pitchers
    )
    
    print(f"\nDataset prepared: {len(X)} records with {len(X.columns)} features")
    print(f"Features: {list(X.columns)}")
    
    # 3. Train Hit Prediction Model
    print("\n" + "="*50)
    print("TRAINING HIT PREDICTION MODEL")
    print("="*50)
    
    X_train, X_test, y_train, y_test = train_test_split(X, y_hit, test_size=0.2, random_state=42)
    
    hit_model = xgb.XGBClassifier(
        objective='binary:logistic',
        n_estimators=100,
        learning_rate=0.05,
        max_depth=4,
        eval_metric='logloss',
        use_label_encoder=False
    )
    
    hit_model.fit(X_train, y_train)
    
    predictions = hit_model.predict(X_test)
    prob_predictions = hit_model.predict_proba(X_test)[:, 1]
    
    print(f"\nHit Model Accuracy: {accuracy_score(y_test, predictions) * 100:.2f}%")
    print(f"Hit Model Log Loss: {log_loss(y_test, prob_predictions):.4f}")
    print("\nClassification Report (Hits):")
    print(classification_report(y_test, predictions))
    
    # Save Hit Model
    hit_model_path = os.path.join(MODEL_DIR, "prop_model_v1.json")
    hit_model.save_model(hit_model_path)
    print(f"Hit model saved to {hit_model_path}")
    
    # 4. Train Extra-Base Hit Model (for Total Bases props)
    print("\n" + "="*50)
    print("TRAINING EXTRA-BASE HIT MODEL (Total Bases)")
    print("="*50)
    
    X_train_xbh, X_test_xbh, y_train_xbh, y_test_xbh = train_test_split(X, y_xbh, test_size=0.2, random_state=42)
    
    xbh_model = xgb.XGBClassifier(
        objective='binary:logistic',
        n_estimators=150,
        learning_rate=0.03,
        max_depth=5,
        eval_metric='logloss',
        use_label_encoder=False,
        scale_pos_weight=3  # XBH are less common, balance the classes
    )
    
    xbh_model.fit(X_train_xbh, y_train_xbh)
    
    xbh_predictions = xbh_model.predict(X_test_xbh)
    xbh_prob = xbh_model.predict_proba(X_test_xbh)[:, 1]
    
    print(f"\nXBH Model Accuracy: {accuracy_score(y_test_xbh, xbh_predictions) * 100:.2f}%")
    print(f"XBH Model Log Loss: {log_loss(y_test_xbh, xbh_prob):.4f}")
    print("\nClassification Report (Extra-Base Hits):")
    print(classification_report(y_test_xbh, xbh_predictions))
    
    # Save XBH Model
    xbh_model_path = os.path.join(MODEL_DIR, "xbh_model_v1.json")
    xbh_model.save_model(xbh_model_path)
    print(f"XBH model saved to {xbh_model_path}")
    
    # 5. Train Home Run Model (for HR props)
    print("\n" + "="*50)
    print("TRAINING HOME RUN MODEL")
    print("="*50)
    
    X_train_hr, X_test_hr, y_train_hr, y_test_hr = train_test_split(X, y_hr, test_size=0.2, random_state=42)
    
    hr_model = xgb.XGBClassifier(
        objective='binary:logistic',
        n_estimators=200,
        learning_rate=0.02,
        max_depth=6,
        eval_metric='logloss',
        use_label_encoder=False,
        scale_pos_weight=10  # HRs are rare, heavily balance
    )
    
    hr_model.fit(X_train_hr, y_train_hr)
    
    hr_predictions = hr_model.predict(X_test_hr)
    hr_prob = hr_model.predict_proba(X_test_hr)[:, 1]
    
    print(f"\nHR Model Accuracy: {accuracy_score(y_test_hr, hr_predictions) * 100:.2f}%")
    print(f"HR Model Log Loss: {log_loss(y_test_hr, hr_prob):.4f}")
    print("\nClassification Report (Home Runs):")
    print(classification_report(y_test_hr, hr_predictions))
    
    # Save HR Model
    hr_model_path = os.path.join(MODEL_DIR, "hr_model_v1.json")
    hr_model.save_model(hr_model_path)
    print(f"HR model saved to {hr_model_path}")
    
    # 6. Feature Importance Summary
    print("\n" + "="*50)
    print("FEATURE IMPORTANCE (Hit Model)")
    print("="*50)
    importance = pd.DataFrame({
        'feature': X.columns,
        'importance': hit_model.feature_importances_
    }).sort_values('importance', ascending=False)
    print(importance.to_string(index=False))
    
    print("\n" + "="*50)
    print("TRAINING COMPLETE")
    print("="*50)
    print(f"\nModels saved to {MODEL_DIR}:")
    print(f"  - prop_model_v1.json (Hit prediction)")
    print(f"  - xbh_model_v1.json (Extra-base hit prediction)")
    print(f"  - hr_model_v1.json (Home run prediction)")

if __name__ == "__main__":
    train_xgboost()

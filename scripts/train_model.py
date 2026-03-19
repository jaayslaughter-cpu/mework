import xgboost as xgb
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, log_loss, classification_report
import pybaseball as pyb
import os
import warnings
warnings.filterwarnings('ignore')

pyb.cache.enable()

MODEL_DIR = os.path.join(os.path.dirname(__file__), "../api/models")
os.makedirs(MODEL_DIR, exist_ok=True)

def fetch_training_data(start_date: str, end_date: str):
    """Fetches historical pitch data for training."""
    print(f"Fetching Statcast data from {start_date} to {end_date}...")
    df = pyb.statcast(start_dt=start_date, end_dt=end_date)
    return df

def engineer_features(df):
    """
    Transforms raw pitch data into predictive features.
    Filters for balls in play and calculates barrel percentages.
    """
    print("Engineering features...")
    
    # Focus on balls hit into play
    bip = df.dropna(subset=['launch_speed', 'launch_angle', 'events']).copy()
    
    # Define a "Barrel" (simplistic representation for scaffolding: Exit Velo >= 98, Angle between 26-30)
    bip['is_barrel'] = np.where(
        (bip['launch_speed'] >= 98) & 
        (bip['launch_angle'] >= 26) & 
        (bip['launch_angle'] <= 30), 
        1, 0
    )
    
    # In a production model, we would group by player and roll these stats over a 14-day window.
    # For this training scaffold, we will use the raw pitch metrics to predict the event outcome.
    
    # Target Variable: Did the event result in a Hit? (1 = Yes, 0 = No)
    hit_events = ['single', 'double', 'triple', 'home_run']
    bip['target_is_hit'] = bip['events'].apply(lambda x: 1 if x in hit_events else 0)
    
    # Select our core ML features
    features = ['release_speed', 'release_spin_rate', 'launch_speed', 'launch_angle', 'is_barrel']
    
    # Clean NaNs before training
    model_df = bip[features + ['target_is_hit']].dropna()
    
    return model_df[features], model_df['target_is_hit']

def train_xgboost():
    # 1. Get Data (Using a small window for the scaffold to prevent massive memory spikes)
    # In production, this should be expanded to span multiple seasons.
    raw_df = fetch_training_data('2023-04-01', '2023-04-15')
    
    if raw_df.empty:
        print("Error: No data fetched. Aborting training.")
        return
        
    # 2. Prepare Features and Target
    X, y = engineer_features(raw_df)
    
    print(f"Dataset prepared: {len(X)} records.")
    
    # 3. Split Data (80% Train, 20% Test)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 4. Initialize and Train the XGBoost Classifier
    print("Training XGBoost Model...")
    model = xgb.XGBClassifier(
        objective='binary:logistic',
        n_estimators=100,
        learning_rate=0.05,
        max_depth=4,
        eval_metric='logloss',
        use_label_encoder=False
    )
    
    model.fit(X_train, y_train)
    
    # 5. Evaluate the Model
    predictions = model.predict(X_test)
    prob_predictions = model.predict_proba(X_test)[:, 1]
    
    accuracy = accuracy_score(y_test, predictions)
    loss = log_loss(y_test, prob_predictions)
    
    print("\n--- Model Evaluation ---")
    print(f"Accuracy: {accuracy * 100:.2f}%")
    print(f"Log Loss: {loss:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_test, predictions))
    
    # 6. Save the Model for FastAPI to consume
    model_path = os.path.join(MODEL_DIR, "prop_model_v1.json")
    model.save_model(model_path)
    print(f"\nModel successfully saved to {model_path}")

if __name__ == "__main__":
    train_xgboost()

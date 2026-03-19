import streamlit as st
import requests
import pandas as pd

# Configuration
st.set_page_config(page_title="PropIQ Command Center", layout="wide", page_icon="⚾")

HUB_URL = "http://localhost:3002/api/slates/today"
ENGINE_URL = "http://localhost:8000/api/predict/edge"

@st.cache_data(ttl=60)
def fetch_hub_data():
    """Fetch the daily slate and live markets from the Node.js Hub."""
    try:
        response = requests.get(HUB_URL)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"Failed to connect to Fast-Data Hub: {e}")
        return None

def evaluate_prop(player_id, category, line, over_odds, under_odds):
    """Send market data to the ML Engine to calculate the edge."""
    payload = {
        "player_id": player_id,
        "prop_category": category,
        "line": line,
        "over_odds": over_odds or 0,
        "under_odds": under_odds or 0
    }
    try:
        res = requests.post(ENGINE_URL, json=payload)
        if res.status_code == 200:
            return res.json()
        return None
    except:
        return None

# --- UI Layout ---
st.title("⚾ PropIQ Command Center")
st.markdown("Live Data Hub & ML Edge Detection")

data = fetch_hub_data()

if data and data.get("status") == "success":
    games = data["slate"]["games"]
    markets = data["markets"]
    
    st.sidebar.header("Game Filter")
    if not games:
        st.warning("No games found for today.")
    else:
        game_options = {g["GameID"]: f"{g['AwayTeam']} @ {g['HomeTeam']}" for g in games}
        selected_game = st.sidebar.selectbox("Select a Matchup", options=list(game_options.keys()), format_func=lambda x: game_options[x])
        
        st.subheader(f"Markets: {game_options[selected_game]}")
        
        # Filter markets for the selected game
        game_markets = [m for m in markets if str(m["game_id"]) == str(selected_game)]
        
        if not game_markets:
            st.info("No active lines for this game yet.")
        else:
            # Process and display markets
            edges = []
            with st.spinner("Calculating ML Edges..."):
                for m in game_markets:
                    # Using a dummy player_id of 0 for scaffold until rosters are fully mapped
                    edge_data = evaluate_prop(
                        player_id=0, 
                        category=m["prop_category"], 
                        line=m["line"], 
                        over_odds=m["over_odds"], 
                        under_odds=m["under_odds"]
                    )
                    if edge_data:
                        edge_data["Sportsbook"] = m["sportsbook"]
                        edge_data["Category"] = m["prop_category"]
                        edges.append(edge_data)
            
            if edges:
                df = pd.DataFrame(edges)
                # Reorder columns for readability
                df = df[["Sportsbook", "Category", "line", "vegas_implied_over", "model_projected_over", "edge_percentage", "is_playable"]]
                
                # Highlight +EV plays
                st.dataframe(
                    df.style.map(lambda x: 'background-color: #2ecc71; color: black;' if x else '', subset=['is_playable'])
                )

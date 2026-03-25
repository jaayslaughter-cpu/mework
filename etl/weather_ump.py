"""
etl/weather_ump.py
Updates park weather conditions and umpire assignments in the database.
Uses SportsData.io for game data and umpire info.
"""
import os
import logging
import requests
import psycopg2

logger = logging.getLogger(__name__)

SPORTSDATA_KEY = os.environ.get("SPORTSDATA_API_KEY", "")
BASE = "https://api.sportsdata.io/v3/mlb"

DB_CONN = {
    "dbname": os.environ.get("POSTGRES_DB", "propiq"),
    "user": os.environ.get("POSTGRES_USER", "propiq_admin"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": 5432,
}

# Umpire run factor lookup (historical average runs allowed above/below MLB avg)
# Source: umpire scorecards / historical analysis
UMP_RUN_FACTORS = {
    "Angel Hernandez": 1.08, "CB Bucknor": 1.06, "Phil Cuzzi": 1.05,
    "Doug Eddings": 0.97, "Andy Fletcher": 0.96, "Kerwin Danley": 1.02,
    "Ted Barrett": 1.01, "Jeff Nelson": 0.99, "John Hirschbeck": 1.03,
}


def _fetch_games(date: str) -> list:
    if not SPORTSDATA_KEY:
        logger.warning("[WeatherUmp] SPORTSDATA_API_KEY not set.")
        return []
    try:
        r = requests.get(
            f"{BASE}/scores/json/GamesByDate/{date}?key={SPORTSDATA_KEY}",
            timeout=15,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error("[WeatherUmp] Failed to fetch games: %s", e)
        return []


def update_weather_ump(date: str) -> None:
    """
    For each game today:
    - Updates park_weather (wind_mph, temp_f, is_dome) in games table
    - Updates ump_run_factor and ump_k_index in games table
    """
    games = _fetch_games(date)
    if not games:
        logger.warning("[WeatherUmp] No games found for %s", date)
        return

    try:
        conn = psycopg2.connect(**DB_CONN)
        cur = conn.cursor()

        for game in games:
            game_id = str(game.get("GameID", ""))
            if not game_id:
                continue

            # Weather data
            wind_speed = game.get("WindSpeed", 0) or 0
            temp = game.get("Temperature", 72) or 72
            stadium = game.get("StadiumDetails", {}) or {}
            is_dome = bool(stadium.get("IsDome", False))

            # Umpire data
            ump_name = game.get("HomePlateSupervisor", "") or ""
            ump_run_factor = UMP_RUN_FACTORS.get(ump_name, 1.0)
            # K index: umpires with tight zones increase strikeouts
            ump_k_index = 1.0 + (1.0 - ump_run_factor) * 0.5

            cur.execute("""
                UPDATE games SET
                    wind_mph = %s,
                    temp_f = %s,
                    is_dome = %s,
                    ump_name = %s,
                    ump_run_factor = %s,
                    ump_k_index = %s
                WHERE game_id = %s;
            """, (wind_speed, temp, is_dome, ump_name, ump_run_factor, ump_k_index, game_id))

        conn.commit()
        cur.close()
        conn.close()
        logger.info("[WeatherUmp] Updated %s games for %s", len(games), date)
    except Exception as e:
        logger.error("[WeatherUmp] DB error: %s", e)

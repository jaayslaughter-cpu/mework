"""
etl/odds_pipeline.py
Fetches today's odds from The Odds API and loads into the betting_markets table.
"""
import os
import logging
import requests
import psycopg2
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")
BASE_URL = "https://api.the-odds-api.com/v4/sports/baseball_mlb"

DB_CONN = {
    "dbname": os.environ.get("POSTGRES_DB", "propiq"),
    "user": os.environ.get("POSTGRES_USER", "propiq_admin"),
    "password": os.environ.get("POSTGRES_PASSWORD", ""),
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": 5432,
}

MARKETS = "pitcher_strikeouts,batter_total_bases,batter_home_runs,batter_hits_runs_rbis"
BOOKMAKERS = "draftkings,fanduel,underdog"


def _get_events() -> list:
    if not ODDS_API_KEY:
        logger.error("[OddsETL] ODDS_API_KEY not set.")
        return []
    try:
        r = requests.get(f"{BASE_URL}/events?apiKey={ODDS_API_KEY}", timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error("[OddsETL] Failed to fetch events: %s", e)
        return []


def _get_props(event_id: str) -> dict:
    try:
        url = (
            f"{BASE_URL}/events/{event_id}/odds"
            f"?apiKey={ODDS_API_KEY}&regions=us&markets={MARKETS}"
            f"&bookmakers={BOOKMAKERS}&oddsFormat=american"
        )
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning("[OddsETL] Props fetch failed for %s: %s", event_id, e)
        return {}


def _ensure_date(date: str = None) -> str:
    if date:
        return date
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _filter_events_by_date(events: list, date: str) -> list:
    return [e for e in events if e.get("commence_time", "")[:10] == date]


def run_odds_etl(date: str = None) -> int:
    """
    Main ETL entry point. Fetches and upserts all MLB prop markets for today.
    Returns number of markets upserted.
    """
    date = _ensure_date(date)

    logger.info("[OddsETL] Starting odds ETL for %s", date)
    events = _get_events()
    if not events:
        logger.warning("[OddsETL] No events found. Exiting.")
        return 0

    today_events = _filter_events_by_date(events, date)
    logger.info("[OddsETL] Found %s games for %s", len(today_events), date)

    upsert_count = 0
    try:
        conn = psycopg2.connect(**DB_CONN)
        cur = conn.cursor()

        for event in today_events:
            props = _get_props(event["id"])
            if not props.get("bookmakers"):
                continue

            for book in props["bookmakers"]:
                for market in book.get("markets", []):
                    outcomes = market.get("outcomes", [])

                    # Aggregate Over/Under by description
                    outcome_map = {}
                    for o in outcomes:
                        desc = o.get("description", "base")
                        if desc not in outcome_map:
                            outcome_map[desc] = {"point": o.get("point", 0.5), "over_odds": None, "under_odds": None}
                        name = (o.get("name") or "").lower()
                        if name == "over":
                            outcome_map[desc]["over_odds"] = o.get("price")
                        elif name == "under":
                            outcome_map[desc]["under_odds"] = o.get("price")

                    for desc, data in outcome_map.items():
                        if data["over_odds"] is None and data["under_odds"] is None:
                            continue

                        market_id = (
                            f"{event['id']}_{book['key']}_{market['key']}_{desc}_{data['point']}"
                            .replace(" ", "_").lower()
                        )

                        cur.execute("""
                            INSERT INTO betting_markets (
                                market_id, game_id, pitcher_id, sportsbook, prop_category,
                                line, over_odds, under_odds, updated_at
                            ) VALUES (%s, %s, NULL, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (market_id) DO UPDATE SET
                                line = EXCLUDED.line,
                                over_odds = EXCLUDED.over_odds,
                                under_odds = EXCLUDED.under_odds,
                                updated_at = NOW();
                        """, (
                            market_id, event["id"], book["key"], market["key"],
                            data["point"], data["over_odds"], data["under_odds"],
                        ))
                        upsert_count += 1

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error("[OddsETL] Database error: %s", e)

    logger.info("[OddsETL] Upserted %s markets for %s", upsert_count, date)
    return upsert_count
